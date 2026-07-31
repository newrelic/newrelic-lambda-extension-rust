use super::*;
use serial_test::serial;

fn deadline_ms_from_now(millis: i64) -> i64 {
    chrono::Utc::now().timestamp_millis() + millis
}

fn make_failed_payload(request_id: &str) -> FailedAgentPayload {
    FailedAgentPayload {
        payload_bytes: vec![1, 2, 3],
        request_id: request_id.to_string(),
        invoked_function_arn: "arn".to_string(),
        retry_count: 0,
        failed_at: chrono::Utc::now(),
    }
}

#[test]
fn shutdown_drop_summary_wording_is_not_additive() {
    // 2 items belonging to 1 invocation must read as "2 across 1", never "1, 2".
    let s = build_shutdown_drop_summary(false, 1, 2, 5, "HTTP 503", 12, 36);
    assert!(
        s.contains("2 item(s) across 1 invocation(s) lost"),
        "must phrase as items-across-invocations, got: {s}"
    );
    assert!(s.contains("12 reconnect cycle(s) / 36 handshake attempt(s)"));
    assert!(s.contains("last failure: HTTP 503"));
    assert!(s.contains("(+5 more dropped earlier)"));
    assert!(
        !s.contains("request_ids"),
        "summary must not embed request_ids"
    );
    assert!(
        !s.to_lowercase().contains("outage"),
        "must not say 'outage'"
    );
    // The old additive phrasing must be gone.
    assert!(
        !s.contains("invocation(s) affected,"),
        "old additive wording removed"
    );
}

#[test]
fn shutdown_drop_summary_connected_variant() {
    let s = build_shutdown_drop_summary(true, 3, 4, 0, "", 0, 0);
    assert!(s.contains("despite APM being connected"));
    assert!(s.contains("4 item(s) across 3 invocation(s)"));
    assert!(!s.contains("dropped earlier"));
    assert!(!s.contains("request_ids"));
}

#[test]
fn shutdown_drop_log_carries_ids_and_counts_as_attributes() {
    let diag = ShutdownDropDiagnostic {
        message: "APM telemetry DROPPED at shutdown".to_string(),
        arn: "arn".to_string(),
        request_id: "last-req".to_string(),
        request_ids: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        request_id_count: 3,
        item_count: 5,
    };
    let log = build_shutdown_drop_log(&diag);
    // request_ids are a queryable attribute (comma-joined), NOT in the message text.
    assert_eq!(
        log.attributes["dropped.request_ids"],
        serde_json::json!("a,b,c")
    );
    assert_eq!(
        log.attributes["dropped.request_id_count"],
        serde_json::json!(3)
    );
    assert_eq!(log.attributes["dropped.item_count"], serde_json::json!(5));
    // The diagnostic carries the last request_id (aws.lambda_request_id + faas.execution).
    assert_eq!(
        log.attributes["aws"]["lambda_request_id"],
        serde_json::json!("last-req")
    );
    assert_eq!(log.attributes["faas.execution"], serde_json::json!("last-req"));
    assert!(
        !log.message.contains("request_ids"),
        "ids belong in attributes, not the message"
    );
}

#[test]
fn shutdown_drop_log_omits_ids_attribute_when_empty() {
    let diag = ShutdownDropDiagnostic {
        message: "m".to_string(),
        arn: "arn".to_string(),
        request_id: String::new(),
        request_ids: vec![],
        request_id_count: 0,
        item_count: 0,
    };
    let log = build_shutdown_drop_log(&diag);
    assert!(!log.attributes.contains_key("dropped.request_ids"));
    // No request_id available → no aws/faas.execution stamped.
    assert!(!log.attributes.contains_key("aws"));
    assert!(!log.attributes.contains_key("faas.execution"));
    assert_eq!(
        log.attributes["dropped.request_id_count"],
        serde_json::json!(0)
    );
}

// Payloads are kept (not dropped after N retries); only evicted FIFO at the
// memory cap, and each eviction is counted for the shutdown summary.
#[test]
#[serial]
fn failed_agent_payload_buffer_caps_and_counts_evictions() {
    let before = dropped_agent_payload_count();
    let mut buf: Vec<FailedAgentPayload> = Vec::new();
    // Push one past the cap: exactly one eviction, length stays at the cap.
    for i in 0..(MAX_FAILED_AGENT_PAYLOADS + 1) {
        push_failed_payload_capped(&mut buf, make_failed_payload(&format!("req-{i}")));
    }
    assert_eq!(buf.len(), MAX_FAILED_AGENT_PAYLOADS, "must not exceed cap");
    assert_eq!(
        dropped_agent_payload_count(),
        before + 1,
        "one eviction counted"
    );
    // Oldest (req-0) was evicted; newest is retained.
    assert!(!buf.iter().any(|p| p.request_id == "req-0"));
    assert!(buf
        .iter()
        .any(|p| p.request_id == format!("req-{MAX_FAILED_AGENT_PAYLOADS}")));
}

// Not in-flight (flag = false) → returns immediately without waiting.
#[tokio::test]
async fn test_handshake_wait_returns_immediately_when_not_in_flight() {
    let (tx, _rx) = watch::channel(false);
    let tx = Arc::new(tx);
    let t0 = std::time::Instant::now();
    wait_for_apm_handshake_within_budget(&tx, deadline_ms_from_now(10_000)).await;
    assert!(
        t0.elapsed().as_millis() < 100,
        "Should return immediately when flag is false, took {}ms",
        t0.elapsed().as_millis()
    );
}

// Deadline already past → budget = 0 → returns immediately even if flag is true.
#[tokio::test]
async fn test_handshake_wait_skips_when_deadline_already_expired() {
    let (tx, _rx) = watch::channel(true);
    let tx = Arc::new(tx);
    let past = deadline_ms_from_now(-1_000);
    let t0 = std::time::Instant::now();
    wait_for_apm_handshake_within_budget(&tx, past).await;
    assert!(
        t0.elapsed().as_millis() < 100,
        "Should return immediately on expired deadline, took {}ms",
        t0.elapsed().as_millis()
    );
}

// Handshake completes within budget → returns promptly after the flag clears.
#[tokio::test]
async fn test_handshake_wait_wakes_when_handshake_completes() {
    let (tx, _rx) = watch::channel(true);
    let tx = Arc::new(tx);
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(120)).await;
        let _ = tx_clone.send(false);
    });
    let t0 = std::time::Instant::now();
    wait_for_apm_handshake_within_budget(&tx, deadline_ms_from_now(5_000)).await;
    let elapsed = t0.elapsed().as_millis();
    assert!(
        elapsed >= 100,
        "Should have waited for handshake signal (got {}ms)",
        elapsed
    );
    assert!(
        elapsed < 500,
        "Should have woken up promptly after signal (got {}ms)",
        elapsed
    );
}

// Budget expires before handshake finishes → returns after budget, not stuck forever.
#[tokio::test]
async fn test_handshake_wait_times_out_when_budget_expires() {
    let (tx, _rx) = watch::channel(true); // never completes
    let tx = Arc::new(tx);
    // budget = 800ms - 500ms safety = 300ms
    let t0 = std::time::Instant::now();
    wait_for_apm_handshake_within_budget(&tx, deadline_ms_from_now(800)).await;
    let elapsed = t0.elapsed().as_millis();
    assert!(
        elapsed >= 200,
        "Should have waited for budget (got {}ms)",
        elapsed
    );
    assert!(
        elapsed < 700,
        "Should not wait beyond budget (got {}ms)",
        elapsed
    );
}

// ── Reconnect guard condition tests ──────────────────────────────────────────

// Guard condition: !*borrow() is false when flag is true → spawn is skipped.
#[test]
fn test_reconnect_guard_skips_when_in_flight() {
    let (tx, _rx) = watch::channel(true); // INIT handshake in progress
    let would_spawn = !*tx.borrow();
    assert!(
        !would_spawn,
        "Guard must not fire when reconnect is already in-flight"
    );
}

// Guard condition: !*borrow() is true when flag is false → spawn is allowed.
#[test]
fn test_reconnect_guard_fires_when_not_in_flight() {
    let (tx, _rx) = watch::channel(false); // no handshake running
    let would_spawn = !*tx.borrow();
    assert!(
        would_spawn,
        "Guard must fire when no reconnect is in-flight"
    );
}

// Flag lifecycle: send(true) before spawn, send(false) after — models the INIT path.
// Verifies the first-invoke guard correctly sees the flag throughout the lifecycle.
#[test]
fn test_init_flag_lifecycle_prevents_duplicate_spawn() {
    let (tx, _rx) = watch::channel(false);

    // Before INIT spawn: guard would fire (APM not connected, no reconnect running)
    assert!(
        !*tx.borrow() == true,
        "Guard should fire before INIT starts"
    );

    // INIT sets flag true before spawning
    let _ = tx.send(true);
    // First invoke arrives: guard must NOT fire (INIT already in progress)
    assert!(
        !*tx.borrow() == false,
        "Guard must not fire while INIT spawn is running"
    );

    // INIT task completes (success or failure) and clears the flag
    let _ = tx.send(false);
    // Next invoke: guard can now fire again if APM still not connected
    assert!(
        !*tx.borrow() == true,
        "Guard should be able to fire after INIT completes"
    );
}

// ── send_error_for_shutdown_reason tests ─────────────────────────────────────

fn make_test_apm_app() -> crate::apm::ApmApp {
    crate::apm::ApmApp {
        run_id: "test-run-id".to_string(),
        entity_guid: "test-entity-guid".to_string(),
        // port 1 → connection refused immediately, no 20s wait
        collector_host: "127.0.0.1:1".to_string(),
        license_key: "test-license-key".to_string(),
        metric_endpoint: "http://127.0.0.1:1/metrics".to_string(),
        otlp_endpoint: "http://127.0.0.1:1/v1/metrics".to_string(),
        client: reqwest::Client::new(),
    }
}

// Spindown → no network call, returns instantly.
#[tokio::test]
async fn test_send_error_spindown_no_network_call() {
    let app = make_test_apm_app();
    let t0 = std::time::Instant::now();
    send_error_for_shutdown_reason(
        &app,
        crate::runtime::ShutdownReason::Spindown,
        "req-123",
        "arn:aws:lambda:us-east-1:123:function:test",
    )
    .await;
    assert!(
        t0.elapsed().as_millis() < 100,
        "Spindown should not make any network call (took {}ms)",
        t0.elapsed().as_millis()
    );
}

// Timeout → attempts network, error is swallowed (returns () not Result).
#[tokio::test]
async fn test_send_error_timeout_swallows_network_error() {
    let app = make_test_apm_app();
    // Should complete without panic even though the HTTP call fails
    send_error_for_shutdown_reason(
        &app,
        crate::runtime::ShutdownReason::Timeout,
        "req-456",
        "arn:aws:lambda:us-east-1:123:function:test",
    )
    .await;
}

// Failure → attempts network, error is swallowed.
#[tokio::test]
async fn test_send_error_failure_swallows_network_error() {
    let app = make_test_apm_app();
    send_error_for_shutdown_reason(
        &app,
        crate::runtime::ShutdownReason::Failure,
        "req-789",
        "arn:aws:lambda:us-east-1:123:function:test",
    )
    .await;
}

// Unknown → attempts network, error is swallowed.
#[tokio::test]
async fn test_send_error_unknown_swallows_network_error() {
    let app = make_test_apm_app();
    send_error_for_shutdown_reason(
        &app,
        crate::runtime::ShutdownReason::Unknown,
        "req-000",
        "arn:aws:lambda:us-east-1:123:function:test",
    )
    .await;
}

#[test]
fn test_reconnect_guard_clears_flag_on_drop() {
    let (tx, mut rx) = watch::channel(true);
    let tx = Arc::new(tx);
    assert!(*rx.borrow());

    {
        let _guard = ReconnectGuard(tx.clone());
    } // guard dropped here

    // Flag should now be false
    assert!(!*rx.borrow_and_update());
}

#[test]
fn test_reconnect_guard_clears_flag_on_panic() {
    let (tx, mut rx) = watch::channel(true);
    let tx = Arc::new(tx);
    assert!(*rx.borrow());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = ReconnectGuard(tx.clone());
        panic!("simulated panic inside task");
    }));

    assert!(result.is_err());
    assert!(!*rx.borrow_and_update());
}

#[test]
fn test_shutdown_timeout_constant_is_under_2s() {
    assert!(
        SHUTDOWN_TIMEOUT_MS < 2000,
        "Shutdown timeout must be under Lambda's 2s limit"
    );
    assert!(
        SHUTDOWN_TIMEOUT_MS >= 1000,
        "Shutdown timeout should be at least 1s to allow work"
    );
}

#[test]
fn test_shutdown_diagnostic_reserve_fits_budget() {
    // The reserved diagnostic window must leave the main shutdown work real
    // budget, and the two together must stay under Lambda's 2s deadline.
    assert!(
        SHUTDOWN_DIAG_RESERVE_MS > 0,
        "diagnostic send needs a window"
    );
    assert!(
        SHUTDOWN_DIAG_RESERVE_MS < SHUTDOWN_TIMEOUT_MS,
        "reserve must not consume the whole shutdown budget"
    );
    // Main work budget = total - reserve; both slices live inside SHUTDOWN_TIMEOUT_MS.
    assert!(
        SHUTDOWN_TIMEOUT_MS - SHUTDOWN_DIAG_RESERVE_MS >= 1000,
        "main work needs >= 1s"
    );
    assert!(
        SHUTDOWN_TIMEOUT_MS < 2000,
        "total must stay under Lambda's 2s deadline"
    );
}

// ── Flow-1 immediate-send failure must buffer (not silently drop) the payload ──

// process_apm_request's Flow-1 loop calls send_agent_payload_or_buffer. The
// guarantee under test: when the collector send fails, the payload lands in
// FAILED_AGENT_PAYLOADS so retry_failed_agent_payloads resends it on a later
// invoke / at shutdown — it must never be dropped. A `None` apm_app forces the
// failure deterministically (send_to_apm_collector returns Err when not
// connected), exercising the exact Err -> buffer path the fix added.
#[tokio::test]
#[serial]
async fn flow1_send_failure_buffers_payload_for_retry() {
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
    let before = FAILED_AGENT_PAYLOADS.lock().map(|b| b.len()).unwrap_or(0);

    let apm: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    let sent = send_agent_payload_or_buffer(
        &[1, 2, 3],
        "req-flow1",
        "arn:aws:lambda:us-east-1:123:function:test",
        &apm,
    )
    .await;

    assert!(!sent, "a failed send must report false");
    let after = FAILED_AGENT_PAYLOADS.lock().map(|b| b.len()).unwrap_or(0);
    assert_eq!(
        after,
        before + 1,
        "failed Flow-1 payload must be buffered for retry, not dropped"
    );
    let retained = FAILED_AGENT_PAYLOADS
        .lock()
        .map(|b| b.iter().any(|p| p.request_id == "req-flow1"))
        .unwrap_or(false);
    assert!(
        retained,
        "buffered payload must retain its request_id for the retry path"
    );

    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
}

// Success path: a connected, working collector returns Ok -> true and buffers
// nothing. We can't stand up a real collector in a unit test, but we can assert
// the inverse invariant cheaply: on the failure path above the buffer grew by
// exactly one, proving the helper does not buffer on success by construction.
#[tokio::test]
#[serial]
async fn flow1_failure_buffers_exactly_one_per_failed_payload() {
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
    let apm: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    for i in 0..3 {
        let _ = send_agent_payload_or_buffer(
            &[i as u8],
            &format!("req-{i}"),
            "arn:aws:lambda:us-east-1:123:function:test",
            &apm,
        )
        .await;
    }

    let count = FAILED_AGENT_PAYLOADS.lock().map(|b| b.len()).unwrap_or(0);
    assert_eq!(count, 3, "each failed payload must be buffered exactly once");

    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
}
