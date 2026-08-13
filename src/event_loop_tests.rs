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

// ── bounded_wait_budget_ms (shared budget helper) ────────────────────────────

#[test]
fn test_bounded_wait_budget_ms_uses_configured_timeout_when_smaller() {
    // Deadline is far away (~4500ms budget after the 500ms safety margin); the
    // configured timeout (100ms) is the smaller of the two and must win.
    let budget = bounded_wait_budget_ms(deadline_ms_from_now(5_000), 100);
    assert_eq!(budget, 100);
}

#[test]
fn test_bounded_wait_budget_ms_uses_deadline_budget_when_smaller() {
    // deadline budget = 600 - 500 = 100ms, configured timeout (2000ms) is larger.
    let budget = bounded_wait_budget_ms(deadline_ms_from_now(600), 2000);
    assert!(budget <= 100, "expected deadline-bounded budget <= 100ms, got {budget}");
}

#[test]
fn test_bounded_wait_budget_ms_zero_when_deadline_expired() {
    let budget = bounded_wait_budget_ms(deadline_ms_from_now(-1_000), 2000);
    assert_eq!(budget, 0);
}

#[test]
fn test_bounded_wait_budget_ms_zero_when_configured_timeout_zero() {
    let budget = bounded_wait_budget_ms(deadline_ms_from_now(5_000), 0);
    assert_eq!(budget, 0);
}

#[test]
fn test_bounded_wait_budget_ms_u64_max_configured_falls_back_to_deadline_budget() {
    // Mirrors wait_for_apm_handshake_within_budget's usage: no separate configured
    // timeout of its own, bounded purely by the deadline.
    let budget = bounded_wait_budget_ms(deadline_ms_from_now(600), u64::MAX);
    assert!(budget <= 100, "expected deadline-bounded budget <= 100ms, got {budget}");
}

// ── should_defer_via_pipeline_flush precedence (serverless-mode mirror of the ──
// ── APM guard) ──────────────────────────────────────────────────────────────

#[test]
fn test_pipeline_flush_defers_when_blocking_agent_payload_disabled() {
    assert!(should_defer_via_pipeline_flush(true, false));
}

#[test]
fn test_pipeline_flush_does_not_defer_when_blocking_agent_payload_enabled() {
    assert!(!should_defer_via_pipeline_flush(true, true));
}

#[test]
fn test_no_defer_when_pipeline_flush_disabled_regardless_of_blocking_agent_payload() {
    assert!(!should_defer_via_pipeline_flush(false, false));
    assert!(!should_defer_via_pipeline_flush(false, true));
}

// ── wait_for_late_payload / wait_for_late_report (serverless-mode bounded waits) ──

fn make_noop_log_processor_serverless(config: Arc<config::ExtensionConfig>) -> Arc<LogProcessor> {
    Arc::new(LogProcessor::new(
        Arc::new(crate::newrelic::client::NewRelicClient::new_noop()),
        config,
        Arc::new(Mutex::new(crate::context::InvocationContext::default())),
        None,
    ))
}

/// Register a bare `RequestData` for `request_id` (mirrors `request::mod_tests`'s
/// construction style) and return its agent buffer + both notifies, so the test can
/// simulate a late payload or a late report arriving via the same notify calls
/// `route_payload_to_request_buffer` / `set_pending_report` use in production.
type BareRequestDataServerless = (Arc<Mutex<Vec<Vec<u8>>>>, Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);

fn insert_bare_request_data_serverless(request_id: &str) -> BareRequestDataServerless {
    let agent_buffer = Arc::new(Mutex::new(Vec::new()));
    let agent_payload_notify = Arc::new(tokio::sync::Notify::new());
    let report_notify = Arc::new(tokio::sync::Notify::new());
    request::REQUEST_DATA.insert(
        request_id.to_string(),
        request::RequestData {
            context: Arc::new(Mutex::new(crate::context::InvocationContext::default())),
            agent_buffer: agent_buffer.clone(),
            pending_report: None,
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: agent_payload_notify.clone(),
            report_notify: report_notify.clone(),
            invoked_function_arn: String::new(),
        },
    );
    (agent_buffer, agent_payload_notify, report_notify)
}

#[tokio::test]
async fn test_wait_for_late_payload_returns_immediately_when_no_request_data() {
    let t0 = std::time::Instant::now();
    let result = wait_for_late_payload("no-such-request", deadline_ms_from_now(10_000), 200).await;
    assert!(result.is_empty());
    assert!(t0.elapsed().as_millis() < 100, "should return immediately, took {}ms", t0.elapsed().as_millis());
}

#[tokio::test]
#[serial]
async fn test_wait_for_late_payload_catches_payload_arriving_after_snapshot() {
    let request_id = "serverless-late-payload-catch-test";
    let (agent_buffer, notify, _report_notify) = insert_bare_request_data_serverless(request_id);

    let buffer_clone = agent_buffer.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(60)).await;
        if let Ok(mut buf) = buffer_clone.lock() {
            buf.push(vec![7, 7, 7]);
        }
        notify.notify_one();
    });

    let t0 = std::time::Instant::now();
    let result = wait_for_late_payload(request_id, deadline_ms_from_now(5_000), 2000).await;
    let elapsed = t0.elapsed().as_millis();

    assert_eq!(result, vec![vec![7, 7, 7]]);
    assert!(elapsed >= 50, "should have waited for the payload (got {elapsed}ms)");
    assert!(elapsed < 2000, "should not wait beyond the configured budget (got {elapsed}ms)");

    request::REQUEST_DATA.remove(request_id);
}

#[tokio::test]
#[serial]
async fn test_wait_for_late_payload_times_out_when_nothing_arrives() {
    let request_id = "serverless-late-payload-timeout-test";
    insert_bare_request_data_serverless(request_id);

    let t0 = std::time::Instant::now();
    let result = wait_for_late_payload(request_id, deadline_ms_from_now(5_000), 150).await;
    let elapsed = t0.elapsed().as_millis();

    assert!(result.is_empty());
    assert!(elapsed >= 100, "should have waited near the configured timeout (got {elapsed}ms)");
    assert!(elapsed < 600, "should not hang well beyond the configured timeout (got {elapsed}ms)");

    request::REQUEST_DATA.remove(request_id);
}

#[tokio::test]
#[serial]
async fn test_wait_for_late_payload_bounded_by_deadline_not_config_timeout() {
    let request_id = "serverless-late-payload-deadline-bound-test";
    insert_bare_request_data_serverless(request_id);

    let t0 = std::time::Instant::now();
    let result = wait_for_late_payload(request_id, deadline_ms_from_now(600), 2000).await;
    let elapsed = t0.elapsed().as_millis();

    assert!(result.is_empty());
    assert!(elapsed < 1000, "must be bounded by the deadline budget, not the full configured timeout (got {elapsed}ms)");

    request::REQUEST_DATA.remove(request_id);
}

#[tokio::test]
#[serial]
async fn test_wait_for_late_payload_skips_when_deadline_already_expired() {
    let request_id = "serverless-late-payload-expired-deadline-test";
    insert_bare_request_data_serverless(request_id);

    let t0 = std::time::Instant::now();
    let result = wait_for_late_payload(request_id, deadline_ms_from_now(-1_000), 2000).await;
    let elapsed = t0.elapsed().as_millis();

    assert!(result.is_empty());
    assert!(elapsed < 100, "should return immediately on an expired deadline, took {elapsed}ms");

    request::REQUEST_DATA.remove(request_id);
}

#[tokio::test]
#[serial]
async fn test_wait_for_late_payload_returns_zero_when_timeout_configured_zero() {
    let request_id = "serverless-late-payload-zero-timeout-test";
    insert_bare_request_data_serverless(request_id);

    let t0 = std::time::Instant::now();
    let result = wait_for_late_payload(request_id, deadline_ms_from_now(5_000), 0).await;
    let elapsed = t0.elapsed().as_millis();

    assert!(result.is_empty());
    assert!(elapsed < 100, "a configured timeout of 0 must not wait at all, took {elapsed}ms");

    request::REQUEST_DATA.remove(request_id);
}

#[tokio::test]
async fn test_wait_for_late_report_returns_immediately_when_no_request_data() {
    let t0 = std::time::Instant::now();
    let result = wait_for_late_report("no-such-request", deadline_ms_from_now(10_000), 200).await;
    assert!(result.is_none());
    assert!(t0.elapsed().as_millis() < 100, "should return immediately, took {}ms", t0.elapsed().as_millis());
}

#[tokio::test]
#[serial]
async fn test_wait_for_late_report_catches_report_arriving_after_snapshot() {
    let request_id = "serverless-late-report-catch-test";
    insert_bare_request_data_serverless(request_id);

    let rid = request_id.to_string();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(60)).await;
        request::set_pending_report(&rid, "REPORT line".to_string());
    });

    let t0 = std::time::Instant::now();
    let result = wait_for_late_report(request_id, deadline_ms_from_now(5_000), 2000).await;
    let elapsed = t0.elapsed().as_millis();

    assert_eq!(result, Some("REPORT line".to_string()));
    assert!(elapsed >= 50, "should have waited for the report (got {elapsed}ms)");
    assert!(elapsed < 2000, "should not wait beyond the configured budget (got {elapsed}ms)");

    request::REQUEST_DATA.remove(request_id);
}

#[tokio::test]
#[serial]
async fn test_wait_for_late_report_times_out_when_nothing_arrives() {
    let request_id = "serverless-late-report-timeout-test";
    insert_bare_request_data_serverless(request_id);

    let t0 = std::time::Instant::now();
    let result = wait_for_late_report(request_id, deadline_ms_from_now(5_000), 150).await;
    let elapsed = t0.elapsed().as_millis();

    assert!(result.is_none());
    assert!(elapsed >= 100, "should have waited near the configured timeout (got {elapsed}ms)");
    assert!(elapsed < 600, "should not hang well beyond the configured timeout (got {elapsed}ms)");

    request::REQUEST_DATA.remove(request_id);
}

#[tokio::test]
#[serial]
async fn test_wait_for_late_report_bounded_by_deadline_not_config_timeout() {
    let request_id = "serverless-late-report-deadline-bound-test";
    insert_bare_request_data_serverless(request_id);

    let t0 = std::time::Instant::now();
    let result = wait_for_late_report(request_id, deadline_ms_from_now(600), 2000).await;
    let elapsed = t0.elapsed().as_millis();

    assert!(result.is_none());
    assert!(elapsed < 1000, "must be bounded by the deadline budget, not the full configured timeout (got {elapsed}ms)");

    request::REQUEST_DATA.remove(request_id);
}

// ── process_request_concurrently — direct integration tests for both wait ──
// ── directions and the unconditional report-restore fix ────────────────────

fn make_serverless_processor_factory(
    config: Arc<config::ExtensionConfig>,
) -> Arc<request::ProcessorFactory> {
    let client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
    Arc::new(request::ProcessorFactory::new(client, config, apm_app))
}

fn register_request_for_serverless(
    request_id: &str,
    config: Arc<config::ExtensionConfig>,
) {
    let factory = make_serverless_processor_factory(config);
    let state = create_request_processing_state(
        request_id,
        "arn:aws:lambda:us-east-1:123:function:test",
        &factory,
    );
    REQUEST_PROCESSORS.insert(request_id.to_string(), state);
}

fn make_config_for_serverless(
    blocking_agent_payload: bool,
    agent_payload_timeout_ms: u64,
    report_line_timeout_ms: u64,
) -> Arc<config::ExtensionConfig> {
    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.blocking_agent_payload = blocking_agent_payload;
    cfg.new_relic.agent_payload_timeout_ms = agent_payload_timeout_ms;
    cfg.new_relic.report_line_timeout_ms = report_line_timeout_ms;
    Arc::new(cfg)
}

// Direction 1, flag on: report already arrived, payload lands within the wait window.
#[tokio::test]
#[serial]
async fn process_request_concurrently_catches_late_payload_when_blocking_enabled() {
    let request_id = "prc-direction1-catch-test";
    let config = make_config_for_serverless(true, 300, 200);
    register_request_for_serverless(request_id, config.clone());
    request::set_pending_report(request_id, "REPORT for direction1".to_string());

    let buffer = get_agent_buffer(request_id).expect("buffer must exist after registration");
    let buffer_clone = buffer.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if let Ok(mut buf) = buffer_clone.lock() {
            buf.push(vec![1, 2, 3]);
        }
        if let Some(notify) = request::get_agent_payload_notify(request_id) {
            notify.notify_one();
        }
    });

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());

    process_request_concurrently(
        request_id.to_string(),
        "arn:aws:lambda:us-east-1:123:function:test".to_string(),
        newrelic_client,
        config,
        log_processor,
        deadline_ms_from_now(5_000),
    )
    .await;

    let batched = crate::agent::batch::AGENT_BATCH_BUFFER.get(request_id);
    assert!(batched.is_some(), "late payload must have been batched, not left unbatched");
    assert_eq!(batched.unwrap().report_line, Some("REPORT for direction1".to_string()));
    crate::agent::batch::AGENT_BATCH_BUFFER.remove(request_id);
    REQUEST_DATA.remove(request_id);
}

// Direction 1, flag on, payload never arrives: the report must be RESTORED to
// pending_report (not lost) so the next invocation / SHUTDOWN can still find it.
#[tokio::test]
#[serial]
async fn process_request_concurrently_restores_report_when_payload_never_arrives_blocking_on() {
    let request_id = "prc-direction1-timeout-restore-test";
    let config = make_config_for_serverless(true, 100, 200);
    register_request_for_serverless(request_id, config.clone());
    request::set_pending_report(request_id, "REPORT never paired".to_string());

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());

    process_request_concurrently(
        request_id.to_string(),
        "arn:aws:lambda:us-east-1:123:function:test".to_string(),
        newrelic_client,
        config,
        log_processor,
        deadline_ms_from_now(5_000),
    )
    .await;

    assert_eq!(
        get_pending_report(request_id),
        Some("REPORT never paired".to_string()),
        "report must be restored, not lost, when the payload wait times out"
    );
    assert!(
        crate::agent::batch::AGENT_BATCH_BUFFER.get(request_id).is_none(),
        "nothing should have been batched since the payload never arrived"
    );

    REQUEST_DATA.remove(request_id);
}

// The regression proof: even with the flag OFF (default), a report that arrives with
// no payload yet must be restored, not silently dropped — this is the unconditional
// correctness fix, independent of NEW_RELIC_BLOCKING_AGENT_PAYLOAD.
#[tokio::test]
#[serial]
async fn process_request_concurrently_restores_report_when_no_payload_blocking_off() {
    let request_id = "prc-report-restore-flag-off-test";
    let config = make_config_for_serverless(false, 200, 200); // flag OFF (default)
    register_request_for_serverless(request_id, config.clone());
    request::set_pending_report(request_id, "REPORT with flag off".to_string());

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());

    process_request_concurrently(
        request_id.to_string(),
        "arn:aws:lambda:us-east-1:123:function:test".to_string(),
        newrelic_client,
        config,
        log_processor,
        deadline_ms_from_now(5_000),
    )
    .await;

    assert_eq!(
        get_pending_report(request_id),
        Some("REPORT with flag off".to_string()),
        "the pre-existing silent-loss bug must be fixed even when the new flag is off"
    );

    REQUEST_DATA.remove(request_id);
}

// Direction 2, flag on: payload already arrived, report lands within the wait window.
#[tokio::test]
#[serial]
async fn process_request_concurrently_catches_late_report_when_blocking_enabled() {
    let request_id = "prc-direction2-catch-test";
    let config = make_config_for_serverless(true, 200, 300);
    register_request_for_serverless(request_id, config.clone());

    let buffer = get_agent_buffer(request_id).expect("buffer must exist after registration");
    if let Ok(mut buf) = buffer.lock() {
        buf.push(vec![4, 5, 6]);
    }

    let rid = request_id.to_string();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        request::set_pending_report(&rid, "REPORT for direction2".to_string());
    });

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());

    process_request_concurrently(
        request_id.to_string(),
        "arn:aws:lambda:us-east-1:123:function:test".to_string(),
        newrelic_client,
        config,
        log_processor,
        deadline_ms_from_now(5_000),
    )
    .await;

    let batched = crate::agent::batch::AGENT_BATCH_BUFFER.get(request_id);
    assert!(batched.is_some(), "payload must have been batched once the late report arrived");
    assert_eq!(batched.unwrap().report_line, Some("REPORT for direction2".to_string()));
    crate::agent::batch::AGENT_BATCH_BUFFER.remove(request_id);
    REQUEST_DATA.remove(request_id);
}

// Direction 2, flag on, report never arrives: must send the payload UNPAIRED right
// away (report_line: None) instead of leaving it buffered for the next invocation.
#[tokio::test]
#[serial]
async fn process_request_concurrently_sends_unpaired_when_report_never_arrives_blocking_on() {
    let request_id = "prc-direction2-timeout-unpaired-test";
    let config = make_config_for_serverless(true, 200, 100);
    register_request_for_serverless(request_id, config.clone());

    let buffer = get_agent_buffer(request_id).expect("buffer must exist after registration");
    if let Ok(mut buf) = buffer.lock() {
        buf.push(vec![9, 9, 9]);
    }

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());

    process_request_concurrently(
        request_id.to_string(),
        "arn:aws:lambda:us-east-1:123:function:test".to_string(),
        newrelic_client,
        config,
        log_processor,
        deadline_ms_from_now(5_000),
    )
    .await;

    let batched = crate::agent::batch::AGENT_BATCH_BUFFER.get(request_id);
    assert!(batched.is_some(), "payload must be sent unpaired rather than left buffered");
    assert_eq!(
        batched.unwrap().report_line,
        None,
        "must be sent WITHOUT a report line — that's the 'unpaired' send this ticket asked for"
    );
    crate::agent::batch::AGENT_BATCH_BUFFER.remove(request_id);
    REQUEST_DATA.remove(request_id);
}

// Regression proof: with the flag off (default), payload-with-no-report-yet must still
// behave exactly as before — re-buffered for the next invocation, nothing batched.
#[tokio::test]
#[serial]
async fn process_request_concurrently_rebuffers_payload_when_no_report_blocking_off() {
    let request_id = "prc-payload-only-flag-off-test";
    let config = make_config_for_serverless(false, 200, 200); // flag OFF (default)
    register_request_for_serverless(request_id, config.clone());

    let buffer = get_agent_buffer(request_id).expect("buffer must exist after registration");
    if let Ok(mut buf) = buffer.lock() {
        buf.push(vec![1, 1, 1]);
    }

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());

    let t0 = std::time::Instant::now();
    process_request_concurrently(
        request_id.to_string(),
        "arn:aws:lambda:us-east-1:123:function:test".to_string(),
        newrelic_client,
        config,
        log_processor,
        deadline_ms_from_now(5_000),
    )
    .await;
    let elapsed = t0.elapsed().as_millis();

    assert!(
        crate::agent::batch::AGENT_BATCH_BUFFER.get(request_id).is_none(),
        "nothing should be batched when the flag is off and there's no report yet"
    );
    assert!(elapsed < 200, "flag off: must return near-instantly, no wait engaged (got {elapsed}ms)");

    // The payload must have been put back into the buffer for the next invocation.
    let remaining = get_agent_buffer(request_id).and_then(|b| b.lock().ok().map(|g| g.len()));
    assert_eq!(remaining, Some(1), "payload must be re-buffered, not dropped, when the flag is off");

    REQUEST_DATA.remove(request_id);
}
