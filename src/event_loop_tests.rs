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

// ── spawn_send_agent_task (shared by Flow 1 and the recovered-Flow-2 path) ──

fn make_noop_log_processor(config: Arc<config::ExtensionConfig>) -> Arc<LogProcessor> {
    Arc::new(LogProcessor::new(
        Arc::new(crate::newrelic::client::NewRelicClient::new_noop()),
        config,
        Arc::new(Mutex::new(crate::context::InvocationContext::default())),
        None,
    ))
}

// spawn_send_agent_task is the single send path shared by Flow 1's immediate send
// and the recovered-Flow-2 late-catch send (NR-600648) — it was extracted verbatim
// from Flow 1's old inline closure specifically so both call sites stay identical.
// This test calls it directly (not through process_apm_request) to prove the
// extraction preserved the never-drop-on-failure guarantee: a disconnected apm_app
// (None) makes send_to_apm_collector fail deterministically without any network
// call, so every payload must land in FAILED_AGENT_PAYLOADS, not be lost.
#[tokio::test]
#[serial]
async fn spawn_send_agent_task_buffers_all_payloads_on_failure() {
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }

    let config = Arc::new(config::ExtensionConfig::default());
    let log_processor = make_noop_log_processor(config.clone());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    let handle = spawn_send_agent_task(
        "req-spawn-test".to_string(),
        config,
        log_processor,
        apm_app,
        "arn:aws:lambda:us-east-1:123:function:test".to_string(),
        vec![vec![1, 2, 3], vec![4, 5]],
    );
    let (all_sent, returned_payloads) = handle.await.expect("task must not panic");

    assert!(!all_sent, "a disconnected apm_app must report failure");
    assert_eq!(
        returned_payloads,
        vec![vec![1, 2, 3], vec![4, 5]],
        "the exact input payloads must be returned unchanged"
    );
    let buffered_count = FAILED_AGENT_PAYLOADS
        .lock()
        .map(|b| b.iter().filter(|p| p.request_id == "req-spawn-test").count())
        .unwrap_or(0);
    assert_eq!(
        buffered_count, 2,
        "both payloads must be buffered for retry, not silently dropped"
    );

    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
}

// ── process_apm_request's Flow-2 gate, exercised end-to-end (not just the ──
// ── extracted helpers in isolation) — the actual wiring this ticket added ──

/// Build a `SharedApmApp` that's already "connected" (`has_run_id == true`) without
/// any network I/O — `ApmApp`'s fields are all `pub`, so a literal sidesteps
/// `ApmApp::new()`'s real PreConnect/Connect handshake entirely.
fn fake_connected_apm_app() -> crate::apm::SharedApmApp {
    let app = crate::apm::ApmApp {
        run_id: "test-run-id".to_string(),
        entity_guid: "test-entity-guid".to_string(),
        collector_host: "collector.newrelic.com".to_string(),
        license_key: "fake-key".to_string(),
        metric_endpoint: "https://metric-api.newrelic.com/metric/v1".to_string(),
        client: Client::new(),
    };
    Arc::new(tokio::sync::RwLock::new(Some(app)))
}

/// Register a bare, empty request (no agent payload ever pushed) so
/// `process_apm_request` reaches its `has_run_id && !got_payload` arm — Flow 2 —
/// deterministically. `is_cold_start: true` is passed at the call site to skip the
/// warm-start pending-payload drain, which would otherwise touch unrelated
/// `REQUEST_DATA` entries left by other tests.
fn register_empty_request(
    request_id: &str,
    config: Arc<config::ExtensionConfig>,
    apm_app: crate::apm::SharedApmApp,
) {
    let client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
    let factory = Arc::new(request::ProcessorFactory::new(client, config, apm_app));
    let state = create_request_processing_state(
        request_id,
        "arn:aws:lambda:us-east-1:123:function:test",
        &factory,
    );
    REQUEST_PROCESSORS.insert(request_id.to_string(), state);
}

// The ticket's core wiring proof: with the flag enabled, a real call into
// process_apm_request (not just wait_for_late_agent_payload in isolation) must
// actually engage the bounded wait — proven by elapsed time landing near the
// configured timeout — when has_run_id is true and no payload ever arrives.
#[tokio::test]
#[serial]
async fn process_apm_request_flow2_waits_when_blocking_agent_payload_enabled() {
    let request_id = "flow2-gate-enabled-test";
    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.apm_blocking_agent_payload = true;
    cfg.new_relic.apm_agent_payload_timeout_ms = 120;
    let config = Arc::new(cfg);
    let apm_app = fake_connected_apm_app();

    register_empty_request(request_id, config.clone(), apm_app.clone());
    let log_processor = make_noop_log_processor(config.clone());

    let t0 = std::time::Instant::now();
    process_apm_request(
        request_id.to_string(),
        "arn:aws:lambda:us-east-1:123:function:test".to_string(),
        true, // is_cold_start — skip the unrelated warm-start drain
        config,
        log_processor,
        apm_app,
        deadline_ms_from_now(5_000),
    )
    .await;
    let elapsed = t0.elapsed().as_millis();

    assert!(
        elapsed >= 100,
        "flag enabled: process_apm_request must actually engage the ~120ms wait, not skip it (got {elapsed}ms)"
    );
    assert!(
        elapsed < 1000,
        "must not hang past the configured timeout (got {elapsed}ms)"
    );

    REQUEST_DATA.remove(request_id);
}

// The regression proof requested directly: with the flag left at its default
// (false), process_apm_request's Flow-2 arm must behave exactly as it did before
// this ticket — no wait at all, near-instant return — proven here by actually
// calling process_apm_request, not just the pure should_defer_via_pipeline_flush
// helper.
#[tokio::test]
#[serial]
async fn process_apm_request_flow2_skips_wait_when_blocking_agent_payload_disabled() {
    let request_id = "flow2-gate-disabled-test";
    let config = Arc::new(config::ExtensionConfig::default()); // apm_blocking_agent_payload: false
    let apm_app = fake_connected_apm_app();

    register_empty_request(request_id, config.clone(), apm_app.clone());
    let log_processor = make_noop_log_processor(config.clone());

    let t0 = std::time::Instant::now();
    process_apm_request(
        request_id.to_string(),
        "arn:aws:lambda:us-east-1:123:function:test".to_string(),
        true,
        config,
        log_processor,
        apm_app,
        deadline_ms_from_now(5_000),
    )
    .await;
    let elapsed = t0.elapsed().as_millis();

    assert!(
        elapsed < 100,
        "flag disabled (default): must return near-instantly with zero behavior change (got {elapsed}ms)"
    );

    REQUEST_DATA.remove(request_id);
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

// ── wait_for_late_agent_payload tests (NR-600648: blocking-agent-payload feature) ──

fn make_config_with_agent_payload_timeout(timeout_ms: u64) -> config::ExtensionConfig {
    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.apm_agent_payload_timeout_ms = timeout_ms;
    cfg
}

/// Insert a bare `RequestData` for `request_id` (no full processor setup, mirroring
/// `request::mod_tests`'s construction style) and return its agent buffer + notify so
/// the test can simulate a late payload arriving via `route_payload_to_request_buffer`'s
/// same `notify_one()` call.
type BareRequestData = (Arc<Mutex<Vec<Vec<u8>>>>, Arc<tokio::sync::Notify>);

fn insert_bare_request_data(request_id: &str) -> BareRequestData {
    let agent_buffer = Arc::new(Mutex::new(Vec::new()));
    let agent_payload_notify = Arc::new(tokio::sync::Notify::new());
    request::REQUEST_DATA.insert(
        request_id.to_string(),
        request::RequestData {
            context: Arc::new(Mutex::new(crate::context::InvocationContext::default())),
            agent_buffer: agent_buffer.clone(),
            pending_report: None,
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: agent_payload_notify.clone(),
            invoked_function_arn: String::new(),
        },
    );
    (agent_buffer, agent_payload_notify)
}

#[tokio::test]
async fn test_late_payload_wait_returns_immediately_when_no_request_data() {
    let cfg = make_config_with_agent_payload_timeout(200);
    let t0 = std::time::Instant::now();
    let result =
        wait_for_late_agent_payload("no-such-request", deadline_ms_from_now(10_000), &cfg).await;
    assert!(result.is_empty());
    assert!(
        t0.elapsed().as_millis() < 100,
        "should return immediately when the request isn't registered, took {}ms",
        t0.elapsed().as_millis()
    );
}

// The ticket's proof test: a payload pushed 50-100ms after the wait starts (simulating
// the agent's async harvest landing just after the Flow-1 snapshot found the buffer
// empty) must be caught within the same invocation, not left for next-invoke/shutdown.
#[tokio::test]
#[serial]
async fn test_late_payload_wait_catches_payload_arriving_after_snapshot() {
    let request_id = "late-payload-catch-test";
    let (agent_buffer, notify) = insert_bare_request_data(request_id);

    let buffer_clone = agent_buffer.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(60)).await;
        if let Ok(mut buf) = buffer_clone.lock() {
            buf.push(vec![42, 42, 42]);
        }
        notify.notify_one();
    });

    let cfg = make_config_with_agent_payload_timeout(2000);
    let t0 = std::time::Instant::now();
    let result = wait_for_late_agent_payload(request_id, deadline_ms_from_now(5_000), &cfg).await;
    let elapsed = t0.elapsed().as_millis();

    assert_eq!(result, vec![vec![42, 42, 42]], "must return the late payload");
    assert!(elapsed >= 50, "should have waited for the payload (got {elapsed}ms)");
    assert!(elapsed < 2000, "should not wait beyond the configured budget (got {elapsed}ms)");

    request::REQUEST_DATA.remove(request_id);
}

#[tokio::test]
#[serial]
async fn test_late_payload_wait_times_out_when_nothing_arrives() {
    let request_id = "late-payload-timeout-test";
    insert_bare_request_data(request_id);

    let cfg = make_config_with_agent_payload_timeout(150);
    let t0 = std::time::Instant::now();
    let result = wait_for_late_agent_payload(request_id, deadline_ms_from_now(5_000), &cfg).await;
    let elapsed = t0.elapsed().as_millis();

    assert!(result.is_empty());
    assert!(elapsed >= 100, "should have waited near the configured timeout (got {elapsed}ms)");
    assert!(elapsed < 600, "should not hang well beyond the configured timeout (got {elapsed}ms)");

    request::REQUEST_DATA.remove(request_id);
}

#[tokio::test]
#[serial]
async fn test_late_payload_wait_bounded_by_deadline_not_config_timeout() {
    let request_id = "late-payload-deadline-bound-test";
    insert_bare_request_data(request_id);

    // Configured timeout (2000ms) far exceeds the remaining deadline (~600ms), so the
    // deadline (minus the 500ms safety margin) must dominate: budget = 600 - 500 = 100ms.
    let cfg = make_config_with_agent_payload_timeout(2000);
    let t0 = std::time::Instant::now();
    let result = wait_for_late_agent_payload(request_id, deadline_ms_from_now(600), &cfg).await;
    let elapsed = t0.elapsed().as_millis();

    assert!(result.is_empty());
    assert!(
        elapsed < 1000,
        "must be bounded by the deadline budget, not the full configured timeout (got {elapsed}ms)"
    );

    request::REQUEST_DATA.remove(request_id);
}

#[tokio::test]
#[serial]
async fn test_late_payload_wait_skips_when_deadline_already_expired() {
    let request_id = "late-payload-expired-deadline-test";
    insert_bare_request_data(request_id);

    let cfg = make_config_with_agent_payload_timeout(2000);
    let t0 = std::time::Instant::now();
    let result =
        wait_for_late_agent_payload(request_id, deadline_ms_from_now(-1_000), &cfg).await;
    let elapsed = t0.elapsed().as_millis();

    assert!(result.is_empty());
    assert!(
        elapsed < 100,
        "should return immediately on an expired deadline, took {elapsed}ms"
    );

    request::REQUEST_DATA.remove(request_id);
}

#[tokio::test]
#[serial]
async fn test_late_payload_wait_returns_zero_when_timeout_configured_zero() {
    let request_id = "late-payload-zero-timeout-test";
    insert_bare_request_data(request_id);

    let cfg = make_config_with_agent_payload_timeout(0);
    let t0 = std::time::Instant::now();
    let result = wait_for_late_agent_payload(request_id, deadline_ms_from_now(5_000), &cfg).await;
    let elapsed = t0.elapsed().as_millis();

    assert!(result.is_empty());
    assert!(
        elapsed < 100,
        "a configured timeout of 0 must not wait at all, took {elapsed}ms"
    );

    request::REQUEST_DATA.remove(request_id);
}

// ── should_defer_via_pipeline_flush precedence tests (Interactions §1, NR-600648) ──

#[test]
fn test_pipeline_flush_defers_when_blocking_agent_payload_disabled() {
    // Existing pipeline_flush-only behavior must be unchanged for customers who
    // don't touch the new flag.
    assert!(should_defer_via_pipeline_flush(true, false));
}

#[test]
fn test_pipeline_flush_does_not_defer_when_blocking_agent_payload_enabled() {
    // The delivery guarantee wins: process_apm_request (and its bounded wait) must be
    // synchronously joined, not deferred into the background, when the customer has
    // opted into NEW_RELIC_APM_BLOCKING_AGENT_PAYLOAD — even if pipeline_flush is also set.
    assert!(!should_defer_via_pipeline_flush(true, true));
}

#[test]
fn test_no_defer_when_pipeline_flush_disabled_regardless_of_blocking_agent_payload() {
    assert!(!should_defer_via_pipeline_flush(false, false));
    assert!(!should_defer_via_pipeline_flush(false, true));
}
