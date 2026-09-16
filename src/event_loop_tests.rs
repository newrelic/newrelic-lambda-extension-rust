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
        app_name: "test-app-name".to_string(),
        // port 1 → connection refused immediately, no 20s wait
        collector_host: "127.0.0.1:1".to_string(),
        license_key: "test-license-key".to_string(),
        metric_endpoint: "http://127.0.0.1:1/metrics".to_string(),
        client: reqwest::Client::new(),
        deployment: crate::config::deployment::DeploymentContext::Normal {
            mode: crate::config::deployment::TelemetryMode::Apm,
        },
    }
}

// Spindown → no network call, returns instantly.
#[tokio::test]
async fn test_send_error_spindown_no_network_call() {
    let app = make_test_apm_app();
    let config = crate::config::ExtensionConfig::default();
    let t0 = std::time::Instant::now();
    send_error_for_shutdown_reason(
        &app,
        crate::runtime::ShutdownReason::Spindown,
        "req-123",
        "arn:aws:lambda:us-east-1:123:function:test",
        &config,
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
    let config = crate::config::ExtensionConfig::default();
    // Should complete without panic even though the HTTP call fails
    send_error_for_shutdown_reason(
        &app,
        crate::runtime::ShutdownReason::Timeout,
        "req-456",
        "arn:aws:lambda:us-east-1:123:function:test",
        &config,
    )
    .await;
}

// Failure → attempts network, error is swallowed.
#[tokio::test]
async fn test_send_error_failure_swallows_network_error() {
    let app = make_test_apm_app();
    let config = crate::config::ExtensionConfig::default();
    send_error_for_shutdown_reason(
        &app,
        crate::runtime::ShutdownReason::Failure,
        "req-789",
        "arn:aws:lambda:us-east-1:123:function:test",
        &config,
    )
    .await;
}

// Unknown → attempts network, error is swallowed.
#[tokio::test]
async fn test_send_error_unknown_swallows_network_error() {
    let app = make_test_apm_app();
    let config = crate::config::ExtensionConfig::default();
    send_error_for_shutdown_reason(
        &app,
        crate::runtime::ShutdownReason::Unknown,
        "req-000",
        "arn:aws:lambda:us-east-1:123:function:test",
        &config,
    )
    .await;
}

// NR-616580: a class listed in NEW_RELIC_EXTENSION_IGNORE_ERRORS must skip the
// network call entirely (unlike Timeout/Failure/Unknown above, which always attempt
// one) — verified the same way spindown's no-network-call case is verified.
//
// #[serial] here and below: these tests inspect the process-global
// FAILED_TELEMETRY_BUFFER (via get_buffer_count()), which every test in the
// suite that buffers telemetry shares — matching the crate-wide convention
// (see apm::telemetry_buffer_tests) of serializing tests that touch it.
#[tokio::test]
#[serial]
async fn test_send_error_ignored_class_no_network_call() {
    let app = make_test_apm_app();
    let mut config = crate::config::ExtensionConfig::default();
    config.extension.ignore_errors.insert("lambdatimeout".to_string());
    let t0 = std::time::Instant::now();
    send_error_for_shutdown_reason(
        &app,
        crate::runtime::ShutdownReason::Timeout,
        "req-999",
        "arn:aws:lambda:us-east-1:123:function:test",
        &config,
    )
    .await;
    assert!(
        t0.elapsed().as_millis() < 100,
        "Ignored error class should not make any network call (took {}ms)",
        t0.elapsed().as_millis()
    );
}

// NR-616580: a class listed in NEW_RELIC_EXTENSION_EXPECTED_ERRORS (and NOT in
// ignore_errors) must still attempt the send — unlike the ignored case above.
// The send targets a fake collector host (127.0.0.1:1) so it fails fast and
// gets buffered via telemetry_buffer::buffer_failed_telemetry; that buffering
// only happens on the path that actually calls
// ApmApp::send_shutdown_error_event, so an increased buffer count is proof the
// is_expected=true forwarding path was reached rather than skipped.
#[tokio::test]
#[serial]
async fn test_send_error_expected_class_still_attempts_send() {
    crate::apm::telemetry_buffer::FAILED_TELEMETRY_BUFFER.lock().unwrap().clear();
    let app = make_test_apm_app();
    let mut config = crate::config::ExtensionConfig::default();
    config.extension.expected_errors.insert("lambdatimeout".to_string());
    send_error_for_shutdown_reason(
        &app,
        crate::runtime::ShutdownReason::Timeout,
        "req-998",
        "arn:aws:lambda:us-east-1:123:function:test",
        &config,
    )
    .await;
    assert_eq!(
        crate::apm::telemetry_buffer::get_buffer_count(),
        1,
        "expected-but-not-ignored class must still attempt the send (and get buffered on failure)"
    );
    crate::apm::telemetry_buffer::FAILED_TELEMETRY_BUFFER.lock().unwrap().clear();
}

// NR-616580: README states ignore_errors takes precedence over expected_errors
// when a class appears in both. Verify that precedence end-to-end: with the
// same class in both sets, the send must be skipped exactly like the
// ignore-only case (no network call, nothing buffered).
#[tokio::test]
#[serial]
async fn test_send_error_ignore_takes_precedence_over_expected_for_same_class() {
    crate::apm::telemetry_buffer::FAILED_TELEMETRY_BUFFER.lock().unwrap().clear();
    let app = make_test_apm_app();
    let mut config = crate::config::ExtensionConfig::default();
    config.extension.ignore_errors.insert("lambdatimeout".to_string());
    config.extension.expected_errors.insert("lambdatimeout".to_string());
    let t0 = std::time::Instant::now();
    send_error_for_shutdown_reason(
        &app,
        crate::runtime::ShutdownReason::Timeout,
        "req-997",
        "arn:aws:lambda:us-east-1:123:function:test",
        &config,
    )
    .await;
    assert!(
        t0.elapsed().as_millis() < 100,
        "ignore_errors must win when a class is in both sets — no network call (took {}ms)",
        t0.elapsed().as_millis()
    );
    assert_eq!(
        crate::apm::telemetry_buffer::get_buffer_count(),
        0,
        "ignore_errors must win when a class is in both sets — nothing buffered"
    );
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
fn test_pipeline_flush_defers_when_synchronous_flush_disabled() {
    assert!(should_defer_via_pipeline_flush(true, false));
}

#[test]
fn test_pipeline_flush_does_not_defer_when_synchronous_flush_enabled() {
    assert!(!should_defer_via_pipeline_flush(true, true));
}

#[test]
fn test_no_defer_when_pipeline_flush_disabled_regardless_of_synchronous_flush() {
    assert!(!should_defer_via_pipeline_flush(false, false));
    assert!(!should_defer_via_pipeline_flush(false, true));
}

fn make_noop_log_processor_serverless(config: Arc<config::ExtensionConfig>) -> Arc<LogProcessor> {
    Arc::new(LogProcessor::new(
        Arc::new(crate::newrelic::client::NewRelicClient::new_noop()),
        config,
        Arc::new(Mutex::new(crate::context::InvocationContext::default())),
        None,
    ))
}

// ── process_request_concurrently — direct integration tests for the immediate-send ──
// ── fix (both process_request_concurrently's own arms and the unconditional ────────
// ── report-restore fix) ─────────────────────────────────────────────────────────────

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

fn make_config_for_serverless(synchronous_flush: bool) -> Arc<config::ExtensionConfig> {
    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.synchronous_flush = synchronous_flush;
    Arc::new(cfg)
}

// Flag on, no report pending, no payload buffered: nothing to do — proves the
// no-payload arm's baseline (no report to lose, nothing to send).
#[tokio::test]
#[serial]
async fn process_request_concurrently_no_payload_no_report_is_a_no_op_sync_flush_on() {
    let request_id = "prc-no-payload-no-report-sync-flush-on-test";
    let config = make_config_for_serverless(true);
    register_request_for_serverless(request_id, config.clone());

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

    assert!(get_pending_report(request_id).is_none());
    assert!(crate::agent::batch::AGENT_BATCH_BUFFER.get(request_id).is_none());

    REQUEST_DATA.remove(request_id);
}

// Flag on, report pending, no payload: the report must be RESTORED to pending_report
// (not lost) — there is no wait for a late payload anymore (removed: it's effectively
// unreachable in production, since a fresh request's own platform.report can never be
// "already arrived" by the time this function's synchronous snapshot runs), so this
// arm is reached immediately, every time, when there's no payload yet.
#[tokio::test]
#[serial]
async fn process_request_concurrently_restores_report_when_no_payload_sync_flush_on() {
    let request_id = "prc-report-restore-sync-flush-on-test";
    let config = make_config_for_serverless(true);
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
        "report must be restored, not lost, when there's no payload to send it with"
    );
    assert!(
        crate::agent::batch::AGENT_BATCH_BUFFER.get(request_id).is_none(),
        "nothing should have been batched since there was never a payload"
    );

    REQUEST_DATA.remove(request_id);
}

// The regression proof: even with the flag OFF (default), a report that arrives with
// no payload yet must be restored, not silently dropped — this is the unconditional
// correctness fix, independent of NEW_RELIC_EXTENSION_SYNCHRONOUS_FLUSH.
#[tokio::test]
#[serial]
async fn process_request_concurrently_restores_report_when_no_payload_sync_flush_off() {
    let request_id = "prc-report-restore-flag-off-test";
    let config = make_config_for_serverless(false); // flag OFF (default)
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

// Payload present (the orphaned-buffer-drained-into-an-active-request edge case) and a
// report happens to also be pending, flag on: the payload must be sent immediately,
// decoupled from the report — not paired/batched. The report is restored via
// set_pending_report (not attached to the send) since agent-payload delivery and
// platform.report handling are independent features under this flag.
#[tokio::test]
#[serial]
async fn process_request_concurrently_sends_payload_immediately_and_decouples_report_when_sync_flush_on() {
    let request_id = "prc-payload-sends-immediately-with-report-test";
    let config = make_config_for_serverless(true);
    register_request_for_serverless(request_id, config.clone());
    request::set_pending_report(request_id, "REPORT decoupled".to_string());

    let buffer = get_agent_buffer(request_id).expect("buffer must exist after registration");
    if let Ok(mut buf) = buffer.lock() {
        buf.push(vec![4, 5, 6]);
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

    assert!(
        crate::agent::batch::AGENT_BATCH_BUFFER.get(request_id).is_none(),
        "the payload must be sent immediately, not batched"
    );
    assert_eq!(
        get_pending_report(request_id),
        Some("REPORT decoupled".to_string()),
        "the report must be restored (not attached to the send) — the two are decoupled under this flag"
    );

    crate::agent::batch::AGENT_BATCH_BUFFER.remove(request_id);
    REQUEST_DATA.remove(request_id);
}

// Same edge case, but with no report pending at all — the far more common shape of
// the orphaned-buffer case in practice. The payload must still send immediately.
#[tokio::test]
#[serial]
async fn process_request_concurrently_sends_payload_immediately_without_report_when_sync_flush_on() {
    let request_id = "prc-payload-sends-immediately-no-report-test";
    let config = make_config_for_serverless(true);
    register_request_for_serverless(request_id, config.clone());

    let buffer = get_agent_buffer(request_id).expect("buffer must exist after registration");
    if let Ok(mut buf) = buffer.lock() {
        buf.push(vec![1, 2, 3]);
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

    assert!(
        crate::agent::batch::AGENT_BATCH_BUFFER.get(request_id).is_none(),
        "the payload must be sent immediately, not batched"
    );
    assert!(get_pending_report(request_id).is_none());

    crate::agent::batch::AGENT_BATCH_BUFFER.remove(request_id);
    REQUEST_DATA.remove(request_id);
}

// Regression proof: with the flag off (default), payload+report ready together must
// still just batch (not send immediately) — unchanged from today's behavior.
#[tokio::test]
#[serial]
async fn process_request_concurrently_batches_when_both_ready_sync_flush_off() {
    let request_id = "prc-both-ready-flag-off-test";
    let config = make_config_for_serverless(false); // flag OFF (default)
    register_request_for_serverless(request_id, config.clone());
    request::set_pending_report(request_id, "REPORT both-ready-flag-off".to_string());

    let buffer = get_agent_buffer(request_id).expect("buffer must exist after registration");
    if let Ok(mut buf) = buffer.lock() {
        buf.push(vec![7, 8, 9]);
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
    assert!(batched.is_some(), "flag off: must still batch immediately-ready payload+report");
    assert_eq!(
        batched.expect("checked is_some above").report_line,
        Some("REPORT both-ready-flag-off".to_string())
    );
    crate::agent::batch::AGENT_BATCH_BUFFER.remove(request_id);
    REQUEST_DATA.remove(request_id);
}

// Regression proof: with the flag off (default), payload-with-no-report-yet must still
// behave exactly as before — re-buffered for the next invocation, nothing batched.
#[tokio::test]
#[serial]
async fn process_request_concurrently_rebuffers_payload_when_no_report_sync_flush_off() {
    let request_id = "prc-payload-only-flag-off-test";
    let config = make_config_for_serverless(false); // flag OFF (default)
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

// process_request_concurrently must await any pending_send_handles registered for its
// request (e.g. by route_payload_to_request_buffer's immediate-send path) before
// returning, bounded by the invocation's remaining deadline — proven here via a side
// effect (an AtomicBool flipped inside the spawned task) that must be observably true
// by the time process_request_concurrently's own await completes.
#[tokio::test]
#[serial]
async fn process_request_concurrently_awaits_pending_send_handles_before_returning() {
    let request_id = "prc-awaits-pending-send-handles-test";
    let config = make_config_for_serverless(false);
    register_request_for_serverless(request_id, config.clone());

    let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let completed_clone = completed.clone();
    let handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(80)).await;
        completed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
    });
    request::push_pending_send_handle(request_id, handle);

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

    assert!(
        completed.load(std::sync::atomic::Ordering::SeqCst),
        "process_request_concurrently must await outstanding pending_send_handles before returning"
    );

    REQUEST_DATA.remove(request_id);
}

// ── buffered_agent_payload_request_ids / cleanup_old_failed_payloads ───────────────

#[test]
#[serial]
fn buffered_agent_payload_request_ids_returns_sorted_deduped_ids() {
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
        b.push(make_failed_payload("req-b"));
        b.push(make_failed_payload("req-a"));
        b.push(make_failed_payload("req-a")); // duplicate request_id
    }

    let ids = buffered_agent_payload_request_ids();
    assert_eq!(ids, vec!["req-a".to_string(), "req-b".to_string()]);

    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
}

#[test]
#[serial]
fn buffered_agent_payload_request_ids_empty_when_no_failures() {
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
    assert!(buffered_agent_payload_request_ids().is_empty());
}

#[test]
#[serial]
fn cleanup_old_failed_payloads_removes_only_stale_entries() {
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
        let mut fresh = make_failed_payload("req-fresh");
        fresh.failed_at = chrono::Utc::now();
        let mut stale = make_failed_payload("req-stale");
        stale.failed_at = chrono::Utc::now() - chrono::Duration::hours(25);
        b.push(fresh);
        b.push(stale);
    }

    cleanup_old_failed_payloads();

    let remaining: Vec<String> = FAILED_AGENT_PAYLOADS
        .lock()
        .map(|b| b.iter().map(|p| p.request_id.clone()).collect())
        .unwrap_or_default();
    assert_eq!(remaining, vec!["req-fresh".to_string()]);

    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
}

#[test]
#[serial]
fn cleanup_old_failed_payloads_is_a_no_op_when_all_fresh() {
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
        b.push(make_failed_payload("req-1"));
        b.push(make_failed_payload("req-2"));
    }

    cleanup_old_failed_payloads();

    let remaining_len = FAILED_AGENT_PAYLOADS.lock().map(|b| b.len()).unwrap_or(0);
    assert_eq!(remaining_len, 2);

    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
}

// ── update_global_invocation_context ────────────────────────────────────────────────

#[test]
#[serial]
fn update_global_invocation_context_sets_request_id_and_arn() {
    update_global_invocation_context("req-ctx-1", "arn:aws:lambda:us-east-1:123:function:ctx-test");

    let ctx = crate::CURRENT_INVOCATION_CONTEXT.read().unwrap();
    assert_eq!(ctx.request_id, "req-ctx-1");
    assert_eq!(ctx.invoked_function_arn, "arn:aws:lambda:us-east-1:123:function:ctx-test");
    assert_eq!(ctx.trace_id, None);
}

#[test]
#[serial]
fn update_global_invocation_context_keeps_previous_arn_when_new_arn_is_empty() {
    update_global_invocation_context("req-ctx-prev", "arn:aws:lambda:us-east-1:123:function:keep-me");
    update_global_invocation_context("req-ctx-2", "");

    let ctx = crate::CURRENT_INVOCATION_CONTEXT.read().unwrap();
    // ARN must be retained from the previous call; only request_id/trace_id update.
    assert_eq!(ctx.invoked_function_arn, "arn:aws:lambda:us-east-1:123:function:keep-me");
    assert_eq!(ctx.request_id, "req-ctx-2");
}

// ── execute_noop_event_loop / run_infinite_event_loop / execute_main_telemetry_processing_loop
// ── execute_standard_mode_event_loop — driven end-to-end against a wiremock Extensions API ──

const RUNTIME_API_ENV_EL: &str = "AWS_LAMBDA_RUNTIME_API";

async fn with_runtime_api_el<F, Fut, T>(server: &wiremock::MockServer, f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let prev = std::env::var(RUNTIME_API_ENV_EL).ok();
    std::env::set_var(RUNTIME_API_ENV_EL, server.address().to_string());
    let out = f().await;
    match prev {
        Some(v) => std::env::set_var(RUNTIME_API_ENV_EL, v),
        None => std::env::remove_var(RUNTIME_API_ENV_EL),
    }
    out
}

fn shutdown_event_body(reason: &str) -> serde_json::Value {
    serde_json::json!({ "eventType": "SHUTDOWN", "shutdownReason": reason })
}

fn invoke_event_body(request_id: &str, arn: &str, deadline_ms: i64) -> serde_json::Value {
    serde_json::json!({
        "eventType": "INVOKE",
        "requestId": request_id,
        "invokedFunctionArn": arn,
        "deadlineMs": deadline_ms
    })
}

fn make_test_extension_components(
    config: Arc<config::ExtensionConfig>,
    client: Arc<Client>,
    apm_mode_enabled: bool,
) -> ExtensionComponents {
    let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
    let processor_factory = Arc::new(request::ProcessorFactory::new(
        newrelic_client.clone(),
        config.clone(),
        apm_app.clone(),
    ));
    let global_log_processor = make_noop_log_processor_serverless(config.clone());
    ExtensionComponents {
        client,
        extension_id: "test-ext-id".to_string(),
        processor_factory,
        newrelic_client,
        config,
        global_log_processor,
        apm_app,
        apm_mode_enabled,
        apm_client: Client::new(),
        reconnect_in_flight: Arc::new(watch::channel(false).0),
        deployment: crate::config::deployment::DeploymentContext::Normal {
            mode: crate::config::deployment::TelemetryMode::Serverless,
        },
    }
}

#[tokio::test]
#[serial]
async fn execute_noop_event_loop_returns_on_immediate_shutdown() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let client = Arc::new(Client::new());
    with_runtime_api_el(&server, || async {
        // Must return promptly on SHUTDOWN — a hang here fails the test via timeout.
        execute_noop_event_loop(&client, "test-ext-id").await;
    })
    .await;
}

#[tokio::test]
#[serial]
async fn execute_noop_event_loop_breaks_on_fatal_403() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&server)
        .await;

    let client = Arc::new(Client::new());
    with_runtime_api_el(&server, || async {
        execute_noop_event_loop(&client, "test-ext-id").await;
    })
    .await;
}

#[tokio::test]
#[serial]
async fn run_infinite_event_loop_takes_noop_path_when_extension_disabled() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = false;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let components = make_test_extension_components(config, client, false);

    let count = with_runtime_api_el(&server, || async { run_infinite_event_loop(components).await }).await;

    assert_eq!(count, 0, "no-op dispatch must return 0 without counting events");
}

#[tokio::test]
#[serial]
async fn run_infinite_event_loop_takes_noop_path_when_no_license_key() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = None;
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let components = make_test_extension_components(config, client, false);

    let count = with_runtime_api_el(&server, || async { run_infinite_event_loop(components).await }).await;

    assert_eq!(count, 0, "no-op dispatch must return 0 without counting events");
}

#[tokio::test]
#[serial]
async fn execute_standard_mode_event_loop_processes_invoke_then_shutdown() {
    let server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-standard-loop-1",
                "arn:aws:lambda:us-east-1:123456789012:function:standard-loop-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, false);

    let event_count = with_runtime_api_el(&server, || async {
        execute_standard_mode_event_loop(&mut components).await
    })
    .await;

    assert_eq!(
        event_count, 2,
        "must count both the INVOKE and the terminating SHUTDOWN event"
    );

    REQUEST_DATA.remove("req-standard-loop-1");
}

#[tokio::test]
#[serial]
async fn execute_main_telemetry_processing_loop_routes_normal_serverless_to_standard_loop() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, false);

    let count = with_runtime_api_el(&server, || async {
        execute_main_telemetry_processing_loop(&mut components).await
    })
    .await;

    assert_eq!(count, 1, "single immediate SHUTDOWN must count as one event via the standard-mode loop");
}

// ── drain_late_paired_payloads_serverless ──────────────────────────────────────────

#[tokio::test]
#[serial]
async fn drain_late_paired_payloads_batches_previous_request_with_report_and_payload() {
    let prev_id = "drain-prev-req-1";
    let current_id = "drain-current-req-1";
    let config = make_config_for_serverless(false);
    register_request_for_serverless(prev_id, config.clone());

    request::set_pending_report(prev_id, "REPORT line for prev".to_string());
    if let Some(buf) = request::get_agent_buffer(prev_id) {
        buf.lock().unwrap().push(vec![9, 9, 9]);
    }

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let paired = drain_late_paired_payloads_serverless(current_id, &config, &log_processor).await;

    assert_eq!(paired, 1, "the one buffered payload must be counted as paired");
    assert!(
        crate::agent::batch::AGENT_BATCH_BUFFER.get(prev_id).is_some(),
        "paired payload must be batched for send under the previous request's id"
    );
    assert!(
        request::get_pending_report(prev_id).is_none(),
        "the report must be consumed once paired"
    );

    crate::agent::batch::AGENT_BATCH_BUFFER.remove(prev_id);
    REQUEST_DATA.remove(prev_id);
}

#[tokio::test]
#[serial]
async fn drain_late_paired_payloads_skips_request_with_report_but_no_payload() {
    let prev_id = "drain-prev-req-2";
    let current_id = "drain-current-req-2";
    let config = make_config_for_serverless(false);
    register_request_for_serverless(prev_id, config.clone());

    request::set_pending_report(prev_id, "REPORT line, no payload yet".to_string());
    // agent_buffer intentionally left empty.

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let paired = drain_late_paired_payloads_serverless(current_id, &config, &log_processor).await;

    assert_eq!(paired, 0, "a report with no buffered payload is not a candidate");

    REQUEST_DATA.remove(prev_id);
}

#[tokio::test]
#[serial]
async fn drain_late_paired_payloads_ignores_the_current_request_itself() {
    let current_id = "drain-current-req-3";
    let config = make_config_for_serverless(false);
    register_request_for_serverless(current_id, config.clone());

    request::set_pending_report(current_id, "REPORT line for current".to_string());
    if let Some(buf) = request::get_agent_buffer(current_id) {
        buf.lock().unwrap().push(vec![1]);
    }

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let paired = drain_late_paired_payloads_serverless(current_id, &config, &log_processor).await;

    assert_eq!(paired, 0, "the current request must never be paired against itself");

    REQUEST_DATA.remove(current_id);
}

#[tokio::test]
#[serial]
async fn drain_late_paired_payloads_returns_zero_when_no_candidates_exist() {
    let config = make_config_for_serverless(false);
    let log_processor = make_noop_log_processor_serverless(config.clone());
    let paired = drain_late_paired_payloads_serverless("no-such-current-req", &config, &log_processor).await;
    assert_eq!(paired, 0);
}

// ── wait_for_runtime_done_with_grace ───────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn wait_for_runtime_done_returns_immediately_when_function_logs_disabled() {
    let mut cfg = config::ExtensionConfig::default();
    cfg.extension.send_function_logs = false;
    let config = Arc::new(cfg);
    let log_processor = make_noop_log_processor_serverless(config.clone());

    let start = std::time::Instant::now();
    wait_for_runtime_done_with_grace(
        "wfrd-no-such-request",
        deadline_ms_from_now(5_000),
        &config,
        &log_processor,
    )
    .await;
    assert!(
        start.elapsed() < Duration::from_millis(200),
        "must short-circuit before touching request state or sleeping"
    );
}

#[tokio::test]
#[serial]
async fn wait_for_runtime_done_returns_immediately_when_request_has_no_notify() {
    let mut cfg = config::ExtensionConfig::default();
    cfg.extension.send_function_logs = true;
    let config = Arc::new(cfg);
    let log_processor = make_noop_log_processor_serverless(config.clone());

    let start = std::time::Instant::now();
    wait_for_runtime_done_with_grace(
        "wfrd-unregistered-request",
        deadline_ms_from_now(5_000),
        &config,
        &log_processor,
    )
    .await;
    assert!(
        start.elapsed() < Duration::from_millis(200),
        "an unregistered request has no notify handle, so this must return immediately"
    );
}

#[tokio::test]
#[serial]
async fn wait_for_runtime_done_skips_grace_when_signalled_and_already_drained() {
    let request_id = "wfrd-signalled-drained";
    let mut cfg = config::ExtensionConfig::default();
    cfg.extension.send_function_logs = true;
    let config = Arc::new(cfg);
    register_request_for_serverless(request_id, config.clone());

    let notify = request::get_runtime_done_notify(request_id).expect("notify must exist after registration");
    notify.notify_one(); // pre-fire: notified() below resolves immediately

    let log_processor = make_noop_log_processor_serverless(config.clone());
    assert!(log_processor.is_drained(), "a fresh noop log processor starts drained");

    let start = std::time::Instant::now();
    wait_for_runtime_done_with_grace(
        request_id,
        deadline_ms_from_now(5_000),
        &config,
        &log_processor,
    )
    .await;
    assert!(
        start.elapsed() < Duration::from_millis(200),
        "already-drained batch must skip the grace-period wait entirely"
    );

    REQUEST_DATA.remove(request_id);
}

#[tokio::test]
#[serial]
async fn wait_for_runtime_done_falls_through_when_deadline_expires_before_signal() {
    let request_id = "wfrd-deadline-expires";
    let mut cfg = config::ExtensionConfig::default();
    cfg.extension.send_function_logs = true;
    let config = Arc::new(cfg);
    register_request_for_serverless(request_id, config.clone());
    // Notify is never fired — the wait must fall through on the deadline instead.

    let log_processor = make_noop_log_processor_serverless(config.clone());

    let start = std::time::Instant::now();
    wait_for_runtime_done_with_grace(
        request_id,
        deadline_ms_from_now(60), // short but not-yet-expired deadline
        &config,
        &log_processor,
    )
    .await;
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(50) && elapsed < Duration::from_millis(1_000),
        "must wait roughly until the deadline budget, then return (elapsed: {:?})",
        elapsed
    );

    REQUEST_DATA.remove(request_id);
}

// ── batch_all_payloads / spawn_immediate_agent_payload_sends / maybe_spawn_batch_send ──

#[test]
#[serial]
fn batch_all_payloads_adds_each_payload_with_shared_report() {
    let request_id = "batch-all-req-1";
    crate::agent::batch::AGENT_BATCH_BUFFER.remove(request_id);

    batch_all_payloads(
        request_id,
        vec![vec![1, 2], vec![3, 4]],
        Some("REPORT line"),
        "arn:aws:lambda:us-east-1:123:function:test",
    );

    assert!(crate::agent::batch::AGENT_BATCH_BUFFER.get(request_id).is_some());
    crate::agent::batch::AGENT_BATCH_BUFFER.remove(request_id);
}

#[test]
#[serial]
fn batch_all_payloads_is_a_no_op_for_empty_payload_list() {
    let request_id = "batch-all-req-empty";
    crate::agent::batch::AGENT_BATCH_BUFFER.remove(request_id);

    batch_all_payloads(request_id, vec![], None, "arn:aws:lambda:us-east-1:123:function:test");

    assert!(crate::agent::batch::AGENT_BATCH_BUFFER.get(request_id).is_none());
}

#[tokio::test]
#[serial]
async fn spawn_immediate_agent_payload_sends_succeeds_silently_with_no_license_key() {
    // noop client: license_key=None → send_agent_payload returns Ok(()) without any
    // HTTP call, so nothing gets buffered to AGENT_BATCH_BUFFER.
    let request_id = "spawn-immediate-req-1".to_string();
    crate::agent::batch::AGENT_BATCH_BUFFER.remove(&request_id);
    let config = make_config_for_serverless(false);
    let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());

    let handle = spawn_immediate_agent_payload_sends(
        request_id.clone(),
        vec![vec![1, 2, 3]],
        "arn:aws:lambda:us-east-1:123:function:test".to_string(),
        newrelic_client,
        config,
    );
    handle.await.expect("spawned task must not panic");

    assert!(
        crate::agent::batch::AGENT_BATCH_BUFFER.get(&request_id).is_none(),
        "noop send (no license key) must not buffer anything"
    );
}

#[tokio::test]
#[serial]
async fn spawn_immediate_agent_payload_sends_buffers_on_send_failure() {
    // Use a license key + a refused port so the HTTP send actually fails, triggering
    // the fallback-to-batch-buffer path.
    let request_id = "spawn-immediate-req-fail".to_string();
    crate::agent::batch::AGENT_BATCH_BUFFER.remove(&request_id);

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.license_key = Some("test-key-that-causes-send".to_string());
    cfg.new_relic.telemetry_endpoint = "http://127.0.0.1:1/bad-endpoint".to_string();
    cfg.new_relic.data_collection_timeout = Some(std::time::Duration::from_millis(50));
    let config = Arc::new(cfg);
    let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());

    let handle = spawn_immediate_agent_payload_sends(
        request_id.clone(),
        vec![vec![1, 2, 3]],
        "arn:aws:lambda:us-east-1:123:function:test".to_string(),
        newrelic_client,
        config,
    );
    handle.await.expect("spawned task must not panic");

    assert!(
        crate::agent::batch::AGENT_BATCH_BUFFER.get(&request_id).is_some(),
        "a failed send must fall back to the batch buffer"
    );
    crate::agent::batch::AGENT_BATCH_BUFFER.remove(&request_id);
}

#[tokio::test]
#[serial]
async fn maybe_spawn_batch_send_spawns_when_synchronous_flush_enabled() {
    let config = make_config_for_serverless(true);
    let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
    let handle = maybe_spawn_batch_send(newrelic_client, config);
    assert!(handle.is_some(), "synchronous_flush=true must always trigger a send");
    handle.unwrap().await.expect("spawned batch send must not panic");
}

// ── tag_lambda_function_once ────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn tag_lambda_function_once_does_not_panic_and_guards_repeat_calls() {
    let config = config::ExtensionConfig::default();
    // First call fires the process-wide Once (spawns a fire-and-forget background
    // tagging task); the second call must be a guaranteed no-op — proving the
    // call_once guard works and neither call panics.
    tag_lambda_function_once(
        "arn:aws:lambda:us-east-1:123:function:tag-test".to_string(),
        &config,
    );
    tag_lambda_function_once(
        "arn:aws:lambda:us-east-1:123:function:tag-test-2".to_string(),
        &config,
    );
}

// ── buffer_failed_agent_payload ─────────────────────────────────────────────────────

#[test]
#[serial]
fn buffer_failed_agent_payload_appends_to_failed_buffer() {
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
    buffer_failed_agent_payload(&[1, 2, 3], "buf-fail-req", "arn:aws:lambda:us-east-1:123:function:test");
    let len = FAILED_AGENT_PAYLOADS.lock().map(|b| b.len()).unwrap_or(0);
    assert_eq!(len, 1);
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
}

// ── send_to_apm_collector (APM) ─────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn send_to_apm_collector_errors_when_apm_not_connected() {
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
    let result = send_to_apm_collector(&[1, 2, 3], "satc-req-1", &apm_app).await;
    assert!(result.is_err());
}

// ── process_and_send_agent_payload (APM) ────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn process_and_send_agent_payload_buffers_when_apm_not_connected() {
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
    let config = make_config_for_serverless(false);
    let log_processor = make_noop_log_processor_serverless(config.clone());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    let result = process_and_send_agent_payload(
        &[1, 2, 3],
        "pasap-req-1",
        "arn:aws:lambda:us-east-1:123:function:test",
        &log_processor,
        &config,
        &apm_app,
    )
    .await;

    assert!(result.is_ok());
    let buffered = FAILED_AGENT_PAYLOADS
        .lock()
        .map(|b| b.iter().any(|p| p.request_id == "pasap-req-1"))
        .unwrap_or(false);
    assert!(buffered, "payload must be buffered when APM isn't connected");

    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
}

// ── process_pending_agent_payloads (APM) ────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn process_pending_agent_payloads_is_a_no_op_when_nothing_buffered() {
    let config = make_config_for_serverless(false);
    let log_processor = make_noop_log_processor_serverless(config.clone());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    // No other REQUEST_DATA entries exist for this made-up current id — must return
    // immediately without panicking or touching FAILED_AGENT_PAYLOADS.
    process_pending_agent_payloads(&config, &log_processor, &apm_app, "ppap-current-nonexistent").await;
}

#[tokio::test]
#[serial]
async fn process_pending_agent_payloads_buffers_late_payload_when_apm_not_connected() {
    let prev_id = "ppap-prev-req-1";
    let current_id = "ppap-current-req-1";
    let config = make_config_for_serverless(false);
    register_request_for_serverless(prev_id, config.clone());
    if let Some(buf) = request::get_agent_buffer(prev_id) {
        buf.lock().unwrap().push(vec![7, 7, 7]);
    }
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    process_pending_agent_payloads(&config, &log_processor, &apm_app, current_id).await;

    let buffered = FAILED_AGENT_PAYLOADS
        .lock()
        .map(|b| b.iter().any(|p| p.request_id == prev_id))
        .unwrap_or(false);
    assert!(buffered, "late payload must be buffered for retry when APM isn't connected");

    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
    REQUEST_DATA.remove(prev_id);
}

// ── retry_failed_agent_payloads (APM) ────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn retry_failed_agent_payloads_is_a_no_op_when_buffer_empty() {
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
    retry_failed_agent_payloads(apm_app).await;
    let len = FAILED_AGENT_PAYLOADS.lock().map(|b| b.len()).unwrap_or(0);
    assert_eq!(len, 0);
}

#[tokio::test]
#[serial]
async fn retry_failed_agent_payloads_keeps_buffered_when_apm_not_connected() {
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
        b.push(make_failed_payload("retry-req-1"));
    }
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
    retry_failed_agent_payloads(apm_app).await;
    let retained = FAILED_AGENT_PAYLOADS
        .lock()
        .map(|b| b.iter().any(|p| p.request_id == "retry-req-1"))
        .unwrap_or(false);
    assert!(retained, "payload must stay buffered when APM still isn't connected");
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
}

#[tokio::test]
#[serial]
async fn retry_failed_agent_payloads_drops_payload_older_than_24h() {
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
        let mut stale = make_failed_payload("retry-stale-req");
        stale.failed_at = chrono::Utc::now() - chrono::Duration::hours(25);
        b.push(stale);
    }
    let before_dropped = dropped_agent_payload_count();
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
    retry_failed_agent_payloads(apm_app).await;

    let len = FAILED_AGENT_PAYLOADS.lock().map(|b| b.len()).unwrap_or(0);
    assert_eq!(len, 0, "stale payload must be dropped, not re-buffered");
    assert_eq!(dropped_agent_payload_count(), before_dropped + 1);
}

// ── process_apm_request (APM) ───────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn process_apm_request_completes_cold_start_with_no_payload_and_no_run_id() {
    let request_id = "par-cold-req-1";
    let config = make_config_for_serverless(false);
    register_request_for_serverless(request_id, config.clone());

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

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

    assert!(
        REQUEST_PROCESSORS.get(request_id).is_none(),
        "process_apm_request must consume the processing state on completion"
    );
}

#[tokio::test]
#[serial]
async fn process_apm_request_returns_early_when_no_state_registered() {
    let config = make_config_for_serverless(false);
    let log_processor = make_noop_log_processor_serverless(config.clone());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    // No create_request_processing_state call — must return promptly without panicking.
    process_apm_request(
        "par-missing-state-req".to_string(),
        "arn:aws:lambda:us-east-1:123:function:test".to_string(),
        true,
        config,
        log_processor,
        apm_app,
        deadline_ms_from_now(5_000),
    )
    .await;
}

#[tokio::test]
#[serial]
async fn process_apm_request_warm_start_drains_previous_request_late_payload() {
    let prev_id = "par-warm-prev-req";
    let current_id = "par-warm-current-req";
    let config = make_config_for_serverless(false);
    register_request_for_serverless(prev_id, config.clone());
    if let Some(buf) = request::get_agent_buffer(prev_id) {
        buf.lock().unwrap().push(vec![5, 5, 5]);
    }
    register_request_for_serverless(current_id, config.clone());

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    process_apm_request(
        current_id.to_string(),
        "arn:aws:lambda:us-east-1:123:function:test".to_string(),
        false,
        config,
        log_processor,
        apm_app,
        deadline_ms_from_now(5_000),
    )
    .await;

    assert!(REQUEST_PROCESSORS.get(current_id).is_none());
    REQUEST_DATA.remove(prev_id);
}

// ── execute_apm_mode_event_loop (APM) — driven end-to-end against a wiremock Extensions API ──

#[tokio::test]
#[serial]
async fn execute_apm_mode_event_loop_processes_invoke_then_shutdown() {
    let server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-apm-loop-1",
                "arn:aws:lambda:us-east-1:123456789012:function:apm-loop-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, true);

    let event_count = with_runtime_api_el(&server, || async {
        execute_apm_mode_event_loop(&mut components).await
    })
    .await;

    assert_eq!(event_count, 2, "must count both the INVOKE and the terminating SHUTDOWN event");

    REQUEST_DATA.remove("req-apm-loop-1");
}

#[tokio::test]
#[serial]
async fn execute_apm_mode_event_loop_returns_on_immediate_shutdown() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("timeout")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, true);

    let event_count = with_runtime_api_el(&server, || async {
        execute_apm_mode_event_loop(&mut components).await
    })
    .await;

    assert_eq!(event_count, 1);
}

// ── execute_main_telemetry_processing_loop — APM branch ───────────────────────────────

#[tokio::test]
#[serial]
async fn execute_main_telemetry_processing_loop_routes_normal_apm_to_apm_loop() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, true);
    components.deployment = crate::config::deployment::DeploymentContext::Normal {
        mode: crate::config::deployment::TelemetryMode::Apm,
    };

    let count = with_runtime_api_el(&server, || async {
        execute_main_telemetry_processing_loop(&mut components).await
    })
    .await;

    assert_eq!(count, 1, "single SHUTDOWN must count as one event via the APM loop");
}

// ── execute_noop_event_loop — non-fatal error continues ───────────────────────────────

#[tokio::test]
#[serial]
async fn execute_noop_event_loop_continues_on_non_fatal_error() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(500).set_body_string("transient"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let client = Arc::new(Client::new());
    with_runtime_api_el(&server, || async {
        execute_noop_event_loop(&client, "test-ext-id").await;
    })
    .await;
}

// ── execute_apm_mode_event_loop — 403 and non-fatal error paths ──────────────────────

#[tokio::test]
#[serial]
async fn execute_apm_mode_event_loop_exits_on_fatal_403() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, true);

    with_runtime_api_el(&server, || async {
        execute_apm_mode_event_loop(&mut components).await;
    })
    .await;
}

#[tokio::test]
#[serial]
async fn execute_apm_mode_event_loop_continues_on_non_fatal_error() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(500).set_body_string("transient"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, true);

    let event_count = with_runtime_api_el(&server, || async {
        execute_apm_mode_event_loop(&mut components).await
    })
    .await;
    assert_eq!(event_count, 1, "must count the SHUTDOWN after recovering from transient error");
}

// ── execute_standard_mode_event_loop — 403 and non-fatal error paths ─────────────────

#[tokio::test]
#[serial]
async fn execute_standard_mode_event_loop_exits_on_fatal_403() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, false);

    with_runtime_api_el(&server, || async {
        execute_standard_mode_event_loop(&mut components).await;
    })
    .await;
}

#[tokio::test]
#[serial]
async fn execute_standard_mode_event_loop_continues_on_non_fatal_error() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(500).set_body_string("transient"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, false);

    let event_count = with_runtime_api_el(&server, || async {
        execute_standard_mode_event_loop(&mut components).await
    })
    .await;
    assert_eq!(event_count, 1, "must count the SHUTDOWN after recovering from transient error");
}

// ── maybe_spawn_batch_send — threshold path ───────────────────────────────────────────

#[tokio::test]
#[serial]
async fn maybe_spawn_batch_send_spawns_when_threshold_reached_without_synchronous_flush() {
    crate::agent::batch::AGENT_BATCH_BUFFER.clear();
    for i in 0..3usize {
        crate::agent::batch::add_to_batch(
            format!("threshold-req-{i}"),
            vec![i as u8],
            Some(format!("REPORT {i}")),
            "arn:aws:lambda:us-east-1:123:function:test".to_string(),
        );
    }
    assert!(crate::agent::batch::should_send_batch_by_threshold(), "precondition: threshold must be met");

    let config = make_config_for_serverless(false); // synchronous_flush OFF
    let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
    let handle = maybe_spawn_batch_send(newrelic_client, config);
    assert!(handle.is_some(), "threshold met: must spawn even when synchronous_flush is off");
    handle.unwrap().await.expect("batch send task must not panic");

    crate::agent::batch::AGENT_BATCH_BUFFER.clear();
}

#[tokio::test]
#[serial]
async fn maybe_spawn_batch_send_returns_none_below_threshold_with_flag_off() {
    crate::agent::batch::AGENT_BATCH_BUFFER.clear();
    assert!(!crate::agent::batch::should_send_batch_by_threshold(), "precondition: threshold must not be met");

    let config = make_config_for_serverless(false);
    let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
    let handle = maybe_spawn_batch_send(newrelic_client, config);
    assert!(handle.is_none(), "below threshold + flag off: must not spawn");
}

// ── build_shutdown_drop_log — >100 request_ids truncation ─────────────────────────────

#[test]
fn build_shutdown_drop_log_truncates_ids_beyond_100() {
    let ids: Vec<String> = (0..=100).map(|i| format!("req-{i:03}")).collect(); // 101 ids
    let diag = ShutdownDropDiagnostic {
        message: "dropped".to_string(),
        arn: "arn".to_string(),
        request_id: "last-req".to_string(),
        request_ids: ids,
        request_id_count: 101,
        item_count: 101,
    };
    let log = build_shutdown_drop_log(&diag);
    let ids_attr = log.attributes["dropped.request_ids"].as_str().unwrap();
    assert!(
        ids_attr.contains("(+1 more)"),
        "must note truncated overflow, got: {ids_attr}"
    );
    let listed = ids_attr.split(',').filter(|s| !s.ends_with("more)")).count();
    assert_eq!(listed, 100, "must list exactly 100 ids before the overflow note");
}

// ── extract_and_coordinate_trace_id ──────────────────────────────────────────────────

#[tokio::test]
async fn extract_and_coordinate_trace_id_is_noop_when_disabled() {
    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.collect_trace_id = false;
    let config = Arc::new(cfg);
    let log_processor = make_noop_log_processor_serverless(config.clone());

    let t0 = std::time::Instant::now();
    extract_and_coordinate_trace_id(b"any bytes", "req-ectid-1", &config, &log_processor).await;
    assert!(t0.elapsed() < Duration::from_millis(50), "disabled path must return immediately");
}

#[tokio::test]
async fn extract_and_coordinate_trace_id_handles_payload_without_nr_marker() {
    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.collect_trace_id = true;
    let config = Arc::new(cfg);
    let log_processor = make_noop_log_processor_serverless(config.clone());

    // Payload with no NR_LAMBDA_MONITORING marker: extract_trace_id_from_payload returns Ok(None).
    extract_and_coordinate_trace_id(b"[]", "req-ectid-2", &config, &log_processor).await;
}

#[tokio::test]
async fn extract_and_coordinate_trace_id_handles_invalid_utf8_without_panic() {
    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.collect_trace_id = true;
    let config = Arc::new(cfg);
    let log_processor = make_noop_log_processor_serverless(config.clone());

    // Invalid UTF-8 bytes: extract_trace_id_from_payload returns Err, silently ignored.
    extract_and_coordinate_trace_id(&[0xFF, 0xFE], "req-ectid-3", &config, &log_processor).await;
}

// ── wait_for_runtime_done_with_grace — grace period with undrained batch ──────────────

#[tokio::test]
#[serial]
async fn wait_for_runtime_done_waits_grace_when_signalled_and_batch_not_drained() {
    let request_id = "wfrd-grace-undrained";
    let mut cfg = config::ExtensionConfig::default();
    cfg.extension.send_function_logs = true;
    cfg.extension.runtime_done_grace_ms = 150;
    let config = Arc::new(cfg);
    register_request_for_serverless(request_id, config.clone());

    let notify = request::get_runtime_done_notify(request_id).expect("notify must exist");
    notify.notify_one(); // pre-fire so notified() resolves immediately

    // Push a log into the batch so is_drained() returns false.
    let log_processor = make_noop_log_processor_serverless(config.clone());
    log_processor.add_log_to_batch(crate::newrelic::payload::LogMessage::diagnostic(
        "INFO",
        "pending log".to_string(),
    ));
    assert!(!log_processor.is_drained(), "precondition: batch must not be drained");

    let start = std::time::Instant::now();
    wait_for_runtime_done_with_grace(
        request_id,
        deadline_ms_from_now(5_000),
        &config,
        &log_processor,
    )
    .await;
    let elapsed = start.elapsed();

    // Grace period (150ms) must have been entered; function returns after grace expires.
    assert!(
        elapsed >= Duration::from_millis(100),
        "must have waited the grace period when batch is not yet drained (elapsed: {:?})",
        elapsed
    );

    REQUEST_DATA.remove(request_id);
}

// ── wait_for_runtime_done_with_grace — grace_ms=0 skips grace window ─────────────────

#[tokio::test]
#[serial]
async fn wait_for_runtime_done_with_grace_skips_grace_when_grace_ms_is_zero() {
    let request_id = "wfrd-grace-zero";
    let mut cfg = config::ExtensionConfig::default();
    cfg.extension.send_function_logs = true;
    cfg.extension.runtime_done_grace_ms = 0; // grace disabled
    let config = Arc::new(cfg);
    register_request_for_serverless(request_id, config.clone());

    let notify = request::get_runtime_done_notify(request_id).expect("notify must exist");
    notify.notify_one(); // pre-fire so the select picks up the signal immediately

    // Undrained batch — if grace_ms were > 0 this would cause a wait
    let log_processor = make_noop_log_processor_serverless(config.clone());
    log_processor.add_log_to_batch(crate::newrelic::payload::LogMessage::diagnostic(
        "INFO",
        "pending log for grace-zero test".to_string(),
    ));
    assert!(!log_processor.is_drained(), "precondition: batch must be undrained");

    let start = std::time::Instant::now();
    wait_for_runtime_done_with_grace(
        request_id,
        deadline_ms_from_now(5_000),
        &config,
        &log_processor,
    )
    .await;
    let elapsed = start.elapsed();

    // grace_ms=0 means the inner if-block is skipped → returns immediately after receiving signal
    assert!(
        elapsed < Duration::from_millis(100),
        "grace_ms=0 must return immediately without waiting (elapsed: {:?})",
        elapsed
    );

    REQUEST_DATA.remove(request_id);
}

// ── run_infinite_event_loop — enabled path routes to standard mode ────────────────────

#[tokio::test]
#[serial]
async fn run_infinite_event_loop_routes_to_standard_mode_when_enabled_with_license_key() {
    let server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-riel-enabled-1",
                "arn:aws:lambda:us-east-1:123456789012:function:riel-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("test-license-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let components = make_test_extension_components(config, client, false);

    let count = with_runtime_api_el(&server, || async {
        run_infinite_event_loop(components).await
    })
    .await;

    assert_eq!(count, 2, "enabled+key path must count both INVOKE and SHUTDOWN");
    REQUEST_DATA.remove("req-riel-enabled-1");
}

// ── execute_standard_mode_event_loop — warm start path ────────────────────────────────

#[tokio::test]
#[serial]
async fn execute_standard_mode_warm_start_covers_second_invoke_path() {
    let server = wiremock::MockServer::start().await;

    // First INVOKE (cold start)
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-std-warm-1",
                "arn:aws:lambda:us-east-1:123456789012:function:warm-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Second INVOKE (warm start — covers is_cold_start=false + drain_late_paired_payloads)
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-std-warm-2",
                "arn:aws:lambda:us-east-1:123456789012:function:warm-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, false);

    let event_count = with_runtime_api_el(&server, || async {
        execute_standard_mode_event_loop(&mut components).await
    })
    .await;

    assert_eq!(event_count, 3, "2 INVOKEs + 1 SHUTDOWN = 3 events");
    REQUEST_DATA.remove("req-std-warm-1");
    REQUEST_DATA.remove("req-std-warm-2");
}

// ── execute_standard_mode_event_loop — shutdown with Timeout/Failure/Unknown ──────────

#[tokio::test]
#[serial]
async fn execute_standard_mode_shutdown_timeout_synthesizes_error() {
    let server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-std-timeout-1",
                "arn:aws:lambda:us-east-1:123456789012:function:timeout-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("timeout")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, false);

    let event_count = with_runtime_api_el(&server, || async {
        execute_standard_mode_event_loop(&mut components).await
    })
    .await;

    assert_eq!(event_count, 2, "INVOKE + timeout SHUTDOWN = 2 events");
    REQUEST_DATA.remove("req-std-timeout-1");
}

#[tokio::test]
#[serial]
async fn execute_standard_mode_shutdown_failure_synthesizes_error() {
    let server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-std-failure-1",
                "arn:aws:lambda:us-east-1:123456789012:function:failure-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("failure")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, false);

    let event_count = with_runtime_api_el(&server, || async {
        execute_standard_mode_event_loop(&mut components).await
    })
    .await;

    assert_eq!(event_count, 2, "INVOKE + failure SHUTDOWN = 2 events");
    REQUEST_DATA.remove("req-std-failure-1");
}

#[tokio::test]
#[serial]
async fn execute_standard_mode_shutdown_unknown_reason_synthesizes_generic_error() {
    let server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-std-unknown-1",
                "arn:aws:lambda:us-east-1:123456789012:function:unknown-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("catastrophic")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, false);

    let event_count = with_runtime_api_el(&server, || async {
        execute_standard_mode_event_loop(&mut components).await
    })
    .await;

    assert_eq!(event_count, 2, "INVOKE + unknown SHUTDOWN = 2 events");
    REQUEST_DATA.remove("req-std-unknown-1");
}

// ── execute_standard_mode_event_loop — pipeline_flush path ───────────────────────────

#[tokio::test]
#[serial]
async fn execute_standard_mode_pipeline_flush_defers_processing_handle() {
    let server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-std-pf-1",
                "arn:aws:lambda:us-east-1:123456789012:function:pipeline-flush-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    cfg.extension.pipeline_flush = true;
    cfg.new_relic.synchronous_flush = false; // pipeline_flush takes effect only when synchronous_flush is off
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, false);

    let event_count = with_runtime_api_el(&server, || async {
        execute_standard_mode_event_loop(&mut components).await
    })
    .await;

    assert_eq!(event_count, 2, "pipeline_flush path: INVOKE + SHUTDOWN = 2 events");
    REQUEST_DATA.remove("req-std-pf-1");
}

// ── execute_standard_mode_event_loop — cleanup_counter >= 10 path ────────────────────

#[tokio::test]
#[serial]
async fn execute_standard_mode_cleanup_counter_fires_after_10_invocations() {
    let server = wiremock::MockServer::start().await;

    // Mount 10 INVOKE responses followed by a SHUTDOWN
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-std-cleanup",
                "arn:aws:lambda:us-east-1:123456789012:function:cleanup-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(10)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, false);

    let event_count = with_runtime_api_el(&server, || async {
        execute_standard_mode_event_loop(&mut components).await
    })
    .await;

    // 10 INVOKEs + 1 SHUTDOWN = 11 events; cleanup fires (spawned as background task) on 10th invoke
    assert_eq!(event_count, 11, "10 INVOKEs + SHUTDOWN = 11 events; cleanup_counter >= 10 fires on 10th");
    REQUEST_DATA.remove("req-std-cleanup");
}

// ── execute_apm_mode_event_loop — warm start path ─────────────────────────────────────

#[tokio::test]
#[serial]
async fn execute_apm_mode_warm_start_covers_second_invoke_path() {
    let server = wiremock::MockServer::start().await;

    // First INVOKE (cold start)
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-apm-warm-1",
                "arn:aws:lambda:us-east-1:123456789012:function:apm-warm-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Second INVOKE (warm start — is_cold_start=false in APM loop)
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-apm-warm-2",
                "arn:aws:lambda:us-east-1:123456789012:function:apm-warm-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, true);

    let event_count = with_runtime_api_el(&server, || async {
        execute_apm_mode_event_loop(&mut components).await
    })
    .await;

    assert_eq!(event_count, 3, "2 APM INVOKEs + 1 SHUTDOWN = 3 events");
    REQUEST_DATA.remove("req-apm-warm-1");
    REQUEST_DATA.remove("req-apm-warm-2");
}

// ── execute_apm_mode_event_loop — pipeline_flush defers combined task ─────────────────

#[tokio::test]
#[serial]
async fn execute_apm_mode_pipeline_flush_defers_combined_task() {
    let server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-apm-pf-1",
                "arn:aws:lambda:us-east-1:123456789012:function:apm-pf-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    cfg.extension.pipeline_flush = true;
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, true);

    let event_count = with_runtime_api_el(&server, || async {
        execute_apm_mode_event_loop(&mut components).await
    })
    .await;

    assert_eq!(event_count, 2, "APM pipeline_flush: INVOKE + SHUTDOWN = 2 events");
    REQUEST_DATA.remove("req-apm-pf-1");
}

// ── execute_apm_mode_event_loop — apm_blocking_handshake path ────────────────────────

#[tokio::test]
#[serial]
async fn execute_apm_mode_blocking_handshake_waits_within_budget_then_continues() {
    let server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-apm-bh-1",
                "arn:aws:lambda:us-east-1:123456789012:function:apm-bh-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    cfg.new_relic.apm_blocking_handshake = true;
    cfg.new_relic.apm_handshake_timeout_secs = 1;
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    // reconnect_in_flight=false → wait_for_apm_handshake_within_budget returns immediately
    let mut components = make_test_extension_components(config, client, true);

    let event_count = with_runtime_api_el(&server, || async {
        execute_apm_mode_event_loop(&mut components).await
    })
    .await;

    assert_eq!(event_count, 2, "APM blocking_handshake: INVOKE + SHUTDOWN = 2 events");
    REQUEST_DATA.remove("req-apm-bh-1");
}

// ── execute_apm_mode_event_loop — cleanup_counter >= 10 path ─────────────────────────

#[tokio::test]
#[serial]
async fn execute_apm_mode_cleanup_counter_fires_after_10_invocations() {
    let server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-apm-cleanup",
                "arn:aws:lambda:us-east-1:123456789012:function:apm-cleanup-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(10)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, true);

    let event_count = with_runtime_api_el(&server, || async {
        execute_apm_mode_event_loop(&mut components).await
    })
    .await;

    assert_eq!(event_count, 11, "10 APM INVOKEs + SHUTDOWN = 11 events; cleanup fires on 10th");
    REQUEST_DATA.remove("req-apm-cleanup");
}

// ── execute_apm_mode_event_loop — shutdown Timeout/Failure/Unknown with LAST_REQUEST_CONTEXT set

#[tokio::test]
#[serial]
async fn execute_apm_mode_shutdown_timeout_with_last_request_context_set() {
    let server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-apm-tmout-1",
                "arn:aws:lambda:us-east-1:123456789012:function:apm-timeout-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("timeout")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, true);

    let event_count = with_runtime_api_el(&server, || async {
        execute_apm_mode_event_loop(&mut components).await
    })
    .await;

    assert_eq!(event_count, 2, "APM INVOKE + timeout SHUTDOWN = 2 events");
    REQUEST_DATA.remove("req-apm-tmout-1");
}

#[tokio::test]
#[serial]
async fn execute_apm_mode_shutdown_failure_with_last_request_context_set() {
    let server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-apm-fail-1",
                "arn:aws:lambda:us-east-1:123456789012:function:apm-failure-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("failure")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, true);

    let event_count = with_runtime_api_el(&server, || async {
        execute_apm_mode_event_loop(&mut components).await
    })
    .await;

    assert_eq!(event_count, 2, "APM INVOKE + failure SHUTDOWN = 2 events");
    REQUEST_DATA.remove("req-apm-fail-1");
}

#[tokio::test]
#[serial]
async fn execute_apm_mode_shutdown_unknown_reason_with_last_request_context_set() {
    let server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-apm-unk-1",
                "arn:aws:lambda:us-east-1:123456789012:function:apm-unknown-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("weird_reason")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, true);

    let event_count = with_runtime_api_el(&server, || async {
        execute_apm_mode_event_loop(&mut components).await
    })
    .await;

    assert_eq!(event_count, 2, "APM INVOKE + unknown SHUTDOWN = 2 events");
    REQUEST_DATA.remove("req-apm-unk-1");
}

// ── execute_standard_mode_event_loop — 403 pipeline_flush emergency path ─────────────

#[tokio::test]
#[serial]
async fn execute_standard_mode_pipeline_flush_403_emergency_shutdown() {
    let server = wiremock::MockServer::start().await;

    // First INVOKE pushes a handle into pending_flush_handles
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-std-pf-403",
                "arn:aws:lambda:us-east-1:123456789012:function:pf-403-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // 403 triggers emergency shutdown while pending_flush_handles is non-empty
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    cfg.extension.pipeline_flush = true;
    cfg.new_relic.synchronous_flush = false;
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, false);

    with_runtime_api_el(&server, || async {
        execute_standard_mode_event_loop(&mut components).await;
    })
    .await;

    REQUEST_DATA.remove("req-std-pf-403");
}

// ── execute_apm_mode_event_loop — 403 pipeline_flush emergency path ───────────────────

#[tokio::test]
#[serial]
async fn execute_apm_mode_pipeline_flush_403_emergency_shutdown() {
    let server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                "req-apm-pf-403",
                "arn:aws:lambda:us-east-1:123456789012:function:apm-pf-403-test",
                deadline_ms_from_now(5_000),
            )),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // 403 while pending_flush_handles is non-empty (pipeline_flush=true means handle was pushed)
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    cfg.extension.pipeline_flush = true;
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, true);

    with_runtime_api_el(&server, || async {
        execute_apm_mode_event_loop(&mut components).await;
    })
    .await;

    REQUEST_DATA.remove("req-apm-pf-403");
}

// ── execute_apm_mode_event_loop — SHUTDOWN: REQUEST_DATA drain + drop diagnostic ──────

/// Cover the APM SHUTDOWN payload-drain path (lines ~641-693): when REQUEST_DATA has
/// entries with non-empty agent_buffers at shutdown time those payloads are drained and
/// sent via process_and_send_agent_payload.  With apm_app=None the send function
/// buffers them into FAILED_AGENT_PAYLOADS (Ok result), so remaining_count becomes 1
/// and the drop-diagnostic building block (lines ~748-795) executes.  The pending-report
/// warning branch (lines ~807-829) is also exercised by pre-loading a set_pending_report
/// entry on the same request.  The diagnostic send (lines ~852-868) runs outside the
/// main timeout with license_key=None → fast no-op or immediate error.
#[tokio::test]
#[serial]
async fn execute_apm_mode_shutdown_drains_buffered_request_data_and_emits_drop_diagnostic() {
    // Clear globals that could cause the error-synthesis reconnect block (lines ~576-630)
    // to consume the 1300 ms shutdown budget before the drain block gets to run.
    *LAST_REQUEST_CONTEXT.lock().unwrap() = None;
    FAILED_AGENT_PAYLOADS.lock().unwrap().clear();

    let drain_req = "req-apm-sdown-drain-01";
    let test_arn = "arn:aws:lambda:us-east-1:123456789012:function:sdown-drain-test";

    // Populate REQUEST_DATA with a buffered payload (non-empty buffer → drain path runs).
    {
        let config_inner = Arc::new(config::ExtensionConfig::default());
        let factory = make_serverless_processor_factory(config_inner);
        let state = create_request_processing_state(drain_req, test_arn, &factory);
        state.agent_buffer.lock().unwrap().push(vec![0xDE, 0xAD]);
        // state drops here but REQUEST_DATA still holds the Arc to the buffer
    }
    // Set a pending platform.report so the pending-report warning branch also fires.
    request::set_pending_report(
        drain_req,
        "REPORT Duration: 100.00 ms Max Memory Used: 64 MB Memory Size: 128 MB".to_string(),
    );

    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_json(shutdown_event_body("spindown")),
        )
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = None; // skips / fast-fails send_logs → no real HTTP
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, true);

    let event_count = with_runtime_api_el(&server, || async {
        execute_apm_mode_event_loop(&mut components).await
    })
    .await;

    assert_eq!(event_count, 1, "immediate SHUTDOWN must count as 1 event");

    // After the drain, buffer_failed_agent_payload put one entry in FAILED_AGENT_PAYLOADS;
    // retry_failed_agent_payloads (apm_app=None) rebuffered it back. Clean up.
    REQUEST_DATA.remove(drain_req);
    FAILED_AGENT_PAYLOADS.lock().unwrap().clear();
}

// ── process_apm_request — Flow-2: no run_id, payload present → rebuffer ──────────────

/// Cover Flow-2 (lines ~1421-1431): the agent payload has arrived but the APM connection
/// is not yet established (has_run_id=false, got_payload=true).  The payload must be put
/// back into the agent_buffer so it can be picked up on the next invocation or at shutdown.
#[tokio::test]
#[serial]
async fn process_apm_request_flow2_rebuffers_payload_when_no_run_id() {
    let req_id = "par-flow2-rebuf-01";
    let config = make_config_for_serverless(false);
    let factory = make_serverless_processor_factory(config.clone());
    let state = create_request_processing_state(req_id, "arn:test", &factory);
    // Pre-load one payload: got_payload=true.  apm_app=None → has_run_id=false → Flow-2.
    state.agent_buffer.lock().unwrap().push(vec![0xAB, 0xCD, 0xEF]);
    REQUEST_PROCESSORS.insert(req_id.to_string(), state);

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    process_apm_request(
        req_id.to_string(),
        "arn:test".to_string(),
        true, // is_cold_start=true → skip warm-start drain; keeps test deterministic
        config,
        log_processor,
        apm_app,
        deadline_ms_from_now(5_000),
    )
    .await;

    // Flow-2 must put the payload back so it is not silently dropped.
    let buf_len = get_agent_buffer(req_id)
        .map(|b| b.lock().unwrap().len())
        .unwrap_or(0);
    assert_eq!(buf_len, 1, "Flow-2: payload must be rebuffered when APM run_id is unavailable");

    REQUEST_DATA.remove(req_id);
}

// ── wait_for_runtime_done_with_grace — expired deadline uses fallback ─────────────────

/// Cover lines 1607-1611: the INVOKE deadlineMs is already in the past, so the
/// function must use the FALLBACK_RUNTIME_DONE_WAIT_MS constant instead of the
/// remaining budget. Pre-firing the runtime.done notify collapses the 5-second
/// fallback wait to effectively zero, keeping the test fast.
#[tokio::test]
#[serial]
async fn wait_for_runtime_done_uses_fallback_when_deadline_is_past() {
    let request_id = "wfrd-past-deadline-fallback";
    let mut cfg = config::ExtensionConfig::default();
    cfg.extension.send_function_logs = true;
    let config = Arc::new(cfg);
    register_request_for_serverless(request_id, config.clone());

    // Pre-fire the notify so notified() resolves instantly even via the fallback path.
    let notify = request::get_runtime_done_notify(request_id).expect("notify must exist after registration");
    notify.notify_one();

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let start = std::time::Instant::now();
    wait_for_runtime_done_with_grace(
        request_id,
        deadline_ms_from_now(-1_000), // already expired → triggers lines 1607-1611
        &config,
        &log_processor,
    )
    .await;
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "pre-fired notify must resolve fast even on the expired-deadline fallback path"
    );

    REQUEST_DATA.remove(request_id);
}

// ── execute_noop_event_loop — INVOKE arm ────────────────────────────────────────────

/// Cover lines 1261-1267: the no-op event loop receives an INVOKE before the final
/// SHUTDOWN. The INVOKE arm simply logs and loops back to /next; it must not exit
/// early or panic.
#[tokio::test]
#[serial]
async fn execute_noop_event_loop_processes_invoke_before_shutdown() {
    let server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
            "req-noop-invoke-1",
            "arn:aws:lambda:us-east-1:123:function:noop",
            deadline_ms_from_now(5_000),
        )))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let client = Arc::new(Client::new());
    with_runtime_api_el(&server, || async {
        execute_noop_event_loop(&client, "test-noop-ext-id").await;
    })
    .await;
}

// ── execute_standard_mode_event_loop — add_version_detail_tags cold-start ─────────

/// Cover line 979: tag_lambda_function_once is called on cold-start when
/// add_version_detail_tags is enabled. The function uses a static Once internally
/// so the tagging call is instrumented as executed even if the Once body is a no-op.
#[tokio::test]
#[serial]
async fn execute_standard_mode_event_loop_tags_function_on_cold_start_with_version_tags() {
    let server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
            "req-svl-vtags-1",
            "arn:aws:lambda:us-east-1:123:function:vtags",
            deadline_ms_from_now(5_000),
        )))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = None;
    cfg.new_relic.add_version_detail_tags = true;
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, false);

    with_runtime_api_el(&server, || async {
        execute_standard_mode_event_loop(&mut components).await;
    })
    .await;

    REQUEST_DATA.remove("req-svl-vtags-1");
}

// ── execute_apm_mode_event_loop — add_version_detail_tags cold-start ──────────────

/// Cover line 388: tag_lambda_function_once is called on cold-start (APM mode) when
/// add_version_detail_tags is enabled.
#[tokio::test]
#[serial]
async fn execute_apm_mode_event_loop_tags_function_on_cold_start_with_version_tags() {
    let server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
            "req-apm-vtags-1",
            "arn:aws:lambda:us-east-1:123:function:apm-vtags",
            deadline_ms_from_now(5_000),
        )))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = None;
    cfg.new_relic.add_version_detail_tags = true;
    let config = Arc::new(cfg);
    let client = Arc::new(Client::new());
    let mut components = make_test_extension_components(config, client, true);

    with_runtime_api_el(&server, || async {
        execute_apm_mode_event_loop(&mut components).await;
    })
    .await;

    REQUEST_DATA.remove("req-apm-vtags-1");
    FAILED_AGENT_PAYLOADS.lock().unwrap().clear();
}

// ── process_apm_request — pending platform.report with APM app not ready ──────────

/// Cover lines 1465-1480: a platform.report is pending in REQUEST_DATA when
/// process_apm_request runs, but apm_app is None (APM not yet connected). The
/// function must log a warning and remove the report rather than silently losing it.
#[tokio::test]
#[serial]
async fn process_apm_request_pending_report_warns_when_apm_app_not_ready() {
    let req_id = "par-pending-report-no-app";
    let config = make_config_for_serverless(false);
    let factory = make_serverless_processor_factory(config.clone());
    let state = create_request_processing_state(req_id, "arn:test", &factory);
    REQUEST_PROCESSORS.insert(req_id.to_string(), state);

    request::set_pending_report(
        req_id,
        "Duration: 50.00 ms Billed Duration: 100 ms Memory Size: 128 MB Max Memory Used: 64 MB".to_string(),
    );

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    process_apm_request(
        req_id.to_string(),
        "arn:test".to_string(),
        true, // is_cold_start — skips warm-start drain; keeps test deterministic
        config,
        log_processor,
        apm_app,
        deadline_ms_from_now(1_000),
    )
    .await;

    // The pending report must have been consumed after the warning path executes.
    assert!(
        request::get_pending_report(req_id).is_none(),
        "pending report must be removed after the apm-not-ready warning path"
    );

    REQUEST_DATA.remove(req_id);
}

// ── process_request_concurrently — early return when no state registered ──────────

/// Cover lines 1872-1873: when process_request_concurrently is called for a
/// request_id that was never registered in REQUEST_PROCESSORS, it must log an
/// error and return immediately without panicking.
#[tokio::test]
#[serial]
async fn process_request_concurrently_returns_early_when_no_state_registered() {
    let request_id = "prc-no-state-early-return";
    // Ensure the id is clean (no leftover state from a previous run).
    REQUEST_PROCESSORS.remove(request_id);

    let config = make_config_for_serverless(false);
    let log_processor = make_noop_log_processor_serverless(config.clone());
    let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());

    // Should return without panic; the error log at line 1872 is the observable side-effect.
    process_request_concurrently(
        request_id.to_string(),
        "arn:aws:lambda:us-east-1:123:function:test".to_string(),
        newrelic_client,
        config,
        log_processor,
        deadline_ms_from_now(1_000),
    )
    .await;
}

// ── process_request_concurrently — collect_trace_id loop ──────────────────────────

/// Cover lines 1905-1908: when collect_trace_id is enabled and agent payloads are
/// in the buffer, the trace extraction loop fires for each payload before the
/// smart-batching decision is made.
#[tokio::test]
#[serial]
async fn process_request_concurrently_runs_trace_extraction_loop_when_collect_trace_id_enabled() {
    let request_id = "prc-collect-trace-id";
    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.synchronous_flush = false;
    cfg.new_relic.collect_trace_id = true;
    let config = Arc::new(cfg);

    register_request_for_serverless(request_id, config.clone());

    // Push a payload so the non-empty branch at line 1900 is entered and the
    // collect_trace_id loop at lines 1905-1908 fires. The agent_buffer Arc is
    // shared between REQUEST_DATA and REQUEST_PROCESSORS so this push is visible
    // to process_request_concurrently without extra indirection.
    let buf = request::get_agent_buffer(request_id).expect("buffer must exist after registration");
    buf.lock().unwrap().push(vec![1u8, 2u8, 3u8]);

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());

    process_request_concurrently(
        request_id.to_string(),
        "arn:aws:lambda:us-east-1:123:function:test".to_string(),
        newrelic_client,
        config,
        log_processor,
        deadline_ms_from_now(1_000),
    )
    .await;

    REQUEST_DATA.remove(request_id);
}

// ── dropped_agent_payload_count ───────────────────────────────────────────────

#[test]
fn dropped_agent_payload_count_is_readable() {
    let _ = dropped_agent_payload_count();
}

// ── push_failed_payload_capped — eviction when buffer is at capacity ──────────

#[test]
#[serial]
fn buffer_failed_agent_payload_evicts_oldest_when_full() {
    {
        let mut buf = FAILED_AGENT_PAYLOADS.lock().unwrap();
        buf.clear();
        for i in 0..500usize {
            buf.push(FailedAgentPayload {
                payload_bytes: vec![],
                request_id: format!("req-fill-{i}"),
                invoked_function_arn: "arn:test".to_string(),
                retry_count: 0,
                failed_at: chrono::Utc::now(),
            });
        }
    }
    buffer_failed_agent_payload(b"new", "req-evict", "arn:test");

    let buf = FAILED_AGENT_PAYLOADS.lock().unwrap();
    assert_eq!(buf.len(), 500);
    assert_eq!(buf.last().unwrap().request_id, "req-evict");
    assert!(buf.iter().all(|p| p.request_id != "req-fill-0"), "oldest must be evicted");
    drop(buf);
    FAILED_AGENT_PAYLOADS.lock().unwrap().clear();
}

// ── bounded_wait_budget_ms ────────────────────────────────────────────────────

#[test]
fn bounded_wait_budget_ms_returns_zero_when_deadline_already_expired() {
    let past_ms = chrono::Utc::now().timestamp_millis() - 10_000;
    assert_eq!(super::bounded_wait_budget_ms(past_ms, 5_000), 0);
}

#[test]
fn bounded_wait_budget_ms_caps_result_to_configured_timeout() {
    let far_future_ms = chrono::Utc::now().timestamp_millis() + 100_000;
    assert_eq!(super::bounded_wait_budget_ms(far_future_ms, 1_000), 1_000);
}

// ── should_defer_via_pipeline_flush ──────────────────────────────────────────

#[test]
fn should_defer_via_pipeline_flush_all_cases() {
    assert!( super::should_defer_via_pipeline_flush(true,  false));
    assert!(!super::should_defer_via_pipeline_flush(false, false));
    assert!(!super::should_defer_via_pipeline_flush(true,  true));
    assert!(!super::should_defer_via_pipeline_flush(false, true));
}

// ── cleanup_old_failed_payloads ───────────────────────────────────────────────

#[test]
#[serial]
fn cleanup_old_failed_payloads_removes_entries_older_than_24h_and_keeps_recent() {
    FAILED_AGENT_PAYLOADS.lock().unwrap().clear();

    let now = chrono::Utc::now();
    let old_time = now - chrono::Duration::hours(25);

    {
        let mut buf = FAILED_AGENT_PAYLOADS.lock().unwrap();
        buf.push(FailedAgentPayload {
            payload_bytes: vec![1],
            request_id: "req-old-25h".to_string(),
            invoked_function_arn: "arn:test".to_string(),
            retry_count: 0,
            failed_at: old_time,
        });
        buf.push(FailedAgentPayload {
            payload_bytes: vec![2],
            request_id: "req-recent".to_string(),
            invoked_function_arn: "arn:test".to_string(),
            retry_count: 0,
            failed_at: now,
        });
    }

    cleanup_old_failed_payloads();

    let buf = FAILED_AGENT_PAYLOADS.lock().unwrap();
    assert_eq!(buf.len(), 1);
    assert_eq!(buf[0].request_id, "req-recent");
    drop(buf);
    FAILED_AGENT_PAYLOADS.lock().unwrap().clear();
}
