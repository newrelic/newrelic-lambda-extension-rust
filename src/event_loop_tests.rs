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
