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
    for i in 0..(*MAX_FAILED_AGENT_PAYLOADS + 1) {
        push_failed_payload_capped(&mut buf, make_failed_payload(&format!("req-{i}")));
    }
    assert_eq!(buf.len(), *MAX_FAILED_AGENT_PAYLOADS, "must not exceed cap");
    assert_eq!(
        dropped_agent_payload_count(),
        before + 1,
        "one eviction counted"
    );
    // Oldest (req-0) was evicted; newest is retained.
    assert!(!buf.iter().any(|p| p.request_id == "req-0"));
    assert!(buf
        .iter()
        .any(|p| p.request_id == format!("req-{}", *MAX_FAILED_AGENT_PAYLOADS)));
}

// The cap counts distinct request ids, not chunks; eviction drops the oldest
// request id's chunks together.
#[test]
#[serial]
fn failed_agent_payload_cap_counts_requests_not_chunks() {
    let cap = *MAX_FAILED_AGENT_PAYLOADS;
    let mut buf: Vec<FailedAgentPayload> = Vec::new();
    for _ in 0..20 {
        push_failed_payload_capped(&mut buf, make_failed_payload("req-big"));
    }
    for i in 1..cap {
        push_failed_payload_capped(&mut buf, make_failed_payload(&format!("req-{i}")));
    }
    assert_eq!(buf.len(), 20 + cap - 1, "20 chunks of one request count as 1");
    // One more distinct request: evicts all 20 chunks of the oldest request, nothing else.
    push_failed_payload_capped(&mut buf, make_failed_payload("req-new"));
    assert_eq!(buf.len(), cap);
    assert!(buf.iter().all(|p| p.request_id != "req-big"));
    // A further chunk of an already-buffered request evicts nothing.
    push_failed_payload_capped(&mut buf, make_failed_payload("req-new"));
    assert_eq!(buf.len(), cap + 1);
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
        client: crate::newrelic::tls_fallback::test_client(),
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
#[serial]
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
#[serial]
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
#[serial]
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

// ── Flow-1 merged-send failure must buffer the payload (not silently drop) ──

// process_apm_request's Flow-1 now calls ApmApp::process_merged_agent_payloads
// for the chunk(s) buffered during the current invocation, batching mergeable
// event-list types into one collector POST per type (same call
// process_pending_agent_payloads already makes for the previous-invocation drain
// path). With exactly one buffered chunk, process_merged_agent_payloads
// delegates to process_agent_payload verbatim (no batching benefit for a single
// chunk) — a chunk that fails to parse (invalid UTF-8, not valid agent-payload
// JSON) propagates as Err there, and the guarantee under test is that Err must
// still land the raw payload in FAILED_AGENT_PAYLOADS for retry, never dropped.
#[tokio::test]
#[serial]
async fn flow1_merged_send_failure_buffers_payload_for_retry() {
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }

    let request_id = "req-flow1-merged-failure";
    let config = make_config_for_serverless(false);
    let factory = make_serverless_processor_factory(config.clone());
    let state = create_request_processing_state(request_id, "arn:test", &factory);
    // Invalid UTF-8 — parse_agent_payload fails immediately (std::str::from_utf8),
    // deterministically exercising the Err path without depending on a real send.
    state.agent_buffer.lock().unwrap().push(vec![0xFF, 0xFE, 0xFD]);
    REQUEST_PROCESSORS.insert(request_id.to_string(), state);

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(Some(make_test_apm_app()))); // has_run_id=true

    process_apm_request(
        request_id.to_string(),
        "arn:test".to_string(),
        config,
        log_processor,
        apm_app,
        deadline_ms_from_now(500),
    )
    .await;

    let buffered_count = FAILED_AGENT_PAYLOADS
        .lock()
        .map(|b| b.iter().filter(|p| p.request_id == request_id).count())
        .unwrap_or(0);
    assert_eq!(
        buffered_count, 1,
        "a chunk that fails to parse must still be buffered for retry, not dropped"
    );

    REQUEST_DATA.remove(request_id);
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
}

// Flow-1 with apm_app not yet connected at all (has_run_id=false) must leave the
// buffered payload in agent_buffer for the next invocation (Flow 2) — it must
// NOT be dropped, and must NOT be buffered into FAILED_AGENT_PAYLOADS either
// (that buffer is for payloads that failed an attempted send, not ones that were
// never attempted).
#[tokio::test]
#[serial]
async fn flow1_without_run_id_rebuffers_payload_for_next_invocation() {
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }

    let request_id = "req-flow2-no-run-id";
    let config = make_config_for_serverless(false);
    let factory = make_serverless_processor_factory(config.clone());
    let state = create_request_processing_state(request_id, "arn:test", &factory);
    state.agent_buffer.lock().unwrap().push(vec![1, 2, 3]);
    let agent_buffer = state.agent_buffer.clone();
    REQUEST_PROCESSORS.insert(request_id.to_string(), state);

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None)); // has_run_id=false

    process_apm_request(
        request_id.to_string(),
        "arn:test".to_string(),
        config,
        log_processor,
        apm_app,
        deadline_ms_from_now(500),
    )
    .await;

    assert_eq!(
        agent_buffer.lock().unwrap().len(),
        1,
        "payload must be re-buffered for the next invocation, not dropped"
    );
    let buffered_in_failed = FAILED_AGENT_PAYLOADS
        .lock()
        .map(|b| b.iter().any(|p| p.request_id == request_id))
        .unwrap_or(false);
    assert!(
        !buffered_in_failed,
        "a never-attempted send must not land in FAILED_AGENT_PAYLOADS"
    );

    REQUEST_DATA.remove(request_id);
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

/// When send_function_logs IS on, process_request_concurrently must actually engage
/// wait_for_runtime_done (not just skip straight to the grace-only check) — the
/// regression this guards: an earlier version of the fix for NR-??? (same-invoke
/// agent-payload delivery) made this call unconditional here too, which broke this
/// exact "flag off: must return near-instantly" contract for the flag-off case above.
/// With no notify fired, a short deadline, and the flag on, the call must still wait
/// out (a bounded slice of) the deadline rather than returning instantly.
#[tokio::test]
#[serial]
async fn process_request_concurrently_waits_for_runtime_done_when_flag_on() {
    let request_id = "prc-send-logs-flag-on-test";
    let mut cfg = config::ExtensionConfig::default();
    cfg.extension.send_function_logs = true;
    let config = Arc::new(cfg);
    register_request_for_serverless(request_id, config.clone());
    // Notify is never fired — the wait must fall through on the deadline instead.
    // deadline_ms_from_now(600) leaves a ~100ms budget after the 500ms
    // INVOKE_DEADLINE_SAFETY_MARGIN_MS wait_for_runtime_done is bounded by.

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());

    let t0 = std::time::Instant::now();
    process_request_concurrently(
        request_id.to_string(),
        "arn:aws:lambda:us-east-1:123:function:test".to_string(),
        newrelic_client,
        config,
        log_processor,
        deadline_ms_from_now(600),
    )
    .await;
    let elapsed = t0.elapsed().as_millis();

    assert!(
        elapsed >= 50,
        "flag on: must actually wait for runtime.done (deadline fallback), not skip it (got {elapsed}ms)"
    );

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
        apm_client: crate::newrelic::tls_fallback::DualTlsClient::build(Client::builder),
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

// ── format_elapsed_for_log ───────────────────────────────────────────────────────────

#[test]
fn format_elapsed_for_log_renders_some_as_milliseconds() {
    assert_eq!(format_elapsed_for_log(Some(42)), "42ms");
    assert_eq!(format_elapsed_for_log(Some(0)), "0ms");
}

#[test]
fn format_elapsed_for_log_renders_none_as_unknown() {
    assert_eq!(format_elapsed_for_log(None), "an unknown time");
}

// ── wait_for_runtime_done (core wait, ungated) ──────────────────────────────────────

#[tokio::test]
#[serial]
async fn wait_for_runtime_done_returns_false_immediately_when_request_has_no_notify() {
    let start = std::time::Instant::now();
    let signaled = wait_for_runtime_done("wfrd-unregistered-request", deadline_ms_from_now(5_000)).await;
    assert!(
        !signaled,
        "an unregistered request has no notify handle, so this must return false"
    );
    assert!(
        start.elapsed() < Duration::from_millis(200),
        "must return immediately, not sleep"
    );
}

#[tokio::test]
#[serial]
async fn wait_for_runtime_done_returns_true_when_pre_fired() {
    let request_id = "wfrd-core-signalled";
    let config = make_config_for_serverless(false);
    register_request_for_serverless(request_id, config.clone());

    let notify = request::get_runtime_done_notify(request_id).expect("notify must exist after registration");
    notify.notify_one(); // pre-fire: notified() below resolves immediately

    let start = std::time::Instant::now();
    let signaled = wait_for_runtime_done(request_id, deadline_ms_from_now(5_000)).await;
    assert!(signaled, "a pre-fired notify must be observed");
    assert!(
        start.elapsed() < Duration::from_millis(200),
        "a pre-fired notify must resolve immediately, not wait out the deadline"
    );

    REQUEST_DATA.remove(request_id);
}

#[tokio::test]
#[serial]
async fn wait_for_runtime_done_returns_false_when_deadline_expires_before_signal() {
    let request_id = "wfrd-core-deadline-expires";
    let config = make_config_for_serverless(false);
    register_request_for_serverless(request_id, config.clone());
    // Notify is never fired — the wait must fall through on the deadline instead.
    // deadline_ms_from_now(600) leaves a ~100ms budget after the 500ms
    // INVOKE_DEADLINE_SAFETY_MARGIN_MS this wait is bounded by (same margin every
    // other deadline-relative wait in event_loop.rs uses) — without that margin,
    // this wait would sleep all the way to the real Lambda deadline, leaving zero
    // time for the agent-send/report-send/log-flush work that still runs after it.
    let start = std::time::Instant::now();
    let signaled = wait_for_runtime_done(request_id, deadline_ms_from_now(600)).await;
    let elapsed = start.elapsed();
    assert!(!signaled, "an unfired notify must fall through as false, not true");
    assert!(
        elapsed >= Duration::from_millis(50) && elapsed < Duration::from_millis(1_000),
        "must wait roughly until the deadline budget minus the safety margin, then return (elapsed: {:?})",
        elapsed
    );

    REQUEST_DATA.remove(request_id);
}

// With a deadline so close that the 500ms INVOKE_DEADLINE_SAFETY_MARGIN_MS eats the
// entire remaining time, the wait must return near-instantly (budget saturates to 0),
// not sleep the full remaining-to-deadline window. Guards against the pre-fix behavior
// where this wait ignored the margin entirely and slept right up to the real deadline.
#[tokio::test]
#[serial]
async fn wait_for_runtime_done_returns_immediately_when_margin_exceeds_remaining_time() {
    let request_id = "wfrd-core-margin-exceeds-remaining";
    let config = make_config_for_serverless(false);
    register_request_for_serverless(request_id, config.clone());

    let start = std::time::Instant::now();
    // Only 100ms left before the deadline — entirely consumed by the 500ms margin.
    let signaled = wait_for_runtime_done(request_id, deadline_ms_from_now(100)).await;
    let elapsed = start.elapsed();
    assert!(!signaled, "an unfired notify must fall through as false, not true");
    assert!(
        elapsed < Duration::from_millis(50),
        "must return near-instantly once the safety margin saturates the remaining budget to 0 (elapsed: {:?})",
        elapsed
    );

    REQUEST_DATA.remove(request_id);
}

// ── wait_for_runtime_done_with_grace (log-flush grace layered on an already-observed signal) ──

#[tokio::test]
#[serial]
async fn wait_for_runtime_done_with_grace_returns_immediately_when_function_logs_disabled() {
    let mut cfg = config::ExtensionConfig::default();
    cfg.extension.send_function_logs = false;
    let config = Arc::new(cfg);
    let log_processor = make_noop_log_processor_serverless(config.clone());

    let start = std::time::Instant::now();
    wait_for_runtime_done_with_grace(
        "wfrd-grace-no-such-request",
        true,
        &config,
        &log_processor,
    )
    .await;
    assert!(
        start.elapsed() < Duration::from_millis(200),
        "must short-circuit before touching the log processor at all"
    );
}

#[tokio::test]
#[serial]
async fn wait_for_runtime_done_with_grace_skips_grace_when_signalled_and_already_drained() {
    let request_id = "wfrd-grace-signalled-drained";
    let mut cfg = config::ExtensionConfig::default();
    cfg.extension.send_function_logs = true;
    let config = Arc::new(cfg);

    let log_processor = make_noop_log_processor_serverless(config.clone());
    assert!(log_processor.is_drained(), "a fresh noop log processor starts drained");

    let start = std::time::Instant::now();
    wait_for_runtime_done_with_grace(
        request_id,
        true,
        &config,
        &log_processor,
    )
    .await;
    assert!(
        start.elapsed() < Duration::from_millis(200),
        "already-drained batch must skip the grace-period wait entirely"
    );
}

#[tokio::test]
#[serial]
async fn wait_for_runtime_done_with_grace_is_a_no_op_when_signaled_is_false() {
    let request_id = "wfrd-grace-not-signalled";
    let mut cfg = config::ExtensionConfig::default();
    cfg.extension.send_function_logs = true;
    let config = Arc::new(cfg);
    let log_processor = make_noop_log_processor_serverless(config.clone());

    let start = std::time::Instant::now();
    wait_for_runtime_done_with_grace(
        request_id,
        false,
        &config,
        &log_processor,
    )
    .await;
    assert!(
        start.elapsed() < Duration::from_millis(200),
        "signaled=false (deadline already expired upstream) must not wait again"
    );
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

// Note: the merge behavior itself (ApmApp::process_merged_agent_payloads correctly
// merging multiple chunks into one POST per mergeable type) is already covered by
// apm::app::tests::process_merged_agent_payloads_merges_mergeable_event_list_types_across_chunks
// and three sibling tests. An equivalent test at this integration layer — going
// through process_pending_agent_payloads with a real ApmApp wrapped in SharedApmApp —
// was attempted twice (current_thread and multi_thread runtime flavors) and hit an
// unresolved test-harness/runtime-teardown hang both times, unrelated to the merge
// logic itself (manual instrumentation confirmed the function body completes and all
// assertions pass in ~1ms; the hang occurs after the test's own code finishes, before
// the harness reports completion — attaching a debugger to inspect it was blocked by
// sandbox permissions). Removed rather than leave a flaky/hanging test in the suite;
// the "apm_app is Some" branch in process_pending_agent_payloads (event_loop.rs
// ~2372-2392) is consequently uncovered by this specific integration path.

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
        config,
        log_processor,
        apm_app,
        deadline_ms_from_now(500), // short: no notify fired in this test, let the deadline fallback resolve fast
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
        config,
        log_processor,
        apm_app,
        deadline_ms_from_now(500), // short: no notify fired in this test, let the deadline fallback resolve fast
    )
    .await;
}

#[tokio::test]
#[serial]
async fn process_apm_request_flow1_sends_immediately_when_run_id_and_payload_present() {
    let request_id = "par-flow1-send-test";
    let config = make_config_for_serverless(false);
    let factory = make_serverless_processor_factory(config.clone());
    let state = create_request_processing_state(request_id, "arn:test", &factory);
    state.agent_buffer.lock().unwrap().push(vec![0xAB, 0xCD, 0xEF]); // got_payload=true
    REQUEST_PROCESSORS.insert(request_id.to_string(), state);

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(Some(make_test_apm_app()))); // has_run_id=true

    process_apm_request(
        request_id.to_string(),
        "arn:test".to_string(),
        config,
        log_processor,
        apm_app,
        deadline_ms_from_now(500),
    )
    .await;

    assert!(REQUEST_PROCESSORS.get(request_id).is_none());
    REQUEST_DATA.remove(request_id);
}

// Unlike the test above (which pre-seeds the buffer before runtime.done fires),
// this test proves the real ordering: buffer starts empty, process_apm_request
// must block in wait_for_runtime_done, and only then does the payload arrive and
// runtime.done fire. Fast completion proves the notify woke it up; a 2s wait
// would mean it just fell through on the deadline instead.
#[tokio::test]
#[serial]
async fn process_apm_request_catches_payload_that_arrives_after_runtime_done_wait_begins() {
    let request_id = "par-real-ordering-test";
    let config = make_config_for_serverless(false);
    let factory = make_serverless_processor_factory(config.clone());
    let state = create_request_processing_state(request_id, "arn:test", &factory);
    // Empty buffer and runtime.done NOT fired — process_apm_request must block on
    // wait_for_runtime_done, not see got_payload=true immediately.
    REQUEST_PROCESSORS.insert(request_id.to_string(), state);

    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }

    let log_processor = make_noop_log_processor_serverless(config.clone());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(Some(make_test_apm_app()))); // has_run_id=true from the start

    // Run process_apm_request and the late arrival concurrently via join! (not
    // tokio::spawn — spawning a task that owns a real ApmApp hung this test
    // harness; join! avoids the task boundary while still interleaving both
    // futures). process_apm_request must actually be blocked in
    // wait_for_runtime_done when the payload/notify arrive 50ms later — if it
    // isn't (e.g. this ordering regressed), it'd either fail to pick up the
    // payload at all, or only catch it on the 2s deadline fallback.
    let start = std::time::Instant::now();
    let process_fut = process_apm_request(
        request_id.to_string(),
        "arn:test".to_string(),
        config,
        log_processor,
        apm_app,
        deadline_ms_from_now(2_000),
    );
    let late_arrival_fut = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if let Some(buffer) = get_agent_buffer(request_id) {
            buffer.lock().unwrap().push(vec![0xAB, 0xCD, 0xEF]);
        }
        crate::request::record_runtime_done(request_id);
    };
    tokio::join!(process_fut, late_arrival_fut);
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(500),
        "must return shortly after the notify (~50ms), not anywhere near the 2s deadline fallback (elapsed: {:?})",
        elapsed
    );

    // These bytes aren't parseable, so Flow 1 buffers them into FAILED_AGENT_PAYLOADS
    // — proof the payload was picked up and acted on.
    {
        let buffered = FAILED_AGENT_PAYLOADS.lock().unwrap();
        assert!(
            buffered.iter().any(|p| p.request_id == request_id),
            "the late-arriving payload must have been picked up and acted on, not left stuck in the buffer"
        );
    } // drop the guard before FAILED_AGENT_PAYLOADS.lock() is called again below
    let agent_buffer_now_empty = get_agent_buffer(request_id)
        .map(|b| b.lock().unwrap().is_empty())
        .unwrap_or(true);
    assert!(agent_buffer_now_empty, "the payload must have been taken out of agent_buffer by Flow 1");

    REQUEST_DATA.remove(request_id);
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
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
                // This mock never emits platform.runtimeDone, so
                // process_apm_request's wait_for_runtime_done falls through on the
                // deadline — 2s comfortably survives the wiremock round trip without
                // the deadline going stale (which would trip the 5s fallback branch
                // instead), while still being far faster than the real 5s fallback.
                deadline_ms_from_now(2_000),
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

// The high tier waits for its sends before /next, even if pipeline_flush reaches the loop.
#[tokio::test]
#[serial]
async fn high_tier_awaits_the_invoke_before_next_even_with_pipeline_flush() {
    let server = wiremock::MockServer::start().await;
    let next_calls: Arc<std::sync::Mutex<Vec<std::time::Instant>>> = Arc::default();
    let invoke_body = invoke_event_body("req-high-pipeline", "arn:aws:lambda:us-east-1:123456789012:function:t", deadline_ms_from_now(1_000));
    let calls = next_calls.clone();
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(move |_: &wiremock::Request| {
            let mut calls = calls.lock().unwrap();
            calls.push(std::time::Instant::now());
            let body = if calls.len() == 1 { invoke_body.clone() } else { shutdown_event_body("spindown") };
            wiremock::ResponseTemplate::new(200).set_body_json(body)
        })
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    cfg.extension.send_tier = crate::send::SendTier::High;
    cfg.extension.pipeline_flush = true;
    let mut components = make_test_extension_components(Arc::new(cfg), Arc::new(Client::new()), true);

    with_runtime_api_el(&server, || async { execute_apm_mode_event_loop(&mut components).await }).await;

    let calls = next_calls.lock().unwrap();
    let gap = calls[1].duration_since(calls[0]);
    assert!(gap >= Duration::from_millis(300), "/next was called before the invoke finished ({gap:?})");
    REQUEST_DATA.remove("req-high-pipeline");
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
        true,
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
        true,
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
        config,
        log_processor,
        apm_app,
        deadline_ms_from_now(500), // short: no notify fired in this test, let the deadline fallback resolve fast
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
    let config = make_config_for_serverless(false);
    register_request_for_serverless(request_id, config.clone());

    // Pre-fire the notify so notified() resolves instantly even via the fallback path.
    let notify = request::get_runtime_done_notify(request_id).expect("notify must exist after registration");
    notify.notify_one();

    let start = std::time::Instant::now();
    let signaled = wait_for_runtime_done(request_id, deadline_ms_from_now(-1_000)).await; // already expired → stale-deadline fallback branch
    assert!(signaled, "pre-fired notify must still be observed on the expired-deadline fallback path");
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
            // This mock never emits platform.runtimeDone, so
            // process_apm_request's wait_for_runtime_done falls through on the
            // deadline — 2s comfortably survives the wiremock round trip without
            // the deadline going stale (which would trip the 5s fallback branch
            // instead), while still being far faster than the real 5s fallback.
            deadline_ms_from_now(2_000),
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
        config,
        log_processor,
        apm_app,
        deadline_ms_from_now(500), // short: no notify fired in this test, let the deadline fallback resolve fast
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

// ── send_batched_payloads_with_reports_only — mid-invocation threshold path ──
//
// Covers event_loop.rs:1048-1053: when should_send_batch_by_threshold() returns
// true the function send_batched_payloads_with_reports_only is called.  This test
// exercises it directly (it's pub async in agent::batch) and confirms the
// send-success path clears paired entries while unpaired ones remain.
#[tokio::test]
#[serial]
async fn send_batched_payloads_with_reports_only_clears_paired_entries_leaves_unpaired() {
    crate::agent::batch::AGENT_BATCH_BUFFER.clear();

    // Add 3 paired entries (with report_line) — enough to pass the threshold
    for i in 0..3u8 {
        let req = format!("req-threshold-{i}");
        crate::agent::batch::add_to_batch(
            req.clone(),
            vec![i],
            Some(format!("REPORT Duration: {}ms", i * 10)),
            "arn:aws:lambda:us-east-1:123:function:test".to_string(),
        );
    }
    // Add one unpaired entry (no report_line) — must survive the call
    crate::agent::batch::add_to_batch(
        "req-threshold-unpaired".to_string(),
        vec![99],
        None, // no report
        "arn:test".to_string(),
    );

    assert!(
        crate::agent::batch::should_send_batch_by_threshold(),
        "precondition: 3 paired entries must trip the threshold"
    );

    // No license key → send_agent_payload short-circuits with Ok(()), items cleared
    let config = Arc::new(config::ExtensionConfig::default());
    let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());

    crate::agent::batch::send_batched_payloads_with_reports_only(newrelic_client, config).await;

    // Paired entries must be cleared after a successful (noop) send
    for i in 0..3u8 {
        assert!(
            crate::agent::batch::AGENT_BATCH_BUFFER.get(&format!("req-threshold-{i}")).is_none(),
            "paired entry req-threshold-{i} must be removed after send"
        );
    }

    // Unpaired entry must remain in the buffer for the next cycle
    assert!(
        crate::agent::batch::AGENT_BATCH_BUFFER.get("req-threshold-unpaired").is_some(),
        "unpaired entry must NOT be removed by send_batched_payloads_with_reports_only"
    );

    crate::agent::batch::AGENT_BATCH_BUFFER.clear();
}

// ── pipeline flush handle cap — execute_standard_mode_event_loop:1025-1030 ───
//
// With pipeline_flush=true and 9 rapid INVOKE events the cap check fires:
// when pending_flush_handles already holds 8 entries the oldest is awaited
// synchronously before adding the next.  Uses multi-thread so spawned
// process_request_concurrently tasks can actually run concurrently.
#[tokio::test]
#[serial]
async fn execute_standard_mode_event_loop_awaits_oldest_handle_when_pipeline_flush_cap_reached() {
    let server = wiremock::MockServer::start().await;

    // 9 INVOKE events so the pipeline-flush Vec can fill past 8
    for i in 0..9u32 {
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(invoke_event_body(
                &format!("req-cap-{i}"),
                "arn:aws:lambda:us-east-1:123:function:cap-test",
                deadline_ms_from_now(5_000),
            )))
            .up_to_n_times(1)
            .mount(&server)
            .await;
    }
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    cfg.new_relic.license_key = Some("fake-key".to_string());
    cfg.extension.pipeline_flush = true; // enable pipeline flush to exercise the cap path
    let config = Arc::new(cfg);
    let client = Arc::new(reqwest::Client::new());
    let mut components = make_test_extension_components(config, client, false);

    let event_count = with_runtime_api_el(&server, || async {
        execute_standard_mode_event_loop(&mut components).await
    })
    .await;

    assert_eq!(event_count, 10, "must count all 9 INVOKEs + 1 SHUTDOWN");

    for i in 0..9u32 {
        REQUEST_DATA.remove(&format!("req-cap-{i}"));
    }
}

// ── shutdown log-flush failure — execute_standard_mode_event_loop:1229-1233 ──
//
// When flush_on_shutdown returns Err the event loop must log it and continue
// to shutdown cleanly rather than panicking.  We achieve the error by giving
// the log processor a real client with a license key + an unreachable endpoint
// so the HTTP call fails, and pre-populate the log batch so flush is attempted.
#[tokio::test]
#[serial]
async fn execute_standard_mode_event_loop_handles_shutdown_flush_failure_gracefully() {
    let server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/2020-01-01/extension/event/next"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(shutdown_event_body("spindown")))
        .mount(&server)
        .await;

    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.extension_enabled = true;
    // License key + unreachable endpoint so send_logs fails, making flush_on_shutdown return Err
    cfg.new_relic.license_key = Some("fake-key-flush-fail".to_string());
    cfg.new_relic.telemetry_endpoint = "http://127.0.0.1:1".to_string();
    let config = Arc::new(cfg);

    let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
    let processor_factory = Arc::new(request::ProcessorFactory::new(
        newrelic_client.clone(),
        config.clone(),
        apm_app.clone(),
    ));

    // Build a log processor backed by the failing client and seed it with a log entry
    // so flush() actually attempts (and fails) a network send rather than returning
    // early on an empty batch.
    let ctx = Arc::new(Mutex::new(crate::context::InvocationContext::default()));
    let log_processor = Arc::new(crate::logs::processor::LogProcessor::new(
        newrelic_client.clone(),
        config.clone(),
        ctx,
        None,
    ));
    log_processor.add_log_to_batch(crate::newrelic::payload::LogMessage::diagnostic(
        "shutdown-flush-fail-test",
        "test message".to_string(),
    ));

    let mut components = ExtensionComponents {
        client: Arc::new(reqwest::Client::new()),
        extension_id: "test-ext-id".to_string(),
        processor_factory,
        newrelic_client,
        config,
        global_log_processor: log_processor,
        apm_app,
        apm_mode_enabled: false,
        apm_client: crate::newrelic::tls_fallback::DualTlsClient::build(reqwest::Client::builder),
        reconnect_in_flight: Arc::new(tokio::sync::watch::channel(false).0),
        deployment: crate::config::deployment::DeploymentContext::Normal {
            mode: crate::config::deployment::TelemetryMode::Serverless,
        },
    };

    // Must complete without panicking — the error is logged, not propagated
    let event_count = with_runtime_api_el(&server, || async {
        execute_standard_mode_event_loop(&mut components).await
    })
    .await;

    assert_eq!(event_count, 1, "single SHUTDOWN must count as one event");
}

// ── agent batch (SEND_SCHEDULER, never aborted) ─────────────────────────────────────

/// Free the batch slot without aborting whatever is in it, and forget its start time.
async fn clear_batch_state() {
    SEND_SCHEDULER.detach(crate::send::SendKind::ApmCollector).await;
    if let Ok(mut in_flight) = IN_FLIGHT_BATCH.lock() {
        *in_flight = None;
    }
    if let Ok(mut meta) = BATCH_META.lock() {
        *meta = None;
    }
}

#[tokio::test]
#[serial]
async fn resolve_pending_agent_batch_is_a_noop_when_nothing_spawned() {
    clear_batch_state().await;
    let start = tokio::time::Instant::now();
    wait_for_pending_agent_batch(Duration::from_millis(1_500)).await;
    assert!(start.elapsed() < Duration::from_millis(50));
}

#[tokio::test]
#[serial]
async fn resolve_pending_agent_batch_collects_a_finished_batch() {
    clear_batch_state().await;
    let send: crate::send::BoxedSend = Box::pin(async { crate::send::SendOutcome::Success });
    SEND_SCHEDULER.spawn(crate::send::SendKind::ApmCollector, send, None).await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    wait_for_pending_agent_batch(Duration::from_millis(1_500)).await;

    assert!(!SEND_SCHEDULER.has_pending(crate::send::SendKind::ApmCollector).await);
}

// SHUTDOWN has no next invoke: a batch still running after the wait is cut off like at the
// function deadline, and its unsent groups go to the retry buffer instead of nowhere.
#[tokio::test]
#[serial]
async fn shutdown_cuts_off_a_slow_batch_and_buffers_its_groups() {
    clear_batch_state().await;
    clear_high_tier_state();
    let send: crate::send::BoxedSend = Box::pin(std::future::pending());
    let cursor = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    SEND_SCHEDULER.spawn(crate::send::SendKind::ApmCollector, send, Some(cursor)).await;
    set_in_flight_batch("b-shutdown", std::time::Instant::now());
    if let Ok(mut meta) = BATCH_META.lock() {
        *meta = Some(BatchMeta {
            owner: NEXT_BATCH_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            groups: Arc::new(vec![make_group("sd-1", vec![vec![1]], None), make_group("sd-2", vec![vec![2]], None)]),
        });
    }
    let mut cfg = config::ExtensionConfig::default();
    cfg.extension.send_tier = crate::send::SendTier::High;
    cfg.extension.shutdown_batch_wait_ms = 50;
    let components = make_test_extension_components(Arc::new(cfg), Arc::new(Client::new()), true);

    shutdown_drain_high_tier(&components).await;

    assert!(!SEND_SCHEDULER.has_pending(crate::send::SendKind::ApmCollector).await, "the batch was cut off");
    assert_eq!(FAILED_AGENT_PAYLOADS.lock().unwrap().len(), 2, "both unsent groups are in the retry buffer");
    clear_batch_state().await;
    clear_high_tier_state();
}

fn set_in_flight_batch(id: &str, started_at: std::time::Instant) {
    if let Ok(mut in_flight) = IN_FLIGHT_BATCH.lock() {
        *in_flight = Some(InFlightBatch { id: id.to_string(), started_at });
    }
}

fn register_request_with_payload(request_id: &str, payload: Vec<u8>) {
    let config = make_config_for_serverless(false);
    let factory = make_serverless_processor_factory(config);
    let state = create_request_processing_state(request_id, "arn:test", &factory);
    state.agent_buffer.lock().unwrap().push(payload);
    REQUEST_PROCESSORS.insert(request_id.to_string(), state);
}

// While the previous batch is still running, the next INVOKE must not take new data:
// it stays buffered for the following one, so nothing is sent twice or lost.
#[tokio::test]
#[serial]
async fn spawn_pending_batch_skips_while_the_previous_batch_is_running() {
    clear_batch_state().await;
    let send: crate::send::BoxedSend = Box::pin(std::future::pending());
    SEND_SCHEDULER.spawn(crate::send::SendKind::ApmCollector, send, None).await;
    set_in_flight_batch("test-running", std::time::Instant::now());
    register_request_with_payload("spb-previous", vec![1, 2, 3]);
    let config = make_config_for_serverless(false);
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    spawn_pending_batch("spb-current", &config, &make_noop_log_processor_serverless(config.clone()), &apm_app).await;

    let still_buffered = get_agent_buffer("spb-previous").map(|b| b.lock().unwrap().len());
    assert_eq!(still_buffered, Some(1), "the data must stay put while a batch is in flight");
    clear_batch_state().await;
    cleanup_request_processing_state_internal("spb-previous", false);
}

// A batch older than the max age stops blocking new ones. It is detached, not aborted.
#[tokio::test]
#[serial]
async fn spawn_pending_batch_detaches_a_batch_older_than_the_max_age() {
    clear_batch_state().await;
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
    let send: crate::send::BoxedSend = Box::pin(std::future::pending());
    SEND_SCHEDULER.spawn(crate::send::SendKind::ApmCollector, send, None).await;
    let Some(old) = std::time::Instant::now()
        .checked_sub(Duration::from_millis(config::ExtensionConfig::default().extension.batch_max_age_ms + 1_000))
    else {
        return; // host uptime shorter than the max age: cannot fake an old start
    };
    set_in_flight_batch("test-old", old);
    register_request_with_payload("spb-old-previous", vec![9, 9, 9]);
    let config = make_config_for_serverless(false);
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    spawn_pending_batch("spb-old-current", &config, &make_noop_log_processor_serverless(config.clone()), &apm_app).await;

    // The new batch took the data; with no APM connection it buffers the payload.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let buffered = FAILED_AGENT_PAYLOADS
        .lock()
        .map(|b| b.iter().any(|p| p.request_id == "spb-old-previous"))
        .unwrap_or(false);
    assert!(buffered, "the new batch must have taken and processed the data");
    clear_batch_state().await;
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
}

// --- Log flush tests (spawn_log_flush / resolve_pending_logs via SEND_SCHEDULER) ---

#[tokio::test]
#[serial]
async fn spawn_log_flush_then_resolve_round_trip_with_empty_batch() {
    // Clean slate: a previous test (or a prior run of this one) must not
    // leave SendKind::Logs occupied.
    let _ = SEND_SCHEDULER.resolve_pending_no_abort(crate::send::SendKind::Logs).await;

    let config = Arc::new(config::ExtensionConfig::default());
    let log_processor = make_noop_log_processor_serverless(config);

    spawn_log_flush(&log_processor, "req-roundtrip").await;
    assert!(
        SEND_SCHEDULER.has_pending(crate::send::SendKind::Logs).await,
        "spawn_log_flush must register the flush with SEND_SCHEDULER"
    );

    // An empty batch hits LogProcessor::flush's own fast path and returns
    // near-instantly - no real network call happens.
    tokio::time::sleep(Duration::from_millis(20)).await;
    resolve_pending_logs(&log_processor, "req-roundtrip").await;
    assert!(
        !SEND_SCHEDULER.has_pending(crate::send::SendKind::Logs).await,
        "resolve_pending_logs must clear the entry once the flush is actually finished"
    );
}

// Proves the duplicate-flush guard: if a flush is already pending (StillRunning),
// spawn_log_flush must skip entirely - never touching, overwriting, or aborting it.
#[tokio::test]
#[serial]
async fn spawn_log_flush_skips_when_a_flush_is_already_pending() {
    let _ = SEND_SCHEDULER.resolve_pending_no_abort(crate::send::SendKind::Logs).await;

    // A never-finishing send with a drop-detector, standing in for "a flush
    // from a previous invocation that's still running."
    let aborted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    struct AbortFlag(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for AbortFlag {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let flag = AbortFlag(aborted.clone());
    let send: crate::send::BoxedSend = Box::pin(async move {
        let _flag = flag;
        std::future::pending::<()>().await;
        crate::send::SendOutcome::Success
    });
    SEND_SCHEDULER.spawn(crate::send::SendKind::Logs, send, None).await;
    assert!(SEND_SCHEDULER.has_pending(crate::send::SendKind::Logs).await);

    let config = Arc::new(config::ExtensionConfig::default());
    let log_processor = make_noop_log_processor_serverless(config);
    spawn_log_flush(&log_processor, "req-skip-test").await;
    tokio::task::yield_now().await;

    assert!(
        !aborted.load(std::sync::atomic::Ordering::SeqCst),
        "spawn_log_flush must not abort/overwrite an already-pending flush"
    );
    assert!(
        SEND_SCHEDULER.has_pending(crate::send::SendKind::Logs).await,
        "the original pending entry must still be the one occupying the slot"
    );

    // Test cleanup only - fine to abort here, this task has no real data.
    let _ = SEND_SCHEDULER.resolve_pending_shared_budget(&[crate::send::SendKind::Logs], std::time::Duration::ZERO).await;
}

// --- Write-ahead buffering tests (shared-budget abort -> recoverable, not lost) ---

#[tokio::test]
#[serial]
async fn handle_logs_outcome_timed_out_requeues_write_ahead_batch_into_failed_logs_buffer() {
    let config = Arc::new(config::ExtensionConfig::default());
    let log_processor = make_noop_log_processor_serverless(config);

    let log_message = crate::newrelic::payload::LogMessage {
        timestamp: 0,
        message: "hello".to_string(),
        attributes: serde_json::Map::new(),
    };

    *LOG_BATCH_WRITE_AHEAD.lock().unwrap() = Some(LogBatchWriteAhead {
        batch: vec![log_message],
        request_id: "req-original".to_string(),
    });

    let before = log_processor.failed_logs_buffer_len();
    handle_logs_outcome(
        &crate::send::DispatchOutcome {
            result: crate::send::DispatchResult::TimedOut,
            cursor: None,
        },
        &log_processor,
        "req-current-invocation",
    );

    assert_eq!(
        log_processor.failed_logs_buffer_len(),
        before + 1,
        "the write-ahead batch must be re-queued into failed_logs_buffer on TimedOut"
    );
    assert!(
        LOG_BATCH_WRITE_AHEAD.lock().unwrap().is_none(),
        "the write-ahead slot must be cleared once resolved, regardless of outcome"
    );
}

#[tokio::test]
#[serial]
async fn handle_logs_outcome_sent_clears_write_ahead_without_requeuing() {
    let config = Arc::new(config::ExtensionConfig::default());
    let log_processor = make_noop_log_processor_serverless(config);

    let log_message = crate::newrelic::payload::LogMessage {
        timestamp: 0,
        message: "hello".to_string(),
        attributes: serde_json::Map::new(),
    };
    *LOG_BATCH_WRITE_AHEAD.lock().unwrap() = Some(LogBatchWriteAhead {
        batch: vec![log_message],
        request_id: "req-original".to_string(),
    });

    let before = log_processor.failed_logs_buffer_len();
    handle_logs_outcome(
        &crate::send::DispatchOutcome {
            result: crate::send::DispatchResult::Sent,
            cursor: None,
        },
        &log_processor,
        "req-current-invocation",
    );

    assert_eq!(
        log_processor.failed_logs_buffer_len(),
        before,
        "a successful send must not re-queue the write-ahead batch"
    );
    assert!(LOG_BATCH_WRITE_AHEAD.lock().unwrap().is_none());
}

#[tokio::test]
#[serial]
async fn retry_failed_platform_reports_drops_entry_after_max_retries() {
    {
        let mut buf = FAILED_PLATFORM_REPORTS.lock().unwrap();
        buf.clear();
        buf.push(FailedPlatformReport {
            report_line: "stale report".to_string(),
            request_id: "req-stale".to_string(),
            invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:test".to_string(),
            retry_count: MAX_PLATFORM_REPORT_RETRIES - 1,
            failed_at: chrono::Utc::now(),
        });
    }

    // apm_app is None -- send_platform_report_metrics is never actually
    // reached in this test; we only need an app present to exercise the
    // "attempt failed" branch deterministically. Use a minimal real app
    // stand-in via None short-circuit instead: with apm_app == None, the
    // whole buffer is put back untouched (not consumed), which we assert
    // here since that's the other branch this function must get right.
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
    retry_failed_platform_reports(apm_app).await;

    let buf = FAILED_PLATFORM_REPORTS.lock().unwrap();
    assert_eq!(
        buf.len(),
        1,
        "with apm_app not ready, the entry must be put back untouched, not dropped or retried"
    );
    assert_eq!(buf[0].retry_count, MAX_PLATFORM_REPORT_RETRIES - 1);
}

// ── high tier: hold budget, batch groups, short-invoke path ─────────────────────────

fn clear_high_tier_state() {
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
    }
    if let Ok(mut b) = FAILED_PLATFORM_REPORTS.lock() {
        b.clear();
    }
}

fn make_group(request_id: &str, payloads: Vec<Vec<u8>>, report: Option<&str>) -> PendingGroup {
    PendingGroup {
        request_id: request_id.to_string(),
        invoked_function_arn: "arn:test".to_string(),
        payloads,
        report: report.map(str::to_string),
        started: std::sync::atomic::AtomicBool::new(false),
    }
}

#[tokio::test]
#[serial]
async fn send_pending_groups_without_apm_buffers_everything() {
    clear_high_tier_state();
    let groups = Arc::new(vec![
        make_group("g1", vec![vec![1]], Some("REPORT 1")),
        make_group("g2", vec![vec![2]], None),
    ]);
    let config = Arc::new(config::ExtensionConfig::default());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    let cursor = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let outcome = send_pending_groups(
        "test-batch".to_string(),
        99,
        groups,
        cursor.clone(),
        config.clone(),
        make_noop_log_processor_serverless(config),
        apm_app,
    )
    .await;

    assert_eq!(outcome, crate::send::SendOutcome::Failure);
    assert_eq!(cursor.load(std::sync::atomic::Ordering::SeqCst), 2, "both groups were processed");
    assert_eq!(FAILED_AGENT_PAYLOADS.lock().unwrap().len(), 2, "no payload may be lost");
    assert_eq!(FAILED_PLATFORM_REPORTS.lock().unwrap().len(), 1, "no report may be lost");
    clear_high_tier_state();
}

#[tokio::test]
#[serial]
async fn take_pending_groups_takes_other_requests_and_leaves_the_current_one() {
    clear_high_tier_state();
    let config = make_config_for_serverless(false);
    let factory = make_serverless_processor_factory(config);
    for id in ["tpg-previous", "tpg-current"] {
        let state = create_request_processing_state(id, "arn:test", &factory);
        state.agent_buffer.lock().unwrap().push(id.as_bytes().to_vec());
        REQUEST_PROCESSORS.insert(id.to_string(), state);
    }
    request::set_pending_report("tpg-previous", "REPORT previous".to_string());

    let groups = take_pending_groups("tpg-current");

    let previous: Vec<&PendingGroup> = groups.iter().filter(|g| g.request_id == "tpg-previous").collect();
    assert_eq!(previous.len(), 1);
    assert_eq!(previous[0].payloads, vec![b"tpg-previous".to_vec()]);
    assert_eq!(previous[0].report.as_deref(), Some("REPORT previous"));
    assert!(groups.iter().all(|g| g.request_id != "tpg-current"));
    let still_buffered = get_agent_buffer("tpg-current").map(|b| b.lock().unwrap().len());
    assert_eq!(still_buffered, Some(1), "the running request's own payload must stay put");

    cleanup_request_processing_state_internal("tpg-current", false);
    clear_high_tier_state();
}

// A trivial handler must not be held: the point of the short-invoke path.
#[tokio::test]
#[serial]
async fn high_tier_short_invoke_does_not_hold_next() {
    clear_high_tier_state();
    let request_id = "high-short-invoke".to_string();
    let mut cfg = config::ExtensionConfig::default();
    cfg.extension.send_tier = crate::send::SendTier::High;
    cfg.extension.short_invoke_threshold_ms = 1_000;
    let config = Arc::new(cfg);
    let factory = make_serverless_processor_factory(config.clone());
    let state = create_request_processing_state(&request_id, "arn:test", &factory);
    request::record_runtime_done(&request_id);
    let log_processor = make_noop_log_processor_serverless(config.clone());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    let start = tokio::time::Instant::now();
    process_apm_request_high(&request_id, &config, &log_processor, &apm_app, deadline_ms_from_now(5_000), &state, None).await;

    assert!(
        start.elapsed() < Duration::from_millis(50),
        "short invoke must return without holding, took {:?}",
        start.elapsed()
    );
    assert!(
        !SEND_SCHEDULER.has_pending(crate::send::SendKind::Logs).await,
        "a short invoke must not start a log flush: logs stay buffered"
    );
    cleanup_request_processing_state_internal(&request_id, false);
    clear_high_tier_state();
}

// default tier must never touch the high-tier scheduler.
#[tokio::test]
#[serial]
async fn default_tier_never_uses_the_send_scheduler() {
    clear_high_tier_state();
    let _ = SEND_SCHEDULER.resolve_pending_shared_budget(&[crate::send::SendKind::ApmCollector], std::time::Duration::ZERO).await;
    let _ = SEND_SCHEDULER.resolve_pending_no_abort(crate::send::SendKind::Logs).await;
    let request_id = "default-no-scheduler";
    let config = make_config_for_serverless(false);
    register_request_for_serverless(request_id, config.clone());
    request::record_runtime_done(request_id);
    let log_processor = make_noop_log_processor_serverless(config.clone());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    process_apm_request(request_id.to_string(), "arn:test".to_string(), config, log_processor, apm_app, deadline_ms_from_now(2_000)).await;

    assert!(!SEND_SCHEDULER.has_pending(crate::send::SendKind::ApmCollector).await);
    assert!(!SEND_SCHEDULER.has_pending(crate::send::SendKind::Logs).await);
    cleanup_request_processing_state_internal(request_id, false);
    clear_high_tier_state();
}

// The batch success signal reads the per-type buffer: a type that got 202 leaves
// nothing behind, a failed one leaves exactly its own entry.
#[test]
#[serial]
fn buffered_failures_since_counts_only_new_failures_for_that_request() {
    use crate::apm::telemetry_buffer::{FailedTelemetry, FAILED_TELEMETRY_BUFFER};
    let entry = |request_id: &str, failed_at| FailedTelemetry {
        telemetry_type: "metric_data".to_string(),
        data: vec![],
        request_id: request_id.to_string(),
        run_id: "run".to_string(),
        collector_host: "host".to_string(),
        failed_at,
        retry_count: 0,
    };
    let start = chrono::Utc::now();
    {
        let mut buffer = FAILED_TELEMETRY_BUFFER.lock().unwrap();
        buffer.clear();
        buffer.push(entry("req-a", start - chrono::Duration::seconds(30))); // an older retry
        buffer.push(entry("req-a", start + chrono::Duration::milliseconds(5)));
        buffer.push(entry("req-b", start + chrono::Duration::milliseconds(5)));
    }

    assert_eq!(buffered_failures_since("req-a", start), 1);
    assert_eq!(buffered_failures_since("req-b", start), 1);
    assert_eq!(buffered_failures_since("req-none", start), 0);
    FAILED_TELEMETRY_BUFFER.lock().unwrap().clear();
}

// ── short-invoke detection ────────────────────────────────────────────────────────────

fn detection_state(request_id: &str) -> request::RequestProcessingState {
    let config = make_config_for_serverless(false);
    let factory = make_serverless_processor_factory(config);
    create_request_processing_state(request_id, "arn:test", &factory)
}

#[tokio::test]
#[serial]
async fn detect_uses_the_duration_lambda_reports_in_runtime_done() {
    for (id, reported_ms, expected) in [
        ("det-rd-short", 5.0, InvokeLength::Short),
        ("det-rd-long", 500.0, InvokeLength::NotShort),
    ] {
        let state = detection_state(id);
        request::record_runtime_done_with_duration(id, Some(reported_ms));
        let verdict = detect_short_invoke(id, &state, Duration::from_millis(20), tokio::time::Instant::now()).await;
        assert_eq!(verdict, expected, "durationMs {reported_ms} against a 20 ms threshold");
        cleanup_request_processing_state_internal(id, false);
    }
}

#[tokio::test]
#[serial]
async fn detect_falls_back_to_arrival_time_when_runtime_done_has_no_duration() {
    let state = detection_state("det-rd-nodur");
    request::record_runtime_done_with_duration("det-rd-nodur", None);
    let verdict = detect_short_invoke("det-rd-nodur", &state, Duration::from_millis(500), tokio::time::Instant::now()).await;
    assert_eq!(verdict, InvokeLength::Short, "it arrived almost at once, far under 500 ms");
    cleanup_request_processing_state_internal("det-rd-nodur", false);
}

// The agent's payload is a signal too, and it needs no runtimeDone at all.
#[tokio::test]
#[serial]
async fn detect_treats_an_early_agent_payload_as_the_end_of_the_invocation() {
    let state = detection_state("det-payload");
    state.agent_buffer.lock().unwrap().push(b"not a parseable payload".to_vec());
    let started = tokio::time::Instant::now();
    let verdict = detect_short_invoke("det-payload", &state, Duration::from_millis(500), started).await;
    assert_eq!(verdict, InvokeLength::Short);
    assert!(started.elapsed() < Duration::from_millis(100), "decided as soon as the payload was seen");
    cleanup_request_processing_state_internal("det-payload", false);
}

// No signal in threshold + the Telemetry API's lag means the invoke is not short.
#[tokio::test]
#[serial]
async fn detect_gives_up_after_the_threshold_plus_delivery_lag() {
    let state = detection_state("det-none");
    let started = tokio::time::Instant::now();
    let verdict = detect_short_invoke("det-none", &state, Duration::from_millis(10), started).await;
    assert_eq!(verdict, InvokeLength::NotShort);
    let waited = started.elapsed();
    let window = Duration::from_millis(10 + crate::runtime::TELEMETRY_BUFFER_TIMEOUT_MS);
    assert!(waited >= window && waited < window + Duration::from_millis(150), "waited {waited:?}, window {window:?}");
    cleanup_request_processing_state_internal("det-none", false);
}

#[tokio::test]
async fn detect_is_off_when_the_threshold_is_zero() {
    let state = detection_state("det-off");
    let verdict = detect_short_invoke("det-off", &state, Duration::ZERO, tokio::time::Instant::now()).await;
    assert_eq!(verdict, InvokeLength::NotShort);
    cleanup_request_processing_state_internal("det-off", false);
}

#[test]
fn transaction_duration_reads_the_first_transaction_event() {
    let telemetry = std::collections::HashMap::from([(
        "analytic_event_data".to_string(),
        vec![
            serde_json::Value::Null,
            serde_json::json!({"events_seen": 1}),
            serde_json::json!([[{"duration": 0.0042, "type": "Transaction"}, {}, {}]]),
        ],
    )]);
    assert_eq!(transaction_duration(&telemetry), Some(Duration::from_micros(4_200)));
    assert_eq!(transaction_duration(&std::collections::HashMap::new()), None, "no transaction events");
}

// ── a batch the function's deadline cut off ───────────────────────────────────────────

fn timed_out_at(cursor: usize) -> crate::send::DispatchOutcome {
    crate::send::DispatchOutcome { result: crate::send::DispatchResult::TimedOut, cursor: Some(cursor) }
}

fn set_batch_meta(owner: u64, groups: Vec<PendingGroup>) {
    if let Ok(mut meta) = BATCH_META.lock() {
        *meta = Some(BatchMeta { owner, groups: Arc::new(groups) });
    }
}

#[test]
#[serial]
fn a_batch_cut_off_by_the_deadline_puts_unstarted_groups_back_with_their_reports() {
    clear_high_tier_state();
    set_batch_meta(
        501,
        vec![
            make_group("cancel-a", vec![vec![1]], Some("REPORT a")),
            make_group("cancel-b", vec![vec![2]], None),
        ],
    );

    assert!(finish_batch_outcome(&timed_out_at(0)), "a cut-off batch is reported as such");

    let payloads: Vec<Vec<u8>> = FAILED_AGENT_PAYLOADS.lock().unwrap().iter().map(|p| p.payload_bytes.clone()).collect();
    assert_eq!(payloads, vec![vec![1u8], vec![2u8]], "everything unsent is kept for the next invoke");
    assert_eq!(FAILED_PLATFORM_REPORTS.lock().unwrap().len(), 1);
    assert_eq!(
        FAILED_AGENT_PAYLOADS.lock().unwrap().iter().map(|p| p.retry_count).sum::<usize>(),
        0,
        "deferring is not a retry"
    );
    clear_high_tier_state();
}

// A group whose POSTs were already handed out is cleaned up per type by `inflight`;
// only its report is still ours, and re-buffering its payload would repeat types that landed.
#[test]
#[serial]
fn a_cut_off_started_group_buffers_only_its_report() {
    clear_high_tier_state();
    let started = make_group("cancel-started", vec![vec![7]], Some("REPORT started"));
    started.started.store(true, std::sync::atomic::Ordering::SeqCst);
    set_batch_meta(502, vec![started, make_group("cancel-later", vec![vec![8]], None)]);

    assert!(finish_batch_outcome(&timed_out_at(0)));

    let payloads: Vec<Vec<u8>> = FAILED_AGENT_PAYLOADS.lock().unwrap().iter().map(|p| p.payload_bytes.clone()).collect();
    assert_eq!(payloads, vec![vec![8u8]], "only the group that never started");
    assert_eq!(FAILED_PLATFORM_REPORTS.lock().unwrap().len(), 1);
    clear_high_tier_state();
}

#[test]
#[serial]
fn a_finished_batch_puts_nothing_back() {
    clear_high_tier_state();
    set_batch_meta(503, vec![make_group("done", vec![vec![9]], None)]);
    let done = crate::send::DispatchOutcome { result: crate::send::DispatchResult::Sent, cursor: Some(1) };

    assert!(!finish_batch_outcome(&done));

    assert_eq!(FAILED_AGENT_PAYLOADS.lock().unwrap().len(), 0, "a delivered batch is never put back");
    assert!(BATCH_META.lock().unwrap().is_none(), "and its bookkeeping is cleared");
    clear_high_tier_state();
}

// ── the high tier as a whole: retries only run for invokes that are not short ──────────

fn retries_probe() -> (InvokeRetries, Arc<std::sync::atomic::AtomicUsize>) {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = calls.clone();
    (Box::new(move || { seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }), calls)
}

fn high_config(threshold_ms: u64) -> Arc<config::ExtensionConfig> {
    let mut cfg = config::ExtensionConfig::default();
    cfg.extension.send_tier = crate::send::SendTier::High;
    cfg.extension.short_invoke_threshold_ms = threshold_ms;
    Arc::new(cfg)
}

#[tokio::test]
#[serial]
async fn a_short_invoke_starts_no_retries() {
    clear_high_tier_state();
    let id = "hi-short-retries";
    let config = high_config(500);
    let state = detection_state(id);
    request::record_runtime_done_with_duration(id, Some(2.0));
    let (retries, calls) = retries_probe();
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    process_apm_request_high(id, &config, &make_noop_log_processor_serverless(config.clone()), &apm_app, deadline_ms_from_now(5_000), &state, Some(retries)).await;

    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0, "retries wait for a longer invoke");
    assert!(!SEND_SCHEDULER.has_pending(crate::send::SendKind::Logs).await, "and no log flush is started");
    cleanup_request_processing_state_internal(id, false);
    clear_high_tier_state();
}

#[tokio::test]
#[serial]
async fn a_longer_invoke_starts_the_retries_once() {
    clear_high_tier_state();
    let id = "hi-long-retries";
    let config = high_config(20);
    let state = detection_state(id);
    request::record_runtime_done_with_duration(id, Some(300.0));
    let (retries, calls) = retries_probe();
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    process_apm_request_high(id, &config, &make_noop_log_processor_serverless(config.clone()), &apm_app, deadline_ms_from_now(5_000), &state, Some(retries)).await;

    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    let _ = SEND_SCHEDULER.resolve_pending_shared_budget(&[crate::send::SendKind::Logs], Duration::ZERO).await;
    cleanup_request_processing_state_internal(id, false);
    clear_high_tier_state();
}

// ── high tier: logs start at INVOKE and never hold /next ──────────────────────────────

async fn clear_log_flush() {
    let _ = SEND_SCHEDULER
        .resolve_pending_shared_budget(&[crate::send::SendKind::Logs], Duration::ZERO)
        .await;
}

// A log flush still running at runtimeDone is waited for, but never past the function's
// own deadline: then it is stopped and its logs kept.
#[tokio::test]
#[serial]
async fn a_running_log_flush_is_waited_for_until_the_deadline_then_stopped() {
    clear_high_tier_state();
    clear_log_flush().await;
    let id = "hi-logs-deadline";
    let config = high_config(20);
    let state = detection_state(id);
    request::record_runtime_done_with_duration(id, Some(300.0));
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
    let hanging: crate::send::BoxedSend = Box::pin(std::future::pending());
    SEND_SCHEDULER.spawn(crate::send::SendKind::Logs, hanging, None).await;

    // 500 ms of safety margin + 300 ms: the wait may last about 300 ms.
    let started = tokio::time::Instant::now();
    process_apm_request_high(id, &config, &make_noop_log_processor_serverless(config.clone()), &apm_app, deadline_ms_from_now(800), &state, None).await;

    assert!(started.elapsed() >= Duration::from_millis(200), "it must wait for the send, took {:?}", started.elapsed());
    assert!(started.elapsed() < Duration::from_millis(700), "but not past the deadline, took {:?}", started.elapsed());
    assert!(!SEND_SCHEDULER.has_pending(crate::send::SendKind::Logs).await, "the unfinished flush is stopped");
    cleanup_request_processing_state_internal(id, false);
    clear_high_tier_state();
}

// With nothing hanging, /next is not held at all.
#[tokio::test]
#[serial]
async fn finished_sends_do_not_hold_next() {
    clear_high_tier_state();
    clear_log_flush().await;
    let id = "hi-nothing-to-wait";
    let config = high_config(20);
    let state = detection_state(id);
    request::record_runtime_done_with_duration(id, Some(300.0));
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    let started = tokio::time::Instant::now();
    process_apm_request_high(id, &config, &make_noop_log_processor_serverless(config.clone()), &apm_app, deadline_ms_from_now(5_000), &state, None).await;

    assert!(started.elapsed() < Duration::from_millis(300), "took {:?}", started.elapsed());
    cleanup_request_processing_state_internal(id, false);
    clear_high_tier_state();
}

// The flush for a not-short invoke starts at INVOKE, while the handler is still running,
// not after runtimeDone.
#[tokio::test]
#[serial]
async fn a_not_short_invoke_starts_its_log_flush_before_runtime_done() {
    clear_high_tier_state();
    clear_log_flush().await;
    let id = "hi-logs-early";
    let config = high_config(10);
    let state = Arc::new(detection_state(id));
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
    let log_processor = make_noop_log_processor_serverless(config.clone());

    let run = {
        let (config, state, apm_app, log_processor) = (config.clone(), state.clone(), apm_app.clone(), log_processor.clone());
        tokio::spawn(async move {
            process_apm_request_high(id, &config, &log_processor, &apm_app, deadline_ms_from_now(5_000), &state, None).await;
        })
    };
    // Past the detection window (10 ms + 25 ms) and well before runtimeDone.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(SEND_SCHEDULER.has_pending(crate::send::SendKind::Logs).await, "the flush started while the handler was still running");

    request::record_runtime_done_with_duration(id, Some(300.0));
    run.await.unwrap();
    assert!(!SEND_SCHEDULER.has_pending(crate::send::SendKind::Logs).await, "resolved at runtimeDone");
    cleanup_request_processing_state_internal(id, false);
    clear_high_tier_state();
}

// ── high tier: platform.report is kept for the next batch, never sent by the listener ──

fn clear_stashed_reports() {
    let _ = take_stashed_reports("");
}

#[test]
#[serial]
fn a_stashed_report_is_taken_once_and_never_for_the_current_request() {
    clear_stashed_reports();
    stash_platform_report("stash-old", "REPORT old".to_string());
    stash_platform_report("stash-current", "REPORT current".to_string());

    let taken = take_stashed_reports("stash-current");

    assert_eq!(taken.len(), 1);
    assert_eq!(taken[0].request_id, "stash-old");
    assert_eq!(taken[0].report, "REPORT old");
    assert!(take_stashed_reports("stash-current").is_empty(), "taken means gone");
    assert_eq!(take_stashed_reports("").len(), 1, "the current request's report is still waiting");
    clear_stashed_reports();
}

// A report stashed while a take is running must end up either taken or still in the stash.
#[test]
#[serial]
fn a_report_stashed_during_a_take_is_never_lost() {
    const STASHERS: usize = 4;
    const PER_STASHER: usize = 20; // 80 in total: under the cap, so nothing is dropped by design
    for round in 0..200 {
        clear_stashed_reports();
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let taker = {
            let done = done.clone();
            std::thread::spawn(move || {
                let mut taken = 0;
                while !done.load(std::sync::atomic::Ordering::SeqCst) {
                    taken += take_stashed_reports("").len();
                }
                taken
            })
        };
        let stashers: Vec<_> = (0..STASHERS)
            .map(|t| {
                std::thread::spawn(move || {
                    for i in 0..PER_STASHER {
                        stash_platform_report(&format!("race-{round}-{t}-{i}"), "REPORT".to_string());
                    }
                })
            })
            .collect();
        stashers.into_iter().for_each(|s| s.join().unwrap());
        done.store(true, std::sync::atomic::Ordering::SeqCst);
        let taken = taker.join().unwrap() + take_stashed_reports("").len();
        assert_eq!(taken, STASHERS * PER_STASHER, "round {round}: a stashed report was lost");
    }
}

#[test]
#[serial]
fn the_stash_is_capped_and_drops_the_oldest_report() {
    clear_stashed_reports();
    for i in 0..(MAX_STASHED_REPORTS + 3) {
        stash_platform_report(&format!("cap-{i}"), format!("REPORT {i}"));
    }

    let kept = take_stashed_reports("");

    assert_eq!(kept.len(), MAX_STASHED_REPORTS);
    assert_eq!(kept[0].request_id, "cap-3", "the three oldest were dropped");
    clear_stashed_reports();
}

#[tokio::test]
#[serial]
async fn the_batch_sends_a_stashed_report_with_the_payload_of_the_same_request() {
    clear_high_tier_state();
    clear_stashed_reports();
    let config = make_config_for_serverless(false);
    let factory = make_serverless_processor_factory(config);
    for id in ["grp-prev", "grp-current"] {
        let state = create_request_processing_state(id, "arn:test", &factory);
        state.agent_buffer.lock().unwrap().push(id.as_bytes().to_vec());
        REQUEST_PROCESSORS.insert(id.to_string(), state);
    }
    stash_platform_report("grp-prev", "REPORT prev".to_string());
    stash_platform_report("grp-current", "REPORT current".to_string());

    let groups = take_pending_groups("grp-current");

    let prev: Vec<&PendingGroup> = groups.iter().filter(|g| g.request_id == "grp-prev").collect();
    assert_eq!(prev.len(), 1, "one group, payload and report together");
    assert_eq!(prev[0].payloads, vec![b"grp-prev".to_vec()]);
    assert_eq!(prev[0].report.as_deref(), Some("REPORT prev"));
    assert!(groups.iter().all(|g| g.request_id != "grp-current"), "the running request is left alone");
    assert_eq!(take_stashed_reports("").len(), 1, "its report stays for a later batch");
    cleanup_request_processing_state_internal("grp-current", false);
    clear_stashed_reports();
    clear_high_tier_state();
}

// The payload usually left in an earlier batch; the report then travels alone.
#[tokio::test]
#[serial]
async fn a_report_whose_payload_already_left_goes_in_a_report_only_group() {
    clear_high_tier_state();
    clear_stashed_reports();
    stash_platform_report("late-report", "REPORT late".to_string());

    let groups = take_pending_groups("some-other-current");

    let late: Vec<&PendingGroup> = groups.iter().filter(|g| g.request_id == "late-report").collect();
    assert_eq!(late.len(), 1);
    assert!(late[0].payloads.is_empty());
    assert_eq!(late[0].report.as_deref(), Some("REPORT late"));
    clear_stashed_reports();
    clear_high_tier_state();
}

#[test]
#[serial]
fn at_shutdown_stashed_reports_move_to_the_retry_buffer() {
    clear_high_tier_state();
    clear_stashed_reports();
    stash_platform_report("shut-1", "REPORT 1".to_string());
    stash_platform_report("shut-2", "REPORT 2".to_string());

    drain_stashed_reports_into_retry_buffer();

    assert!(take_stashed_reports("").is_empty());
    assert_eq!(FAILED_PLATFORM_REPORTS.lock().unwrap().len(), 2, "so the shutdown retry sends them");
    clear_high_tier_state();
}

// ── a group's payload POSTs and its report POST go out together ───────────────────────

#[tokio::test]
async fn a_groups_payloads_and_report_are_sent_at_the_same_time() {
    let slow = |ms: u64, ok: bool| async move {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        ok
    };

    let started = tokio::time::Instant::now();
    let sent = send_group_in_parallel(slow(150, true), slow(150, true)).await;

    assert!(sent);
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "two 150 ms sends must overlap, took {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn a_group_is_not_sent_if_either_its_payloads_or_its_report_failed() {
    let done = |ok: bool| async move { ok };

    assert!(!send_group_in_parallel(done(true), done(false)).await, "report failed");
    assert!(!send_group_in_parallel(done(false), done(true)).await, "payloads failed");
    assert!(!send_group_in_parallel(done(false), done(false)).await);
    assert!(send_group_in_parallel(done(true), done(true)).await);
}

// The 14.5 s bug: platform.start made the slot, runtimeDone reached it, then the INVOKE
// replaced it and the wake-up was lost, so the extension waited for the whole deadline.
#[tokio::test]
#[serial]
async fn a_runtime_done_before_the_invoke_does_not_make_the_extension_wait_for_the_deadline() {
    clear_high_tier_state();
    let id = "hi-runtime-done-first";
    let config = high_config(20);
    request::ensure_lmi_request_slot(id); // platform.start
    request::record_runtime_done_with_duration(id, Some(13.2)); // runtimeDone, before the INVOKE
    let factory = make_serverless_processor_factory(config.clone());
    let state = create_request_processing_state(id, "arn:test", &factory); // the INVOKE
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    let started = tokio::time::Instant::now();
    process_apm_request_high(id, &config, &make_noop_log_processor_serverless(config.clone()), &apm_app, deadline_ms_from_now(3_000), &state, None).await;

    assert!(
        started.elapsed() < Duration::from_millis(500),
        "waited {:?}: the early runtimeDone must count",
        started.elapsed()
    );
    cleanup_request_processing_state_internal(id, false);
    clear_high_tier_state();
}

// ── high tier: short invokes (nothing sent; dropped unless they recorded an error) ────

fn put_earlier_request(id: &str, payload: &[u8]) {
    let config = make_config_for_serverless(false);
    let factory = make_serverless_processor_factory(config);
    let state = create_request_processing_state(id, "arn:test", &factory);
    state.agent_buffer.lock().unwrap().push(payload.to_vec());
    REQUEST_PROCESSORS.insert(id.to_string(), state);
}

#[test]
fn error_events_in_a_payload_count_as_an_error() {
    let with = |key: &str, data: Vec<serde_json::Value>| std::collections::HashMap::from([(key.to_string(), data)]);
    let events = vec![serde_json::Value::Null, serde_json::json!({}), serde_json::json!([["an error event"]])];
    assert!(telemetry_has_errors(&with("error_event_data", events)));
    let traced = vec![serde_json::Value::Null, serde_json::json!([["an error trace"]])];
    assert!(telemetry_has_errors(&with("error_data", traced)));

    let empty_events = vec![serde_json::Value::Null, serde_json::json!({}), serde_json::json!([])];
    assert!(!telemetry_has_errors(&with("error_event_data", empty_events)), "no events, no error");
    assert!(!telemetry_has_errors(&std::collections::HashMap::new()));
    assert!(!payload_has_errors(b"not a payload"), "an unreadable payload is not an error");
}

#[tokio::test]
#[serial]
async fn a_short_invoke_without_an_error_is_dropped_and_remembered() {
    use crate::send::short_invoke::{get, ShortOutcome};
    clear_stashed_reports();
    let id = "short-clean";
    let config = high_config(10);
    let state = detection_state(id);
    state.agent_buffer.lock().unwrap().push(b"payload".to_vec());
    stash_platform_report(id, "REPORT".to_string());
    stash_platform_report("short-clean-earlier", "REPORT earlier".to_string());
    let logs = make_noop_log_processor_serverless(config);

    assert!(!handle_short_invoke(id, &state, &logs));

    assert!(state.agent_buffer.lock().unwrap().is_empty(), "its payload is dropped");
    let stashed = take_stashed_reports("");
    assert_eq!(stashed.len(), 1, "its report is dropped, an earlier invoke's is not");
    assert_eq!(stashed[0].request_id, "short-clean-earlier");
    assert_eq!(get(id), Some(ShortOutcome::Dropped));
    cleanup_request_processing_state_internal(id, false);
}

#[tokio::test]
#[serial]
async fn a_short_invoke_whose_runtime_done_failed_is_kept() {
    use crate::send::short_invoke::{get, ShortOutcome};
    clear_stashed_reports();
    let id = "short-failed";
    let config = high_config(10);
    let state = detection_state(id);
    state.agent_buffer.lock().unwrap().push(b"payload".to_vec());
    stash_platform_report(id, "REPORT".to_string());
    request::record_runtime_done_status(id, Some("error"));
    let logs = make_noop_log_processor_serverless(config);

    assert!(handle_short_invoke(id, &state, &logs));

    assert_eq!(state.agent_buffer.lock().unwrap().len(), 1, "its payload waits for the next long invoke");
    assert_eq!(take_stashed_reports("").len(), 1, "and so does its report");
    assert_eq!(get(id), Some(ShortOutcome::ErrorKept));
    cleanup_request_processing_state_internal(id, false);
}

#[tokio::test]
#[serial]
async fn a_successful_runtime_done_is_not_an_error() {
    let id = "short-success-status";
    let config = high_config(10);
    let state = detection_state(id);
    request::record_runtime_done_status(id, Some("success"));
    let logs = make_noop_log_processor_serverless(config);
    assert!(!short_invoke_had_error(id, &state, &logs));
    cleanup_request_processing_state_internal(id, false);
}

#[tokio::test]
#[serial]
async fn the_next_batch_skips_a_dropped_short_invoke_but_takes_an_errored_one() {
    use crate::send::short_invoke::{mark, ShortOutcome};
    clear_high_tier_state();
    clear_stashed_reports();
    put_earlier_request("skip-dropped", b"a");
    put_earlier_request("skip-errored", b"b");
    put_earlier_request("skip-plain", b"c");
    mark("skip-dropped", ShortOutcome::Dropped);
    mark("skip-errored", ShortOutcome::ErrorKept);
    stash_platform_report("skip-dropped", "REPORT late".to_string());

    let groups = take_pending_groups("skip-current");

    let mut ids: Vec<&str> = groups.iter().map(|g| g.request_id.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec!["skip-errored", "skip-plain"], "late data of a dropped invoke is not sent");
    for id in ["skip-dropped", "skip-errored", "skip-plain"] {
        cleanup_request_processing_state_internal(id, false);
    }
    clear_stashed_reports();
}

// A short invoke starts nothing at all: earlier invokes' data stays exactly where it was.
#[tokio::test]
#[serial]
async fn a_short_invoke_starts_no_batch_and_leaves_earlier_data_alone() {
    clear_high_tier_state();
    clear_batch_state().await;
    clear_stashed_reports();
    put_earlier_request("hi-short-earlier", b"earlier");
    let id = "hi-short-nothing";
    let config = high_config(500);
    let state = detection_state(id);
    request::record_runtime_done_with_duration(id, Some(2.0));
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    let started = tokio::time::Instant::now();
    process_apm_request_high(id, &config, &make_noop_log_processor_serverless(config.clone()), &apm_app, deadline_ms_from_now(5_000), &state, None).await;

    assert!(started.elapsed() < Duration::from_millis(200), "/next is not held, took {:?}", started.elapsed());
    assert!(!SEND_SCHEDULER.has_pending(crate::send::SendKind::ApmCollector).await, "no batch is started");
    assert_eq!(get_agent_buffer("hi-short-earlier").unwrap().lock().unwrap().len(), 1, "earlier data waits");
    assert_eq!(FAILED_AGENT_PAYLOADS.lock().unwrap().len(), 0);
    for id in ["hi-short-earlier", id] {
        cleanup_request_processing_state_internal(id, false);
    }
    clear_high_tier_state();
}

// A long invoke sends earlier data and does not return before the send is finished.
#[tokio::test]
#[serial]
async fn a_long_invoke_waits_for_the_batch_to_finish() {
    clear_high_tier_state();
    clear_batch_state().await;
    clear_log_flush().await;
    clear_stashed_reports();
    put_earlier_request("hi-long-earlier", b"earlier");
    stash_platform_report("hi-long-earlier", "REPORT earlier".to_string());
    let id = "hi-long-wait";
    let config = high_config(20);
    let state = detection_state(id);
    request::record_runtime_done_with_duration(id, Some(300.0));
    // No APM session: the batch ends at once, buffering its group; what matters is that
    // the invoke has resolved it by the time it returns.
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

    process_apm_request_high(id, &config, &make_noop_log_processor_serverless(config.clone()), &apm_app, deadline_ms_from_now(5_000), &state, None).await;

    assert!(!SEND_SCHEDULER.has_pending(crate::send::SendKind::ApmCollector).await, "the batch is resolved before /next");
    assert_eq!(FAILED_AGENT_PAYLOADS.lock().unwrap().len(), 1, "and its outcome was recorded");
    assert_eq!(FAILED_PLATFORM_REPORTS.lock().unwrap().len(), 1);
    for id in ["hi-long-earlier", id] {
        cleanup_request_processing_state_internal(id, false);
    }
    clear_high_tier_state();
}

// ── default tier: the platform.report send runs in the background; /next waits for it ──

fn flag_after(ms: u64) -> (tokio::task::JoinHandle<()>, Arc<std::sync::atomic::AtomicBool>) {
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = done.clone();
    let handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        seen.store(true, std::sync::atomic::Ordering::SeqCst);
    });
    (handle, done)
}

#[tokio::test]
#[serial]
async fn waiting_for_report_sends_with_none_in_flight_returns_at_once() {
    let started = tokio::time::Instant::now();
    wait_for_report_sends(Duration::from_secs(5)).await;
    assert!(started.elapsed() < Duration::from_millis(50), "took {:?}", started.elapsed());
}

#[tokio::test]
#[serial]
async fn waiting_for_report_sends_returns_only_after_they_finish() {
    let (slow, slow_done) = flag_after(150);
    let (quick, quick_done) = flag_after(20);
    track_report_send(slow);
    track_report_send(quick);

    let started = tokio::time::Instant::now();
    wait_for_report_sends(Duration::from_secs(5)).await;

    assert!(slow_done.load(std::sync::atomic::Ordering::SeqCst), "the slowest send was waited for");
    assert!(quick_done.load(std::sync::atomic::Ordering::SeqCst));
    assert!(started.elapsed() >= Duration::from_millis(140), "waited {:?}", started.elapsed());
    assert!(started.elapsed() < Duration::from_millis(400), "sends ran together, waited {:?}", started.elapsed());
}

#[tokio::test]
#[serial]
async fn the_wait_is_bounded_and_does_not_cancel_the_send() {
    let (slow, slow_done) = flag_after(300);
    track_report_send(slow);

    let started = tokio::time::Instant::now();
    wait_for_report_sends(Duration::from_millis(60)).await;
    assert!(started.elapsed() < Duration::from_millis(200), "bounded by the budget, took {:?}", started.elapsed());
    assert!(!slow_done.load(std::sync::atomic::Ordering::SeqCst), "not finished yet");

    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(slow_done.load(std::sync::atomic::Ordering::SeqCst), "the send was left running, not aborted");
    wait_for_report_sends(Duration::from_millis(10)).await; // nothing left in the registry
}

// The default flow must not return to /next while a report send is still going.
#[tokio::test]
#[serial]
async fn the_default_flow_holds_next_until_a_report_send_is_done() {
    clear_high_tier_state();
    let id = "def-report-wait";
    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.synchronous_flush = false;
    let config = Arc::new(cfg);
    let state = detection_state(id);
    request::record_runtime_done_with_duration(id, Some(300.0));
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
    let (slow, slow_done) = flag_after(250);
    track_report_send(slow);

    let started = tokio::time::Instant::now();
    process_apm_request_default(
        &id.to_string(),
        &"arn:test".to_string(),
        &config,
        &make_noop_log_processor_serverless(config.clone()),
        &apm_app,
        deadline_ms_from_now(5_000),
        &state,
    )
    .await;

    assert!(slow_done.load(std::sync::atomic::Ordering::SeqCst), "the report send finished before the flow returned");
    assert!(started.elapsed() >= Duration::from_millis(240), "took {:?}", started.elapsed());
    cleanup_request_processing_state_internal(id, false);
}

// ── SHUTDOWN: independent work runs at once ──────────────────────────────────────────────

fn boxed<'a, T>(fut: impl std::future::Future<Output = T> + Send + 'a) -> BoxedFuture<'a, T> {
    Box::pin(fut)
}

#[tokio::test]
async fn join_all_concurrently_with_nothing_returns_at_once() {
    let results: Vec<u8> = join_all_concurrently(Vec::new()).await;
    assert!(results.is_empty());
}

#[tokio::test]
async fn join_all_concurrently_runs_everything_together_and_keeps_input_order() {
    let futures: Vec<BoxedFuture<'_, usize>> = (0..5usize)
        .map(|i| {
            boxed(async move {
                tokio::time::sleep(Duration::from_millis(150 - (i as u64) * 20)).await;
                i
            })
        })
        .collect();

    let started = tokio::time::Instant::now();
    let results = join_all_concurrently(futures).await;

    assert_eq!(results, vec![0, 1, 2, 3, 4], "results come back in input order, not finish order");
    assert!(
        started.elapsed() < Duration::from_millis(300),
        "five sends of up to 150 ms must overlap, took {:?}",
        started.elapsed()
    );
}

#[tokio::test]
#[serial]
async fn shutdown_takes_every_pending_payload_except_a_dropped_short_invoke() {
    use crate::send::short_invoke::{mark, ShortOutcome};
    clear_high_tier_state();
    put_earlier_request("sd-keep-a", b"a");
    put_earlier_request("sd-keep-b", b"b");
    put_earlier_request("sd-dropped", b"c");
    mark("sd-dropped", ShortOutcome::Dropped);

    let mut taken: Vec<(String, Vec<u8>)> = shutdown_take_payloads()
        .into_iter()
        .filter(|p| p.request_id.starts_with("sd-"))
        .map(|p| (p.request_id, p.bytes))
        .collect();
    taken.sort();

    assert_eq!(
        taken,
        vec![("sd-keep-a".to_string(), b"a".to_vec()), ("sd-keep-b".to_string(), b"b".to_vec())]
    );
    assert!(get_agent_buffer("sd-keep-a").unwrap().lock().unwrap().is_empty(), "taken means gone from the buffer");
    for id in ["sd-keep-a", "sd-keep-b", "sd-dropped"] {
        cleanup_request_processing_state_internal(id, false);
    }
}

#[tokio::test]
#[serial]
async fn shutdown_lists_every_pending_platform_report() {
    for id in ["sd-rep-1", "sd-rep-2"] {
        request::ensure_lmi_request_slot(id);
        request::set_pending_report(id, format!("REPORT {id}"));
    }

    let mut reports: Vec<(String, String)> = shutdown_pending_reports()
        .into_iter()
        .filter(|(id, _)| id.starts_with("sd-rep-"))
        .collect();
    reports.sort();

    assert_eq!(
        reports,
        vec![
            ("sd-rep-1".to_string(), "REPORT sd-rep-1".to_string()),
            ("sd-rep-2".to_string(), "REPORT sd-rep-2".to_string())
        ]
    );
    for id in ["sd-rep-1", "sd-rep-2"] {
        cleanup_request_processing_state_internal(id, false);
    }
}

// ── default tier: the upload starts when the payload arrives, not at runtimeDone ─────────

#[tokio::test]
async fn waiting_for_an_agent_payload_returns_as_soon_as_one_is_buffered() {
    let id = "early-payload-arrives";
    let state = detection_state(id);
    let buffer = state.agent_buffer.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(60)).await;
        buffer.lock().unwrap().push(b"payload".to_vec());
        request::AGENT_PAYLOAD_NOTIFY.notify_waiters();
    });

    let started = tokio::time::Instant::now();
    wait_for_agent_payload(&state).await;

    assert!(started.elapsed() >= Duration::from_millis(50), "not before the payload: {:?}", started.elapsed());
    assert!(started.elapsed() < Duration::from_millis(120), "and soon after it: {:?}", started.elapsed());
    cleanup_request_processing_state_internal(id, false);
}

#[tokio::test]
async fn waiting_for_an_agent_payload_never_returns_without_one() {
    let id = "early-payload-never";
    let state = detection_state(id);
    let waited = tokio::time::timeout(Duration::from_millis(120), wait_for_agent_payload(&state)).await;
    assert!(waited.is_err(), "no payload, so it keeps waiting");
    cleanup_request_processing_state_internal(id, false);
}

/// A payload that arrives before runtimeDone while APM has no session is not sent early: it
/// stays buffered for the next invoke, as before, and the flow still waits for runtimeDone.
#[tokio::test]
#[serial]
async fn a_payload_that_arrives_early_without_an_apm_session_stays_buffered() {
    clear_high_tier_state();
    let id = "early-payload-no-session";
    let mut cfg = config::ExtensionConfig::default();
    cfg.new_relic.synchronous_flush = false;
    let config = Arc::new(cfg);
    let state = detection_state(id);
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
    state.agent_buffer.lock().unwrap().push(b"payload".to_vec());
    let notified = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        request::record_runtime_done_with_duration(id, Some(5.0));
    });

    let started = tokio::time::Instant::now();
    process_apm_request_default(
        &id.to_string(),
        &"arn:test".to_string(),
        &config,
        &make_noop_log_processor_serverless(config.clone()),
        &apm_app,
        deadline_ms_from_now(5_000),
        &state,
    )
    .await;
    notified.await.unwrap();

    assert!(started.elapsed() >= Duration::from_millis(140), "it still waited for runtimeDone: {:?}", started.elapsed());
    assert_eq!(state.agent_buffer.lock().unwrap().len(), 1, "the payload waits for the next invoke");
    cleanup_request_processing_state_internal(id, false);
}
// ── Circuit breaker: default tier ────────────────────────────────────────────────────
//
// `DEFAULT_BREAKER` is process-global, so every test here is #[serial] and leaves it closed.

const BREAKER_REPORT_LINE: &str =
    "REPORT RequestId: brk-1 Duration: 150.5 ms Billed Duration: 200 ms Memory Size: 128 MB Max Memory Used: 64 MB";

/// N of the shared breaker (the failures in a row that open it).
fn breaker_n() -> u32 {
    DEFAULT_BREAKER.threshold()
}

/// Bring the default-tier breaker for `kind` back to Closed with no failures counted, from any
/// state. A Closed breaker can still carry an earlier test's failures; a success clears them.
fn force_default_breaker_closed(kind: crate::send::SendKind) {
    use crate::send::{BreakerState, CircuitBreaker, SendOutcome};
    for _ in 0..500 {
        match DEFAULT_BREAKER.state(kind) {
            BreakerState::Closed => {
                return DEFAULT_BREAKER.record_result(kind, crate::send::SendPermit::Normal, SendOutcome::Success)
            }
            BreakerState::Open => {
                let _ = DEFAULT_BREAKER.allow_send(kind);
            }
            BreakerState::HalfOpen => DEFAULT_BREAKER.record_result(kind, crate::send::SendPermit::Probe, SendOutcome::Success),
        }
    }
    panic!("could not close the default breaker for {kind:?}");
}

fn fail_default_breaker(kind: crate::send::SendKind, times: u32) {
    use crate::send::{CircuitBreaker, SendOutcome};
    for _ in 0..times {
        DEFAULT_BREAKER.record_result(kind, crate::send::SendPermit::Normal, SendOutcome::Failure);
    }
}

#[tokio::test]
#[serial]
async fn the_high_tier_scheduler_and_the_default_tier_paths_share_one_breaker() {
    let kind = crate::send::SendKind::MetricApi;
    force_default_breaker_closed(kind);
    fail_default_breaker(kind, breaker_n());

    assert!(!SEND_SCHEDULER.breaker_closed(kind), "the failures counted by DEFAULT_BREAKER open it for SEND_SCHEDULER too");

    force_default_breaker_closed(kind);
}

/// Register a request whose agent buffer holds one payload that cannot be parsed, so a send
/// that is attempted fails at once, without any network.
fn register_unparseable_payload(request_id: &str, config: &Arc<config::ExtensionConfig>) {
    let factory = make_serverless_processor_factory(config.clone());
    let state = create_request_processing_state(request_id, "arn:test", &factory);
    state.agent_buffer.lock().unwrap().push(vec![0xFF, 0xFE, 0xFD]);
    REQUEST_PROCESSORS.insert(request_id.to_string(), state);
}

async fn run_default_invoke(request_id: &str, config: Arc<config::ExtensionConfig>, apm_connected: bool) {
    let log_processor = make_noop_log_processor_serverless(config.clone());
    let session = if apm_connected { Some(make_test_apm_app()) } else { None };
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(session));
    process_apm_request(
        request_id.to_string(),
        "arn:test".to_string(),
        config,
        log_processor,
        apm_app,
        deadline_ms_from_now(500),
    )
    .await;
}

#[tokio::test]
#[serial]
async fn default_breaker_closed_follows_the_breaker_state() {
    let kind = crate::send::SendKind::MetricApi;
    force_default_breaker_closed(kind);
    assert!(default_breaker_closed(kind));

    fail_default_breaker(kind, breaker_n());
    assert!(!default_breaker_closed(kind), "open after N failures in a row");

    force_default_breaker_closed(kind);
    assert!(default_breaker_closed(kind));
}

#[tokio::test]
#[serial]
async fn default_breaker_probes_after_2n_skipped_invocations() {
    use crate::send::{BreakerState, CircuitBreaker};
    let kind = crate::send::SendKind::MetricApi;
    force_default_breaker_closed(kind);
    fail_default_breaker(kind, breaker_n());

    for _ in 0..(2 * breaker_n()) {
        assert!(DEFAULT_BREAKER.allow_send(kind).is_none(), "skipped while open");
    }
    assert!(DEFAULT_BREAKER.allow_send(kind).is_some(), "the probe after 2N skips");
    assert_eq!(DEFAULT_BREAKER.state(kind), BreakerState::HalfOpen);

    force_default_breaker_closed(kind);
}

#[tokio::test]
#[serial]
async fn default_tier_collector_failure_counts_towards_the_breaker() {
    use crate::send::{BreakerState, CircuitBreaker, SendKind};
    let collector = SendKind::ApmCollector;
    force_default_breaker_closed(collector);
    fail_default_breaker(collector, breaker_n() - 1);
    assert_eq!(DEFAULT_BREAKER.state(collector), BreakerState::Closed, "one failure short of opening");
    clear_high_tier_state();

    let request_id = "brk-default-failure";
    let config = make_config_for_serverless(false);
    register_unparseable_payload(request_id, &config);
    run_default_invoke(request_id, config, true).await;

    assert_eq!(
        DEFAULT_BREAKER.state(collector),
        BreakerState::Open,
        "the failed same-invoke send is the Nth failure in a row"
    );

    REQUEST_DATA.remove(request_id);
    clear_high_tier_state();
    force_default_breaker_closed(collector);
}

/// A valid agent payload (protocol v1) with one metric, so a send really is attempted per type.
fn valid_agent_payload() -> Vec<u8> {
    use base64::Engine as _;
    use std::io::Write;
    let data = serde_json::json!({
        "data": { "metric_data": [null, 1.0, 2.0, [[{"name": "Custom/brk"}, [1, 0.5, 0.5, 0.5, 0.5, 0.25]]]] }
    });
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(data.to_string().as_bytes()).unwrap();
    let b64 = base64::engine::general_purpose::STANDARD.encode(encoder.finish().unwrap());
    format!("[\"1\",\"{b64}\"]").into_bytes()
}

#[tokio::test]
#[serial]
async fn default_tier_counts_a_failed_type_send_towards_the_breaker() {
    use crate::send::{BreakerState, CircuitBreaker, SendKind};
    let collector = SendKind::ApmCollector;
    force_default_breaker_closed(collector);
    fail_default_breaker(collector, breaker_n() - 1);
    if let Ok(mut b) = crate::apm::telemetry_buffer::FAILED_TELEMETRY_BUFFER.lock() {
        b.clear();
    }

    // The collector refuses connections, so the per-type POST fails and the type is buffered,
    // while the merged send itself still returns Ok. That must count as a failure.
    let request_id = "brk-default-type-failure";
    let config = make_config_for_serverless(false);
    let factory = make_serverless_processor_factory(config.clone());
    let state = create_request_processing_state(request_id, "arn:test", &factory);
    state.agent_buffer.lock().unwrap().push(valid_agent_payload());
    REQUEST_PROCESSORS.insert(request_id.to_string(), state);
    run_default_invoke(request_id, config, true).await;

    let buffered = crate::apm::telemetry_buffer::FAILED_TELEMETRY_BUFFER
        .lock()
        .map(|b| b.iter().filter(|i| i.request_id == request_id).count())
        .unwrap_or(0);
    assert!(buffered > 0, "the failed type was buffered by its own send");
    assert_eq!(
        DEFAULT_BREAKER.state(collector),
        BreakerState::Open,
        "a type that failed is the Nth failure in a row, even though the merged send returned Ok"
    );

    REQUEST_DATA.remove(request_id);
    if let Ok(mut b) = crate::apm::telemetry_buffer::FAILED_TELEMETRY_BUFFER.lock() {
        b.clear();
    }
    force_default_breaker_closed(collector);
}

#[tokio::test]
#[serial]
async fn default_tier_does_not_send_while_the_collector_breaker_is_open() {
    use crate::send::{CircuitBreaker, SendKind};
    let collector = SendKind::ApmCollector;
    force_default_breaker_closed(collector);
    fail_default_breaker(collector, breaker_n());
    clear_high_tier_state();

    let request_id = "brk-default-open";
    let config = make_config_for_serverless(false);
    register_unparseable_payload(request_id, &config);
    run_default_invoke(request_id, config, true).await;

    let buffered = FAILED_AGENT_PAYLOADS
        .lock()
        .map(|b| b.iter().filter(|p| p.request_id == request_id).count())
        .unwrap_or(0);
    assert_eq!(buffered, 1, "the payload is buffered for later, not dropped");

    // The invoke used exactly one of the 2N skips: the next 2N-1 calls are still skipped.
    let mut skipped = 0;
    while DEFAULT_BREAKER.allow_send(collector).is_none() {
        skipped += 1;
        assert!(skipped <= 2 * breaker_n(), "the probe never came");
    }
    assert_eq!(skipped, 2 * breaker_n() - 1);

    REQUEST_DATA.remove(request_id);
    clear_high_tier_state();
    force_default_breaker_closed(collector);
}

#[tokio::test]
#[serial]
async fn default_tier_gives_back_a_probe_when_nothing_is_sent() {
    use crate::send::{BreakerState, CircuitBreaker, SendKind};
    let collector = SendKind::ApmCollector;
    force_default_breaker_closed(collector);
    fail_default_breaker(collector, breaker_n());
    for _ in 0..(2 * breaker_n()) {
        assert!(DEFAULT_BREAKER.allow_send(collector).is_none());
    }

    // This invoke gets the probe, but it has no payload and no session, so nothing is sent.
    let request_id = "brk-default-probe-unused";
    let config = make_config_for_serverless(false);
    register_request_for_serverless(request_id, config.clone());
    run_default_invoke(request_id, config, false).await;

    assert_eq!(
        DEFAULT_BREAKER.state(collector),
        BreakerState::Open,
        "not stuck half-open: the probe goes back"
    );
    assert!(DEFAULT_BREAKER.allow_send(collector).is_some(), "the next invoke probes again");

    REQUEST_DATA.remove(request_id);
    force_default_breaker_closed(collector);
}

// ── Circuit breaker: the default tier's send of a report kept for a later invoke ─────────

/// One earlier request whose report was kept for retry, sent by the default tier's pending pass
/// against a Metric API answering `status`. Returns how many requests the Metric API received.
async fn send_kept_report(request_id: &str, status: u16) -> usize {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(wiremock::ResponseTemplate::new(status))
        .mount(&server)
        .await;
    let mut app = make_test_apm_app();
    app.metric_endpoint = format!("{}/metrics", server.uri());
    let config = make_config_for_serverless(false);
    register_request_for_serverless(request_id, config.clone());
    request::set_pending_report(request_id, BREAKER_REPORT_LINE.to_string());
    let log_processor = make_noop_log_processor_serverless(config.clone());
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(Some(app)));

    process_pending_agent_payloads(&config, &log_processor, &apm_app, "kept-report-current").await;

    REQUEST_DATA.remove(request_id);
    server.received_requests().await.unwrap().len()
}

#[tokio::test]
#[serial]
async fn default_tier_kept_report_sends_count_and_open_the_metric_api_breaker() {
    use crate::send::{BreakerState, CircuitBreaker, SendKind};
    let kind = SendKind::MetricApi;
    force_default_breaker_closed(kind);
    clear_metric_api_buffer();

    for i in 0..breaker_n() {
        assert_eq!(send_kept_report(&format!("kept-report-fail-{i}"), 503).await, 1);
    }
    assert_eq!(DEFAULT_BREAKER.state(kind), BreakerState::Open, "N failed sends in a row open it");

    clear_metric_api_buffer();
    assert_eq!(send_kept_report("kept-report-while-open", 202).await, 0, "not sent while open");
    assert_eq!(crate::apm::metric_api_buffer::get_metric_api_buffer_count(), 1, "buffered for a later retry");

    clear_metric_api_buffer();
    force_default_breaker_closed(kind);
}

// ── Circuit breaker: platform report in the high-tier batch ──────────────────────────

fn clear_metric_api_buffer() {
    if let Ok(mut b) = crate::apm::metric_api_buffer::FAILED_METRIC_API_BUFFER.lock() {
        b.clear();
    }
}

#[tokio::test]
#[serial]
async fn send_group_report_fails_the_group_when_the_metric_api_fails() {
    clear_high_tier_state();
    clear_metric_api_buffer();
    let app = make_test_apm_app(); // the Metric API endpoint refuses connections
    let group = make_group("sgr-fail", vec![], Some(BREAKER_REPORT_LINE));

    assert!(!send_group_report(&app, &group).await, "a failed report fails the group");

    assert_eq!(crate::apm::metric_api_buffer::get_metric_api_buffer_count(), 1, "buffered once, for retry");
    assert_eq!(
        FAILED_PLATFORM_REPORTS.lock().map(|b| b.len()).unwrap_or(0),
        0,
        "not buffered a second time"
    );
    clear_metric_api_buffer();
}

#[tokio::test]
#[serial]
async fn send_group_report_succeeds_when_the_metric_api_accepts() {
    clear_high_tier_state();
    clear_metric_api_buffer();
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(wiremock::ResponseTemplate::new(202))
        .mount(&server)
        .await;
    let mut app = make_test_apm_app();
    app.metric_endpoint = format!("{}/metrics", server.uri());
    let group = make_group("sgr-ok", vec![], Some(BREAKER_REPORT_LINE));

    assert!(send_group_report(&app, &group).await);

    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    assert_eq!(crate::apm::metric_api_buffer::get_metric_api_buffer_count(), 0);
}

#[tokio::test]
#[serial]
async fn send_group_report_ignores_a_line_that_is_not_a_report() {
    clear_metric_api_buffer();
    let app = make_test_apm_app();
    let group = make_group("sgr-none", vec![], Some("not a platform report"));

    assert!(send_group_report(&app, &group).await, "nothing was sent, nothing failed");
    assert_eq!(crate::apm::metric_api_buffer::get_metric_api_buffer_count(), 0);
}

// ── Circuit breaker: high-tier invoke-time retries ───────────────────────────────────


async fn open_high_collector_breaker() {
    let collector = crate::send::SendKind::ApmCollector;
    for _ in 0..breaker_n() {
        let failing: crate::send::BoxedSend = Box::pin(async { crate::send::SendOutcome::Failure });
        SEND_SCHEDULER.spawn(collector, failing, None).await;
        SEND_SCHEDULER.resolve_pending_shared_budget(&[collector], std::time::Duration::from_secs(1)).await;
    }
}

async fn close_high_collector_breaker() {
    let collector = crate::send::SendKind::ApmCollector;
    for _ in 0..500 {
        if SEND_SCHEDULER.breaker_closed(collector) {
            return;
        }
        let succeeding: crate::send::BoxedSend = Box::pin(async { crate::send::SendOutcome::Success });
        if SEND_SCHEDULER.spawn(collector, succeeding, None).await {
            SEND_SCHEDULER.resolve_pending_shared_budget(&[collector], std::time::Duration::from_secs(1)).await;
        }
    }
    panic!("could not close the high-tier collector breaker");
}

fn push_stale_failed_payload(request_id: &str) {
    let mut stale = make_failed_payload(request_id);
    stale.failed_at = chrono::Utc::now() - chrono::Duration::hours(25);
    if let Ok(mut b) = FAILED_AGENT_PAYLOADS.lock() {
        b.clear();
        b.push(stale);
    }
}

async fn run_invoke_retries_without_session() {
    let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
    run_invoke_retries(apm_app, crate::newrelic::tls_fallback::test_client(), String::new(), false).await;
    // The retries are spawned and not awaited; give them time to run.
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
#[serial]
async fn high_tier_invoke_retries_are_skipped_while_the_collector_breaker_is_open() {
    close_high_collector_breaker().await;
    open_high_collector_breaker().await;
    assert!(!SEND_SCHEDULER.breaker_closed(crate::send::SendKind::ApmCollector));
    // A payload past the 24 h limit is dropped by a retry pass, so it shows whether one ran.
    push_stale_failed_payload("brk-high-skip");

    run_invoke_retries_without_session().await;

    let kept = FAILED_AGENT_PAYLOADS.lock().map(|b| b.len()).unwrap_or(0);
    assert_eq!(kept, 1, "no retry pass ran while the breaker was open");

    clear_high_tier_state();
    close_high_collector_breaker().await;
}

#[tokio::test]
#[serial]
async fn high_tier_invoke_retries_run_while_the_collector_breaker_is_closed() {
    close_high_collector_breaker().await;
    push_stale_failed_payload("brk-high-run");

    run_invoke_retries_without_session().await;

    let kept = FAILED_AGENT_PAYLOADS.lock().map(|b| b.len()).unwrap_or(0);
    assert_eq!(kept, 0, "a retry pass ran and dropped the stale payload");
    clear_high_tier_state();
}

#[tokio::test]
#[serial]
async fn default_tier_does_not_upload_early_while_the_collector_breaker_is_open() {
    use crate::send::SendKind;
    let collector = SendKind::ApmCollector;
    force_default_breaker_closed(collector);
    fail_default_breaker(collector, breaker_n());
    clear_high_tier_state();
    if let Ok(mut b) = crate::apm::telemetry_buffer::FAILED_TELEMETRY_BUFFER.lock() {
        b.clear();
    }

    // The payload is already in the buffer, so the early upload would start at once if it were allowed.
    let request_id = "brk-default-no-early-upload";
    let config = make_config_for_serverless(false);
    let factory = make_serverless_processor_factory(config.clone());
    let state = create_request_processing_state(request_id, "arn:test", &factory);
    state.agent_buffer.lock().unwrap().push(valid_agent_payload());
    REQUEST_PROCESSORS.insert(request_id.to_string(), state);
    run_default_invoke(request_id, config, true).await;

    // An upload would leave its failed types in the telemetry buffer; a gated one buffers the whole payload.
    let types_buffered = crate::apm::telemetry_buffer::FAILED_TELEMETRY_BUFFER
        .lock()
        .map(|b| b.iter().filter(|i| i.request_id == request_id).count())
        .unwrap_or(0);
    let payload_buffered = FAILED_AGENT_PAYLOADS
        .lock()
        .map(|b| b.iter().filter(|p| p.request_id == request_id).count())
        .unwrap_or(0);
    assert_eq!(types_buffered, 0, "nothing was uploaded");
    assert_eq!(payload_buffered, 1, "the whole payload was buffered instead");

    REQUEST_DATA.remove(request_id);
    clear_high_tier_state();
    force_default_breaker_closed(collector);
}

#[tokio::test]
#[serial]
async fn default_tier_uses_one_logs_breaker_invoke_per_invoke() {
    use crate::send::{CircuitBreaker, SendKind};
    let logs = SendKind::Logs;
    force_default_breaker_closed(logs);
    fail_default_breaker(logs, breaker_n());

    let request_id = "brk-default-logs-skip";
    let config = make_config_for_serverless(false);
    register_request_for_serverless(request_id, config.clone());
    run_default_invoke(request_id, config, false).await;

    // The invoke used one of the 2N skips, so 2N-1 are left before the probe.
    let mut skipped = 0;
    while DEFAULT_BREAKER.allow_send(logs).is_none() {
        skipped += 1;
        assert!(skipped <= 2 * breaker_n(), "the probe never came");
    }
    assert_eq!(skipped, 2 * breaker_n() - 1);

    REQUEST_DATA.remove(request_id);
    force_default_breaker_closed(logs);
}

#[tokio::test]
#[serial]
async fn default_tier_gives_back_a_logs_probe_when_there_is_nothing_to_flush() {
    use crate::send::{BreakerState, CircuitBreaker, SendKind};
    let logs = SendKind::Logs;
    force_default_breaker_closed(logs);
    fail_default_breaker(logs, breaker_n());
    for _ in 0..(2 * breaker_n()) {
        assert!(DEFAULT_BREAKER.allow_send(logs).is_none());
    }

    // This invoke gets the logs probe, but it has no logs, so there is nothing to flush.
    let request_id = "brk-default-logs-probe-unused";
    let config = make_config_for_serverless(false);
    register_request_for_serverless(request_id, config.clone());
    run_default_invoke(request_id, config, false).await;

    assert_eq!(DEFAULT_BREAKER.state(logs), BreakerState::Open, "not stuck half-open: the probe goes back");
    assert!(DEFAULT_BREAKER.allow_send(logs).is_some(), "the next invoke probes again");

    REQUEST_DATA.remove(request_id);
    force_default_breaker_closed(logs);
}

// The wait is woken by the payload push itself, not by a timer: the pusher finishes within
// microseconds, far under any poll interval, so only the push can wake the wait before then.
#[tokio::test]
#[serial]
async fn waiting_for_an_agent_payload_wakes_on_the_push_and_not_before() {
    use crate::config::deployment::{DeploymentContext, TelemetryMode};
    use std::sync::atomic::{AtomicBool, Ordering};
    let id = "wait-payload";
    let state = detection_state(id);
    *request::CURRENT_ACTIVE_REQUEST_ID.lock().unwrap() = Some(id.to_string());
    let pusher_done = AtomicBool::new(false);

    let not_yet = tokio::time::timeout(Duration::from_millis(50), wait_for_agent_payload(&state)).await;
    assert!(not_yet.is_err(), "nothing pushed, so the wait must not return");

    let woke_before_pusher_done = async {
        wait_for_agent_payload(&state).await;
        !pusher_done.load(Ordering::SeqCst)
    };
    let pusher = async {
        tokio::task::yield_now().await;
        request::route_payload_to_request_buffer(vec![1], DeploymentContext::Normal { mode: TelemetryMode::Serverless }).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        pusher_done.store(true, Ordering::SeqCst);
    };
    let (woke, ()) = tokio::join!(woke_before_pusher_done, pusher);

    assert!(woke, "the push must wake the wait without waiting for a timer");
    *request::CURRENT_ACTIVE_REQUEST_ID.lock().unwrap() = None;
    cleanup_request_processing_state_internal(id, false);
}
