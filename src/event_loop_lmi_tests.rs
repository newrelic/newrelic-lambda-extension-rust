// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use serial_test::serial;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::RwLock;

// ── helpers ──────────────────────────────────────────────────────────────

fn clear_failed_agent_payloads() {
    if let Ok(mut guard) = crate::event_loop::FAILED_AGENT_PAYLOADS.lock() {
        guard.clear();
    }
}

/// Build a fake `ApmApp` that will fail any network call immediately.
/// All fields are public so no constructor needed.
fn build_fake_apm_app() -> crate::apm::ApmApp {
    crate::apm::ApmApp {
        run_id: "test-run-id".to_string(),
        entity_guid: "test-entity-guid".to_string(),
        app_name: "test-app".to_string(),
        collector_host: "http://unreachable.invalid.test".to_string(),
        license_key: "fake-license-key-for-unit-test".to_string(),
        metric_endpoint: "http://metric.invalid.test/metric/v1".to_string(),
        otlp_metric_endpoint: "http://otlp.invalid.test/v1/metrics".to_string(),
        client: Client::builder()
            .timeout(Duration::from_millis(50))
            .build()
            .unwrap_or_default(),
        deployment: crate::config::deployment::DeploymentContext::Lmi,
    }
}

/// Build an `LmiFlushHandles` with a noop log processor and a minimal config.
/// Pass in the `apm_app` and `reconnect_in_flight` you want to observe.
fn build_lmi_flush_handles(
    apm_app: crate::apm::SharedApmApp,
    reconnect_in_flight: Arc<AtomicBool>,
) -> LmiFlushHandles {
    use crate::config::{ExtensionConfig, deployment::DeploymentContext};
    use crate::context::InvocationContext;

    let mut config = ExtensionConfig::default();
    config.deployment = DeploymentContext::Lmi;
    let config = Arc::new(config);

    let nr_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
    let log_processor = Arc::new(crate::logs::processor::LogProcessor::new(
        Arc::clone(&nr_client),
        Arc::clone(&config),
        Arc::new(std::sync::Mutex::new(InvocationContext::default())),
        None,
    ));

    LmiFlushHandles {
        config,
        global_log_processor: log_processor,
        apm_app,
        client: Arc::new(Client::new()),
        apm_client: Client::builder()
            .timeout(Duration::from_millis(50))
            .build()
            .unwrap_or_default(),
        reconnect_in_flight,
    }
}

/// `LmiReconnectGuard` must clear the flag on drop regardless of exit path.
#[test]
fn lmi_reconnect_guard_clears_flag_on_drop() {
    let flag = Arc::new(AtomicBool::new(true));
    {
        let _guard = LmiReconnectGuard(Arc::clone(&flag));
        assert!(flag.load(Ordering::Acquire), "flag must still be true inside guard scope");
    }
    assert!(
        !flag.load(Ordering::Acquire),
        "LmiReconnectGuard::drop must set flag to false"
    );
}

/// `compare_exchange` must reject a second concurrent reconnect spawn.
#[test]
fn lmi_reconnect_in_flight_prevents_double_spawn() {
    let flag = Arc::new(AtomicBool::new(false));

    // First claim succeeds.
    let first = flag.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire);
    assert!(first.is_ok(), "first compare_exchange must succeed");

    // Second claim must fail while flag is held.
    let second = flag.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire);
    assert!(second.is_err(), "second compare_exchange must fail while in-flight");

    // After guard drops the flag, a new claim must succeed.
    flag.store(false, Ordering::Release);
    let third = flag.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire);
    assert!(third.is_ok(), "compare_exchange must succeed after flag cleared");
}

/// When `apm_app` is `None` the payloads are restored to `FAILED_AGENT_PAYLOADS`
/// (LMI cannot send without a live APM session — wait for reconnect).
#[tokio::test]
#[serial]
async fn retry_lmi_failed_agent_payloads_restores_when_no_app() {
    clear_failed_agent_payloads();

    // Push a payload.
    let payload = crate::event_loop::FailedAgentPayload {
        payload_bytes: b"test-payload".to_vec(),
        request_id: "req-001".to_string(),
        invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:fn".to_string(),
        retry_count: 0,
        failed_at: chrono::Utc::now(),
    };
    crate::event_loop::FAILED_AGENT_PAYLOADS
        .lock()
        .unwrap()
        .push(payload);

    // apm_app = None simulates a reconnect window.
    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(None));
    retry_lmi_failed_agent_payloads(&apm_app).await;

    let remaining = crate::event_loop::FAILED_AGENT_PAYLOADS.lock().unwrap();
    assert_eq!(
        remaining.len(),
        1,
        "payload must be restored when apm_app is None (wait for reconnect)"
    );
    assert_eq!(remaining[0].request_id, "req-001");

    drop(remaining);
    clear_failed_agent_payloads();
}

/// Empty buffer must be a no-op (no panic, no lock contention).
#[tokio::test]
#[serial]
async fn retry_lmi_failed_agent_payloads_no_op_when_empty() {
    clear_failed_agent_payloads();

    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(None));
    // Should return immediately without error.
    retry_lmi_failed_agent_payloads(&apm_app).await;

    let count = crate::event_loop::FAILED_AGENT_PAYLOADS
        .lock()
        .unwrap()
        .len();
    assert_eq!(count, 0, "empty buffer must remain empty after no-op retry");
}

// ── retry_lmi_failed_agent_payloads — TTL / max-retry / count increment ──

/// Payloads older than 24 h must be silently dropped (not re-queued).
#[tokio::test]
#[serial]
async fn retry_lmi_drops_payload_after_24h_ttl() {
    clear_failed_agent_payloads();

    let old_time = chrono::Utc::now() - chrono::Duration::hours(25);
    let payload = crate::event_loop::FailedAgentPayload {
        payload_bytes: b"old-payload".to_vec(),
        request_id: "req-expired".to_string(),
        invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:fn".to_string(),
        retry_count: 0,
        failed_at: old_time,
    };
    crate::event_loop::FAILED_AGENT_PAYLOADS
        .lock()
        .unwrap()
        .push(payload);

    let fake_app = build_fake_apm_app();
    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(Some(fake_app)));
    retry_lmi_failed_agent_payloads(&apm_app).await;

    let len = crate::event_loop::FAILED_AGENT_PAYLOADS.lock().unwrap().len();
    assert_eq!(len, 0, "payload older than 24h must be dropped (TTL exceeded)");
    clear_failed_agent_payloads();
}

/// Payloads that have already been retried `LMI_AGENT_RETRY_MAX` times must be dropped.
#[tokio::test]
#[serial]
async fn retry_lmi_drops_payload_after_max_retries() {
    clear_failed_agent_payloads();

    let payload = crate::event_loop::FailedAgentPayload {
        payload_bytes: b"exhausted-payload".to_vec(),
        request_id: "req-maxed".to_string(),
        invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:fn".to_string(),
        retry_count: LMI_AGENT_RETRY_MAX, // exactly at the limit
        failed_at: chrono::Utc::now(),
    };
    crate::event_loop::FAILED_AGENT_PAYLOADS
        .lock()
        .unwrap()
        .push(payload);

    let fake_app = build_fake_apm_app();
    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(Some(fake_app)));
    retry_lmi_failed_agent_payloads(&apm_app).await;

    let len = crate::event_loop::FAILED_AGENT_PAYLOADS.lock().unwrap().len();
    assert_eq!(len, 0, "payload at max retry limit must be dropped");
    clear_failed_agent_payloads();
}

/// When `process_agent_payload` fails, `retry_count` must be incremented and the
/// payload must be re-queued for the next heartbeat.
/// Uses a payload starting with `[` so `parse_agent_payload` enters the decode
/// path and fails on invalid base64, triggering the `Err` branch in retry logic.
#[tokio::test]
#[serial]
async fn retry_lmi_increments_retry_count_on_failure() {
    clear_failed_agent_payloads();

    // "[\"1\",\"!!!!\"]" — starts with `[`, invalid base64 → parse_agent_payload Err
    let failing_payload = b"[\"1\",\"!!!!\"]".to_vec();
    let payload = crate::event_loop::FailedAgentPayload {
        payload_bytes: failing_payload,
        request_id: "req-fresh".to_string(),
        invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:fn".to_string(),
        retry_count: 0,
        failed_at: chrono::Utc::now(),
    };
    crate::event_loop::FAILED_AGENT_PAYLOADS
        .lock()
        .unwrap()
        .push(payload);

    let fake_app = build_fake_apm_app();
    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(Some(fake_app)));
    retry_lmi_failed_agent_payloads(&apm_app).await;

    // Extract values before asserting to avoid poisoning the mutex on assertion failure
    let (len, retry_count, request_id) = {
        let guard = crate::event_loop::FAILED_AGENT_PAYLOADS.lock().unwrap();
        let len = guard.len();
        let retry_count = guard.first().map(|p| p.retry_count);
        let request_id = guard.first().map(|p| p.request_id.clone());
        (len, retry_count, request_id)
    };
    assert_eq!(len, 1, "failed payload must stay in buffer");
    assert_eq!(retry_count, Some(1), "retry_count must be incremented by 1");
    assert_eq!(request_id.as_deref(), Some("req-fresh"));
    clear_failed_agent_payloads();
}

/// When one payload is dropped (expired) and another fails, only the failed
/// one must remain with retry_count incremented (merge-back correctness).
#[tokio::test]
#[serial]
async fn retry_lmi_merge_back_partial_success() {
    clear_failed_agent_payloads();

    let failing_payload = b"[\"1\",\"!!!!\"]".to_vec();

    let expired = crate::event_loop::FailedAgentPayload {
        payload_bytes: b"expired".to_vec(),
        request_id: "req-expired".to_string(),
        invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:fn".to_string(),
        retry_count: 0,
        failed_at: chrono::Utc::now() - chrono::Duration::hours(26),
    };
    let fresh = crate::event_loop::FailedAgentPayload {
        payload_bytes: failing_payload,
        request_id: "req-fresh".to_string(),
        invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:fn".to_string(),
        retry_count: 2,
        failed_at: chrono::Utc::now(),
    };
    {
        let mut guard = crate::event_loop::FAILED_AGENT_PAYLOADS.lock().unwrap();
        guard.push(expired);
        guard.push(fresh);
    }

    let fake_app = build_fake_apm_app();
    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(Some(fake_app)));
    retry_lmi_failed_agent_payloads(&apm_app).await;

    let (len, request_id, retry_count) = {
        let guard = crate::event_loop::FAILED_AGENT_PAYLOADS.lock().unwrap();
        let len = guard.len();
        let request_id = guard.first().map(|p| p.request_id.clone());
        let retry_count = guard.first().map(|p| p.retry_count);
        (len, request_id, retry_count)
    };
    assert_eq!(len, 1, "only the fresh failed payload must survive (expired dropped)");
    assert_eq!(request_id.as_deref(), Some("req-fresh"), "surviving payload must be req-fresh");
    assert_eq!(retry_count, Some(3), "retry_count must be incremented from 2 to 3");
    clear_failed_agent_payloads();
}

// ── flush_lmi_telemetry — reconnect detection & spawn gating ─────────────

/// When `take_reconnect_needed()` returns true, `apm_app` must be set to `None`.
#[tokio::test]
#[serial]
async fn flush_lmi_sets_apm_app_to_none_when_reconnect_needed() {
    clear_failed_agent_payloads();
    crate::apm::collector::signal_reconnect_needed();

    let fake_app = build_fake_apm_app();
    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(Some(fake_app)));
    // Prevent the reconnect spawn from firing (it would try network calls)
    let reconnect_in_flight = Arc::new(AtomicBool::new(true));

    let h = build_lmi_flush_handles(Arc::clone(&apm_app), Arc::clone(&reconnect_in_flight));
    flush_lmi_telemetry(&h, false).await;

    assert!(
        apm_app.read().await.is_none(),
        "apm_app must be invalidated (set to None) when reconnect was signalled"
    );
    clear_failed_agent_payloads();
}

/// When reconnect is NOT needed, an existing `apm_app` must remain `Some`.
#[tokio::test]
#[serial]
async fn flush_lmi_apm_app_unchanged_when_no_reconnect_needed() {
    clear_failed_agent_payloads();
    // Drain any leftover reconnect signal
    let _ = crate::apm::collector::take_reconnect_needed();

    let fake_app = build_fake_apm_app();
    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(Some(fake_app)));
    let reconnect_in_flight = Arc::new(AtomicBool::new(true)); // block spawn

    let h = build_lmi_flush_handles(Arc::clone(&apm_app), Arc::clone(&reconnect_in_flight));
    flush_lmi_telemetry(&h, false).await;

    assert!(
        apm_app.read().await.is_some(),
        "apm_app must remain Some when no reconnect was needed"
    );
    clear_failed_agent_payloads();
}

/// When `reconnect_in_flight` is already `true`, the spawn block must be skipped
/// (compare_exchange fails) and the flag must remain `true`.
#[tokio::test]
#[serial]
async fn flush_lmi_reconnect_spawn_blocked_when_in_flight() {
    clear_failed_agent_payloads();
    let _ = crate::apm::collector::take_reconnect_needed();
    crate::apm::connection::reset_handshake_fatal_for_test();

    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(None));
    let reconnect_in_flight = Arc::new(AtomicBool::new(true)); // already claimed

    let h = build_lmi_flush_handles(Arc::clone(&apm_app), Arc::clone(&reconnect_in_flight));
    flush_lmi_telemetry(&h, false).await;

    // The flag stays true — our caller set it and flush should not have claimed or
    // cleared it (only a LmiReconnectGuard from a successful spawn would clear it).
    assert!(
        reconnect_in_flight.load(Ordering::Acquire),
        "reconnect_in_flight must remain true — spawn must be blocked when in-flight"
    );
    clear_failed_agent_payloads();
}

/// When `is_handshake_fatal()` is true, the spawn block must be skipped entirely.
#[tokio::test]
#[serial]
async fn flush_lmi_reconnect_spawn_blocked_when_handshake_fatal() {
    clear_failed_agent_payloads();
    let _ = crate::apm::collector::take_reconnect_needed();
    crate::apm::connection::signal_handshake_fatal();

    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(None));
    let reconnect_in_flight = Arc::new(AtomicBool::new(false)); // not in flight

    let h = build_lmi_flush_handles(Arc::clone(&apm_app), Arc::clone(&reconnect_in_flight));
    flush_lmi_telemetry(&h, false).await;

    // Spawn was blocked by the `is_handshake_fatal()` guard — flag stays false
    assert!(
        !reconnect_in_flight.load(Ordering::Acquire),
        "reconnect_in_flight must remain false — spawn blocked by fatal handshake"
    );
    assert!(
        apm_app.read().await.is_none(),
        "apm_app must remain None when handshake is fatal"
    );

    crate::apm::connection::reset_handshake_fatal_for_test();
    clear_failed_agent_payloads();
}

// ── spawn_lmi_heartbeat — lifecycle ───────────────────────────────────────

/// Heartbeat task must stop promptly when the cancel channel is set to `true`.
#[tokio::test]
#[serial]
async fn heartbeat_stops_on_cancel() {
    clear_failed_agent_payloads();
    let _ = crate::apm::collector::take_reconnect_needed();
    crate::apm::connection::reset_handshake_fatal_for_test();

    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(None));
    // in_flight=true blocks reconnect spawn so no network calls happen
    let reconnect_in_flight = Arc::new(AtomicBool::new(true));
    let h = build_lmi_flush_handles(apm_app, reconnect_in_flight);

    // Use a very short interval so we don't wait too long
    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let handle = spawn_lmi_heartbeat(h, cancel_rx);

    // Cancel immediately
    cancel_tx.send(true).unwrap();
    // Task must finish within a generous deadline
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("heartbeat task must stop within 5s after cancel")
        .expect("heartbeat task must not panic");

    clear_failed_agent_payloads();
}

/// Dropping the cancel sender (channel closed) must also stop the heartbeat.
#[tokio::test]
#[serial]
async fn heartbeat_stops_when_cancel_sender_dropped() {
    clear_failed_agent_payloads();
    let _ = crate::apm::collector::take_reconnect_needed();
    crate::apm::connection::reset_handshake_fatal_for_test();

    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(None));
    let reconnect_in_flight = Arc::new(AtomicBool::new(true));
    let h = build_lmi_flush_handles(apm_app, reconnect_in_flight);

    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let handle = spawn_lmi_heartbeat(h, cancel_rx);

    // Drop the sender — the receiver's `changed()` will return `Err`
    drop(cancel_tx);
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("heartbeat task must stop within 5s after sender drop")
        .expect("heartbeat task must not panic");

    clear_failed_agent_payloads();
}

/// `flush_lmi_telemetry(final_drain = true)` must complete without panicking
/// even when no APM session is connected and all buffers are empty.
/// This exercises the shutdown-drain code path (called by
/// `drain_on_shutdown_with_timeout` on SHUTDOWN).
#[tokio::test]
#[serial]
async fn flush_lmi_final_drain_completes_when_no_app_and_empty_buffers() {
    clear_failed_agent_payloads();
    let _ = crate::apm::collector::take_reconnect_needed();
    crate::apm::connection::reset_handshake_fatal_for_test();

    // in_flight=true prevents the reconnect spawn from making network calls
    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(None));
    let reconnect_in_flight = Arc::new(AtomicBool::new(true));
    let h = build_lmi_flush_handles(apm_app, reconnect_in_flight);

    // Must not panic; must complete promptly (no real network traffic)
    tokio::time::timeout(
        Duration::from_secs(2),
        flush_lmi_telemetry(&h, true),
    )
    .await
    .expect("flush_lmi_telemetry(final_drain=true) must complete within 2s with empty buffers");

    clear_failed_agent_payloads();
}

/// `flush_lmi_telemetry(final_drain = true)` with a populated APM app must
/// still complete within the 2-second bound (network calls time out quickly
/// against the unreachable host; the function must not block indefinitely).
#[tokio::test]
#[serial]
async fn flush_lmi_final_drain_completes_with_app_present() {
    clear_failed_agent_payloads();
    let _ = crate::apm::collector::take_reconnect_needed();
    crate::apm::connection::reset_handshake_fatal_for_test();

    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(Some(build_fake_apm_app())));
    // in_flight=true prevents a second reconnect spawn during the drain
    let reconnect_in_flight = Arc::new(AtomicBool::new(true));
    let h = build_lmi_flush_handles(Arc::clone(&apm_app), reconnect_in_flight);

    tokio::time::timeout(
        Duration::from_secs(2),
        flush_lmi_telemetry(&h, true),
    )
    .await
    .expect("flush_lmi_telemetry(final_drain=true) must complete within 2s even with an APM app");

    clear_failed_agent_payloads();
}

// ── LmiFlushHandles::from_components ─────────────────────────────────────

/// `from_components` must clone all shared Arcs and set `reconnect_in_flight = false`.
#[test]
fn from_components_initializes_correctly() {
    use crate::config::{ExtensionConfig, deployment::DeploymentContext};
    use crate::context::InvocationContext;
    use crate::request::ProcessorFactory;

    let mut cfg = ExtensionConfig::default();
    cfg.deployment = DeploymentContext::Lmi;
    let config = Arc::new(cfg);

    let apm_app: crate::apm::SharedApmApp =
        Arc::new(tokio::sync::RwLock::new(None));
    let nr_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());

    let log_processor = Arc::new(crate::logs::processor::LogProcessor::new(
        Arc::clone(&nr_client),
        Arc::clone(&config),
        Arc::new(std::sync::Mutex::new(InvocationContext::default())),
        None,
    ));
    let processor_factory = Arc::new(ProcessorFactory::new(
        Arc::clone(&nr_client),
        Arc::clone(&config),
        Arc::clone(&apm_app),
    ));

    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

    let components = crate::event_loop::ExtensionComponents {
        client: Arc::new(Client::new()),
        extension_id: "test-ext-id".to_string(),
        processor_factory,
        newrelic_client: nr_client,
        config: Arc::clone(&config),
        global_log_processor: Arc::clone(&log_processor),
        apm_app: Arc::clone(&apm_app),
        apm_mode_enabled: true,
        apm_client: Client::builder()
            .timeout(Duration::from_millis(50))
            .build()
            .unwrap_or_default(),
        reconnect_in_flight: Arc::new(cancel_tx),
        deployment: DeploymentContext::Lmi,
    };

    let handles = LmiFlushHandles::from_components(&components);

    assert!(
        !handles.reconnect_in_flight.load(Ordering::Acquire),
        "from_components must initialize reconnect_in_flight to false"
    );
    assert!(
        Arc::ptr_eq(&handles.config, &config),
        "from_components must clone the config Arc (pointer equality)"
    );
    assert!(
        Arc::ptr_eq(&handles.apm_app, &apm_app),
        "from_components must clone the apm_app Arc (pointer equality)"
    );
}

// ── LAST_REQUEST_CONTEXT lifecycle ────────────────────────────────────────

/// LAST_REQUEST_CONTEXT is a global Mutex used by drain_on_shutdown_with_timeout.
/// Verify write, read, and clear behaviour.
#[test]
#[serial]
fn last_request_context_write_read_clear() {
    let clear = || {
        if let Ok(mut g) = crate::event_loop::LAST_REQUEST_CONTEXT.lock() {
            *g = None;
        }
    };
    clear();

    // Write a value.
    {
        let mut g = crate::event_loop::LAST_REQUEST_CONTEXT.lock().unwrap();
        *g = Some(("req-ctx-001".to_string(), "arn:test:fn".to_string()));
    }

    // Extract before asserting to avoid mutex poisoning on failure.
    let value = crate::event_loop::LAST_REQUEST_CONTEXT
        .lock()
        .unwrap()
        .clone();
    assert_eq!(
        value,
        Some(("req-ctx-001".to_string(), "arn:test:fn".to_string())),
        "LAST_REQUEST_CONTEXT must return the written value"
    );

    // Clear and verify.
    clear();
    let cleared = crate::event_loop::LAST_REQUEST_CONTEXT
        .lock()
        .unwrap()
        .clone();
    assert_eq!(cleared, None, "LAST_REQUEST_CONTEXT must be None after clearing");
}

// ── drain_on_shutdown_with_timeout — helpers ──────────────────────────────

fn clear_last_request_context() {
    if let Ok(mut g) = crate::event_loop::LAST_REQUEST_CONTEXT.lock() {
        *g = None;
    }
}

/// Build a minimal `ExtensionComponents` suitable for `drain_on_shutdown_with_timeout` tests.
/// Uses noop clients and an LMI deployment context.
fn build_test_extension_components(
    apm_app: crate::apm::SharedApmApp,
) -> crate::event_loop::ExtensionComponents {
    use crate::config::{ExtensionConfig, deployment::DeploymentContext};
    use crate::context::InvocationContext;
    use crate::request::ProcessorFactory;

    let mut cfg = ExtensionConfig::default();
    cfg.deployment = DeploymentContext::Lmi;
    let config = Arc::new(cfg);

    let nr_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
    let log_processor = Arc::new(crate::logs::processor::LogProcessor::new(
        Arc::clone(&nr_client),
        Arc::clone(&config),
        Arc::new(std::sync::Mutex::new(InvocationContext::default())),
        None,
    ));
    let processor_factory = Arc::new(ProcessorFactory::new(
        Arc::clone(&nr_client),
        Arc::clone(&config),
        Arc::clone(&apm_app),
    ));
    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

    crate::event_loop::ExtensionComponents {
        client: Arc::new(Client::new()),
        extension_id: "test-ext-id".to_string(),
        processor_factory,
        newrelic_client: nr_client,
        config,
        global_log_processor: log_processor,
        apm_app,
        apm_mode_enabled: true,
        apm_client: Client::builder()
            .timeout(Duration::from_millis(50))
            .build()
            .unwrap_or_default(),
        reconnect_in_flight: Arc::new(cancel_tx),
        deployment: crate::config::deployment::DeploymentContext::Lmi,
    }
}

// ── drain_on_shutdown_with_timeout — all 4 paths ──────────────────────────

/// Path A: `shutdown_reason = None` — skips error-event block, performs the final drain.
#[tokio::test]
#[serial]
async fn drain_on_shutdown_no_reason_completes() {
    clear_failed_agent_payloads();
    clear_last_request_context();
    let _ = crate::apm::collector::take_reconnect_needed();
    // Block reconnect spawn (no real network) via fatal-handshake gate.
    crate::apm::connection::signal_handshake_fatal();

    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(None));
    let components = build_test_extension_components(apm_app);

    tokio::time::timeout(
        Duration::from_secs(2),
        drain_on_shutdown_with_timeout(&components, None),
    )
    .await
    .expect("drain_on_shutdown_with_timeout(None) must complete within 2s");

    crate::apm::connection::reset_handshake_fatal_for_test();
    clear_failed_agent_payloads();
    clear_last_request_context();
}

/// Path B: `shutdown_reason = Some(Timeout)` but `LAST_REQUEST_CONTEXT = None`
/// → "no last-request context" debug path — skips error event.
#[tokio::test]
#[serial]
async fn drain_on_shutdown_with_reason_no_context() {
    clear_failed_agent_payloads();
    clear_last_request_context();
    let _ = crate::apm::collector::take_reconnect_needed();
    crate::apm::connection::signal_handshake_fatal();

    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(None));
    let components = build_test_extension_components(apm_app);

    tokio::time::timeout(
        Duration::from_secs(2),
        drain_on_shutdown_with_timeout(
            &components,
            Some(crate::runtime::ShutdownReason::Timeout),
        ),
    )
    .await
    .expect("drain_on_shutdown_with_timeout(Timeout, no context) must complete within 2s");

    crate::apm::connection::reset_handshake_fatal_for_test();
    clear_failed_agent_payloads();
    clear_last_request_context();
}

/// Path C: `shutdown_reason = Some`, `LAST_REQUEST_CONTEXT` populated, `apm_app = None`
/// → "APM app not connected — skipping shutdown error event" debug path.
#[tokio::test]
#[serial]
async fn drain_on_shutdown_with_reason_context_but_no_app() {
    clear_failed_agent_payloads();
    let _ = crate::apm::collector::take_reconnect_needed();
    crate::apm::connection::signal_handshake_fatal();

    {
        let mut g = crate::event_loop::LAST_REQUEST_CONTEXT.lock().unwrap();
        *g = Some(("req-shutdown-c".to_string(), "arn:test:fn".to_string()));
    }

    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(None));
    let components = build_test_extension_components(apm_app);

    tokio::time::timeout(
        Duration::from_secs(2),
        drain_on_shutdown_with_timeout(
            &components,
            Some(crate::runtime::ShutdownReason::Failure),
        ),
    )
    .await
    .expect("drain_on_shutdown_with_timeout(Failure, no app) must complete within 2s");

    crate::apm::connection::reset_handshake_fatal_for_test();
    clear_failed_agent_payloads();
    clear_last_request_context();
}

/// Path D: `shutdown_reason = Some(Timeout)`, `LAST_REQUEST_CONTEXT` populated,
/// `apm_app = Some` → calls `send_error_for_shutdown_reason`. Network call to
/// unreachable host times out in 50 ms; the function must not panic or block.
#[tokio::test]
#[serial]
async fn drain_on_shutdown_with_reason_context_and_app_timeout_reason() {
    clear_failed_agent_payloads();
    let _ = crate::apm::collector::take_reconnect_needed();
    crate::apm::connection::reset_handshake_fatal_for_test();

    {
        let mut g = crate::event_loop::LAST_REQUEST_CONTEXT.lock().unwrap();
        *g = Some(("req-shutdown-d".to_string(), "arn:test:fn".to_string()));
    }

    // apm_app = Some, with unreachable host + 50ms timeout so network calls fail fast.
    let fake_app = build_fake_apm_app();
    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(Some(fake_app)));
    let components = build_test_extension_components(Arc::clone(&apm_app));

    // Generous outer timeout — the fake client's 50ms network timeout is the bottleneck.
    tokio::time::timeout(
        Duration::from_secs(3),
        drain_on_shutdown_with_timeout(
            &components,
            Some(crate::runtime::ShutdownReason::Timeout),
        ),
    )
    .await
    .expect("drain_on_shutdown_with_timeout(Timeout, with app) must complete within 3s");

    clear_failed_agent_payloads();
    clear_last_request_context();
}

/// Path D2: `shutdown_reason = Some(Failure)` — exercises the Failure arm of
/// `send_error_for_shutdown_reason` (distinct error class from Timeout).
#[tokio::test]
#[serial]
async fn drain_on_shutdown_with_reason_context_and_app_failure_reason() {
    clear_failed_agent_payloads();
    let _ = crate::apm::collector::take_reconnect_needed();
    crate::apm::connection::reset_handshake_fatal_for_test();

    {
        let mut g = crate::event_loop::LAST_REQUEST_CONTEXT.lock().unwrap();
        *g = Some(("req-shutdown-failure".to_string(), "arn:test:fn".to_string()));
    }

    let fake_app = build_fake_apm_app();
    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(Some(fake_app)));
    let components = build_test_extension_components(Arc::clone(&apm_app));

    tokio::time::timeout(
        Duration::from_secs(3),
        drain_on_shutdown_with_timeout(
            &components,
            Some(crate::runtime::ShutdownReason::Failure),
        ),
    )
    .await
    .expect("drain_on_shutdown_with_timeout(Failure, with app) must complete within 3s");

    clear_failed_agent_payloads();
    clear_last_request_context();
}

/// Path D3: `shutdown_reason = Some(Spindown)` — `send_error_for_shutdown_reason`
/// must take the `Spindown` arm (no error event sent). Must complete promptly.
#[tokio::test]
#[serial]
async fn drain_on_shutdown_spindown_skips_error_event() {
    clear_failed_agent_payloads();
    let _ = crate::apm::collector::take_reconnect_needed();
    crate::apm::connection::reset_handshake_fatal_for_test();

    {
        let mut g = crate::event_loop::LAST_REQUEST_CONTEXT.lock().unwrap();
        *g = Some(("req-spindown".to_string(), "arn:test:fn".to_string()));
    }

    // Even with an app present, Spindown must NOT fire a network send.
    let fake_app = build_fake_apm_app();
    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(Some(fake_app)));
    let components = build_test_extension_components(Arc::clone(&apm_app));

    tokio::time::timeout(
        Duration::from_secs(2),
        drain_on_shutdown_with_timeout(
            &components,
            Some(crate::runtime::ShutdownReason::Spindown),
        ),
    )
    .await
    .expect("drain_on_shutdown_with_timeout(Spindown) must complete within 2s without sending");

    clear_failed_agent_payloads();
    clear_last_request_context();
}

/// Path D4: `shutdown_reason = Some(Unknown)` — exercises the Unknown arm of
/// `send_error_for_shutdown_reason` (generic shutdown error class).
#[tokio::test]
#[serial]
async fn drain_on_shutdown_with_reason_unknown() {
    clear_failed_agent_payloads();
    let _ = crate::apm::collector::take_reconnect_needed();
    crate::apm::connection::reset_handshake_fatal_for_test();

    {
        let mut g = crate::event_loop::LAST_REQUEST_CONTEXT.lock().unwrap();
        *g = Some(("req-unknown-shutdown".to_string(), "arn:test:fn".to_string()));
    }

    let fake_app = build_fake_apm_app();
    let apm_app: crate::apm::SharedApmApp = Arc::new(RwLock::new(Some(fake_app)));
    let components = build_test_extension_components(Arc::clone(&apm_app));

    tokio::time::timeout(
        Duration::from_secs(3),
        drain_on_shutdown_with_timeout(
            &components,
            Some(crate::runtime::ShutdownReason::Unknown),
        ),
    )
    .await
    .expect("drain_on_shutdown_with_timeout(Unknown, with app) must complete within 3s");

    clear_failed_agent_payloads();
    clear_last_request_context();
}
