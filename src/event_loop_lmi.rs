// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Event loop for Lambda Managed Instances (LMI).
//!
//! ## Why a separate loop
//!
//! LMI extensions can register only for `SHUTDOWN` — AWS rejects `INVOKE`
//! registration because LMI supports concurrent invocations within a single
//! execution environment. `/event/next` therefore returns only `SHUTDOWN`,
//! and the per-invocation work that Normal Lambda drives off `INVOKE` is
//! moved entirely onto the Telemetry API: `platform.start`, `platform.report`,
//! `platform.runtimeDone`. See `LMI_SUPPORT.md` §3.
//!
//! Reusing the existing `execute_apm_mode_event_loop` would deadlock — it
//! blocks on `/event/next` waiting for `INVOKE` events that never arrive.
//! Refactoring the existing loop to handle both lifecycles was rejected
//! (Option A in `LMI_OPTION_C_PLAN.md` §0).
//!
//! ## What this loop owns
//!
//! 1. Polling `/event/next` for the single `SHUTDOWN` event AWS will send.
//! 2. Defensively ignoring stray `INVOKE` events (AWS spec violation, but
//!    don't panic — log and continue).
//! 3. Draining pending state on `SHUTDOWN` (a subset of the APM-mode drain;
//!    `platform.report` metrics have already been sent during invocation by
//!    the telemetry listener, so no metric send is needed here).
//!
//! Per-invocation telemetry (agent payloads, `platform.report` → APM metrics,
//! function logs) flows through the listener and IPC plumbing that
//! `perform_one_time_initialization` sets up — both run as background tasks
//! and are mode-agnostic. The LMI loop never touches them directly.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};

use crate::{
    config::ExtensionConfig,
    event_loop::{
        process_pending_agent_payloads, send_error_for_shutdown_reason,
        ExtensionComponents, LAST_REQUEST_CONTEXT,
    },
    logs::processor::LogProcessor,
    newrelic::flush::Flush,
    runtime,
};

/// Lambda gives ~2 s for the SHUTDOWN handler. Keep the drain bounded so we
/// exit before AWS terminates us mid-flight.
const LMI_SHUTDOWN_TIMEOUT_MS: u64 = 1800;

/// After this many failed retries an agent payload is dropped to bound memory.
/// Higher than Normal Lambda's 5 to account for the longer LMI lifetime (~14 days).
const LMI_AGENT_RETRY_MAX: usize = 10;

/// Cloneable handles for the LMI flush path. All fields are either `Arc` or
/// cheaply-clonable (`reqwest::Client` is internally `Arc`-wrapped), so the
/// heartbeat task can own its own copy without borrowing `&mut components`.
#[derive(Clone)]
struct LmiFlushHandles {
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
    apm_app: crate::apm::SharedApmApp,
    /// Lambda runtime client — talks to localhost Extensions API only (`no_proxy()`).
    /// Used for `/event/next`. Do NOT use for outbound APM collector calls.
    client: Arc<Client>,
    /// Proxy-aware outbound client with correct timeout for NR APM collector calls.
    /// Built by `build_outbound_client(proxy_url)`. Cloned from `ExtensionComponents`.
    apm_client: Client,
    /// Prevents concurrent reconnect spawns. Set to `true` when a reconnect task is
    /// in flight; cleared by `LmiReconnectGuard` on task exit (success or failure).
    reconnect_in_flight: Arc<AtomicBool>,
}

impl LmiFlushHandles {
    fn from_components(c: &ExtensionComponents) -> Self {
        Self {
            config: Arc::clone(&c.config),
            global_log_processor: Arc::clone(&c.global_log_processor),
            apm_app: Arc::clone(&c.apm_app),
            client: Arc::clone(&c.client),
            apm_client: c.apm_client.clone(),
            reconnect_in_flight: Arc::new(AtomicBool::new(false)),
        }
    }
}

/// Drop guard that clears `reconnect_in_flight` on any exit path (success, error, or panic).
struct LmiReconnectGuard(Arc<AtomicBool>);

impl Drop for LmiReconnectGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Drain buffered telemetry to New Relic. The single flush implementation,
/// shared by the periodic heartbeat (`final_drain = false`) and the SHUTDOWN
/// drain (`final_drain = true`).
///
/// Agent payloads are delivered by `run_id` (empty `request_id` = "every
/// pending request"); `platform.report` metrics are already sent on arrival by
/// the telemetry listener, so only previously-failed APM telemetry is retried.
/// Logs use the normal batch flush on the heartbeat and the retrying shutdown
/// flush (pre-invoke buffer + `flush_on_shutdown`) on the final drain. When the
/// buffers are empty these calls perform no network I/O, so idle ticks are
/// effectively a no-op.
///
/// Also handles APM reconnect: if the collector signalled that the cached
/// `run_id` is stale (401/409/410), the APM app is invalidated and a fresh
/// handshake is spawned in the background using the proxy-aware `apm_client`.
/// `FAILED_AGENT_PAYLOADS` are retried via the APM endpoint once connected.
async fn flush_lmi_telemetry(h: &LmiFlushHandles, final_drain: bool) {
    process_pending_agent_payloads(&h.config, &h.global_log_processor, &h.apm_app, "").await;

    // Invalidate the cached APM session if the collector returned 401/409/410
    // during a recent send. Under Normal Lambda this check happens at every INVOKE;
    // under LMI there are no INVOKE events so we check here on every heartbeat tick.
    if crate::apm::collector::take_reconnect_needed() {
        let mut w = h.apm_app.write().await;
        if w.is_some() {
            *w = None;
            warn!("LMI: APM run_id invalidated (collector restart/disconnect) — will reconnect on next tick");
        }
    }

    // Spawn a fresh APM handshake if not connected and no reconnect is already in
    // flight. Uses `compare_exchange` so only one heartbeat tick can claim the slot.
    // The spawned task uses `apm_client` (proxy-aware, bounded timeout) — NOT the
    // Lambda runtime client (localhost-only, no proxy, no timeout).
    {
        let is_connected = h.apm_app.read().await.is_some();
        if !is_connected
            && !crate::apm::connection::is_handshake_fatal()
            && h.reconnect_in_flight
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            let apm_app = Arc::clone(&h.apm_app);
            let reconnect_flag = Arc::clone(&h.reconnect_in_flight);
            let license_key = h.config.new_relic.license_key.clone().unwrap_or_default();
            let apm_host = h.config.new_relic.apm_host.clone();
            let metric_endpoint = h.config.new_relic.metric_endpoint.clone();
            let apm_client = h.apm_client.clone();
            let lambda_function_name = h.config.aws.function_name.clone();
            let function_name = std::env::var("NEW_RELIC_APP_NAME")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| lambda_function_name.clone());
            let function_version = h
                .config
                .aws
                .function_version
                .clone()
                .unwrap_or_else(|| "$LATEST".to_string());
            let account_id = h.config.aws.account_id.clone();
            let region = h.config.aws.region.clone();
            let timeout_secs = h.config.new_relic.apm_handshake_timeout_secs;
            let deployment = h.config.deployment;

            tokio::spawn(async move {
                let _guard = LmiReconnectGuard(reconnect_flag);
                debug!("LMI: APM reconnect attempt started");
                match crate::apm::ApmApp::new(
                    license_key,
                    apm_host,
                    metric_endpoint,
                    apm_client,
                    function_name,
                    lambda_function_name,
                    function_version,
                    account_id,
                    region,
                    timeout_secs,
                    deployment,
                )
                .await
                {
                    Ok(app) => {
                        info!(
                            "LMI: APM reconnect succeeded — Entity GUID: {}",
                            app.get_entity_guid()
                        );
                        *apm_app.write().await = Some(app);
                    }
                    Err(e) => {
                        if !crate::apm::connection::is_handshake_fatal() {
                            warn!(
                                "LMI: APM reconnect attempt failed: {} — will retry next heartbeat",
                                e
                            );
                        }
                    }
                }
            });
        }
    }

    // Retry agent payloads that were buffered while the APM session was down
    // (reconnect window). Uses the APM endpoint directly. If apm_app is still
    // None (reconnect in progress) the payloads stay buffered until next tick.
    retry_lmi_failed_agent_payloads(&h.apm_app).await;

    let license_key = h.config.new_relic.license_key.as_deref().unwrap_or("");

    // Override stale buffered run_id/collector_host with the live session's
    // values when connected (mirrors the APM-mode retry path in event_loop.rs).
    // After a reconnect the buffered run_id is expired, so retrying with it
    // fails forever; None falls back to the stored values when not connected.
    let (cur_run_id, cur_collector_host) = {
        let guard = h.apm_app.read().await;
        match guard.as_ref() {
            Some(app) => (Some(app.run_id.clone()), Some(app.collector_host.clone())),
            None => (None, None),
        }
    };

    crate::apm::telemetry_buffer::retry_buffered_telemetry(
        &h.client,
        license_key,
        cur_run_id.as_deref(),
        cur_collector_host.as_deref(),
    )
    .await;

    // Retry platform.report metrics that failed the Metric API send-on-arrival.
    // Under LMI these are buffered by send_platform_report_metrics and would
    // otherwise never resend (no INVOKE-driven retry on LMI).
    crate::apm::metric_api_buffer::retry_buffered_metric_api(&h.client, license_key).await;

    if final_drain {
        if let Err(e) = h
            .global_log_processor
            .flush_pre_invoke_buffer_on_shutdown()
            .await
        {
            error!("LMI: failed to flush pre-invoke log buffer: {}", e);
        }
        if let Err(e) = h.global_log_processor.flush_on_shutdown().await {
            error!("LMI: failed to flush logs on shutdown: {}", e);
        }
    } else {
        h.global_log_processor.process_pre_invoke_logs_lmi();
        if let Err(e) = h.global_log_processor.flush().await {
            error!("LMI: heartbeat log flush failed: {}", e);
        }
    }
}

/// Spawn the periodic heartbeat flush task.
///
/// LMI runs continuously (no freeze between invokes) and never delivers
/// `platform.runtimeDone`, so periodic flushing must be time-driven and live in
/// its own task — the `/event/next` loop is blocked waiting for SHUTDOWN.
/// `MissedTickBehavior::Skip` avoids burst catch-up. The task stops when
/// `cancel_rx` flips to `true`; it does NOT drain on cancel because the main
/// loop performs the authoritative final drain on SHUTDOWN.
fn spawn_lmi_heartbeat(
    h: LmiFlushHandles,
    mut cancel_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    let interval_ms = h.config.extension.lmi_flush_interval_ms;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker.tick().await; // discard the immediate first tick
        info!("LMI heartbeat flush task started (interval={}ms)", interval_ms);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    debug!("LMI heartbeat: periodic flush tick");
                    flush_lmi_telemetry(&h, false).await;
                }
                res = cancel_rx.changed() => {
                    if res.is_err() || *cancel_rx.borrow() {
                        break;
                    }
                }
            }
        }
        info!("LMI heartbeat flush task stopped");
    })
}

/// LMI event loop. Telemetry-driven; `/event/next` is used only as a
/// SHUTDOWN waiter.
pub async fn execute_lmi_mode_event_loop(components: &mut ExtensionComponents) -> u32 {
    debug_assert!(components.deployment.is_lmi(), "LMI loop entered without LMI deployment context");
    debug_assert!(components.apm_mode_enabled, "LMI must force APM mode");

    // The heartbeat flush task runs concurrently: /event/next below blocks
    // waiting for SHUTDOWN, so it cannot drive periodic flushing itself. The
    // task drains buffered telemetry every `lmi_flush_interval_ms`; on cancel it
    // stops (the final drain is performed by this loop on SHUTDOWN).
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let heartbeat = spawn_lmi_heartbeat(LmiFlushHandles::from_components(components), cancel_rx);

    let mut event_counter: u32 = 0;

    loop {
        debug!("LMI mode: waiting for SHUTDOWN (INVOKE is not delivered on LMI)");

        let runtime_event = match runtime::fetch_next_event(
            &components.client,
            &components.extension_id,
        )
        .await
        {
            Ok(event) => event,
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("403") || error_msg.contains("State transition") {
                    error!("LMI mode: fatal /next error (403 — Lambda shutting down): {:?}", e);
                    let _ = cancel_tx.send(true);
                    drain_on_shutdown_with_timeout(components, None).await;
                    info!("LMI mode: emergency shutdown cleanup completed. Extension exiting.");
                    break;
                }
                error!("LMI mode: error receiving next event: {:?}. Continuing.", e);
                continue;
            }
        };

        event_counter = event_counter.saturating_add(1);

        match runtime_event {
            // AWS spec violation on LMI — log loudly but stay alive. Per-invocation
            // work is driven by the telemetry listener regardless of what /next
            // returns, so a stray INVOKE is informational, not fatal.
            runtime::LambdaRuntimeEvent::Invoke {
                request_id,
                invoked_function_arn: _,
                deadline_ms: _,
            } => {
                error!(
                    "LMI mode: received INVOKE event for request {} — AWS spec violation, ignoring",
                    request_id
                );
            }
            runtime::LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
                let shutdown_start = std::time::Instant::now();
                info!(
                    "LMI mode: SHUTDOWN received (reason: {}). Draining…",
                    shutdown_reason
                );

                let _ = cancel_tx.send(true);
                drain_on_shutdown_with_timeout(components, Some(shutdown_reason)).await;

                info!(
                    "LMI mode: shutdown drain completed in {}ms",
                    shutdown_start.elapsed().as_millis()
                );
                break;
            }
        }
    }

    // Stop and join the heartbeat task before returning (idempotent if already
    // cancelled on the shutdown/fatal path above).
    let _ = cancel_tx.send(true);
    let _ = heartbeat.await;

    event_counter
}

/// Retry agent payloads that failed while the APM session was unavailable (reconnect
/// window). Drains `FAILED_AGENT_PAYLOADS` atomically so concurrent sends during
/// processing don't get lost: any new items appended while we run are merged back in.
///
/// Uses `ApmApp::process_agent_payload` (APM collector endpoint) — correct for LMI.
/// Normal Lambda's `retry_failed_agent_payloads` uses the serverless endpoint, which
/// is wrong here. Payloads are dropped after `LMI_AGENT_RETRY_MAX` retries or 24h TTL.
async fn retry_lmi_failed_agent_payloads(apm_app: &crate::apm::SharedApmApp) {
    // Atomically drain the buffer so concurrent sends during our loop don't get lost.
    let payloads = match crate::event_loop::FAILED_AGENT_PAYLOADS.lock() {
        Ok(mut guard) => std::mem::take(&mut *guard),
        Err(_) => {
            error!("LMI: failed to lock FAILED_AGENT_PAYLOADS for retry");
            return;
        }
    };

    if payloads.is_empty() {
        return;
    }

    let apm_guard = apm_app.read().await;
    let app = match apm_guard.as_ref() {
        Some(a) => a,
        None => {
            // Not connected yet — put payloads back; they will be retried once the
            // reconnect spawned by flush_lmi_telemetry succeeds.
            drop(apm_guard);
            if let Ok(mut guard) = crate::event_loop::FAILED_AGENT_PAYLOADS.lock() {
                let mut restored = payloads;
                restored.extend(guard.drain(..));
                *guard = restored;
            }
            return;
        }
    };

    debug!(
        "LMI: retrying {} failed agent payload(s) via APM endpoint",
        payloads.len()
    );

    let now = chrono::Utc::now();
    let mut remaining: Vec<crate::event_loop::FailedAgentPayload> = Vec::new();
    let mut succeeded = 0usize;
    let mut dropped = 0usize;

    for mut payload in payloads {
        let age_hours = now
            .signed_duration_since(payload.failed_at)
            .num_hours();
        if age_hours >= 24 {
            warn!(
                "LMI: dropping agent payload for request {} ({}h old, exceeds 24h TTL)",
                payload.request_id, age_hours
            );
            dropped += 1;
            continue;
        }
        if payload.retry_count >= LMI_AGENT_RETRY_MAX {
            warn!(
                "LMI: dropping agent payload for request {} after {} retries",
                payload.request_id, payload.retry_count
            );
            dropped += 1;
            continue;
        }

        match app
            .process_agent_payload(payload.payload_bytes.clone(), &payload.request_id)
            .await
        {
            Ok(_) => {
                debug!(
                    "LMI: agent payload retry succeeded for request {} (attempt {})",
                    payload.request_id,
                    payload.retry_count + 1
                );
                succeeded += 1;
            }
            Err(e) => {
                payload.retry_count += 1;
                warn!(
                    "LMI: agent payload retry failed for request {} (attempt {}): {}",
                    payload.request_id, payload.retry_count, e
                );
                remaining.push(payload);
            }
        }
    }

    if succeeded > 0 || dropped > 0 {
        debug!(
            "LMI: agent payload retry: {} succeeded, {} dropped, {} re-queued",
            succeeded,
            dropped,
            remaining.len()
        );
    }

    // Merge re-queued items with any new failures that arrived during processing,
    // then store back. Drop the read guard first to avoid holding it across the lock.
    drop(apm_guard);
    if !remaining.is_empty() {
        if let Ok(mut guard) = crate::event_loop::FAILED_AGENT_PAYLOADS.lock() {
            remaining.extend(guard.drain(..));
            *guard = remaining;
        }
    }
}

/// Drain all pending telemetry within the SHUTDOWN budget.
///
/// Subset of the APM-mode shutdown handler — skips the platform.report
/// metric send because the listener already dispatches each report as
/// metrics during the invocation that produced it.
async fn drain_on_shutdown_with_timeout(
    components: &ExtensionComponents,
    shutdown_reason: Option<runtime::ShutdownReason>,
) {
    let timed = tokio::time::timeout(Duration::from_millis(LMI_SHUTDOWN_TIMEOUT_MS), async {
        // Optional APM error event on the way out, only when shutdown was
        // abnormal and we actually have a last-known request to attach it to.
        if let Some(reason) = shutdown_reason {
            if let Some((last_request_id, last_arn)) =
                LAST_REQUEST_CONTEXT.lock().ok().and_then(|guard| guard.clone())
            {
                let apm_app_guard = components.apm_app.read().await;
                if let Some(ref app) = *apm_app_guard {
                    send_error_for_shutdown_reason(app, reason, &last_request_id, &last_arn).await;
                } else {
                    debug!(
                        "LMI shutdown: APM app not connected — skipping shutdown error event for request {}",
                        last_request_id
                    );
                }
            } else {
                debug!("LMI shutdown: no last-request context — skipping shutdown error event");
            }
        }

        // Unified final drain — the same flush path the heartbeat uses, with
        // shutdown log-flush semantics (agent payloads by run_id, retry buffered
        // APM telemetry, then pre-invoke buffer + retrying shutdown log flush).
        flush_lmi_telemetry(&LmiFlushHandles::from_components(components), true).await;

        let remaining = crate::apm::telemetry_buffer::get_buffer_count();
        if remaining > 0 {
            error!("LMI shutdown: {} telemetry items could not be sent", remaining);
        }
    })
    .await;

    if timed.is_err() {
        warn!(
            "LMI shutdown: drain timed out after {}ms — Lambda will terminate remaining work",
            LMI_SHUTDOWN_TIMEOUT_MS
        );
    }
}

#[cfg(test)]
mod tests {
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
            collector_host: "http://unreachable.invalid.test".to_string(),
            license_key: "fake-license-key-for-unit-test".to_string(),
            metric_endpoint: "http://metric.invalid.test/metric/v1".to_string(),
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
}
