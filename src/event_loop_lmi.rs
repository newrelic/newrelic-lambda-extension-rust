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

/// Cloneable handles for the LMI flush path. All `Arc`, so the heartbeat task
/// can own its own copy without borrowing the event loop's `&mut components`.
#[derive(Clone)]
struct LmiFlushHandles {
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
    apm_app: crate::apm::SharedApmApp,
    client: Arc<Client>,
}

impl LmiFlushHandles {
    fn from_components(c: &ExtensionComponents) -> Self {
        Self {
            config: Arc::clone(&c.config),
            global_log_processor: Arc::clone(&c.global_log_processor),
            apm_app: Arc::clone(&c.apm_app),
            client: Arc::clone(&c.client),
        }
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
async fn flush_lmi_telemetry(h: &LmiFlushHandles, final_drain: bool) {
    process_pending_agent_payloads(&h.config, &h.global_log_processor, &h.apm_app, "").await;

    crate::apm::telemetry_buffer::retry_buffered_telemetry(
        &h.client,
        h.config.new_relic.license_key.as_deref().unwrap_or(""),
    )
    .await;

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
