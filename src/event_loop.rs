// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0



use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use std::time::Duration;
use reqwest::Client;
use tracing::{debug, error, info, trace, warn};

use crate::{
    runtime,
    config::{self, ExtensionConfig},
    newrelic::client::NewRelicClient,
    newrelic::flush::Flush,
    logs::processor::LogProcessor,
    request::{
        self,
        ProcessorFactory,
        create_request_processing_state,
        cleanup_request_processing_state_internal,
        REQUEST_PROCESSORS, REQUEST_DATA,
        get_agent_buffer, get_request_context, get_pending_report,
        remove_pending_report, request_data_len,
    },
    agent::batch::{
        add_to_batch, should_send_batch_by_threshold, 
        send_batched_payloads_with_reports_only,
        send_all_pending_payloads_on_shutdown,
    },
    error_synthesis,
    trace,
    version,
    IS_WARM_START,
};

const SHUTDOWN_TIMEOUT_MS: u64 = 1800;

/// Safety margin (ms) reserved before a function's own remaining deadline when
/// bound-waiting for something within an invocation (an APM handshake, or a late
/// agent payload) — leaves headroom for the downstream flush/cleanup work that
/// still has to run before the extension returns to `/next`. Shared by
/// `wait_for_apm_handshake_within_budget` and `wait_for_late_agent_payload` so the
/// two bounded waits can't drift apart on this value.
const INVOKE_DEADLINE_SAFETY_MARGIN_MS: u64 = 500;

/// Budget reserved (out of SHUTDOWN_TIMEOUT_MS) to POST the APM "telemetry
/// dropped" diagnostic directly to New Relic Logs. The main shutdown work runs
/// in `SHUTDOWN_TIMEOUT_MS - SHUTDOWN_DIAG_RESERVE_MS`, then the diagnostic gets
/// this protected window — so a slow flush/reconnect can't starve the one log
/// the customer most needs to see. Sum stays < Lambda's 2s SHUTDOWN deadline.
const SHUTDOWN_DIAG_RESERVE_MS: u64 = 500;

/// Drop guard that clears `reconnect_in_flight` on any exit path (success, error, or panic).
struct ReconnectGuard(Arc<watch::Sender<bool>>);

impl Drop for ReconnectGuard {
    fn drop(&mut self) {
        self.0.send_replace(false);
    }
}

#[derive(Debug)]
pub struct ExtensionComponents {
    pub client: Arc<Client>,
    pub extension_id: String,
    pub processor_factory: Arc<ProcessorFactory>,
    pub newrelic_client: Arc<NewRelicClient>,
    pub config: Arc<ExtensionConfig>,
    pub global_log_processor: Arc<LogProcessor>,
    pub apm_app: crate::apm::SharedApmApp,
    pub apm_mode_enabled: bool, // Actual mode after runtime detection (may differ from config for Java)
    pub apm_client: Client,
    pub reconnect_in_flight: Arc<watch::Sender<bool>>,
}

#[derive(Debug, Clone)]
pub struct FailedAgentPayload {
    pub payload_bytes: Vec<u8>,
    pub request_id: String,
    pub invoked_function_arn: String,
    pub retry_count: usize,
    pub failed_at: chrono::DateTime<chrono::Utc>,
}

pub static FAILED_AGENT_PAYLOADS: once_cell::sync::Lazy<Arc<Mutex<Vec<FailedAgentPayload>>>> =
    once_cell::sync::Lazy::new(|| Arc::new(Mutex::new(Vec::new())));

/// Max buffered agent payloads (one per disconnected invoke). A sandbox lives at
/// most ~30 min, so this is a memory guard, not a retry policy: payloads are kept
/// until they flush on reconnect or the container shuts down — NOT dropped after
/// a few retries. ~500 × a few KB ≈ low single-digit MB. When full, the oldest is
/// evicted (and counted) so a long disconnected period on a busy function can't grow unbounded.
const MAX_FAILED_AGENT_PAYLOADS: usize = 500;

/// Count of buffered agent payloads dropped *before* shutdown (evicted when the
/// buffer is full). Lets the shutdown summary report the true loss, since evicted
/// payloads are no longer in the buffer to be counted.
static DROPPED_AGENT_PAYLOADS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Number of agent payloads dropped before shutdown due to buffer-full eviction.
pub fn dropped_agent_payload_count() -> u64 {
    DROPPED_AGENT_PAYLOADS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Distinct request_ids whose agent payloads are still buffered (un-sent). Used
/// by the shutdown summary, since this buffer is the main bucket of un-delivered
/// data when APM never connected.
pub fn buffered_agent_payload_request_ids() -> Vec<String> {
    let mut ids: Vec<String> = FAILED_AGENT_PAYLOADS
        .lock()
        .map(|b| b.iter().map(|p| p.request_id.clone()).collect())
        .unwrap_or_default();
    ids.sort();
    ids.dedup();
    ids
}

/// Push a failed agent payload, evicting (and counting) the oldest if the buffer
/// is at capacity. Single choke point so both first-failure and retry re-buffer
/// paths enforce the same cap.
fn push_failed_payload_capped(buf: &mut Vec<FailedAgentPayload>, payload: FailedAgentPayload) {
    if buf.len() >= MAX_FAILED_AGENT_PAYLOADS {
        buf.remove(0);
        DROPPED_AGENT_PAYLOADS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        warn!(
            "Failed agent payload buffer full ({}) - evicted oldest invocation's data",
            MAX_FAILED_AGENT_PAYLOADS
        );
    }
    buf.push(payload);
}

/// Track last processed request for error synthesis on shutdown
pub static LAST_REQUEST_CONTEXT: once_cell::sync::Lazy<Arc<Mutex<Option<(String, String)>>>> =
    once_cell::sync::Lazy::new(|| Arc::new(Mutex::new(None)));

/// Event loop: handles cold start (first invoke) and warm starts (subsequent invokes)
pub async fn run_infinite_event_loop(
    mut extension_components: ExtensionComponents,
) -> u32 {
    if !extension_components.config.new_relic.extension_enabled
        || extension_components.config.new_relic.license_key.is_none()
    {
        info!("Running in no-op mode");
        execute_noop_event_loop(&extension_components.client, &extension_components.extension_id)
            .await;
        return 0;
    }

    execute_main_telemetry_processing_loop(&mut extension_components).await
}

/// Lambda extension pattern: GET /next (block) → process INVOKE → repeat until SHUTDOWN
/// Routes to APM or serverless mode based on config (or runtime override for Java)
async fn execute_main_telemetry_processing_loop(components: &mut ExtensionComponents) -> u32 {
    let apm_mode_enabled = components.apm_mode_enabled;
    if apm_mode_enabled {
        info!("Starting APM mode event loop (connection may still be in progress)");
        execute_apm_mode_event_loop(components).await
    } else {
        debug!("Starting serverless mode event loop");
        execute_standard_mode_event_loop(components).await
    }
}

/// APM mode: immediate sending to collector, no batching, keeps buffers alive for late payloads
pub async fn execute_apm_mode_event_loop(components: &mut ExtensionComponents) -> u32 {
    let mut event_counter = 0;
    let mut cleanup_counter = 0; // Track when to run periodic cleanup
    let mut pending_flush_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    loop {
        debug!("APM mode: waiting for next lambda invocation event...");

        let runtime_event = match runtime::fetch_next_event(&components.client, &components.extension_id).await
        {
            Ok(event) => event,
            Err(e) => {
                let error_msg = e.to_string();
                // Check if this is a fatal 403 state transition error (AWS shutting down without sending SHUTDOWN event)
                if error_msg.contains("403") || error_msg.contains("State transition") {
                    error!("Fatal extension state error (403 - Lambda shutting down): {:?}", e);
                    debug!("Performing emergency shutdown cleanup...");

                    let _ = tokio::time::timeout(Duration::from_millis(SHUTDOWN_TIMEOUT_MS), async {
                        if components.config.extension.pipeline_flush {
                            for handle in pending_flush_handles.drain(..) {
                                if let Err(e) = handle.await {
                                    error!("Pipeline flush task panicked during emergency shutdown: {}", e);
                                }
                            }
                        }

                        process_pending_agent_payloads(
                            &components.config,
                            &components.global_log_processor,
                            &components.apm_app,
                            "",
                        )
                        .await;
                    }).await;

                    info!("Emergency shutdown cleanup completed. Extension exiting.");
                    break;
                }
                error!("Error receiving next event: {:?}. Continuing.", e);
                continue;
            }
        };

        event_counter += 1;
        let is_cold_start = event_counter == 1;

        match runtime_event {
            runtime::LambdaRuntimeEvent::Invoke {
                request_id,
                invoked_function_arn,
                deadline_ms,
            } => {
                let event_start = std::time::Instant::now();

                // If a prior send saw a collector restart (401/409) or disconnect (410),
                // the cached run_id is stale. Invalidate the connection so the block below
                // re-establishes a fresh handshake; buffered telemetry then retries with the
                // new run_id (see retry_buffered_telemetry's current_run_id override).
                if components.apm_mode_enabled
                    && crate::apm::collector::take_reconnect_needed()
                    && components.apm_app.read().await.is_some()
                {
                    *components.apm_app.write().await = None;
                    warn!("APM run_id invalidated (collector restart/disconnect) - will reconnect");
                }

                // Retry buffered telemetry on each invoke. APM telemetry needs a live session
                // (run_id), so it only fires when apm_app is Some. Metric API is license-key-only
                // and retries unconditionally.
                if components.apm_mode_enabled
                    && crate::apm::telemetry_buffer::get_buffer_count() > 0
                    && components.apm_app.read().await.is_some()
                {
                    let (cur_run_id, cur_collector_host) = {
                        let guard = components.apm_app.read().await;
                        match guard.as_ref() {
                            Some(app) => (Some(app.run_id.clone()), Some(app.collector_host.clone())),
                            None => (None, None),
                        }
                    };
                    let http_client = components.client.clone();
                    let license_key = components.config.new_relic.license_key.clone().unwrap_or_default();
                    tokio::spawn(async move {
                        crate::apm::telemetry_buffer::retry_buffered_telemetry(
                            &http_client,
                            &license_key,
                            cur_run_id.as_deref(),
                            cur_collector_host.as_deref(),
                        )
                        .await;
                    });
                }

                if components.apm_mode_enabled
                    && crate::apm::metric_api_buffer::get_metric_api_buffer_count() > 0
                {
                    let http_client = components.client.clone();
                    let license_key = components.config.new_relic.license_key.clone().unwrap_or_default();
                    tokio::spawn(async move {
                        crate::apm::metric_api_buffer::retry_buffered_metric_api(
                            &http_client,
                            &license_key,
                        )
                        .await;
                    });
                }

                // If APM handshake hasn't completed yet, spawn a fresh reconnect attempt.
                // The spawn is non-blocking — the invoke proceeds immediately. If
                // NEW_RELIC_APM_BLOCKING_HANDSHAKE=true the post-invoke wait may still capture
                // this invoke's data; otherwise APM data arrives on a later invoke.
                // watch::Sender guard prevents multiple concurrent reconnects.
                if components.apm_mode_enabled
                    && components.apm_app.read().await.is_none()
                    && !crate::apm::connection::is_handshake_fatal()
                {
                    if *components.reconnect_in_flight.borrow() {
                        debug!("APM handshake already in progress — skipping duplicate spawn (BLOCKING_HANDSHAKE will wait if enabled)");
                    } else {
                        components.reconnect_in_flight.send_replace(true);
                        let apm_app = components.apm_app.clone();
                        let reconnect_flag = components.reconnect_in_flight.clone();
                        let license_key = components.config.new_relic.license_key
                            .clone()
                            .unwrap_or_default();
                        let apm_host = components.config.new_relic.apm_host.clone();
                        let metric_endpoint = components.config.new_relic.metric_endpoint.clone();
                        let apm_client = components.apm_client.clone();
                        let lambda_function_name = components.config.aws.function_name.clone();
                        let function_name = std::env::var("NEW_RELIC_APP_NAME")
                            .ok()
                            .filter(|s| !s.is_empty())
                            .unwrap_or_else(|| lambda_function_name.clone());
                        let function_version = components.config.aws.function_version
                            .clone()
                            .unwrap_or_else(|| "$LATEST".to_string());
                        let account_id = components.config.aws.account_id.clone();
                        let region = components.config.aws.region.clone();
                        let timeout_secs = components.config.new_relic.apm_handshake_timeout_secs;

                        tokio::spawn(async move {
                            let _guard = ReconnectGuard(reconnect_flag);
                            debug!("APM reconnect attempt started (no delays — fresh invoke)");
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
                            )
                            .await
                            {
                                Ok(app) => {
                                    info!(
                                        "APM reconnect succeeded - Entity GUID: {}",
                                        app.get_entity_guid()
                                    );
                                    let mut w = apm_app.write().await;
                                    *w = Some(app);
                                }
                                Err(e) => {
                                    // A permanent auth failure already logged an error and
                                    // latched APM off in ApmApp::new — don't claim we'll retry.
                                    if !crate::apm::connection::is_handshake_fatal() {
                                        warn!("APM reconnect attempt failed: {} - will retry next invoke", e);
                                    }
                                }
                            }
                        });
                    }
                }

                if is_cold_start {
                    let mut updated_config = (*components.config).clone();
                    updated_config.aws.extract_and_update_account_id_from_arn(&invoked_function_arn);
                    components.config = Arc::new(updated_config);

                    match std::fs::read_to_string("/opt/newrelic/java-agent-version.txt") {
                        Ok(version) => info!("Java agent version: {}", version.trim()),
                        Err(e) => info!("Java agent version file not found at /opt/newrelic/java-agent-version.txt: {}", e),
                    }
                }

                if let Ok(mut guard) = LAST_REQUEST_CONTEXT.lock() {
                    *guard = Some((request_id.clone(), invoked_function_arn.clone()));
                }

                error_synthesis::clear_sent_errors_for_request(&request_id);

                error_synthesis::retry_failed_errors(&components.newrelic_client, &components.config).await;

                if is_cold_start && components.config.new_relic.add_version_detail_tags {
                    tag_lambda_function_once(invoked_function_arn.clone(), &components.config);
                }

                update_global_invocation_context(&request_id, &invoked_function_arn);

                // Set this as the currently active request for agent payload routing
                if let Ok(mut active_request) = request::CURRENT_ACTIVE_REQUEST_ID.lock() {
                    *active_request = Some(request_id.clone());
                }

                // Retry any previously-failed agent payloads in the background so the
                // HTTP round-trips happen during function execution, not post-runtime-done.
                {
                    let apm = components.apm_app.clone();
                    tokio::spawn(retry_failed_agent_payloads(apm));
                }

                // Create per-request state (platform_processor, agent_buffer, context)
                let request_state = create_request_processing_state(
                    &request_id,
                    &invoked_function_arn,
                    &components.processor_factory,
                );

                // Update global log processor's context to this request BEFORE processing logs
                components
                    .global_log_processor
                    .update_invocation_context(request_state.context.clone());

                // Pipeline flush: prior flushes run in parallel — don't block.
                // Drain finished handles (0ms each); leave in-flight ones running.
                if components.config.extension.pipeline_flush {
                    pending_flush_handles.retain(|h| !h.is_finished());
                    // Cap at 8 to bound memory under sustained fast invocations
                    while pending_flush_handles.len() >= 8 {
                        if let Some(oldest) = pending_flush_handles.drain(..1).next() {
                            if let Err(e) = oldest.await {
                                error!("Oldest pipeline flush task panicked: {}", e);
                            }
                        }
                    }
                }

                components
                    .global_log_processor
                    .start_invocation_retry();

                // Process pre-invoke logs FIRST (add metadata and move to batch)
                components
                    .global_log_processor
                    .process_pre_invoke_logs();

                // THEN process buffered logs (so they don't trigger auto-flush of incomplete logs)
                components
                    .global_log_processor
                    .process_buffered_logs_with_request_id(&request_id);

                REQUEST_PROCESSORS.insert(request_id.clone(), request_state);

                let buffer_count = request_data_len();
                if buffer_count > 0 {
                    debug!(
                        "APM mode: Found {} request buffer(s) before processing (current: {})",
                        buffer_count, request_id
                    );
                }

                let pending_task = tokio::spawn({
                    let config = components.config.clone();
                    let global_log_processor = components.global_log_processor.clone();
                    let apm_app = components.apm_app.clone();
                    let current_request_id = request_id.clone();

                    async move {
                        process_pending_agent_payloads(
                            &config,
                            &global_log_processor,
                            &apm_app,
                            &current_request_id,
                        )
                        .await;
                    }
                });

                let request_id_clone = request_id.clone();
                let invoked_function_arn_clone = invoked_function_arn.clone();
                let config_clone = components.config.clone();
                let global_log_processor_clone = components.global_log_processor.clone();
                let apm_app_clone = components.apm_app.clone();

                let current_task = tokio::spawn(async move {
                    process_apm_request(
                        request_id_clone,
                        invoked_function_arn_clone,
                        is_cold_start,
                        config_clone,
                        global_log_processor_clone,
                        apm_app_clone,
                        deadline_ms,
                    )
                    .await;
                });

                // apm_blocking_agent_payload wins over pipeline_flush: process_apm_request
                // (current_task) contains the bound wait for a late agent payload
                // (wait_for_late_agent_payload). If pipeline_flush deferred current_task into
                // the background here, that wait could be frozen mid-flight by the sandbox
                // freeze the same way it is today — silently defeating the delivery
                // guarantee the customer explicitly opted into. So when the customer has
                // asked for the guarantee, always take the synchronous-join path for this
                // invocation, even if pipeline_flush is also enabled. See NR-600648.
                let defer_via_pipeline_flush = should_defer_via_pipeline_flush(
                    components.config.extension.pipeline_flush,
                    components.config.new_relic.apm_blocking_agent_payload,
                );

                if defer_via_pipeline_flush {
                    let combined = tokio::spawn(async move {
                        let (r1, r2) = tokio::join!(current_task, pending_task);
                        if let Err(e) = r1 { error!("Error in APM request processing: {}", e); }
                        if let Err(e) = r2 { error!("Error in pending payload processing: {}", e); }
                    });
                    pending_flush_handles.push(combined);
                } else {
                    let (current_result, pending_result) = tokio::join!(current_task, pending_task);
                    if let Err(e) = current_result {
                        error!("Error in APM request processing: {}", e);
                    }
                    if let Err(e) = pending_result {
                        error!("Error in pending payload processing: {}", e);
                    }
                }

                // Post-invoke wait: only when NEW_RELIC_APM_BLOCKING_HANDSHAKE=true.
                // Independent of pipeline_flush — handshake establishes APM connection (one-time
                // cost on cold start), pipeline_flush defers data send (every-invoke savings).
                // Once connected, this is a no-op (reconnect_in_flight=false → instant return).
                if components.apm_mode_enabled && components.config.new_relic.apm_blocking_handshake {
                    wait_for_apm_handshake_within_budget(
                        &components.reconnect_in_flight,
                        deadline_ms,
                    )
                    .await;
                }

                if !components.config.extension.pipeline_flush {
                    let event_time = event_start.elapsed();
                    if is_cold_start {
                        debug!(
                            "COLD START: First invocation processed in {:?} (request_id: {})",
                            event_time, request_id
                        );
                        IS_WARM_START.store(true, std::sync::atomic::Ordering::Relaxed);
                    } else {
                        debug!(
                            "WARM START: Event {} processed in {:?} (request_id: {})",
                            event_counter, event_time, request_id
                        );
                    }
                } else if is_cold_start {
                    IS_WARM_START.store(true, std::sync::atomic::Ordering::Relaxed);
                }

                // Periodic cleanup: Run every 10 invocations to prevent memory leaks
                cleanup_counter += 1;
                if cleanup_counter >= 10 {
                    cleanup_counter = 0;
                    let newrelic_client = components.newrelic_client.clone();
                    let config = components.config.clone();
                    tokio::spawn(async move {
                        use crate::agent::batch::cleanup_old_batch_entries;
                        use crate::request::cleanup_old_request_buffers;
                        cleanup_old_batch_entries(newrelic_client.clone(), config.clone()).await;
                        cleanup_old_request_buffers(newrelic_client, config).await;
                        cleanup_old_failed_payloads();
                    });
                }
            }
            runtime::LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
                // Stop forwarding the extension's own shutdown-sequence logs to NR
                // (set before any shutdown log is emitted). The structured drop
                // diagnostic is sent directly below as the single NR record.
                crate::IS_SHUTTING_DOWN.store(true, std::sync::atomic::Ordering::Relaxed);
                let shutdown_start_time = std::time::Instant::now();
                info!("APM mode: Extension shutting down with reason: {} (started at {:?})", shutdown_reason, std::time::SystemTime::now());

                // Computed inside the main block, sent AFTER it with reserved budget
                // so the critical "telemetry dropped" line always reaches New Relic.
                let mut shutdown_diagnostic: Option<ShutdownDropDiagnostic> = None;

                let shutdown_result = tokio::time::timeout(Duration::from_millis(SHUTDOWN_TIMEOUT_MS - SHUTDOWN_DIAG_RESERVE_MS), async {
                if components.config.extension.pipeline_flush {
                    for handle in pending_flush_handles.drain(..) {
                        debug!("APM shutdown: awaiting in-flight pipeline flush");
                        if let Err(e) = handle.await {
                            error!("Pipeline flush task panicked during APM shutdown: {}", e);
                        }
                    }
                }

                // Synthesize and send error based on shutdown reason (to APM collector)
                if let Some((last_request_id, last_arn)) = LAST_REQUEST_CONTEXT.lock().ok().and_then(|guard| guard.clone()) {
                    let apm_app_guard = components.apm_app.read().await;
                    if let Some(ref app) = *apm_app_guard {
                        send_error_for_shutdown_reason(app, shutdown_reason, &last_request_id, &last_arn).await;
                    } else {
                        // Drop the read lock before calling write().await on the same RwLock —
                        // holding a read guard while awaiting a write lock on the same lock deadlocks.
                        drop(apm_app_guard);

                        // If APM was permanently disabled (auth rejected), a reconnect
                        // cannot succeed — don't waste the shutdown budget. The data is
                        // lost, which the earlier error log already recorded.
                        if crate::apm::connection::is_handshake_fatal() {
                            error!("APM permanently disabled (auth rejected) — shutdown error event DROPPED");
                        } else {
                        // One last synchronous attempt during shutdown — sandbox is still active
                        // for the duration of the SHUTDOWN handler so no freeze risk.
                        debug!("APM not connected at shutdown — attempting final sync reconnect");
                        let lambda_function_name = components.config.aws.function_name.clone();
                        let function_name = std::env::var("NEW_RELIC_APP_NAME")
                            .ok()
                            .filter(|s| !s.is_empty())
                            .unwrap_or_else(|| lambda_function_name.clone());
                        let shutdown_app = crate::apm::ApmApp::new(
                            components.config.new_relic.license_key.clone().unwrap_or_default(),
                            components.config.new_relic.apm_host.clone(),
                            components.config.new_relic.metric_endpoint.clone(),
                            components.apm_client.clone(),
                            function_name,
                            lambda_function_name,
                            components.config.aws.function_version.clone().unwrap_or_else(|| "$LATEST".to_string()),
                            components.config.aws.account_id.clone(),
                            components.config.aws.region.clone(),
                            // Lambda gives ~2s for SHUTDOWN; cap to avoid being killed mid-flight.
                            components.config.new_relic.apm_handshake_timeout_secs.min(2),
                        )
                        .await;

                        match shutdown_app {
                            Ok(app) => {
                                info!("APM reconnect succeeded during shutdown - Entity GUID: {}", app.get_entity_guid());
                                send_error_for_shutdown_reason(&app, shutdown_reason, &last_request_id, &last_arn).await;
                                let mut w = components.apm_app.write().await;
                                *w = Some(app);
                            }
                            Err(e) => {
                                // Shutdown is the last chance to flush; a failure here means
                                // the shutdown error event is permanently lost — log as error.
                                error!("APM not connected at shutdown and final reconnect failed: {} - shutdown error event DROPPED", e);
                            }
                        }
                        }
                    }
                }

                // CRITICAL: Process ALL remaining pending agent payloads before shutdown
                debug!("APM mode shutdown: Processing all remaining agent payloads");

                // Distinct request_ids (invocations) whose telemetry we could not
                // deliver — used to build the single shutdown summary below.
                let mut dropped_request_ids: std::collections::BTreeSet<String> =
                    std::collections::BTreeSet::new();

                // Check all request buffers for unsent payloads
                let all_request_ids: Vec<String> = REQUEST_DATA
                    .iter()
                    .map(|entry| entry.key().clone())
                    .collect();
                
                if !all_request_ids.is_empty() {
                    debug!("APM mode shutdown: Found {} request(s) with potential unsent payloads", all_request_ids.len());
                    
                    for request_id in all_request_ids {
                        if let Some(buffer) = get_agent_buffer(&request_id) {
                            let payloads = {
                                if let Ok(mut buf) = buffer.lock() {
                                    std::mem::take(&mut *buf)
                                } else {
                                    Vec::new()
                                }
                            };
                            
                            if !payloads.is_empty() {
                                info!("APM mode shutdown: Sending {} unsent payload(s) for request: {}", payloads.len(), request_id);
                                
                                let invoked_function_arn = get_request_context(&request_id)
                                    .and_then(|ctx_ref| {
                                        ctx_ref.lock()
                                            .ok()
                                            .map(|ctx| ctx.invoked_function_arn.clone())
                                            .filter(|arn| !arn.is_empty())
                                    })
                                    .unwrap_or_else(crate::get_global_fallback_arn);
                                
                                for payload_bytes in payloads {
                                    if let Err(e) = process_and_send_agent_payload(
                                        &payload_bytes,
                                        &request_id,
                                        &invoked_function_arn,
                                        &components.global_log_processor,
                                        &components.config,
                                        &components.apm_app,
                                    )
                                    .await
                                    {
                                        warn!("APM mode shutdown: Failed to send payload for {}: {}", request_id, e);
                                        dropped_request_ids.insert(request_id.clone());
                                    } else {
                                        info!("APM mode shutdown: Successfully sent payload for request: {}", request_id);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    debug!("APM mode shutdown: No pending agent payloads to process");
                }

                // Any logs still held for a request whose payload never arrived (true
                // orphans) get flushed now, untagged — last chance before the sandbox dies.
                components.global_log_processor.flush_pending_logs_unstamped();

                debug!("APM mode shutdown: Retrying all buffered telemetry");
                let license_key = components.config.new_relic.license_key.clone().unwrap_or_default();
                let (cur_run_id, cur_collector_host) = {
                    let guard = components.apm_app.read().await;
                    match guard.as_ref() {
                        Some(app) => (Some(app.run_id.clone()), Some(app.collector_host.clone())),
                        None => (None, None),
                    }
                };
                crate::apm::telemetry_buffer::retry_buffered_telemetry(
                    &components.client,
                    &license_key,
                    cur_run_id.as_deref(),
                    cur_collector_host.as_deref(),
                )
                .await;
                crate::apm::metric_api_buffer::retry_buffered_metric_api(
                    &components.client,
                    &license_key,
                )
                .await;
                // Final attempt for failed agent payloads too (APM collector), so the
                // drop count below reflects only what genuinely couldn't be delivered.
                // Keeps FAILED_AGENT_PAYLOADS symmetric with the two buffers above.
                retry_failed_agent_payloads(components.apm_app.clone()).await;

                // Gather every request_id whose data is still un-delivered:
                //  - agent payloads buffered while disconnected (the main bucket),
                //  - parsed telemetry that failed to send.
                for id in buffered_agent_payload_request_ids() {
                    dropped_request_ids.insert(id);
                }
                for id in crate::apm::telemetry_buffer::buffered_request_ids() {
                    dropped_request_ids.insert(id);
                }
                // Metric-API items count toward remaining_count below, so their requests
                // must count toward `affected` too — otherwise a metric-only request
                // inflates item count without bumping the invocation count.
                for id in crate::apm::metric_api_buffer::buffered_request_ids() {
                    dropped_request_ids.insert(id);
                }

                let remaining_count = FAILED_AGENT_PAYLOADS.lock().map(|b| b.len()).unwrap_or(0)
                    + crate::apm::telemetry_buffer::get_buffer_count()
                    + crate::apm::metric_api_buffer::get_metric_api_buffer_count();
                // Payloads already evicted/aged-out earlier (no longer in the buffer
                // to count) — add them so the total loss is honest.
                let dropped_earlier = dropped_agent_payload_count();

                if remaining_count > 0 || !dropped_request_ids.is_empty() || dropped_earlier > 0 {
                    let apm_connected = cur_run_id.is_some();
                    let affected = dropped_request_ids.len();
                    let ids: Vec<String> = {
                        let mut v: Vec<String> = dropped_request_ids.iter().cloned().collect();
                        v.sort();
                        v
                    };

                    let reason = if apm_connected {
                        String::new()
                    } else {
                        crate::apm::connection::last_failure_reason()
                            .unwrap_or_else(|| "unknown".to_string())
                    };
                    let summary = build_shutdown_drop_summary(
                        apm_connected,
                        affected,
                        remaining_count,
                        dropped_earlier,
                        &reason,
                        crate::apm::connection::connect_cycles(),
                        crate::apm::connection::connect_attempts_total(),
                    );

                    // The drop diagnostic is sent ONLY to New Relic (directly, after this
                    // block, in its reserved budget) — as the log message plus queryable
                    // attributes (dropped.request_ids / dropped.request_id_count /
                    // dropped.item_count). We deliberately do NOT also print it to stdout:
                    // that CloudWatch copy was redundant with the NR record (and was the
                    // source of the duplicate the operator saw). Other shutdown errors
                    // (e.g. the reconnect failure) are still logged to CloudWatch.
                    let last_ctx = LAST_REQUEST_CONTEXT.lock().ok().and_then(|g| g.clone());
                    let last_request_id =
                        last_ctx.as_ref().map(|(rid, _)| rid.clone()).unwrap_or_default();
                    let arn = last_ctx
                        .map(|(_, arn)| arn)
                        .filter(|a| !a.is_empty())
                        .unwrap_or_else(crate::get_global_fallback_arn);
                    shutdown_diagnostic = Some(ShutdownDropDiagnostic {
                        message: summary,
                        arn,
                        request_id: last_request_id,
                        request_ids: ids,
                        request_id_count: affected,
                        item_count: remaining_count,
                    });
                }

                // Process any pending platform.report lines as metrics (APM mode)
                let all_pending_reports: Vec<(String, String)> = REQUEST_DATA
                    .iter()
                    .filter_map(|entry| {
                        entry.pending_report.as_ref().map(|report| {
                            (entry.key().clone(), report.clone())
                        })
                    })
                    .collect();

                if !all_pending_reports.is_empty() {
                    debug!("APM mode shutdown: Found {} pending platform.report(s) to send as metrics", all_pending_reports.len());

                    let apm_app_guard = components.apm_app.read().await;
                    if let Some(ref app) = *apm_app_guard {
                        for (request_id, report_line) in all_pending_reports {
                            debug!("APM mode shutdown: Sending platform report metrics for request: {}", request_id);

                            let shutdown_arn = LAST_REQUEST_CONTEXT.lock().ok()
                                .and_then(|g| g.clone().map(|(_, arn)| arn))
                                .unwrap_or_default();
                            if let Err(e) = app.send_platform_report_metrics(&report_line, &shutdown_arn).await {
                                error!("APM mode shutdown: Failed to send platform report metrics for {}: {}", request_id, e);
                            } else {
                                info!("APM mode shutdown: Successfully sent platform report metrics for request: {}", request_id);
                            }

                            // Remove pending report after sending
                            remove_pending_report(&request_id);
                        }
                    } else {
                        warn!("APM mode shutdown: APM app not initialized - cannot send platform metrics");
                    }
                } else {
                    debug!("APM mode shutdown: No pending platform.report lines to process");
                }

                // Emergency flush of pre-invoke buffer (logs from INIT phase if shutdown before first INVOKE)
                if let Err(e) = components.global_log_processor.flush_pre_invoke_buffer_on_shutdown().await {
                    error!("APM mode shutdown: Failed to flush pre-invoke buffer: {}", e);
                }

                // Shutdown drain: flush + re-flush any entries pushed back into
                // failed_logs_buffer by send failures in the flush itself. Bounded by
                // MAX_RETRIES (via per-entry retry_count filter in start_invocation_retry).
                if let Err(e) = components.global_log_processor.flush_on_shutdown().await {
                    error!("APM mode shutdown: Failed to flush logs: {}", e);
                }
                }).await;

                // Reserved-budget delivery of the "telemetry dropped" diagnostic to
                // New Relic Logs. Runs OUTSIDE the main block (even if that timed out)
                // with its own protected window, so the one log the customer most
                // needs is not starved by a slow flush/reconnect. Direct POST to the
                // log ingest (license-key header) — does not need an APM handshake.
                if let Some(diag_data) = shutdown_diagnostic {
                    let diag = build_shutdown_drop_log(&diag_data);
                    match tokio::time::timeout(
                        Duration::from_millis(SHUTDOWN_DIAG_RESERVE_MS),
                        components.newrelic_client.send_logs(
                            components.config.as_ref(),
                            std::slice::from_ref(&diag),
                            &diag_data.arn,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(())) => info!("Forwarded shutdown drop diagnostic to New Relic Logs"),
                        Ok(Err(e)) => warn!("Could not forward shutdown diagnostic to New Relic: {}", e),
                        Err(_) => warn!("Timed out forwarding shutdown diagnostic to New Relic"),
                    }
                }

                if shutdown_result.is_err() {
                    warn!("APM shutdown timed out after {}ms — Lambda will terminate remaining work", shutdown_start_time.elapsed().as_millis());
                } else {
                    info!("APM mode shutdown: All data processed and sent in {}ms", shutdown_start_time.elapsed().as_millis());
                }
                break;
            }
        }
    }

    event_counter
}

/// Serverless mode: batches payloads with platform.report, sends to serverless API
pub async fn execute_standard_mode_event_loop(components: &mut ExtensionComponents) -> u32 {
    let mut event_counter = 0;
    let mut cleanup_counter = 0; // Track when to run periodic cleanup
    let mut pending_flush_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    loop {
        debug!("Serverless mode: waiting for next lambda invocation event...");

        let runtime_event =
            match runtime::fetch_next_event(&components.client, &components.extension_id).await {
                Ok(event) => event,
                Err(e) => {
                    let error_msg = e.to_string();
                    if error_msg.contains("403") || error_msg.contains("State transition") {
                        error!("Fatal extension state error (403 - Lambda shutting down): {:?}", e);
                        info!("Performing emergency shutdown cleanup...");

                        let _ = tokio::time::timeout(Duration::from_millis(SHUTDOWN_TIMEOUT_MS), async {
                            if components.config.extension.pipeline_flush {
                                for handle in pending_flush_handles.drain(..) {
                                    if let Err(e) = handle.await {
                                        error!("Pipeline flush task panicked during emergency shutdown: {}", e);
                                    }
                                }
                            }

                            send_batched_payloads_with_reports_only(
                                components.newrelic_client.clone(),
                                components.config.clone(),
                            )
                            .await;

                            let _ = components.global_log_processor.flush().await;
                        }).await;

                        info!("Emergency shutdown cleanup completed. Extension exiting.");
                        break;
                    }
                    error!("Error receiving next event: {:?}. Continuing.", e);
                    continue;
                }
            };

        event_counter += 1;
        crate::request::increment_invocation_counter();
        let is_cold_start = event_counter == 1;

        match runtime_event {
            runtime::LambdaRuntimeEvent::Invoke {
                request_id,
                invoked_function_arn,
                deadline_ms,
            } => {
                let event_start = std::time::Instant::now();

                // Extract real account ID from ARN on first invocation (if not already set)
                if is_cold_start {
                    let mut updated_config = (*components.config).clone();
                    updated_config.aws.extract_and_update_account_id_from_arn(&invoked_function_arn);
                    components.config = Arc::new(updated_config);
                }

                // Track this request for potential error synthesis on shutdown
                if let Ok(mut guard) = LAST_REQUEST_CONTEXT.lock() {
                    *guard = Some((request_id.clone(), invoked_function_arn.clone()));
                }

                error_synthesis::clear_sent_errors_for_request(&request_id);

                // Spawn error and telemetry retries into the background so they run during
                // function execution, not pre-invoke. Lambda freeze suspends the tokio runtime,
                // so sleeping/retrying tasks are paused and resume cleanly on unfreeze —
                // retry counts are only incremented on actual HTTP failures, not on suspensions.
                {
                    let nr = components.newrelic_client.clone();
                    let cfg = components.config.clone();
                    tokio::spawn(async move {
                        error_synthesis::retry_failed_errors(&nr, &cfg).await;
                    });
                }
                {
                    let http_client = components.client.clone();
                    let license_key = components.config.new_relic.license_key.clone();
                    tokio::spawn(async move {
                        crate::apm::telemetry_buffer::retry_buffered_telemetry(
                            &http_client,
                            license_key.as_deref().unwrap_or(""),
                            None,
                            None,
                        )
                        .await;
                    });
                }

                if is_cold_start && components.config.new_relic.add_version_detail_tags {
                    tag_lambda_function_once(invoked_function_arn.clone(), &components.config);
                }

                update_global_invocation_context(&request_id, &invoked_function_arn);

                // Set this as the currently active request for agent payload routing
                if let Ok(mut active_request) = request::CURRENT_ACTIVE_REQUEST_ID.lock() {
                    *active_request = Some(request_id.clone());
                }

                // On warm starts, pair any previous-request agent payloads that arrived after
                // their invocation ended but whose platform.report has since been stored.
                // Must run after updating CURRENT_ACTIVE_REQUEST_ID so new pipe payloads
                // route to the current request, not old buffers.
                if !is_cold_start {
                    let paired = drain_late_paired_payloads_serverless(
                        &request_id,
                        &components.config,
                        &components.global_log_processor,
                    )
                    .await;
                    if paired > 0 {
                        debug!(
                            "Serverless mode: Paired {} late agent payload(s) into batch at start of invocation: {}",
                            paired, request_id
                        );
                    }
                }

                // Create per-request state (platform_processor, agent_buffer, context)
                let request_state = create_request_processing_state(
                    &request_id,
                    &invoked_function_arn,
                    &components.processor_factory,
                );

                // Update global log processor's context to this request BEFORE processing logs
                components
                    .global_log_processor
                    .update_invocation_context(request_state.context.clone());

                // Pipeline flush: prior flushes run in parallel — don't block.
                // Drain finished handles (0ms each); leave in-flight ones running.
                if components.config.extension.pipeline_flush {
                    pending_flush_handles.retain(|h| !h.is_finished());
                    // Cap at 8 to bound memory under sustained fast invocations
                    while pending_flush_handles.len() >= 8 {
                        if let Some(oldest) = pending_flush_handles.drain(..1).next() {
                            if let Err(e) = oldest.await {
                                error!("Oldest pipeline flush task panicked: {}", e);
                            }
                        }
                    }
                }

                components
                    .global_log_processor
                    .start_invocation_retry();

                // Process buffered logs and pre-invoke logs using global log processor
                components
                    .global_log_processor
                    .process_buffered_logs_with_request_id(&request_id);
                components
                    .global_log_processor
                    .process_pre_invoke_logs();

                // Send batch if threshold is reached after processing late payloads (only with report lines)
                // CRITICAL: Must await to prevent Lambda from freezing network mid-request
                if should_send_batch_by_threshold() {
                    debug!("Batch threshold reached - sending payloads with report lines only");
                    send_batched_payloads_with_reports_only(
                        components.newrelic_client.clone(),
                        components.config.clone()
                    ).await;
                }

                REQUEST_PROCESSORS.insert(request_id.clone(), request_state);

                let request_id_clone = request_id.clone();
                let invoked_function_arn_clone = invoked_function_arn.clone();
                let newrelic_client_clone = components.newrelic_client.clone();
                let config_clone = components.config.clone();
                let global_log_processor_clone = components.global_log_processor.clone();

                let processing_handle = tokio::spawn(async move {
                    process_request_concurrently(
                        request_id_clone,
                        invoked_function_arn_clone,
                        newrelic_client_clone,
                        config_clone,
                        global_log_processor_clone,
                        deadline_ms,
                    )
                    .await;
                });

                if components.config.extension.pipeline_flush {
                    pending_flush_handles.push(processing_handle);
                } else {
                    if let Err(e) = processing_handle.await {
                        error!("Error in standard mode request processing: {}", e);
                    }
                }

                if !components.config.extension.pipeline_flush {
                    let event_time = event_start.elapsed();
                    if is_cold_start {
                        debug!(
                            "COLD START: First invocation processed in {:?} (request_id: {})",
                            event_time, request_id
                        );
                        IS_WARM_START.store(true, std::sync::atomic::Ordering::Relaxed);
                    } else {
                        debug!(
                            "WARM START: Event {} processed in {:?} (request_id: {})",
                            event_counter, event_time, request_id
                        );
                    }
                } else if is_cold_start {
                    IS_WARM_START.store(true, std::sync::atomic::Ordering::Relaxed);
                }

                // Periodic cleanup: Run every 10 invocations to prevent memory leaks
                cleanup_counter += 1;
                if cleanup_counter >= 10 {
                    cleanup_counter = 0;
                    let newrelic_client = components.newrelic_client.clone();
                    let config = components.config.clone();
                    tokio::spawn(async move {
                        use crate::agent::batch::cleanup_old_batch_entries;
                        use crate::request::cleanup_old_request_buffers;
                        cleanup_old_batch_entries(newrelic_client.clone(), config.clone()).await;
                        cleanup_old_request_buffers(newrelic_client, config).await;
                    });
                }
            }
            runtime::LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
                // Stop forwarding the extension's own shutdown-sequence logs to NR
                // (set before any shutdown log is emitted).
                crate::IS_SHUTTING_DOWN.store(true, std::sync::atomic::Ordering::Relaxed);
                let shutdown_start_time = std::time::Instant::now();
                info!("Serverless mode: Extension shutting down with reason: {} (started at {:?})", shutdown_reason, std::time::SystemTime::now());

                let shutdown_timeout_result = tokio::time::timeout(Duration::from_millis(SHUTDOWN_TIMEOUT_MS), async {
                if components.config.extension.pipeline_flush {
                    for handle in pending_flush_handles.drain(..) {
                        debug!("Shutdown: awaiting in-flight pipeline flush");
                        if let Err(e) = handle.await {
                            error!("Pipeline flush task panicked during shutdown: {}", e);
                        }
                    }
                }

                // Synthesize and send error based on shutdown reason
                if let Some((last_request_id, last_arn)) = LAST_REQUEST_CONTEXT.lock().ok().and_then(|guard| guard.clone()) {
                    match shutdown_reason {
                        runtime::ShutdownReason::Timeout => {
                            // Lambda timeout - send timeout error with reason
                            debug!("Shutdown due to timeout - synthesizing timeout error for request: {}", last_request_id);
                            error_synthesis::send_timeout_error(
                                &last_request_id,
                                &last_arn,
                                None,
                                &components.newrelic_client,
                                &components.config,
                            )
                            .await;
                        }
                        runtime::ShutdownReason::Failure => {
                            // Lambda failure/fault - send platform fault error
                            debug!("Shutdown due to failure - synthesizing fault error for request: {}", last_request_id);
                            error_synthesis::send_platform_fault_error(
                                &last_request_id,
                                &last_arn,
                                &components.newrelic_client,
                                &components.config,
                            )
                            .await;
                        }
                        runtime::ShutdownReason::Spindown => {
                            // Normal shutdown - no error needed
                            debug!("Normal spindown shutdown - no error synthesis needed");
                        }
                        runtime::ShutdownReason::Unknown => {
                            // Unknown/unexpected shutdown reason - send generic error
                            warn!("Unknown shutdown reason - synthesizing generic error for request: {}", last_request_id);
                            error_synthesis::send_lambda_error(
                                &format!("Lambda shutdown with unknown reason"),
                                &last_request_id,
                                &last_arn,
                                "LambdaShutdown",
                                &components.newrelic_client,
                                &components.config,
                            )
                            .await;
                        }
                    }
                }

                // Yield to let the IPC pipe collector task route any last in-flight payloads
                // into agent_buffer before we collect from REQUEST_DATA below.
                // The collector runs in a separate tokio task; two yields ensure items
                // already in the mpsc channel are routed before the shutdown collect.
                tokio::task::yield_now().await;
                tokio::task::yield_now().await;

                // CRITICAL: Send ALL remaining payloads at shutdown (with or without reports)
                debug!("Standard mode shutdown: Sending ALL remaining payloads (including those without reports)");
                send_all_pending_payloads_on_shutdown(
                    components.newrelic_client.clone(),
                    components.config.clone(),
                    Some(&components.global_log_processor),
                )
                .await;

                // Flush any logs still held for a request whose payload never arrived,
                // untagged — last chance before the sandbox dies.
                components.global_log_processor.flush_pending_logs_unstamped();

                // Flush pre-invoke buffer and main log buffer concurrently.
                // Both paths write to independent payloads (pre-invoke vs main batch),
                // so parallel execution is safe and cuts shutdown latency in half
                // when both are non-empty. Each path already chunks at 1MB.
                let (pre_invoke_result, flush_result) = tokio::join!(
                    components.global_log_processor.flush_pre_invoke_buffer_on_shutdown(),
                    components.global_log_processor.flush_on_shutdown(),
                );
                if let Err(e) = pre_invoke_result {
                    error!("Standard mode shutdown: Failed to flush pre-invoke buffer: {}", e);
                }
                if let Err(e) = flush_result {
                    error!("Standard mode shutdown: Failed to flush logs: {}", e);
                }
                }).await;

                if shutdown_timeout_result.is_err() {
                    warn!("Serverless shutdown timed out after {}ms — Lambda will terminate remaining work", shutdown_start_time.elapsed().as_millis());
                } else {
                    info!("Standard mode shutdown: All data processed and sent in {}ms", shutdown_start_time.elapsed().as_millis());
                }
                break;
            }
        }
    }

    event_counter
}

pub async fn execute_noop_event_loop(client: &Arc<Client>, extension_id: &str) {
    info!("Starting no-op mode, no telemetry will be sent");

    loop {
        let loop_start = std::time::Instant::now();
        match runtime::fetch_next_event(client, extension_id).await {
            Ok(runtime::LambdaRuntimeEvent::Shutdown { shutdown_reason: _ }) => {
                debug!("Extension shutting down");
                break;
            }
            Ok(runtime::LambdaRuntimeEvent::Invoke {
                request_id,
                invoked_function_arn: _,
                deadline_ms: _,
            }) => {
                trace!(
                    "No-op mode invocation processed in {:?} (request_id: {})",
                    loop_start.elapsed(),
                    request_id
                );
            }
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("403") || error_msg.contains("State transition") {
                    error!("Fatal extension state error (403 - Lambda shutting down): {:?}", e);
                    debug!("No-op mode exiting due to Lambda shutdown");
                    break;
                }
                error!("Error in no-op event loop: {:?}. Continuing.", e);
            }
        }
    }
}

pub async fn process_apm_request(
    request_id: String,
    invoked_function_arn: String,
    is_cold_start: bool,
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
    apm_app: crate::apm::SharedApmApp,
    deadline_ms: i64,
) {
    debug!("APM mode: Starting processing for request: {}", request_id);


    if !is_cold_start {
        // Atomically drain old request buffers in a single lock to avoid
        // TOCTOU race between emptiness check and mem::take.
        let pending_with_payloads: Vec<(String, Vec<Vec<u8>>)> = REQUEST_DATA
            .iter()
            .filter_map(|entry| {
                let req_id = entry.key();
                if req_id != &request_id {
                    if let Ok(mut buffer) = entry.agent_buffer.lock() {
                        if !buffer.is_empty() {
                            return Some((req_id.clone(), std::mem::take(&mut *buffer)));
                        }
                    }
                }
                None
            })
            .collect();

        if !pending_with_payloads.is_empty() {
            debug!(
                "APM warm start: Found {} pending late agent payload(s) from previous invocations - processing now",
                pending_with_payloads.len()
            );

            for (old_request_id, late_payloads) in pending_with_payloads {
                // Send all late payloads concurrently — each is an independent HTTP POST
                // that only takes a read lock on apm_app (no write contention).
                let mut set = tokio::task::JoinSet::new();
                for payload_bytes in late_payloads {
                    let rid = old_request_id.clone();
                    let apm = apm_app.clone();
                    set.spawn(async move {
                        if let Err(e) = send_to_apm_collector(&payload_bytes, &rid, &apm).await {
                            error!("Failed to send late agent payload for {}: {}", rid, e);
                        } else {
                            info!("Successfully sent late agent payload for request: {}", rid);
                        }
                    });
                }
                while set.join_next().await.is_some() {}

                cleanup_request_processing_state_internal(&old_request_id, false);
            }
        }
    }

    let state = REQUEST_PROCESSORS.remove(&request_id).map(|(_k, v)| v);

    let Some(state) = state else {
        error!("No processing state found for request: {}", request_id);
        return;
    };

    let invocation_start_time = chrono::Utc::now();
    global_log_processor.set_invocation_start_time(invocation_start_time);
    // trace.id buffering is per-request (keyed by request_id) inside LogProcessor, so
    // there's no per-invocation state to reset here: the previous request's logs are
    // stamped + flushed when its deferred agent payload is processed
    // (process_pending_agent_payloads -> on_trace_id_extracted), independent of this
    // invocation.
    state
        .platform_processor
        .process_invoke_event(&request_id, &invoked_function_arn);

    // APM mode: Check if run_id is available
    let has_run_id = {
        let apm_app_guard = apm_app.read().await;
        apm_app_guard.is_some()
    };

    // Check if we have agent payload
    let agent_payloads = {
        if let Ok(mut buffer) = state.agent_buffer.lock() {
            std::mem::take(&mut *buffer)
        } else {
            Vec::new()
        }
    };

    let got_payload = !agent_payloads.is_empty();

    // Flow 1: If run_id exists and payload arrived, send it immediately
    // Flow 2: If run_id exists but no payload, either bound-wait for it (when
    //         NEW_RELIC_APM_BLOCKING_AGENT_PAYLOAD is enabled) or leave it for the
    //         next invocation / shutdown (default)
    // Flow 3: If no run_id, buffer the payload for when run_id arrives (or shutdown)
    let send_agent_task = if has_run_id && got_payload {
        debug!(
            "APM mode: run_id available + agent payload arrived ({} payload(s)) - sending immediately",
            agent_payloads.len()
        );
        Some(spawn_send_agent_task(
            request_id.clone(),
            config.clone(),
            global_log_processor.clone(),
            apm_app.clone(),
            invoked_function_arn.clone(),
            agent_payloads,
        ))
    } else if !has_run_id && got_payload {
        debug!(
            "APM mode: No run_id yet but agent payload arrived ({} payload(s)) - buffering for when run_id becomes available",
            agent_payloads.len()
        );
        // Put payloads back in buffer to send when run_id arrives
        if let Some(buffer_ref) = get_agent_buffer(&request_id) {
            if let Ok(mut buffer) = buffer_ref.lock() {
                buffer.extend(agent_payloads);
            }
        }
        None
    } else if has_run_id && !got_payload {
        if config.new_relic.apm_blocking_agent_payload {
            let late_payloads = wait_for_late_agent_payload(&request_id, deadline_ms, &config).await;
            if !late_payloads.is_empty() {
                debug!(
                    "APM mode: late agent payload arrived within invocation window ({} payload(s)) for request: {} - sending now",
                    late_payloads.len(), request_id
                );
                Some(spawn_send_agent_task(
                    request_id.clone(),
                    config.clone(),
                    global_log_processor.clone(),
                    apm_app.clone(),
                    invoked_function_arn.clone(),
                    late_payloads,
                ))
            } else {
                debug!(
                    "APM mode: run_id available, no agent payload within blocking-payload timeout for request: {} - will catch on next invocation or shutdown",
                    request_id
                );
                None
            }
        } else {
            debug!(
                "APM mode: run_id available but no agent payload yet for request: {} - will catch in next invocation if it arrives late",
                request_id
            );
            None
        }
    } else {
        debug!(
            "APM mode: No run_id and no agent payload for request: {} - normal flow",
            request_id
        );
        None
    };

    // Spawn agent send in background - don't wait for it to complete
    // This ensures we return to /next quickly without blocking on agent sends
    if let Some(handle) = send_agent_task {
        tokio::spawn(async move {
            match handle.await {
                Ok((success, payloads)) => {
                    if !success && !payloads.is_empty() {
                        debug!("APM mode: agent send had failures — unsent payloads buffered for retry (next invoke / shutdown)");
                    }
                }
                Err(e) => {
                    error!("Agent send task failed: {}", e);
                }
            }
        });
    }

    // Check for pending platform.report and send as metrics to Metric API (APM mode)
    if let Some(report_line) = get_pending_report(&request_id) {
        debug!("APM mode: Found platform.report for request {} - converting to metrics", request_id);

        let apm_app_guard = apm_app.read().await;
        if let Some(ref app) = *apm_app_guard {
            if let Err(e) = app.send_platform_report_metrics(&report_line, &invoked_function_arn).await {
                error!("APM mode: Failed to send platform report metrics for {}: {}", request_id, e);
            } else {
                info!("APM mode: Successfully sent platform report metrics for request {}", request_id);
            }
        } else {
            warn!("APM mode: APM app not ready - cannot send platform metrics for {}", request_id);
        }
        drop(apm_app_guard);

        // Remove pending report after sending
        remove_pending_report(&request_id);
    } else {
        debug!("APM mode: No platform.report found for request {} (may arrive in next invocation)", request_id);
    }

    wait_for_runtime_done_with_grace(
        &request_id,
        deadline_ms,
        &config,
        &global_log_processor,
    )
    .await;

    // Wait for logs and platform to complete before returning
    let log_flushing = global_log_processor.flush();
    let platform_flushing = state.platform_processor.flush();

    let (log_result, platform_result) = tokio::join!(
        log_flushing,
        platform_flushing,
    );

    if let Err(e) = log_result {
        error!("Failed to flush logs for request {}: {}", request_id, e);
    }
    if let Err(e) = platform_result {
        error!("Failed to flush platform for request {}: {}", request_id, e);
    }

    // Agent payloads arrive via the named pipe independently and are already sent above.
    // Late agent payloads will be caught by the warm-start pending-payload logic.

    cleanup_request_processing_state_internal(&request_id, true);


    debug!(
        "APM mode: Completed processing for request: {}",
        request_id
    );
}

/// Send agent payload to APM collector (parses and sends 5 telemetry types)
async fn send_to_apm_collector(
    payload_bytes: &[u8],
    request_id: &str,
    apm_app: &crate::apm::SharedApmApp,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let apm_app_guard = apm_app.read().await;
    if let Some(ref app) = *apm_app_guard {
        debug!(
            "APM mode: Processing agent payload for request: {} (size: {} bytes)",
            request_id,
            payload_bytes.len()
        );
        app.process_agent_payload(payload_bytes.to_vec(), request_id).await?;
        info!(
            "APM mode: Agent payload sent successfully for request: {}",
            request_id
        );
    } else {
        // APM connection still in progress - buffer will be kept for retry
        warn!(
            "APM connection still in progress - payload for {} will be buffered and retried",
            request_id
        );
        return Err("APM connection not ready yet - payload buffered for retry".into());
    }
    Ok(())
}

/// Send one agent payload to the APM collector; on failure buffer it into
/// `FAILED_AGENT_PAYLOADS` so `retry_failed_agent_payloads` resends it on the
/// next invoke / at shutdown. Returns `true` on success, `false` on a
/// (now-buffered) failure. Mirrors `process_and_send_agent_payload` so the
/// Flow-1 immediate-send path can never silently drop a payload.
async fn send_agent_payload_or_buffer(
    payload_bytes: &[u8],
    request_id: &str,
    invoked_function_arn: &str,
    apm_app: &crate::apm::SharedApmApp,
) -> bool {
    match send_to_apm_collector(payload_bytes, request_id, apm_app).await {
        Ok(()) => true,
        Err(e) => {
            warn!(
                "APM mode: Agent payload send failed for {}, buffering for retry: {}",
                request_id, e
            );
            buffer_failed_agent_payload(payload_bytes, request_id, invoked_function_arn);
            false
        }
    }
}

/// Whether `execute_apm_mode_event_loop` should defer `process_apm_request` into the
/// background `pending_flush_handles` queue (the `pipeline_flush` optimization) rather
/// than synchronously `tokio::join!`-ing it before calling `/next` again.
///
/// `apm_blocking_agent_payload` always wins: deferring would let the sandbox freeze
/// mid-`wait_for_late_agent_payload`, silently defeating the delivery guarantee the
/// customer explicitly opted into (NR-600648). Extracted as a pure function so the
/// precedence rule is unit-testable without spinning up the event loop.
fn should_defer_via_pipeline_flush(pipeline_flush: bool, apm_blocking_agent_payload: bool) -> bool {
    pipeline_flush && !apm_blocking_agent_payload
}

/// Spawn a task that sends every payload in `agent_payloads` to the APM collector via
/// `send_agent_payload_or_buffer` (never-drop-on-failure). Shared by Flow 1
/// (`has_run_id && got_payload`) and the recovered-Flow-2 case in
/// `wait_for_late_agent_payload`, so both build an identical `JoinHandle` and the
/// caller's `if let Some(handle) = send_agent_task { tokio::spawn(...) }` dispatch
/// needs no per-flow branching.
fn spawn_send_agent_task(
    request_id: String,
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
    apm_app: crate::apm::SharedApmApp,
    invoked_function_arn: String,
    agent_payloads: Vec<Vec<u8>>,
) -> tokio::task::JoinHandle<(bool, Vec<Vec<u8>>)> {
    tokio::spawn(async move {
        let mut all_sent = true;
        for payload_bytes in &agent_payloads {
            extract_and_coordinate_trace_id(
                payload_bytes,
                &request_id,
                &config,
                &global_log_processor,
            )
            .await;

            // On failure the payload is buffered into FAILED_AGENT_PAYLOADS
            // (never silently dropped); the global retry loop resends it on a
            // later invoke / at shutdown. Mirrors process_and_send_agent_payload.
            if !send_agent_payload_or_buffer(
                payload_bytes,
                &request_id,
                &invoked_function_arn,
                &apm_app,
            )
            .await
            {
                all_sent = false;
            }
        }
        (all_sent, agent_payloads)
    })
}

/// Bound-wait for a late agent payload to arrive on the named pipe for `request_id`,
/// only ever called when `NEW_RELIC_APM_BLOCKING_AGENT_PAYLOAD` is enabled (see
/// `process_apm_request`'s Flow 2 arm). Closes the gap where the agent's telemetry for
/// this invocation lands on `agent_buffer` a few milliseconds after the Flow-1 snapshot
/// already found it empty — without this wait, that payload would otherwise only be
/// picked up on the next invocation's warm-start drain or at `SHUTDOWN` (NR-600648).
///
/// Modeled directly on `wait_for_apm_handshake_within_budget`: same `SAFETY_MARGIN_MS`
/// deadline-budget pattern, so the wait can never exceed the invocation's own deadline
/// regardless of how `NEW_RELIC_APM_AGENT_PAYLOAD_TIMEOUT_MS` is configured. Uses the
/// same "arm-before-recheck" idiom as `wait_for_runtime_done_with_grace`'s drain-notify
/// wait to avoid a TOCTOU gap between the Flow-1 snapshot and this await.
async fn wait_for_late_agent_payload(
    request_id: &str,
    deadline_ms: i64,
    config: &ExtensionConfig,
) -> Vec<Vec<u8>> {
    let Some(notify) = request::get_agent_payload_notify(request_id) else {
        return Vec::new();
    };

    let notified = notify.notified();
    tokio::pin!(notified);
    // enable() registers the subscription so any concurrent notify_one() fired
    // between here and the take_agent_buffer_if_nonempty() recheck below is captured,
    // rather than being missed in the gap between the Flow-1 snapshot and this wait.
    notified.as_mut().enable();

    if let Some(payloads) = request::take_agent_buffer_if_nonempty(request_id) {
        return payloads; // landed between the Flow-1 snapshot and here
    }

    let now_ms = chrono::Utc::now().timestamp_millis();
    let deadline_budget_ms = if deadline_ms > now_ms {
        ((deadline_ms - now_ms) as u64).saturating_sub(INVOKE_DEADLINE_SAFETY_MARGIN_MS)
    } else {
        0
    };
    let budget_ms = config.new_relic.apm_agent_payload_timeout_ms.min(deadline_budget_ms);
    if budget_ms == 0 {
        debug!(
            "APM blocking-agent-payload: no deadline budget remaining to wait (request: {})",
            request_id
        );
        return Vec::new();
    }

    debug!(
        "APM mode: blocking-agent-payload waiting up to {}ms within deadline (request: {})",
        budget_ms, request_id
    );

    if tokio::time::timeout(Duration::from_millis(budget_ms), notified).await.is_ok() {
        request::take_agent_buffer_if_nonempty(request_id).unwrap_or_default()
    } else {
        Vec::new()
    }
}

/// Wait for `platform.runtimeDone` for this request, then give a short grace for
/// trailing telemetry, then return. Used by both serverless mode and APM mode before
/// the end-of-invocation flush so late logs land in `log_batch` before it drains.
///
/// Bounds:
/// - Upper bound on the runtime.done wait = function's own deadline (`deadlineMs`
///   from the INVOKE event), clamped to Lambda's 15 min ceiling. Never outlives
///   the function. Falls back to 5 s if `deadline_ms` is missing/stale.
/// - Grace after runtime.done = `NEW_RELIC_RUNTIME_DONE_GRACE_MS` (default 25 ms,
///   clamp `[0, 2000]`). Skipped entirely when `log_processor.is_drained()` is true.
///
/// Notify is pre-armable: if `runtime.done` fired before we reach this point,
/// `notified()` returns immediately.
async fn wait_for_runtime_done_with_grace(
    request_id: &str,
    deadline_ms: i64,
    config: &ExtensionConfig,
    log_processor: &Arc<LogProcessor>,
) {
    if !config.extension.send_function_logs {
        return;
    }
    let Some(notify) = request::get_runtime_done_notify(request_id) else {
        return;
    };

    const MAX_RUNTIME_DONE_WAIT_MS: u64 = 15 * 60 * 1_000;
    const FALLBACK_RUNTIME_DONE_WAIT_MS: u64 = 5_000;

    let now_ms = chrono::Utc::now().timestamp_millis();
    let remaining_ms: u64 = if deadline_ms > now_ms {
        ((deadline_ms - now_ms) as u64).min(MAX_RUNTIME_DONE_WAIT_MS)
    } else {
        warn!(
            "INVOKE for request {} missing/stale deadlineMs ({}); using {}ms fallback for runtime.done wait",
            request_id, deadline_ms, FALLBACK_RUNTIME_DONE_WAIT_MS
        );
        FALLBACK_RUNTIME_DONE_WAIT_MS
    };

    let signal = tokio::select! {
        _ = notify.notified() => true,
        _ = tokio::time::sleep(Duration::from_millis(remaining_ms)) => false,
    };

    if signal {
        debug!(
            "runtime.done signal received for request: {} (after {}ms)",
            request_id,
            chrono::Utc::now().timestamp_millis().saturating_sub(now_ms)
        );
        if !log_processor.is_drained() {
            let grace_ms = config.extension.runtime_done_grace_ms;
            if grace_ms > 0 {
                // Subscribe to drain notification BEFORE re-checking is_drained()
                // to avoid a TOCTOU window where notify_one() fires between the
                // is_drained() check above and the notified().await below.
                let notify = log_processor.drain_notify();
                let notified = notify.notified();
                tokio::pin!(notified);
                // enable() registers the subscription so any concurrent notify_one()
                // fired between here and the await is captured.
                notified.as_mut().enable();

                if !log_processor.is_drained() {
                    debug!(
                        "runtime.done: batch not drained - awaiting drain notify (up to {}ms grace, request: {})",
                        grace_ms, request_id
                    );
                    let grace_start = tokio::time::Instant::now();
                    let _ = tokio::time::timeout(
                        Duration::from_millis(grace_ms),
                        notified,
                    )
                    .await;
                    debug!(
                        "runtime.done: grace period ended after {}ms / {}ms max (request: {})",
                        grace_start.elapsed().as_millis(),
                        grace_ms,
                        request_id
                    );
                }
            }
        } else {
            debug!(
                "runtime.done: batch already drained for request: {} - skipping grace",
                request_id
            );
        }
    } else {
        debug!(
            "runtime.done wait reached function deadline ({}ms) for request: {} - flushing anyway",
            remaining_ms, request_id
        );
    }
}

/// Sends the appropriate APM error event for a given shutdown reason.
/// Called from the SHUTDOWN handler whether APM was already connected or just reconnected.
async fn send_error_for_shutdown_reason(
    app: &crate::apm::ApmApp,
    reason: runtime::ShutdownReason,
    request_id: &str,
    arn: &str,
) {
    match reason {
        runtime::ShutdownReason::Timeout => {
            info!("Shutdown due to timeout - sending error event to APM for request: {}", request_id);
            if let Err(e) = app.send_shutdown_error_event("LambdaTimeout", "Task timed out", request_id, arn).await {
                error!("Failed to send timeout error event to APM: {}", e);
            }
        }
        runtime::ShutdownReason::Failure => {
            info!("Shutdown due to failure - sending error event to APM for request: {}", request_id);
            if let Err(e) = app.send_shutdown_error_event("LambdaPlatformFault", "AWS Lambda platform fault caused a shutdown", request_id, arn).await {
                error!("Failed to send platform fault error event to APM: {}", e);
            }
        }
        runtime::ShutdownReason::Spindown => {
            debug!("Normal spindown shutdown - no error event needed");
        }
        runtime::ShutdownReason::Unknown => {
            warn!("Unknown shutdown reason - sending error event to APM for request: {}", request_id);
            if let Err(e) = app.send_shutdown_error_event("LambdaShutdown", "Lambda shutdown with unknown reason", request_id, arn).await {
                error!("Failed to send shutdown error event to APM: {}", e);
            }
        }
    }
}

/// After `platform.runtimeDone`, use remaining deadline budget to let an in-flight
/// APM handshake complete before calling /next. Sandbox stays active while the
/// extension has not yet called /next — no freeze risk. Bounded by deadline_ms.
async fn wait_for_apm_handshake_within_budget(
    reconnect_sender: &watch::Sender<bool>,
    deadline_ms: i64,
) {
    if !*reconnect_sender.borrow() {
        return;
    }
    let now_ms = chrono::Utc::now().timestamp_millis();
    let budget_ms = if deadline_ms > now_ms {
        ((deadline_ms - now_ms) as u64).saturating_sub(INVOKE_DEADLINE_SAFETY_MARGIN_MS)
    } else {
        0
    };
    if budget_ms == 0 {
        debug!("APM handshake in-flight but no deadline budget remaining — skipping wait");
        return;
    }
    debug!(
        "APM handshake in-flight after runtimeDone — waiting up to {}ms within invoke deadline",
        budget_ms
    );
    let mut rx = reconnect_sender.subscribe();
    let _ = tokio::time::timeout(
        Duration::from_millis(budget_ms),
        rx.wait_for(|in_flight| !in_flight),
    )
    .await;
}

pub async fn process_request_concurrently(
    request_id: String,
    invoked_function_arn: String,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
    deadline_ms: i64,
) {
    debug!(
        "Serverless mode: Starting processing for request: {}",
        request_id
    );


    let state = REQUEST_PROCESSORS.remove(&request_id).map(|(_, v)| v);

    let Some(state) = state else {
        error!("No processing state found for request: {}", request_id);
        return;
    };

    let invocation_start_time = chrono::Utc::now();
    global_log_processor.set_invocation_start_time(invocation_start_time);
    // trace.id buffering is per-request (keyed by request_id) inside LogProcessor — no
    // per-invocation reset needed; each request's held logs are stamped + flushed when
    // its deferred agent payload is processed.
    state
        .platform_processor
        .process_invoke_event(&request_id, &invoked_function_arn);

    // Drain whatever the background pipe listener has already routed to this request's buffer.
    // No waiting — if the agent payload hasn't arrived yet, the Telemetry listener will match
    // it with the platform.report when both arrive (same or next invocation) via the
    // agent_buffer → AGENT_BATCH_BUFFER pairing in listener.rs.
    let agent_payloads = {
        if let Ok(mut buffer) = state.agent_buffer.lock() {
            std::mem::take(&mut *buffer)
        } else {
            Vec::new()
        }
    };

    if agent_payloads.is_empty() {
        debug!("Serverless mode: No agent payload in buffer for request: {} - will be matched when it arrives", request_id);
    } else {
        debug!("Serverless mode: {} agent payload(s) in buffer for request: {}", agent_payloads.len(), request_id);
        // Extract this request's trace.id from its payload(s) and stamp + flush its
        // held logs. The trace lives in the payload (independent of the platform.report),
        // so do this as soon as the payload is in hand — even if it gets put back to
        // wait for its report below. Mirrors the APM path's extract_and_coordinate_trace_id.
        if config.new_relic.collect_trace_id {
            for payload_bytes in &agent_payloads {
                extract_and_coordinate_trace_id(payload_bytes, &request_id, &config, &global_log_processor).await;
            }
        }
    }

    let report_line = remove_pending_report(&request_id).map(|report| {
        debug!(
            "Found pending platform.report for request: {}",
            request_id
        );
        report
    });

    // Check if there was a detected error but no agent payload
    // This can happen when the function code has errors but doesn't send telemetry
    if agent_payloads.is_empty() {
        if let Ok(guard) = crate::error_synthesis::LAST_DETECTED_ERROR.lock() {
            if let Some(ref detected_error) = *guard {
                if detected_error.request_id == request_id {
                    debug!(
                        "Serverless mode: No agent payload for request {} but error detected: {} - sending to telemetry",
                        request_id, detected_error.error_type
                    );
                    // Error was already sent by log processor, just log this for visibility
                }
            }
        }
    }

    // Smart batching: Only send complete payloads (with report)
    let send_agent_task = if agent_payloads.is_empty() {
        debug!("Serverless mode: No agent payload for request: {}", request_id);
        None
    } else if let Some(ref report) = report_line {
        // Both payload and report available - send now (complete data)
        debug!(
            "Serverless mode: Payload + report both ready for {} - adding to batch",
            request_id
        );

        for payload_bytes in agent_payloads {
            add_to_batch(
                request_id.clone(),
                payload_bytes,
                Some(report.clone()),
                invoked_function_arn.clone(),
            );
        }

        // Check if batch threshold is met and send if needed (only payloads with report lines)
        if should_send_batch_by_threshold() {
            debug!("Batch threshold reached - sending payloads with report lines only");
            let newrelic_client_clone = newrelic_client.clone();
            let config_clone = config.clone();

            Some(tokio::spawn(async move {
                send_batched_payloads_with_reports_only(newrelic_client_clone, config_clone).await;
            }))
        } else {
            None
        }
    } else {
        // Only payload, no report yet - put back in buffer for next invocation
        debug!(
            "Serverless mode: Payload ready but NO report yet for {} - keeping in buffer",
            request_id
        );
        debug!(
            "Payload will be sent in next invocation when platform.report arrives"
        );

        // Put payloads back in buffer (they were taken out with mem::take)
        if let Some(buffer_ref) = get_agent_buffer(&request_id) {
            if let Ok(mut buffer) = buffer_ref.lock() {
                buffer.extend(agent_payloads);
            }
        }

        None
    };

    wait_for_runtime_done_with_grace(
        &request_id,
        deadline_ms,
        &config,
        &global_log_processor,
    )
    .await;

    let log_flushing = global_log_processor.flush();
    let platform_flushing = state.platform_processor.flush();

    let (log_result, platform_result, agent_result) = tokio::join!(
        log_flushing,
        platform_flushing,
        async {
            if let Some(handle) = send_agent_task {
                handle.await
            } else {
                Ok(())
            }
        }
    );

    if let Err(e) = log_result {
        error!("Failed to flush logs for request {}: {}", request_id, e);
    }
    if let Err(e) = platform_result {
        error!("Failed to flush platform for request {}: {}", request_id, e);
    }
    if let Err(e) = agent_result {
        error!("Agent send task failed for request {}: {}", request_id, e);
    }

    // Unified cleanup: Always preserve buffers for late payload handling
    cleanup_request_processing_state_internal(&request_id, true);


    debug!(
        "Serverless mode: Completed processing for request: {}",
        request_id
    );
}

/// Tag Lambda function once on first invocation
fn tag_lambda_function_once(invoked_function_arn: String, config: &config::ExtensionConfig) {
    static TAGGING_DONE: std::sync::Once = std::sync::Once::new();
    TAGGING_DONE.call_once(|| {
        debug!("Spawning background task to tag Lambda function with version information");
        let version_info = version::VersionInfo::get_or_detect(config.new_relic.layer_version.clone());
        let add_version_detail_tags = config.new_relic.add_version_detail_tags;
        let layer_version_from_config = config.new_relic.layer_version.clone();
        let function_name = config.aws.function_name.clone();
        version::tagging::tag_lambda_function_background(
            version_info.extension_version.clone(),
            version_info.agent_version.clone(),
            version_info.layer_version.clone(),
            invoked_function_arn,
            layer_version_from_config,
            add_version_detail_tags,
            function_name,
        );
    });
}

/// Update global invocation context for telemetry processors
fn update_global_invocation_context(request_id: &str, invoked_function_arn: &str) {
    if let Ok(mut global_context) = crate::CURRENT_INVOCATION_CONTEXT.write() {
        // Validate ARN before updating
        if invoked_function_arn.is_empty() {
            error!(
                "CRITICAL: Attempted to update global context with EMPTY invoked_function_arn for request_id: {}. Keeping previous ARN: {}",
                request_id,
                global_context.invoked_function_arn
            );
        } else {
            debug!(
                "Updating global context: request_id='{}', invoked_function_arn='{}' (previous ARN: '{}')",
                request_id,
                invoked_function_arn,
                global_context.invoked_function_arn
            );
            global_context.invoked_function_arn = invoked_function_arn.to_string();
        }
        global_context.request_id = request_id.to_string();
        global_context.trace_id = None;
    }
}

/// Extract trace ID from agent payload if enabled in config
async fn extract_and_coordinate_trace_id(
    payload_bytes: &[u8],
    request_id: &str,
    config: &Arc<ExtensionConfig>,
    log_processor: &Arc<LogProcessor>,
) {
    if !config.new_relic.collect_trace_id {
        return;
    }

    if let Ok(Some(trace_id)) = trace::extract_trace_id_from_payload(payload_bytes) {
        debug!("Extracted trace ID: {}, coordinating with logs", trace_id);
        if let Err(e) = log_processor.on_trace_id_extracted(request_id, &trace_id).await {
            error!("Failed to coordinate logs with trace ID: {}", e);
        }
    }
}

/// Serverless mode: drain previous-request buffers that have BOTH a pending agent payload
/// AND a platform.report. Called at the start of each warm INVOKE.
///
/// Race this fixes:
///   1. Invocation A completes — agent payload not yet in buffer.
///   2. platform.report for A arrives in invocation B → listener finds agent_buffer[A]
///      empty → stores as pending_report[A].
///   3. Agent payload for A arrives late via named pipe → routes to agent_buffer[A].
///   4. Neither the listener nor process_request_concurrently pairs them because
///      each only handles its own request.
///   5. This function (called at the start of invocation B+1) sees both, pairs them,
///      and calls add_to_batch so the batch sender can transmit them.
async fn drain_late_paired_payloads_serverless(
    current_request_id: &str,
    config: &Arc<ExtensionConfig>,
    global_log_processor: &Arc<LogProcessor>,
) -> usize {
    // Collect IDs of old entries that have BOTH pending_report AND non-empty agent_buffer.
    // Avoid holding DashMap refs across the subsequent get_mut calls.
    let candidates: Vec<String> = REQUEST_DATA
        .iter()
        .filter(|entry| {
            entry.key() != current_request_id
                && entry.pending_report.is_some()
                && entry.agent_buffer.lock().map(|b| !b.is_empty()).unwrap_or(false)
        })
        .map(|entry| entry.key().clone())
        .collect();

    if candidates.is_empty() {
        return 0;
    }

    debug!(
        "Serverless mode: {} previous request(s) have paired payload+report — batching before {}",
        candidates.len(),
        current_request_id
    );

    let mut batched_count = 0;

    for req_id in candidates {
        let Some(mut entry) = REQUEST_DATA.get_mut(&req_id) else { continue };

        // Take the report — if gone, someone else raced us (listener matched it already).
        let report = match entry.pending_report.take() {
            Some(r) => r,
            None => continue,
        };

        // Clone the Arc refs while we hold the write guard, then drop the guard
        // before locking the Mutex to avoid the borrow-while-mutably-borrowed error.
        let buffer_arc = entry.agent_buffer.clone();
        let arn = if !entry.invoked_function_arn.is_empty() {
            entry.invoked_function_arn.clone()
        } else {
            crate::get_global_fallback_arn()
        };

        drop(entry); // release DashMap write guard

        let payloads: Vec<Vec<u8>> = match buffer_arc.lock() {
            Ok(mut buf) => std::mem::take(&mut *buf),
            Err(_) => Vec::new(),
        };

        if payloads.is_empty() {
            // Buffer was empty (raced with listener or nothing arrived yet).
            // Restore the report so the listener can still pair it when payload arrives.
            if let Some(mut e) = REQUEST_DATA.get_mut(&req_id) {
                if e.pending_report.is_none() {
                    e.pending_report = Some(report);
                }
            }
            continue;
        }

        let payload_count = payloads.len();
        for payload_bytes in payloads {
            // Extract this late payload's trace.id and stamp + flush the request's held
            // logs before batching it for send.
            if config.new_relic.collect_trace_id {
                extract_and_coordinate_trace_id(&payload_bytes, &req_id, config, global_log_processor).await;
            }
            add_to_batch(req_id.clone(), payload_bytes, Some(report.clone()), arn.clone());
        }
        batched_count += payload_count;
        debug!(
            "Serverless mode: Batched {} late payload(s) for previous request: {}",
            payload_count, req_id
        );
    }

    batched_count
}

/// Process any pending agent payloads from previous invocation (APM mode only)
/// Excludes the current request ID to avoid processing empty buffer
async fn process_pending_agent_payloads(
    config: &Arc<ExtensionConfig>,
    global_log_processor: &Arc<LogProcessor>,
    apm_app: &crate::apm::SharedApmApp,
    current_request_id: &str,
) {
    let all_buffers: Vec<(String, usize)> = REQUEST_DATA
        .iter()
        .map(|entry| {
            let buffer_size = entry.agent_buffer.lock().map(|b| b.len()).unwrap_or(0);
            (entry.key().clone(), buffer_size)
        })
        .collect();

    debug!(
        "APM pending check: Total buffers={}, Details: {:?}",
        all_buffers.len(),
        all_buffers
    );

    let pending_requests: Vec<(String, Arc<Mutex<Vec<Vec<u8>>>>)> = REQUEST_DATA
        .iter()
        .filter(|entry| entry.key() != current_request_id)
        .map(|entry| (entry.key().clone(), entry.agent_buffer.clone()))
        .collect();

    if pending_requests.is_empty() {
        debug!(
            "No pending agent payload buffers from previous invocations (current request excluded: {})",
            current_request_id
        );
        return;
    }

    debug!(
        "Processing {} pending agent payload buffer(s) from previous invocations (excluding current: {})",
        pending_requests.len(),
        current_request_id
    );

    for (request_id, buffer) in pending_requests {
        let context = get_request_context(&request_id);

        let invoked_function_arn = if let Some(ctx) = context {
            if let Ok(ctx_guard) = ctx.lock() {
                if !ctx_guard.invoked_function_arn.is_empty() {
                    ctx_guard.invoked_function_arn.clone()
                } else {
                    crate::get_global_fallback_arn()
                }
            } else {
                crate::get_global_fallback_arn()
            }
        } else {
            crate::get_global_fallback_arn()
        };

        let payloads = {
            if let Ok(mut buf) = buffer.lock() {
                std::mem::take(&mut *buf)
            } else {
                Vec::new()
            }
        };

        if !payloads.is_empty() {
            debug!(
                "Found {} pending agent payload(s) for previous request: {}",
                payloads.len(),
                request_id
            );

            // Send all payloads for this request concurrently
            let mut set = tokio::task::JoinSet::new();
            for payload_bytes in payloads {
                let rid = request_id.clone();
                let arn = invoked_function_arn.clone();
                let lp = global_log_processor.clone();
                let cfg = config.clone();
                let apm = apm_app.clone();
                set.spawn(async move {
                    if let Err(e) = process_and_send_agent_payload(
                        &payload_bytes, &rid, &arn, &lp, &cfg, &apm,
                    ).await {
                        error!("Failed to process pending agent payload: {}", e);
                    }
                });
            }
            while set.join_next().await.is_some() {}
        }

        // Check for pending platform.report for this old request and send as metrics (APM mode)
        if let Some(report_line) = get_pending_report(&request_id) {
            debug!("APM mode: Found pending platform.report for previous request {} - converting to metrics", request_id);

            let apm_app_guard = apm_app.read().await;
            if let Some(ref app) = *apm_app_guard {
                if let Err(e) = app.send_platform_report_metrics(&report_line, &invoked_function_arn).await {
                    error!("APM mode: Failed to send platform report metrics for previous request {}: {}", request_id, e);
                } else {
                    info!("APM mode: Successfully sent platform report metrics for previous request {}", request_id);
                }
            }
            drop(apm_app_guard);

            // Remove pending report after sending
            remove_pending_report(&request_id);
        }

        cleanup_request_processing_state_internal(&request_id, false);
    }
}

/// Process and send agent payload following our simple flow
async fn process_and_send_agent_payload(
    payload_bytes: &[u8],
    request_id: &str,
    invoked_function_arn: &str,
    log_processor: &Arc<LogProcessor>,
    config: &Arc<ExtensionConfig>,
    apm_app: &crate::apm::SharedApmApp,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if config.new_relic.collect_trace_id {
        if let Ok(Some(trace_id)) = trace::extract_trace_id_from_payload(payload_bytes) {
            debug!("Extracted trace ID: {}, coordinating with logs", trace_id);

            if let Err(e) = log_processor.on_trace_id_extracted(request_id, &trace_id).await {
                error!("Failed to coordinate logs with trace ID: {}", e);
            }
        } else {
            debug!("No trace ID found in agent payload or extraction failed");
        }
    } else {
        debug!("Trace ID collection disabled, skipping extraction");
    }

    let apm_app_guard = apm_app.read().await;
    if let Some(ref app) = *apm_app_guard {
        debug!(
            "APM mode: Processing agent payload for request: {} (size: {} bytes)",
            request_id,
            payload_bytes.len()
        );
        match app.process_agent_payload(payload_bytes.to_vec(), request_id).await {
            Ok(()) => {
                info!("APM mode: Agent payload sent successfully for request: {}", request_id);
            }
            Err(e) => {
                warn!("APM mode: Failed to send agent payload to APM collector: {}", e);
                buffer_failed_agent_payload(payload_bytes, request_id, invoked_function_arn);
                warn!(
                    "APM mode: Agent payload buffered for retry (size: {} bytes)",
                    payload_bytes.len()
                );
            }
        }
    } else {
        // APM connection not ready - buffer the payload, do NOT send to telemetry endpoint
        warn!(
            "APM connection not established - buffering agent payload for request: {} (size: {} bytes)",
            request_id,
            payload_bytes.len()
        );
        buffer_failed_agent_payload(payload_bytes, request_id, invoked_function_arn);
    }

    Ok(())
}

/// Build the shutdown "telemetry DROPPED" summary in two forms:
/// - the CloudWatch line (counts only — request_ids would bloat it and aren't
///   queryable there), returned first;
/// - the New Relic line (the same summary plus the affected request_ids for
///   correlation), returned second.
/// Pure (no I/O) so the wording and the CloudWatch-vs-NR split are unit-tested.
#[allow(clippy::too_many_arguments)]
/// Data for the shutdown "telemetry DROPPED" diagnostic. The human summary goes to the
/// CloudWatch line AND the NR log message; the request_ids and counts go to NR as
/// queryable attributes (not embedded in the message text).
struct ShutdownDropDiagnostic {
    message: String,
    arn: String,
    /// The last/current request_id (the invocation during which shutdown occurred);
    /// stamped as aws.lambda_request_id / faas.execution for consistency with other
    /// extension logs. The full dropped set is in `request_ids`.
    request_id: String,
    /// Distinct request_ids whose telemetry was dropped (the affected invocations).
    request_ids: Vec<String>,
    /// = request_ids.len() (distinct invocations affected).
    request_id_count: usize,
    /// Raw buffered item count across all APM buffers (can exceed request_id_count
    /// when one invocation has multiple buffered items).
    item_count: usize,
}

/// Max request_ids embedded in the `dropped.request_ids` attribute string. The numeric
/// `dropped.request_id_count` is always exact even if this list is truncated.
const MAX_DROPPED_IDS_IN_ATTR: usize = 100;

/// Build the human-readable shutdown drop summary (CloudWatch line and NR log message
/// body). Counts only — no request_ids. Worded so item/invocation counts read as
/// "N items across M invocations" rather than inviting an additive misread.
/// Pure (no I/O) so the wording is unit-tested.
fn build_shutdown_drop_summary(
    apm_connected: bool,
    affected: usize,
    remaining_count: usize,
    dropped_earlier: u64,
    reason: &str,
    cycles: u64,
    attempts: u64,
) -> String {
    let earlier_note = if dropped_earlier > 0 {
        format!(" (+{dropped_earlier} more dropped earlier)")
    } else {
        String::new()
    };
    if apm_connected {
        format!(
            "APM telemetry DROPPED at shutdown: {remaining_count} item(s) across {affected} invocation(s) could not be sent despite APM being connected{earlier_note}."
        )
    } else {
        format!(
            "APM telemetry DROPPED at shutdown: APM never connected (last failure: {reason}) after {cycles} reconnect cycle(s) / {attempts} handshake attempt(s) — {remaining_count} item(s) across {affected} invocation(s) lost{earlier_note}."
        )
    }
}

/// Build the New Relic diagnostic log: the summary as the message, plus the dropped
/// request_ids and counts as queryable attributes (`dropped.request_ids`,
/// `dropped.request_id_count`, `dropped.item_count`). Pure so attributes are unit-tested.
fn build_shutdown_drop_log(diag: &ShutdownDropDiagnostic) -> crate::newrelic::payload::LogMessage {
    let mut log = crate::newrelic::payload::LogMessage::diagnostic(
        "ERROR",
        format!("[NR_EXT] ERROR {}", diag.message),
    );
    // Stamp request_id (last invocation) so the diagnostic carries the same
    // aws.lambda_request_id / faas.execution as other extension logs.
    if !diag.request_id.is_empty() {
        let mut aws_attrs = serde_json::Map::new();
        aws_attrs.insert(
            "lambda_request_id".to_string(),
            serde_json::json!(diag.request_id),
        );
        log.attributes
            .insert("aws".to_string(), serde_json::Value::Object(aws_attrs));
        log.attributes
            .insert("faas.execution".to_string(), serde_json::json!(diag.request_id));
    }
    if !diag.request_ids.is_empty() {
        let joined = if diag.request_ids.len() > MAX_DROPPED_IDS_IN_ATTR {
            format!(
                "{},(+{} more)",
                diag.request_ids[..MAX_DROPPED_IDS_IN_ATTR].join(","),
                diag.request_ids.len() - MAX_DROPPED_IDS_IN_ATTR
            )
        } else {
            diag.request_ids.join(",")
        };
        log.attributes
            .insert("dropped.request_ids".to_string(), serde_json::json!(joined));
    }
    log.attributes.insert(
        "dropped.request_id_count".to_string(),
        serde_json::json!(diag.request_id_count),
    );
    log.attributes.insert(
        "dropped.item_count".to_string(),
        serde_json::json!(diag.item_count),
    );
    log
}

/// Buffer failed agent payload for retry across invocations
pub(crate) fn buffer_failed_agent_payload(
    payload_bytes: &[u8],
    request_id: &str,
    invoked_function_arn: &str,
) {
    let failed_payload = FailedAgentPayload {
        payload_bytes: payload_bytes.to_vec(),
        request_id: request_id.to_string(),
        invoked_function_arn: invoked_function_arn.to_string(),
        retry_count: 0,
        failed_at: chrono::Utc::now(),
    };

    if let Ok(mut failed_payloads) = FAILED_AGENT_PAYLOADS.lock() {
        push_failed_payload_capped(&mut failed_payloads, failed_payload);
        debug!(
            "Buffered failed agent payload for request {} (total failed: {})",
            request_id,
            failed_payloads.len()
        );
    } else {
        error!("Failed to lock FAILED_AGENT_PAYLOADS buffer - payload lost!");
    }
}

/// Retry failed agent payloads via the APM collector (APM mode only).
///
/// `FAILED_AGENT_PAYLOADS` is populated solely by the APM payload path
/// (`process_and_send_agent_payload`) when the APM collector is unreachable or not yet
/// connected. Retries therefore go back to the **APM collector** — never the serverless
/// telemetry endpoint — so APM-mode telemetry can't leak into the serverless pipeline.
/// Payloads that still can't be sent (APM not connected) stay buffered for the next
/// invoke / shutdown. Mirrors the Err→re-buffer convention of the initial send path.
async fn retry_failed_agent_payloads(apm_app: crate::apm::SharedApmApp) {
    let mut retry_successful_count = 0;
    let mut retry_failed_count = 0;

    let failed_payloads = {
        if let Ok(mut failed_payloads) = FAILED_AGENT_PAYLOADS.lock() {
            std::mem::take(&mut *failed_payloads)
        } else {
            error!("Failed to lock FAILED_AGENT_PAYLOADS for retry");
            return;
        }
    };

    if failed_payloads.is_empty() {
        debug!("No failed agent payloads to retry");
        return;
    }

    debug!(
        "Retrying {} failed agent payloads during final flush",
        failed_payloads.len()
    );

    for mut failed_payload in failed_payloads {
        failed_payload.retry_count += 1;

        // A sandbox lives at most ~30 min, so this 24h guard effectively never
        // fires — it's only a backstop against absurdly stale data. We do NOT
        // drop on retry count: a payload is kept (subject to the buffer cap) and
        // retried every invoke until it flushes on reconnect or the container dies.
        let age = chrono::Utc::now().signed_duration_since(failed_payload.failed_at);
        if age.num_hours() > 24 {
            warn!(
                "Dropping agent payload that's too old ({} hours) for request {}",
                age.num_hours(),
                failed_payload.request_id
            );
            DROPPED_AGENT_PAYLOADS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            continue;
        }

        debug!(
            "Retrying agent payload for request {} (arn: {}, attempt {}) via APM collector",
            failed_payload.request_id, failed_payload.invoked_function_arn, failed_payload.retry_count
        );

        // Send to the APM collector (NOT the serverless endpoint). If APM isn't
        // connected yet, keep the payload buffered for a later invoke / shutdown.
        let send_result = {
            let apm_guard = apm_app.read().await;
            match *apm_guard {
                Some(ref app) => Some(
                    app.process_agent_payload(
                        failed_payload.payload_bytes.clone(),
                        &failed_payload.request_id,
                    )
                    .await,
                ),
                None => None,
            }
        };

        match send_result {
            Some(Ok(())) => {
                retry_successful_count += 1;
                info!(
                    "Successfully retried agent payload for request {} (APM collector)",
                    failed_payload.request_id
                );
            }
            Some(Err(e)) => {
                retry_failed_count += 1;
                warn!("Failed to retry agent payload to APM collector: {}", e);
                // Keep it buffered (capped) so it retries on the next invoke / reconnect.
                if let Ok(mut failed_payloads) = FAILED_AGENT_PAYLOADS.lock() {
                    push_failed_payload_capped(&mut failed_payloads, failed_payload);
                }
            }
            None => {
                // APM not connected yet — not a failed send; keep buffered for later.
                debug!(
                    "APM not connected — keeping agent payload buffered for request {}",
                    failed_payload.request_id
                );
                if let Ok(mut failed_payloads) = FAILED_AGENT_PAYLOADS.lock() {
                    push_failed_payload_capped(&mut failed_payloads, failed_payload);
                }
            }
        }
    }

    if retry_successful_count > 0 || retry_failed_count > 0 {
        debug!(
            "Agent payload retry results: {} successful, {} still failed",
            retry_successful_count, retry_failed_count
        );
    }
}

/// Clean up old failed payloads (older than 24 hours)
pub fn cleanup_old_failed_payloads() {
    if let Ok(mut failed_payloads) = FAILED_AGENT_PAYLOADS.lock() {
        let initial_count = failed_payloads.len();
        let now = chrono::Utc::now();

        failed_payloads.retain(|payload| {
            let age = now.signed_duration_since(payload.failed_at);
            age.num_hours() <= 24
        });

        let removed_count = initial_count - failed_payloads.len();
        if removed_count > 0 {
            debug!(
                "Cleaned up {} old failed agent payloads (kept {} recent ones)",
                removed_count,
                failed_payloads.len()
            );
        }
    }
}

#[cfg(test)]
#[path = "event_loop_tests.rs"]
mod tests;
