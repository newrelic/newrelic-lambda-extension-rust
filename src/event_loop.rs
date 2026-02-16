

use std::sync::{Arc, Mutex};
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
        ProcessorFactory,
        create_request_processing_state,
        cleanup_request_processing_state_internal,
        wait_for_all_requests_completion,
        REQUEST_PROCESSORS, REQUEST_AGENT_BUFFERS, REQUEST_CONTEXTS,
        CURRENT_ACTIVE_REQUEST_ID, PENDING_REPORTS,
    },
    agent::batch::DEFAULT_BATCH_BUFFER,
    agent::payload::send_agent_payload_to_newrelic,
    error_synthesis,
    trace,
    version,
    CURRENT_INVOCATION_CONTEXT,
    IS_WARM_START,
};

#[derive(Debug)]
pub struct ExtensionComponents {
    pub client: Arc<Client>,
    pub extension_id: String,
    pub processor_factory: Arc<ProcessorFactory>,
    pub newrelic_client: Arc<NewRelicClient>,
    pub config: Arc<ExtensionConfig>,
    pub harvester_handle: tokio::task::JoinHandle<()>,
    pub global_log_processor: Arc<LogProcessor>,
    pub apm_app: crate::apm::SharedApmApp,
    pub apm_mode_enabled: bool, // Actual mode after runtime detection (may differ from config for Java)
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

/// Track last processed request for error synthesis on shutdown
pub static LAST_REQUEST_CONTEXT: once_cell::sync::Lazy<Arc<Mutex<Option<(String, String)>>>> =
    once_cell::sync::Lazy::new(|| Arc::new(Mutex::new(None)));

/// Event loop: handles cold start (first invoke) and warm starts (subsequent invokes)
pub async fn run_infinite_event_loop(
    mut extension_components: ExtensionComponents,
) -> (u32, tokio::task::JoinHandle<()>) {
    if !extension_components.config.new_relic.extension_enabled
        || extension_components.config.new_relic.license_key.is_none()
    {
        info!("Running in no-op mode");
        execute_noop_event_loop(&extension_components.client, &extension_components.extension_id)
            .await;
        return (0, extension_components.harvester_handle);
    }

    let total_events = execute_main_telemetry_processing_loop(&mut extension_components).await;
    (total_events, extension_components.harvester_handle)
}

/// Lambda extension pattern: GET /next (block) → process INVOKE → repeat until SHUTDOWN
/// Routes to APM or standard mode based on config (or runtime override for Java)
async fn execute_main_telemetry_processing_loop(components: &mut ExtensionComponents) -> u32 {
    let apm_mode_enabled = components.apm_mode_enabled;
    if apm_mode_enabled {
        info!("Starting APM mode event loop (connection may still be in progress)");
        execute_apm_mode_event_loop(components).await
    } else {
        debug!("Starting standard mode event loop");
        execute_standard_mode_event_loop(components).await
    }
}

/// APM mode: immediate sending to collector, no batching, keeps buffers alive for late payloads
pub async fn execute_apm_mode_event_loop(components: &mut ExtensionComponents) -> u32 {
    let mut event_counter = 0;
    let mut cleanup_counter = 0; // Track when to run periodic cleanup

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
                    
                    process_pending_agent_payloads(
                        &components.newrelic_client,
                        &components.config,
                        &components.global_log_processor,
                        &components.apm_app,
                        "",
                    )
                    .await;
                    
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
            } => {
                let event_start = std::time::Instant::now();

                if is_cold_start {
                    let mut updated_config = (*components.config).clone();
                    updated_config.aws.extract_and_update_account_id_from_arn(&invoked_function_arn);
                    components.config = Arc::new(updated_config);
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

                // Create request state FIRST so we have the updated context
                let request_state = create_request_processing_state(
                    &request_id,
                    &invoked_function_arn,
                    &components.processor_factory,
                    components.apm_mode_enabled,
                );
                
                // Update LogProcessor's context BEFORE processing any logs
                components
                    .global_log_processor
                    .update_invocation_context(request_state.context.clone());
                // Process pre-invoke logs FIRST (add metadata and move to batch)
                components
                    .global_log_processor
                    .process_pre_invoke_logs();
                // THEN process buffered logs (so they don't trigger auto-flush of incomplete logs)
                components
                    .global_log_processor
                    .process_buffered_logs_with_request_id(&request_id);

                REQUEST_PROCESSORS.insert(request_id.clone(), request_state);

                let buffer_count = REQUEST_AGENT_BUFFERS.len();
                if buffer_count > 0 {
                    debug!(
                        "APM mode: Found {} request buffer(s) before processing (current: {})",
                        buffer_count, request_id
                    );
                }

                let pending_task = Some(tokio::spawn({
                    let newrelic_client = components.newrelic_client.clone();
                    let config = components.config.clone();
                    let global_log_processor = components.global_log_processor.clone();
                    let apm_app = components.apm_app.clone();
                    let current_request_id = request_id.clone();

                    async move {
                        process_pending_agent_payloads(
                            &newrelic_client,
                            &config,
                            &global_log_processor,
                            &apm_app,
                            &current_request_id,
                        )
                        .await;
                    }
                }));

                let request_id_clone = request_id.clone();
                let invoked_function_arn_clone = invoked_function_arn.clone();
                let processor_factory_clone = components.processor_factory.clone();
                let newrelic_client_clone = components.newrelic_client.clone();
                let config_clone = components.config.clone();
                let global_log_processor_clone = components.global_log_processor.clone();
                let apm_app_clone = components.apm_app.clone();

                let current_task = tokio::spawn(async move {
                    process_apm_request(
                        request_id_clone,
                        invoked_function_arn_clone,
                        is_cold_start,
                        processor_factory_clone,
                        newrelic_client_clone,
                        config_clone,
                        global_log_processor_clone,
                        apm_app_clone,
                    )
                    .await;
                });

                let (current_result, pending_result) = tokio::join!(current_task, pending_task.unwrap());
                if let Err(e) = current_result {
                    error!("Error in APM request processing: {}", e);
                }
                if let Err(e) = pending_result {
                    error!("Error in pending payload processing: {}", e);
                }

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

                // Periodic cleanup: Run every 10 invocations to prevent memory leaks
                cleanup_counter += 1;
                if cleanup_counter >= 10 {
                    cleanup_counter = 0;
                    let newrelic_client = components.newrelic_client.clone();
                    let config = components.config.clone();
                    tokio::spawn(async move {
                        use crate::request::cleanup_old_request_buffers;
                        DEFAULT_BATCH_BUFFER.cleanup_old_batch_entries(newrelic_client.clone(), config.clone()).await;
                        cleanup_old_request_buffers(newrelic_client, config).await;
                        cleanup_old_failed_payloads();
                    });
                }
            }
            runtime::LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
                let shutdown_start_time = std::time::Instant::now();
                info!("[NR_EXT] APM mode: Extension shutting down with reason: {} (started at {:?})", shutdown_reason, std::time::SystemTime::now());

                // Synthesize and send error based on shutdown reason (to APM collector)
                if let Some((last_request_id, last_arn)) = LAST_REQUEST_CONTEXT.lock().ok().and_then(|guard| guard.clone()) {
                    let apm_app_guard = components.apm_app.read().await;
                    if let Some(ref app) = *apm_app_guard {
                        match shutdown_reason {
                            runtime::ShutdownReason::Timeout => {
                                // Lambda timeout - send timeout error event to APM collector
                                info!("Shutdown due to timeout - sending error event to APM for request: {}", last_request_id);
                                if let Err(e) = app.send_shutdown_error_event(
                                    "LambdaTimeout",
                                    "Task timed out",
                                    &last_request_id,
                                    &last_arn,
                                )
                                .await
                                {
                                    error!("Failed to send timeout error event to APM: {}", e);
                                }
                            }
                            runtime::ShutdownReason::Failure => {
                                // Lambda failure/fault - send platform fault error event to APM collector
                                info!("Shutdown due to failure - sending error event to APM for request: {}", last_request_id);
                                if let Err(e) = app.send_shutdown_error_event(
                                    "LambdaPlatformFault",
                                    "AWS Lambda platform fault caused a shutdown",
                                    &last_request_id,
                                    &last_arn,
                                )
                                .await
                                {
                                    error!("Failed to send platform fault error event to APM: {}", e);
                                }
                            }
                            runtime::ShutdownReason::Spindown => {
                                // Normal shutdown - no error needed
                                debug!("Normal spindown shutdown - no error event needed");
                            }
                            runtime::ShutdownReason::Unknown => {
                                // Unknown/unexpected shutdown reason - send generic error event
                                warn!("Unknown shutdown reason - sending error event to APM for request: {}", last_request_id);
                                if let Err(e) = app.send_shutdown_error_event(
                                    "LambdaShutdown",
                                    "Lambda shutdown with unknown reason",
                                    &last_request_id,
                                    &last_arn,
                                )
                                .await
                                {
                                    error!("Failed to send shutdown error event to APM: {}", e);
                                }
                            }
                        }
                    } else {
                        warn!("APM app not initialized - cannot send shutdown error event");
                    }
                }

                // CRITICAL: Process ALL remaining pending agent payloads before shutdown
                debug!("APM mode shutdown: Processing all remaining agent payloads");
                
                // Check all request buffers for unsent payloads
                let all_request_ids: Vec<String> = REQUEST_AGENT_BUFFERS
                    .iter()
                    .map(|entry| entry.key().clone())
                    .collect();
                
                if !all_request_ids.is_empty() {
                    debug!("APM mode shutdown: Found {} request(s) with potential unsent payloads", all_request_ids.len());
                    
                    for request_id in all_request_ids {
                        if let Some(buffer) = REQUEST_AGENT_BUFFERS.get(&request_id) {
                            let payloads = {
                                if let Ok(mut buf) = buffer.lock() {
                                    std::mem::take(&mut *buf)
                                } else {
                                    Vec::new()
                                }
                            };
                            
                            if !payloads.is_empty() {
                                info!("APM mode shutdown: Sending {} unsent payload(s) for request: {}", payloads.len(), request_id);
                                
                                let invoked_function_arn = REQUEST_CONTEXTS
                                    .get(&request_id)
                                    .map(|ctx_ref| {
                                        ctx_ref.lock()
                                            .ok()
                                            .map(|ctx| ctx.invoked_function_arn.clone())
                                            .unwrap_or_else(|| {
                                                // Fallback to global context ARN (set from registration)
                                                if let Ok(global_ctx) = CURRENT_INVOCATION_CONTEXT.read() {
                                                    global_ctx.invoked_function_arn.clone()
                                                } else {
                                                    String::new()
                                                }
                                            })
                                    })
                                    .unwrap_or_else(|| {
                                        // Fallback to global context ARN (set from registration)
                                        if let Ok(global_ctx) = CURRENT_INVOCATION_CONTEXT.read() {
                                            global_ctx.invoked_function_arn.clone()
                                        } else {
                                            String::new()
                                        }
                                    });
                                
                                for payload_bytes in payloads {
                                    if let Err(e) = process_and_send_agent_payload(
                                        &payload_bytes,
                                        &request_id,
                                        &invoked_function_arn,
                                        &components.global_log_processor,
                                        &components.newrelic_client,
                                        &components.config,
                                        &components.apm_app,
                                    )
                                    .await
                                    {
                                        warn!("APM mode shutdown: Failed to send payload for {}: {}", request_id, e);
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
                debug!("APM mode shutdown: Retrying all buffered telemetry");
                crate::apm::telemetry_buffer::retry_buffered_telemetry(
                    &components.client,
                    components.config.new_relic.license_key.as_deref().unwrap_or(""),
                )
                .await;

                let remaining_count = crate::apm::telemetry_buffer::get_buffer_count();
                if remaining_count > 0 {
                    error!("APM mode shutdown: {} telemetry items could not be sent", remaining_count);
                }

                // Process any pending platform.report lines as metrics (APM mode)
                let all_pending_reports: Vec<(String, String)> = PENDING_REPORTS
                    .iter()
                    .map(|entry| (entry.key().clone(), entry.value().clone()))
                    .collect();

                if !all_pending_reports.is_empty() {
                    debug!("APM mode shutdown: Found {} pending platform.report(s) to send as metrics", all_pending_reports.len());

                    let apm_app_guard = components.apm_app.read().await;
                    if let Some(ref app) = *apm_app_guard {
                        for (request_id, report_line) in all_pending_reports {
                            debug!("APM mode shutdown: Sending platform report metrics for request: {}", request_id);

                            if let Err(e) = app.send_platform_report_metrics(&report_line).await {
                                error!("APM mode shutdown: Failed to send platform report metrics for {}: {}", request_id, e);
                            } else {
                                info!("APM mode shutdown: Successfully sent platform report metrics for request: {}", request_id);
                            }

                            // Remove from pending reports after sending
                            PENDING_REPORTS.remove(&request_id);
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

                // Final flush of logs
                if let Err(e) = components.global_log_processor.flush().await {
                    error!("APM mode shutdown: Failed to flush logs: {}", e);
                }

                let shutdown_duration = shutdown_start_time.elapsed();
                info!("APM mode shutdown: All data processed and sent");
                info!("[NR_EXT] Shutdown completed - Duration: {}ms", shutdown_duration.as_millis());
                break;
            }
        }
    }

    event_counter
}

/// Standard mode: batches payloads with platform.report, sends to serverless API
pub async fn execute_standard_mode_event_loop(components: &mut ExtensionComponents) -> u32 {
    let mut event_counter = 0;
    let mut cleanup_counter = 0; // Track when to run periodic cleanup

    loop {
        debug!("Standard mode: waiting for next lambda invocation event...");

        let runtime_event =
            match runtime::fetch_next_event(&components.client, &components.extension_id).await {
                Ok(event) => event,
                Err(e) => {
                    let error_msg = e.to_string();
                    if error_msg.contains("403") || error_msg.contains("State transition") {
                        error!("Fatal extension state error (403 - Lambda shutting down): {:?}", e);
                        info!("Performing emergency shutdown cleanup...");
                        
                        DEFAULT_BATCH_BUFFER.send_batched_payloads_with_reports_only(
                            components.newrelic_client.clone(),
                            components.config.clone(),
                        )
                        .await;
                        
                        let _ = components.global_log_processor.flush().await;
                        
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

                error_synthesis::retry_failed_errors(&components.newrelic_client, &components.config).await;

                crate::apm::telemetry_buffer::retry_buffered_telemetry(
                    &components.client,
                    components.config.new_relic.license_key.as_deref().unwrap_or(""),
                )
                .await;

                if is_cold_start && components.config.new_relic.add_version_detail_tags {
                    tag_lambda_function_once(invoked_function_arn.clone(), &components.config);
                }

                update_global_invocation_context(&request_id, &invoked_function_arn);

                components
                    .global_log_processor
                    .process_buffered_logs_with_request_id(&request_id);
                
                // Transfer pre-invoke logs to normal batch with ARN/request_id metadata
                components
                    .global_log_processor
                    .process_pre_invoke_logs();

                // SKIP old buffer processing to avoid deadlocks
                // Late payloads are already handled via the buffer matching on next invocation
                // The complex locking in this loop was causing 7-second deadlocks

                // Send batch if threshold is reached after processing late payloads (only with report lines)
                // CRITICAL: Must await to prevent Lambda from freezing network mid-request
                if DEFAULT_BATCH_BUFFER.should_send_batch_by_threshold() {
                    debug!("Batch threshold reached - sending payloads with report lines only");
                    DEFAULT_BATCH_BUFFER.send_batched_payloads_with_reports_only(
                        components.newrelic_client.clone(),
                        components.config.clone()
                    ).await;
                }

                let request_state = create_request_processing_state(
                    &request_id,
                    &invoked_function_arn,
                    &components.processor_factory,
                    components.apm_mode_enabled, // Use actual mode (handles Java override)
                );

                components
                    .global_log_processor
                    .update_invocation_context(request_state.context.clone());

                REQUEST_PROCESSORS.insert(request_id.clone(), request_state);

                let request_id_clone = request_id.clone();
                let invoked_function_arn_clone = invoked_function_arn.clone();
                let processor_factory_clone = components.processor_factory.clone();
                let newrelic_client_clone = components.newrelic_client.clone();
                let config_clone = components.config.clone();
                let global_log_processor_clone = components.global_log_processor.clone();
                let apm_app_clone = components.apm_app.clone();

                let processing_handle = tokio::spawn(async move {
                    process_request_concurrently(
                        request_id_clone,
                        invoked_function_arn_clone,
                        processor_factory_clone,
                        newrelic_client_clone,
                        config_clone,
                        global_log_processor_clone,
                        apm_app_clone,
                    )
                    .await;
                });

                if let Err(e) = processing_handle.await {
                    error!("Error in standard mode request processing: {}", e);
                }

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

                // Periodic cleanup: Run every 10 invocations to prevent memory leaks
                cleanup_counter += 1;
                if cleanup_counter >= 10 {
                    cleanup_counter = 0;
                    let newrelic_client = components.newrelic_client.clone();
                    let config = components.config.clone();
                    tokio::spawn(async move {
                        use crate::request::cleanup_old_request_buffers;
                        DEFAULT_BATCH_BUFFER.cleanup_old_batch_entries(newrelic_client.clone(), config.clone()).await;
                        cleanup_old_request_buffers(newrelic_client, config).await;
                    });
                }
            }
            runtime::LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
                let shutdown_start_time = std::time::Instant::now();
                info!("[NR_EXT] Standard mode: Extension shutting down with reason: {} (started at {:?})", shutdown_reason, std::time::SystemTime::now());

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

                // CRITICAL: Send ALL remaining payloads at shutdown (with or without reports)
                debug!("Standard mode shutdown: Sending ALL remaining payloads (including those without reports)");
                DEFAULT_BATCH_BUFFER.send_all_pending_payloads_on_shutdown(
                    components.newrelic_client.clone(),
                    components.config.clone(),
                )
                .await;

                // Emergency flush of pre-invoke buffer (logs from INIT phase if shutdown before first INVOKE)
                if let Err(e) = components.global_log_processor.flush_pre_invoke_buffer_on_shutdown().await {
                    error!("Standard mode shutdown: Failed to flush pre-invoke buffer: {}", e);
                }

                wait_for_all_requests_completion(
                    components.newrelic_client.clone(),
                    components.config.clone(),
                    components.global_log_processor.clone(),
                    shutdown_start_time,
                )
                .await;

                info!("Standard mode shutdown: All data processed and sent");
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
    _processor_factory: Arc<ProcessorFactory>,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
    apm_app: crate::apm::SharedApmApp,
) {
    debug!("APM mode: Starting processing for request: {}", request_id);

    if let Ok(mut active_request) = CURRENT_ACTIVE_REQUEST_ID.lock() {
        *active_request = Some(request_id.clone());
    }

    if !is_cold_start {
        let pending_buffers: Vec<String> = REQUEST_AGENT_BUFFERS
            .iter()
            .filter_map(|entry| {
                let req_id = entry.key();
                if req_id != &request_id {
                    if let Ok(buffer) = entry.value().lock() {
                        if !buffer.is_empty() {
                            return Some(req_id.clone());
                        }
                    }
                }
                None
            })
            .collect();

        if !pending_buffers.is_empty() {
            debug!(
                "APM warm start: Found {} pending late agent payload(s) from previous invocations - processing now",
                pending_buffers.len()
            );

            for old_request_id in pending_buffers {
                debug!(
                    "Processing late agent payload for request: {}",
                    old_request_id
                );

                let late_payloads = if let Some(buffer_ref) =
                    REQUEST_AGENT_BUFFERS.get(&old_request_id)
                {
                    if let Ok(mut buffer) = buffer_ref.lock() {
                        std::mem::take(&mut *buffer)
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                };

                for payload_bytes in late_payloads {
                    debug!(
                        "Sending late agent payload for request: {} ({} bytes)",
                        old_request_id,
                        payload_bytes.len()
                    );

                    if let Err(e) = send_to_apm_collector(
                        &payload_bytes,
                        &old_request_id,
                        &invoked_function_arn,
                        &newrelic_client,
                        &config,
                        &apm_app,
                    )
                    .await
                    {
                        error!(
                            "Failed to send late agent payload for {}: {}",
                            old_request_id, e
                        );
                    } else {
                        info!(
                            "Successfully sent late agent payload for request: {}",
                            old_request_id
                        );
                    }
                }

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
    global_log_processor.reset_trace_id_state();
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
    // Flow 2: If run_id exists but no payload, buffer will be kept for next invocation
    // Flow 3: If no run_id, buffer the payload for when run_id arrives (or shutdown)
    let send_agent_task = if has_run_id && got_payload {
        debug!(
            "APM mode: run_id available + agent payload arrived ({} payload(s)) - sending immediately",
            agent_payloads.len()
        );
        let request_id_clone = request_id.clone();
        let invoked_function_arn_clone = invoked_function_arn.clone();
        let newrelic_client_clone = newrelic_client.clone();
        let config_clone = config.clone();
        let global_log_processor_clone = global_log_processor.clone();
        let apm_app_clone = apm_app.clone();
        let agent_payloads_clone = agent_payloads.clone();

        Some(tokio::spawn(async move {
            let mut all_sent = true;
            for payload_bytes in &agent_payloads_clone {
                extract_and_coordinate_trace_id(
                    payload_bytes,
                    &config_clone,
                    &global_log_processor_clone,
                )
                .await;

                match send_to_apm_collector(
                    payload_bytes,
                    &request_id_clone,
                    &invoked_function_arn_clone,
                    &newrelic_client_clone,
                    &config_clone,
                    &apm_app_clone,
                )
                .await
                {
                    Ok(()) => {
                        info!("APM mode: Agent payload sent successfully");
                    }
                    Err(e) => {
                        error!("Failed to send agent payload to APM collector: {}", e);
                        all_sent = false;
                    }
                }
            }
            (all_sent, agent_payloads_clone)
        }))
    } else {
        // Flow 2 or 3: Either no run_id yet, or no payload yet
        if !has_run_id && got_payload {
            debug!(
                "APM mode: No run_id yet but agent payload arrived ({} payload(s)) - buffering for when run_id becomes available",
                agent_payloads.len()
            );
            // Put payloads back in buffer to send when run_id arrives
            if let Some(buffer_ref) = REQUEST_AGENT_BUFFERS.get(&request_id) {
                if let Ok(mut buffer) = buffer_ref.lock() {
                    buffer.extend(agent_payloads);
                }
            }
        } else if has_run_id && !got_payload {
            debug!(
                "APM mode: run_id available but no agent payload yet for request: {} - will catch in next invocation if it arrives late",
                request_id
            );
        } else {
            debug!(
                "APM mode: No run_id and no agent payload for request: {} - normal flow",
                request_id
            );
        }
        None
    };

    // Spawn agent send in background - don't wait for it to complete
    // This ensures we return to /next quickly without blocking on agent sends
    if let Some(handle) = send_agent_task {
        tokio::spawn(async move {
            match handle.await {
                Ok((success, payloads)) => {
                    if !success && !payloads.is_empty() {
                        debug!("Agent send completed with failures - payloads will be retried in next invocation");
                    }
                }
                Err(e) => {
                    error!("Agent send task failed: {}", e);
                }
            }
        });
    }

    // Check for pending platform.report and send as metrics to Metric API (APM mode)
    if let Some(entry) = PENDING_REPORTS.get(&request_id) {
        let report_line = entry.value().clone();
        drop(entry); // Release the lock before async operations

        debug!("APM mode: Found platform.report for request {} - converting to metrics", request_id);

        let apm_app_guard = apm_app.read().await;
        if let Some(ref app) = *apm_app_guard {
            if let Err(e) = app.send_platform_report_metrics(&report_line).await {
                error!("APM mode: Failed to send platform report metrics for {}: {}", request_id, e);
            } else {
                info!("APM mode: Successfully sent platform report metrics for request {}", request_id);
            }
        } else {
            warn!("APM mode: APM app not ready - cannot send platform metrics for {}", request_id);
        }
        drop(apm_app_guard);

        // Remove from pending reports after sending
        PENDING_REPORTS.remove(&request_id);
    } else {
        debug!("APM mode: No platform.report found for request {} (may arrive in next invocation)", request_id);
    }

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

    // Note: We do NOT wait for runtime.done here because platform.runtimeDone event
    // arrives during the NEXT invocation in APM mode, not the current one.
    // Agent payloads that arrive late will be caught by warm start logic.

    cleanup_request_processing_state_internal(&request_id, true);

    if let Ok(mut active_request) = CURRENT_ACTIVE_REQUEST_ID.lock() {
        *active_request = Some(request_id.clone());
    }

    debug!(
        "APM mode: Completed processing for request: {}",
        request_id
    );
}

/// Send agent payload to APM collector (parses and sends 5 telemetry types)
async fn send_to_apm_collector(
    payload_bytes: &[u8],
    request_id: &str,
    _invoked_function_arn: &str,
    _newrelic_client: &Arc<NewRelicClient>,
    _config: &Arc<ExtensionConfig>,
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
        // Go-style pattern: APM connection still in progress - buffer will be kept for retry
        warn!(
            "APM connection still in progress - payload for {} will be buffered and retried",
            request_id
        );
        return Err("APM connection not ready yet - payload buffered for retry".into());
    }
    Ok(())
}

pub async fn process_request_concurrently(
    request_id: String,
    invoked_function_arn: String,
    _processor_factory: Arc<ProcessorFactory>,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
    _apm_app: crate::apm::SharedApmApp,
) {
    debug!(
        "Standard mode: Starting processing for request: {}",
        request_id
    );

    if let Ok(mut active_request) = CURRENT_ACTIVE_REQUEST_ID.lock() {
        *active_request = Some(request_id.clone());
    }

    let state = REQUEST_PROCESSORS.remove(&request_id).map(|(_k, v)| v);

    let Some(mut state) = state else {
        error!("No processing state found for request: {}", request_id);
        return;
    };

    let invocation_start_time = chrono::Utc::now();
    global_log_processor.set_invocation_start_time(invocation_start_time);
    global_log_processor.reset_trace_id_state();
    state
        .platform_processor
        .process_invoke_event(&request_id, &invoked_function_arn);

    // Skip runtime.done wait for all invocations (performance optimization)
    // Late payloads will be processed in next invocation
    debug!(
        "Skipping runtime.done wait for request: {} (unified batching approach)",
        request_id
    );

    // Unified wait timeout for all invocations
    let agent_wait_timeout_ms = 100;

    let payload_already_arrived = {
        if let Ok(buffer) = state.agent_buffer.lock() {
            !buffer.is_empty()
        } else {
            false
        }
    };

    if !payload_already_arrived {
        debug!(
            "Standard mode: Waiting up to {}ms for agent payload for request: {}",
            agent_wait_timeout_ms, request_id
        );
        tokio::select! {
            _ = state.coordination_rx.as_mut().expect("coordination_rx should exist").recv() => {
                debug!("Agent payload received early for request: {}", request_id);
            }
            _ = tokio::time::sleep(Duration::from_millis(agent_wait_timeout_ms)) => {
                debug!("Agent payload wait timeout ({}ms) for request: {}", agent_wait_timeout_ms, request_id);
            }
        }
    } else {
        debug!(
            "Agent payload already in buffer for request: {} - no wait needed",
            request_id
        );
    }

    let agent_payloads = {
        if let Ok(mut buffer) = state.agent_buffer.lock() {
            std::mem::take(&mut *buffer)
        } else {
            Vec::new()
        }
    };

    let report_line = PENDING_REPORTS.remove(&request_id).map(|(_, report)| {
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
                        "Standard mode: No agent payload for request {} but error detected: {} - sending to telemetry",
                        request_id, detected_error.error_type
                    );
                    // Error was already sent by log processor, just log this for visibility
                }
            }
        }
    }

    // Smart batching: Only send complete payloads (with report)
    let send_agent_task = if agent_payloads.is_empty() {
        debug!("Standard mode: No agent payload for request: {}", request_id);
        None
    } else if let Some(ref report) = report_line {
        // Both payload and report available - send now (complete data)
        debug!(
            "Standard mode: Payload + report both ready for {} - adding to batch",
            request_id
        );

        for payload_bytes in agent_payloads {
            DEFAULT_BATCH_BUFFER.add_to_batch(
                request_id.clone(),
                payload_bytes,
                Some(report.clone()),
                invoked_function_arn.clone(),
            );
        }

        // Check if batch threshold is met and send if needed (only payloads with report lines)
        if DEFAULT_BATCH_BUFFER.should_send_batch_by_threshold() {
            debug!("Batch threshold reached - sending payloads with report lines only");
            let newrelic_client_clone = newrelic_client.clone();
            let config_clone = config.clone();

            Some(tokio::spawn(async move {
                DEFAULT_BATCH_BUFFER.send_batched_payloads_with_reports_only(newrelic_client_clone, config_clone).await;
            }))
        } else {
            None
        }
    } else {
        // Only payload, no report yet - put back in buffer for next invocation
        debug!(
            "Standard mode: Payload ready but NO report yet for {} - keeping in buffer",
            request_id
        );
        debug!(
            "Payload will be sent in next invocation when platform.report arrives"
        );

        // Put payloads back in buffer (they were taken out with mem::take)
        if let Some(buffer_ref) = REQUEST_AGENT_BUFFERS.get(&request_id) {
            if let Ok(mut buffer) = buffer_ref.lock() {
                buffer.extend(agent_payloads);
            }
        }

        None
    };

    let log_flushing = global_log_processor.flush();
    let platform_flushing = state.platform_processor.flush();
    let failed_retry = retry_failed_agent_payloads(&newrelic_client, &config);

    let (log_result, platform_result, _, agent_result) = tokio::join!(
        log_flushing,
        platform_flushing,
        failed_retry,
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

    if let Ok(mut active_request) = CURRENT_ACTIVE_REQUEST_ID.lock() {
        *active_request = None;
    }

    debug!(
        "Standard mode: Completed processing for request: {}",
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
pub(crate) fn update_global_invocation_context(request_id: &str, invoked_function_arn: &str) {
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
    config: &Arc<ExtensionConfig>,
    log_processor: &Arc<LogProcessor>,
) {
    if !config.new_relic.collect_trace_id {
        return;
    }

    if let Ok(Some(trace_id)) = trace::extract_trace_id_from_payload(payload_bytes) {
        debug!("Extracted trace ID: {}, coordinating with logs", trace_id);
        if let Err(e) = log_processor.on_trace_id_extracted(&trace_id).await {
            error!("Failed to coordinate logs with trace ID: {}", e);
        }
    }
}

/// Process any pending agent payloads from previous invocation (APM mode only)
/// Excludes the current request ID to avoid processing empty buffer
async fn process_pending_agent_payloads(
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<ExtensionConfig>,
    global_log_processor: &Arc<LogProcessor>,
    apm_app: &crate::apm::SharedApmApp,
    current_request_id: &str,
) {
    let all_buffers: Vec<(String, usize)> = REQUEST_AGENT_BUFFERS
        .iter()
        .map(|entry| {
            let buffer_size = entry.value().lock().map(|b| b.len()).unwrap_or(0);
            (entry.key().clone(), buffer_size)
        })
        .collect();

    debug!(
        "APM pending check: Total buffers={}, Details: {:?}",
        all_buffers.len(),
        all_buffers
    );

    let pending_requests: Vec<(String, Arc<Mutex<Vec<Vec<u8>>>>)> = REQUEST_AGENT_BUFFERS
        .iter()
        .filter(|entry| entry.key() != current_request_id)
        .map(|entry| (entry.key().clone(), entry.value().clone()))
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
        let context = REQUEST_CONTEXTS.get(&request_id).map(|entry| entry.value().clone());

        let invoked_function_arn = if let Some(ctx) = context {
            if let Ok(ctx_guard) = ctx.lock() {
                if !ctx_guard.invoked_function_arn.is_empty() {
                    ctx_guard.invoked_function_arn.clone()
                } else {
                    // Use global fallback ARN from registration
                    if let Ok(global_ctx) = CURRENT_INVOCATION_CONTEXT.read() {
                        global_ctx.invoked_function_arn.clone()
                    } else {
                        String::new()
                    }
                }
            } else {
                // Use global fallback ARN
                if let Ok(global_ctx) = CURRENT_INVOCATION_CONTEXT.read() {
                    global_ctx.invoked_function_arn.clone()
                } else {
                    String::new()
                }
            }
        } else {
            // Use global fallback ARN
            if let Ok(global_ctx) = CURRENT_INVOCATION_CONTEXT.read() {
                global_ctx.invoked_function_arn.clone()
            } else {
                String::new()
            }
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

            for payload_bytes in payloads {
                if let Err(e) = process_and_send_agent_payload(
                    &payload_bytes,
                    &request_id,
                    &invoked_function_arn,
                    global_log_processor,
                    newrelic_client,
                    config,
                    apm_app,
                )
                .await
                {
                    error!("Failed to process pending agent payload: {}", e);
                }
            }
        }

        // Check for pending platform.report for this old request and send as metrics (APM mode)
        if let Some(entry) = PENDING_REPORTS.get(&request_id) {
            let report_line = entry.value().clone();
            drop(entry);

            debug!("APM mode: Found pending platform.report for previous request {} - converting to metrics", request_id);

            let apm_app_guard = apm_app.read().await;
            if let Some(ref app) = *apm_app_guard {
                if let Err(e) = app.send_platform_report_metrics(&report_line).await {
                    error!("APM mode: Failed to send platform report metrics for previous request {}: {}", request_id, e);
                } else {
                    info!("APM mode: Successfully sent platform report metrics for previous request {}", request_id);
                }
            }
            drop(apm_app_guard);

            // Remove from pending reports after sending
            PENDING_REPORTS.remove(&request_id);
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
    _newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<ExtensionConfig>,
    apm_app: &crate::apm::SharedApmApp,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if config.new_relic.collect_trace_id {
        if let Ok(Some(trace_id)) = trace::extract_trace_id_from_payload(payload_bytes) {
            debug!("Extracted trace ID: {}, coordinating with logs", trace_id);

            if let Err(e) = log_processor.on_trace_id_extracted(&trace_id).await {
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
                error!("APM mode: Failed to send agent payload to APM collector: {}", e);
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
        failed_payloads.push(failed_payload);
        debug!(
            "Buffered failed agent payload for request {} (total failed: {})",
            request_id,
            failed_payloads.len()
        );
    } else {
        error!("Failed to lock FAILED_AGENT_PAYLOADS buffer - payload lost!");
    }
}

/// Retry failed agent payloads during final flush
async fn retry_failed_agent_payloads(
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<ExtensionConfig>,
) {
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

        let age = chrono::Utc::now().signed_duration_since(failed_payload.failed_at);
        if age.num_hours() > 24 {
            warn!(
                "Dropping agent payload that's too old ({} hours) for request {}",
                age.num_hours(),
                failed_payload.request_id
            );
            continue;
        }

        if failed_payload.retry_count > 5 {
            warn!(
                "Dropping agent payload after {} retries for request {}",
                failed_payload.retry_count, failed_payload.request_id
            );
            continue;
        }

        debug!(
            "Retrying agent payload for request {} (attempt {})",
            failed_payload.request_id, failed_payload.retry_count
        );

        match send_agent_payload_to_newrelic(
            &failed_payload.payload_bytes,
            &failed_payload.request_id,
            &failed_payload.invoked_function_arn,
            newrelic_client,
            config,
            None, // No version line for retries (already sent in original attempt)
        )
        .await
        {
            Ok(()) => {
                retry_successful_count += 1;
                info!(
                    "Successfully retried agent payload for request {}",
                    failed_payload.request_id
                );
            }
            Err(e) => {
                retry_failed_count += 1;
                error!("Failed to retry agent payload: {}", e);

                if failed_payload.retry_count <= 5 {
                    if let Ok(mut failed_payloads) = FAILED_AGENT_PAYLOADS.lock() {
                        failed_payloads.push(failed_payload);
                    }
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
mod tests {
    use super::*;
    use serial_test::serial;

    fn clear_event_loop_state() {
        if let Ok(mut payloads) = FAILED_AGENT_PAYLOADS.lock() {
            payloads.clear();
        }
        if let Ok(mut ctx) = LAST_REQUEST_CONTEXT.lock() {
            *ctx = None;
        }
    }

    // ========================================================================
    // update_global_invocation_context
    // ========================================================================

    #[test]
    #[serial]
    fn test_update_global_context_valid_arn() {
        let arn = "arn:aws:lambda:us-east-1:123456789012:function:my-fn";
        update_global_invocation_context("req-123", arn);

        if let Ok(ctx) = crate::CURRENT_INVOCATION_CONTEXT.read() {
            assert_eq!(ctx.request_id, "req-123");
            assert_eq!(ctx.invoked_function_arn, arn);
            assert!(ctx.trace_id.is_none());
        }
    }

    #[test]
    #[serial]
    fn test_update_global_context_empty_arn_preserves_previous() {
        let arn = "arn:aws:lambda:us-east-1:123:function:fn";
        update_global_invocation_context("req-1", arn);

        // Now update with empty ARN — should keep previous ARN but update request_id
        update_global_invocation_context("req-2", "");

        if let Ok(ctx) = crate::CURRENT_INVOCATION_CONTEXT.read() {
            assert_eq!(ctx.request_id, "req-2");
            assert_eq!(ctx.invoked_function_arn, arn, "ARN should be preserved when empty is passed");
        }
    }

    #[test]
    #[serial]
    fn test_update_global_context_overwrites() {
        update_global_invocation_context("req-1", "arn:first");
        update_global_invocation_context("req-2", "arn:second");

        if let Ok(ctx) = crate::CURRENT_INVOCATION_CONTEXT.read() {
            assert_eq!(ctx.request_id, "req-2");
            assert_eq!(ctx.invoked_function_arn, "arn:second");
        }
    }

    // ========================================================================
    // buffer_failed_agent_payload
    // ========================================================================

    #[test]
    #[serial]
    fn test_buffer_failed_agent_payload_pushes() {
        clear_event_loop_state();
        buffer_failed_agent_payload(b"payload-data", "req-1", "arn:test");

        let guard = FAILED_AGENT_PAYLOADS.lock().expect("should lock");
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0].request_id, "req-1");
        assert_eq!(guard[0].invoked_function_arn, "arn:test");
        assert_eq!(guard[0].payload_bytes, b"payload-data");
        assert_eq!(guard[0].retry_count, 0);
        drop(guard);
        clear_event_loop_state();
    }

    #[test]
    #[serial]
    fn test_buffer_failed_agent_payload_multiple() {
        clear_event_loop_state();
        buffer_failed_agent_payload(b"p1", "req-1", "arn:1");
        buffer_failed_agent_payload(b"p2", "req-2", "arn:2");
        buffer_failed_agent_payload(b"p3", "req-3", "arn:3");

        let guard = FAILED_AGENT_PAYLOADS.lock().expect("should lock");
        assert_eq!(guard.len(), 3);
        drop(guard);
        clear_event_loop_state();
    }

    // ========================================================================
    // FailedAgentPayload struct
    // ========================================================================

    #[test]
    fn test_failed_agent_payload_struct_construction() {
        let now = chrono::Utc::now();
        let payload = FailedAgentPayload {
            payload_bytes: vec![1, 2, 3],
            request_id: "req-abc".to_string(),
            invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:fn".to_string(),
            retry_count: 2,
            failed_at: now,
        };
        assert_eq!(payload.payload_bytes, vec![1, 2, 3]);
        assert_eq!(payload.request_id, "req-abc");
        assert_eq!(payload.retry_count, 2);
        assert_eq!(payload.failed_at, now);
    }

    #[test]
    fn test_failed_agent_payload_clone() {
        let payload = FailedAgentPayload {
            payload_bytes: vec![10, 20],
            request_id: "req".to_string(),
            invoked_function_arn: "arn".to_string(),
            retry_count: 0,
            failed_at: chrono::Utc::now(),
        };
        let cloned = payload.clone();
        assert_eq!(cloned.request_id, payload.request_id);
        assert_eq!(cloned.retry_count, payload.retry_count);
        let _ = format!("{payload:?}");
    }

    // ========================================================================
    // cleanup_old_failed_payloads
    // ========================================================================

    #[test]
    #[serial]
    fn test_cleanup_old_failed_payloads_removes_old() {
        clear_event_loop_state();
        if let Ok(mut payloads) = FAILED_AGENT_PAYLOADS.lock() {
            payloads.push(FailedAgentPayload {
                payload_bytes: vec![1],
                request_id: "old-req".to_string(),
                invoked_function_arn: "arn".to_string(),
                retry_count: 0,
                failed_at: chrono::Utc::now() - chrono::Duration::hours(25),
            });
        }

        cleanup_old_failed_payloads();

        let guard = FAILED_AGENT_PAYLOADS.lock().expect("should lock");
        assert!(guard.is_empty(), "Old payload should have been removed");
        drop(guard);
        clear_event_loop_state();
    }

    #[test]
    #[serial]
    fn test_cleanup_old_failed_payloads_keeps_recent() {
        clear_event_loop_state();
        if let Ok(mut payloads) = FAILED_AGENT_PAYLOADS.lock() {
            payloads.push(FailedAgentPayload {
                payload_bytes: vec![1],
                request_id: "recent-req".to_string(),
                invoked_function_arn: "arn".to_string(),
                retry_count: 0,
                failed_at: chrono::Utc::now(),
            });
        }

        cleanup_old_failed_payloads();

        let guard = FAILED_AGENT_PAYLOADS.lock().expect("should lock");
        assert_eq!(guard.len(), 1, "Recent payload should survive cleanup");
        drop(guard);
        clear_event_loop_state();
    }

    #[test]
    #[serial]
    fn test_cleanup_old_failed_payloads_mixed() {
        clear_event_loop_state();
        if let Ok(mut payloads) = FAILED_AGENT_PAYLOADS.lock() {
            payloads.push(FailedAgentPayload {
                payload_bytes: vec![1],
                request_id: "old".to_string(),
                invoked_function_arn: "arn".to_string(),
                retry_count: 0,
                failed_at: chrono::Utc::now() - chrono::Duration::hours(25),
            });
            payloads.push(FailedAgentPayload {
                payload_bytes: vec![2],
                request_id: "recent-1".to_string(),
                invoked_function_arn: "arn".to_string(),
                retry_count: 0,
                failed_at: chrono::Utc::now(),
            });
            payloads.push(FailedAgentPayload {
                payload_bytes: vec![3],
                request_id: "recent-2".to_string(),
                invoked_function_arn: "arn".to_string(),
                retry_count: 0,
                failed_at: chrono::Utc::now() - chrono::Duration::hours(1),
            });
        }

        cleanup_old_failed_payloads();

        let guard = FAILED_AGENT_PAYLOADS.lock().expect("should lock");
        assert_eq!(guard.len(), 2, "Should keep 2 recent, remove 1 old");
        drop(guard);
        clear_event_loop_state();
    }

    #[test]
    #[serial]
    fn test_cleanup_old_failed_payloads_empty_noop() {
        clear_event_loop_state();
        cleanup_old_failed_payloads(); // Should not panic on empty list
        clear_event_loop_state();
    }

    // ========================================================================
    // LAST_REQUEST_CONTEXT
    // ========================================================================

    #[test]
    #[serial]
    fn test_last_request_context_stores_tuple() {
        clear_event_loop_state();
        if let Ok(mut guard) = LAST_REQUEST_CONTEXT.lock() {
            *guard = Some(("req-1".to_string(), "arn:test".to_string()));
        }

        let guard = LAST_REQUEST_CONTEXT.lock().expect("should lock");
        assert_eq!(*guard, Some(("req-1".to_string(), "arn:test".to_string())));
        drop(guard);
        clear_event_loop_state();
    }

    #[test]
    #[serial]
    fn test_last_request_context_overwrites() {
        clear_event_loop_state();
        if let Ok(mut guard) = LAST_REQUEST_CONTEXT.lock() {
            *guard = Some(("req-1".to_string(), "arn:1".to_string()));
        }
        if let Ok(mut guard) = LAST_REQUEST_CONTEXT.lock() {
            *guard = Some(("req-2".to_string(), "arn:2".to_string()));
        }

        let guard = LAST_REQUEST_CONTEXT.lock().expect("should lock");
        assert_eq!(*guard, Some(("req-2".to_string(), "arn:2".to_string())));
        drop(guard);
        clear_event_loop_state();
    }

    // ========================================================================
    // Mock Lambda Runtime API — for full event loop flow tests
    // ========================================================================

    use std::convert::Infallible;
    use hyper::{Response, StatusCode};
    use hyper::body::Bytes;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use http_body_util::Full;
    use tokio::net::TcpListener;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Start a mock Lambda Runtime API server that serves INVOKE then SHUTDOWN events
    async fn start_mock_runtime_api(
        invoke_count: u32,
    ) -> (u16, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let request_counter = Arc::new(AtomicU32::new(0));

        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { break };
                let counter = request_counter.clone();
                let max_invokes = invoke_count;
                tokio::spawn(async move {
                    let service = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                        let counter = counter.clone();
                        let path = req.uri().path().to_string();
                        async move {
                            if path.contains("/extension/register") {
                                let body = serde_json::json!({
                                    "functionName": "test-function",
                                    "functionVersion": "$LATEST",
                                    "accountId": "123456789012"
                                });
                                Ok::<_, Infallible>(Response::builder()
                                    .status(StatusCode::OK)
                                    .header("Lambda-Extension-Identifier", "test-ext-id")
                                    .body(Full::new(Bytes::from(body.to_string())))
                                    .expect("response"))
                            } else if path.contains("/extension/event/next") {
                                let n = counter.fetch_add(1, Ordering::SeqCst);
                                let body = if n < max_invokes {
                                    serde_json::json!({
                                        "eventType": "INVOKE",
                                        "requestId": format!("req-{n}"),
                                        "invokedFunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test-function"
                                    })
                                } else {
                                    serde_json::json!({
                                        "eventType": "SHUTDOWN",
                                        "shutdownReason": "spindown"
                                    })
                                };
                                Ok(Response::builder()
                                    .status(StatusCode::OK)
                                    .body(Full::new(Bytes::from(body.to_string())))
                                    .expect("response"))
                            } else if path.contains("/telemetry") {
                                Ok(Response::builder()
                                    .status(StatusCode::OK)
                                    .body(Full::new(Bytes::from("OK")))
                                    .expect("response"))
                            } else {
                                Ok(Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .body(Full::new(Bytes::from("not found")))
                                    .expect("response"))
                            }
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });

        (port, handle)
    }

    // ========================================================================
    // execute_noop_event_loop — full INVOKE→SHUTDOWN flow
    // ========================================================================

    #[tokio::test]
    #[serial]
    async fn test_noop_event_loop_processes_invoke_then_shutdown() {
        let (port, server_handle) = start_mock_runtime_api(2).await;
        std::env::set_var("AWS_LAMBDA_RUNTIME_API", format!("127.0.0.1:{port}"));

        let client = Arc::new(reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("client"));

        // Run noop event loop — should process 2 INVOKEs then SHUTDOWN
        execute_noop_event_loop(&client, "test-ext-id").await;

        std::env::remove_var("AWS_LAMBDA_RUNTIME_API");
        server_handle.abort();
        // If we reach here without hanging, the test passed
    }

    #[tokio::test]
    #[serial]
    async fn test_noop_event_loop_immediate_shutdown() {
        let (port, server_handle) = start_mock_runtime_api(0).await;
        std::env::set_var("AWS_LAMBDA_RUNTIME_API", format!("127.0.0.1:{port}"));

        let client = Arc::new(reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("client"));

        // 0 invokes → immediate SHUTDOWN
        execute_noop_event_loop(&client, "test-ext-id").await;

        std::env::remove_var("AWS_LAMBDA_RUNTIME_API");
        server_handle.abort();
    }

    // ========================================================================
    // run_infinite_event_loop — disabled extension → noop mode
    // ========================================================================

    #[tokio::test]
    #[serial]
    async fn test_run_infinite_event_loop_disabled_runs_noop() {
        let (port, server_handle) = start_mock_runtime_api(1).await;
        std::env::set_var("AWS_LAMBDA_RUNTIME_API", format!("127.0.0.1:{port}"));

        let config = Arc::new({
            let mut c = crate::config::ExtensionConfig::default();
            c.new_relic.extension_enabled = false; // disabled → noop
            c
        });
        let client = Arc::new(reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("client"));
        let noop_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let factory = Arc::new(crate::request::ProcessorFactory::new(
            noop_client.clone(), config.clone(), apm_app.clone(),
        ));
        let ctx = Arc::new(std::sync::Mutex::new(crate::context::InvocationContext::default()));
        let log_processor = factory.create_log_processor(ctx.clone());

        let components = ExtensionComponents {
            client,
            extension_id: "test-ext-id".to_string(),
            processor_factory: factory,
            newrelic_client: noop_client,
            config,
            harvester_handle: tokio::spawn(async {}),
            global_log_processor: log_processor,
            apm_app,
            apm_mode_enabled: false,
        };

        let (total_events, harvester_handle) = run_infinite_event_loop(components).await;
        assert_eq!(total_events, 0, "Disabled extension should report 0 events");
        harvester_handle.abort();

        std::env::remove_var("AWS_LAMBDA_RUNTIME_API");
        server_handle.abort();
    }

    // ========================================================================
    // run_infinite_event_loop — no license key → noop mode
    // ========================================================================

    #[tokio::test]
    #[serial]
    async fn test_run_infinite_event_loop_no_license_key_noop() {
        let (port, server_handle) = start_mock_runtime_api(1).await;
        std::env::set_var("AWS_LAMBDA_RUNTIME_API", format!("127.0.0.1:{port}"));

        let config = Arc::new({
            let mut c = crate::config::ExtensionConfig::default();
            c.new_relic.extension_enabled = true;
            c.new_relic.license_key = None; // no license → noop
            c
        });
        let client = Arc::new(reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("client"));
        let noop_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let factory = Arc::new(crate::request::ProcessorFactory::new(
            noop_client.clone(), config.clone(), apm_app.clone(),
        ));
        let ctx = Arc::new(std::sync::Mutex::new(crate::context::InvocationContext::default()));
        let log_processor = factory.create_log_processor(ctx.clone());

        let components = ExtensionComponents {
            client,
            extension_id: "test-ext-id".to_string(),
            processor_factory: factory,
            newrelic_client: noop_client,
            config,
            harvester_handle: tokio::spawn(async {}),
            global_log_processor: log_processor,
            apm_app,
            apm_mode_enabled: false,
        };

        let (total_events, harvester_handle) = run_infinite_event_loop(components).await;
        assert_eq!(total_events, 0, "No license key should report 0 events (noop)");
        harvester_handle.abort();

        std::env::remove_var("AWS_LAMBDA_RUNTIME_API");
        server_handle.abort();
    }

    // ========================================================================
    // execute_standard_mode_event_loop — full INVOKE→process→SHUTDOWN
    // ========================================================================

    #[tokio::test]
    #[serial]
    async fn test_standard_mode_event_loop_invoke_then_shutdown() {
        clear_event_loop_state();
        let (port, server_handle) = start_mock_runtime_api(1).await;
        std::env::set_var("AWS_LAMBDA_RUNTIME_API", format!("127.0.0.1:{port}"));

        let config = Arc::new({
            let mut c = crate::config::ExtensionConfig::default();
            c.new_relic.extension_enabled = true;
            c.new_relic.license_key = Some("test-key".to_string());
            c.new_relic.apm_lambda_mode = false;
            c
        });
        let client = Arc::new(reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("client"));
        let noop_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let factory = Arc::new(crate::request::ProcessorFactory::new(
            noop_client.clone(), config.clone(), apm_app.clone(),
        ));
        let ctx = Arc::new(std::sync::Mutex::new(crate::context::InvocationContext::default()));
        let log_processor = factory.create_log_processor(ctx.clone());

        let mut components = ExtensionComponents {
            client,
            extension_id: "test-ext-id".to_string(),
            processor_factory: factory,
            newrelic_client: noop_client,
            config,
            harvester_handle: tokio::spawn(async {}),
            global_log_processor: log_processor,
            apm_app,
            apm_mode_enabled: false,
        };

        let total_events = execute_standard_mode_event_loop(&mut components).await;
        // event_counter counts INVOKE + SHUTDOWN (1 invoke + 1 shutdown = 2)
        assert_eq!(total_events, 2, "Should count 1 INVOKE + 1 SHUTDOWN = 2 events");

        // Verify LAST_REQUEST_CONTEXT was set
        let guard = LAST_REQUEST_CONTEXT.lock().expect("lock");
        assert!(guard.is_some(), "LAST_REQUEST_CONTEXT should be set after INVOKE");
        if let Some((req_id, _arn)) = guard.as_ref() {
            assert_eq!(req_id, "req-0");
        }
        drop(guard);

        std::env::remove_var("AWS_LAMBDA_RUNTIME_API");
        server_handle.abort();
        clear_event_loop_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_standard_mode_multiple_invocations() {
        clear_event_loop_state();
        let (port, server_handle) = start_mock_runtime_api(3).await;
        std::env::set_var("AWS_LAMBDA_RUNTIME_API", format!("127.0.0.1:{port}"));

        let config = Arc::new({
            let mut c = crate::config::ExtensionConfig::default();
            c.new_relic.extension_enabled = true;
            c.new_relic.license_key = Some("test-key".to_string());
            c.new_relic.apm_lambda_mode = false;
            c
        });
        let client = Arc::new(reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("client"));
        let noop_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let factory = Arc::new(crate::request::ProcessorFactory::new(
            noop_client.clone(), config.clone(), apm_app.clone(),
        ));
        let ctx = Arc::new(std::sync::Mutex::new(crate::context::InvocationContext::default()));
        let log_processor = factory.create_log_processor(ctx.clone());

        let mut components = ExtensionComponents {
            client,
            extension_id: "test-ext-id".to_string(),
            processor_factory: factory,
            newrelic_client: noop_client,
            config,
            harvester_handle: tokio::spawn(async {}),
            global_log_processor: log_processor,
            apm_app,
            apm_mode_enabled: false,
        };

        let total_events = execute_standard_mode_event_loop(&mut components).await;
        // 3 INVOKEs + 1 SHUTDOWN = 4 events
        assert_eq!(total_events, 4, "Should count 3 INVOKE + 1 SHUTDOWN = 4 events");

        // Last request context should be from the last invocation
        let guard = LAST_REQUEST_CONTEXT.lock().expect("lock");
        if let Some((req_id, _)) = guard.as_ref() {
            assert_eq!(req_id, "req-2", "Last request should be req-2");
        }
        drop(guard);

        std::env::remove_var("AWS_LAMBDA_RUNTIME_API");
        server_handle.abort();
        clear_event_loop_state();
    }

    // ========================================================================
    // execute_apm_mode_event_loop — APM mode INVOKE→process→SHUTDOWN
    // ========================================================================

    #[tokio::test]
    #[serial]
    async fn test_apm_mode_event_loop_invoke_then_shutdown() {
        clear_event_loop_state();
        let (port, server_handle) = start_mock_runtime_api(1).await;
        std::env::set_var("AWS_LAMBDA_RUNTIME_API", format!("127.0.0.1:{port}"));

        let config = Arc::new({
            let mut c = crate::config::ExtensionConfig::default();
            c.new_relic.extension_enabled = true;
            c.new_relic.license_key = Some("test-key".to_string());
            c.new_relic.apm_lambda_mode = true;
            c
        });
        let client = Arc::new(reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("client"));
        let noop_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let factory = Arc::new(crate::request::ProcessorFactory::new(
            noop_client.clone(), config.clone(), apm_app.clone(),
        ));
        let ctx = Arc::new(std::sync::Mutex::new(crate::context::InvocationContext::default()));
        let log_processor = factory.create_log_processor(ctx.clone());

        let mut components = ExtensionComponents {
            client,
            extension_id: "test-ext-id".to_string(),
            processor_factory: factory,
            newrelic_client: noop_client,
            config,
            harvester_handle: tokio::spawn(async {}),
            global_log_processor: log_processor,
            apm_app,
            apm_mode_enabled: true,
        };

        let total_events = execute_apm_mode_event_loop(&mut components).await;
        // 1 INVOKE + 1 SHUTDOWN = 2 events
        assert_eq!(total_events, 2, "Should count 1 INVOKE + 1 SHUTDOWN = 2 events in APM mode");

        std::env::remove_var("AWS_LAMBDA_RUNTIME_API");
        server_handle.abort();
        clear_event_loop_state();
    }

    // ========================================================================
    // extract_and_coordinate_trace_id — disabled path
    // ========================================================================

    #[tokio::test]
    async fn test_extract_trace_id_disabled_returns_immediately() {
        let config = Arc::new({
            let mut c = config::ExtensionConfig::default();
            c.new_relic.collect_trace_id = false; // disabled
            c
        });
        let noop_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let factory = crate::request::ProcessorFactory::new(noop_client, config.clone(), apm_app);
        let ctx = Arc::new(std::sync::Mutex::new(crate::context::InvocationContext::default()));
        let log_processor = factory.create_log_processor(ctx);

        // Should return immediately without doing anything
        extract_and_coordinate_trace_id(b"any-payload-data", &config, &log_processor).await;
        // If we reach here without panic, disabled path works
    }

    #[tokio::test]
    async fn test_extract_trace_id_enabled_with_invalid_payload() {
        let config = Arc::new({
            let mut c = config::ExtensionConfig::default();
            c.new_relic.collect_trace_id = true; // enabled
            c
        });
        let noop_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let factory = crate::request::ProcessorFactory::new(noop_client, config.clone(), apm_app);
        let ctx = Arc::new(std::sync::Mutex::new(crate::context::InvocationContext::default()));
        let log_processor = factory.create_log_processor(ctx);

        // Invalid payload — extraction should fail silently (no trace ID found)
        extract_and_coordinate_trace_id(b"not-a-valid-agent-payload", &config, &log_processor).await;
        // No panic = passes
    }

    // ========================================================================
    // send_to_apm_collector — APM app not ready
    // ========================================================================

    #[tokio::test]
    async fn test_send_to_apm_collector_no_app_returns_error() {
        let noop_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let config = Arc::new(config::ExtensionConfig::default());
        let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

        let result = send_to_apm_collector(
            b"test-payload",
            "req-1",
            "arn:test",
            &noop_client,
            &config,
            &apm_app,
        )
        .await;

        assert!(result.is_err(), "No APM app should return error");
        let err_msg = result.expect_err("should be error").to_string();
        assert!(err_msg.contains("not ready") || err_msg.contains("buffered"),
            "Error should mention APM not ready, got: {err_msg}");
    }

    // ========================================================================
    // retry_failed_agent_payloads — with mock NR server
    // ========================================================================

    #[tokio::test]
    #[serial]
    async fn test_retry_failed_agent_payloads_empty_is_noop() {
        clear_event_loop_state();

        let nr_server = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let nr_port = nr_server.local_addr().expect("addr").port();
        let nr_handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = nr_server.accept().await else { break };
                tokio::spawn(async move {
                    let svc = service_fn(|_| async {
                        Ok::<_, Infallible>(Response::builder().status(StatusCode::OK)
                            .body(Full::new(Bytes::from("OK"))).expect("r"))
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), svc).await;
                });
            }
        });

        let mut config = config::ExtensionConfig::default();
        config.new_relic.license_key = Some("test-key".to_string());
        config.new_relic.telemetry_endpoint = format!("http://127.0.0.1:{nr_port}");
        let config = Arc::new(config);
        let client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));

        // Empty list — should return quickly
        retry_failed_agent_payloads(&client, &config).await;

        nr_handle.abort();
        clear_event_loop_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_retry_failed_agent_payloads_drops_old_payloads() {
        clear_event_loop_state();

        // Add a payload older than 24 hours
        if let Ok(mut payloads) = FAILED_AGENT_PAYLOADS.lock() {
            payloads.push(FailedAgentPayload {
                payload_bytes: b"old-data".to_vec(),
                request_id: "req-old".to_string(),
                invoked_function_arn: "arn:test".to_string(),
                retry_count: 0,
                failed_at: chrono::Utc::now() - chrono::Duration::hours(25),
            });
        }

        let nr_server = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let nr_port = nr_server.local_addr().expect("addr").port();
        let nr_handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = nr_server.accept().await else { break };
                tokio::spawn(async move {
                    let svc = service_fn(|_| async {
                        Ok::<_, Infallible>(Response::builder().status(StatusCode::OK)
                            .body(Full::new(Bytes::from("OK"))).expect("r"))
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), svc).await;
                });
            }
        });

        let mut config = config::ExtensionConfig::default();
        config.new_relic.license_key = Some("test-key".to_string());
        config.new_relic.telemetry_endpoint = format!("http://127.0.0.1:{nr_port}");
        let config = Arc::new(config);
        let client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));

        retry_failed_agent_payloads(&client, &config).await;

        // Old payload should have been dropped (>24h)
        let guard = FAILED_AGENT_PAYLOADS.lock().expect("lock");
        assert!(guard.is_empty(), "Old payload should be dropped");
        drop(guard);

        nr_handle.abort();
        clear_event_loop_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_retry_failed_agent_payloads_drops_over_max_retries() {
        clear_event_loop_state();

        // Add a payload with retry_count > 5
        if let Ok(mut payloads) = FAILED_AGENT_PAYLOADS.lock() {
            payloads.push(FailedAgentPayload {
                payload_bytes: b"retry-exhausted".to_vec(),
                request_id: "req-exhausted".to_string(),
                invoked_function_arn: "arn:test".to_string(),
                retry_count: 5, // Will be incremented to 6, exceeding limit
                failed_at: chrono::Utc::now(),
            });
        }

        let nr_server = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let nr_port = nr_server.local_addr().expect("addr").port();
        let nr_handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = nr_server.accept().await else { break };
                tokio::spawn(async move {
                    let svc = service_fn(|_| async {
                        Ok::<_, Infallible>(Response::builder().status(StatusCode::OK)
                            .body(Full::new(Bytes::from("OK"))).expect("r"))
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), svc).await;
                });
            }
        });

        let mut config = config::ExtensionConfig::default();
        config.new_relic.license_key = Some("test-key".to_string());
        config.new_relic.telemetry_endpoint = format!("http://127.0.0.1:{nr_port}");
        let config = Arc::new(config);
        let client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));

        retry_failed_agent_payloads(&client, &config).await;

        // Payload with >5 retries should be dropped
        let guard = FAILED_AGENT_PAYLOADS.lock().expect("lock");
        assert!(guard.is_empty(), "Over-retried payload should be dropped");
        drop(guard);

        nr_handle.abort();
        clear_event_loop_state();
    }

    // ========================================================================
    // process_request_concurrently — with noop components
    // ========================================================================

    #[tokio::test]
    #[serial]
    async fn test_process_request_concurrently_no_state_returns() {
        clear_event_loop_state();
        // Call with a request_id that has no state in REQUEST_PROCESSORS
        // Should return early with error log
        let config = Arc::new(config::ExtensionConfig::default());
        let noop_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let factory = Arc::new(crate::request::ProcessorFactory::new(
            noop_client.clone(), config.clone(), apm_app.clone(),
        ));
        let ctx = Arc::new(std::sync::Mutex::new(crate::context::InvocationContext::default()));
        let log_processor = factory.create_log_processor(ctx);

        // No state inserted for "ghost-req" — should return early
        process_request_concurrently(
            "ghost-req".to_string(),
            "arn:test".to_string(),
            factory,
            noop_client,
            config,
            log_processor,
            apm_app,
        )
        .await;
        // No panic = passes
        clear_event_loop_state();
    }

    // ========================================================================
    // process_apm_request — with noop components
    // ========================================================================

    #[tokio::test]
    #[serial]
    async fn test_process_apm_request_no_state_returns() {
        clear_event_loop_state();
        let config = Arc::new(config::ExtensionConfig::default());
        let noop_client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let factory = Arc::new(crate::request::ProcessorFactory::new(
            noop_client.clone(), config.clone(), apm_app.clone(),
        ));
        let ctx = Arc::new(std::sync::Mutex::new(crate::context::InvocationContext::default()));
        let log_processor = factory.create_log_processor(ctx);

        // No state for "ghost-apm-req" — should return early
        process_apm_request(
            "ghost-apm-req".to_string(),
            "arn:test".to_string(),
            true,
            factory,
            noop_client,
            config,
            log_processor,
            apm_app,
        )
        .await;
        clear_event_loop_state();
    }
}
