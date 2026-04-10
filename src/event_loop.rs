

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
    agent::payload::send_agent_payload_to_newrelic,
    error_synthesis,
    trace,
    version,
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

                let pending_task = Some(tokio::spawn({
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
                }));

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
                        use crate::agent::batch::cleanup_old_batch_entries;
                        use crate::request::cleanup_old_request_buffers;
                        cleanup_old_batch_entries(newrelic_client.clone(), config.clone()).await;
                        cleanup_old_request_buffers(newrelic_client, config).await;
                        cleanup_old_failed_payloads();
                    });
                }
            }
            runtime::LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
                let shutdown_start_time = std::time::Instant::now();
                info!("APM mode: Extension shutting down with reason: {} (started at {:?})", shutdown_reason, std::time::SystemTime::now());

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

                            if let Err(e) = app.send_platform_report_metrics(&report_line).await {
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

                // Final flush of logs
                if let Err(e) = components.global_log_processor.flush().await {
                    error!("APM mode shutdown: Failed to flush logs: {}", e);
                }

                info!("APM mode shutdown: All data processed and sent in {}ms", shutdown_start_time.elapsed().as_millis());
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
                        
                        send_batched_payloads_with_reports_only(
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
        crate::request::increment_invocation_counter();
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

                // Set this as the currently active request for agent payload routing
                if let Ok(mut active_request) = request::CURRENT_ACTIVE_REQUEST_ID.lock() {
                    *active_request = Some(request_id.clone());
                }

                // SKIP old buffer processing to avoid deadlocks
                // Late payloads are already handled via the buffer matching on next invocation
                // The complex locking in this loop was causing 7-second deadlocks

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
                        use crate::agent::batch::cleanup_old_batch_entries;
                        use crate::request::cleanup_old_request_buffers;
                        cleanup_old_batch_entries(newrelic_client.clone(), config.clone()).await;
                        cleanup_old_request_buffers(newrelic_client, config).await;
                    });
                }
            }
            runtime::LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
                let shutdown_start_time = std::time::Instant::now();
                info!("Standard mode: Extension shutting down with reason: {} (started at {:?})", shutdown_reason, std::time::SystemTime::now());

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
                send_all_pending_payloads_on_shutdown(
                    components.newrelic_client.clone(),
                    components.config.clone(),
                )
                .await;

                // Emergency flush of pre-invoke buffer (logs from INIT phase if shutdown before first INVOKE)
                if let Err(e) = components.global_log_processor.flush_pre_invoke_buffer_on_shutdown().await {
                    error!("Standard mode shutdown: Failed to flush pre-invoke buffer: {}", e);
                }

                // Flush remaining buffered logs
                if let Err(e) = components.global_log_processor.flush().await {
                    error!("Standard mode shutdown: Failed to flush logs: {}", e);
                }

                info!("Standard mode shutdown: All data processed and sent in {}ms", shutdown_start_time.elapsed().as_millis());
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
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
    apm_app: crate::apm::SharedApmApp,
) {
    debug!("APM mode: Starting processing for request: {}", request_id);

    // Set active request for agent payload routing (2.4.1 approach - simple and reliable)
    if let Ok(mut active_request) = request::CURRENT_ACTIVE_REQUEST_ID.lock() {
        *active_request = Some(request_id.clone());
    }

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

                for payload_bytes in late_payloads {
                    debug!(
                        "Sending late agent payload for request: {} ({} bytes)",
                        old_request_id,
                        payload_bytes.len()
                    );

                    if let Err(e) = send_to_apm_collector(
                        &payload_bytes,
                        &old_request_id,
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
            if let Some(buffer_ref) = get_agent_buffer(&request_id) {
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
    if let Some(report_line) = get_pending_report(&request_id) {
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

        // Remove pending report after sending
        remove_pending_report(&request_id);
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

    // Keep active request set for late payload routing (agent payloads may arrive after processing)
    // It will be overwritten when next INVOKE arrives
    if let Ok(mut active_request) = request::CURRENT_ACTIVE_REQUEST_ID.lock() {
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

pub async fn process_request_concurrently(
    request_id: String,
    invoked_function_arn: String,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
) {
    debug!(
        "Standard mode: Starting processing for request: {}",
        request_id
    );

    // Set active request for agent payload routing (2.4.1 approach - simple and reliable)
    if let Ok(mut active_request) = request::CURRENT_ACTIVE_REQUEST_ID.lock() {
        *active_request = Some(request_id.clone());
    }

    let state = REQUEST_PROCESSORS.remove(&request_id).map(|(_, v)| v);

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

    // Try to take payloads in a single lock. If empty, wait for coordination
    // signal then take again — avoids a TOCTOU gap between check and drain.
    let mut agent_payloads = {
        if let Ok(mut buffer) = state.agent_buffer.lock() {
            std::mem::take(&mut *buffer)
        } else {
            Vec::new()
        }
    };

    if agent_payloads.is_empty() {
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
        // After wakeup, drain whatever arrived
        agent_payloads = if let Ok(mut buffer) = state.agent_buffer.lock() {
            std::mem::take(&mut *buffer)
        } else {
            Vec::new()
        };
    } else {
        debug!(
            "Agent payload already in buffer for request: {} - no wait needed",
            request_id
        );
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
            "Standard mode: Payload ready but NO report yet for {} - keeping in buffer",
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

    // Keep active request set for late payload routing (agent payloads may arrive after processing)
    // It will be overwritten when next INVOKE arrives
    if let Ok(mut active_request) = request::CURRENT_ACTIVE_REQUEST_ID.lock() {
        *active_request = Some(request_id.clone());
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

            for payload_bytes in payloads {
                if let Err(e) = process_and_send_agent_payload(
                    &payload_bytes,
                    &request_id,
                    &invoked_function_arn,
                    global_log_processor,
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
        if let Some(report_line) = get_pending_report(&request_id) {
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
fn buffer_failed_agent_payload(
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

