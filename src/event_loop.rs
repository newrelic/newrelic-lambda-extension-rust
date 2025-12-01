

use std::sync::{Arc, Mutex};
use std::time::Duration;
use reqwest::Client;
use tracing::{debug, error, info, trace, warn};

use crate::{
    runtime,
    config::ExtensionConfig,
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
    agent::batch::{add_to_batch, should_send_batch_by_threshold, send_batched_payloads_with_reports_only},
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
/// Routes to APM or standard mode based on config
async fn execute_main_telemetry_processing_loop(components: &mut ExtensionComponents) -> u32 {
    let is_apm_mode = components.apm_app.read().await.is_some();

    if is_apm_mode {
        info!("Starting APM mode event loop");
        execute_apm_mode_event_loop(components).await
    } else {
        info!("Starting standard mode event loop");
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
                    tag_lambda_function_once(invoked_function_arn.clone());
                }

                update_global_invocation_context(&request_id, &invoked_function_arn);

                components
                    .global_log_processor
                    .process_buffered_logs_with_request_id(&request_id);

                let request_state = create_request_processing_state(
                    &request_id,
                    &invoked_function_arn,
                    &components.processor_factory,
                );
                REQUEST_PROCESSORS.insert(request_id.clone(), request_state);

                let buffer_count = REQUEST_AGENT_BUFFERS.len();
                if buffer_count > 0 {
                    info!(
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
                    info!(
                        "COLD START: First invocation processed in {:?} (request_id: {})",
                        event_time, request_id
                    );
                    IS_WARM_START.store(true, std::sync::atomic::Ordering::Relaxed);
                } else {
                    info!(
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
                info!("APM mode: Extension shutting down with reason: {}", shutdown_reason);

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

                // Process any remaining pending agent payloads
                process_pending_agent_payloads(
                    &components.newrelic_client,
                    &components.config,
                    &components.global_log_processor,
                    &components.apm_app,
                    "",
                )
                .await;
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

                if is_cold_start && components.config.new_relic.add_version_detail_tags {
                    tag_lambda_function_once(invoked_function_arn.clone());
                }

                update_global_invocation_context(&request_id, &invoked_function_arn);

                components
                    .global_log_processor
                    .process_buffered_logs_with_request_id(&request_id);

                // SKIP old buffer processing to avoid deadlocks
                // Late payloads are already handled via the buffer matching on next invocation
                // The complex locking in this loop was causing 7-second deadlocks

                // Send batch if threshold is reached after processing late payloads (only with report lines)
                // CRITICAL: Must await to prevent Lambda from freezing network mid-request
                if should_send_batch_by_threshold() {
                    info!("Batch threshold reached - sending payloads with report lines only");
                    send_batched_payloads_with_reports_only(
                        components.newrelic_client.clone(),
                        components.config.clone()
                    ).await;
                }

                let request_state = create_request_processing_state(
                    &request_id,
                    &invoked_function_arn,
                    &components.processor_factory,
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
                    info!(
                        "COLD START: First invocation processed in {:?} (request_id: {})",
                        event_time, request_id
                    );
                    IS_WARM_START.store(true, std::sync::atomic::Ordering::Relaxed);
                } else {
                    info!(
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
                info!("Standard mode: Extension shutting down with reason: {}", shutdown_reason);

                // Synthesize and send error based on shutdown reason
                if let Some((last_request_id, last_arn)) = LAST_REQUEST_CONTEXT.lock().ok().and_then(|guard| guard.clone()) {
                    match shutdown_reason {
                        runtime::ShutdownReason::Timeout => {
                            // Lambda timeout - send timeout error with reason
                            info!("Shutdown due to timeout - synthesizing timeout error for request: {}", last_request_id);
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
                            info!("Shutdown due to failure - synthesizing fault error for request: {}", last_request_id);
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

                wait_for_all_requests_completion(
                    components.newrelic_client.clone(),
                    components.config.clone(),
                    components.global_log_processor.clone(),
                )
                .await;
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
                info!("Extension shutting down");
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
            Err(_) => {}
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
            info!(
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

    // Unified approach: Skip runtime.done wait for all invocations (APM mode)
    // Late payloads will be processed in next invocation
    debug!(
        "APM mode: Skipping runtime.done wait for request: {} (unified batching approach)",
        request_id
    );

    let payload_already_arrived = {
        if let Ok(buffer) = state.agent_buffer.lock() {
            !buffer.is_empty()
        } else {
            false
        }
    };

    // Unified wait timeout for all invocations (APM mode)
    if !payload_already_arrived {
        debug!(
            "APM mode: Waiting up to 100ms for agent payload for request: {}",
            request_id
        );
        tokio::select! {
            _ = state.coordination_rx.as_mut().expect("coordination_rx should exist").recv() => {
                debug!("Agent payload received early for request: {} (saved wait time)", request_id);
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                debug!("Agent payload wait timeout (100ms) for request: {} - may arrive late", request_id);
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

    let got_payload_now = !agent_payloads.is_empty();

    // Check if there was a detected error but no agent payload (APM mode)
    // This can happen when the function code has errors but doesn't send telemetry
    if !got_payload_now {
        if let Ok(guard) = crate::error_synthesis::LAST_DETECTED_ERROR.lock() {
            if let Some(ref detected_error) = *guard {
                if detected_error.request_id == request_id {
                    info!(
                        "APM mode: No agent payload for request {} but error detected: {} - error event was already sent by log processor",
                        request_id, detected_error.error_type
                    );
                    // Error event was already sent by log processor via send_error_event_from_fault
                }
            }
        }
    }

    let send_agent_task = if got_payload_now {
        info!(
            "APM mode: Sending {} agent payload(s) immediately to APM collector",
            agent_payloads.len()
        );
        let request_id_clone = request_id.clone();
        let invoked_function_arn_clone = invoked_function_arn.clone();
        let newrelic_client_clone = newrelic_client.clone();
        let config_clone = config.clone();
        let global_log_processor_clone = global_log_processor.clone();
        let apm_app_clone = apm_app.clone();

        Some(tokio::spawn(async move {
            for payload_bytes in agent_payloads {
                extract_and_coordinate_trace_id(
                    &payload_bytes,
                    &config_clone,
                    &global_log_processor_clone,
                )
                .await;

                if let Err(e) = send_to_apm_collector(
                    &payload_bytes,
                    &request_id_clone,
                    &invoked_function_arn_clone,
                    &newrelic_client_clone,
                    &config_clone,
                    &apm_app_clone,
                )
                .await
                {
                    error!("Failed to send agent payload to APM collector: {}", e);
                }
            }
        }))
    } else {
        debug!(
            "APM mode: No agent payload yet for request: {} - buffer kept alive for late arrival",
            request_id
        );
        None
    };

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

    if got_payload_now {
        debug!(
            "APM mode: Agent payload sent - cleaning up all resources for request: {}",
            request_id
        );
        cleanup_request_processing_state_internal(&request_id, false);

        if let Ok(mut active_request) = CURRENT_ACTIVE_REQUEST_ID.lock() {
            *active_request = None;
        }
    } else {
        cleanup_request_processing_state_internal(&request_id, true);

        if let Ok(mut active_request) = CURRENT_ACTIVE_REQUEST_ID.lock() {
            *active_request = Some(request_id.clone());
        }
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
        info!(
            "APM mode: Processing agent payload for request: {} (size: {} bytes)",
            request_id,
            payload_bytes.len()
        );
        app.process_agent_payload(payload_bytes.to_vec()).await?;
        info!(
            "APM mode: Agent payload sent successfully for request: {}",
            request_id
        );
    } else {
        error!("APM app not initialized - cannot send payload");
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
                    info!(
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
        info!("Standard mode: No agent payload for request: {}", request_id);
        None
    } else if let Some(ref report) = report_line {
        // Both payload and report available - send now (complete data)
        info!(
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
            info!("Batch threshold reached - sending payloads with report lines only");
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
        info!(
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
fn tag_lambda_function_once(invoked_function_arn: String) {
    static TAGGING_DONE: std::sync::Once = std::sync::Once::new();
    TAGGING_DONE.call_once(|| {
        info!("Spawning background task to tag Lambda function with version information");
        let version_info = version::VersionInfo::get_or_detect();
        version::tagging::tag_lambda_function_background(
            version_info.extension_version.clone(),
            version_info.agent_version.clone(),
            version_info.layer_version.clone(),
            invoked_function_arn,
        );
    });
}

/// Update global invocation context for telemetry processors
fn update_global_invocation_context(request_id: &str, invoked_function_arn: &str) {
    if let Ok(mut global_context) = crate::CURRENT_INVOCATION_CONTEXT.lock() {
        global_context.request_id = request_id.to_string();
        global_context.invoked_function_arn = invoked_function_arn.to_string();
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
        info!("Extracted trace ID: {}, coordinating with logs", trace_id);
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

    info!(
        "Processing {} pending agent payload buffer(s) from previous invocations (excluding current: {})",
        pending_requests.len(),
        current_request_id
    );

    for (request_id, buffer) in pending_requests {
        let context = REQUEST_CONTEXTS.get(&request_id).map(|entry| entry.value().clone());

        let invoked_function_arn = if let Some(ctx) = context {
            if let Ok(ctx_guard) = ctx.lock() {
                ctx_guard.invoked_function_arn.clone()
            } else {
                "unknown".to_string()
            }
        } else {
            "unknown".to_string()
        };

        let payloads = {
            if let Ok(mut buf) = buffer.lock() {
                std::mem::take(&mut *buf)
            } else {
                Vec::new()
            }
        };

        if !payloads.is_empty() {
            info!(
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

        cleanup_request_processing_state_internal(&request_id, false);
    }
}

/// Process and send agent payload following our simple flow
async fn process_and_send_agent_payload(
    payload_bytes: &[u8],
    request_id: &str,
    invoked_function_arn: &str,
    log_processor: &Arc<LogProcessor>,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<ExtensionConfig>,
    apm_app: &crate::apm::SharedApmApp,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if config.new_relic.collect_trace_id {
        if let Ok(Some(trace_id)) = trace::extract_trace_id_from_payload(payload_bytes) {
            info!("Extracted trace ID: {}, coordinating with logs", trace_id);

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
        info!(
            "APM mode: Processing agent payload (size: {} bytes)",
            payload_bytes.len()
        );
        match app.process_agent_payload(payload_bytes.to_vec()).await {
            Ok(()) => {
                info!("APM agent payload processed and sent successfully");
            }
            Err(e) => {
                error!("Failed to send agent payload to APM collector: {}", e);
                buffer_failed_agent_payload(payload_bytes, request_id, invoked_function_arn);
                warn!(
                    "APM agent payload buffered for retry (size: {} bytes)",
                    payload_bytes.len()
                );
            }
        }
    } else {
        match send_agent_payload_to_newrelic(
            payload_bytes,
            request_id,
            invoked_function_arn,
            newrelic_client,
            config,
        )
        .await
        {
            Ok(()) => {
                info!(
                    "Agent payload processed and sent (size: {} bytes)",
                    payload_bytes.len()
                );
            }
            Err(e) => {
                error!(
                    "Failed to send agent payload for request {}: {}",
                    request_id, e
                );

                buffer_failed_agent_payload(payload_bytes, request_id, invoked_function_arn);

                warn!(
                    "Agent payload buffered for retry (size: {} bytes)",
                    payload_bytes.len()
                );
            }
        }
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
        info!(
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

    info!(
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
        )
        .await
        {
            Ok(()) => {
                retry_successful_count += 1;
                debug!(
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
        info!(
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

