//! Main event processing loops for Lambda extension lifecycle
//!
//! This module contains the core event processing logic for both APM and Standard modes:
//! - execute_apm_mode_event_loop: APM collector mode (immediate sending, no batching)
//! - execute_standard_mode_event_loop: Serverless ingest API mode (batching optimization)
//! - execute_noop_event_loop: No-op mode when extension is disabled
//! - process_apm_request: APM mode request processing
//! - process_request_concurrently: Standard mode request processing with batching
//!
//! Event loop pattern:
//! 1. GET /next (blocks - Lambda freezes extension)
//! 2. Receive INVOKE event (Lambda unfreezes)
//! 3. Process event quickly (< 50ms for warm starts)
//! 4. Return to step 1 (loops until SHUTDOWN)

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
        cleanup_request_processing_state,
        cleanup_request_processing_state_conditional,
        cleanup_request_processing_state_internal,
        wait_for_all_requests_completion,
        REQUEST_PROCESSORS, REQUEST_AGENT_BUFFERS, REQUEST_CONTEXTS,
        CURRENT_ACTIVE_REQUEST_ID, PENDING_REPORTS,
    },
    agent::batch::{add_to_batch, should_send_batch, send_batched_payloads, send_agent_with_report_immediately},
    agent::payload::send_agent_payload_to_newrelic,
    trace,
    version,
    IS_WARM_START,
};

/// Structure holding all extension components for event loop
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

/// Failed agent payload structure for retry across invocations
#[derive(Debug, Clone)]
pub struct FailedAgentPayload {
    pub payload_bytes: Vec<u8>,
    pub request_id: String,
    pub invoked_function_arn: String,
    pub retry_count: usize,
    pub failed_at: chrono::DateTime<chrono::Utc>,
}

/// Global failed agent payloads buffer for retry across invocations
pub static FAILED_AGENT_PAYLOADS: once_cell::sync::Lazy<Arc<Mutex<Vec<FailedAgentPayload>>>> =
    once_cell::sync::Lazy::new(|| Arc::new(Mutex::new(Vec::new())));

/// Infinite event loop - handles first invoke (cold start) and all subsequent invokes (warm starts)
pub async fn run_infinite_event_loop(
    mut extension_components: ExtensionComponents,
) -> (u32, tokio::task::JoinHandle<()>) {
    // Check if this is a no-op mode
    if !extension_components.config.new_relic.extension_enabled
        || extension_components.config.new_relic.license_key.is_none()
    {
        info!("Running in no-op mode");
        execute_noop_event_loop(&extension_components.client, &extension_components.extension_id)
            .await;
        return (0, extension_components.harvester_handle);
    }

    // Execute main telemetry processing loop
    let total_events = execute_main_telemetry_processing_loop(&mut extension_components).await;
    (total_events, extension_components.harvester_handle)
}

/// INFINITE EVENT LOOP - Core of Lambda Extension Lifecycle
///
/// This loop implements the correct Lambda extension pattern:
/// 1. GET /next (blocks here - Lambda freezes extension)
/// 2. Receive INVOKE event (Lambda unfreezes extension)
/// 3. Process event quickly (< 50ms for warm starts)
/// 4. Return to step 1 (loops forever until SHUTDOWN)
///
/// First iteration = Cold Start, subsequent iterations = Warm Starts
///
/// Routes to appropriate loop based on APM mode
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

/// APM MODE EVENT LOOP - Optimized for APM Collector
///
/// Key characteristics:
/// - Sends agent payloads immediately to APM collector (5 telemetry types)
/// - No batching or platform.report coordination
/// - Keeps buffer alive on warm starts to catch late-arriving payloads
/// - Processes pending payloads from previous invocation on warm starts
pub async fn execute_apm_mode_event_loop(components: &mut ExtensionComponents) -> u32 {
    let mut event_counter = 0;

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

                // Tag Lambda function on first invocation (with real ARN)
                if is_cold_start && components.config.new_relic.add_version_detail_tags {
                    tag_lambda_function_once(invoked_function_arn.clone());
                }

                // Update global context
                update_global_invocation_context(&request_id, &invoked_function_arn);

                // Process buffered logs
                components
                    .global_log_processor
                    .process_buffered_logs_with_request_id(&request_id);

                // Create request state FIRST (before processing pending payloads)
                let request_state = create_request_processing_state(
                    &request_id,
                    &invoked_function_arn,
                    &components.processor_factory,
                );
                REQUEST_PROCESSORS.insert(request_id.clone(), request_state);

                // WARM START: Process pending payloads in PARALLEL with current request
                // This prevents blocking the current request while processing old late payloads
                let pending_task = if !is_cold_start {
                    // Log buffer state before processing
                    let buffer_count = REQUEST_AGENT_BUFFERS.len();
                    info!(
                        "APM warm start: Found {} request buffer(s) before processing (current: {})",
                        buffer_count, request_id
                    );

                    Some(tokio::spawn({
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
                                &current_request_id, // Exclude current request
                            )
                            .await;
                        }
                    }))
                } else {
                    None
                };

                // Process current request in APM mode (runs in parallel with pending task)
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

                // Wait for both tasks to complete (current request + pending payloads)
                if let Some(pending) = pending_task {
                    let (current_result, pending_result) = tokio::join!(current_task, pending);
                    if let Err(e) = current_result {
                        error!("Error in APM request processing: {}", e);
                    }
                    if let Err(e) = pending_result {
                        error!("Error in pending payload processing: {}", e);
                    }
                } else if let Err(e) = current_task.await {
                    error!("Error in APM request processing: {}", e);
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
            }
            runtime::LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
                info!("APM mode: Extension shutting down: {}", shutdown_reason);
                // Process any final pending payloads (use empty string to process ALL buffers)
                process_pending_agent_payloads(
                    &components.newrelic_client,
                    &components.config,
                    &components.global_log_processor,
                    &components.apm_app,
                    "", // Empty string = process all pending buffers
                )
                .await;
                break;
            }
        }
    }

    event_counter
}

/// STANDARD MODE EVENT LOOP - Optimized for Serverless Ingest API
///
/// Key characteristics:
/// - Batches agent payloads with platform.report for warm starts
/// - Sends to serverless ingest API (single wrapped payload)
/// - Waits for platform.report coordination
/// - Cleans up immediately after processing
pub async fn execute_standard_mode_event_loop(components: &mut ExtensionComponents) -> u32 {
    let mut event_counter = 0;

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

                // Tag Lambda function on first invocation (with real ARN)
                if is_cold_start && components.config.new_relic.add_version_detail_tags {
                    tag_lambda_function_once(invoked_function_arn.clone());
                }

                // Update global context
                update_global_invocation_context(&request_id, &invoked_function_arn);

                // Process buffered logs
                components
                    .global_log_processor
                    .process_buffered_logs_with_request_id(&request_id);

                // WARM START: Clean up old buffers from previous request and process any late agent payloads
                if !is_cold_start {
                    // Find old request buffers (from previous invocation)
                    let old_requests: Vec<String> = REQUEST_AGENT_BUFFERS
                        .iter()
                        .map(|entry| entry.key().clone())
                        .collect();

                    for old_request_id in old_requests {
                        // Check if there's a late agent payload in the buffer
                        if let Some(buffer) = REQUEST_AGENT_BUFFERS.get(&old_request_id) {
                            if let Ok(buffer_guard) = buffer.lock() {
                                if !buffer_guard.is_empty() {
                                    info!(
                                        "Found {} late agent payload(s) for previous request: {}",
                                        buffer_guard.len(),
                                        old_request_id
                                    );

                                    // Check if there's a matching platform.report in PENDING_REPORTS
                                    let report_line =
                                        PENDING_REPORTS.remove(&old_request_id).map(|(_, report)| {
                                            debug!(
                                                "Found matching platform.report for late agent payload: {}",
                                                old_request_id
                                            );
                                            report
                                        });

                                    // Add to batch for sending
                                    for payload_bytes in buffer_guard.iter() {
                                        let context = REQUEST_CONTEXTS
                                            .get(&old_request_id)
                                            .map(|ctx_entry| {
                                                ctx_entry
                                                    .lock()
                                                    .ok()
                                                    .map(|ctx| ctx.invoked_function_arn.clone())
                                                    .unwrap_or_else(|| "unknown".to_string())
                                            })
                                            .unwrap_or_else(|| "unknown".to_string());

                                        add_to_batch(
                                            old_request_id.clone(),
                                            payload_bytes.clone(),
                                            report_line.clone(),
                                            context,
                                        );
                                    }
                                }
                            }
                        }

                        debug!(
                            "Cleaning up old buffer from previous request: {}",
                            old_request_id
                        );
                        cleanup_request_processing_state(&old_request_id);
                    }

                    // Check if batch should be sent now
                    if should_send_batch() {
                        let newrelic_client = components.newrelic_client.clone();
                        let config = components.config.clone();
                        tokio::spawn(async move {
                            send_batched_payloads(newrelic_client, config).await;
                        });
                    }
                }

                // Create request state
                let request_state = create_request_processing_state(
                    &request_id,
                    &invoked_function_arn,
                    &components.processor_factory,
                );

                // Update global log processor context for this request
                components
                    .global_log_processor
                    .update_invocation_context(request_state.context.clone());

                // Store request processing state
                REQUEST_PROCESSORS.insert(request_id.clone(), request_state);

                // Process request in standard mode
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
            }
            runtime::LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
                info!("Standard mode: Extension shutting down: {}", shutdown_reason);

                // Wait for all requests to complete and flush batched payloads
                wait_for_all_requests_completion(
                    components.newrelic_client.clone(),
                    components.config.clone(),
                )
                .await;
                break;
            }
        }
    }

    event_counter
}

/// No-op event loop when extension is disabled
/// Equivalent to Go's noopLoop function
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
            Err(_) => { /* Ignore errors and continue polling */ }
        }
    }
}

// ============================================================================
// APM MODE REQUEST PROCESSING
// ============================================================================

/// Process request in APM mode - simplified flow with immediate sending
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

    // CRITICAL: Set this as the currently active request for agent payload routing
    if let Ok(mut active_request) = CURRENT_ACTIVE_REQUEST_ID.lock() {
        *active_request = Some(request_id.clone());
    }

    // WARM START: Check for and process any pending late agent payloads from previous invocations
    if !is_cold_start {
        // Find all buffers that have payloads (excluding current request)
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

                // Extract payloads from buffer
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

                // Send late payloads to APM collector
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

                // Cleanup old request resources after sending late payload
                debug!(
                    "Cleaning up resources for old request after late payload: {}",
                    old_request_id
                );
                cleanup_request_processing_state_internal(&old_request_id, false);
            }
        }
    }

    // Get request processing state
    let state = REQUEST_PROCESSORS.remove(&request_id).map(|(_k, v)| v);

    let Some(mut state) = state else {
        error!("No processing state found for request: {}", request_id);
        return;
    };

    // Set invocation start time
    let invocation_start_time = chrono::Utc::now();
    global_log_processor.set_invocation_start_time(invocation_start_time);
    global_log_processor.reset_trace_id_state();
    state
        .platform_processor
        .process_invoke_event(&request_id, &invoked_function_arn);

    // COLD START: Wait for platform.runtimeDone
    // WARM START: Also wait for APM mode (agent sends payload when function completes)
    // Standard mode can skip for performance since it uses batching
    if is_cold_start {
        if let Some(ref mut runtime_done_rx) = state.runtime_done_rx {
            match runtime_done_rx.recv().await {
                Some(_) => info!(
                    "Runtime.done received for request: {} (COLD START)",
                    request_id
                ),
                None => warn!(
                    "Runtime.done channel closed for request: {} - proceeding anyway",
                    request_id
                ),
            }
        }
    } else {
        // APM WARM START: Wait for runtime.done because agent sends payload when function completes
        debug!("APM warm start: Waiting for runtime.done (agent sends payload at function completion)");
        if let Some(ref mut runtime_done_rx) = state.runtime_done_rx {
            match runtime_done_rx.recv().await {
                Some(_) => debug!(
                    "Runtime.done received for APM warm start request: {}",
                    request_id
                ),
                None => warn!(
                    "Runtime.done channel closed for request: {} - proceeding anyway",
                    request_id
                ),
            }
        }
    }

    // Wait up to 200ms for agent payload (early exit on arrival)
    let payload_already_arrived = {
        if let Ok(buffer) = state.agent_buffer.lock() {
            !buffer.is_empty()
        } else {
            false
        }
    };

    if !payload_already_arrived {
        debug!(
            "APM mode: Waiting up to 200ms for agent payload for request: {}",
            request_id
        );
        tokio::select! {
            _ = state.coordination_rx.as_mut().expect("coordination_rx should exist").recv() => {
                debug!("Agent payload received early for request: {} (saved wait time)", request_id);
            }
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                debug!("Agent payload wait timeout (200ms) for request: {} - may arrive late", request_id);
            }
        }
    } else {
        debug!(
            "Agent payload already in buffer for request: {} - no wait needed",
            request_id
        );
    }

    // Extract agent payload from buffer
    let agent_payloads = {
        if let Ok(mut buffer) = state.agent_buffer.lock() {
            std::mem::take(&mut *buffer)
        } else {
            Vec::new()
        }
    };

    // Track if we got the payload now or need to wait for late arrival
    let got_payload_now = !agent_payloads.is_empty();

    // Send agent payload immediately to APM collector if available
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
                // Extract trace ID if enabled
                extract_and_coordinate_trace_id(
                    &payload_bytes,
                    &config_clone,
                    &global_log_processor_clone,
                )
                .await;

                // Send to APM collector
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

    // Flush logs, platform data, and agent send ALL IN PARALLEL
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

    // Log any errors
    if let Err(e) = log_result {
        error!("Failed to flush logs for request {}: {}", request_id, e);
    }
    if let Err(e) = platform_result {
        error!("Failed to flush platform for request {}: {}", request_id, e);
    }
    if let Err(e) = agent_result {
        error!("Agent send task failed for request {}: {}", request_id, e);
    }

    // APM Mode Cleanup Strategy:
    // - If we got the payload now and sent it: CLEANUP immediately
    // - If payload not arrived yet: KEEP buffer alive for late arrival (next invocation will process it)
    if got_payload_now {
        debug!(
            "APM mode: Agent payload sent - cleaning up all resources for request: {}",
            request_id
        );
        cleanup_request_processing_state_internal(&request_id, false);

        // Clear active request ID
        if let Ok(mut active_request) = CURRENT_ACTIVE_REQUEST_ID.lock() {
            *active_request = None;
        }
    } else {
        debug!(
            "APM mode: No payload yet - keeping buffer alive for late arrival (will process on next invoke)"
        );
        // Keep buffer and context alive - cleanup will happen when:
        // 1. Next invocation arrives and processes pending buffers
        // 2. Or late payload arrives and gets sent
        cleanup_request_processing_state_internal(&request_id, true); // skip_buffer_cleanup = true

        // Keep active request ID so late payloads route correctly
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

// ============================================================================
// STANDARD MODE REQUEST PROCESSING
// ============================================================================

/// Process request in Standard mode - with batching and platform.report coordination
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

    // CRITICAL: Set this as the currently active request for agent payload routing
    if let Ok(mut active_request) = CURRENT_ACTIVE_REQUEST_ID.lock() {
        *active_request = Some(request_id.clone());
    }

    // Get request processing state
    let state = REQUEST_PROCESSORS.remove(&request_id).map(|(_k, v)| v);

    let Some(mut state) = state else {
        error!("No processing state found for request: {}", request_id);
        return;
    };

    // Set invocation start time
    let invocation_start_time = chrono::Utc::now();
    global_log_processor.set_invocation_start_time(invocation_start_time);
    global_log_processor.reset_trace_id_state();
    state
        .platform_processor
        .process_invoke_event(&request_id, &invoked_function_arn);

    // Check if this is a cold or warm start
    let is_cold_start = !IS_WARM_START.load(std::sync::atomic::Ordering::Relaxed);

    // COLD START: Wait for platform.runtimeDone event
    // WARM START: Skip this wait for performance
    if is_cold_start {
        if let Some(ref mut runtime_done_rx) = state.runtime_done_rx {
            match runtime_done_rx.recv().await {
                Some(_) => info!(
                    "Runtime.done received for request: {} (COLD START)",
                    request_id
                ),
                None => warn!(
                    "Runtime.done channel closed for request: {} - proceeding anyway",
                    request_id
                ),
            }
        }
    } else {
        debug!(
            "Skipping runtime.done wait for WARM START request: {} (performance optimization)",
            request_id
        );
    }

    // Wait for agent payload - Standard mode uses shorter timeout for warm starts
    let agent_wait_timeout_ms = if is_cold_start { 200 } else { 50 };

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

    // Extract agent payload from buffer
    let agent_payloads = {
        if let Ok(mut buffer) = state.agent_buffer.lock() {
            std::mem::take(&mut *buffer)
        } else {
            Vec::new()
        }
    };

    // Check if platform.report already available in PENDING_REPORTS (non-blocking)
    let report_line = PENDING_REPORTS.remove(&request_id).map(|(_, report)| {
        debug!(
            "Found pending platform.report for request: {}",
            request_id
        );
        report
    });

    // STANDARD MODE STRATEGY: Batch payloads on warm starts without platform.report
    let send_agent_task = if agent_payloads.is_empty() {
        info!("Standard mode: No agent payload for request: {}", request_id);
        None
    } else if is_cold_start {
        // COLD START: Send immediately with or without report
        info!(
            "Standard mode: Cold start - sending agent payload immediately (with report: {})",
            report_line.is_some()
        );
        let request_id_clone = request_id.clone();
        let invoked_function_arn_clone = invoked_function_arn.clone();
        let newrelic_client_clone = newrelic_client.clone();
        let config_clone = config.clone();
        let apm_app_clone = _apm_app.clone();

        Some(tokio::spawn(async move {
            send_agent_with_report_immediately(
                request_id_clone,
                invoked_function_arn_clone,
                agent_payloads,
                report_line,
                newrelic_client_clone,
                config_clone,
                apm_app_clone,
            )
            .await;
        }))
    } else if report_line.is_some() {
        // WARM START + REPORT AVAILABLE: Send immediately with report combined
        info!("Standard mode: Warm start - agent+report ready, sending combined");
        let request_id_clone = request_id.clone();
        let invoked_function_arn_clone = invoked_function_arn.clone();
        let newrelic_client_clone = newrelic_client.clone();
        let config_clone = config.clone();
        let apm_app_clone = _apm_app.clone();

        Some(tokio::spawn(async move {
            send_agent_with_report_immediately(
                request_id_clone,
                invoked_function_arn_clone,
                agent_payloads,
                report_line,
                newrelic_client_clone,
                config_clone,
                apm_app_clone,
            )
            .await;
        }))
    } else {
        // WARM START + NO REPORT: Batch for later (5 min or count threshold)
        info!(
            "Standard mode: Warm start - batching agent payload (no platform.report yet)"
        );
        for payload_bytes in agent_payloads {
            add_to_batch(
                request_id.clone(),
                payload_bytes,
                None,
                invoked_function_arn.clone(),
            );
        }

        // Check if batch threshold reached
        if should_send_batch() {
            debug!("Batch threshold reached - sending batched payloads");
            let newrelic_client_clone = newrelic_client.clone();
            let config_clone = config.clone();

            Some(tokio::spawn(async move {
                send_batched_payloads(newrelic_client_clone, config_clone).await;
            }))
        } else {
            None
        }
    };

    // Flush logs, platform data, and retry failed payloads ALL IN PARALLEL
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

    // Log any errors
    if let Err(e) = log_result {
        error!("Failed to flush logs for request {}: {}", request_id, e);
    }
    if let Err(e) = platform_result {
        error!("Failed to flush platform for request {}: {}", request_id, e);
    }
    if let Err(e) = agent_result {
        error!("Agent send task failed for request {}: {}", request_id, e);
    }

    // Standard mode: Keep buffers alive on WARM STARTS to catch late agent payloads
    // Late payloads are processed at the START of the NEXT request (not via pending payload processing)
    // Cold starts: cleanup immediately since no late payloads expected
    cleanup_request_processing_state_conditional(&request_id, is_cold_start);

    // CRITICAL: Clear the active request ID now that processing is done
    if let Ok(mut active_request) = CURRENT_ACTIVE_REQUEST_ID.lock() {
        *active_request = None;
    }

    debug!(
        "Standard mode: Completed processing for request: {}",
        request_id
    );
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

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
        global_context.trace_id = None; // Reset trace ID for new request
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
    current_request_id: &str, // NEW: Exclude this request from processing
) {
    // Get all pending request buffers EXCEPT the current request
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
        .filter(|entry| entry.key() != current_request_id) // Exclude current request
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
        // Get the context for this request
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

        // Extract payloads from buffer
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

        // Now cleanup the old request completely
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
    // Extract trace ID only if enabled via config
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

    // Route to APM mode or standard mode
    let apm_app_guard = apm_app.read().await;
    if let Some(ref app) = *apm_app_guard {
        // APM mode: parse and send to APM collector
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
                // Buffer for retry
                buffer_failed_agent_payload(payload_bytes, request_id, invoked_function_arn);
                warn!(
                    "APM agent payload buffered for retry (size: {} bytes)",
                    payload_bytes.len()
                );
            }
        }
    } else {
        // Standard mode: send to serverless ingest API
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

                // Buffer the failed payload for retry
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

    // Take all failed payloads for retry (this clears the buffer)
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

        // Skip payloads that are too old (older than 24 hours)
        let age = chrono::Utc::now().signed_duration_since(failed_payload.failed_at);
        if age.num_hours() > 24 {
            warn!(
                "Dropping agent payload that's too old ({} hours) for request {}",
                age.num_hours(),
                failed_payload.request_id
            );
            continue;
        }

        // Skip payloads that have been retried too many times
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

                // Put it back in the buffer for next invocation if not too many retries
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
            age.num_hours() <= 24 // Keep payloads newer than 24 hours
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

/// Start batch timeout background task (checks every 30 seconds for 5-minute timeout)
pub fn start_batch_timeout_task(
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;

            // Check if oldest payload > 5 minutes
            let should_send = {
                if let Ok(meta) = crate::agent::batch::BATCH_META.lock() {
                    if let Some(oldest) = meta.oldest_timestamp {
                        chrono::Utc::now() - oldest > chrono::Duration::seconds(300)
                    } else {
                        false
                    }
                } else {
                    false
                }
            }; // Lock is released here

            if should_send {
                info!("Batch timeout reached (5 minutes) - sending buffered payloads");
                send_batched_payloads(newrelic_client.clone(), config.clone()).await;
            }
        }
    });
}
