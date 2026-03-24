use std::sync::{Arc, Mutex};
use tracing::{debug, error, info, warn};

use crate::{
    config::ExtensionConfig,
    newrelic::client::NewRelicClient,
    newrelic::flush::Flush,
    logs::processor::LogProcessor,
    request::{
        cleanup_request_processing_state_internal, get_agent_buffer, get_pending_report,
        get_request_context, remove_pending_report, REQUEST_DATA,
        REQUEST_PROCESSORS,
    },
    agent::batch::{
        add_to_batch, should_send_batch_by_threshold,
        send_batched_payloads_with_reports_only,
    },
    agent::payload::send_agent_payload_to_newrelic,
    trace,
};

use super::extract_and_coordinate_trace_id;

/// Failed agent payload for retry across invocations
#[derive(Debug, Clone)]
pub struct FailedAgentPayload {
    pub payload_bytes: Vec<u8>,
    pub request_id: String,
    pub invoked_function_arn: String,
    pub retry_count: usize,
    pub failed_at: chrono::DateTime<chrono::Utc>,
}

pub static FAILED_AGENT_PAYLOADS: once_cell::sync::Lazy<Arc<Mutex<std::collections::VecDeque<FailedAgentPayload>>>> =
    once_cell::sync::Lazy::new(|| Arc::new(Mutex::new(std::collections::VecDeque::new())));

/// Track last processed request for error synthesis on shutdown
pub static LAST_REQUEST_CONTEXT: once_cell::sync::Lazy<Arc<Mutex<Option<(String, String)>>>> =
    once_cell::sync::Lazy::new(|| Arc::new(Mutex::new(None)));

/// Send agent payload to APM collector (parses and sends 5 telemetry types)
pub async fn send_to_apm_collector(
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
        debug!(
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

/// Process and send agent payload following our simple flow
pub async fn process_and_send_agent_payload(
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
                debug!("APM mode: Agent payload sent successfully for request: {}", request_id);
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

/// Process any pending agent payloads from previous invocation (APM mode only)
/// Excludes the current request ID to avoid processing empty buffer
pub async fn process_pending_agent_payloads(
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
                    debug!("APM mode: Successfully sent platform report metrics for previous request {}", request_id);
                }
            }
            drop(apm_app_guard);

            // Remove pending report after sending
            remove_pending_report(&request_id);
        }

        cleanup_request_processing_state_internal(&request_id, false);
    }
}

/// Process a standard mode request concurrently
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

    // CURRENT_ACTIVE_REQUEST_ID already set by caller (event loop) -- no duplicate lock needed

    let state = REQUEST_PROCESSORS.remove(&request_id).map(|(_, v)| v);

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

    // Warm start optimization: no waiting for agent payloads.
    // Take whatever is in the buffer immediately. Late payloads will be
    // caught and sent in the next invocation via the batching logic.
    let agent_payloads = {
        if let Ok(mut buffer) = state.agent_buffer.lock() {
            std::mem::take(&mut *buffer)
        } else {
            Vec::new()
        }
    };

    if agent_payloads.is_empty() {
        debug!(
            "Standard mode: No agent payload in buffer for request: {} - will catch in next invocation",
            request_id
        );
    } else {
        debug!(
            "Standard mode: Found {} agent payload(s) in buffer for request: {}",
            agent_payloads.len(), request_id
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

    // Fire-and-forget: spawn log flush in background instead of blocking on tokio::join!
    // Spawned tasks execute while /next blocks waiting for the next event.
    // Failed logs go to failed_logs_buffer for retry on next invocation.
    {
        let log_processor = global_log_processor.clone();
        let rid = request_id.clone();
        tokio::spawn(async move {
            if let Err(e) = log_processor.flush().await {
                error!("Background log flush failed for request {}: {}", rid, e);
            }
        });
    }

    // Platform flush is a no-op (returns Ok immediately) -- skip it entirely

    // Agent retry runs in background -- failures stay in FAILED_AGENT_PAYLOADS for next invocation
    {
        let nr_client = newrelic_client.clone();
        let cfg = config.clone();
        tokio::spawn(async move {
            retry_failed_agent_payloads(&nr_client, &cfg).await;
        });
    }

    // Agent batch send is already a spawned task -- let it complete in background
    if let Some(handle) = send_agent_task {
        drop(handle); // Tasks continue running; failures keep items in AGENT_BATCH_BUFFER
    }

    // Always keep REQUEST_DATA alive for late payload routing after Lambda freeze/thaw.
    // Late agent payloads and platform.report may arrive after processing completes.
    // Periodic cleanup_old_request_buffers (every 5 invocations) prevents memory growth.
    cleanup_request_processing_state_internal(&request_id, true);

    // Keep active request set for late payload routing (agent payloads may arrive after processing)
    // It will be overwritten when next INVOKE arrives
    if let Ok(mut active_request) = crate::request::CURRENT_ACTIVE_REQUEST_ID.lock() {
        *active_request = Some(request_id.clone());
    }

    debug!(
        "Standard mode: Completed processing for request: {}",
        request_id
    );
}

/// Process an APM request (called from apm event loop)
pub async fn process_apm_request(
    request_id: String,
    invoked_function_arn: String,
    is_cold_start: bool,
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
    apm_app: crate::apm::SharedApmApp,
) {
    debug!("APM mode: Starting processing for request: {}", request_id);

    // CURRENT_ACTIVE_REQUEST_ID already set in execute_apm_mode_event_loop

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
                        debug!("APM mode: Agent payload sent successfully");
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
                debug!("APM mode: Successfully sent platform report metrics for request {}", request_id);
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

    // Fire-and-forget: spawn log flush in background instead of blocking
    // Platform flush is a no-op -- skip it entirely
    {
        let log_processor = global_log_processor.clone();
        let rid = request_id.clone();
        tokio::spawn(async move {
            if let Err(e) = log_processor.flush().await {
                error!("APM mode: Background log flush failed for request {}: {}", rid, e);
            }
        });
    }

    // Note: We do NOT wait for runtime.done here because platform.runtimeDone event
    // arrives during the NEXT invocation in APM mode, not the current one.
    // Agent payloads that arrive late will be caught by warm start logic.

    // Always keep REQUEST_DATA alive for late payload routing after Lambda freeze/thaw.
    // Periodic cleanup_old_request_buffers (every 5 invocations) prevents memory growth.
    cleanup_request_processing_state_internal(&request_id, true);

    // Keep active request set for late payload routing (agent payloads may arrive after processing)
    // It will be overwritten when next INVOKE arrives
    if let Ok(mut active_request) = crate::request::CURRENT_ACTIVE_REQUEST_ID.lock() {
        *active_request = Some(request_id.clone());
    }

    debug!(
        "APM mode: Completed processing for request: {}",
        request_id
    );
}

/// Buffer failed agent payload for retry across invocations.
/// Count-capped at 10 entries. No byte-size limit — payloads can be 1MB+ each.
pub fn buffer_failed_agent_payload(
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
        const MAX_FAILED_PAYLOADS: usize = 10;
        if failed_payloads.len() >= MAX_FAILED_PAYLOADS {
            warn!("FAILED_AGENT_PAYLOADS at capacity ({}) - dropping oldest entry", MAX_FAILED_PAYLOADS);
            failed_payloads.pop_front();
        }
        failed_payloads.push_back(failed_payload);
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
pub async fn retry_failed_agent_payloads(
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<ExtensionConfig>,
) {
    let mut retry_successful_count = 0;
    let mut retry_failed_count = 0;

    let failed_payloads = {
        if let Ok(mut failed_payloads) = FAILED_AGENT_PAYLOADS.lock() {
            let taken = std::mem::take(&mut *failed_payloads);
            failed_payloads.shrink_to_fit();
            taken
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

        // Backoff based on retry count before re-attempting
        let backoff = match failed_payload.retry_count {
            1 => std::time::Duration::from_millis(200),
            2 => std::time::Duration::from_millis(400),
            _ => std::time::Duration::from_millis(900),
        };
        tokio::time::sleep(backoff).await;

        debug!(
            "Retrying agent payload for request {} (attempt {}, backoff {}ms)",
            failed_payload.request_id, failed_payload.retry_count, backoff.as_millis()
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
                        const MAX_FAILED_PAYLOADS: usize = 10;
                        if failed_payloads.len() < MAX_FAILED_PAYLOADS {
                            failed_payloads.push_back(failed_payload);
                        } else {
                            warn!("FAILED_AGENT_PAYLOADS at capacity during retry - dropping payload");
                        }
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

/// Clean up old failed payloads that have exceeded max retry count.
/// Hard cap on buffer size is enforced at insertion time (buffer_failed_agent_payload).
pub fn cleanup_old_failed_payloads() {
    if let Ok(mut failed_payloads) = FAILED_AGENT_PAYLOADS.lock() {
        let initial_count = failed_payloads.len();

        // Remove entries that have exceeded retry limit (5 retries)
        failed_payloads.retain(|payload| payload.retry_count <= 5);
        failed_payloads.shrink_to_fit();

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
