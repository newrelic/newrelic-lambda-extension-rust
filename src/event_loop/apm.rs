use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::{
    runtime,
    request::{
        self,
        create_request_processing_state,
        get_agent_buffer, get_request_context,
        remove_pending_report, request_data_len,
        REQUEST_PROCESSORS, REQUEST_DATA,
    },
    error_synthesis,
    IS_WARM_START,
};

use super::{
    ExtensionComponents, tag_lambda_function_once, update_global_invocation_context,
};
use super::payload::{
    LAST_REQUEST_CONTEXT, process_pending_agent_payloads,
    process_and_send_agent_payload, process_apm_request, cleanup_old_failed_payloads,
};

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
                }

                if let Ok(mut guard) = LAST_REQUEST_CONTEXT.lock() {
                    *guard = Some((request_id.clone(), invoked_function_arn.clone()));
                }

                error_synthesis::clear_sent_errors_for_request(&request_id);

                {
                    let nr_client = components.newrelic_client.clone();
                    let cfg = components.config.clone();
                    tokio::spawn(async move {
                        error_synthesis::retry_failed_errors(&nr_client, &cfg).await;
                    });
                }
                {
                    let nr_client = components.newrelic_client.clone();
                    let license_key = components.config.new_relic.license_key.clone().unwrap_or_default();
                    tokio::spawn(async move {
                        crate::apm::telemetry_buffer::retry_buffered_telemetry(
                            nr_client.outbound_client(),
                            &license_key,
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

                // Create per-request state (platform_processor, agent_buffer, context)
                let request_state = create_request_processing_state(
                    &request_id,
                    &invoked_function_arn,
                    &components.processor_factory,
                    &components.global_log_processor,
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

                // APM mode: await both tasks before returning to /next.
                // Unlike standard mode (which batches), APM sends directly to collector.
                // Lambda freezes the environment after /next returns, so background tasks
                // may not complete. We must await them here to ensure delivery.
                let pending_task = tokio::spawn({
                    let config = components.config.clone();
                    let log_proc = components.global_log_processor.clone();
                    let apm_app = components.apm_app.clone();
                    let req_id = request_id.clone();
                    async move {
                        process_pending_agent_payloads(
                            &config,
                            &log_proc,
                            &apm_app,
                            &req_id,
                        )
                        .await;
                    }
                });

                let current_task = tokio::spawn(process_apm_request(
                    request_id.clone(),
                    invoked_function_arn.clone(),
                    is_cold_start,
                    components.config.clone(),
                    components.global_log_processor.clone(),
                    components.apm_app.clone(),
                ));

                let (current_result, pending_result) = tokio::join!(current_task, pending_task);
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
                if cleanup_counter >= 5 {
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

                // CRITICAL: Process ALL remaining pending agent payloads before shutdown
                debug!("APM mode shutdown: Processing all remaining agent payloads");
                let mut agent_payloads_sent: usize = 0;
                let mut platform_reports_sent: usize = 0;

                // Drain orphaned payloads into last request's buffer — release lock first
                let orphaned_payloads: Vec<Vec<u8>> = crate::request::ORPHANED_PAYLOADS
                    .lock()
                    .ok()
                    .map(|mut orphaned| orphaned.drain(..).collect())
                    .unwrap_or_default();

                if !orphaned_payloads.is_empty() {
                    let last_rid = LAST_REQUEST_CONTEXT
                        .lock()
                        .ok()
                        .and_then(|g| g.as_ref().map(|(id, _)| id.clone()));

                    // Try to route into last request's buffer (drop lock before any .await)
                    let remaining = if let Some(ref rid) = last_rid {
                        if let Some(buffer) = get_agent_buffer(rid) {
                            if let Ok(mut buf) = buffer.lock() {
                                debug!("APM mode shutdown: Drained {} orphaned payload(s) into buffer: {}", orphaned_payloads.len(), rid);
                                buf.extend(orphaned_payloads);
                                Vec::new() // all routed
                            } else {
                                orphaned_payloads // lock poisoned
                            }
                        } else {
                            orphaned_payloads // no buffer
                        }
                    } else {
                        orphaned_payloads // no last request
                    };

                    if !remaining.is_empty() {
                        // Fallback: send directly to APM collector
                        let arn = last_rid.as_ref()
                            .and_then(|rid| get_request_context(rid))
                            .and_then(|ctx| ctx.lock().ok().map(|c| c.invoked_function_arn.clone()).filter(|a| !a.is_empty()))
                            .unwrap_or_else(crate::get_global_fallback_arn);
                        let rid = last_rid.as_deref().unwrap_or("init-orphaned");
                        warn!("APM mode shutdown: Buffer unavailable — sending {} orphans directly for {}", remaining.len(), rid);
                        for payload_bytes in &remaining {
                            if let Err(e) = process_and_send_agent_payload(payload_bytes, rid, &arn, &components.global_log_processor, &components.config, &components.apm_app).await {
                                warn!("APM mode shutdown: Failed to send orphaned payload: {}", e);
                            } else {
                                agent_payloads_sent += 1;
                            }
                        }
                    }
                }

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
                                debug!("APM mode shutdown: Sending {} unsent payload(s) for request: {}", payloads.len(), request_id);

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
                                        agent_payloads_sent += 1;
                                        debug!("APM mode shutdown: Successfully sent payload for request: {}", request_id);
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
                    components.newrelic_client.outbound_client(),
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
                                platform_reports_sent += 1;
                                debug!("APM mode shutdown: Successfully sent platform report metrics for request: {}", request_id);
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

                // Final flush of logs (awaits pending auto-flush tasks first)
                if let Err(e) = components.global_log_processor.flush_on_shutdown().await {
                    error!("APM mode shutdown: Failed to flush logs: {}", e);
                }

                let duration_ms = shutdown_start_time.elapsed().as_millis();
                let summary = format!(
                    "APM mode shutdown: {} agent payload(s), {} platform report(s) sent in {}ms",
                    agent_payloads_sent, platform_reports_sent, duration_ms
                );
                // Always print summary — even when extension logs are disabled
                if components.config.extension.extension_logs_enabled {
                    info!("{}", summary);
                } else {
                    eprintln!("[NR_EXT] {}", summary);
                }
                break;
            }
        }
    }

    event_counter
}
