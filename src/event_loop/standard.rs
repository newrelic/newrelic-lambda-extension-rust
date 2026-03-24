use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::{
    runtime,
    request::{
        self,
        create_request_processing_state,
        REQUEST_PROCESSORS,
    },
    agent::batch::{
        should_send_batch_by_threshold,
        send_batched_payloads_with_reports_only,
        send_all_pending_payloads_on_shutdown,
    },
    error_synthesis,
    IS_WARM_START,
};

use super::{
    ExtensionComponents, tag_lambda_function_once, update_global_invocation_context,
};
use super::payload::{
    LAST_REQUEST_CONTEXT, process_request_concurrently,
};

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

                        let _ = components.global_log_processor.flush_on_shutdown().await;

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

                // Fire retries as background tasks -- don't block current invocation processing.
                // These are almost always no-ops (empty buffers) but can make HTTP calls on failures.
                {
                    let nr_client = components.newrelic_client.clone();
                    let config = components.config.clone();
                    tokio::spawn(async move {
                        error_synthesis::retry_failed_errors(&nr_client, &config).await;
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
                {
                    let nr_client = components.newrelic_client.clone();
                    let config = components.config.clone();
                    tokio::spawn(async move {
                        super::payload::retry_failed_agent_payloads(&nr_client, &config).await;
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

                // SKIP old buffer processing to avoid deadlocks
                // Late payloads are already handled via the buffer matching on next invocation
                // The complex locking in this loop was causing 7-second deadlocks

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

                // Process buffered logs and pre-invoke logs using global log processor
                components
                    .global_log_processor
                    .process_buffered_logs_with_request_id(&request_id);
                components
                    .global_log_processor
                    .process_pre_invoke_logs();

                // Send batch in background if threshold reached -- items stay in buffer until successful send
                if should_send_batch_by_threshold() {
                    debug!("Batch threshold reached - sending payloads in background");
                    let nr_client = components.newrelic_client.clone();
                    let cfg = components.config.clone();
                    tokio::spawn(async move {
                        send_batched_payloads_with_reports_only(nr_client, cfg).await;
                    });
                }

                REQUEST_PROCESSORS.insert(request_id.clone(), request_state);

                // Call directly instead of tokio::spawn + await (avoids task allocation overhead)
                process_request_concurrently(
                    request_id.clone(),
                    invoked_function_arn.clone(),
                    components.newrelic_client.clone(),
                    components.config.clone(),
                    components.global_log_processor.clone(),
                )
                .await;

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

                // Flush remaining buffered logs (awaits pending auto-flush tasks first)
                if let Err(e) = components.global_log_processor.flush_on_shutdown().await {
                    error!("Standard mode shutdown: Failed to flush logs: {}", e);
                }

                info!("Standard mode shutdown: All data processed and sent in {}ms", shutdown_start_time.elapsed().as_millis());
                break;
            }
        }
    }

    event_counter
}
