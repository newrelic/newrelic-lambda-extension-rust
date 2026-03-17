
use tracing::{debug, error, info, warn};
use std::sync::Arc;

use crate::util::SafeMutexOps;

use super::processor::LogProcessor;
use super::retry::MAX_RETRIES;

impl LogProcessor {
    /// Transfer logs from pre_invoke_buffer to log_batch with ARN/request_id added
    /// Only processes logs if invocation context is valid. Invalid context leaves logs in buffer.
    pub fn process_pre_invoke_logs(&self) {
        let context_valid = {
            if let Some(context) = self.invocation_context.safe_lock() {
                !context.invoked_function_arn.is_empty()
                    && !context.request_id.is_empty()
                    && context.request_id != "unknown"
            } else {
                false
            }
        };

        if !context_valid {
            if let Some(buf) = self.pre_invoke_buffer.safe_lock() {
                let buf_size = buf.len();
                if buf_size > 0 {
                    debug!("Skipping pre-invoke log processing - context not ready yet ({} logs waiting)", buf_size);
                }
            }
            return;
        }

        let mut pre_invoke_logs = {
            if let Some(mut buf) = self.pre_invoke_buffer.safe_lock() {
                std::mem::take(&mut *buf)
            } else {
                return;
            }
        };

        if pre_invoke_logs.is_empty() {
            return;
        }

        debug!("Processing {} pre-invoke logs with invocation metadata", pre_invoke_logs.len());

        // At this point, context is guaranteed valid - stamp all logs
        for log in &mut pre_invoke_logs {
            if let Some(context) = self.invocation_context.safe_lock() {
                log.attributes.insert("faas.arn".to_string(),
                    serde_json::Value::String(context.invoked_function_arn.clone()));

                // Stamp request_id in New Relic expected format
                let mut aws_attrs = serde_json::Map::new();
                aws_attrs.insert("lambda_request_id".to_string(),
                    serde_json::Value::String(context.request_id.clone()));
                log.attributes.insert("aws".to_string(),
                    serde_json::Value::Object(aws_attrs));
                log.attributes.insert("faas.execution".to_string(),
                    serde_json::Value::String(context.request_id.clone()));

                // Stamp trace_id if available
                if let Some(ref trace_id) = context.trace_id {
                    log.attributes.insert("trace.id".to_string(),
                        serde_json::Value::String(trace_id.clone()));
                }
            }

            // Stamp entity.guid if APM app available
            if let Some(ref apm_app_arc) = self.apm_app {
                if let Ok(apm_guard) = apm_app_arc.try_read() {
                    if let Some(ref app) = *apm_guard {
                        let entity_guid = app.get_entity_guid();
                        if !entity_guid.is_empty() {
                            log.attributes.insert("entity.guid".to_string(),
                                serde_json::Value::String(entity_guid.to_string()));
                        }
                    }
                }
            }
        }

        // All logs are now complete - move to batch for sending
        if let Ok(mut batch) = self.log_batch.lock() {
            batch.extend(pre_invoke_logs);
        }
    }

    /// Send pre-invoke logs on shutdown with last request ID (or force flush with marker in error cases)
    /// Normal case: Use last request ID from previous invocation
    /// Error case (crash before first invoke): Send with nr.forceFlushed=true marker
    pub async fn flush_pre_invoke_buffer_on_shutdown(&self) -> std::io::Result<()> {
        let mut pre_invoke_logs = {
            if let Some(mut buf) = self.pre_invoke_buffer.safe_lock() {
                std::mem::take(&mut *buf)
            } else {
                warn!("Failed to acquire pre_invoke_buffer lock on shutdown");
                return Ok(());
            }
        };

        if pre_invoke_logs.is_empty() {
            debug!("No pre-invoke logs to flush on shutdown");
            return Ok(());
        }

        // Try to get last request ID from previous invocations
        let last_context = if let Ok(guard) = crate::event_loop::LAST_REQUEST_CONTEXT.lock() {
            guard.as_ref().cloned()
        } else {
            None
        };

        let function_arn = if let Some(context) = self.invocation_context.safe_lock() {
            if !context.invoked_function_arn.is_empty() {
                context.invoked_function_arn.clone()
            } else {
                self.fallback_function_arn.lock()
                    .ok()
                    .and_then(|guard| guard.as_ref().cloned())
                    .unwrap_or_else(String::new)
            }
        } else {
            String::new()
        };

        match last_context {
            Some((request_id, arn)) => {
                info!("Shutdown: Sending {} pre-invoke logs with last request ID: {}", pre_invoke_logs.len(), request_id);

                let use_arn = if !arn.is_empty() { arn } else { function_arn };

                for log in &mut pre_invoke_logs {
                    if !use_arn.is_empty() {
                        log.attributes.insert("faas.arn".to_string(),
                            serde_json::Value::String(use_arn.clone()));
                    }
                    // Create nested AWS structure: {"aws": {"lambda_request_id": "..."}}
                    let mut aws_attrs = serde_json::Map::new();
                    aws_attrs.insert("lambda_request_id".to_string(),
                        serde_json::Value::String(request_id.clone()));
                    log.attributes.insert("aws".to_string(),
                        serde_json::Value::Object(aws_attrs));
                    log.attributes.insert("faas.execution".to_string(),
                        serde_json::Value::String(request_id.clone()));

                    // Add entity.guid if in APM mode
                    if let Some(ref apm_app_arc) = self.apm_app {
                        if let Ok(apm_guard) = apm_app_arc.try_read() {
                            if let Some(ref app) = *apm_guard {
                                let entity_guid = app.get_entity_guid();
                                if !entity_guid.is_empty() {
                                    log.attributes.insert("entity.guid".to_string(),
                                        serde_json::Value::String(entity_guid.to_string()));
                                }
                            }
                        }
                    }
                }

                let client = Arc::clone(&self.newrelic_client);
                let config = Arc::clone(&self.config);

                Self::send_logs_with_chunking(&client, &config, pre_invoke_logs, &use_arn).await;
            }
            None => {
                // Error case: Crash/shutdown before first invoke - force flush with marker
                warn!("Shutdown before first invoke (error/crash) - force flushing {} pre-invoke logs with nr.forceFlushed marker", pre_invoke_logs.len());

                if function_arn.is_empty() {
                    error!("Cannot flush pre-invoke logs: no ARN available (neither from INVOKE nor registration)");
                    return Ok(());
                }

                for log in &mut pre_invoke_logs {
                    log.attributes.insert("faas.arn".to_string(),
                        serde_json::Value::String(function_arn.clone()));
                    // Create nested AWS structure: {"aws": {"lambda_request_id": "..."}}
                    let mut aws_attrs = serde_json::Map::new();
                    aws_attrs.insert("lambda_request_id".to_string(),
                        serde_json::Value::String("INIT_PHASE_LOGS".to_string()));
                    log.attributes.insert("aws".to_string(),
                        serde_json::Value::Object(aws_attrs));
                    log.attributes.insert("nr.forceFlushed".to_string(),
                        serde_json::Value::Bool(true));

                    // Add entity.guid if in APM mode
                    if let Some(ref apm_app_arc) = self.apm_app {
                        if let Ok(apm_guard) = apm_app_arc.try_read() {
                            if let Some(ref app) = *apm_guard {
                                let entity_guid = app.get_entity_guid();
                                if !entity_guid.is_empty() {
                                    log.attributes.insert("entity.guid".to_string(),
                                        serde_json::Value::String(entity_guid.to_string()));
                                }
                            }
                        }
                    }
                }

                let client = Arc::clone(&self.newrelic_client);
                let config = Arc::clone(&self.config);

                Self::send_logs_with_chunking(&client, &config, pre_invoke_logs, &function_arn).await;
            }
        }

        Ok(())
    }

    /// Process buffered logs that were waiting for a valid request_id
    /// Also retries failed logs from previous invocations on warm starts
    pub fn process_buffered_logs_with_request_id(&self, request_id: &str) {
        let is_warm_start = crate::IS_WARM_START.load(std::sync::atomic::Ordering::Relaxed);

        if is_warm_start {
            let failed_logs = {
                if let Some(mut buffer) = self.failed_logs_buffer.safe_lock() {
                    std::mem::take(&mut *buffer)
                } else {
                    std::collections::VecDeque::new()
                }
            };

            if !failed_logs.is_empty() {
                debug!("Retrying {} failed logs from previous invocation", failed_logs.len());

                let client = Arc::clone(&self.newrelic_client);
                let config = Arc::clone(&self.config);
                let failed_buffer = Arc::clone(&self.failed_logs_buffer);

                tokio::spawn(async move {
                    let mut still_failed = Vec::new();

                    for mut entry in failed_logs {
                        entry.retry_count += 1;

                        if entry.retry_count > MAX_RETRIES {
                            warn!("Dropping log after {} retries (original request: {})",
                                  entry.retry_count, entry.original_request_id);
                            continue;
                        }

                        let logs_to_send = vec![entry.log_message.clone()];
                        match client.send_logs(&config, logs_to_send, "retry").await {
                            Ok(()) => {
                                debug!("Successfully retried failed log");
                            }
                            Err(e) => {
                                debug!("Failed log retry failed again: {}", e);
                                still_failed.push(entry);
                            }
                        }
                    }

                    if !still_failed.is_empty() {
                        if let Some(mut buffer) = failed_buffer.safe_lock() {
                            buffer.extend(still_failed);
                            debug!("Re-buffered {} logs that failed retry", buffer.len());
                        } else {
                            warn!("Failed to re-buffer {} logs - mutex poisoned", still_failed.len());
                        }
                    }
                });
            }
        }

        let buffered_logs = {
            if let Some(mut buffer) = self.request_id_buffer.safe_lock() {
                std::mem::take(&mut *buffer)
            } else {
                return;
            }
        };

        if !buffered_logs.is_empty() {
            debug!("Processing {} buffered logs with new request_id: {}", buffered_logs.len(), request_id);

            for mut log_message in buffered_logs {
                // New Relic expects nested structure: {"aws": {"lambda_request_id": "..."}}
                let mut aws_attrs = serde_json::Map::new();
                aws_attrs.insert("lambda_request_id".to_string(),
                    serde_json::Value::String(request_id.to_string()));
                log_message.attributes.insert("aws".to_string(),
                    serde_json::Value::Object(aws_attrs));
                log_message.attributes.insert("faas.execution".to_string(),
                                serde_json::Value::String(request_id.to_string()));

                if let (Some(ref extraction_state), Some(ref buffered_logs_arc)) =
                    (&self.trace_extraction_state, &self.buffered_logs) {

                    if let Some(state) = extraction_state.safe_lock() {
                        let has_trace_id = {
                            if let Some(context) = self.invocation_context.safe_lock() {
                                context.trace_id.is_some()
                            } else {
                                false
                            }
                        };

                        if *state == super::processor::TraceIdExtractionState::Waiting && !has_trace_id {
                            drop(state);
                            if let Some(mut buffered) = buffered_logs_arc.safe_lock() {
                                buffered.push(log_message);
                            }
                            continue;
                        }
                    }
                }

                if let Some(mut batch) = self.log_batch.safe_lock() {
                    batch.push(log_message);
                }
            }
        }
    }
}
