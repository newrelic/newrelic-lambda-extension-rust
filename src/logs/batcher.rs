
use tracing::{debug, error, info, warn};
use std::sync::Arc;

use crate::newrelic::{client::NewRelicClient, payload};
use crate::config::ExtensionConfig;
use crate::util::SafeMutexOps;

use super::processor::LogProcessor;
use super::retry::{
    estimate_log_size, should_retry_on_failure, FailedLogEntry,
    MAX_FAILED_LOGS,
};

impl LogProcessor {
    pub async fn send_and_clear_batch_simple(&self) -> std::io::Result<()> {
        // FIRST: Try to stamp any remaining pre-invoke logs before final flush
        // This catches logs that arrived early (before context was ready)
        debug!("Final flush: Attempting to process pre-invoke logs one last time");
        self.process_pre_invoke_logs();

        // Master check: if all log types are disabled, don't send anything
        if !self.config.extension.send_function_logs
            && !self.config.extension.send_extension_logs
            && !self.config.extension.send_platform_logs {
            debug!("All log types disabled - clearing batch without sending");
            if let Some(mut batch_guard) = self.log_batch.safe_lock() {
                batch_guard.clear();
            }
            return Ok(());
        }

        // Don't await pending auto-flush handles -- they complete in background.
        // If they fail, their logs are already buffered via failed_logs_buffer.
        // Use flush_on_shutdown() during SHUTDOWN to await pending handles.
        {
            let mut handles = self.pending_flush_handles.lock().unwrap_or_else(|e| e.into_inner());
            let count = handles.len();
            handles.clear(); // Drop handles -- spawned tasks continue running
            handles.shrink_to_fit();
            if count > 0 {
                debug!("Cleared {} pending auto-flush handles (tasks continue in background)", count);
            }
        }

        let batch = {
            if let Some(mut batch_guard) = self.log_batch.safe_lock() {
                let taken = std::mem::take(&mut *batch_guard);
                batch_guard.shrink_to_fit();
                taken
            } else {
                warn!("Failed to acquire log_batch lock for final flush");
                return Ok(());
            }
        };

        if batch.is_empty() {
            debug!("No logs in batch to send");
            return Ok(());
        }

        let deduplicated_batch = {
            use std::collections::HashMap;
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            let mut seen = HashMap::with_capacity(batch.len());
            let mut unique_logs = Vec::with_capacity(batch.len());
            let mut duplicate_count = 0;

            for log in batch {
                let mut hasher = DefaultHasher::new();
                log.message.hash(&mut hasher);
                log.timestamp.hash(&mut hasher);

                // Check nested AWS structure for request_id
                if let Some(aws_value) = log.attributes.get("aws") {
                    if let Some(aws_obj) = aws_value.as_object() {
                        if let Some(request_id_value) = aws_obj.get("lambda_request_id") {
                            if let Some(request_id_str) = request_id_value.as_str() {
                                request_id_str.hash(&mut hasher);
                            }
                        }
                    }
                }

                let log_hash = hasher.finish();

                if seen.insert(log_hash, log.timestamp).is_none() {
                    unique_logs.push(log);
                } else {
                    duplicate_count += 1;
                }
            }

            if duplicate_count > 0 {
                debug!("Deduplicated {} duplicate log(s) before sending", duplicate_count);
            }

            unique_logs
        };

        if deduplicated_batch.is_empty() {
            debug!("All logs were duplicates, nothing to send");
            return Ok(());
        }

        debug!("Final flush: sending {} logs to New Relic", deduplicated_batch.len());

        let client = Arc::clone(&self.newrelic_client);
        let config = Arc::clone(&self.config);
        let context = if let Some(ctx) = self.invocation_context.safe_lock() {
            ctx.clone()
        } else {
            warn!("Failed to acquire invocation context for log flush - putting logs back");
            if let Some(mut batch_guard) = self.log_batch.safe_lock() {
                batch_guard.extend(deduplicated_batch);
            }
            return Ok(());
        };

        // GUARD: Never send logs without ARN - use fallback chain if context ARN is empty
        let effective_arn = if !context.invoked_function_arn.is_empty() {
            context.invoked_function_arn.clone()
        } else {
            let fallback = self.get_best_available_arn();
            if fallback.is_empty() {
                error!(
                    "BLOCKED: Refusing to flush {} logs without ARN (request_id: '{}'). \
                     Neither invocation context nor fallback ARN available.",
                    deduplicated_batch.len(),
                    context.request_id
                );
                // Put logs back in batch so they can be sent later when ARN is available
                if let Ok(mut batch) = self.log_batch.lock() {
                    batch.extend(deduplicated_batch);
                }
                return Ok(());
            }
            warn!(
                "Log flush: Using fallback ARN '{}' (invocation context ARN was empty, request_id: '{}')",
                fallback, context.request_id
            );
            fallback
        };

        const MAX_PAYLOAD_SIZE: usize = 1_000_000; // 1MB
        let mut chunks: Vec<Vec<payload::LogMessage>> = Vec::new();
        let mut current_chunk = Vec::new();
        let mut current_size = 0;

        for log in deduplicated_batch {
            let log_size = estimate_log_size(&log);

            if current_size + log_size > MAX_PAYLOAD_SIZE && !current_chunk.is_empty() {
                chunks.push(std::mem::take(&mut current_chunk));
                current_size = 0;
            }

            current_chunk.push(log);
            current_size += log_size;
        }

        if !current_chunk.is_empty() {
            chunks.push(current_chunk);
        }

        if chunks.len() > 1 {
            debug!("Chunking {} logs into {} size-based batches (max 1MB each)",
                  chunks.iter().map(|c| c.len()).sum::<usize>(), chunks.len());
        }

        let mut successful_chunks = 0;
        let mut failed_chunks = 0;

        for chunk in chunks {
            // send_chunk handles buffering failed logs internally for cross-invocation retry
            match self.send_chunk(&client, &config, chunk, &effective_arn).await {
                Ok(()) => {
                    successful_chunks += 1;
                },
                Err(_) => {
                    failed_chunks += 1;
                }
            }
        }

        if successful_chunks > 0 {
            info!("Successfully sent {} log chunks", successful_chunks);
        }
        if failed_chunks > 0 {
            warn!("{} log chunks failed (buffered for retry on next invocation)", failed_chunks);
        }

        Ok(())
    }

    /// Helper method to send logs with proper 1MB chunking
    /// Used by both auto-flush (25 logs) and end-of-request flush
    pub(crate) async fn send_logs_with_chunking(
        client: &Arc<NewRelicClient>,
        config: &Arc<ExtensionConfig>,
        logs: Vec<payload::LogMessage>,
        function_arn: &str,
    ) {
        if logs.is_empty() {
            return;
        }

        const MAX_PAYLOAD_SIZE: usize = 1_000_000; // 1MB
        let mut chunks: Vec<Vec<payload::LogMessage>> = Vec::new();
        let mut current_chunk = Vec::new();
        let mut current_size = 0;

        for log in logs {
            let log_size = estimate_log_size(&log);

            if current_size + log_size > MAX_PAYLOAD_SIZE && !current_chunk.is_empty() {
                chunks.push(std::mem::take(&mut current_chunk));
                current_size = 0;
            }

            current_chunk.push(log);
            current_size += log_size;
        }

        if !current_chunk.is_empty() {
            chunks.push(current_chunk);
        }

        if chunks.len() > 1 {
            debug!("Chunking logs into {} batches (max 1MB each)", chunks.len());
        }

        for chunk in chunks {
            if let Err(e) = client.send_logs(config, chunk, function_arn).await {
                error!("Failed to send log chunk: {}", e);
            }
        }
    }

    /// Send a log chunk to New Relic. client.send_logs() already retries 3 times
    /// with exponential backoff internally — no caller-side retry needed.
    /// On failure, buffers retriable logs for cross-invocation retry on next warm start.
    async fn send_chunk(
        &self,
        client: &NewRelicClient,
        config: &ExtensionConfig,
        chunk: Vec<payload::LogMessage>,
        function_arn: &str,
    ) -> std::io::Result<()> {
        self.send_chunk_internal(client, config, chunk, function_arn, true).await
    }

    pub(crate) async fn send_chunk_internal(
        &self,
        client: &NewRelicClient,
        config: &ExtensionConfig,
        chunk: Vec<payload::LogMessage>,
        function_arn: &str,
        use_failed_buffer: bool,
    ) -> std::io::Result<()> {
        // client.send_logs() internally retries 3 times with 200/400/900ms backoff.
        // No caller-side retry loop — if all 3 internal retries fail, buffer for
        // cross-invocation retry on next warm start instead of blocking this invocation.
        //
        // Clone once for the failed buffer path (send_logs takes ownership).
        // Old code cloned on every retry attempt (up to 3x); this clones at most once.
        let backup = if use_failed_buffer { Some(chunk.clone()) } else { None };

        match client.send_logs(config, chunk, function_arn).await {
            Ok(()) => Ok(()),
            Err(e) => {
                warn!("Log send failed after client retries: {}", e);

                if let Some(failed_chunk) = backup {
                    let request_id = if let Some(ctx) = self.invocation_context.safe_lock() {
                        ctx.request_id.clone()
                    } else {
                        String::from("unknown")
                    };
                    if let Some(mut failed_buffer) = self.failed_logs_buffer.safe_lock() {
                        let mut buffered = 0;
                        let mut dropped = 0;
                        for log in failed_chunk {
                            if should_retry_on_failure(&log) {
                                if failed_buffer.len() >= MAX_FAILED_LOGS {
                                    warn!("Failed logs buffer at capacity ({}) - dropping oldest entry", MAX_FAILED_LOGS);
                                    failed_buffer.pop_front();
                                }
                                failed_buffer.push_back(FailedLogEntry {
                                    log_message: log,
                                    original_request_id: request_id.clone(),
                                    retry_count: 0,
                                });
                                buffered += 1;
                            } else {
                                dropped += 1;
                            }
                        }
                        if buffered > 0 {
                            warn!("Buffering {} retriable logs for cross-invocation retry", buffered);
                        }
                        if dropped > 0 {
                            debug!("Dropped {} non-retriable extension/platform logs", dropped);
                        }
                    } else {
                        error!("Failed to buffer logs - mutex poisoned");
                    }
                } else {
                    error!("Buffered log retry failed - dropping logs");
                }
                Err(std::io::Error::new(std::io::ErrorKind::Other, e))
            }
        }
    }

    /// Shutdown-only flush: awaits all pending auto-flush tasks, then does final flush.
    /// Use this during SHUTDOWN to ensure all in-flight data is sent before Lambda kills the process.
    /// During normal invocations, use `flush()` which skips the pending handle wait.
    pub async fn flush_on_shutdown(&self) -> std::io::Result<()> {
        let pending_handles = {
            let mut handles = self.pending_flush_handles.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *handles)
        };
        if !pending_handles.is_empty() {
            debug!("Shutdown: Waiting for {} pending auto-flush tasks", pending_handles.len());
            for handle in pending_handles {
                let _ = handle.await;
            }
            debug!("Shutdown: All pending auto-flush tasks completed");
        }
        self.send_and_clear_batch_simple().await
    }
}
