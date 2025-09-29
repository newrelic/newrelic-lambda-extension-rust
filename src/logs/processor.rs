use tracing::{debug, error, info, trace, warn};
use crate::{
    config::ExtensionConfig,
    context::InvocationContext,
    newrelic::{client::NewRelicClient, flush::Flush, payload},
    telemetry::listener::TelemetryRecord,
};
use async_trait::async_trait;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

/// State of trace ID extraction for the current invocation
#[derive(Debug, Clone, PartialEq)]
enum TraceIdExtractionState {
    /// Waiting to attempt trace ID extraction
    Waiting,
    /// Successfully extracted a trace ID
    Extracted,
    /// Attempted extraction but failed (error or no trace ID found)
    Failed,
}

/// The LogProcessor is responsible for handling and transforming function and extension logs.
#[derive(Debug, Clone)]
pub struct LogProcessor {
    log_batch: Arc<Mutex<Vec<payload::LogMessage>>>,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
    invocation_context: Arc<Mutex<InvocationContext>>,
    /// Buffered logs waiting for trace ID extraction (only allocated if collection is enabled)
    buffered_logs: Option<Arc<Mutex<Vec<payload::LogMessage>>>>,
    /// Current state of trace ID extraction (only allocated if collection is enabled)
    trace_extraction_state: Option<Arc<Mutex<TraceIdExtractionState>>>,
    /// Buffered logs waiting for request_id from NextEvent (always allocated)
    request_id_buffer: Arc<Mutex<Vec<payload::LogMessage>>>,
    /// Timestamp when current invocation started (for filtering late telemetry)
    invocation_start_time: Arc<Mutex<chrono::DateTime<chrono::Utc>>>,
    /// Failed logs buffer - logs that failed to send after all retries
    failed_logs_buffer: Arc<Mutex<Vec<payload::LogMessage>>>,
}

/// Configuration constants for batching and retry logic
const MAX_BATCH_SIZE: usize = 100; // Maximum logs per batch to avoid 413 errors
const MAX_RETRIES: usize = 3; // Maximum retry attempts for failed sends
const RETRY_DELAY_MS: u64 = 200; // Base retry delay in milliseconds

impl LogProcessor {
    /// Creates a new LogProcessor.
    pub fn new(
        newrelic_client: Arc<NewRelicClient>,
        config: Arc<ExtensionConfig>,
        invocation_context: Arc<Mutex<InvocationContext>>,
    ) -> Self {
        // Only allocate trace ID structures if collection is enabled
        let (buffered_logs, trace_extraction_state) = if config.new_relic.collect_trace_id {
            (
                Some(Arc::new(Mutex::new(Vec::new()))),
                Some(Arc::new(Mutex::new(TraceIdExtractionState::Waiting))),
            )
        } else {
            (None, None)
        };

        Self {
            log_batch: Arc::new(Mutex::new(Vec::new())),
            newrelic_client,
            config,
            invocation_context,
            buffered_logs,
            trace_extraction_state,
            request_id_buffer: Arc::new(Mutex::new(Vec::new())),
            invocation_start_time: Arc::new(Mutex::new(chrono::Utc::now())),
            failed_logs_buffer: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Updates the invocation start time (called when new invocation begins)
    pub fn set_invocation_start_time(&self, start_time: chrono::DateTime<chrono::Utc>) {
        *self.invocation_start_time.lock().unwrap() = start_time;
    }

    /// Retry sending all failed logs before starting a new invocation
    /// Sends them in chunks to handle large volumes efficiently
    pub async fn retry_failed_logs_before_invocation(&self) -> std::io::Result<()> {
        let failed_logs = {
            let mut failed_buffer = self.failed_logs_buffer.lock().unwrap();
            std::mem::take(&mut *failed_buffer)
        };

        if failed_logs.is_empty() {
            return Ok(());
        }

        info!("Retrying {} failed logs from previous invocations", failed_logs.len());

        // Log a message about retrying failed logs
        self.log_retry_attempt_message(failed_logs.len()).await;

        // Send failed logs in chunks of MAX_BATCH_SIZE
        let chunks: Vec<_> = failed_logs.chunks(MAX_BATCH_SIZE).map(|chunk| chunk.to_vec()).collect();
        let total_chunks = chunks.len();
        
        let mut successful_chunks = 0;
        let mut failed_chunks = 0;

        for (chunk_idx, chunk) in chunks.into_iter().enumerate() {
            info!("Retrying failed logs chunk {}/{} ({} logs)", chunk_idx + 1, total_chunks, chunk.len());
            
            let context = self.invocation_context.lock().unwrap().clone();
            match self.send_chunk_with_retry_internal(&self.newrelic_client, &self.config, chunk, &context.invoked_function_arn, false).await {
                Ok(()) => {
                    successful_chunks += 1;
                    info!("Successfully resent failed logs chunk {}/{}", chunk_idx + 1, total_chunks);
                }
                Err(e) => {
                    failed_chunks += 1;
                    error!("Failed to resend chunk {}/{}: {}", chunk_idx + 1, total_chunks, e);
                }
            }
        }

        info!("Failed logs retry complete: {} chunks successful, {} chunks failed", 
              successful_chunks, failed_chunks);
        
        Ok(())
    }

    /// Log a message about attempting to retry failed logs
    async fn log_retry_attempt_message(&self, failed_count: usize) {
        let context = self.invocation_context.lock().unwrap().clone();
        
        let retry_log_message = payload::LogMessage {
            timestamp: chrono::Utc::now().timestamp_millis(),
            message: format!("[NR_EXT] Retrying {} failed logs from previous invocations", failed_count),
            attributes: {
                let mut attrs = serde_json::Map::new();
                attrs.insert("aws.lambda_request_id".to_string(), context.request_id.clone().into());
                attrs.insert("faas.execution".to_string(), context.request_id.clone().into());
                attrs.insert("newrelic.logPattern".to_string(), "nr.DID_NOT_MATCH".into());
                attrs.insert("newrelic.source".to_string(), "api.logs".into());
                attrs.insert("log.level".to_string(), "INFO".into());
                attrs.insert("retry_operation".to_string(), true.into());
                attrs
            },
        };

        // Add to batch and try to send immediately
        {
            let mut batch = self.log_batch.lock().unwrap();
            batch.push(retry_log_message);
        }
    }

    /// Processes a single log telemetry record, adding it to the batch if valid.
    pub fn process_record(&self, record: TelemetryRecord) {
        let message_str = record.record.get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("Unknown log message");
        
        // Avoid recursive logging from our own processors
        if message_str.contains("[LogProcessor]") || message_str.contains("[PlatformProcessor]") {
            return;
        }

        // Filter out telemetry that's older than current invocation start time
        // This prevents late telemetry from previous invocations getting wrong request_id
        let is_current_invocation = {
            let context = self.invocation_context.lock().unwrap();
            let invocation_start = *self.invocation_start_time.lock().unwrap();
            if context.request_id == "unknown" {
                true // No active invocation yet, accept all logs for buffering
            } else {
                // Only accept logs that are from current invocation timeframe (no tolerance)
                record.time >= invocation_start
            }
        };

        if !is_current_invocation {
            debug!("Filtering out late telemetry from previous invocation: timestamp={}, message='{}'", 
                   record.time, message_str);
            return;
        }

        if let Some(mut log_message) = self.to_log_message(record) {
            // First check: Do we have a valid request_id?
            let has_request_id = {
                let context = self.invocation_context.lock().unwrap();
                !context.request_id.is_empty() && context.request_id != "unknown"
            };

            // If we don't have a request_id yet, buffer the log
            if !has_request_id {
                let mut request_buffer = self.request_id_buffer.lock().unwrap();
                request_buffer.push(log_message);
                return;
            }

            // We have a request_id - update the log message attributes with it
            {
                let context = self.invocation_context.lock().unwrap();
                log_message.attributes.insert("aws.lambda_request_id".to_string(), 
                                serde_json::Value::String(context.request_id.clone()));
            }

            // Check if trace ID collection is enabled and we should buffer for trace ID
            if let (Some(ref extraction_state), Some(ref buffered_logs)) = 
                (&self.trace_extraction_state, &self.buffered_logs) {
                
                let state = extraction_state.lock().unwrap();
                let has_trace_id = {
                    let context = self.invocation_context.lock().unwrap();
                    context.trace_id.is_some()
                };
                
                // Buffer logs only if we're still waiting for trace ID extraction attempt
                if *state == TraceIdExtractionState::Waiting && !has_trace_id {
                    drop(state); // Release lock before modifying buffer
                    let mut buffered = buffered_logs.lock().unwrap();
                    buffered.push(log_message);
                    return;
                }
            }
            
            // Normal processing - add to batch and potentially send
            let mut batch = self.log_batch.lock().unwrap();
            batch.push(log_message);
            let batch_size = batch.len();
            
            // Send immediately if we have 3+ logs (simple batch condition)
            if batch_size >= 3 {
                drop(batch); // Release the lock before async operation
                let processor = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = processor.send_and_clear_batch_simple().await {
                        error!("Failed to send logs: {}", e);
                    }
                });
            }
        } else {
            warn!("Failed to convert telemetry record to log message");
        }
    }

    /// Converts a TelemetryRecord into a LogMessage, if applicable.
    fn to_log_message(&self, record: TelemetryRecord) -> Option<payload::LogMessage> {
        let timestamp = record.time.timestamp_millis();
        let message = record.record.to_string();
        let mut attributes = serde_json::Map::new();
        
        // Get request_id and trace_id from the context
        let context = self.invocation_context.lock().unwrap();
        let request_id = &context.request_id;
        let trace_id = &context.trace_id;
        
        // Add AWS Lambda request ID
        attributes.insert("aws.lambda_request_id".to_string(), request_id.clone().into());
        
        // Add faas.execution (same as request_id)
        attributes.insert("faas.execution".to_string(), request_id.clone().into());
        
        // Only add trace ID if it's present (not None)
        if let Some(ref trace_id_value) = trace_id {
            attributes.insert("trace.id".to_string(), serde_json::Value::String(trace_id_value.clone()));
        }
        
        // Add newrelic.logPattern
        attributes.insert("newrelic.logPattern".to_string(), "nr.DID_NOT_MATCH".into());
        
        // Add newrelic.source
        attributes.insert("newrelic.source".to_string(), "api.logs".into());

        Some(payload::LogMessage {
            timestamp,
            message,
            attributes,
        })
    }

    /// Called when request_id becomes available - updates all buffered logs with request_id and processes them
    pub async fn on_request_id_available(&self, request_id: &str) -> std::io::Result<()> {
        // Get all buffered logs waiting for request_id
        let mut request_buffered_logs = {
            let mut buffered = self.request_id_buffer.lock().unwrap();
            std::mem::take(&mut *buffered)
        };
        
        if request_buffered_logs.is_empty() {
            return Ok(());
        }

        // Update all buffered logs with the request_id
        for log_message in &mut request_buffered_logs {
            log_message.attributes.insert("aws.lambda_request_id".to_string(), 
                            serde_json::Value::String(request_id.to_string()));
        }

        // Now process each log through the normal flow (which may buffer for trace ID if needed)
        for log_message in request_buffered_logs {
            // Check if trace ID collection is enabled and we should buffer for trace ID
            if let (Some(ref extraction_state), Some(ref buffered_logs)) = 
                (&self.trace_extraction_state, &self.buffered_logs) {
                
                let state = extraction_state.lock().unwrap();
                let has_trace_id = {
                    let context = self.invocation_context.lock().unwrap();
                    context.trace_id.is_some()
                };
                
                // Buffer logs only if we're still waiting for trace ID extraction attempt
                if *state == TraceIdExtractionState::Waiting && !has_trace_id {
                    drop(state); // Release lock before modifying buffer
                    let mut buffered = buffered_logs.lock().unwrap();
                    buffered.push(log_message);
                    continue;
                }
            }
            
            // Normal processing - add to batch
            let mut batch = self.log_batch.lock().unwrap();
            batch.push(log_message);
        }

        // Send the batch if it has logs
        let batch_size = {
            let batch = self.log_batch.lock().unwrap();
            batch.len()
        };
        
        if batch_size > 0 {
            self.send_and_clear_batch_simple().await?;
        }

        Ok(())
    }

    /// Called when a trace ID is extracted - updates all buffered logs and sends them
    pub async fn on_trace_id_extracted(&self, trace_id: &str) -> std::io::Result<()> {
        let (Some(ref extraction_state), Some(ref buffered_logs_arc)) = 
            (&self.trace_extraction_state, &self.buffered_logs) else {
            return Ok(()); // Nothing to do if trace ID collection is disabled
        };

        // Mark that we've successfully extracted the trace ID
        *extraction_state.lock().unwrap() = TraceIdExtractionState::Extracted;
        
        // Get all buffered logs
        let mut buffered_logs = {
            let mut buffered = buffered_logs_arc.lock().unwrap();
            std::mem::take(&mut *buffered)
        };
        
        if buffered_logs.is_empty() {
            return Ok(());
        }
        
        debug!("Applied trace ID to {} buffered logs", buffered_logs.len());
        
        // Update all buffered logs with the trace ID
        for log in &mut buffered_logs {
            log.attributes.insert("trace.id".to_string(), trace_id.into());
        }
        
        // Send buffered logs immediately with chunking and retry logic
        self.send_buffered_logs_with_retry(buffered_logs).await
    }

    /// Called when trace ID extraction fails - sends all buffered logs without trace ID
    pub async fn on_trace_id_extraction_failed(&self) -> std::io::Result<()> {
        let (Some(ref extraction_state), Some(ref buffered_logs_arc)) = 
            (&self.trace_extraction_state, &self.buffered_logs) else {
            return Ok(()); // Nothing to do if trace ID collection is disabled
        };

        // Mark that we've attempted extraction but failed
        *extraction_state.lock().unwrap() = TraceIdExtractionState::Failed;
        
        // Get all buffered logs
        let buffered_logs = {
            let mut buffered = buffered_logs_arc.lock().unwrap();
            std::mem::take(&mut *buffered)
        };
        
        if buffered_logs.is_empty() {
            return Ok(());
        }
        
        warn!("Trace ID extraction failed - sending {} buffered logs without trace ID", buffered_logs.len());
        
        // Send buffered logs immediately with chunking and retry logic (without trace ID)
        self.send_buffered_logs_with_retry(buffered_logs).await
    }

    /// Reset the trace ID collection state for a new invocation
    pub fn reset_trace_id_state(&self) {
        if let (Some(ref extraction_state), Some(ref buffered_logs)) = 
            (&self.trace_extraction_state, &self.buffered_logs) {
            *extraction_state.lock().unwrap() = TraceIdExtractionState::Waiting;
            buffered_logs.lock().unwrap().clear();
        }
    }

    /// Flush ALL buffers before clearing request_id to ensure no logs are lost
    /// This includes: request_id_buffer, trace_id_buffer (if enabled), and regular log_batch
    pub async fn flush_all_buffers_before_clear(&self) -> std::io::Result<()> {
        // Flush all buffers: request_id_buffer, trace_id_buffer, log_batch, failed_logs_buffer
        let mut request_buffered_logs = self.request_id_buffer.lock().unwrap();
        if !request_buffered_logs.is_empty() {
            warn!("Found {} logs still in request_id buffer during flush - this shouldn't happen", request_buffered_logs.len());
            let has_request_id = {
                let context = self.invocation_context.lock().unwrap();
                !context.request_id.is_empty() && context.request_id != "unknown"
            };
            if has_request_id {
                let mut batch = self.log_batch.lock().unwrap();
                batch.extend(request_buffered_logs.drain(..));
            } else {
                warn!("Cannot send request_id buffered logs - no valid request_id available");
                request_buffered_logs.clear();
            }
        }
        drop(request_buffered_logs);

        // Flush trace ID buffered logs if trace ID collection is enabled
        if let Err(e) = self.flush_buffered_logs_at_invocation_end().await {
            error!("Error flushing trace ID buffered logs: {}", e);
        }

        // Flush regular log batch
        if let Err(e) = self.send_and_clear_batch_simple().await {
            error!("Error flushing regular log batch: {}", e);
        }

        // Flush failed logs buffer (send with original attributes)
        let mut failed_buffer = self.failed_logs_buffer.lock().unwrap();
        if !failed_buffer.is_empty() {
            warn!("Flushing {} failed logs before clearing request_id", failed_buffer.len());
            let failed_logs = std::mem::take(&mut *failed_buffer);
            self.send_buffered_logs_with_retry(failed_logs).await?;
        }
        drop(failed_buffer);

        Ok(())
    }

    /// Clear the request_id when the invocation is complete
    /// This ensures logs are buffered again for the next invocation until new request_id arrives
    pub fn clear_request_id(&self) {
        let mut context = self.invocation_context.lock().unwrap();
        context.request_id = "unknown".to_string();
        context.invoked_function_arn = "arn:aws:lambda:unknown:unknown:function:unknown".to_string();
        context.trace_id = None;
        // Clear all buffers to prevent cross-invocation pollution
        self.request_id_buffer.lock().unwrap().clear();
        self.log_batch.lock().unwrap().clear();
        self.failed_logs_buffer.lock().unwrap().clear();
        if let Some(ref buffered_logs) = self.buffered_logs {
            buffered_logs.lock().unwrap().clear();
        }
    }

    /// Called at the end of an invocation to flush any remaining buffered logs
    /// This ensures logs are not lost if trace ID was never extracted
    pub async fn flush_buffered_logs_at_invocation_end(&self) -> std::io::Result<()> {
        let Some(ref buffered_logs_arc) = self.buffered_logs else {
            return Ok(()); // Nothing to do if trace ID collection is disabled
        };

        let buffered_logs = {
            let mut buffered = buffered_logs_arc.lock().unwrap();
            std::mem::take(&mut *buffered)
        };
        
        if buffered_logs.is_empty() {
            return Ok(());
        }
        
        warn!("Invocation ended without trace ID extraction - flushing {} buffered logs", buffered_logs.len());
        
        // Send buffered logs immediately with chunking and retry logic
        self.send_buffered_logs_with_retry(buffered_logs).await
    }

    /// Send buffered logs directly with chunking and retry logic
    /// This bypasses the regular batch to ensure immediate sending with proper context
    async fn send_buffered_logs_with_retry(&self, logs: Vec<payload::LogMessage>) -> std::io::Result<()> {
        if logs.is_empty() {
            return Ok(());
        }
        
        let client = Arc::clone(&self.newrelic_client);
        let config = Arc::clone(&self.config);
        let context = self.invocation_context.lock().unwrap().clone();
        
        // Split large batches into smaller chunks to avoid 413 errors
        let chunks: Vec<Vec<payload::LogMessage>> = logs
            .chunks(MAX_BATCH_SIZE)
            .map(|chunk| chunk.to_vec())
            .collect();
        
        if chunks.len() > 1 {
            debug!("Chunking {} buffered logs into {} batches", logs.len(), chunks.len());
        }
        
        let mut failed_count = 0;
        let mut successful_chunks = 0;
        
        for (_chunk_idx, chunk) in chunks.into_iter().enumerate() {
            // Don't use failed buffer for trace ID buffered logs to prevent metadata pollution
            match self.send_chunk_with_retry_internal(&client, &config, chunk.clone(), &context.invoked_function_arn, false).await {
                Ok(()) => {
                    successful_chunks += 1;
                },
                Err(e) => {
                    error!("Buffered logs send failed: {}", e);
                    failed_count += chunk.len();
                    // These failed logs are dropped to prevent cross-invocation issues
                }
            }
        }
        
        if successful_chunks > 0 {
            info!("Successfully sent {} buffered log chunks", successful_chunks);
        }
        if failed_count > 0 {
            warn!("Dropped {} buffered logs due to send failures", failed_count);
        }
        
        Ok(())
    }

    /// Robust send method with chunking, retry logic, and proper error handling
    pub async fn send_and_clear_batch_simple(&self) -> std::io::Result<()> {
        let batch = {
            let mut batch_guard = self.log_batch.lock().unwrap();
            std::mem::take(&mut *batch_guard)
        };
        
        if batch.is_empty() {
            return Ok(());
        }

        let client = Arc::clone(&self.newrelic_client);
        let config = Arc::clone(&self.config);
        let context = self.invocation_context.lock().unwrap().clone();
        
        // Split large batches into smaller chunks to avoid 413 errors
        let chunks: Vec<Vec<payload::LogMessage>> = batch
            .chunks(MAX_BATCH_SIZE)
            .map(|chunk| chunk.to_vec())
            .collect();
        
        if chunks.len() > 1 {
            debug!("Chunking {} logs into {} batches", batch.len(), chunks.len());
        }
        
        let mut failed_logs = Vec::new();
        let mut successful_chunks = 0;
        
        for (chunk_idx, chunk) in chunks.into_iter().enumerate() {
            match self.send_chunk_with_retry(&client, &config, chunk.clone(), &context.invoked_function_arn, chunk_idx).await {
                Ok(()) => {
                    successful_chunks += 1;
                },
                Err(e) => {
                    error!("Log batch send failed: {}", e);
                    // Store failed logs for potential retry in current invocation only
                    // DO NOT carry over to next invocation to avoid request_id/trace_id pollution
                    failed_logs.extend(chunk);
                }
            }
        }
        
        if successful_chunks > 0 {
            info!("Successfully sent {} log chunks", successful_chunks);
        }
        if !failed_logs.is_empty() {
            warn!("Dropped {} logs to prevent cross-invocation pollution", failed_logs.len());
            // Explicitly drop failed logs rather than risk sending them with wrong metadata
            // in the next invocation
        }
        
        Ok(())
    }
    
    /// Send a single chunk with retry logic
    async fn send_chunk_with_retry(
        &self,
        client: &NewRelicClient,
        config: &ExtensionConfig,
        chunk: Vec<payload::LogMessage>,
        function_arn: &str,
        _chunk_idx: usize,
    ) -> std::io::Result<()> {
        self.send_chunk_with_retry_internal(client, config, chunk, function_arn, true).await
    }

    /// Send a single chunk with retry logic - internal method with buffer control
    async fn send_chunk_with_retry_internal(
        &self,
        client: &NewRelicClient,
        config: &ExtensionConfig,
        chunk: Vec<payload::LogMessage>,
        function_arn: &str,
        use_failed_buffer: bool, // If false, don't add to failed buffer (for retrying failed logs)
    ) -> std::io::Result<()> {
        let mut retries = 0;
        
        loop {
            match client.send_logs(config, chunk.clone(), function_arn).await {
                Ok(()) => {
                    return Ok(());
                },
                Err(e) => {
                    if retries == 0 {
                        warn!("Log send failed: {}", e);
                    }
                    
                    // Check if this is a 413 (Payload Too Large) error
                    if e.to_string().contains("413") || e.to_string().contains("Payload Too Large") {
                        error!("Payload too large even after chunking - dropping {} logs", chunk.len());
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData, 
                            "Payload too large even after chunking"
                        ));
                    }
                    
                    if retries < MAX_RETRIES {
                        retries += 1;
                        let delay = Duration::from_millis(RETRY_DELAY_MS * (2_u64.pow(retries as u32 - 1)));
                        tokio::time::sleep(delay).await;
                        continue;
                    } else {
                        if use_failed_buffer {
                            // Move failed logs to failed buffer for retry later
                            warn!("Max retries exceeded - moving {} logs to failed buffer for retry", chunk.len());
                            {
                                let mut failed_buffer = self.failed_logs_buffer.lock().unwrap();
                                failed_buffer.extend(chunk);
                            }
                        } else {
                            // For failed log retries, put back in failed buffer (but log it)
                            error!("Failed log retry exceeded max retries - keeping {} logs in failed buffer", chunk.len());
                            {
                                let mut failed_buffer = self.failed_logs_buffer.lock().unwrap();
                                failed_buffer.extend(chunk);
                            }
                        }
                        return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
                    }
                }
            }
        }
    }


}

#[async_trait]
impl Flush for LogProcessor {
    async fn flush(&self) -> std::io::Result<()> {
        self.send_and_clear_batch_simple().await
    }

    async fn final_flush(&self) -> std::io::Result<()> {
        self.send_and_clear_batch_simple().await
    }
}

