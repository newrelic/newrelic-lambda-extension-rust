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
        }
    }

    /// Processes a single log telemetry record, adding it to the batch if valid.
    pub fn process_record(&self, record: TelemetryRecord) {
        let message_str = record.record.to_string();
        
        // Avoid recursive logging from our own processors
        if message_str.contains("[LogProcessor]") || message_str.contains("[PlatformProcessor]") {
            return;
        }

        if let Some(log_message) = self.to_log_message(record) {
            // Check if trace ID collection is enabled and we should buffer logs
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
            attributes.insert("trace.id".to_string(), trace_id_value.clone().into());
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
        
        for (chunk_idx, chunk) in chunks.into_iter().enumerate() {
            match self.send_chunk_with_retry(&client, &config, chunk.clone(), &context.invoked_function_arn, chunk_idx).await {
                Ok(()) => {
                    successful_chunks += 1;
                },
                Err(e) => {
                    error!("Buffered logs send failed: {}", e);
                    failed_count += chunk.len();
                    // Don't accumulate failed logs - they will be dropped to prevent cross-invocation issues
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
                        error!("Max retries exceeded - dropping {} logs", chunk.len());
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

