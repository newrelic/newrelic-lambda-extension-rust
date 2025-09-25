use tracing::{debug, error, trace, warn};
use crate::{
    config::ExtensionConfig,
    context::InvocationContext,
    newrelic::{client::NewRelicClient, flush::Flush, payload},
    telemetry::listener::TelemetryRecord,
};
use async_trait::async_trait;
use std::{
    io::Result,
    sync::{Arc, Mutex},
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
                    debug!("Buffering log while waiting for trace ID extraction. Buffered count: {}", buffered.len());
                    return;
                }
            }
            
            // Normal processing - add to batch and potentially send
            let mut batch = self.log_batch.lock().unwrap();
            batch.push(log_message);
            let batch_size = batch.len();
            
            // Only log batch size every 10th addition to reduce noise
            if batch_size % 10 == 1 {
                trace!("Added log to batch. Current batch size: {}", batch_size);
            }
            
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
    pub async fn on_trace_id_extracted(&self, trace_id: &str) -> Result<()> {
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
            debug!("No buffered logs to update with trace ID: {}", trace_id);
            return Ok(());
        }
        
        debug!("Updating {} buffered logs with trace ID: {}", buffered_logs.len(), trace_id);
        
        // Update all buffered logs with the trace ID
        for log in &mut buffered_logs {
            log.attributes.insert("trace.id".to_string(), trace_id.into());
        }
        
        // Add buffered logs to the current batch
        {
            let mut batch = self.log_batch.lock().unwrap();
            batch.extend(buffered_logs);
        }
        
        // Send the batch immediately
        self.send_and_clear_batch_simple().await
    }

    /// Called when trace ID extraction fails - sends all buffered logs without trace ID
    pub async fn on_trace_id_extraction_failed(&self) -> Result<()> {
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
            debug!("No buffered logs to send after failed trace ID extraction");
            return Ok(());
        }
        
        warn!("Sending {} buffered logs without trace ID due to extraction failure", buffered_logs.len());
        
        // Add buffered logs to the current batch (without trace ID)
        {
            let mut batch = self.log_batch.lock().unwrap();
            batch.extend(buffered_logs);
        }
        
        // Send the batch immediately
        self.send_and_clear_batch_simple().await
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
    pub async fn flush_buffered_logs_at_invocation_end(&self) -> Result<()> {
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
        
        warn!("Flushing {} buffered logs without trace ID - invocation ending without trace extraction", buffered_logs.len());
        
        // Add buffered logs to the current batch without trace ID
        {
            let mut batch = self.log_batch.lock().unwrap();
            batch.extend(buffered_logs);
        }
        
        // Send the batch
        self.send_and_clear_batch_simple().await
    }

    /// Simple synchronous send method - just send the data without complex async handling
    pub async fn send_and_clear_batch_simple(&self) -> Result<()> {
        let batch = {
            let mut batch_guard = self.log_batch.lock().unwrap();
            std::mem::take(&mut *batch_guard)
        };
        
        if batch.is_empty() {
            return Ok(());
        }

        debug!("Sending {} logs to New Relic", batch.len());
        
        let client = Arc::clone(&self.newrelic_client);
        let config = Arc::clone(&self.config);
        let context = self.invocation_context.lock().unwrap().clone();
        
        // Send directly without spawning - simpler and more reliable
        match client.send_logs(&config, batch, &context.invoked_function_arn).await {
            Ok(()) => {
                trace!("Successfully sent logs to New Relic");
                Ok(())
            },
            Err(e) => {
                error!("Failed to send logs: {}", e);
                Err(std::io::Error::new(std::io::ErrorKind::Other, e))
            }
        }
    }
}

#[async_trait]
impl Flush for LogProcessor {
    async fn flush(&self) -> Result<()> {
        self.send_and_clear_batch_simple().await
    }

    async fn final_flush(&self) -> Result<()> {
        self.send_and_clear_batch_simple().await
    }
}

