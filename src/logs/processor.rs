
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

/// Safe mutex operations that won't panic and allow graceful degradation
trait SafeMutexOps<T> {
    /// Safely lock a mutex, returning None if poisoned (instead of panicking)
    fn safe_lock(&self) -> Option<std::sync::MutexGuard<T>>;
}

impl<T> SafeMutexOps<T> for Mutex<T> {
    fn safe_lock(&self) -> Option<std::sync::MutexGuard<T>> {
        match self.lock() {
            Ok(guard) => Some(guard),
            Err(e) => {
                error!("Mutex poisoned (extension will continue in degraded mode): {}", e);
                None
            }
        }
    }
}

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
    /// Failed logs buffer - logs that failed to send after all retries (with metadata stripped)
    failed_logs_buffer: Arc<Mutex<Vec<FailedLogEntry>>>,
}

/// Failed log entry that stores the original log without invocation-specific metadata
#[derive(Debug, Clone)]
struct FailedLogEntry {
    /// Original log message with invocation-specific metadata stripped
    log_message: payload::LogMessage,
    /// When this log failed (for cleanup of very old logs)
    failed_at: chrono::DateTime<chrono::Utc>,
    /// Number of retry attempts for this specific log
    retry_count: usize,
}

/// Configuration constants for batching and retry logic
const MAX_BATCH_SIZE: usize = 100; // Maximum logs per batch to avoid 413 errors
const MAX_RETRIES: usize = 3; // Maximum retry attempts for failed sends
const RETRY_DELAY_MS: u64 = 200; // Base retry delay in milliseconds
const MAX_FAILED_LOG_AGE_HOURS: i64 = 24; // Drop failed logs older than 24 hours
const MAX_FAILED_LOGS_BUFFER_SIZE: usize = 1000; // Limit failed buffer size

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

    /// Creates a no-op LogProcessor for disabled mode.
    pub fn new_noop() -> Self {
        use crate::config::ExtensionConfig;
        use crate::context::InvocationContext;
        
        let noop_config = Arc::new(ExtensionConfig::default());
        let noop_invocation_context = Arc::new(Mutex::new(InvocationContext::default()));
        let noop_client = Arc::new(NewRelicClient::new_noop());
        
        Self {
            log_batch: Arc::new(Mutex::new(Vec::new())),
            newrelic_client: noop_client,
            config: noop_config,
            invocation_context: noop_invocation_context,
            buffered_logs: None,
            trace_extraction_state: None,
            request_id_buffer: Arc::new(Mutex::new(Vec::new())),
            invocation_start_time: Arc::new(Mutex::new(chrono::Utc::now())),
            failed_logs_buffer: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub fn get_invocation_context(&self) -> Arc<Mutex<InvocationContext>> {
        Arc::clone(&self.invocation_context)
    }

    /// Update the invocation context for a new request (used by global log processor)
    pub fn update_invocation_context(&self, new_context: Arc<Mutex<InvocationContext>>) {
        // Copy the new context data to the existing context to maintain the same Arc reference
        if let (Some(mut current), Some(new)) = (self.invocation_context.safe_lock(), new_context.safe_lock()) {
            current.request_id = new.request_id.clone();
            current.invoked_function_arn = new.invoked_function_arn.clone();
            current.trace_id = new.trace_id.clone();
        } else {
            warn!("Failed to update invocation context - mutex poisoned, extension continuing in degraded mode");
        }
    }
    /// Updates the invocation start time (called when new invocation begins)
    pub fn set_invocation_start_time(&self, start_time: chrono::DateTime<chrono::Utc>) {
        if let Some(mut guard) = self.invocation_start_time.safe_lock() {
            *guard = start_time;
        } else {
            warn!("Failed to update invocation start time - mutex poisoned, extension continuing in degraded mode");
        }
    }

    /// Strips invocation-specific metadata from a log message for safe storage in failed buffer
    fn strip_invocation_metadata(&self, mut log_message: payload::LogMessage) -> payload::LogMessage {
        // Remove invocation-specific attributes that should not persist across invocations
        log_message.attributes.remove("aws.lambda_request_id");
        log_message.attributes.remove("faas.execution");
        log_message.attributes.remove("trace.id");
        log_message
    }

    /// Applies current invocation metadata to a failed log before retry
    fn apply_current_invocation_metadata(&self, mut log_message: payload::LogMessage) -> payload::LogMessage {
        if let Some(context) = self.invocation_context.safe_lock() {
            // Apply current request_id if available
            if !context.request_id.is_empty() && context.request_id != "unknown" {
                log_message.attributes.insert("aws.lambda_request_id".to_string(), 
                    serde_json::Value::String(context.request_id.clone()));
                log_message.attributes.insert("faas.execution".to_string(), 
                    serde_json::Value::String(context.request_id.clone()));
            }
            
            // Apply current trace_id if available
            if let Some(ref trace_id) = context.trace_id {
                log_message.attributes.insert("trace.id".to_string(), 
                    serde_json::Value::String(trace_id.clone()));
            }
        } else {
            warn!("Cannot apply invocation metadata - context mutex poisoned, log will be sent without metadata");
        }
        
        log_message
    }

    /// Cleans up old failed logs and limits buffer size
    fn cleanup_failed_logs_buffer(&self) {
        if let Some(mut failed_buffer) = self.failed_logs_buffer.safe_lock() {
            let now = chrono::Utc::now();
            
            // Remove logs older than MAX_FAILED_LOG_AGE_HOURS
            let initial_count = failed_buffer.len();
            failed_buffer.retain(|entry| {
                let age_hours = (now - entry.failed_at).num_hours();
                age_hours < MAX_FAILED_LOG_AGE_HOURS
            });
            
            let after_age_cleanup = failed_buffer.len();
            if after_age_cleanup < initial_count {
                info!("Cleaned up {} old failed logs (older than {} hours)", 
                      initial_count - after_age_cleanup, MAX_FAILED_LOG_AGE_HOURS);
            }
            
            // If still too many, remove oldest logs to stay within limit
            if failed_buffer.len() > MAX_FAILED_LOGS_BUFFER_SIZE {
                let excess = failed_buffer.len() - MAX_FAILED_LOGS_BUFFER_SIZE;
                failed_buffer.drain(0..excess);
                warn!("Failed logs buffer exceeded limit, dropped {} oldest entries", excess);
            }
        } else {
            warn!("Cannot cleanup failed logs buffer - mutex poisoned, skipping cleanup");
        }
    }

    /// Retry failed logs from previous invocations at the start of a new invocation
    pub async fn retry_failed_logs_before_invocation(&self) -> std::io::Result<()> {
        // First, cleanup old logs and limit buffer size
        self.cleanup_failed_logs_buffer();
        
        let failed_entries = {
            if let Some(mut failed_buffer) = self.failed_logs_buffer.safe_lock() {
                std::mem::take(&mut *failed_buffer)
            } else {
                warn!("Cannot retry failed logs - buffer mutex poisoned, skipping retry");
                return Ok(());
            }
        };

        if failed_entries.is_empty() {
            return Ok(());
        }

        let total_failed = failed_entries.len();
        let high_retry_logs = failed_entries.iter().filter(|e| e.retry_count >= MAX_RETRIES - 1).count();
        
        if high_retry_logs > 0 {
            warn!("Retrying {} failed logs (including {} near max retry limit) from previous invocations", 
                  total_failed, high_retry_logs);
        } else {
            info!("Retrying {} failed logs from previous invocations with current context", total_failed);
        }

        // Convert failed entries back to log messages with current invocation metadata
        let mut logs_to_retry = Vec::new();
        let mut logs_to_drop = Vec::new();

        for mut entry in failed_entries {
            // Check if this log has exceeded max retries
            if entry.retry_count >= MAX_RETRIES {
                logs_to_drop.push(entry);
            } else {
                // Apply current invocation metadata
                entry.log_message = self.apply_current_invocation_metadata(entry.log_message);
                entry.retry_count += 1;
                logs_to_retry.push(entry);
            }
        }

        if !logs_to_drop.is_empty() {
            warn!("Dropping {} failed logs that exceeded max retry attempts", logs_to_drop.len());
        }

        if logs_to_retry.is_empty() {
            return Ok(());
        }

        // Extract just the log messages for sending
        let log_messages: Vec<payload::LogMessage> = logs_to_retry
            .iter()
            .map(|entry| entry.log_message.clone())
            .collect();

        // Try to send the logs with current context
        match self.send_buffered_logs_with_retry(log_messages).await {
            Ok(()) => {
                info!("Successfully sent {} previously failed logs", logs_to_retry.len());
            }
            Err(e) => {
                error!("Failed to send previously failed logs: {}", e);
                
                // Put back the failed entries (with incremented retry count)
                {
                    let mut failed_buffer = self.failed_logs_buffer.lock().unwrap();
                    for entry in logs_to_retry {
                        // Strip metadata again before storing back
                        let stripped_log = self.strip_invocation_metadata(entry.log_message);
                        failed_buffer.push(FailedLogEntry {
                            log_message: stripped_log,
                            failed_at: chrono::Utc::now(),
                            retry_count: entry.retry_count,
                        });
                    }
                }
            }
        }

        Ok(())
    }

    /// Processes a single log telemetry record, adding it to the batch if valid.
    pub fn process_record(&self, record: TelemetryRecord) {
        trace!("LogProcessor received record type: {}", record.record_type);

        // Add diagnostic logging to see the actual record structure
        trace!("Full telemetry record: {}", serde_json::to_string_pretty(&record).unwrap_or_else(|_| "Failed to serialize".to_string()));

        // Check if this log type should be sent based on configuration
        match record.record_type.as_str() {
            "function" => {
                if !self.config.extension.send_function_logs {
                    trace!("Skipping function log - send_function_logs is disabled");
                    return;
                }
            }
            "extension" => {
                if !self.config.extension.send_extension_logs {
                    trace!("Skipping extension log - send_extension_logs is disabled");
                    return;
                }
            }
            _ => {
                // Unknown log type - process it
                trace!("Processing unknown log type: {}", record.record_type);
            }
        }
        
        // Add more detailed message extraction debugging
        let message_str = if let Some(message_value) = record.record.get("message") {
            trace!("Found message field, type: {:?}, value: {:?}", message_value, message_value);
            message_value.as_str().unwrap_or("Message field exists but not a string")
        } else {
            // Fix: Check if record is an object before trying to get keys
            let available_fields = if let serde_json::Value::Object(ref map) = record.record {
                map.keys().map(|k| k.as_str()).collect::<Vec<_>>()
            } else {
                vec!["<not an object>"]
            };
            trace!("No 'message' field found in record. Available fields: {:?}", available_fields);
            "No message field found"
        };
        
        trace!("Extracted message: {}", message_str.chars().take(200).collect::<String>());
        
        // Avoid recursive logging from our own processors - CRITICAL to prevent infinite loops
        // Only block very specific log processing messages that would create infinite feedback loops
        if message_str.contains("[LogProcessor]") || 
           message_str.contains("[PlatformProcessor]") ||
           message_str.contains("Processing log record") ||
           message_str.contains("Added log to batch") ||
           message_str.contains("Batching log for") ||
           message_str.contains("No logs in batch to send") ||
           message_str.contains("Buffered log for trace ID extraction") ||
           message_str.contains("Applied trace ID to") && message_str.contains("buffered logs") ||
           message_str.contains("Flushing batch of") && message_str.contains("logs") ||
           message_str.contains("Chunking") && message_str.contains("logs into") && message_str.contains("batches") ||
           (message_str.contains("Successfully sent") || message_str.contains("Failed to send")) && 
           (message_str.contains("log batch") || message_str.contains("previously failed logs")) {
            trace!("Filtering out recursive log message: {}", message_str.chars().take(100).collect::<String>());
            return;
        }
        
        trace!("Processing log message: {}", message_str.chars().take(100).collect::<String>());

        // Remove the timestamp filtering logic - we should flush old logs properly instead
        // The main.rs should handle flushing previous invocation logs before processing new ones

        if let Some(mut log_message) = self.to_log_message(record) {
            // First check: Do we have a valid request_id?
            let has_request_id = {
                let context = self.invocation_context.lock().unwrap();
                !context.request_id.is_empty()
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
                    debug!("Buffered log for trace ID extraction, buffer size: {}", buffered.len());
                    return;
                }
            }
            
            // If trace ID collection is disabled, use performance-optimized batching
            if self.trace_extraction_state.is_none() {
                let mut batch = self.log_batch.lock().unwrap();
                batch.push(log_message);
                let batch_size = batch.len();
                
                // Check if this is a warm start for performance optimization
                let is_warm_start = crate::IS_WARM_START.load(std::sync::atomic::Ordering::Relaxed);
                
                // Performance optimization: Use different strategies for cold vs warm starts
                // For cold starts: Send immediately to ensure logs are visible quickly  
                // For warm starts: Hold all logs until final flush to avoid impacting function duration
                if is_warm_start {
                    // For warm starts: Don't send during function execution, let harvester/final flush handle it
                    return;
                }
                
                // Cold start: send immediately when we hit threshold
                let flush_threshold = 15;
                let should_flush = batch_size >= flush_threshold;
                
                if should_flush {
                    let logs_to_send = std::mem::take(&mut *batch);
                    drop(batch); // Release lock
                    
                    debug!("Flushing batch of {} logs (warm_start={})", logs_to_send.len(), is_warm_start);
                    
                    let client = Arc::clone(&self.newrelic_client);
                    let config = Arc::clone(&self.config);
                    let context = self.invocation_context.lock().unwrap().clone();
                    
                    // Cold start: send immediately but async to not block other processing
                    tokio::spawn(async move {
                        if let Err(e) = client.send_logs(&config, logs_to_send, &context.invoked_function_arn).await {
                            error!("Failed to send log batch (cold start): {}", e);
                        } else {
                            debug!("Successfully sent log batch (cold start)");
                        }
                    });
                }
                return;
            }
            
            // Trace ID collection enabled but already extracted - add to batch for coordinated flush
            let mut batch = self.log_batch.lock().unwrap();
            batch.push(log_message);
            let batch_size = batch.len();
            
            debug!("Added log to batch for coordinated flush, current size: {}", batch_size);
        } else {
            warn!("Failed to convert telemetry record to log message");
        }
    }

    /// Converts a TelemetryRecord into a LogMessage, if applicable.
    fn to_log_message(&self, record: TelemetryRecord) -> Option<payload::LogMessage> {
        let timestamp = record.time.timestamp_millis();
        
        // Try multiple ways to extract the message from the telemetry record
        let message = if let Some(message_value) = record.record.get("message") {
            // Standard case: message field exists
            match message_value {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string()
            }
        } else {
            // Fallback: use the entire record as the message
            match &record.record {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string()
            }
        };
        
        let mut attributes = serde_json::Map::new();
        
        // Get request_id, invoked_function_arn and trace_id from the context
        let context = self.invocation_context.lock().unwrap();
        let request_id = &context.request_id;
        let invoked_function_arn = &context.invoked_function_arn;
        let trace_id = &context.trace_id;
        
        // Only add AWS Lambda specific attributes if we have a valid request_id
        if !request_id.is_empty() {
            attributes.insert("aws.lambda_request_id".to_string(), request_id.clone().into());
            attributes.insert("faas.execution".to_string(), request_id.clone().into());
        }
        
        // Only add faas.arn if we have a valid invoked_function_arn
        if !invoked_function_arn.is_empty() {
            attributes.insert("faas.arn".to_string(), invoked_function_arn.clone().into());
        }
        
        // Only add trace ID if it's present (not None)
        if let Some(ref trace_id_value) = trace_id {
            attributes.insert("trace.id".to_string(), serde_json::Value::String(trace_id_value.clone()));
        }
        
        // Add log level - extract from message content for proper filtering in New Relic UI
        let log_level = self.extract_log_level(&message);
        attributes.insert("level".to_string(), log_level.into());
        
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

    /// Extract log level from log message content - simple and fast implementation
    fn extract_log_level(&self, message: &str) -> &'static str {
        let message_upper = message.to_uppercase();
        
        // Check for common log level patterns (case insensitive)
        if message_upper.contains("ERROR") || message_upper.contains("FATAL") {
            "ERROR"
        } else if message_upper.contains("WARN") || message_upper.contains("WARNING") {
            "WARN"
        } else if message_upper.contains("DEBUG") {
            "DEBUG"
        } else if message_upper.contains("TRACE") {
            "TRACE"
        } else if message_upper.contains("INFO") {
            "INFO"
        } else {
            // Default to INFO if no level detected
            "INFO"
        }
    }

    /// Called when a trace ID is extracted - updates all buffered logs and moves them to main batch for coordinated sending
    pub fn on_trace_id_extracted_to_batch(&self, trace_id: &str) {
        let (Some(ref extraction_state), Some(ref buffered_logs_arc)) = 
            (&self.trace_extraction_state, &self.buffered_logs) else {
            return; // Nothing to do if trace ID collection is disabled
        };

        // Mark that we've successfully extracted the trace ID
        *extraction_state.lock().unwrap() = TraceIdExtractionState::Extracted;
        
        // Get all buffered logs
        let buffered_logs = {
            let mut buffered = buffered_logs_arc.lock().unwrap();
            std::mem::take(&mut *buffered)
        };
        
        if buffered_logs.is_empty() {
            return;
        }
        
        debug!("Moving {} trace-buffered logs to main batch with trace ID: {}", buffered_logs.len(), trace_id);
        
        // Update all buffered logs with the trace ID and move to main batch
        let mut batch = self.log_batch.lock().unwrap();
        for mut log in buffered_logs {
            log.attributes.insert("trace.id".to_string(), trace_id.into());
            batch.push(log);
        }
    }

    /// Called when a trace ID is extracted - updates all buffered logs and sends them immediately
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



    /// Flush all buffers with the previous invocation's context before starting new invocation
    /// This ensures logs from previous invocation get the correct request_id and trace_id
    pub async fn flush_with_previous_context(
        &self, 
        previous_request_id: &str, 
        previous_trace_id: Option<&str>
    ) -> std::io::Result<()> {
        // Flush request_id buffer with previous context
        let mut request_buffered_logs = {
            let mut buffered = self.request_id_buffer.lock().unwrap();
            std::mem::take(&mut *buffered)
        };

        // Update request_id buffered logs with previous context
        for log_message in &mut request_buffered_logs {
            log_message.attributes.insert("aws.lambda_request_id".to_string(), 
                            serde_json::Value::String(previous_request_id.to_string()));
            log_message.attributes.insert("faas.execution".to_string(), 
                            serde_json::Value::String(previous_request_id.to_string()));
            if let Some(trace_id) = previous_trace_id {
                log_message.attributes.insert("trace.id".to_string(), 
                                serde_json::Value::String(trace_id.to_string()));
            }
        }

        // Flush trace_id buffer with previous context if enabled
        let mut trace_buffered_logs = if let Some(ref buffered_logs_arc) = self.buffered_logs {
            let mut buffered = buffered_logs_arc.lock().unwrap();
            std::mem::take(&mut *buffered)
        } else {
            Vec::new()
        };

        // Update trace_id buffered logs with previous context
        for log_message in &mut trace_buffered_logs {
            if let Some(trace_id) = previous_trace_id {
                log_message.attributes.insert("trace.id".to_string(), 
                                serde_json::Value::String(trace_id.to_string()));
            }
        }

        // Combine all logs and send them
        let mut all_logs = request_buffered_logs;
        all_logs.extend(trace_buffered_logs);

        // Also flush current batch
        let current_batch = {
            let mut batch_guard = self.log_batch.lock().unwrap();
            std::mem::take(&mut *batch_guard)
        };
        all_logs.extend(current_batch);

        if !all_logs.is_empty() {
            info!("Flushing {} logs with previous invocation context (request_id: {})", 
                  all_logs.len(), previous_request_id);
            self.send_buffered_logs_with_retry(all_logs).await?;
        }

        // Reset trace ID state for new invocation
        self.reset_trace_id_state();

        Ok(())
    }

    /// Process buffered logs when request_id becomes available
    /// This moves logs from request_id_buffer to the main processing flow
    pub fn process_buffered_logs_with_request_id(&self, request_id: &str) {
        let buffered_logs = {
            let mut buffer = self.request_id_buffer.lock().unwrap();
            std::mem::take(&mut *buffer)
        };
        
        if !buffered_logs.is_empty() {
            info!("Processing {} buffered logs with new request_id: {}", buffered_logs.len(), request_id);
            
            for mut log_message in buffered_logs {
                // Add the request_id to the log message
                log_message.attributes.insert("aws.lambda_request_id".to_string(), 
                                serde_json::Value::String(request_id.to_string()));
                log_message.attributes.insert("faas.execution".to_string(), 
                                serde_json::Value::String(request_id.to_string()));
                
                // Now process the log through the normal flow (check trace ID, etc.)
                // Add to appropriate buffer based on trace ID collection state
                if let (Some(ref extraction_state), Some(ref buffered_logs_arc)) = 
                    (&self.trace_extraction_state, &self.buffered_logs) {
                    
                    let state = extraction_state.lock().unwrap();
                    let has_trace_id = {
                        let context = self.invocation_context.lock().unwrap();
                        context.trace_id.is_some()
                    };
                    
                    // Buffer logs only if we're still waiting for trace ID extraction attempt
                    if *state == TraceIdExtractionState::Waiting && !has_trace_id {
                        drop(state);
                        let mut buffered = buffered_logs_arc.lock().unwrap();
                        buffered.push(log_message);
                        continue;
                    }
                }
                
                // Add to main batch for sending
                let mut batch = self.log_batch.lock().unwrap();
                batch.push(log_message);
            }
        }
    }

    /// Clear the request_id when the invocation is complete
    /// This ensures logs are buffered again for the next invocation until new request_id arrives
    /// Note: We keep invoked_function_arn since it doesn't change between invocations
    pub fn clear_request_id(&self) {
        let mut context = self.invocation_context.lock().unwrap();
        context.request_id = String::new(); // Use empty string instead of "unknown"
        // Keep invoked_function_arn - it's the same for all invocations of this function
        // context.invoked_function_arn = String::new(); // DON'T clear this
        context.trace_id = None;
        // Clear all buffers to prevent cross-invocation pollution
        self.request_id_buffer.lock().unwrap().clear();
        self.log_batch.lock().unwrap().clear();
        self.failed_logs_buffer.lock().unwrap().clear();
        if let Some(ref buffered_logs) = self.buffered_logs {
            buffered_logs.lock().unwrap().clear();
        }
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
            debug!("No logs in batch to send");
            return Ok(());
        }

        info!("Sending {} logs to New Relic", batch.len());

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
            info!("Buffering {} failed logs for retry in next invocation", failed_logs.len());
            // Store failed logs with metadata stripped for safe retry in next invocation
            {
                let mut failed_buffer = self.failed_logs_buffer.lock().unwrap();
                let now = chrono::Utc::now();
                
                for log in failed_logs {
                    let stripped_log = self.strip_invocation_metadata(log);
                    failed_buffer.push(FailedLogEntry {
                        log_message: stripped_log,
                        failed_at: now,
                        retry_count: 0, // This is the first failure
                    });
                }
            }
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
                                let now = chrono::Utc::now();
                                
                                for log in chunk {
                                    let stripped_log = self.strip_invocation_metadata(log);
                                    failed_buffer.push(FailedLogEntry {
                                        log_message: stripped_log,
                                        failed_at: now,
                                        retry_count: 0,
                                    });
                                }
                            }
                        } else {
                            // For failed log retries, don't buffer again to avoid infinite loops
                            error!("Failed log retry exceeded max retries - dropping {} logs", chunk.len());
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
        // Send logs when explicitly flushed (called by main loop or harvester)
        self.send_and_clear_batch_simple().await
    }

    async fn final_flush(&self) -> std::io::Result<()> {
        // Final flush during shutdown
        self.send_and_clear_batch_simple().await
    }
}