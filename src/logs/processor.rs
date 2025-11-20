
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

use crate::apm::app::ApmApp;

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
    /// APM application instance for error event generation and entity.guid
    apm_app: Option<Arc<tokio::sync::RwLock<Option<ApmApp>>>>,
}

/// Failed log entry that stores the original log without invocation-specific metadata
#[derive(Debug, Clone)]
struct FailedLogEntry {
    /// Original log message (with original attribution preserved)
    log_message: payload::LogMessage,
    /// Original request ID where this log was generated
    original_request_id: String,
    /// Original function ARN where this log was generated
    original_arn: String,
    /// When the log was originally generated
    original_timestamp: chrono::DateTime<chrono::Utc>,
    /// When this log failed (for cleanup of very old logs)
    failed_at: chrono::DateTime<chrono::Utc>,
    /// Number of retry attempts for this specific log
    retry_count: usize,
}

/// Configuration constants for batching and retry logic
const MAX_BATCH_SIZE: usize = 100; // Maximum logs per batch to avoid 413 errors
const MAX_RETRIES: usize = 3; // Maximum retry attempts for failed sends

// Standardized backoff delays: 200ms, 400ms, 900ms
fn get_backoff_delay(retry_attempt: usize) -> Duration {
    match retry_attempt {
        1 => Duration::from_millis(200),
        2 => Duration::from_millis(400),
        _ => Duration::from_millis(900),
    }
}

impl LogProcessor {
    
    /// Creates a new LogProcessor.
    pub fn new(
        newrelic_client: Arc<NewRelicClient>,
        config: Arc<ExtensionConfig>,
        invocation_context: Arc<Mutex<InvocationContext>>,
        apm_app: Option<Arc<tokio::sync::RwLock<Option<ApmApp>>>>,
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
            apm_app,
        }
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

    /// Applies current invocation metadata to a log message
    fn apply_current_invocation_metadata(&self, mut log_message: payload::LogMessage) -> payload::LogMessage {
        if let Some(context) = self.invocation_context.safe_lock() {
            // Apply current request_id if available and valid
            if !context.request_id.is_empty() && context.request_id != "temp" && context.request_id != "unknown" {
                log_message.attributes.insert("aws.lambda_request_id".to_string(),
                    serde_json::Value::String(context.request_id.clone()));
                log_message.attributes.insert("faas.execution".to_string(),
                    serde_json::Value::String(context.request_id.clone()));
            }

            // Apply current invoked_function_arn if available and valid
            if !context.invoked_function_arn.is_empty() && context.invoked_function_arn != "temp" {
                log_message.attributes.insert("faas.arn".to_string(),
                    serde_json::Value::String(context.invoked_function_arn.clone()));
            }

            // Apply current trace_id if available
            if let Some(ref trace_id) = context.trace_id {
                log_message.attributes.insert("trace.id".to_string(),
                    serde_json::Value::String(trace_id.clone()));
            }
        } else {
            warn!("Cannot apply invocation metadata - context mutex poisoned, log will be sent without metadata");
        }
        
        // Add entity.guid if APM mode is active
        if let Some(ref apm_app_arc) = self.apm_app {
            // Try to read the APM app without blocking
            if let Ok(apm_guard) = apm_app_arc.try_read() {
                if let Some(ref app) = *apm_guard {
                    let entity_guid = app.get_entity_guid();
                    if !entity_guid.is_empty() {
                        log_message.attributes.insert("entity.guid".to_string(),
                            serde_json::Value::String(entity_guid.to_string()));
                    }
                }
            }
        }

        log_message
    }

    /// Processes a single log telemetry record, adding it to the batch if valid.
    pub fn process_record(&self, record: TelemetryRecord) {
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
        
        // Extract message for recursive filtering - handle both string and object records
        let message_str = match &record.record {
            serde_json::Value::String(s) => s.as_str(),
            serde_json::Value::Object(obj) => {
                if let Some(message_value) = obj.get("message") {
                    message_value.as_str().unwrap_or("")
                } else {
                    // If no message field, convert the entire object to string for filtering
                    &serde_json::to_string(&record.record).unwrap_or_default()
                }
            }
            _ => {
                // For other types, convert to string for filtering
                &serde_json::to_string(&record.record).unwrap_or_default()
            }
        };
        
        // CRITICAL: Prevent infinite recursion by filtering ALL extension-related log messages
        // This must be comprehensive to prevent feedback loops
        if 
           message_str.contains("Processing log record") ||
           message_str.contains("Added log to batch") ||
           message_str.contains("Batching log for") ||
           message_str.contains("No logs in batch to send") ||
           message_str.contains("Buffered log for trace ID extraction") ||
           message_str.contains("Applied trace ID to") && message_str.contains("buffered logs") ||
           message_str.contains("Flushing batch of") && message_str.contains("logs") ||
           message_str.contains("Chunking") && message_str.contains("logs into") && message_str.contains("batches") ||
           message_str.contains("Successfully sent") && (message_str.contains("log batch") || message_str.contains("previously failed logs")) ||
           message_str.contains("Failed to send") && (message_str.contains("log batch") || message_str.contains("previously failed logs")) ||
           message_str.contains("Full telemetry record") ||
           message_str.contains("Extracted message") ||
           message_str.contains("Processing log message") ||
           message_str.contains("No 'message' field found in record") ||
           message_str.contains("Available fields") ||
           message_str.contains("checkout") ||
           message_str.contains("Http::connect") ||
           message_str.contains("http1 handshake") ||
           message_str.contains("waiting for connection") ||
           message_str.contains("connection is ready") ||
           message_str.contains("connecting to") ||
           message_str.contains("connected to") ||
           message_str.contains("put; add idle connection") ||
           message_str.contains("put; found waiter") ||
           message_str.contains("Sending") && message_str.contains("log messages to NR") ||
           message_str.contains("Sending payload to NR endpoint") ||
           message_str.contains("Successfully sent payload to NR") ||
           message_str.contains("Request timeout") ||
           message_str.contains("LogProcessor received record type") ||
           message_str.contains("Processing unknown log type") ||
           message_str.contains("Added log to batch for coordinated flush") {
            // Silently drop these messages to prevent infinite recursion
            return;
        }
    
        if let Some(log_message) = self.to_log_message(record.clone()) {
            // CRITICAL FIX: Always check if we have valid invocation context before processing
            let has_valid_context = {
                let context = self.invocation_context.lock().unwrap();
                !context.request_id.is_empty() && 
                context.request_id != "temp" && 
                !context.invoked_function_arn.is_empty() && 
                context.invoked_function_arn != "temp"
            };
    
            // If we don't have valid invocation context, buffer the log
            if !has_valid_context {
                let mut request_buffer = self.request_id_buffer.lock().unwrap();
                request_buffer.push(log_message);
               
                return;
            }
            
            // APM MODE: Check for function log faults and generate error events
            if let Some(ref apm_app_arc) = self.apm_app {
                // Only check function logs, not extension logs
                if record.record_type == "function" && message_str.len() > 0 {
                    // Check if this looks like a fault/timeout/error
                    if message_str.contains("Task timed out") ||
                       message_str.contains("error") || message_str.contains("Error") ||
                       message_str.contains("Exception") || message_str.contains("exception") ||
                       message_str.contains("Fatal") || message_str.contains("fatal") {
                        
                        // Get context for error event
                        let (request_id, function_arn) = {
                            let context = self.invocation_context.lock().unwrap();
                            (context.request_id.clone(), context.invoked_function_arn.clone())
                        };
                        
                        // Send error event async (don't block log processing)
                        let apm_clone = Arc::clone(apm_app_arc);
                        let msg_clone = message_str.to_string();
                        tokio::spawn(async move {
                            let apm_guard = apm_clone.read().await;
                            if let Some(ref app) = *apm_guard {
                                if let Err(e) = app.send_error_event_from_fault(&msg_clone, &request_id, &function_arn).await {
                                    debug!("Failed to send error event from function log fault: {}", e);
                                }
                            }
                        });
                    }
                }
            }
    
            // Apply current invocation metadata (this will now have valid values)
            let log_message = self.apply_current_invocation_metadata(log_message);
    
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
            
            // Add to main batch for sending
            let mut batch = self.log_batch.lock().unwrap();
            batch.push(log_message);
            let batch_size = batch.len();
            
            // Check if this is a warm start for performance optimization
            let is_warm_start = crate::IS_WARM_START.load(std::sync::atomic::Ordering::Relaxed);
            
            // Improved batching strategy:
            // - Cold starts: Send smaller batches immediately for visibility
            // - Warm starts: Use larger batches but still send during execution to avoid loss
            let flush_threshold = if is_warm_start { 25 } else { 10 };
            let should_flush = batch_size >= flush_threshold;
            
            if should_flush {
                let logs_to_send = std::mem::take(&mut *batch);
                drop(batch); // Release lock
                
                debug!("Flushing batch of {} logs (warm_start={}, threshold={})", 
                       logs_to_send.len(), is_warm_start, flush_threshold);
                
                let client = Arc::clone(&self.newrelic_client);
                let config = Arc::clone(&self.config);
                let context = self.invocation_context.lock().unwrap().clone();
                
                // Send async to not block other processing
                tokio::spawn(async move {
                    if let Err(e) = client.send_logs(&config, logs_to_send, &context.invoked_function_arn).await {
                        error!("Failed to send log batch: {}", e);
                    } else {
                        debug!("Successfully sent log batch");
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
        
        // Add log level - extract from message content for proper filtering in New Relic UI
        let log_level = self.extract_log_level(&message);
        attributes.insert("level".to_string(), log_level.into());
        
        // Add newrelic.logPattern
        attributes.insert("newrelic.logPattern".to_string(), "nr.DID_NOT_MATCH".into());
        
        // Add newrelic.source
        attributes.insert("newrelic.source".to_string(), "api.logs".into());
    
        // NOTE: Do NOT add AWS Lambda attributes here - they will be added later
        // when we have valid invocation context via apply_current_invocation_metadata()
    
        Some(payload::LogMessage {
            timestamp,
            message,
            attributes,
        })
    }

    /// Extract log level from log message content - optimized case-insensitive check
    fn extract_log_level(&self, message: &str) -> &'static str {
        // Check for common log level patterns (case insensitive) without allocating
        if message.contains("ERROR") || message.contains("error")
           || message.contains("FATAL") || message.contains("fatal") {
            "ERROR"
        } else if message.contains("WARN") || message.contains("warn")
                  || message.contains("WARNING") || message.contains("warning") {
            "WARN"
        } else if message.contains("DEBUG") || message.contains("debug") {
            "DEBUG"
        } else if message.contains("TRACE") || message.contains("trace") {
            "TRACE"
        } else if message.contains("INFO") || message.contains("info") {
            "INFO"
        } else {
            // Default to INFO if no level detected
            "INFO"
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

    /// Reset the trace ID collection state for a new invocation
    pub fn reset_trace_id_state(&self) {
        if let (Some(ref extraction_state), Some(ref buffered_logs)) = 
            (&self.trace_extraction_state, &self.buffered_logs) {
            *extraction_state.lock().unwrap() = TraceIdExtractionState::Waiting;
            buffered_logs.lock().unwrap().clear();
        }
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

        debug!("Sending {} logs to New Relic", batch.len());

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
        
        let mut failed_logs = Vec::with_capacity(batch.len() / 10); // Estimate 10% might fail
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
            debug!("Successfully sent {} log chunks", successful_chunks);
        }
        if !failed_logs.is_empty() {
            warn!("Buffering {} failed logs for retry in next invocation", failed_logs.len());
            // Store failed logs with original attribution for accurate retry
            {
                let mut failed_buffer = self.failed_logs_buffer.lock().unwrap();
                let now = chrono::Utc::now();

                for log in failed_logs {
                    failed_buffer.push(FailedLogEntry {
                        log_message: log,
                        original_request_id: context.request_id.clone(),
                        original_arn: context.invoked_function_arn.clone(),
                        original_timestamp: now,
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
                        let delay = get_backoff_delay(retries);
                        tokio::time::sleep(delay).await;
                        continue;
                    } else {
                        if use_failed_buffer {
                            // Move failed logs to failed buffer for retry later
                            warn!("Max retries exceeded - moving {} logs to failed buffer for retry", chunk.len());
                            {
                                let context = self.invocation_context.lock().unwrap().clone();
                                let mut failed_buffer = self.failed_logs_buffer.lock().unwrap();
                                let now = chrono::Utc::now();

                                for log in chunk {
                                    failed_buffer.push(FailedLogEntry {
                                        log_message: log,
                                        original_request_id: context.request_id.clone(),
                                        original_arn: context.invoked_function_arn.clone(),
                                        original_timestamp: now,
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