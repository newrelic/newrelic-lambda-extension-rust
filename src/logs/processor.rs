
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
   
    fn safe_lock(&self) -> Option<std::sync::MutexGuard<'_, T>>;
}

impl<T> SafeMutexOps<T> for Mutex<T> {
    fn safe_lock(&self) -> Option<std::sync::MutexGuard<'_, T>> {
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
   
    Waiting,
   
    Extracted,
}

/// The LogProcessor is responsible for handling and transforming function and extension logs.
#[derive(Debug, Clone)]
pub struct LogProcessor {
    log_batch: Arc<Mutex<Vec<payload::LogMessage>>>,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
    invocation_context: Arc<Mutex<InvocationContext>>,

    buffered_logs: Option<Arc<Mutex<Vec<payload::LogMessage>>>>,

    trace_extraction_state: Option<Arc<Mutex<TraceIdExtractionState>>>,

    request_id_buffer: Arc<Mutex<Vec<payload::LogMessage>>>,

    invocation_start_time: Arc<Mutex<chrono::DateTime<chrono::Utc>>>,

    apm_app: Option<Arc<tokio::sync::RwLock<Option<ApmApp>>>>,
    failed_logs_buffer: Arc<Mutex<Vec<FailedLogEntry>>>,

    /// Track pending auto-flush tasks to ensure they complete before function ends
    pending_flush_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,

    /// Buffer for logs received during INIT phase before first INVOKE event
    pre_invoke_buffer: Arc<Mutex<Vec<payload::LogMessage>>>,

    /// Fallback ARN constructed from registration response (function_name + account_id + AWS_REGION)
    fallback_function_arn: Arc<Mutex<Option<String>>>,

    is_auto_flushing: Arc<Mutex<bool>>,
}

#[derive(Debug, Clone)]
struct FailedLogEntry {
    log_message: payload::LogMessage,
    original_request_id: String,
    retry_count: usize,
}

/// Configuration constants for batching and retry logic
const MAX_BATCH_SIZE: usize = 100;
const MAX_RETRIES: usize = 3;

fn get_backoff_delay(retry_attempt: usize) -> Duration {
    match retry_attempt {
        1 => Duration::from_millis(200),
        2 => Duration::from_millis(400),
        _ => Duration::from_millis(900),
    }
}

/// Extract structured log level from JSON record
/// Returns the uppercase level string if found in common level field names
fn get_structured_log_level(record: &serde_json::Value) -> Option<String> {
    if let serde_json::Value::Object(obj) = record {
        // Check common level field names used by various logging frameworks
        for level_key in &["level", "Level", "LogLevel", "log_level", "severity", "Severity"] {
            if let Some(level_value) = obj.get(*level_key) {
                if let Some(level_str) = level_value.as_str() {
                    return Some(level_str.to_uppercase());
                }
            }
        }
    }
    None
}

impl LogProcessor {
    
   
    pub fn new(
        newrelic_client: Arc<NewRelicClient>,
        config: Arc<ExtensionConfig>,
        invocation_context: Arc<Mutex<InvocationContext>>,
        apm_app: Option<Arc<tokio::sync::RwLock<Option<ApmApp>>>>,
    ) -> Self {
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
            pending_flush_handles: Arc::new(Mutex::new(Vec::new())),
            pre_invoke_buffer: Arc::new(Mutex::new(Vec::new())),
            fallback_function_arn: Arc::new(Mutex::new(None)),
            is_auto_flushing: Arc::new(Mutex::new(false)),
        }
    }

   
    /// Add a log message directly to the batch (used by platform processor)
    pub fn add_log_to_batch(&self, log_message: payload::LogMessage) {
        if let Ok(mut batch) = self.log_batch.lock() {
            batch.push(log_message);
        }
    }

   
    pub fn update_invocation_context(&self, new_context: Arc<Mutex<InvocationContext>>) {
        if let (Some(mut current), Some(new)) = (self.invocation_context.safe_lock(), new_context.safe_lock()) {
            current.request_id = new.request_id.clone();
            current.invoked_function_arn = new.invoked_function_arn.clone();
            current.trace_id = new.trace_id.clone();
        } else {
            warn!("Failed to update invocation context - mutex poisoned, extension continuing in degraded mode");
        }
    }
   
    pub fn set_invocation_start_time(&self, start_time: chrono::DateTime<chrono::Utc>) {
        if let Some(mut guard) = self.invocation_start_time.safe_lock() {
            *guard = start_time;
        } else {
            warn!("Failed to update invocation start time - mutex poisoned, extension continuing in degraded mode");
        }
    }

   
    pub fn apply_current_invocation_metadata(&self, mut log_message: payload::LogMessage) -> payload::LogMessage {
        if let Some(context) = self.invocation_context.safe_lock() {
            if !context.request_id.is_empty() && context.request_id != "unknown" {
                let mut aws_attrs = serde_json::Map::new();
                aws_attrs.insert("lambda_request_id".to_string(),
                    serde_json::Value::String(context.request_id.clone()));
                log_message.attributes.insert("aws".to_string(),
                    serde_json::Value::Object(aws_attrs));
                log_message.attributes.insert("faas.execution".to_string(),
                    serde_json::Value::String(context.request_id.clone()));
            }

            // Always use best available ARN (prefer invoked_function_arn, fallback to global context ARN)
            let arn = if !context.invoked_function_arn.is_empty() {
                context.invoked_function_arn.clone()
            } else {
                // Use fallback ARN from LogProcessor or global context
                self.get_best_available_arn()
            };
            
            if !arn.is_empty() {
                log_message.attributes.insert("faas.arn".to_string(),
                    serde_json::Value::String(arn));
            }

            if let Some(ref trace_id) = context.trace_id {
                log_message.attributes.insert("trace.id".to_string(),
                    serde_json::Value::String(trace_id.clone()));
            }
        } else {
            warn!("Cannot apply invocation metadata - context mutex poisoned, log will be sent without metadata");
        }
        
        if let Some(ref apm_app_arc) = self.apm_app {
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

   
    pub async fn process_record(&self, record: TelemetryRecord) {
        match record.record_type.as_str() {
            "function" => {
                if !self.config.extension.send_function_logs {
                    return;
                }
            }
            "extension" => {
                if !self.config.extension.send_extension_logs {
                    return;
                }
            }
            "platform" => {
                if !self.config.extension.send_platform_logs {
                    return;
                }
            }
            _ => {
                trace!("Processing unknown log type: {}", record.record_type);
            }
        }
        
        let message_str = match &record.record {
            serde_json::Value::String(s) => s.as_str(),
            serde_json::Value::Object(obj) => {
                if let Some(message_value) = obj.get("message") {
                    message_value.as_str().unwrap_or("")
                } else {
                    &serde_json::to_string(&record.record).unwrap_or_default()
                }
            }
            _ => {
                &serde_json::to_string(&record.record).unwrap_or_default()
            }
        };
    
        if let Some(log_message) = self.to_log_message(record.clone()) {
            // Route to pre_invoke_buffer if ARN is empty (INIT phase before first INVOKE)
            let has_arn = {
                let context = self.invocation_context.lock().unwrap();
                !context.invoked_function_arn.is_empty()
            };
    
            if !has_arn {
                let mut pre_invoke_buf = self.pre_invoke_buffer.lock().unwrap();
                pre_invoke_buf.push(log_message);
                return;
            }

            let has_valid_request_id = {
                let context = self.invocation_context.lock().unwrap();
                !context.request_id.is_empty() && context.request_id != "unknown"
            };
    
            if !has_valid_request_id {
                let mut request_buffer = self.request_id_buffer.lock().unwrap();
                request_buffer.push(log_message);
                return;
            }
            
            // Check if we're actually in APM mode (both outer and inner Option must be Some)
            let is_apm_mode = self.apm_app.as_ref().and_then(|apm_arc| {
                apm_arc.try_read().ok().and_then(|guard| {
                    if guard.is_some() { Some(()) } else { None }
                })
            }).is_some();

            // Determine if this log should be treated as an error
            // Uses extract_log_level which handles both structured JSON levels and
            // unstructured keyword matching with position-priority and word boundaries
            let should_treat_as_error = if record.record_type == "function" && !message_str.is_empty() {
                message_str.contains("Task timed out")
                    || self.extract_log_level(&record.record, message_str) == "ERROR"
            } else {
                false
            };

            if should_treat_as_error {
                // Escape newlines to prevent log corruption when captured by Lambda Telemetry API
                let sanitized_msg: String = message_str.chars().take(100).collect::<String>()
                    .replace('\n', "\\n").replace('\r', "\\r");
                
                let (request_id, function_arn) = {
                    let context = self.invocation_context.lock().unwrap();
                    (context.request_id.clone(), context.invoked_function_arn.clone())
                };

                // Store error details for potential platform fault correlation
                let error_type = if message_str.contains("Task timed out") {
                    "Timeout"
                } else if message_str.contains("Exception") || message_str.contains("exception") {
                    "Exception"
                } else if message_str.contains("Fatal") || message_str.contains("fatal") {
                    "Fatal"
                } else {
                    "Error"
                };

                if let Ok(mut last_error) = crate::error_synthesis::LAST_DETECTED_ERROR.lock() {
                    *last_error = Some(crate::error_synthesis::LastDetectedError {
                        request_id: request_id.clone(),
                        error_type: error_type.to_string(),
                    });
                }

                if is_apm_mode {
                    debug!("APM mode: Error detected in function log: {}", sanitized_msg);
                    debug!("APM mode: Sending error event for request_id: {}", request_id);

                    if let Some(ref apm_app_arc) = self.apm_app {
                        let apm_clone = Arc::clone(apm_app_arc);
                        let msg_clone = message_str.to_string();

                        // Send error asynchronously during the current invoke
                        let apm_guard = apm_clone.read().await;
                        if let Some(ref app) = *apm_guard {
                            if let Err(e) = app.send_error_event_from_fault(&msg_clone, &request_id, &function_arn).await {
                                debug!("Failed to send error event from function log fault: {}", e);
                            }
                        }
                    }
                } else {
                    // Standard (non-APM) mode: Send errors to telemetry endpoint
                    debug!("Standard mode: Error detected in function log: {}", sanitized_msg);
                    debug!("Standard mode: Sending error for request_id: {}", request_id);

                    // Determine error type - use consistent LambdaError for all function errors
                    // (except timeout which should match platform timeout)
                    let error_type = if message_str.contains("Task timed out") {
                        "LambdaTimeout"  // Match platform timeout error class
                    } else {
                        "LambdaError"    // Use single consistent error class
                    };

                    let client = Arc::clone(&self.newrelic_client);
                    let config = Arc::clone(&self.config);
                    let msg_clone = message_str.to_string();
                    let error_type_clone = error_type.to_string();

                    // Format error message like timeout/platform errors for Error Inbox recognition
                    // Format: "{ISO_TIMESTAMP} {REQUEST_ID} {ERROR_CLASS} {original_error_message}"
                    // Keep the original error message with [ERROR] prefix intact
                    let formatted_error_msg = format!(
                        "{} {} {} {}",
                        chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
                        request_id,
                        error_type,  // LambdaError or LambdaTimeout
                        msg_clone    // Keep original message with [ERROR] prefix
                    );

                    // Store error details for potential platform fault correlation
                    if let Ok(mut last_error) = crate::error_synthesis::LAST_DETECTED_ERROR.lock() {
                        *last_error = Some(crate::error_synthesis::LastDetectedError {
                            request_id: request_id.clone(),
                            error_type: error_type_clone.clone(),
                        });
                    }

                    // Send error asynchronously during the current invoke
                    crate::error_synthesis::send_lambda_error(
                        &formatted_error_msg,  // Send formatted message, not raw log
                        &request_id,
                        &function_arn,
                        &error_type_clone,
                        &client,
                        &config,
                    ).await;
                }
            }
    
            let log_message = self.apply_current_invocation_metadata(log_message);
    
            if let (Some(ref extraction_state), Some(ref buffered_logs)) = 
                (&self.trace_extraction_state, &self.buffered_logs) {
                
                let state = extraction_state.lock().unwrap();
                let has_trace_id = {
                    let context = self.invocation_context.lock().unwrap();
                    context.trace_id.is_some()
                };
                
                if *state == TraceIdExtractionState::Waiting && !has_trace_id {
                    drop(state);
                    let mut buffered = buffered_logs.lock().unwrap();
                    buffered.push(log_message);
                    return;
                }
            }
            
            let mut batch = self.log_batch.lock().unwrap();
            batch.push(log_message);
            let batch_size = batch.len();

            // Auto-flush threshold: 25 logs for faster delivery
            // End-of-request flush ensures completeness of remaining logs
            const FLUSH_THRESHOLD: usize = 25;
            let should_flush = batch_size >= FLUSH_THRESHOLD;

            if should_flush {
                // Check if already flushing to prevent infinite recursion
                let mut is_flushing = self.is_auto_flushing.lock().unwrap();
                if *is_flushing {
                    debug!("Auto-flush already in progress - skipping to prevent infinite loop");
                    return;
                }
                *is_flushing = true;
                drop(is_flushing);
                
                let logs_to_send = std::mem::take(&mut *batch);
                drop(batch);

                debug!("Auto-flushing batch of {} logs (threshold={})",
                       logs_to_send.len(), FLUSH_THRESHOLD);

                let client = Arc::clone(&self.newrelic_client);
                let config = Arc::clone(&self.config);
                let context = self.invocation_context.lock().unwrap().clone();
                let failed_buffer = Arc::clone(&self.failed_logs_buffer);

                // Spawn background task with proper retry logic and failed log buffering
                // Store handle to ensure it completes before function ends
                let handle = tokio::spawn(async move {
                    const MAX_PAYLOAD_SIZE: usize = 1_000_000; // 1MB
                    let mut chunks: Vec<Vec<payload::LogMessage>> = Vec::new();
                    let mut current_chunk = Vec::new();
                    let mut current_size = 0;

                    for log in logs_to_send {
                        let log_size = 8 + log.message.len() +
                                      serde_json::to_string(&log.attributes).unwrap_or_default().len();

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
                    let mut successful = 0;
                    for chunk in chunks {
                        let mut retries = 0;
                        let mut send_failed = false;
                        
                        loop {
                            match client.send_logs(&config, chunk.clone(), &context.invoked_function_arn).await {
                                Ok(()) => {
                                    successful += 1;
                                    break;
                                },
                                Err(_e) => {
                                    if retries < MAX_RETRIES {
                                        retries += 1;
                                        tokio::time::sleep(get_backoff_delay(retries)).await;
                                        continue;
                                    } else {
                                        warn!("Auto-flush failed after {} retries - buffering {} logs", MAX_RETRIES, chunk.len());
                                        send_failed = true;
                                        // Buffer failed logs for retry on next invocation
                                        if let Ok(mut buffer) = failed_buffer.lock() {
                                            for log in chunk {
                                                buffer.push(FailedLogEntry {
                                                    log_message: log,
                                                    original_request_id: context.request_id.clone(),
                                                    retry_count: 0,
                                                });
                                            }
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                        
                        if send_failed {
                            break; // Stop trying other chunks if one failed
                        }
                    }
                    
                    if successful > 0 {
                        debug!("Auto-flush sent {} chunk(s) successfully", successful);
                    }
                });

                // Track this handle so end-of-request flush can await it
                if let Ok(mut handles) = self.pending_flush_handles.lock() {
                    handles.push(handle);
                }
                // Reset the flushing flag after spawning background task
                if let Ok(mut is_flushing) = self.is_auto_flushing.lock() {
                    *is_flushing = false;
                }
            }
        } else {
            warn!("Failed to convert telemetry record to log message");
        }
    }

   
    fn to_log_message(&self, record: TelemetryRecord) -> Option<payload::LogMessage> {
        let timestamp = record.time.timestamp_millis();
        
        let message = if let Some(message_value) = record.record.get("message") {
            match message_value {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string()
            }
        } else {
            match &record.record {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string()
            }
        };
        
        let mut attributes = serde_json::Map::new();
        
        let log_level = self.extract_log_level(&record.record, &message);
        attributes.insert("level".to_string(), log_level.into());
        
        attributes.insert("newrelic.logPattern".to_string(), "nr.DID_NOT_MATCH".into());
        
        attributes.insert("newrelic.source".to_string(), "api.logs".into());
    
    
        Some(payload::LogMessage {
            timestamp,
            message,
            attributes,
        })
    }

    /// Extract log level from structured JSON or unstructured message string
    /// Priority: 1) Structured JSON fields, 2) Keyword patterns with word boundaries
    /// Made pub(crate) for unit testing
    pub(crate) fn extract_log_level(&self, record: &serde_json::Value, message: &str) -> &'static str {
        // FIRST: Check for structured level field in JSON record
        if let Some(structured_level) = get_structured_log_level(record) {
            return match structured_level.as_str() {
                "TRACE" | "VERBOSE" => "TRACE",
                "DEBUG" => "DEBUG",
                "INFO" | "INFORMATION" => "INFO",
                "WARN" | "WARNING" => "WARN",
                "ERROR" | "FATAL" | "CRITICAL" => "ERROR",
                _ => "INFO",
            };
        }

        // FALLBACK: Position-priority parsing for unstructured logs
        // The earliest keyword match wins, because log level prefixes (e.g. [INFO], ERROR:)
        // appear at the start of the line, before any message body that might contain
        // level-like words (e.g. "No error detected").
        let search_limit = message.len().min(150);
        let search_area = &message[..search_limit];
        let lower = search_area.to_lowercase();

        // Level keywords mapped to their output levels
        const LEVELS: &[(&str, &str)] = &[
            ("fatal", "ERROR"),
            ("critical", "ERROR"),
            ("error", "ERROR"),
            ("warning", "WARN"),
            ("warn", "WARN"),
            ("debug", "DEBUG"),
            ("trace", "TRACE"),
            ("info", "INFO"),
        ];

        // Find all keyword matches with valid word boundaries, pick earliest position
        let mut best_match: Option<(usize, &str)> = None;

        for &(keyword, level) in LEVELS {
            if let Some(pos) = lower.find(keyword) {
                // Check word boundaries to prevent false matches
                // Before: must be start of string or non-alphanumeric
                let before_ok = pos == 0 || {
                    let prev_byte = lower.as_bytes()[pos - 1];
                    !prev_byte.is_ascii_alphanumeric()
                };

                // After: must be end of string or non-alphanumeric
                let after_pos = pos + keyword.len();
                let after_ok = after_pos >= lower.len() || {
                    let next_byte = lower.as_bytes()[after_pos];
                    !next_byte.is_ascii_alphanumeric()
                };

                if before_ok && after_ok {
                    match best_match {
                        Some((best_pos, _)) if pos >= best_pos => {}
                        _ => best_match = Some((pos, level)),
                    }
                }
            }
        }

        match best_match {
            Some((_, level)) => level,
            None => "INFO",
        }
    }

    /// Construct ARN from registration response Format: arn:aws:lambda:{region}:{account_id}:function:{function_name}
    /// Store fallback ARN from registration response for emergency shutdown scenarios
    /// ARN is pre-constructed from registration data to ensure consistency
    pub fn set_fallback_arn(&self, registration_fallback_arn: &str) {
        if let Ok(mut arn_guard) = self.fallback_function_arn.lock() {
            *arn_guard = Some(registration_fallback_arn.to_string());
            debug!("Set registration fallback ARN: {}", registration_fallback_arn);
        }
    }

    /// Get best available ARN: check fallback_function_arn, then global context
    fn get_best_available_arn(&self) -> String {
        // First try LogProcessor's fallback ARN
        if let Ok(arn_guard) = self.fallback_function_arn.lock() {
            if let Some(ref arn) = *arn_guard {
                return arn.clone();
            }
        }
        
        // Fallback to global context ARN (set during registration)
        if let Ok(global_ctx) = crate::CURRENT_INVOCATION_CONTEXT.read() {
            if !global_ctx.invoked_function_arn.is_empty() {
                return global_ctx.invoked_function_arn.clone();
            }
        }
        
        String::new()
    }

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
            let buf_size = self.pre_invoke_buffer.lock().unwrap().len();
            if buf_size > 0 {
                debug!("Skipping pre-invoke log processing - context not ready yet ({} logs waiting)", buf_size);
            }
            return;
        }
        
        let mut pre_invoke_logs = {
            let mut buf = self.pre_invoke_buffer.lock().unwrap();
            std::mem::take(&mut *buf)
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
            let mut buf = self.pre_invoke_buffer.lock().unwrap();
            std::mem::take(&mut *buf)
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

   
    pub async fn on_trace_id_extracted(&self, trace_id: &str) -> std::io::Result<()> {
        let (Some(ref extraction_state), Some(ref buffered_logs_arc)) = 
            (&self.trace_extraction_state, &self.buffered_logs) else {
            return Ok(());
        };

        *extraction_state.lock().unwrap() = TraceIdExtractionState::Extracted;
        
        let mut buffered_logs = {
            let mut buffered = buffered_logs_arc.lock().unwrap();
            std::mem::take(&mut *buffered)
        };
        
        if buffered_logs.is_empty() {
            return Ok(());
        }
        
        debug!("Applied trace ID to {} buffered logs", buffered_logs.len());
        
        for log in &mut buffered_logs {
            log.attributes.insert("trace.id".to_string(), trace_id.into());
        }
        
        self.send_buffered_logs_with_retry(buffered_logs).await
    }

   
    pub fn reset_trace_id_state(&self) {
        if let (Some(ref extraction_state), Some(ref buffered_logs)) = 
            (&self.trace_extraction_state, &self.buffered_logs) {
            *extraction_state.lock().unwrap() = TraceIdExtractionState::Waiting;
            buffered_logs.lock().unwrap().clear();
        }
    }

   
   
    pub fn process_buffered_logs_with_request_id(&self, request_id: &str) {
        let is_warm_start = crate::IS_WARM_START.load(std::sync::atomic::Ordering::Relaxed);
        
        if is_warm_start {
            let failed_logs = {
                let mut buffer = self.failed_logs_buffer.lock().unwrap();
                std::mem::take(&mut *buffer)
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
                        let mut buffer = failed_buffer.lock().unwrap();
                        buffer.extend(still_failed);
                        debug!("Re-buffered {} logs that failed retry", buffer.len());
                    }
                });
            }
        }
        
        let buffered_logs = {
            let mut buffer = self.request_id_buffer.lock().unwrap();
            std::mem::take(&mut *buffer)
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
                    
                    let state = extraction_state.lock().unwrap();
                    let has_trace_id = {
                        let context = self.invocation_context.lock().unwrap();
                        context.trace_id.is_some()
                    };
                    
                    if *state == TraceIdExtractionState::Waiting && !has_trace_id {
                        drop(state);
                        let mut buffered = buffered_logs_arc.lock().unwrap();
                        buffered.push(log_message);
                        continue;
                    }
                }
                
                let mut batch = self.log_batch.lock().unwrap();
                batch.push(log_message);
            }
        }
    }



   
   
    async fn send_buffered_logs_with_retry(&self, logs: Vec<payload::LogMessage>) -> std::io::Result<()> {
        if logs.is_empty() {
            return Ok(());
        }
        
        let client = Arc::clone(&self.newrelic_client);
        let config = Arc::clone(&self.config);
        let context = self.invocation_context.lock().unwrap().clone();
        
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
            match self.send_chunk_with_retry_internal(&client, &config, chunk.clone(), &context.invoked_function_arn, false).await {
                Ok(()) => {
                    successful_chunks += 1;
                },
                Err(e) => {
                    error!("Buffered logs send failed: {}", e);
                    failed_count += chunk.len();
                }
            }
        }
        
        if successful_chunks > 0 {
            debug!("Successfully sent {} buffered log chunks", successful_chunks);
        }
        if failed_count > 0 {
            warn!("Dropped {} buffered logs due to send failures", failed_count);
        }
        
        Ok(())
    }


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

        // First, await all pending auto-flush tasks to ensure they complete
        // before we do the final flush (prevents logs from being cancelled)
        let pending_handles = {
            let mut handles = self.pending_flush_handles.lock().unwrap();
            std::mem::take(&mut *handles)
        };

        if !pending_handles.is_empty() {
            debug!("Waiting for {} pending auto-flush tasks to complete", pending_handles.len());
            for handle in pending_handles {
                let _ = handle.await; // Ignore JoinErrors, just ensure task completes
            }
            debug!("All pending auto-flush tasks completed");
        }

        let batch = {
            let mut batch_guard = self.log_batch.lock().unwrap();
            std::mem::take(&mut *batch_guard)
        };
        
        if batch.is_empty() {
            debug!("No logs in batch to send");
            return Ok(());
        }

        let deduplicated_batch = {
            use std::collections::HashMap;
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            
            let mut seen = HashMap::new();
            let mut unique_logs = Vec::new();
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
        let context = self.invocation_context.lock().unwrap().clone();
        
        const MAX_PAYLOAD_SIZE: usize = 1_000_000; // 1MB
        let mut chunks: Vec<Vec<payload::LogMessage>> = Vec::new();
        let mut current_chunk = Vec::new();
        let mut current_size = 0;
        
        for log in deduplicated_batch {
            let log_size = 8 + log.message.len() + 
                          serde_json::to_string(&log.attributes).unwrap_or_default().len();
            
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
        
        let mut failed_logs = Vec::new();
        let mut successful_chunks = 0;
        
        for (chunk_idx, chunk) in chunks.into_iter().enumerate() {
            match self.send_chunk_with_retry(&client, &config, chunk.clone(), &context.invoked_function_arn, chunk_idx).await {
                Ok(()) => {
                    successful_chunks += 1;
                },
                Err(e) => {
                    error!("Log batch send failed: {}", e);
                    failed_logs.extend(chunk);
                }
            }
        }
        
        if successful_chunks > 0 {
            info!("Successfully sent {} log chunks", successful_chunks);
        }
        if !failed_logs.is_empty() {
            warn!("Buffering {} failed logs for retry on next invocation", failed_logs.len());
            let mut failed_buffer = self.failed_logs_buffer.lock().unwrap();
            
            for log in failed_logs {
                failed_buffer.push(FailedLogEntry {
                    log_message: log,
                    original_request_id: context.request_id.clone(),
                    retry_count: 0,
                });
            }
        }
        
        Ok(())
    }
    
   
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

   
    async fn send_chunk_with_retry_internal(
        &self,
        client: &NewRelicClient,
        config: &ExtensionConfig,
        chunk: Vec<payload::LogMessage>,
        function_arn: &str,
        use_failed_buffer: bool,
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
                            warn!("Max retries exceeded - buffering {} logs for retry on next invocation", chunk.len());
                            let context = self.invocation_context.lock().unwrap().clone();
                            let mut failed_buffer = self.failed_logs_buffer.lock().unwrap();
                            
                            for log in chunk {
                                failed_buffer.push(FailedLogEntry {
                                    log_message: log,
                                    original_request_id: context.request_id.clone(),
                                    retry_count: 0,
                                });
                            }
                        } else {
                            error!("Failed log retry exceeded max retries - dropping {} logs", chunk.len());
                        }
                        return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
                    }
                }
            }
        }
    }

    /// Helper method to send logs with proper 1MB chunking
    /// Used by both auto-flush (25 logs) and end-of-request flush
    async fn send_logs_with_chunking(
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
            let log_size = 8 + log.message.len() +
                          serde_json::to_string(&log.attributes).unwrap_or_default().len();

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

}

#[async_trait]
impl Flush for LogProcessor {
    async fn flush(&self) -> std::io::Result<()> {
        self.send_and_clear_batch_simple().await
    }
}