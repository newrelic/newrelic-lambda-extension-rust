
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

   
    fn apply_current_invocation_metadata(&self, mut log_message: payload::LogMessage) -> payload::LogMessage {
        if let Some(context) = self.invocation_context.safe_lock() {
            if !context.request_id.is_empty() && context.request_id != "temp" && context.request_id != "unknown" {
                log_message.attributes.insert("aws.lambda_request_id".to_string(),
                    serde_json::Value::String(context.request_id.clone()));
                log_message.attributes.insert("faas.execution".to_string(),
                    serde_json::Value::String(context.request_id.clone()));
            }

            if !context.invoked_function_arn.is_empty() && context.invoked_function_arn != "temp" {
                log_message.attributes.insert("faas.arn".to_string(),
                    serde_json::Value::String(context.invoked_function_arn.clone()));
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
            return;
        }
    
        if let Some(log_message) = self.to_log_message(record.clone()) {
            let has_valid_context = {
                let context = self.invocation_context.lock().unwrap();
                !context.request_id.is_empty() && 
                context.request_id != "temp" && 
                !context.invoked_function_arn.is_empty() && 
                context.invoked_function_arn != "temp"
            };
    
            if !has_valid_context {
                let mut request_buffer = self.request_id_buffer.lock().unwrap();
                request_buffer.push(log_message);
               
                return;
            }
            
            if let Some(ref apm_app_arc) = self.apm_app {
                if record.record_type == "function" && message_str.len() > 0 {
                    if message_str.contains("Task timed out") ||
                       message_str.contains("error") || message_str.contains("Error") ||
                       message_str.contains("Exception") || message_str.contains("exception") ||
                       message_str.contains("Fatal") || message_str.contains("fatal") {

                        info!("APM mode: Error detected in function log: {}", message_str.chars().take(100).collect::<String>());

                        let (request_id, function_arn) = {
                            let context = self.invocation_context.lock().unwrap();
                            (context.request_id.clone(), context.invoked_function_arn.clone())
                        };

                        info!("APM mode: Sending error event for request_id: {}", request_id);

                        let apm_clone = Arc::clone(apm_app_arc);
                        let msg_clone = message_str.to_string();

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
                                error_message: msg_clone.clone(),
                                error_type: error_type.to_string(),
                            });
                        }

                        // Send error asynchronously during the current invoke
                        // This prevents accumulation across invocations
                        let apm_guard = apm_clone.read().await;
                        if let Some(ref app) = *apm_guard {
                            if let Err(e) = app.send_error_event_from_fault(&msg_clone, &request_id, &function_arn).await {
                                debug!("Failed to send error event from function log fault: {}", e);
                            }
                        }
                    }
                }
            } else {
                // Standard (non-APM) mode: Send errors to telemetry endpoint
                if record.record_type == "function" && message_str.len() > 0 {
                    if message_str.contains("Task timed out") ||
                       message_str.contains("error") || message_str.contains("Error") ||
                       message_str.contains("Exception") || message_str.contains("exception") ||
                       message_str.contains("Fatal") || message_str.contains("fatal") {

                        info!("Standard mode: Error detected in function log: {}", message_str.chars().take(100).collect::<String>());

                        let (request_id, function_arn) = {
                            let context = self.invocation_context.lock().unwrap();
                            (context.request_id.clone(), context.invoked_function_arn.clone())
                        };

                        info!("Standard mode: Sending error for request_id: {}", request_id);
                        
                        // Determine error type
                        let error_type = if message_str.contains("Task timed out") {
                            "LambdaTimeout"
                        } else if message_str.contains("Exception") || message_str.contains("exception") {
                            "LambdaException"
                        } else if message_str.contains("Fatal") || message_str.contains("fatal") {
                            "LambdaFatalError"
                        } else {
                            "LambdaError"
                        };
                        
                        let client = Arc::clone(&self.newrelic_client);
                        let config = Arc::clone(&self.config);
                        let msg_clone = message_str.to_string();
                        let error_type_clone = error_type.to_string();

                        // Store error details for potential platform fault correlation
                        if let Ok(mut last_error) = crate::error_synthesis::LAST_DETECTED_ERROR.lock() {
                            *last_error = Some(crate::error_synthesis::LastDetectedError {
                                request_id: request_id.clone(),
                                error_message: msg_clone.clone(),
                                error_type: error_type_clone.clone(),
                            });
                        }

                        // Send error asynchronously during the current invoke
                        // This prevents accumulation across invocations
                        crate::error_synthesis::send_lambda_error(
                            &msg_clone,
                            &request_id,
                            &function_arn,
                            &error_type_clone,
                            &client,
                            &config,
                        ).await;
                    }
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
            
            let is_warm_start = crate::IS_WARM_START.load(std::sync::atomic::Ordering::Relaxed);
            
           
           
            let flush_threshold = if is_warm_start { 25 } else { 10 };
            let should_flush = batch_size >= flush_threshold;
            
            if should_flush {
                let logs_to_send = std::mem::take(&mut *batch);
                drop(batch);
                
                debug!("Flushing batch of {} logs (warm_start={}, threshold={})", 
                       logs_to_send.len(), is_warm_start, flush_threshold);
                
                let client = Arc::clone(&self.newrelic_client);
                let config = Arc::clone(&self.config);
                let context = self.invocation_context.lock().unwrap().clone();
                
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
        
        let log_level = self.extract_log_level(&message);
        attributes.insert("level".to_string(), log_level.into());
        
        attributes.insert("newrelic.logPattern".to_string(), "nr.DID_NOT_MATCH".into());
        
        attributes.insert("newrelic.source".to_string(), "api.logs".into());
    
    
        Some(payload::LogMessage {
            timestamp,
            message,
            attributes,
        })
    }

   
    fn extract_log_level(&self, message: &str) -> &'static str {
        
        let check_str = &message[..message.len().min(100)]; // Check first 100 chars
        
        if let Some(bracket_end) = check_str.find(']') {
            let after_bracket = &check_str[bracket_end+1..].trim_start();
            if after_bracket.starts_with("TRACE") || after_bracket.starts_with("trace") {
                return "TRACE";
            } else if after_bracket.starts_with("DEBUG") || after_bracket.starts_with("debug") {
                return "DEBUG";
            } else if after_bracket.starts_with("INFO") || after_bracket.starts_with("info") {
                return "INFO";
            } else if after_bracket.starts_with("WARN") || after_bracket.starts_with("warn") 
                      || after_bracket.starts_with("WARNING") || after_bracket.starts_with("warning") {
                return "WARN";
            } else if after_bracket.starts_with("ERROR") || after_bracket.starts_with("error") 
                      || after_bracket.starts_with("FATAL") || after_bracket.starts_with("fatal") {
                return "ERROR";
            }
        }
        
        if check_str.contains(" ERROR ") || check_str.contains(" error ") 
           || check_str.contains(" FATAL ") || check_str.contains(" fatal ")
           || check_str.starts_with("ERROR") || check_str.starts_with("error")
           || check_str.starts_with("FATAL") || check_str.starts_with("fatal") {
            "ERROR"
        } else if check_str.contains(" WARN ") || check_str.contains(" warn ")
                  || check_str.contains(" WARNING ") || check_str.contains(" warning ")
                  || check_str.starts_with("WARN") || check_str.starts_with("warn")
                  || check_str.starts_with("WARNING") || check_str.starts_with("warning") {
            "WARN"
        } else if check_str.contains(" DEBUG ") || check_str.contains(" debug ")
                  || check_str.starts_with("DEBUG") || check_str.starts_with("debug") {
            "DEBUG"
        } else if check_str.contains(" TRACE ") || check_str.contains(" trace ")
                  || check_str.starts_with("TRACE") || check_str.starts_with("trace") {
            "TRACE"
        } else if check_str.contains(" INFO ") || check_str.contains(" info ")
                  || check_str.starts_with("INFO") || check_str.starts_with("info") {
            "INFO"
        } else {
            "INFO"
        }
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
                info!("Retrying {} failed logs from previous invocation", failed_logs.len());
                
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
                        info!("Re-buffered {} logs that failed retry", buffer.len());
                    }
                });
            }
        }
        
        let buffered_logs = {
            let mut buffer = self.request_id_buffer.lock().unwrap();
            std::mem::take(&mut *buffer)
        };
        
        if !buffered_logs.is_empty() {
            info!("Processing {} buffered logs with new request_id: {}", buffered_logs.len(), request_id);
            
            for mut log_message in buffered_logs {
                log_message.attributes.insert("aws.lambda_request_id".to_string(), 
                                serde_json::Value::String(request_id.to_string()));
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
            info!("Successfully sent {} buffered log chunks", successful_chunks);
        }
        if failed_count > 0 {
            warn!("Dropped {} buffered logs due to send failures", failed_count);
        }
        
        Ok(())
    }

   
    pub async fn send_and_clear_batch_simple(&self) -> std::io::Result<()> {
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
                
                if let Some(request_id_value) = log.attributes.get("aws.lambda_request_id") {
                    if let Some(request_id_str) = request_id_value.as_str() {
                        request_id_str.hash(&mut hasher);
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
                info!("Deduplicated {} duplicate log(s) before sending", duplicate_count);
            }
            
            unique_logs
        };

        if deduplicated_batch.is_empty() {
            debug!("All logs were duplicates, nothing to send");
            return Ok(());
        }

        info!("Final flush: sending {} logs to New Relic", deduplicated_batch.len());

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
            info!("Chunking {} logs into {} size-based batches (max 1MB each)", 
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

}

#[async_trait]
impl Flush for LogProcessor {
    async fn flush(&self) -> std::io::Result<()> {
        self.send_and_clear_batch_simple().await
    }
}