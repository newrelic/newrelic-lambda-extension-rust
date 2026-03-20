
use tracing::{debug, error, trace, warn};
use crate::{
    config::ExtensionConfig,
    context::InvocationContext,
    newrelic::{client::NewRelicClient, flush::Flush, payload},
    telemetry::listener::TelemetryRecord,
};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

use crate::apm::app::ApmApp;
use crate::util::SafeMutexOps;

use super::retry::{
    estimate_log_size, should_retry_on_failure, FailedLogEntry,
    MAX_FAILED_LOGS,
};

/// State of trace ID extraction for the current invocation
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TraceIdExtractionState {
    Waiting,
    Extracted,
}

/// The LogProcessor is responsible for handling and transforming function and extension logs.
#[derive(Debug, Clone)]
pub struct LogProcessor {
    pub(crate) log_batch: Arc<Mutex<Vec<payload::LogMessage>>>,
    pub(crate) newrelic_client: Arc<NewRelicClient>,
    pub(crate) config: Arc<ExtensionConfig>,
    pub(crate) invocation_context: Arc<Mutex<InvocationContext>>,

    pub(crate) buffered_logs: Option<Arc<Mutex<Vec<payload::LogMessage>>>>,

    pub(crate) trace_extraction_state: Option<Arc<Mutex<TraceIdExtractionState>>>,

    pub(crate) request_id_buffer: Arc<Mutex<Vec<payload::LogMessage>>>,

    pub(crate) invocation_start_time: Arc<Mutex<chrono::DateTime<chrono::Utc>>>,

    pub(crate) apm_app: Option<Arc<tokio::sync::RwLock<Option<ApmApp>>>>,
    pub(crate) failed_logs_buffer: Arc<Mutex<std::collections::VecDeque<FailedLogEntry>>>,

    /// Track pending auto-flush tasks to ensure they complete before function ends
    pub(crate) pending_flush_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,

    /// Buffer for logs received during INIT phase before first INVOKE event
    pub(crate) pre_invoke_buffer: Arc<Mutex<Vec<payload::LogMessage>>>,

    /// Fallback ARN constructed from registration response (function_name + account_id + AWS_REGION)
    pub(crate) fallback_function_arn: Arc<Mutex<Option<String>>>,

    pub(crate) is_auto_flushing: Arc<std::sync::atomic::AtomicBool>,
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
            failed_logs_buffer: Arc::new(Mutex::new(std::collections::VecDeque::new())),
            apm_app,
            pending_flush_handles: Arc::new(Mutex::new(Vec::new())),
            pre_invoke_buffer: Arc::new(Mutex::new(Vec::new())),
            fallback_function_arn: Arc::new(Mutex::new(None)),
            is_auto_flushing: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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
            // REQUEST_ID: Prefer TELEMETRY_CURRENT_REQUEST_ID over invocation context.
            //
            // WHY: The event loop updates invocation context immediately when GET /next returns
            // with the NEW invoke, but telemetry API delivers function logs asynchronously.
            // Late logs from request_A can arrive AFTER the context has been updated to request_B.
            //
            // platform.start always arrives BEFORE function logs for that request in the telemetry
            // stream, so TELEMETRY_CURRENT_REQUEST_ID gives us the correct request_id association.
            // Falls back to invocation context if telemetry tracking hasn't started yet.
            let effective_request_id = crate::request::TELEMETRY_CURRENT_REQUEST_ID
                .lock()
                .ok()
                .and_then(|guard| guard.clone())
                .filter(|id| !id.is_empty())
                .unwrap_or_else(|| context.request_id.clone());

            if !effective_request_id.is_empty() && effective_request_id != "unknown" {
                let mut aws_attrs = serde_json::Map::new();
                aws_attrs.insert("lambda_request_id".to_string(),
                    serde_json::Value::String(effective_request_id.clone()));
                log_message.attributes.insert("aws".to_string(),
                    serde_json::Value::Object(aws_attrs));
                log_message.attributes.insert("faas.execution".to_string(),
                    serde_json::Value::String(effective_request_id));
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

        if let Some(log_message) = self.to_log_message(&record) {
            // Route to pre_invoke_buffer if ARN is empty (INIT phase before first INVOKE)
            let has_arn = {
                if let Some(context) = self.invocation_context.safe_lock() {
                    !context.invoked_function_arn.is_empty()
                } else {
                    false
                }
            };

            if !has_arn {
                if let Some(mut pre_invoke_buf) = self.pre_invoke_buffer.safe_lock() {
                    pre_invoke_buf.push(log_message);
                }
                return;
            }

            let has_valid_request_id = {
                if let Some(context) = self.invocation_context.safe_lock() {
                    !context.request_id.is_empty() && context.request_id != "unknown"
                } else {
                    false
                }
            };

            if !has_valid_request_id {
                if let Some(mut request_buffer) = self.request_id_buffer.safe_lock() {
                    request_buffer.push(log_message);
                }
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
                    if let Some(context) = self.invocation_context.safe_lock() {
                        (context.request_id.clone(), context.invoked_function_arn.clone())
                    } else {
                        (String::from("unknown"), String::new())
                    }
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

                if let Some(state) = extraction_state.safe_lock() {
                    let has_trace_id = {
                        if let Some(context) = self.invocation_context.safe_lock() {
                            context.trace_id.is_some()
                        } else {
                            false
                        }
                    };

                    if *state == TraceIdExtractionState::Waiting && !has_trace_id {
                        drop(state);
                        if let Some(mut buffered) = buffered_logs.safe_lock() {
                            buffered.push(log_message);
                        }
                        return;
                    }
                }
            }

            let Some(mut batch) = self.log_batch.safe_lock() else {
                warn!("Failed to acquire log_batch lock - dropping log message");
                return;
            };
            batch.push(log_message);
            let batch_size = batch.len();

            // Auto-flush threshold: 25 logs for faster delivery
            // End-of-request flush ensures completeness of remaining logs
            const FLUSH_THRESHOLD: usize = 25;
            let should_flush = batch_size >= FLUSH_THRESHOLD;

            if should_flush {
                // Check if already flushing to prevent infinite recursion
                if self.is_auto_flushing.swap(true, std::sync::atomic::Ordering::AcqRel) {
                    debug!("Auto-flush already in progress - skipping to prevent infinite loop");
                    return;
                }

                let logs_to_send = std::mem::take(&mut *batch);
                batch.shrink_to_fit();
                drop(batch);

                debug!("Auto-flushing batch of {} logs (threshold={})",
                       logs_to_send.len(), FLUSH_THRESHOLD);

                let client = Arc::clone(&self.newrelic_client);
                let config = Arc::clone(&self.config);
                let context = if let Some(ctx) = self.invocation_context.safe_lock() {
                    ctx.clone()
                } else {
                    warn!("Failed to acquire invocation context for auto-flush - returning logs to batch");
                    if let Some(mut batch) = self.log_batch.safe_lock() {
                        batch.extend(logs_to_send);
                    }
                    self.is_auto_flushing.store(false, std::sync::atomic::Ordering::Release);
                    return;
                };
                let failed_buffer = Arc::clone(&self.failed_logs_buffer);

                // GUARD: Use fallback ARN chain if context ARN is empty
                let auto_flush_arn = if !context.invoked_function_arn.is_empty() {
                    context.invoked_function_arn.clone()
                } else {
                    let fallback = self.get_best_available_arn();
                    if fallback.is_empty() {
                        error!(
                            "BLOCKED: Auto-flush skipped - no ARN available (request_id: '{}', {} logs returned to batch)",
                            context.request_id, logs_to_send.len()
                        );
                        // Put logs back in batch
                        if let Ok(mut batch) = self.log_batch.lock() {
                            batch.extend(logs_to_send);
                        }
                        self.is_auto_flushing.store(false, std::sync::atomic::Ordering::Release);
                        return;
                    }
                    fallback
                };

                // Spawn background task with proper retry logic and failed log buffering
                // Store handle to ensure it completes before function ends
                let handle = tokio::spawn(async move {
                    const MAX_PAYLOAD_SIZE: usize = 1_000_000; // 1MB
                    let mut chunks: Vec<Vec<payload::LogMessage>> = Vec::new();
                    let mut current_chunk = Vec::new();
                    let mut current_size = 0;

                    for log in logs_to_send {
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
                    // client.send_logs() already retries 3 times internally with backoff.
                    // No caller-side retry needed — on failure, buffer for cross-invocation retry.
                    let mut successful = 0;
                    for chunk in chunks {
                        let backup = chunk.clone(); // Clone once for failed buffer path
                        match client.send_logs(&config, chunk, &auto_flush_arn).await {
                            Ok(()) => {
                                successful += 1;
                            },
                            Err(_e) => {
                                warn!("Auto-flush send failed after client retries - buffering {} logs", backup.len());
                                if let Ok(mut buffer) = failed_buffer.lock() {
                                    let mut dropped = 0;
                                    for log in backup {
                                        if should_retry_on_failure(&log) {
                                            if buffer.len() >= MAX_FAILED_LOGS {
                                                buffer.pop_front();
                                            }
                                            buffer.push_back(FailedLogEntry {
                                                log_message: log,
                                                original_request_id: context.request_id.clone(),
                                                retry_count: 0,
                                            });
                                        } else {
                                            dropped += 1;
                                        }
                                    }
                                    if dropped > 0 {
                                        debug!("Dropped {} non-retriable extension/platform logs", dropped);
                                    }
                                }
                                break; // Stop trying other chunks if one failed
                            }
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
                self.is_auto_flushing.store(false, std::sync::atomic::Ordering::Release);
            }
        } else {
            warn!("Failed to convert telemetry record to log message");
        }
    }

    fn to_log_message(&self, record: &TelemetryRecord) -> Option<payload::LogMessage> {
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

        let log_source = match record.record_type.as_str() {
            "function" => payload::LogSource::Function,
            "extension" => payload::LogSource::Extension,
            t if t.starts_with("platform") => payload::LogSource::Platform,
            _ => payload::LogSource::Unknown,
        };

        Some(payload::LogMessage {
            timestamp,
            message,
            attributes,
            log_source,
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
    pub(crate) fn get_best_available_arn(&self) -> String {
        // First try LogProcessor's fallback ARN
        if let Ok(arn_guard) = self.fallback_function_arn.lock() {
            if let Some(ref arn) = *arn_guard {
                return arn.clone();
            }
        }

        // Fallback to global registration ARN
        crate::get_global_fallback_arn()
    }

    pub async fn on_trace_id_extracted(&self, trace_id: &str) -> std::io::Result<()> {
        let (Some(ref extraction_state), Some(ref buffered_logs_arc)) =
            (&self.trace_extraction_state, &self.buffered_logs) else {
            return Ok(());
        };

        if let Some(mut state) = extraction_state.safe_lock() {
            *state = TraceIdExtractionState::Extracted;
        } else {
            warn!("Failed to update trace extraction state - mutex poisoned");
        }

        let mut buffered_logs = {
            if let Some(mut buffered) = buffered_logs_arc.safe_lock() {
                std::mem::take(&mut *buffered)
            } else {
                return Ok(());
            }
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
            if let Some(mut state) = extraction_state.safe_lock() {
                *state = TraceIdExtractionState::Waiting;
            }
            if let Some(mut buf) = buffered_logs.safe_lock() {
                buf.clear();
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
