use crate::{
    config::ExtensionConfig,
    context::InvocationContext,
    newrelic::{client::NewRelicClient, flush::Flush},
    telemetry::listener::TelemetryRecord,
};
use async_trait::async_trait;
use std::{
    io::Result,
    sync::{Arc, Mutex},
};
use tracing::{debug, error, trace};

/// The PlatformProcessor is responsible for handling all platform-related telemetry events.
#[derive(Debug)]
pub struct PlatformProcessor {
    platform_events_batch: Mutex<Vec<serde_json::Value>>,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
    invocation_context: Arc<Mutex<InvocationContext>>,
}

impl PlatformProcessor {
    /// Creates a new PlatformProcessor.
    pub fn new(
        newrelic_client: Arc<NewRelicClient>,
        config: Arc<ExtensionConfig>,
        invocation_context: Arc<Mutex<InvocationContext>>,
    ) -> Self {
        Self {
            platform_events_batch: Mutex::new(Vec::new()),
            newrelic_client,
            config,
            invocation_context,
        }
    }

    /// Creates a no-op PlatformProcessor for disabled mode.
    pub fn new_noop() -> Self {
        use crate::config::ExtensionConfig;
        use crate::context::InvocationContext;
        
        let noop_config = Arc::new(ExtensionConfig::default());
        let noop_invocation_context = Arc::new(Mutex::new(InvocationContext::default()));
        let noop_client = Arc::new(NewRelicClient::new_noop());
        
        Self {
            platform_events_batch: Mutex::new(Vec::new()),
            newrelic_client: noop_client,
            config: noop_config,
            invocation_context: noop_invocation_context,
        }
    }

    /// Processes a single platform telemetry record.
    pub fn process_record(&self, record: TelemetryRecord) {
        // Convert platform event to a proper log message format
        let (message, level) = self.create_platform_log_message(&record);
        
        // Create structured log event
        let log_event = serde_json::json!({
            "timestamp": record.time,
            "message": message,
            "level": level,
            "type": record.record_type,
            "requestId": self.extract_request_id_from_record(&record)
        });
        
        let mut batch = self.platform_events_batch.lock().unwrap();
        batch.push(log_event);
        
        // Only log batch size every 10th addition to reduce noise
        if batch.len() % 10 == 1 {
            trace!("Added platform event to batch. Current batch size: {}", batch.len());
        }
    }

    /// Create a proper log message for platform events with readable format
    fn create_platform_log_message(&self, record: &TelemetryRecord) -> (String, String) {
        match record.record_type.as_str() {
            "platform.report" => {
                // Format as clean AWS CloudWatch REPORT log line (no [NR_EXT] prefix)
                // This creates the exact same format as CloudWatch logs for consistency
                if let Some(report_line) = self.convert_platform_report_to_log_line(record) {
                    (report_line, "INFO".to_string())
                } else {
                    ("REPORT formatting failed - missing required fields".to_string(), "WARN".to_string())
                }
            }
            "platform.initStart" => {
                let init_type = record.record.get("initializationType")
                    .and_then(|v| v.as_str()).unwrap_or("unknown");
                let runtime_version = record.record.get("runtimeVersion")
                    .and_then(|v| v.as_str()).unwrap_or("unknown");
                let phase = record.record.get("phase")
                    .and_then(|v| v.as_str()).unwrap_or("unknown");
                    
                (format!("INIT START RequestId: {} Type: {} Runtime: {} Phase: {}", 
                    self.extract_request_id_from_record(record).unwrap_or_else(|| "unknown".to_string()),
                    init_type, runtime_version, phase), "INFO".to_string())
            }
            "platform.initRuntimeDone" => {
                let init_type = record.record.get("initializationType")
                    .and_then(|v| v.as_str()).unwrap_or("unknown");
                let phase = record.record.get("phase")
                    .and_then(|v| v.as_str()).unwrap_or("unknown");
                let status = record.record.get("status")
                    .and_then(|v| v.as_str()).unwrap_or("unknown");
                    
                (format!("INIT RUNTIME DONE RequestId: {} Type: {} Phase: {} Status: {}", 
                    self.extract_request_id_from_record(record).unwrap_or_else(|| "unknown".to_string()),
                    init_type, phase, status), "INFO".to_string())
            }
            "platform.initReport" => {
                let init_type = record.record.get("initializationType")
                    .and_then(|v| v.as_str()).unwrap_or("unknown");
                let phase = record.record.get("phase")
                    .and_then(|v| v.as_str()).unwrap_or("unknown");
                let metrics = record.record.get("metrics");
                
                let duration_info = if let Some(metrics) = metrics {
                    let duration = metrics.get("durationMs").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    format!(" Duration: {:.2} ms", duration)
                } else {
                    "".to_string()
                };
                
                (format!("INIT REPORT RequestId: {} Type: {} Phase: {}{}", 
                    self.extract_request_id_from_record(record).unwrap_or_else(|| "unknown".to_string()),
                    init_type, phase, duration_info), "INFO".to_string())
            }
            "platform.start" => {
                let request_id = self.extract_request_id_from_record(record).unwrap_or_else(|| "unknown".to_string());
                (format!("START RequestId: {}", request_id), "INFO".to_string())
            }
            "platform.end" => {
                let request_id = self.extract_request_id_from_record(record).unwrap_or_else(|| "unknown".to_string());
                (format!("END RequestId: {}", request_id), "INFO".to_string())
            }
            "platform.runtimeDone" => {
                let request_id = self.extract_request_id_from_record(record).unwrap_or_else(|| "unknown".to_string());
                let status = record.record.get("status")
                    .and_then(|v| v.as_str()).unwrap_or("unknown");
                    
                (format!("RUNTIME DONE RequestId: {} Status: {}", request_id, status), "INFO".to_string())
            }
            _ => {
                // For unknown platform events, create a readable summary
                let request_id = self.extract_request_id_from_record(record).unwrap_or_else(|| "unknown".to_string());
                (format!("PLATFORM EVENT {} RequestId: {} Data: {}", 
                    record.record_type.to_uppercase(), request_id, 
                    serde_json::to_string(&record.record).unwrap_or_else(|_| "{}".to_string())), "INFO".to_string())
            }
        }
    }

    /// Convert platform.report event to AWS CloudWatch REPORT log format
    fn convert_platform_report_to_log_line(&self, record: &TelemetryRecord) -> Option<String> {
        // Record is already a serde_json::Value, no need to parse from string
        let request_id = record.record.get("requestId")?.as_str()?;
        let metrics = record.record.get("metrics")?;
        
        let duration_ms = metrics.get("durationMs")?.as_f64()?;
        let billed_duration_ms = metrics.get("billedDurationMs")?.as_u64()?;
        let memory_size_mb = metrics.get("memorySizeMB")?.as_u64()?;
        let max_memory_used_mb = metrics.get("maxMemoryUsedMB")?.as_u64()?;
        
        // Get init duration if available (for cold starts)
        let init_duration_part = if let Some(init_duration) = metrics.get("initDurationMs").and_then(|v| v.as_f64()) {
            format!("\tInit Duration: {:.2} ms", init_duration)
        } else {
            String::new()
        };
        
        // Format as clean AWS CloudWatch REPORT log line (no extension prefix, no extra escaping)
        // This will be sent to New Relic exactly as-is with literal tabs, matching CloudWatch format
        Some(format!(
            "REPORT RequestId: {}\tDuration: {:.2} ms\tBilled Duration: {} ms\tMemory Size: {} MB\tMax Memory Used: {} MB{}",
            request_id, duration_ms, billed_duration_ms, memory_size_mb, max_memory_used_mb, init_duration_part
        ))
    }

    /// Extract request ID from telemetry record for log correlation
    fn extract_request_id_from_record(&self, record: &TelemetryRecord) -> Option<String> {
        record.record.get("requestId")?.as_str().map(String::from)
    }
    
    /// Extract RequestId from REPORT message content
    /// Example: "REPORT RequestId: ace7dff2-1e6d-4dd4-85b4-09c0b94dd0d1\t..."
    fn extract_request_id_from_message(&self, message: &str) -> Option<String> {
        if message.starts_with("REPORT RequestId: ") {
            // Find the RequestId value between "REPORT RequestId: " and the first tab
            if let Some(start) = message.find("REPORT RequestId: ") {
                let after_prefix = &message[start + "REPORT RequestId: ".len()..];
                if let Some(tab_pos) = after_prefix.find('\t') {
                    return Some(after_prefix[..tab_pos].to_string());
                } else {
                    // No tab found, take until space or end of string
                    let request_id = after_prefix.split_whitespace().next()?;
                    return Some(request_id.to_string());
                }
            }
        }
        None
    }
    
    /// Extract log level from message content for platform events
    /// Returns appropriate log level based on message content
    fn extract_log_level_from_message(&self, message: &str) -> &'static str {
        let message_upper = message.to_uppercase();
        
        // Check for error patterns
        if message_upper.contains("ERROR") || 
           message_upper.contains("FAIL") ||
           message_upper.contains("EXCEPTION") {
            "ERROR"
        } 
        // Check for warning patterns
        else if message_upper.contains("WARN") || 
                message_upper.contains("WARNING") {
            "WARNING"
        }
        // Check for debug patterns
        else if message_upper.contains("DEBUG") {
            "DEBUG"
        }
        // Check for trace patterns
        else if message_upper.contains("TRACE") {
            "TRACE"
        }
        // Default to INFO for platform events (REPORT, INIT, etc.)
        else {
            "INFO"
        }
    }

    /// Updates the invocation context with the latest invoke event details.
    pub fn process_invoke_event(&self, request_id: &str, invoked_function_arn: &str) {
        let mut context = self.invocation_context.lock().unwrap();
        context.request_id = request_id.to_string();
        context.invoked_function_arn = invoked_function_arn.to_string();
    }
    
    /// Simple synchronous send method for platform events - sends as LOGS to log endpoint
    pub async fn send_and_clear_batch_simple(&self) -> Result<()> {
        let batch = {
            let mut batch_guard = self.platform_events_batch.lock().unwrap();
            std::mem::take(&mut *batch_guard)
        };

        if batch.is_empty() {
            return Ok(());
        }

        debug!("Sending {} platform events as logs to New Relic", batch.len());

        let client = Arc::clone(&self.newrelic_client);
        let config = Arc::clone(&self.config);
        let context = self.invocation_context.lock().unwrap().clone();
        
        // Convert platform events to log messages format
        let log_messages: Vec<crate::newrelic::payload::LogMessage> = batch
            .into_iter()
            .filter_map(|event| {
                // Extract the formatted message from the event
                let message = event.get("message")
                    .and_then(|m| m.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| {
                        // Fallback: create a basic platform event message
                        let event_type = event.get("type")
                            .and_then(|t| t.as_str())
                            .unwrap_or("platform.unknown");
                        let request_id = event.get("requestId")
                            .and_then(|r| r.as_str())
                            .unwrap_or("unknown");
                        format!("PLATFORM EVENT {} RequestId: {}", event_type.to_uppercase(), request_id)
                    });

                // Convert timestamp string to i64 (milliseconds since epoch)
                let timestamp_str = event.get("timestamp")
                    .and_then(|t| t.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| {
                        // Fallback: use current timestamp
                        chrono::Utc::now().to_rfc3339()
                    });

                // Parse timestamp string to DateTime and convert to milliseconds
                let timestamp = chrono::DateTime::parse_from_rfc3339(&timestamp_str)
                    .unwrap_or_else(|_| chrono::Utc::now().into())
                    .timestamp_millis();

                // Create attributes map with structured platform event info
                let mut attributes = serde_json::Map::new();
                
                // Get the invocation context for standard attributes
                let context = self.invocation_context.lock().unwrap();
                
                // Extract RequestId from the message itself (for REPORT logs) or use context
                let request_id = if message.starts_with("REPORT RequestId: ") {
                    // Extract RequestId directly from REPORT message
                    self.extract_request_id_from_message(&message)
                        .unwrap_or_else(|| context.request_id.clone())
                } else {
                    context.request_id.clone()
                };
                
                // Only add AWS Lambda attributes if we have a valid request_id
                if !request_id.is_empty() {
                    attributes.insert("aws.lambda_request_id".to_string(), 
                                    serde_json::Value::String(request_id.clone()));
                    attributes.insert("faas.execution".to_string(), 
                                    serde_json::Value::String(request_id.clone()));
                }
                
                // Only add faas.arn if we have a valid invoked_function_arn
                if !context.invoked_function_arn.is_empty() {
                    attributes.insert("faas.arn".to_string(), 
                                    serde_json::Value::String(context.invoked_function_arn.clone()));
                }
                
                // Add log level (use event level or extract from message)
                let log_level = if let Some(level) = event.get("level").and_then(|l| l.as_str()) {
                    level.to_string()
                } else {
                    // Extract log level from message content
                    self.extract_log_level_from_message(&message).to_string()
                };
                attributes.insert("level".to_string(), serde_json::Value::String(log_level));
                
                // Add platform event type
                if let Some(event_type) = event.get("type").and_then(|t| t.as_str()) {
                    attributes.insert("platform.type".to_string(), serde_json::Value::String(event_type.to_string()));
                }
                
                // Add standard New Relic attributes for platform logs
                attributes.insert("newrelic.logPattern".to_string(), "nr.DID_NOT_MATCH".into());
                attributes.insert("newrelic.source".to_string(), "api.platform".into());

                Some(crate::newrelic::payload::LogMessage {
                    timestamp,
                    message,
                    attributes,
                })
            })
            .collect();

        if log_messages.is_empty() {
            return Ok(());
        }

        // Send as logs to New Relic log endpoint (not telemetry endpoint)
        match client.send_logs(&config, log_messages, &context.invoked_function_arn).await {
            Ok(()) => {
                trace!("Successfully sent platform events as logs to New Relic");
                Ok(())
            },
            Err(e) => {
                error!("Failed to send platform events as logs: {}", e);
                Err(std::io::Error::new(std::io::ErrorKind::Other, e))
            }
        }
    }
}

#[async_trait]
impl Flush for PlatformProcessor {
    async fn flush(&self) -> Result<()> {
        self.send_and_clear_batch_simple().await
    }
    
    async fn final_flush(&self) -> Result<()> {
        self.send_and_clear_batch_simple().await
    }
}

