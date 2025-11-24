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

   
    pub fn process_record(&self, record: TelemetryRecord) {
        let (message, level) = self.create_platform_log_message(&record);
        
        // Store platform metrics for error synthesis (timeout/fault detection)
        if record.record_type == "platform.report" {
            if let Some(request_id) = record.record.get("requestId").and_then(|v| v.as_str()) {
                if let Some(metrics) = record.record.get("metrics") {
                    let duration_ms = metrics.get("durationMs").and_then(|v| v.as_f64());
                    let memory_size_mb = metrics.get("memorySizeMB").and_then(|v| v.as_u64());
                    let max_memory_used_mb = metrics.get("maxMemoryUsedMB").and_then(|v| v.as_u64());
                    let billed_duration_ms = metrics.get("billedDurationMs").and_then(|v| v.as_u64());
                    
                    crate::error_synthesis::store_platform_metrics(
                        request_id.to_string(),
                        duration_ms,
                        memory_size_mb,
                        max_memory_used_mb,
                        billed_duration_ms,
                    );
                }
            }
        }
        
        // Check for platform errors (from platform events with error/failure/timeout status)
        // These events have errorType field: platform.initReport, platform.initRuntimeDone,
        // platform.runtimeDone, platform.restoreRuntimeDone, platform.restoreReport
        self.check_and_send_platform_errors(&record);
        
        let log_event = serde_json::json!({
            "timestamp": record.time,
            "message": message,
            "level": level,
            "type": record.record_type,
            "requestId": self.extract_request_id_from_record(&record)
        });
        
        let mut batch = self.platform_events_batch.lock().unwrap();
        batch.push(log_event);
        
        if batch.len() % 10 == 1 {
            trace!("Added platform event to batch. Current batch size: {}", batch.len());
        }
    }

   
    fn create_platform_log_message(&self, record: &TelemetryRecord) -> (String, String) {
        match record.record_type.as_str() {
            "platform.report" => {
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
                let request_id = self.extract_request_id_from_record(record).unwrap_or_else(|| "unknown".to_string());
                (format!("PLATFORM EVENT {} RequestId: {} Data: {}", 
                    record.record_type.to_uppercase(), request_id, 
                    serde_json::to_string(&record.record).unwrap_or_else(|_| "{}".to_string())), "INFO".to_string())
            }
        }
    }

   
    pub fn convert_platform_report_to_log_line(&self, record: &TelemetryRecord) -> Option<String> {
        let request_id = record.record.get("requestId")?.as_str()?;
        let metrics = record.record.get("metrics")?;
        
        let duration_ms = metrics.get("durationMs")?.as_f64()?;
        let billed_duration_ms = metrics.get("billedDurationMs")?.as_u64()?;
        let memory_size_mb = metrics.get("memorySizeMB")?.as_u64()?;
        let max_memory_used_mb = metrics.get("maxMemoryUsedMB")?.as_u64()?;
        
        let init_duration_part = if let Some(init_duration) = metrics.get("initDurationMs").and_then(|v| v.as_f64()) {
            format!("\tInit Duration: {:.2} ms", init_duration)
        } else {
            String::new()
        };
        
        Some(format!(
            "REPORT RequestId: {}\tDuration: {:.2} ms\tBilled Duration: {} ms\tMemory Size: {} MB\tMax Memory Used: {} MB{}",
            request_id, duration_ms, billed_duration_ms, memory_size_mb, max_memory_used_mb, init_duration_part
        ))
    }

   
    fn extract_request_id_from_record(&self, record: &TelemetryRecord) -> Option<String> {
        record.record.get("requestId")?.as_str().map(String::from)
    }
    
    /// Check platform events for errors and send to telemetry endpoint
    /// Platform events that can have errors: platform.initReport, platform.initRuntimeDone,
    /// platform.runtimeDone, platform.restoreRuntimeDone, platform.restoreReport
    fn check_and_send_platform_errors(&self, record: &TelemetryRecord) {
        // Check if this event type can have errors
        let event_type = record.record_type.as_str();
        let can_have_errors = matches!(
            event_type,
            "platform.initReport" | "platform.initRuntimeDone" | "platform.runtimeDone" |
            "platform.restoreRuntimeDone" | "platform.restoreReport"
        );
        
        if !can_have_errors {
            return;
        }
        
        // Check status field - should be error, failure, or timeout
        let status = record.record.get("status").and_then(|v| v.as_str());
        let has_error = matches!(status, Some("error") | Some("failure") | Some("timeout"));
        
        if !has_error {
            return;
        }
        
        // Extract error information - only if errorType field exists
        let error_type = record.record.get("errorType")
            .and_then(|v| v.as_str());
        
        // For init errors, requestId might not exist - use instanceId or generate one
        let request_id = if let Some(rid) = self.extract_request_id_from_record(record) {
            if !rid.is_empty() {
                rid
            } else {
                // No requestId in event, try instanceId for init events
                record.record.get("instanceId")
                    .and_then(|v| v.as_str())
                    .map(|s| format!("init-{}", &s[..8.min(s.len())]))
                    .unwrap_or_else(|| {
                        // Last resort: use context or generate ID
                        let context = self.invocation_context.lock().unwrap();
                        if !context.request_id.is_empty() && context.request_id != "temp" {
                            context.request_id.clone()
                        } else {
                            format!("init-{}", chrono::Utc::now().timestamp())
                        }
                    })
            }
        } else {
            // No requestId in event, try instanceId for init events
            record.record.get("instanceId")
                .and_then(|v| v.as_str())
                .map(|s| format!("init-{}", &s[..8.min(s.len())]))
                .unwrap_or_else(|| {
                    // Last resort: use context or generate ID
                    let context = self.invocation_context.lock().unwrap();
                    if !context.request_id.is_empty() && context.request_id != "temp" {
                        context.request_id.clone()
                    } else {
                        format!("init-{}", chrono::Utc::now().timestamp())
                    }
                })
        };
        
        // Build error message based on event type and available information
        // Format: YYYY-MM-DDTHH:MM:SSZ request-id Message
        let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ");
        
        let error_message = match event_type {
            "platform.initReport" | "platform.initRuntimeDone" => {
                let phase = record.record.get("phase")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let function_name = &self.config.aws.function_name;
                
                if let Some(err_type) = error_type {
                    format!(
                        "{} {} Function: {} Initialization {} failed: {}",
                        timestamp, request_id, function_name, phase, err_type
                    )
                } else {
                    format!(
                        "{} {} Function: {} Initialization {} failed",
                        timestamp, request_id, function_name, phase
                    )
                }
            }
            "platform.runtimeDone" => {
                let metrics = record.record.get("metrics");
                let duration_seconds = if let Some(m) = metrics {
                    m.get("durationMs").and_then(|v| v.as_f64()).map(|ms| ms / 1000.0)
                } else {
                    None
                };
                
                // For timeout status, format like CloudWatch timeout message
                if status == Some("timeout") {
                    if let Some(duration) = duration_seconds {
                        format!(
                            "{} {} Task timed out after {:.2} seconds",
                            timestamp, request_id, duration
                        )
                    } else {
                        format!(
                            "{} {} Task timed out",
                            timestamp, request_id
                        )
                    }
                } else if let Some(err_type) = error_type {
                    if let Some(duration) = duration_seconds {
                        format!(
                            "{} {} Function invocation failed after {:.2} seconds: {}",
                            timestamp, request_id, duration, err_type
                        )
                    } else {
                        format!(
                            "{} {} Function invocation failed: {}",
                            timestamp, request_id, err_type
                        )
                    }
                } else {
                    if let Some(duration) = duration_seconds {
                        format!(
                            "{} {} Function invocation failed after {:.2} seconds",
                            timestamp, request_id, duration
                        )
                    } else {
                        format!(
                            "{} {} Function invocation failed",
                            timestamp, request_id
                        )
                    }
                }
            }
            "platform.restoreRuntimeDone" | "platform.restoreReport" => {
                if let Some(err_type) = error_type {
                    format!(
                        "{} {} Runtime restore failed: {}",
                        timestamp, request_id, err_type
                    )
                } else {
                    format!(
                        "{} {} Runtime restore failed",
                        timestamp, request_id
                    )
                }
            }
            _ => {
                if let Some(err_type) = error_type {
                    format!(
                        "{} {} Platform error ({}): {}",
                        timestamp, request_id, event_type, err_type
                    )
                } else {
                    format!(
                        "{} {} Platform error ({})",
                        timestamp, request_id, event_type
                    )
                }
            }
        };
        
        // Get invoked function ARN from context
        let invoked_function_arn = {
            let context = self.invocation_context.lock().unwrap();
            context.invoked_function_arn.clone()
        };
        
        // Map platform error to appropriate Lambda error type
        let lambda_error_type = match status {
            Some("timeout") => "LambdaTimeout",
            Some("failure") => "LambdaFatalError",
            Some("error") => "LambdaError",
            _ => "LambdaError",
        };
        
        // Send error to telemetry endpoint synchronously to ensure it's sent during the current invoke
        let client = Arc::clone(&self.newrelic_client);
        let config = Arc::clone(&self.config);
        let error_msg = error_message.clone();
        let req_id = request_id.clone();
        let func_arn = invoked_function_arn.clone();
        let err_type = lambda_error_type.to_string();

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.block_on(async move {
                crate::error_synthesis::send_lambda_error(
                    &error_msg,
                    &req_id,
                    &func_arn,
                    &err_type,
                    &client,
                    &config,
                )
                .await;
            });
        }
        
        debug!(
            "Detected platform error in {}: {} - will send to telemetry endpoint", 
            event_type, 
            error_type.unwrap_or("no errorType field")
        );
    }
    
   
   
    fn extract_request_id_from_message(&self, message: &str) -> Option<String> {
        if message.starts_with("REPORT RequestId: ") {
            if let Some(start) = message.find("REPORT RequestId: ") {
                let after_prefix = &message[start + "REPORT RequestId: ".len()..];
                if let Some(tab_pos) = after_prefix.find('\t') {
                    return Some(after_prefix[..tab_pos].to_string());
                } else {
                    let request_id = after_prefix.split_whitespace().next()?;
                    return Some(request_id.to_string());
                }
            }
        }
        None
    }
    
   
   
    fn extract_log_level_from_message(&self, message: &str) -> &'static str {
        let message_upper = message.to_uppercase();
        
        if message_upper.contains("ERROR") || 
           message_upper.contains("FAIL") ||
           message_upper.contains("EXCEPTION") {
            "ERROR"
        } 
        else if message_upper.contains("WARN") || 
                message_upper.contains("WARNING") {
            "WARNING"
        }
        else if message_upper.contains("DEBUG") {
            "DEBUG"
        }
        else if message_upper.contains("TRACE") {
            "TRACE"
        }
        else {
            "INFO"
        }
    }

   
    pub fn process_invoke_event(&self, request_id: &str, invoked_function_arn: &str) {
        let mut context = self.invocation_context.lock().unwrap();
        context.request_id = request_id.to_string();
        context.invoked_function_arn = invoked_function_arn.to_string();
    }
    
   
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
        
        let log_messages: Vec<crate::newrelic::payload::LogMessage> = batch
            .into_iter()
            .filter_map(|event| {
                let message = event.get("message")
                    .and_then(|m| m.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| {
                        let event_type = event.get("type")
                            .and_then(|t| t.as_str())
                            .unwrap_or("platform.unknown");
                        let request_id = event.get("requestId")
                            .and_then(|r| r.as_str())
                            .unwrap_or("unknown");
                        format!("PLATFORM EVENT {} RequestId: {}", event_type.to_uppercase(), request_id)
                    });

                let timestamp_str = event.get("timestamp")
                    .and_then(|t| t.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| {
                        chrono::Utc::now().to_rfc3339()
                    });

                let timestamp = chrono::DateTime::parse_from_rfc3339(&timestamp_str)
                    .unwrap_or_else(|_| chrono::Utc::now().into())
                    .timestamp_millis();

                let mut attributes = serde_json::Map::new();
                
                let context = self.invocation_context.lock().unwrap();
                
                let request_id = if message.starts_with("REPORT RequestId: ") {
                    self.extract_request_id_from_message(&message)
                        .unwrap_or_else(|| context.request_id.clone())
                } else {
                    context.request_id.clone()
                };
                
                if !request_id.is_empty() {
                    attributes.insert("aws.lambda_request_id".to_string(), 
                                    serde_json::Value::String(request_id.clone()));
                    attributes.insert("faas.execution".to_string(), 
                                    serde_json::Value::String(request_id.clone()));
                }
                
                if !context.invoked_function_arn.is_empty() {
                    attributes.insert("faas.arn".to_string(), 
                                    serde_json::Value::String(context.invoked_function_arn.clone()));
                } else if let Some(constructed_arn) = config.aws.construct_function_arn() {
                    attributes.insert("faas.arn".to_string(), 
                                    serde_json::Value::String(constructed_arn.clone()));
                    debug!("Used constructed faas.arn from registration details: {}", constructed_arn);
                } else {
                    debug!("Cannot construct faas.arn - missing registration details for platform event: {}", 
                          message.chars().take(100).collect::<String>());
                }
                
                let log_level = if let Some(level) = event.get("level").and_then(|l| l.as_str()) {
                    level.to_string()
                } else {
                    self.extract_log_level_from_message(&message).to_string()
                };
                attributes.insert("level".to_string(), serde_json::Value::String(log_level));
                
                if let Some(event_type) = event.get("type").and_then(|t| t.as_str()) {
                    attributes.insert("platform.type".to_string(), serde_json::Value::String(event_type.to_string()));
                }
                
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
        let batch_size = {
            if let Ok(batch) = self.platform_events_batch.lock() {
                batch.len()
            } else { 0 }
        };
        
        if batch_size > 0 {
            trace!("Harvester flush: {} platform events accumulated (will be sent by coordinated flush)", batch_size);
        }
        
        Ok(())
    }
}

