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
        // Check if this is a platform.report event that needs conversion to REPORT log format
        if record.record_type == "platform.report" {
            if let Some(log_line) = self.convert_platform_report_to_log_line(&record) {
                // Log the formatted REPORT line for debugging
                debug!("Formatted platform.report as: {}", log_line);
                
                // Convert to New Relic log format and add to batch
                let log_event = serde_json::json!({
                    "timestamp": record.time,
                    "message": log_line,
                    "level": "INFO",
                    "requestId": self.extract_request_id_from_record(&record)
                });
                
                let mut batch = self.platform_events_batch.lock().unwrap();
                batch.push(log_event);
                
                trace!("Added platform report log to batch. Current batch size: {}", batch.len());
                return;
            }
        }
        
        // For other platform events, convert to JSON (fallback)
        let event = serde_json::to_value(&record).unwrap_or_else(|e| {
            error!("Failed to serialize platform record: {}", e);
            serde_json::Value::Null
        });
        let mut batch = self.platform_events_batch.lock().unwrap();
        batch.push(event);
        
        // Only log batch size every 10th addition to reduce noise
        if batch.len() % 10 == 1 {
            trace!("Added platform event to batch. Current batch size: {}", batch.len());
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
        
        // Format as AWS CloudWatch REPORT log line
        Some(format!(
            "REPORT RequestId: {}\tDuration: {:.2} ms\tBilled Duration: {} ms\tMemory Size: {} MB\tMax Memory Used: {} MB{}",
            request_id, duration_ms, billed_duration_ms, memory_size_mb, max_memory_used_mb, init_duration_part
        ))
    }

    /// Extract request ID from telemetry record for log correlation
    fn extract_request_id_from_record(&self, record: &TelemetryRecord) -> Option<String> {
        record.record.get("requestId")?.as_str().map(String::from)
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
                // Extract message and timestamp from the event
                let message = event.get("message")
                    .and_then(|m| m.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| {
                        // Fallback: stringify the entire event if no message field
                        serde_json::to_string(&event).unwrap_or_default()
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

                // Create attributes map with level and request_id
                let mut attributes = serde_json::Map::new();
                
                if let Some(level) = event.get("level").and_then(|l| l.as_str()) {
                    attributes.insert("level".to_string(), serde_json::Value::String(level.to_string()));
                }
                
                if let Some(request_id) = event.get("requestId").and_then(|r| r.as_str()) {
                    attributes.insert("requestId".to_string(), serde_json::Value::String(request_id.to_string()));
                }

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

