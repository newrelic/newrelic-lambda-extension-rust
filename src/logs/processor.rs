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

/// The LogProcessor is responsible for handling and transforming function and extension logs.
#[derive(Debug, Clone)]
pub struct LogProcessor {
    log_batch: Arc<Mutex<Vec<payload::LogMessage>>>,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
    invocation_context: Arc<Mutex<InvocationContext>>,
}

impl LogProcessor {
    /// Creates a new LogProcessor.
    pub fn new(
        newrelic_client: Arc<NewRelicClient>,
        config: Arc<ExtensionConfig>,
        invocation_context: Arc<Mutex<InvocationContext>>,
    ) -> Self {
        Self {
            log_batch: Arc::new(Mutex::new(Vec::new())),
            newrelic_client,
            config,
            invocation_context,
        }
    }

    /// Processes a single log telemetry record, adding it to the batch if valid.
    pub fn process_record(&self, record: TelemetryRecord) {
        let message_str = record.record.to_string();
        
        // Avoid recursive logging from our own processors
        if message_str.contains("[LogProcessor]") || message_str.contains("[PlatformProcessor]") {
            return;
        }

        tracing::debug!("Processing log record: type={}, time={}, message_preview={}...", 
            record.record_type, 
            record.time,
            if message_str.len() > 100 { &message_str[..100] } else { &message_str }
        );

        if let Some(log_message) = self.to_log_message(record) {
            let mut batch = self.log_batch.lock().unwrap();
            batch.push(log_message);
            let batch_size = batch.len();
            tracing::debug!("Added log to batch. Current batch size: {}", batch_size);
            
            // Send immediately if we have 3+ logs (simple batch condition)
            if batch_size >= 3 {
                drop(batch); // Release the lock before async operation
                tracing::info!("🚀 Batch size reached 3, sending logs immediately!");
                let processor = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = processor.send_and_clear_batch_simple().await {
                        tracing::error!("Failed to send logs: {}", e);
                    }
                });
            }
        } else {
            tracing::warn!("Failed to convert telemetry record to log message");
        }
    }

    /// Converts a TelemetryRecord into a LogMessage, if applicable.
    fn to_log_message(&self, record: TelemetryRecord) -> Option<payload::LogMessage> {
        let timestamp = record.time.timestamp_millis();
        let message = record.record.to_string();
        let mut attributes = serde_json::Map::new();
        if let Some(request_id) = record.record.get("requestId").and_then(|v| v.as_str()) {
            attributes.insert("request_id".to_string(), request_id.into());
        }

        Some(payload::LogMessage {
            timestamp,
            message,
            attributes,
        })
    }

    /// Check if we should send logs immediately (simple batching)
    pub fn should_send_immediately(&self) -> bool {
        let batch = self.log_batch.lock().unwrap();
        batch.len() >= 5 // Send every 5 logs
    }

    /// Get current batch size
    pub fn get_batch_size(&self) -> usize {
        let batch = self.log_batch.lock().unwrap();
        batch.len()
    }

    /// Simple synchronous send method - just send the data without complex async handling
    pub async fn send_and_clear_batch_simple(&self) -> Result<()> {
        let batch = {
            let mut batch_guard = self.log_batch.lock().unwrap();
            std::mem::take(&mut *batch_guard)
        };
        
        if batch.is_empty() {
            tracing::debug!("[LogProcessor] No logs to send");
            return Ok(());
        }

        tracing::info!("[LogProcessor] 🚀 Sending {} logs to New Relic NOW", batch.len());
        
        let client = Arc::clone(&self.newrelic_client);
        let config = Arc::clone(&self.config);
        let context = self.invocation_context.lock().unwrap().clone();
        
        // Send directly without spawning - simpler and more reliable
        match client.send_logs(&config, batch, &context.invoked_function_arn).await {
            Ok(()) => {
                tracing::info!("[LogProcessor] ✅ Successfully sent logs to New Relic");
                Ok(())
            },
            Err(e) => {
                tracing::error!("[LogProcessor] ❌ Failed to send logs: {}", e);
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

