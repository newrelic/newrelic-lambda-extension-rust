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


        if let Some(log_message) = self.to_log_message(record) {
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

