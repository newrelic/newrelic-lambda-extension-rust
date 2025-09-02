use crate::{
    config::ExtensionConfig,
    logs::processor::LogProcessor,
    newrelic::{client::NewRelicClient, harvester},
    telemetry::listener::TelemetryRecord,
};
use std::sync::{Arc, Mutex};
use tokio::runtime::Handle;
use tracing::{info, warn};

/// The PlatformProcessor is responsible for handling all platform-related telemetry events.
#[derive(Debug)]
pub struct PlatformProcessor {
    log_processor: Arc<LogProcessor>,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
    invoked_function_arn: Mutex<String>,
}

impl PlatformProcessor {
    /// Creates a new PlatformProcessor.
    pub fn new(
        log_processor: Arc<LogProcessor>,
        newrelic_client: Arc<NewRelicClient>,
        config: Arc<ExtensionConfig>,
    ) -> Self {
        Self {
            log_processor,
            newrelic_client,
            config,
            invoked_function_arn: Mutex::new(String::new()),
        }
    }

    /// Processes a single platform telemetry record.
    pub fn process_record(&self, record: TelemetryRecord) {
        let payload_str = serde_json::to_string_pretty(&record.record)
            .unwrap_or_else(|_| "Failed to serialize payload".to_string());

        match record.record_type.as_str() {
            "platform.start" => {
                if let Some(arn) = record.record.get("invokedFunctionArn").and_then(|v| v.as_str()) {
                    if let Ok(mut arn_guard) = self.invoked_function_arn.lock() {
                        *arn_guard = arn.to_string();
                    }
                }
                info!("[PlatformProcessor] Processed Invoke Start - Payload: {}", payload_str);
            }
            "platform.runtimeDone" => {
                // This is the end of an invocation, so do a final harvest of all remaining logs.
                self.final_harvest();
                info!("[PlatformProcessor] Processed Invoke Runtime Done - Payload: {}", payload_str);
            }
            "platform.initStart" => info!("[PlatformProcessor] Processed Init Start - Payload: {}", payload_str),
            "platform.initRuntimeDone" => info!("[PlatformProcessor] Processed Init Runtime Done - Payload: {}", payload_str),
            "platform.initReport" => info!("[PlatformProcessor] Processed Init Report - Payload: {}", payload_str),
            "platform.report" => info!("[PlatformProcessor] Processed Invoke Report - Payload: {}", payload_str),
            _ => warn!("[PlatformProcessor] Received Unknown Record Type: {} - Payload: {}", record.record_type, payload_str),
        }
    }

    /// Checks if a batch of logs is ready and sends it.
    pub fn harvest(&self) {
        if let Some(batch) = self.log_processor.harvest() {
            let handle = Handle::current();
            let client = Arc::clone(&self.newrelic_client);
            let config = Arc::clone(&self.config);
            let invoked_function_arn = self.invoked_function_arn.lock().unwrap().clone();
            
            handle.spawn(async move {
                harvester::send_log_batch(&config, batch, &client, &invoked_function_arn).await;
            });
        }
    }

    /// Perform a final harvest before the extension shuts down.
    pub async fn final_harvest(&self) {
        let batch = self.log_processor.harvest_all();
        if !batch.is_empty() {
            let arn = self.invoked_function_arn.lock().unwrap().clone();
            harvester::send_log_batch(&self.config, batch, &self.newrelic_client, &arn).await;
        }
    }
}

