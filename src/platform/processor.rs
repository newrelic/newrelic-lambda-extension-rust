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
        
        // Convert the record to JSON and add to our batch
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

    /// Updates the invocation context with the latest invoke event details.
    pub fn process_invoke_event(&self, request_id: &str, invoked_function_arn: &str) {
        let mut context = self.invocation_context.lock().unwrap();
        context.request_id = request_id.to_string();
        context.invoked_function_arn = invoked_function_arn.to_string();
    }
    
    /// Simple synchronous send method for platform events
    pub async fn send_and_clear_batch_simple(&self) -> Result<()> {
        let batch = {
            let mut batch_guard = self.platform_events_batch.lock().unwrap();
            std::mem::take(&mut *batch_guard)
        };

        if batch.is_empty() {
            trace!("No platform events to send");
            return Ok(());
        }

        debug!("Sending {} platform events to New Relic", batch.len());

        let client = Arc::clone(&self.newrelic_client);
        let config = Arc::clone(&self.config);
        let context = self.invocation_context.lock().unwrap().clone();
        
        let payload = serde_json::json!([{
            "common": {
                "attributes": {
                    "plugin.type": "telemetry-api",
                    "faas.arn": context.invoked_function_arn,
                    "faas.name": &config.aws.function_name,
                }
            },
            "telemetry": batch,
        }]);
        
        // Send directly without spawning
        match client.send_platform_events(&config, payload).await {
            Ok(()) => {
                trace!("Successfully sent platform events to New Relic");
                Ok(())
            },
            Err(e) => {
                error!("Failed to send platform events: {}", e);
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

