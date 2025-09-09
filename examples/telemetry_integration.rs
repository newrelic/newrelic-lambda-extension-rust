//! Example integration of RealTimeTelemetryProcessor
//! 
//! This shows how to use the new telemetry processor in your Lambda extension.

use std::sync::Arc;
use tokio::sync::Mutex;
use crate::{
    config::ExtensionConfig,
    context::InvocationContext,
    newrelic::{client::NewRelicClient, telemetry_channel::RealTimeTelemetryProcessor},
};

/// Example of how to integrate the RealTimeTelemetryProcessor into your main application
pub async fn example_integration() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize your components (these would come from your main function)
    let config = Arc::new(ExtensionConfig::default()); // Your actual config
    let newrelic_client = Arc::new(NewRelicClient::new());
    let invocation_context = Arc::new(Mutex::new(InvocationContext::default()));

    // Create the real-time telemetry processor
    let mut telemetry_processor = RealTimeTelemetryProcessor::new(
        Arc::clone(&newrelic_client),
        Arc::clone(&config),
        Arc::clone(&invocation_context),
    );

    println!("Starting real-time telemetry processor...");
    
    // Start the processor - this will:
    // 1. Create a Unix domain socket at /tmp/newrelic-telemetry.sock
    // 2. Listen for connections from the Lambda agent
    // 3. Process incoming telemetry data immediately
    // 4. Send each log event to New Relic as soon as it's received
    telemetry_processor.start().await?;

    println!("Telemetry processor started successfully!");
    println!("The processor is now listening for agent data and will:");
    println!("  - Accept connections from Lambda agent on /tmp/newrelic-telemetry.sock");
    println!("  - Parse agent telemetry payloads in real-time");
    println!("  - Decompress NR_LAMBDA_MONITORING data automatically");
    println!("  - Send each log event to New Relic immediately (no batching)");

    // Your main application logic continues here...
    // The telemetry processor runs in the background

    // When your extension is shutting down:
    println!("Stopping telemetry processor...");
    telemetry_processor.stop().await;
    
    Ok(())
}

/// Example of the agent telemetry payload format that the processor handles
pub fn example_agent_payload() {
    let example_payload = r#"{
    "context": {
        "function_name": "my-lambda-function",
        "invoked_function_arn": "arn:aws:lambda:us-east-1:123456789012:function:my-lambda-function",
        "log_group_name": "/aws/lambda/my-lambda-function",
        "log_stream_name": "2024/01/15/[$LATEST]abcd1234efgh5678"
    },
    "entry": "{\"logEvents\":[{\"id\":\"12345\",\"message\":\"[1,\\\"NR_LAMBDA_MONITORING\\\",\\\"H4sIAAAAAAAAA...\\\"]}\",\"timestamp\":1705123456789}],\"logGroup\":\"/aws/lambda/my-lambda-function\",\"logStream\":\"2024/01/15/[$LATEST]abcd1234efgh5678\",\"messageType\":\"DATA\",\"owner\":\"123456789012\"}"
}"#;
    
    println!("Example agent payload format:");
    println!("{}", example_payload);
    println!();
    println!("The processor will:");
    println!("1. Parse the JSON payload");
    println!("2. Extract the 'entry' field containing log events");
    println!("3. Parse each log event");
    println!("4. Check if the message contains NR_LAMBDA_MONITORING data");
    println!("5. If found, decode base64 and decompress gzip data");
    println!("6. Send the processed log to New Relic immediately");
}

/// Example environment variable configuration
pub fn example_environment_setup() {
    println!("Environment variable configuration:");
    println!();
    println!("Enable/disable telemetry processing:");
    println!("  NR_TELEMETRY_ENABLED=true   # Enable real-time processing (default)");
    println!("  NR_TELEMETRY_ENABLED=false  # Disable telemetry processing");
    println!();
    println!("New Relic configuration (standard extension variables):");
    println!("  NEW_RELIC_LICENSE_KEY=your_license_key");
    println!("  NEW_RELIC_LOG_ENDPOINT=https://log-api.newrelic.com/log/v1");
    println!("  NEW_RELIC_TELEMETRY_ENDPOINT=https://metric-api.newrelic.com/metric/v1");
    println!();
    println!("Lambda agent will send telemetry to the socket at:");
    println!("  /tmp/newrelic-telemetry.sock");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_telemetry_processor_creation() {
        let config = Arc::new(ExtensionConfig::default());
        let client = Arc::new(NewRelicClient::new());
        let context = Arc::new(Mutex::new(InvocationContext::default()));
        
        let processor = RealTimeTelemetryProcessor::new(config, client, context);
        assert!(!processor.is_running());
    }
}
