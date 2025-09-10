#![deny(clippy::all)]
#![deny(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(missing_debug_implementations)]

mod config;
mod telemetry;
mod logs;
mod platform;
mod newrelic;
mod context;

#[cfg(debug_assertions)]
mod test_telemetry;

use reqwest::Client;
use serde::Deserialize;
use std::{
    collections::HashMap,
    io::{Error, Result},
    sync::{Arc, Mutex},
    time::Duration,
};
use tracing::{info, warn, error};
use crate::{
    context::InvocationContext,
    logs::processor::LogProcessor,
    platform::processor::PlatformProcessor,
    telemetry::listener::setup_telemetry_listener,
    newrelic::{client::NewRelicClient, harvester::Harvester, agent_payload_processor::AgentPayloadProcessor},
};

// --- Extension Constants ---
const EXTENSION_NAME_HEADER: &str = "Lambda-Extension-Name";
const EXTENSION_ID_HEADER: &str = "Lambda-Extension-Identifier";

// --- Structs for API Responses ---

#[derive(Clone, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct RegisterResponse {
    #[serde(skip_deserializing)]
    extension_id: String,
    function_name: String,
    function_version: String,
    handler: String,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "eventType")]
enum NextEventResponse {
    #[serde(rename(deserialize = "INVOKE"))]
    Invoke {
        #[serde(rename(deserialize = "requestId"))]
        request_id: String,
        #[serde(rename(deserialize = "invokedFunctionArn"))]
        invoked_function_arn: String,
    },
    #[serde(rename(deserialize = "SHUTDOWN"))]
    Shutdown {
        #[serde(rename(deserialize = "shutdownReason"))]
        shutdown_reason: String,
    },
}

// --- Main Application Logic ---

#[tokio::main]
async fn main() -> Result<()> {
    // --- 1. Initialize Configuration & Logging ---
    let config = Arc::new(config::init_config().clone());
    info!("Starting extension: {}", &config.extension.name);

    let client = Arc::new(Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| Error::new(std::io::ErrorKind::Other, e))?);

    // Check for test mode
    if std::env::var("NR_EXTENSION_TEST_MODE").unwrap_or_default() == "true" {
        info!("Running in TEST MODE - will simulate telemetry events instead of connecting to Lambda Runtime API");
        
        let newrelic_client = Arc::new(NewRelicClient::new());
        let invocation_context = Arc::new(Mutex::new(InvocationContext::default()));

        let log_processor = Arc::new(LogProcessor::new(
            Arc::clone(&newrelic_client),
            Arc::clone(&config),
            Arc::clone(&invocation_context),
        ));
        let platform_processor = Arc::new(PlatformProcessor::new(
            Arc::clone(&newrelic_client),
            Arc::clone(&config),
            Arc::clone(&invocation_context),
        ));

        // Set up a test function context
        {
            let mut context = invocation_context.lock().unwrap();
            context.request_id = "test-request-12345".to_string();
            context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test-function".to_string();
        }

        #[cfg(debug_assertions)]
        {
            // Simulate some telemetry events
            test_telemetry::simulate_telemetry_processing(&log_processor, &platform_processor).await;
            
            // Send the data immediately
            info!("Sending test data to New Relic...");
            if let Err(e) = log_processor.send_and_clear_batch_simple().await {
                error!("Error sending test logs: {}", e);
            }
            if let Err(e) = platform_processor.send_and_clear_batch_simple().await {
                error!("Error sending test platform events: {}", e);
            }
            
            info!("Test mode completed. Check the logs above for any issues.");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        
        return Ok(());
    }

    if !config.new_relic.extension_enabled {
        // --- NO-OP MODE ---
        info!("Extension is in no-op mode because NEW_RELIC_LAMBDA_EXTENSION_ENABLED is set to false.");
        let response = register(&client).await?;
        let ext_id = response.extension_id.clone();
        info!("[No-op] Extension registered with ID: {}. Waiting for SHUTDOWN signal.", ext_id);

        loop {
            if let NextEventResponse::Shutdown { shutdown_reason } = next_event(&client, &ext_id).await? {
                info!("[No-op] Received SHUTDOWN event: {}. Exiting.", shutdown_reason);
                break;
            }
        }
    } else {
        // --- ACTIVE MODE ---
        let newrelic_client = Arc::new(NewRelicClient::new());
        
        // Test connectivity to New Relic endpoints early
        if let Err(e) = newrelic_client.test_connectivity(&config).await {
            warn!("Connectivity test failed: {}. Extension will continue but may have delivery issues.", e);
        }
        
        let invocation_context = Arc::new(Mutex::new(InvocationContext::default()));

        // Initialize agent payload processor for APM agent data
        info!("Starting agent payload processor pipeline...");
        let agent_processor = AgentPayloadProcessor::new(
            Arc::clone(&newrelic_client),
            Arc::clone(&config),
            Arc::clone(&invocation_context),
        );
        
        // Start the agent payload pipeline in background and keep handle for shutdown coordination
        info!("Spawning agent payload processor background task...");
        let agent_payload_handle = tokio::spawn(async move {
            info!("Agent payload processor background task started");
            if let Err(e) = agent_processor.start_pipeline().await {
                error!("Agent payload pipeline failed: {}", e);
            }
            info!("Agent payload processor background task completed");
        });
        
        info!("Agent payload processor pipeline started successfully");

        let log_processor = Arc::new(LogProcessor::new(
            Arc::clone(&newrelic_client),
            Arc::clone(&config),
            Arc::clone(&invocation_context),
        ));
        let platform_processor = Arc::new(PlatformProcessor::new(
            Arc::clone(&newrelic_client),
            Arc::clone(&config),
            Arc::clone(&invocation_context),
        ));

        let harvester = Harvester::new(
            vec![log_processor.clone(), platform_processor.clone()],
            config.new_relic.harvest_interval,
        );
        let harvester_handle = tokio::spawn(async move {
            harvester.run().await;
        });

        let response = register(&client).await?;
        let ext_id = response.extension_id.clone();
        info!("Extension registered with ID: {}", ext_id);

        let telemetry_addr = setup_telemetry_listener(log_processor.clone(), platform_processor.clone()).await?;
        subscribe_to_telemetry(&client, &ext_id, telemetry_addr.port()).await?;
        info!("Successfully subscribed to telemetry on port {}", telemetry_addr.port());

        // Initialize agent payload processor (but don't start listening yet)
        info!("Agent payload processor initialized successfully");

        loop {
            match next_event(&client, &ext_id).await? {
                NextEventResponse::Invoke { request_id, invoked_function_arn } => {
                    info!("Received INVOKE event for request ID: {}", request_id);
                    platform_processor.process_invoke_event(&request_id, &invoked_function_arn);
                    
                    // Agent payload processing now runs in background pipeline
                    // No need to explicitly process here
                }
                NextEventResponse::Shutdown { shutdown_reason } => {
                    info!("=== SHUTDOWN EVENT RECEIVED ===");
                    info!("Received SHUTDOWN event: {}", shutdown_reason);
                    info!("Starting graceful shutdown process...");

                    // Stop the harvester first
                    info!("Stopping harvester...");
                    harvester_handle.abort();
                    info!("Harvester stopped");
                    
                    // Perform final flush of logs and platform events
                    info!("Performing FINAL flush of all logs and platform events...");
                    if let Err(e) = log_processor.send_and_clear_batch_simple().await {
                        error!("Error during final log flush: {}", e);
                    } else {
                        info!("Final log flush completed successfully");
                    }
                    
                    if let Err(e) = platform_processor.send_and_clear_batch_simple().await {
                        error!("Error during final platform events flush: {}", e);
                    } else {
                        info!("Final platform events flush completed successfully");
                    }
                    
                    // Give agent payload processor time to complete pending requests
                    // AWS Lambda gives us ~2 seconds after function completion
                    info!("Processing pending requests before shutdown...");
                    
                    // Process any pending requests that failed during normal operation
                    let pending_result = newrelic_client.process_pending_requests(&config, Duration::from_millis(1500)).await;
                    match pending_result {
                        Ok(_) => {
                            info!("All pending requests processed successfully");
                        }
                        Err(e) => {
                            error!("Failed to process some pending requests: {}", e);
                        }
                    }
                    
                    // Now abort the agent payload processor
                    info!("Aborting agent payload processor...");
                    agent_payload_handle.abort();
                    info!("Agent payload processor aborted");
                    
                    info!("=== EXTENSION SHUTDOWN COMPLETE ===");
                    break;
                }
            }
        }
    }

    Ok(())
}

// --- Helper Functions ---

/// Registers the extension with the Lambda Runtime API.
async fn register(client: &Client) -> Result<RegisterResponse> {
    let config = config::get_config();
    let url = format!("http://{}/2020-01-01/extension/register", &config.aws.runtime_api);

    let mut map = HashMap::new();
    map.insert("events", vec!["INVOKE", "SHUTDOWN"]);

    let resp = client
        .post(&url)
        .header(EXTENSION_NAME_HEADER, &config.extension.name)
        .json(&map)
        .send()
        .await
        .map_err(|e| Error::new(std::io::ErrorKind::Other, e))?;

    if !resp.status().is_success() {
        let err_msg = format!("Failed to register extension: {}", resp.status());
        return Err(Error::new(std::io::ErrorKind::Other, err_msg));
    }

    let extension_id = resp
        .headers()
        .get(EXTENSION_ID_HEADER)
        .ok_or_else(|| Error::new(std::io::ErrorKind::NotFound, "Extension ID header not found"))?
        .to_str()
        .map_err(|e| Error::new(std::io::ErrorKind::InvalidData, e))?
        .to_string();

    let mut register_response: RegisterResponse = resp
        .json()
        .await
        .map_err(|e| Error::new(std::io::ErrorKind::InvalidData, e))?;

    register_response.extension_id = extension_id;
    Ok(register_response)
}

/// Subscribes to the Lambda Telemetry API.
async fn subscribe_to_telemetry(client: &Client, ext_id: &str, port: u16) -> Result<()> {
    let config = config::get_config();
    let url = format!("http://{}/2022-07-01/telemetry", &config.aws.runtime_api);

    let body = serde_json::json!({
        "schemaVersion": "2022-07-01",
        "destination": {
            "protocol": "HTTP",
            "URI": format!("http://sandbox:{}", port),
        },
        "types": ["platform", "function", "extension"],
        "buffering": {
            "maxItems": config.extension.max_batch_items,
            "maxBytes": config.extension.max_batch_size,
            "timeoutMs": config.extension.telemetry_timeout,
        }
    });

    let resp = client
        .put(&url)
        .header(EXTENSION_ID_HEADER, ext_id)
        .json(&body)
        .send()
        .await
        .map_err(|e| Error::new(std::io::ErrorKind::Other, e))?;

    if !resp.status().is_success() {
        let err_msg = format!("Failed to subscribe to telemetry: {}", resp.status());
        return Err(Error::new(std::io::ErrorKind::Other, err_msg));
    }

    Ok(())
}

/// Fetches the next event from the Lambda Runtime API.
async fn next_event(client: &Client, ext_id: &str) -> Result<NextEventResponse> {
    let config = config::get_config();
    let url = format!("http://{}/2020-01-01/extension/event/next", &config.aws.runtime_api);

    let resp = client
        .get(&url)
        .header(EXTENSION_ID_HEADER, ext_id)
        .timeout(Duration::from_secs(300)) // Increase timeout to 5 minutes
        .send()
        .await
        .map_err(|e| {
            // Log more details about timeout errors
            if e.is_timeout() {
                warn!("⏰ Timeout waiting for next Lambda event (this is normal during idle periods)");
            } else {
                error!("🌐 Network error getting next event: {}", e);
            }
            Error::new(std::io::ErrorKind::Other, e)
        })?;

    if !resp.status().is_success() {
        let err_msg = format!("Failed to get next event: {}", resp.status());
        return Err(Error::new(std::io::ErrorKind::Other, err_msg));
    }

    let event = resp
        .json()
        .await
        .map_err(|e| Error::new(std::io::ErrorKind::InvalidData, e))?;

    Ok(event)
}

