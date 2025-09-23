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
mod agent;
mod credentials;

#[cfg(debug_assertions)]
mod test_telemetry;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{Error, Result},
    sync::{Arc, Mutex},
    time::Duration,
};
use tracing::{info, error, warn};
use crate::{
    context::InvocationContext,
    logs::processor::LogProcessor,
    platform::processor::PlatformProcessor,
    telemetry::listener::setup_telemetry_listener,
    newrelic::{client::NewRelicClient, harvester::Harvester},
    credentials::get_new_relic_license_key,
};
use chrono::Utc;

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

// --- Structs for building the wrapped payload ---
#[derive(Serialize)]
struct WrappedPayload<'a> {
    context: Context<'a>,
    entry: String,
}

#[derive(Serialize)]
struct Context<'a> {
    function_name: &'a str,
    invoked_function_arn: &'a str,
    log_group_name: String,
    log_stream_name: &'a str,
}

#[derive(Serialize)]
struct EntryPayload<'a> {
    #[serde(rename = "logEvents")]
    log_events: Vec<LogEvent<'a>>,
    #[serde(rename = "logGroup")]
    log_group: String,
    #[serde(rename = "logStream")]
    log_stream: &'a str,
    #[serde(rename = "messageType")]
    message_type: &'a str,
    owner: &'a str,
}

#[derive(Serialize)]
struct LogEvent<'a> {
    id: &'a str,
    message: &'a str,
    timestamp: i64,
}


// --- Main Application Logic ---

#[tokio::main]
async fn main() -> Result<()> {
    let start_time = std::time::Instant::now();
    
    // --- 1. Initialize Configuration & Logging ---
    let config = Arc::new(config::init_config().clone());
    info!("Starting extension: {} (config loaded in {:?})", &config.extension.name, start_time.elapsed());

    // --- 2. License Key Extraction Phase (First Priority) ---
    let init_start = std::time::Instant::now();
    
    // Check configuration for credential sources
    let credentials_config = config::Configuration::from(config.as_ref());
    
    // OPTIMIZATION: Determine if we need AWS services at all
    // AWS is needed ONLY when NEW_RELIC_LICENSE_KEY is not set AND we have AWS credential sources
    let needs_aws = credentials_config.license_key.is_empty() && (
        // Explicit AWS credential sources via environment variables
        std::env::var("NEW_RELIC_LICENSE_KEY_SECRET").is_ok() ||
        std::env::var("NEW_RELIC_LICENSE_KEY_SSM_PARAMETER_NAME").is_ok() ||
        // Configured AWS credential sources in config
        !credentials_config.license_key_secret_id.is_empty() ||
        !credentials_config.license_key_ssm_parameter_name.is_empty() ||
        // Default: try AWS with default key name "NEW_RELIC_LICENSE_KEY" when no direct license key
        std::env::var("AWS_LAMBDA_RUNTIME_API").is_ok()
    );
    
        // OPTIMIZATION: Extract license key FIRST before any other initialization
    let license_key = if !credentials_config.license_key.is_empty() {
        info!("Using license key from environment variable NEW_RELIC_LICENSE_KEY (found in {:?}) - AWS services not needed", init_start.elapsed());
        Some(credentials_config.license_key.clone())
    } else if needs_aws {
        info!("No direct license key found, checking AWS credential sources...");
        
        let cred_check_start = std::time::Instant::now();
        match get_new_relic_license_key(&credentials_config).await {
            Ok(key) => {
                info!("Successfully obtained New Relic license key from AWS (took {:?})", cred_check_start.elapsed());
                Some(key)
            }
            Err(e) => {
                warn!("No license key found from AWS sources: {}. Extension will run in no-op mode. (took {:?})", e, cred_check_start.elapsed());
                None
            }
        }
    } else {
        warn!("No license key available and not in AWS Lambda environment. Extension will run in no-op mode. (took {:?})", init_start.elapsed());
        None
    };

    // Update the config with the obtained license key IMMEDIATELY
    let mut updated_config = config.as_ref().clone();
    if let Some(ref key) = license_key {
        updated_config.new_relic.license_key = Some(key.clone());
    }
    let config = Arc::new(updated_config);

    // --- 3. Parallel Initialization Phase (Now with license key ready) ---
    let parallel_start = std::time::Instant::now();
    
    // OPTIMIZATION: If license key is available immediately, start EVERYTHING in parallel
    let (client_result, registration_result) = if license_key.is_some() {
        // License key available immediately - maximum parallelization
        tokio::join!(
            async {
                Client::builder()
                    .timeout(Duration::from_secs(30))
                    .build()
                    .map_err(|e| Error::new(std::io::ErrorKind::Other, e))
            },
            async {
                register(&Client::new()).await
            }
        )
    } else {
        // No license key - still need to register for no-op mode
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| Error::new(std::io::ErrorKind::Other, e))?;
        let registration = register(&client).await?;
        (Ok(client), Ok(registration))
    };
    
    let client = Arc::new(client_result?);
    let registration = registration_result?;

    info!("Parallel initialization completed in {:?}", parallel_start.elapsed());
    info!("Total initialization time: {:?}", start_time.elapsed());

    if !config.new_relic.extension_enabled || license_key.is_none() {
        // --- NO-OP MODE ---
        info!("Extension is in no-op mode because NEW_RELIC_LAMBDA_EXTENSION_ENABLED is set to false. Or NEW_RELIC_LICENSE_KEY is not set.");
        let ext_id = registration.extension_id.clone();
        info!("[No-op] Extension registered with ID: {}. Waiting for SHUTDOWN signal.", ext_id);

        loop {
            match next_event(&client, &ext_id).await {
                Ok(NextEventResponse::Shutdown { shutdown_reason }) => {
                    info!("[No-op] Received SHUTDOWN event: {}. Exiting.", shutdown_reason);
                    break;
                }
                Ok(_) => { /* Ignore INVOKE events in no-op mode */ }
                Err(_) => { /* Ignore errors and continue polling */ }
            }
        }
    } else {
        // --- ACTIVE MODE ---
        let active_mode_start = std::time::Instant::now();
        
        // MAXIMUM PARALLEL OPTIMIZATION: Initialize ALL components at once since license key is ready
        let (invocation_context, agent_payload_buffer, agent_telemetry_rx, newrelic_client) = tokio::join!(
            async { Arc::new(Mutex::new(InvocationContext::default())) },
            async { Arc::new(Mutex::new(Vec::new())) },
            async {
                match agent::ipc::init_telemetry_channel().await {
                    Ok(rx) => {
                        info!(
                            "Agent telemetry channel initialized, listening on pipe: {}",
                            agent::ipc::TELEMETRY_NAMED_PIPE_PATH
                        );
                        Ok(rx)
                    }
                    Err(e) => {
                        error!("FATAL: Failed to initialize agent telemetry pipe: {}. Exiting.", e);
                        Err(e)
                    }
                }
            },
            async {
                // Create NewRelic client in parallel since license key is now available
                Arc::new(NewRelicClient::new(&config))
            }
        );
        
        let agent_telemetry_rx = agent_telemetry_rx?;
        let newrelic_client = newrelic_client;

        // Start the agent payload collector
        agent::processor::start_agent_payload_collector(
            agent_telemetry_rx,
            Arc::clone(&agent_payload_buffer),
        );

        // PARALLEL OPTIMIZATION: Create processors in parallel, then setup telemetry listener
        let (log_processor, platform_processor) = tokio::join!(
            async {
                Arc::new(LogProcessor::new(
                    Arc::clone(&newrelic_client),
                    Arc::clone(&config),
                    Arc::clone(&invocation_context),
                ))
            },
            async {
                Arc::new(PlatformProcessor::new(
                    Arc::clone(&newrelic_client),
                    Arc::clone(&config),
                    Arc::clone(&invocation_context),
                ))
            }
        );
        
        let telemetry_addr = setup_telemetry_listener(log_processor.clone(), platform_processor.clone()).await?;

        let harvester = Harvester::new(
            vec![log_processor.clone(), platform_processor.clone()],
            config.new_relic.harvest_interval,
        );
        let harvester_handle = tokio::spawn(async move {
            harvester.run().await;
        });

        let ext_id = registration.extension_id.clone();
        info!("Extension registered with ID: {} (active mode setup took {:?})", ext_id, active_mode_start.elapsed());
        subscribe_to_telemetry(&client, &ext_id, telemetry_addr.port()).await?;
        info!("Successfully subscribed to telemetry on port {}", telemetry_addr.port());

        // State for tracking the previous invocation
        let mut last_request_id: Option<String> = None;
        let mut last_invoked_arn: Option<String> = None;

        loop {
            let event_result = next_event(&client, &ext_id).await;

            // Process the previous invocation's agent payload now.
            process_agent_payloads(
                &agent_payload_buffer, 
                &last_request_id, 
                &last_invoked_arn, 
                &newrelic_client, 
                &config
            ).await;

            match event_result {
                Ok(NextEventResponse::Invoke { request_id, invoked_function_arn }) => {
                    info!("Received INVOKE event for request ID: {}", request_id);
                    
                    // Update the global context for other processors
                    {
                        let mut context = invocation_context.lock().unwrap();
                        context.request_id = request_id.clone();
                        context.invoked_function_arn = invoked_function_arn.clone();
                    }

                    platform_processor.process_invoke_event(&request_id, &invoked_function_arn);
                    
                    // Wait a bit for agent telemetry to arrive, then send it immediately
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    
                    // Check buffer size for debugging
                    let buffer_size = {
                        let buffer = agent_payload_buffer.lock().unwrap();
                        buffer.len()
                    };
                    info!("[agentsend] Buffer contains {} payloads after invoke, processing immediately", buffer_size);
                    
                    // Process current invocation's agent payload immediately
                    process_agent_payloads(
                        &agent_payload_buffer, 
                        &Some(request_id.clone()), 
                        &Some(invoked_function_arn.clone()), 
                        &newrelic_client, 
                        &config
                    ).await;
                    
                    // Save the context for the *next* loop iteration (for any remaining payloads)
                    last_request_id = Some(request_id);
                    last_invoked_arn = Some(invoked_function_arn);
                }
                Ok(NextEventResponse::Shutdown { shutdown_reason }) => {
                    info!("Received SHUTDOWN event: {}", shutdown_reason);
                    
                    harvester_handle.abort();
                    
                    info!("Performing FINAL flush of all logs and platform events...");
                    if let Err(e) = log_processor.send_and_clear_batch_simple().await {
                        error!("Error during final log flush: {}", e);
                    }
                    if let Err(e) = platform_processor.send_and_clear_batch_simple().await {
                        error!("Error during final platform events flush: {}", e);
                    }

                    // FINAL AGENT FLUSH: Process the very last payload before exiting.
                    process_agent_payloads(
                        &agent_payload_buffer, 
                        &last_request_id, 
                        &last_invoked_arn, 
                        &newrelic_client, 
                        &config
                    ).await;
                    
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    info!("Extension shutdown complete.");
                    break;
                }
                Err(e) => {
                    error!("Error receiving next event: {}. Continuing.", e);
                    continue;
                }
            }
        }
    }

    Ok(())
}

/// Drains the agent payload buffer and sends all pending payloads to New Relic.
async fn process_agent_payloads(
    buffer: &Arc<Mutex<Vec<Vec<u8>>>>,
    request_id_opt: &Option<String>,
    invoked_arn_opt: &Option<String>,
    client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
) {
    let payloads = {
        let mut buf = buffer.lock().unwrap();
        std::mem::take(&mut *buf)
    };

    if payloads.is_empty() {
        return;
    }

    let (Some(request_id), Some(invoked_arn)) = (request_id_opt, invoked_arn_opt) else {
        error!("[agentsend] Payloads exist in buffer but no previous context is available. Discarding {} payloads.", payloads.len());
        return;
    };

    info!("[agentsend] Processing {} buffered agent payloads for request_id: {}", payloads.len(), request_id);

    let function_name = invoked_arn.split(':').last().unwrap_or("");
    let log_group_name = format!("/aws/lambda/{}", function_name);

    for payload_bytes in payloads {
        let Ok(payload_str) = String::from_utf8(payload_bytes) else {
            error!("[agentsend] Failed to decode payload as UTF-8 for request_id: {}", request_id);
            continue;
        };

        let entry_payload = EntryPayload {
            log_events: vec![LogEvent { id: request_id, message: &payload_str, timestamp: Utc::now().timestamp_millis() }],
            log_group: log_group_name.clone(),
            log_stream: "",
            message_type: "",
            owner: "",
        };

        let Ok(entry_string) = serde_json::to_string(&entry_payload) else {
            error!("[agentsend] Failed to serialize entry payload for request_id: {}", request_id);
            continue;
        };

        let wrapped_payload = WrappedPayload {
            context: Context {
                function_name,
                invoked_function_arn: invoked_arn,
                log_group_name: log_group_name.clone(),
                log_stream_name: "newrelic-lambda-extension:2.3.19",
            },
            entry: entry_string,
        };

        if let Ok(final_json) = serde_json::to_string(&wrapped_payload) {
            match client.send_agent_payload(config, &final_json).await {
                Ok(()) => {
                    info!("[agentsend] Successfully sent agent payload to New Relic for request_id: {}", request_id);
                }
                Err(e) => {
                    error!("[agentsend] Error sending agent payload to New Relic for request_id {}: {}", request_id, e);
                }
            }
        } else {
            error!("[agentsend] Failed to serialize final wrapped payload for request_id: {}", request_id);
        }
    }
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
    // Always include platform; optionally include function/extension based on config flags.
    let mut types = vec!["platform".to_string()];
    if config.extension.send_function_logs {
        types.push("function".to_string());
    }
    if config.extension.send_extension_logs {
        types.push("extension".to_string());
    }

    let body = serde_json::json!({
        "schemaVersion": "2022-07-01",
        "destination": {
            "protocol": "HTTP",
            "URI": format!("http://sandbox:{}", port),
        },
        "types": types,
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
                tracing::warn!("Timeout waiting for next Lambda event (this is normal during idle periods)");
            } else {
                tracing::error!("Network error getting next event: {}", e);
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

