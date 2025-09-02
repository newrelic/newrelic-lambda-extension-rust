#![deny(clippy::all)]
#![deny(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(missing_debug_implementations)]

mod config;
mod telemetry;
mod logs;
mod platform;
mod newrelic;

use reqwest::Client;
use serde::Deserialize;
use std::{
    collections::HashMap,
    io::{Error, Result},
    time::Duration,
    sync::Arc,
};
use tracing::info;
use tracing_subscriber::EnvFilter;
use crate::logs::processor::LogProcessor;
use crate::platform::processor::PlatformProcessor;
use crate::telemetry::listener::setup_telemetry_listener;
use crate::newrelic::client::NewRelicClient;

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
    let config = config::init_config();

    // Initialize logging AFTER config is loaded
    let env_filter = EnvFilter::try_new(&config.new_relic.extension_log_level)
        .unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_level(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");


    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| Error::new(std::io::ErrorKind::Other, e))?;

    if !config.new_relic.extension_enabled {
        // --- NO-OP MODE ---
        info!("Extension is in no-op mode because NEW_RELIC_LAMBDA_EXTENSION_ENABLED is set to false.");
        
        let response = register(&client).await?;
        let ext_id = response.extension_id.clone();
        info!("[No-op] Extension registered with ID: {}. Waiting for SHUTDOWN signal.", ext_id);

        loop {
            let event = next_event(&client, &ext_id).await?;
            if let NextEventResponse::Shutdown { shutdown_reason } = event {
                info!("[No-op] Received SHUTDOWN event: {}. Exiting.", shutdown_reason);
                break;
            }
        }
    } else {
        // --- ACTIVE MODE ---
        info!("Starting extension: {}", &config.extension.name);

        // Create the New Relic client and processors
        let newrelic_client = Arc::new(NewRelicClient::new());
        let log_processor = Arc::new(LogProcessor::new());
        let platform_processor = Arc::new(PlatformProcessor::new(
            Arc::clone(&log_processor),
            Arc::clone(&newrelic_client),
            Arc::new(config.clone()),
        ));

        let response = register(&client).await?;
        let ext_id = response.extension_id.clone();
        info!("Extension registered with ID: {}", ext_id);

        let telemetry_addr = setup_telemetry_listener(log_processor, Arc::clone(&platform_processor)).await?;
        subscribe_to_telemetry(&client, &ext_id, telemetry_addr.port()).await?;
        info!(
            "Successfully subscribed to telemetry on port {}",
            telemetry_addr.port()
        );

        loop {
            match next_event(&client, &ext_id).await? {
                NextEventResponse::Invoke { request_id } => {
                    info!("Received INVOKE event for request ID: {}", request_id);
                }
                NextEventResponse::Shutdown { shutdown_reason } => {
                    info!("Received SHUTDOWN event: {}", shutdown_reason);
                    // Final harvest before shutting down
                    platform_processor.final_harvest().await;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    info!("Shutting down extension.");
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
        .send()
        .await
        .map_err(|e| Error::new(std::io::ErrorKind::Other, e))?;

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

