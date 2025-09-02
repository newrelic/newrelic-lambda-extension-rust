#![deny(clippy::all)]
#![deny(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(missing_debug_implementations)]

mod telemetry;

use reqwest::Client;
use serde::Deserialize;
use std::{
    collections::HashMap,
    env,
    io::{Error, Result},
    time::Duration,
};
use tracing::{info};
use tracing_subscriber::EnvFilter;
use crate::telemetry::listener::setup_telemetry_listener;

// --- AWS Lambda Runtime Environment Variables ---
const LAMBDA_RUNTIME_API: &str = "AWS_LAMBDA_RUNTIME_API";

// --- Extension Constants ---
const EXTENSION_NAME: &str = "newrelic-lambda-extension";
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
    // --- 1. Initialize Logging ---
    let env_filter = "info";
    let subscriber = tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::try_new(env_filter).unwrap())
        .with_level(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    info!("Starting extension: {}", EXTENSION_NAME);

    // --- 2. Create HTTP Client ---
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| Error::new(std::io::ErrorKind::Other, e))?;

    // --- 3. Register the Extension ---
    let response = register(&client).await?;
    let ext_id = response.extension_id.clone();
    info!("Extension registered with ID: {}", ext_id);

    // --- 4. Set up Telemetry Subscription ---
    let telemetry_addr = setup_telemetry_listener().await?;
    subscribe_to_telemetry(&client, &ext_id, telemetry_addr.port()).await?;
    info!(
        "Successfully subscribed to telemetry on port {}",
        telemetry_addr.port()
    );

    // --- 5. Start the Main Event Loop ---
    loop {
        let event = next_event(&client, &ext_id).await?;
        match event {
            NextEventResponse::Invoke { request_id } => {
                info!("Received INVOKE event for request ID: {}", request_id);
            }
            NextEventResponse::Shutdown { shutdown_reason } => {
                info!("Received SHUTDOWN event: {}", shutdown_reason);
                // Add a brief delay to allow the telemetry listener to process final events.
                // This is crucial for newer runtimes like AL2023.
                tokio::time::sleep(Duration::from_millis(200)).await;
                info!("Shutting down extension.");
                break;
            }
        }
    }

    Ok(())
}

// --- Helper Functions ---

/// Registers the extension with the Lambda Runtime API.
async fn register(client: &Client) -> Result<RegisterResponse> {
    let base_url = env::var(LAMBDA_RUNTIME_API)
        .map_err(|e| Error::new(std::io::ErrorKind::NotFound, e))?;
    let url = format!("http://{base_url}/2020-01-01/extension/register");

    let mut map = HashMap::new();
    map.insert("events", vec!["INVOKE", "SHUTDOWN"]);

    let resp = client
        .post(&url)
        .header(EXTENSION_NAME_HEADER, EXTENSION_NAME)
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
    let base_url = env::var(LAMBDA_RUNTIME_API)
        .map_err(|e| Error::new(std::io::ErrorKind::NotFound, e))?;
    let url = format!("http://{base_url}/2022-07-01/telemetry");

    let body = serde_json::json!({
        "schemaVersion": "2022-07-01",
        "destination": {
            "protocol": "HTTP",
            "URI": format!("http://sandbox:{port}"),
        },
        "types": ["platform", "function", "extension"],
        "buffering": {
            "maxItems": 1000,
            "maxBytes": 262144,
            "timeoutMs": 100,
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
    let base_url = env::var(LAMBDA_RUNTIME_API)
        .map_err(|e| Error::new(std::io::ErrorKind::NotFound, e))?;
    let url = format!("http://{base_url}/2020-01-01/extension/event/next");

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

