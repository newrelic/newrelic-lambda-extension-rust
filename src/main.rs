//! New Relic Lambda Extension
//! 
//! This is the main binary for the New Relic Lambda Extension that collects
//! telemetry data (logs, metrics, traces) from AWS Lambda functions and
//! forwards them to New Relic's APIs.

// Use jemalloc as the global allocator for better memory management
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

mod telemetry;
mod config;
mod event_bus;

use lambda_extension::{service_fn, Extension, LambdaEvent, NextEvent, Error as LambdaError};
use tracing_subscriber;
use hyper::{Method, Request, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use http_body_util::BodyExt;
use std::sync::Arc;
use tokio::sync::OnceCell;
use telemetry::TelemetryServer;
use config::{init_config, get_config};
use event_bus::{EventBus, Event};

/// Global telemetry initialization tracker
static TELEMETRY_INITIALIZED: OnceCell<()> = OnceCell::const_new();

/// Verify telemetry server is ready to accept connections
// async fn verify_telemetry_server_ready() -> bool {
//     let client = Client::builder(TokioExecutor::new()).build_http::<String>();
    
//     for attempt in 1..=10 {
//         match client.get("http://127.0.0.1:4243/health".parse().unwrap()).await {
//             Ok(response) if response.status().is_success() => {
//                 tracing::info!("✅ [TelemetryServer] Health check passed on attempt {}", attempt);
//                 return true;
//             }
//             Ok(response) => {
//                 tracing::debug!("⚠️ [TelemetryServer] Health check failed with status: {}", response.status());
//             }
//             Err(e) => {
//                 tracing::debug!("⚠️ [TelemetryServer] Health check attempt {} failed: {}", attempt, e);
//             }
//         }
        
//         // Wait 50ms between attempts
//         tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
//     }
    
//     tracing::error!("❌ [TelemetryServer] Health check failed after 10 attempts");
//     false
// }

/// Initialize telemetry with actual extension ID from registration
async fn initialize_telemetry_with_extension_id(extension_id: String) {
    let _ = TELEMETRY_INITIALIZED.get_or_init(|| async {
        tracing::info!("🔧 Starting telemetry subscription with extension ID: {}", &extension_id[..8.min(extension_id.len())]);
        
        // Retry logic with intelligent backoff
        let max_retries = 5;
        let base_delay = 200; // Start with 200ms
        
        for attempt in 1..=max_retries {
            // Calculate delay with exponential backoff but cap at 2 seconds
            let delay = std::cmp::min(base_delay * (1 << (attempt - 1)), 2000);
            
            tracing::info!("🔄 Telemetry subscription attempt {}/{} (delay: {}ms)", attempt, max_retries, delay);
            
            // Try to subscribe
            match subscribe_to_lambda_telemetry_api_with_id(&extension_id).await {
                Ok(()) => {
                    tracing::info!("✅ Telemetry subscription successful on attempt {}", attempt);
                    return;
                }
                Err(e) => {
                    if attempt == max_retries {
                        tracing::error!("❌ Telemetry subscription failed after {} attempts: {}", max_retries, e);
                        tracing::error!("🚨 Extension will continue without telemetry - this significantly reduces functionality!");
                        return;
                    } else {
                        tracing::warn!("⚠️ Telemetry subscription attempt {} failed: {}", attempt, e);
                        
                        // Wait before retry
                        tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
                    }
                }
            }
        }
    }).await;
}
/// Subscribe to AWS Lambda Telemetry API with extension ID
async fn subscribe_to_lambda_telemetry_api_with_id(
    extension_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create a simple HTTP client for telemetry subscription
    let client = Client::builder(TokioExecutor::new())
        .build_http::<String>();
    let config = get_config();
    
    tracing::info!("[NewRelicExtension] Subscribing to telemetry with extension ID: {}", &extension_id[..8.min(extension_id.len())]);
    
    let telemetry_api_url = config.telemetry_subscription_url();
    let destination_uri = config.telemetry_destination_uri();
    
    let subscription_request = serde_json::json!({
        "schemaVersion": "2022-07-01",
        "types": ["platform", "function", "extension"],
        "buffering": {
            "maxBytes": config.extension.max_batch_size,
            "maxItems": config.extension.max_batch_items,
            "timeoutMs": config.extension.telemetry_timeout
        },
        "destination": {
            "protocol": "HTTP",
            "URI": destination_uri
        }
    });
    
    tracing::info!("[NewRelicExtension] Telemetry subscription to: {}", telemetry_api_url);
    tracing::info!("[NewRelicExtension] Destination: {}", destination_uri);
    
    let body = serde_json::to_string(&subscription_request)?;
    let uri: Uri = telemetry_api_url.parse()?;
    
    let request = Request::builder()
        .method(Method::PUT)
        .uri(uri)
        .header("Lambda-Extension-Identifier", extension_id)
        .header("Content-Type", "application/json")
        .body(body)?;
    
    let response = client.request(request).await?;
    let status = response.status();
    
    if status.is_success() {
        tracing::info!("✅ [NewRelicExtension] Successfully subscribed to Lambda Telemetry API");
        tracing::info!("📡 [NewRelicExtension] Will receive events: platform, function, extension logs");
        tracing::info!("🎯 [NewRelicExtension] Telemetry events will be sent to: {}", destination_uri);
        Ok(())
    } else {
        let body_bytes = response.into_body().collect().await?.to_bytes();
        let error_body = String::from_utf8_lossy(&body_bytes);
        tracing::error!("⚠️ [NewRelicExtension] Failed to subscribe to telemetry. Status: {}, Body: {}", status, error_body);
        Err(format!("Telemetry subscription failed with status: {}", status).into())
    }
}

async fn newrelic_handler(event: LambdaEvent) -> Result<(), LambdaError> {
    match event.next {
        NextEvent::Invoke(invoke_event) => {
            tracing::info!("🚀 Lambda invocation started: {}", invoke_event.request_id);
            tracing::info!("✅ Lambda invocation completed: {}", invoke_event.request_id);
        }
        NextEvent::Shutdown(shutdown_event) => {
            tracing::info!("🛑 Lambda shutdown requested: {:?}", shutdown_event.shutdown_reason);
            
            // Send shutdown signal to event bus
            // Note: We would need access to the event bus sender here for clean shutdown
            // For now, just give some time for cleanup
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .compact()
        .init();

    tracing::info!("🚀 Starting New Relic Lambda Extension with jemalloc allocator");

    // Initialize configuration from environment variables
    let config = init_config();
    
    // Check if extension is enabled
    if !config.new_relic.extension_enabled {
        tracing::warn!("⚠️ New Relic Lambda Extension is disabled via NEW_RELIC_LAMBDA_EXTENSION_ENABLED");
        return Ok(());
    }

    // Create and start the event bus
    let config_arc = Arc::new(config.clone());
    let event_bus = EventBus::new(Arc::clone(&config_arc));
    let event_bus_sender = event_bus.get_sender();
    
    // Start the event bus processing loop in background
    let event_bus_handle = tokio::spawn(async move {
        tracing::info!("🚌 [EventBus] Starting event bus processing");
        event_bus.run().await;
    });

    // Start the telemetry HTTP server with event bus integration
    let telemetry_server = Arc::new(TelemetryServer::with_event_bus(event_bus_sender.clone()));
    let server_addr = config.telemetry_socket_addr();
    
    let server_clone = Arc::clone(&telemetry_server);
    let telemetry_handle = tokio::spawn(async move {
        if let Err(e) = server_clone.start_server(server_addr).await {
            tracing::error!("[NewRelicExtension] Failed to start telemetry server: {}", e);
        }
    });

    // CRITICAL: Wait for telemetry server to be fully ready
    // tracing::info!("⏳ Waiting for telemetry server to be ready...");
    // if !verify_telemetry_server_ready().await {
    //     return Err("Failed to start telemetry server".into());
    // }
    // tracing::info!("✅ Telemetry server is ready and accepting connections");

    tracing::info!("📝 Registering extension with AWS Lambda Extensions API");
    
    // Step 1: Create and register the extension to get the extension ID
    let extension = Extension::new()
        .with_extension_name(&config.extension.name)
        .with_events(&["INVOKE", "SHUTDOWN"])
        .with_events_processor(service_fn(newrelic_handler));

    // Register the extension and get the RegisteredExtension with extension_id
    let registered_extension = extension.register().await.map_err(|e| {
        tracing::error!("❌ Failed to register extension: {}", e);
        e
    })?;

    let extension_id = registered_extension.extension_id.clone();
    tracing::info!("✅ Extension registered successfully with ID: {}", &extension_id[..8.min(extension_id.len())]);
    tracing::info!("📋 Extension details - Function: {}, Version: {}", 
        registered_extension.function_name,
        registered_extension.function_version
    );

    // Step 2: Initialize telemetry with the actual extension ID
    tracing::info!("🔌 Initializing telemetry subscription with registered extension ID");
    
    // CRITICAL: Wait for telemetry subscription to complete BEFORE starting event loop
    initialize_telemetry_with_extension_id(extension_id.clone()).await;
    tracing::info!("✅ Telemetry initialization completed - ready for events");

    tracing::info!("🔄 Starting Lambda extension event loop");

    // Step 3: Run the registered extension event loop
    let extension_result = registered_extension.run().await;

    // Handle extension completion
    match extension_result {
        Ok(()) => tracing::info!("✅ Extension completed successfully"),
        Err(e) => tracing::error!("❌ Extension error: {}", e),
    }

    // Send shutdown signal to event bus
    if let Err(e) = event_bus_sender.send(Event::Shutdown).await {
        tracing::warn!("⚠️ Failed to send shutdown signal to event bus: {}", e);
    }

    // Wait for event bus and telemetry server to shutdown
    tracing::info!("⏳ Waiting for event bus shutdown...");
    if let Err(e) = tokio::time::timeout(std::time::Duration::from_secs(5), event_bus_handle).await {
        tracing::warn!("⚠️ Event bus shutdown timeout: {}", e);
    }

    tracing::info!("⏳ Waiting for telemetry server shutdown...");
    telemetry_handle.abort(); // Force shutdown of telemetry server
    
    tracing::info!("👋 New Relic Lambda Extension shutdown complete");
    Ok(())
}
