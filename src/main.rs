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
mod trace;

#[cfg(debug_assertions)]
mod test_telemetry;

use std::{
    env,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use tokio::sync::mpsc;
use once_cell::sync::Lazy;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, trace, warn};
use reqwest::Client;

use crate::{
    context::InvocationContext,
    logs::processor::LogProcessor,
    platform::processor::PlatformProcessor,
    telemetry::listener::setup_telemetry_listener,
    newrelic::{
        client::NewRelicClient, 
        harvester::Harvester,
        flush::Flush,
    },
    credentials::get_new_relic_license_key,
};

const EXTENSION_NAME: &str = env!("CARGO_PKG_NAME");
const EXTENSION_VERSION: &str = env!("CARGO_PKG_VERSION");

// --- Extension Constants ---
const EXTENSION_NAME_HEADER: &str = "Lambda-Extension-Name";
const EXTENSION_ID_HEADER: &str = "Lambda-Extension-Identifier";

// --- Global state for warm starts (safer approach) ---
static INVOKED_FUNCTION_ARN: OnceLock<Mutex<Option<String>>> = OnceLock::new();
static LAST_REQUEST_ID: OnceLock<Mutex<Option<String>>> = OnceLock::new();

// --- CONCURRENT REQUEST HANDLING ---
// Per-request contexts to handle concurrent Lambda invocations safely
static REQUEST_CONTEXTS: Lazy<Arc<Mutex<HashMap<String, Arc<Mutex<InvocationContext>>>>>> = 
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));
static REQUEST_AGENT_BUFFERS: Lazy<Arc<Mutex<HashMap<String, Arc<Mutex<Vec<Vec<u8>>>>>>>> = 
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));



// --- Fallback global components for backward compatibility ---
static INVOCATION_CONTEXT: Lazy<Arc<Mutex<InvocationContext>>> = 
    Lazy::new(|| Arc::new(Mutex::new(InvocationContext::default())));
static AGENT_PAYLOAD_BUFFER: Lazy<Arc<Mutex<Vec<Vec<u8>>>>> = 
    Lazy::new(|| Arc::new(Mutex::new(Vec::new())));

// --- Global components for immediate agent payload processing ---
static GLOBAL_CONFIG: OnceLock<Arc<config::ExtensionConfig>> = OnceLock::new();
static GLOBAL_NEWRELIC_CLIENT: OnceLock<Arc<NewRelicClient>> = OnceLock::new();
static GLOBAL_LOG_PROCESSOR: OnceLock<Arc<LogProcessor>> = OnceLock::new();

// Global coordination channels per request for agent payload processing
static PAYLOAD_COORDINATION: Lazy<Arc<Mutex<HashMap<String, mpsc::UnboundedSender<()>>>>> = 
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

// Structure to hold all initialized components for warm starts
#[derive(Debug)]
struct ExtensionComponents {
    client: Arc<Client>,
    extension_id: String,
    log_processor: Arc<LogProcessor>,
    platform_processor: Arc<PlatformProcessor>,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<config::ExtensionConfig>,
    runtime_done_rx: mpsc::UnboundedReceiver<()>,
    harvester_handle: tokio::task::JoinHandle<()>,
}

// --- Structs for API Responses ---

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ExtensionRegistrationResponse {
    #[serde(rename = "functionName")]
    function_name: String,
    #[serde(rename = "functionVersion")]
    function_version: String,
    #[serde(rename = "handler")]
    handler: String,
    #[serde(rename = "accountId", default)]
    account_id: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "eventType")]
enum LambdaRuntimeEvent {
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

/// Main entry point with CRITICAL panic safety to prevent Lambda crashes
#[tokio::main]
async fn main() -> std::io::Result<()> {
    // CRITICAL: Set up global panic hook to prevent ANY panic from crashing Lambda
    std::panic::set_hook(Box::new(|panic_info| {
        let location = if let Some(location) = panic_info.location() {
            format!("{}:{}", location.file(), location.line())
        } else {
            "unknown location".to_string()
        };
        
        let message = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            *s
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            s.as_str()
        } else {
            "unknown panic message"
        };
        
        // Use eprintln! since logging might not be available during panic
        eprintln!("[NR_EXT] ERROR Extension panic caught (Lambda will continue): {}", message);
        eprintln!("[NR_EXT] ERROR Panic location: {}", location);
        
        // Don't re-panic - just log and continue
    }));
    
    // CRITICAL: Wrap everything in error handling to prevent Lambda crashes
    match run_extension().await {
        Ok(_) => {
            eprintln!("[NR_EXT] INFO Extension completed successfully");
            Ok(())
        }
        Err(e) => {
            // Log error but don't propagate - this prevents Lambda from crashing
            eprintln!("[NR_EXT] ERROR Extension failed but continuing gracefully: {}", e);
            eprintln!("[NR_EXT] WARN Lambda function will continue without New Relic monitoring");
            
            // Return Ok to prevent Lambda crash - this is critical!
            Ok(())
        }
    }
}

/// Safely spawn a background task with error protection
/// This prevents errors in background tasks from propagating up and provides logging
fn spawn_safe_task<F>(task_name: &str, future: F) -> tokio::task::JoinHandle<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let task_name = task_name.to_string();
    tokio::spawn(async move {
        // The panic hook will catch any panics in this task
        // We just need to ensure proper error logging
        future.await;
        debug!("Background task '{}' completed", task_name);
    })
}

/// Run the extension in true no-op mode - follows Extension API lifecycle but does nothing
/// Registers with Extension API and waits for INVOKE/SHUTDOWN events but processes nothing
async fn run_noop_extension() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Extension running in NO-OP mode - no telemetry will be collected");
    eprintln!("[NR_EXT] INFO Extension running in NO-OP mode - Lambda function will continue normally");
    
    // Even in no-op mode, we need to properly register with the Lambda Extensions API
    // and wait for INVOKE/SHUTDOWN events to follow the correct lifecycle
    let (client, extension_id, _registration) = initialize_lambda_runtime_client_and_register().await?;
    
    info!("Extension registered in no-op mode with ID: {}", extension_id);
    
    // Follow proper Extension API lifecycle - wait for events but do nothing with them
    loop {
        match fetch_next_lambda_runtime_event(&client, &extension_id).await {
            Ok(LambdaRuntimeEvent::Invoke { request_id, invoked_function_arn: _ }) => {
                debug!("No-op mode: Received INVOKE event for request {}, doing nothing", request_id);
                // Do absolutely nothing - no telemetry, no processing, no network calls
            }
            Ok(LambdaRuntimeEvent::Shutdown { shutdown_reason }) => {
                info!("No-op mode: Extension shutting down: {}", shutdown_reason);
                break;
            }
            Err(e) => {
                error!("Error receiving next event in no-op mode: {:?}. Continuing.", e);
                continue;
            }
        }
    }
    
    Ok(())
}

/// Main extension logic following correct Lambda extension lifecycle
async fn run_extension() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let extension_startup_time = std::time::Instant::now();
    
    info!("=== COLD START: Initializing version {} of the New Relic Lambda Extension ===", EXTENSION_VERSION);
    
    // PHASE 1: ONE-TIME COLD START INITIALIZATION (with true no-op fallback)
    let extension_components = match perform_one_time_initialization().await {
        Ok(components) => {
            info!("Extension initialization successful");
            Some(components)
        }
        Err(e) => {
            error!("Extension initialization failed, entering true no-op mode: {}", e);
            eprintln!("[NR_EXT] ERROR Initialization failed, entering true no-op mode (Lambda function will continue normally): {}", e);
            
            // Enter true no-op mode - do absolutely nothing
            run_noop_extension().await?;
            return Ok(()); // This return will never be reached since noop runs forever
        }
    };

    let extension_components = extension_components.unwrap(); // Safe because we handled the None case above

    info!("Cold start initialization complete (duration: {:?})", extension_startup_time.elapsed());
    
    // PHASE 2: INFINITE EVENT LOOP (handles both first invoke and all warm starts)
    let (total_events_processed, harvester_handle) = run_infinite_event_loop(extension_components).await;
    
    // PHASE 3: CLEANUP ON SHUTDOWN (only happens once per container lifecycle)
    perform_extension_shutdown_cleanup(total_events_processed, harvester_handle, extension_startup_time).await;    Ok(())
}

/// Perform all one-time initialization - called only once per container
async fn perform_one_time_initialization() -> Result<ExtensionComponents, Box<dyn std::error::Error + Send + Sync>> {
    // Initialize config first (this sets up logging)
    let config = config::init_config().clone();
    let config = Arc::new(config);
    
    // --- PHASE 1: PARALLEL CRITICAL OPERATIONS (maximum performance) ---
    
    // 1. Early exit for disabled extension (no-op mode)
    if !config.new_relic.extension_enabled {
        info!("Extension telemetry processing disabled - entering no-op mode");
        let (client, extension_id, _registration) = initialize_lambda_runtime_client_and_register().await?;
        
        // Return no-op components
        return Ok(ExtensionComponents {
            client,
            extension_id,
            log_processor: Arc::new(LogProcessor::new_noop()),
            platform_processor: Arc::new(PlatformProcessor::new_noop()),
            newrelic_client: Arc::new(NewRelicClient::new_noop()),
            config: config.clone(),
            runtime_done_rx: mpsc::unbounded_channel::<()>().1,
            harvester_handle: tokio::spawn(async {}),
        });
    }

    // 2. PARALLEL CRITICAL OPERATIONS: License key validation + Extension registration
    //    Both are required regardless of no-op mode (extension needs SHUTDOWN events)
    info!("Starting parallel license key validation and extension registration...");
    let (license_key_result, registration_result) = tokio::join!(
        resolve_license_key_with_aws_fallback(&config),
        initialize_lambda_runtime_client_and_register()
    );

    let license_key_option = license_key_result?;
    let (client, extension_id, registration) = registration_result?;
    
    let Some(license_key) = license_key_option else {
        warn!("No license key available after checking all sources. Running in no-op mode.");
        
        // Update config with registration details even in no-op mode
        let mut updated_config = (*config).clone();
        updated_config.aws.update_from_registration(
            registration.function_name,
            registration.function_version,
            registration.account_id,
        );
        let config = Arc::new(updated_config);
        
        // Return no-op components (extension already registered for SHUTDOWN events)
        return Ok(ExtensionComponents {
            client,
            extension_id,
            log_processor: Arc::new(LogProcessor::new_noop()),
            platform_processor: Arc::new(PlatformProcessor::new_noop()),
            newrelic_client: Arc::new(NewRelicClient::new_noop()),
            config: config.clone(),
            runtime_done_rx: mpsc::unbounded_channel::<()>().1,
            harvester_handle: tokio::spawn(async {}),
        });
    };

    // 3. Update config with validated license key
    let mut updated_config = (*config).clone();
    updated_config.new_relic.license_key = Some(license_key);
    let config = Arc::new(updated_config);
    
    info!("License key validated and extension registered - proceeding with full initialization");

    info!("NEW_RELIC_COLLECT_TRACE_ID setting: {}", config.new_relic.collect_trace_id);

    // --- PHASE 2: PARALLEL INITIALIZATION (using already registered extension) ---
    
    // Update config with registration details first (needed for NewRelic client)
    let mut updated_config = (*config).clone();
    updated_config.aws.update_from_registration(
        registration.function_name,
        registration.function_version,
        registration.account_id,
    );
    let config = Arc::new(updated_config);
    
    // 4. MAXIMALLY PARALLEL: Initialize remaining core components simultaneously 
    let (
        agent_telemetry_rx_result,
        newrelic_client,
        runtime_done_channels
    ) = tokio::join!(
        initialize_agent_telemetry_ipc_channel(),
        async { Arc::new(NewRelicClient::new(&config)) },
        async { mpsc::unbounded_channel::<()>() }
    );

    let agent_telemetry_rx = agent_telemetry_rx_result?;
    let (runtime_done_tx, runtime_done_rx) = runtime_done_channels;

    info!("Extension components initialized - ID: {} (license key pre-validated)", extension_id);

    // Start agent payload collector (background task)
    start_agent_payload_collector_background_task(agent_telemetry_rx);

    // Initialize processors with updated config
    let (log_processor, platform_processor) = tokio::join!(
        async {
            Arc::new(LogProcessor::new(
                Arc::clone(&newrelic_client),
                Arc::clone(&config),
                Arc::clone(&INVOCATION_CONTEXT),
            ))
        },
        async {
            Arc::new(PlatformProcessor::new(
                Arc::clone(&newrelic_client),
                Arc::clone(&config),
                Arc::clone(&INVOCATION_CONTEXT),
            ))
        }
    );
    
    // Setup telemetry listener with the processors
    let telemetry_listener_address = setup_telemetry_listener(
        log_processor.clone(),
        platform_processor.clone(),
        Some(runtime_done_tx)
    ).await?;
    
    // 9. Subscribe to Lambda Telemetry API
    subscribe_to_lambda_telemetry_api(&client, &extension_id, telemetry_listener_address.port()).await?;

    // 10. Start harvester background task
    let harvester_handle = start_harvester_background_task(
        vec![
            log_processor.clone() as Arc<dyn Flush>,
            platform_processor.clone() as Arc<dyn Flush>
        ],
        config.new_relic.harvest_interval,
    );

    // Initialize global components for immediate agent payload processing
    initialize_global_components(
        config.clone(),
        newrelic_client.clone(),
        log_processor.clone(),
    );

    // 11. Return initialized components directly
    Ok(ExtensionComponents {
        client,
        extension_id,
        log_processor,
        platform_processor,
        newrelic_client,
        config,
        runtime_done_rx,
        harvester_handle,
    })
}

/// Resolve license key with AWS fallback if needed
async fn resolve_license_key_with_aws_fallback(config: &Arc<config::ExtensionConfig>) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    // Fix: Dereference the Arc to get &ExtensionConfig
    let credentials_config = config::Configuration::from(config.as_ref());
    
    // Check if AWS services are needed for license key resolution
    let aws_services_required = credentials_config.license_key.is_empty() && (
        std::env::var("NEW_RELIC_LICENSE_KEY_SECRET").is_ok() ||
        std::env::var("NEW_RELIC_LICENSE_KEY_SSM_PARAMETER_NAME").is_ok() ||
        !credentials_config.license_key_secret_id.is_empty() ||
        !credentials_config.license_key_ssm_parameter_name.is_empty() ||
        std::env::var("AWS_LAMBDA_RUNTIME_API").is_ok()
    );

    if !credentials_config.license_key.is_empty() {
        Ok(Some(credentials_config.license_key.clone()))
    } else if aws_services_required {
        match get_new_relic_license_key(&credentials_config).await {
            Ok(key) => {
                debug!("Successfully obtained New Relic license key from AWS");
                Ok(Some(key))
            }
            Err(e) => {
                warn!("No license key found from AWS sources: {}. Extension will run in no-op mode.", e);
                Ok(None)
            }
        }
    } else {
        warn!("No license key available and not in AWS Lambda environment. Extension will run in no-op mode.");
        Ok(None)
    }
}

/// Initialize HTTP client with appropriate timeout
async fn initialize_http_client_with_timeout() -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
    Ok(Client::builder()
        // Remove global timeout - let individual requests set their own timeouts
        // Extension event polling needs 5+ minute timeouts, but other requests need shorter ones
        .pool_idle_timeout(Duration::from_secs(90)) // Keep connections alive longer for Lambda runtime API
        .pool_max_idle_per_host(2) // Limit idle connections
        .tcp_keepalive(Duration::from_secs(60)) // Enable TCP keepalive
        .build()?)
}

/// Initialize Lambda runtime client and register extension
async fn initialize_lambda_runtime_client_and_register() -> Result<(Arc<Client>, String, ExtensionRegistrationResponse), Box<dyn std::error::Error + Send + Sync>> {
    let client = Arc::new(initialize_http_client_with_timeout().await?);
    let (registration, extension_id) = register_extension_with_lambda_runtime(&client).await?;
    Ok((client, extension_id, registration))
}

/// Initialize agent telemetry IPC channel
async fn initialize_agent_telemetry_ipc_channel() -> Result<mpsc::Receiver<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
    match agent::ipc::init_telemetry_channel().await {
        Ok(rx) => {
            info!("Agent telemetry channel initialized, listening on pipe: {}", agent::ipc::TELEMETRY_NAMED_PIPE_PATH);
            Ok(rx)
        }
        Err(e) => {
            error!("FATAL: Failed to initialize agent telemetry pipe: {}. Exiting.", e);
            Err(Box::new(e))
        }
    }
}

/// Start agent payload collector as background task with concurrent request handling
fn start_agent_payload_collector_background_task(agent_telemetry_rx: mpsc::Receiver<Vec<u8>>) {
    start_concurrent_agent_payload_collector(agent_telemetry_rx);
}

/// Channel-based agent payload collector with immediate processing and notification
fn start_concurrent_agent_payload_collector(mut receiver: mpsc::Receiver<Vec<u8>>) {
    tokio::spawn(async move {
        info!("Agent payload collector started - continuously listening for agent payloads");
        let mut payload_count = 0;

        while let Some(payload_bytes) = receiver.recv().await {
            payload_count += 1;
            
            info!("Received agent payload #{} ({} bytes) - processing immediately", payload_count, payload_bytes.len());
            
            if payload_count <= 5 {
                debug!("Agent Payload preview: {:?}", 
                       String::from_utf8_lossy(&payload_bytes[..std::cmp::min(100, payload_bytes.len())]));
            }
            
            // Route payload (will try immediate processing first, store if it fails)
            route_payload_to_request_buffer(payload_bytes).await;
        }

        warn!("Agent payload collector channel closed. No more agent payloads will be received");
    });
}

/// Route agent payload to the correct per-request buffer or global buffer
async fn route_payload_to_request_buffer(payload_bytes: Vec<u8>) {
    // Strategy: Process immediately if possible, otherwise store for later processing
    let current_request_id = {
        if let Ok(contexts) = REQUEST_CONTEXTS.lock() {
            // Get the most recent request (assumes HashMap iteration order reflects insertion order in recent Rust)
            contexts.keys().last().cloned()
        } else {
            None
        }
    };
    
    if let Some(request_id) = current_request_id {
        // Try immediate processing first
        let processed_immediately = process_received_agent_payload_with_notification(payload_bytes.clone()).await;
        
        if !processed_immediately {
            // Only store if immediate processing failed
            let request_buffer = get_request_agent_buffer(&request_id);
            let buffer_result = request_buffer.lock();
            
            match buffer_result {
                Ok(mut buffer) => {
                    buffer.push(payload_bytes);
                    info!("Stored agent payload in request buffer for {} (buffer size now {})", 
                         request_id, buffer.len());
                }
                Err(e) => {
                    error!("Failed to lock request buffer for {}: {}, using global buffer", request_id, e);
                    route_to_global_buffer(payload_bytes);
                }
            }
        }
    } else {
        // No active requests, use global buffer
        info!("No active requests found, routing payload to global buffer");
        route_to_global_buffer(payload_bytes);
    }
}

/// Route payload to global buffer (fallback)
fn route_to_global_buffer(payload_bytes: Vec<u8>) {
    if let Ok(mut buffer) = AGENT_PAYLOAD_BUFFER.lock() {
        buffer.push(payload_bytes);
        info!("Routed payload to global buffer (buffer size now {})", buffer.len());
    } else {
        error!("Failed to lock global agent payload buffer - payload lost!");
    }
}

// --- Helper functions for global component access ---
fn get_global_config() -> Option<Arc<config::ExtensionConfig>> {
    GLOBAL_CONFIG.get().cloned()
}

fn get_global_newrelic_client() -> Option<Arc<NewRelicClient>> {
    GLOBAL_NEWRELIC_CLIENT.get().cloned()
}

fn get_global_log_processor() -> Option<Arc<LogProcessor>> {
    GLOBAL_LOG_PROCESSOR.get().cloned()
}

fn initialize_global_components(
    config: Arc<config::ExtensionConfig>,
    newrelic_client: Arc<NewRelicClient>,
    log_processor: Arc<LogProcessor>,
) {
    let _ = GLOBAL_CONFIG.set(config);
    let _ = GLOBAL_NEWRELIC_CLIENT.set(newrelic_client);
    let _ = GLOBAL_LOG_PROCESSOR.set(log_processor);
}



fn get_current_invoked_function_arn() -> Option<String> {
    if let Some(arn_mutex) = INVOKED_FUNCTION_ARN.get() {
        if let Ok(arn_guard) = arn_mutex.lock() {
            return arn_guard.clone();
        }
    }
    None
}

fn store_payload_for_later(payload_bytes: Vec<u8>) {
    if let Ok(mut buffer) = AGENT_PAYLOAD_BUFFER.lock() {
        buffer.push(payload_bytes);
    }
}

// Channel coordination helper functions
fn create_payload_coordination_channel(request_id: &str) -> mpsc::UnboundedReceiver<()> {
    let (tx, rx) = mpsc::unbounded_channel();
    
    if let Ok(mut channels) = PAYLOAD_COORDINATION.lock() {
        channels.insert(request_id.to_string(), tx);
    }
    
    rx
}

fn cleanup_payload_coordination_channel(request_id: &str) {
    if let Ok(mut channels) = PAYLOAD_COORDINATION.lock() {
        if channels.remove(request_id).is_some() {
            debug!("Cleaned up coordination channel for request {}", request_id);
        }
    }
}

/// Process agent payload immediately when received from channel and notify completion
async fn process_received_agent_payload_with_notification(payload_bytes: Vec<u8>) -> bool {
    // Use the same strategy as route_payload_to_request_buffer for consistency
    let current_request_id = {
        if let Ok(contexts) = REQUEST_CONTEXTS.lock() {
            // Get the most recent request (assumes HashMap iteration order reflects insertion order in recent Rust)
            contexts.keys().last().cloned()
        } else {
            None
        }
    };
    
    if current_request_id.is_none() {
        info!("No active request context, payload will be processed later");
        store_payload_for_later(payload_bytes);
        return false;
    }
    
    let request_id = current_request_id.unwrap();
    let invoked_function_arn = get_current_invoked_function_arn();
    
    if invoked_function_arn.is_none() {
        warn!("No function ARN available, storing payload for later processing");
        store_payload_for_later(payload_bytes);
        return false;
    }
    
    let arn = invoked_function_arn.unwrap();
    let config = get_global_config();
    let newrelic_client = get_global_newrelic_client();
    let log_processor = get_global_log_processor();
    
    if config.is_none() || newrelic_client.is_none() || log_processor.is_none() {
        info!("Extension components not ready, storing payload for later processing");
        store_payload_for_later(payload_bytes);
        return false;
    }
    
    let config = config.unwrap();
    let newrelic_client = newrelic_client.unwrap();
    let log_processor = log_processor.unwrap();
    
    info!("Processing agent payload immediately for request {}", request_id);
    
    // Process and send immediately
    process_and_send_agent_payload(
        &payload_bytes,
        &request_id,
        &arn,
        &newrelic_client,
        &config,
        &log_processor,
    ).await;
    
    // Notify completion via channel
    if let Ok(channels) = PAYLOAD_COORDINATION.lock() {
        if let Some(tx) = channels.get(&request_id) {
            let _ = tx.send(()); // Notify that payload processing is complete
            info!("Notified runtime done handler that payload is complete for request {}", request_id);
        }
    }
    
    true // Successfully processed immediately
}

/// Process and send agent payload following our simple flow
async fn process_and_send_agent_payload(
    payload_bytes: &[u8],
    request_id: &str,
    invoked_function_arn: &str,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
    log_processor: &Arc<LogProcessor>,
) {
    // 1. Check if trace ID is enabled via config
    if config.new_relic.collect_trace_id {
        // Extract trace ID if enabled
        if let Ok(Some(trace_id)) = trace::extract_trace_id_from_payload(payload_bytes) {
            info!("Extracted trace ID: {}, coordinating with logs", trace_id);
            
            update_trace_id_in_context(request_id, &trace_id);
            
            if let Err(e) = log_processor.on_trace_id_extracted(&trace_id).await {
                error!("Failed to coordinate logs with trace ID: {}", e);
            }
        }
    }
    
    // 2. Send agent payload to New Relic immediately
    send_agent_payload_to_newrelic(
        payload_bytes,
        request_id,
        invoked_function_arn,
        newrelic_client,
        config,
    ).await;
    
    info!("Agent payload processed and sent for request {}", request_id);
}

fn update_trace_id_in_context(request_id: &str, trace_id: &str) {
    let context = get_request_context(request_id);
    if let Ok(mut ctx) = context.lock() {
        ctx.trace_id = Some(trace_id.to_string());
    };
}

async fn send_agent_payload_to_newrelic(
    payload_bytes: &[u8],
    request_id: &str,
    invoked_function_arn: &str,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
) {
    let function_name = invoked_function_arn.split(':').last().unwrap_or("");
    let log_group_name = format!("/aws/lambda/{}", function_name);
    
    let wrapped_payload = create_wrapped_agent_payload_json(
        payload_bytes,
        function_name,
        invoked_function_arn,
        &log_group_name,
        request_id,
    );
    
    match newrelic_client.send_agent_payload(config, &wrapped_payload).await {
        Ok(_) => {
            info!("Successfully sent agent payload for request {}", request_id);
        }
        Err(e) => {
            error!("Failed to send agent payload for request {}: {}", request_id, e);
        }
    }
}

/// Start harvester as background task
fn start_harvester_background_task(
    processors: Vec<Arc<dyn Flush>>,
    harvest_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    let harvester = Harvester::new(processors, harvest_interval);
    tokio::spawn(async move {
        harvester.run().await;
    })
}

/// Infinite event loop - handles first invoke (cold start) and all subsequent invokes (warm starts)
async fn run_infinite_event_loop(mut extension_components: ExtensionComponents) -> (u32, tokio::task::JoinHandle<()>) {
    // Check if this is a no-op mode
    if !extension_components.config.new_relic.extension_enabled || extension_components.config.new_relic.license_key.is_none() {
        info!("Running in no-op mode");
        execute_noop_event_loop(&extension_components.client, &extension_components.extension_id).await;
        return (0, extension_components.harvester_handle);
    }

    // Execute main telemetry processing loop
    let total_events = execute_main_telemetry_processing_loop(&mut extension_components).await;
    (total_events, extension_components.harvester_handle)
}

/// INFINITE EVENT LOOP - Core of Lambda Extension Lifecycle
/// 
/// This loop implements the correct Lambda extension pattern:
/// 1. GET /next (blocks here - Lambda freezes extension)
/// 2. Receive INVOKE event (Lambda unfreezes extension)  
/// 3. Process event quickly (< 50ms for warm starts)
/// 4. Return to step 1 (loops forever until SHUTDOWN)
///
/// First iteration = Cold Start, subsequent iterations = Warm Starts
async fn execute_main_telemetry_processing_loop(components: &mut ExtensionComponents) -> u32 {
    let mut event_counter = 0;
    let mut previous_invocation_context: Option<(String, String)> = None;

    loop {
        // WARM START CRITICAL PATH: This is the ONLY work done on warm starts
        debug!("mainLoop: waiting for next lambda invocation event...");
        
        // RACE CONDITION PROTECTION: Check if agent payloads arrived during previous warm start processing
        // This handles the case where payloads arrive after we start waiting for the next event
        let buffer_size_before = {
            if let Ok(buffer) = AGENT_PAYLOAD_BUFFER.lock() {
                buffer.len()
            } else { 0 }
        };
        
        if buffer_size_before > 0 && previous_invocation_context.is_some() {
            warn!("Found {} orphaned agent payloads from previous invocation, processing them now", buffer_size_before);
            let previous_context = previous_invocation_context.clone();
            tokio::spawn({
                let newrelic_client = Arc::clone(&components.newrelic_client);
                let config = Arc::clone(&components.config);
                let log_processor = Arc::clone(&components.log_processor);
                async move {
                    process_previous_invocation_agent_payloads(
                        &previous_context,
                        &newrelic_client,
                        &config,
                        &log_processor
                    ).await;
                }
            });
        }
        
        // WARM START CRITICAL PATH: No processing here - just setup for next event
        // All telemetry processing happens in detached background tasks
        
        // Immediately clear previous request context (no network I/O)
        if previous_invocation_context.is_some() {
            components.log_processor.clear_request_id();
        }

        // Fetch next Lambda runtime event (WARM START: Only real network call)
        let runtime_event = match fetch_next_lambda_runtime_event(&components.client, &components.extension_id).await {
            Ok(event) => event,
            Err(e) => {
                error!("Error receiving next event: {:?}. Continuing.", e);
                continue;
            }
        };

        event_counter += 1;

        match runtime_event {
            LambdaRuntimeEvent::Invoke { request_id, invoked_function_arn } => {
                // CRITICAL: Update context IMMEDIATELY to prevent race conditions with agent payload processing
                let invocation_start_time = chrono::Utc::now();
                update_global_invocation_context(&request_id, &invoked_function_arn, invocation_start_time);
                
                // PERFORMANCE: Start timing AFTER receiving the event (not including wait time)
                let event_processing_start_time = std::time::Instant::now();
                
                // CRITICAL: Check for unsent agent payloads from previous invocation FIRST
                // This ensures we don't lose any agent data between invocations
                if previous_invocation_context.is_some() {
                    let (pending_request_payloads, pending_global_payloads) = check_pending_agent_payloads(&previous_invocation_context);
                    
                    debug!("Checking for pending payloads from previous invocation: {} request, {} global", 
                           pending_request_payloads, pending_global_payloads);
                    
                    if pending_request_payloads > 0 || pending_global_payloads > 0 {
                        warn!("Found {} request + {} global unsent agent payloads from previous invocation, processing immediately", 
                              pending_request_payloads, pending_global_payloads);
                        
                        // Process previous invocation payloads synchronously to ensure they're sent
                        // before we start the new invocation
                        process_previous_invocation_agent_payloads(
                            &previous_invocation_context,
                            &components.newrelic_client,
                            &components.config,
                            &components.log_processor
                        ).await;
                        
                        info!("Previous invocation payloads processed successfully");
                    } else {
                        debug!("No pending payloads from previous invocation, proceeding with new invocation");
                    }
                }
                
                // Fast local state updates (no network I/O)
                components.log_processor.set_invocation_start_time(invocation_start_time);
                components.log_processor.reset_trace_id_state();
                // Request context isolation is handled automatically via shared invocation_context
                components.platform_processor.process_invoke_event(&request_id, &invoked_function_arn);
                
                // Retry failed logs from previous invocation in background (non-blocking)
                let log_processor_clone = components.log_processor.clone();
                spawn_safe_task("failed_log_retry", async move {
                    if let Err(e) = log_processor_clone.retry_failed_logs_before_invocation().await {
                        error!("Error retrying failed logs: {}", e);
                    }
                });
                
                // OPTIMIZED PROCESSING STRATEGY WITH PROPER AGENT PAYLOAD TIMING:
                // ===============================================================
                // Wait for Lambda function completion (runtimeDone event) before processing agent payloads
                // Agent payloads are generated AFTER function completion, typically 100-300ms later
                
                if event_counter == 1 {
                    // COLD START: Synchronous processing to ensure delivery before container shutdown
                    wait_for_function_completion_and_process_payloads(
                        &mut components.runtime_done_rx,
                        &request_id,
                    ).await;
                } else {
                    // WARM START: Optimized background processing
                    // Note: Previous invocation payloads are processed synchronously above if detected
                    // The before-freeze check provides a safety net for any missed payloads
                    
                    // For current invocation: Background processing with safety net
                    tokio::spawn({
                        let current_request_id = request_id.clone();
                        let current_invoked_arn = invoked_function_arn.clone();
                        let newrelic_client = Arc::clone(&components.newrelic_client);
                        let config = Arc::clone(&components.config);
                        let log_processor = Arc::clone(&components.log_processor);
                        
                        async move {
                            debug!("WARM START: Starting background agent payload processing for request: {}", current_request_id);
                            
                            // Wait for function completion and process agent payloads
                            // The before-freeze check will catch any payloads we miss here
                            wait_for_function_execution_and_agent_payloads(
                                &current_request_id,
                                &current_invoked_arn,
                                &newrelic_client,
                                &config,
                                &log_processor,
                            ).await;
                        }
                    });
                }
                
                // PERFORMANCE: Measure actual processing time (excludes wait time)
                let event_processing_time = event_processing_start_time.elapsed();
                
                // Log performance - first event after initialization is cold start, rest are warm starts
                if event_counter == 1 {
                    info!("COLD START: First invocation processed in {:?} (request_id: {})", 
                          event_processing_time, request_id);
                } else {
                    info!("WARM START: Event {} processed in {:?} (request_id: {})", 
                          event_counter, event_processing_time, request_id);
                }
                
                // CRITICAL: Check for any pending agent payloads before going into freeze mode
                // This ensures we don't miss any late-arriving payloads before Lambda freezes the extension
                check_and_process_pending_agent_payloads_before_freeze(
                    &request_id,
                    &invoked_function_arn,
                    &components.newrelic_client,
                    &components.config,
                    &components.log_processor,
                ).await;
                
                // Save context for next iteration (clone to avoid move issues)
                previous_invocation_context = Some((request_id.clone(), invoked_function_arn.clone()));
            }
            LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
                info!("Extension shutting down: {}", shutdown_reason);
                
                // Final cleanup
                perform_final_telemetry_flush(&components.log_processor, &components.platform_processor).await;
                process_final_agent_payloads(&previous_invocation_context, &components.newrelic_client, &components.config, &components.log_processor).await;
                
                break;
            }
        }
    }

    event_counter
}



/// Create per-request context for concurrent request handling
fn create_request_context(request_id: &str, invoked_function_arn: &str) -> Arc<Mutex<InvocationContext>> {
    let context = Arc::new(Mutex::new(InvocationContext {
        request_id: request_id.to_string(),
        invoked_function_arn: invoked_function_arn.to_string(),
        trace_id: None,
    }));
    
    // Store in per-request contexts map
    if let Ok(mut contexts) = REQUEST_CONTEXTS.lock() {
        contexts.insert(request_id.to_string(), context.clone());
        info!("Created per-request context for {}", request_id);
    } else {
        error!("Failed to lock REQUEST_CONTEXTS for request {}", request_id);
    }
    
    context
}

/// Create per-request agent buffer for concurrent request handling
fn create_request_agent_buffer(request_id: &str) -> Arc<Mutex<Vec<Vec<u8>>>> {
    let buffer = Arc::new(Mutex::new(Vec::new()));
    
    // Store in per-request buffers map
    if let Ok(mut buffers) = REQUEST_AGENT_BUFFERS.lock() {
        buffers.insert(request_id.to_string(), buffer.clone());
        info!("Created per-request agent buffer for {}", request_id);
    } else {
        error!("Failed to lock REQUEST_AGENT_BUFFERS for request {}", request_id);
    }
    
    buffer
}

/// Get per-request context (for concurrent requests) or fallback to global context
fn get_request_context(request_id: &str) -> Arc<Mutex<InvocationContext>> {
    if let Ok(contexts) = REQUEST_CONTEXTS.lock() {
        if let Some(context) = contexts.get(request_id) {
            return context.clone();
        }
    }
    
    // Fallback to global context for backward compatibility
    warn!("Using fallback global context for request: {}", request_id);
    INVOCATION_CONTEXT.clone()
}

/// Get per-request agent buffer (for concurrent requests) or fallback to global buffer
fn get_request_agent_buffer(request_id: &str) -> Arc<Mutex<Vec<Vec<u8>>>> {
    if let Ok(buffers) = REQUEST_AGENT_BUFFERS.lock() {
        if let Some(buffer) = buffers.get(request_id) {
            return buffer.clone();
        }
    }
    
    // Fallback to global buffer for backward compatibility
    warn!("Using fallback global agent buffer for request: {}", request_id);
    AGENT_PAYLOAD_BUFFER.clone()
}

/// Clean up per-request context and buffer after processing
fn cleanup_request_resources(request_id: &str) {
    // Clean up context
    if let Ok(mut contexts) = REQUEST_CONTEXTS.lock() {
        if contexts.remove(request_id).is_some() {
            debug!("Cleaned up context for request {}", request_id);
        } else {
            debug!("No context found to clean up for request {}", request_id);
        }
    }
    
    // Clean up agent buffer
    if let Ok(mut buffers) = REQUEST_AGENT_BUFFERS.lock() {
        if buffers.remove(request_id).is_some() {
            debug!("Cleaned up agent buffer for request {}", request_id);
        } else {
            debug!("No agent buffer found to clean up for request {}", request_id);
        }
    }
    
    // Clean up payload coordination channel
    cleanup_payload_coordination_channel(request_id);
}



/// Check pending agent payloads for a given request context
/// Returns (request_buffer_size, global_buffer_size)
fn check_pending_agent_payloads(context: &Option<(String, String)>) -> (usize, usize) {
    if let Some((request_id, _)) = context {
        let request_buffer = get_request_agent_buffer(request_id);
        let request_size = if let Ok(buffer) = request_buffer.lock() {
            buffer.len()
        } else {
            0
        };
        
        let global_size = if let Ok(buffer) = AGENT_PAYLOAD_BUFFER.lock() {
            buffer.len()
        } else {
            0
        };
        
        (request_size, global_size)
    } else {
        (0, 0)
    }
}

/// Simple check before freeze mode with continuous listening
async fn check_and_process_pending_agent_payloads_before_freeze(
    request_id: &str,
    invoked_function_arn: &str,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
    log_processor: &Arc<LogProcessor>,
) {
    info!("Checking for pending agent payloads before freeze for request {}", request_id);
    
    // Create a receiver for this request to wait for agent payload completion
    let mut payload_completion_rx = create_payload_coordination_channel(request_id);
    
    // Wait for agent payload completion with timeout before freeze
    info!("Waiting for agent payload completion before freeze for request {} (timeout)...", request_id);
    
    tokio::select! {
        _ = payload_completion_rx.recv() => {
            info!("Agent payload processing completed before freeze for request {}", request_id);
        }
        _ = tokio::time::sleep(std::time::Duration::from_millis(150)) => {
            info!("Timeout (150ms) waiting for agent payload before freeze for request {}", request_id);
            
            // Process any stored payloads as fallback
            process_any_stored_payloads_for_request(
                request_id,
                invoked_function_arn,
                newrelic_client,
                config,
                log_processor,
            ).await;
        }
    }
}



/// Update global invocation context (legacy function, now also creates per-request context)
fn update_global_invocation_context(request_id: &str, invoked_function_arn: &str, invocation_start_time: chrono::DateTime<chrono::Utc>) {
    // Create per-request context for concurrent handling
    create_request_context(request_id, invoked_function_arn);
    create_request_agent_buffer(request_id);
    
    // Update global state for backward compatibility
    {
        let arn_mutex = INVOKED_FUNCTION_ARN.get_or_init(|| Mutex::new(None));
        let mut arn = arn_mutex.lock().unwrap();
        *arn = Some(invoked_function_arn.to_string());
        
        let request_id_mutex = LAST_REQUEST_ID.get_or_init(|| Mutex::new(None));
        let mut last_request_id = request_id_mutex.lock().unwrap();
        *last_request_id = Some(request_id.to_string());
    }

    // Update fallback global context
    if let Ok(mut context) = INVOCATION_CONTEXT.lock() {
        context.request_id = request_id.to_string();
        context.invoked_function_arn = invoked_function_arn.to_string();
        context.trace_id = None; // Reset trace ID for new invocation
    } else {
        error!("Failed to lock invocation_context");
    }
    
    info!("New invocation started - request_id: {}, timestamp: {}", request_id, invocation_start_time);
}



/// Simplified: Wait for function completion and then check agent payload status
async fn wait_for_function_completion_and_process_payloads(
    runtime_done_rx: &mut mpsc::UnboundedReceiver<()>,
    request_id: &str,
) {
    // Step 1: Wait for runtime done signal (function completion)
    info!("Waiting for Lambda function completion (runtimeDone event)...");
    if runtime_done_rx.recv().await.is_none() {
        warn!("Runtime done channel closed unexpectedly");
    }
    
    info!("Function completed, checking for agent payload completion...");
    
    // Step 2: Create a receiver for this request to check for immediate agent payload completion
    let mut payload_completion_rx = create_payload_coordination_channel(request_id);
    
    // Check for immediate agent payload completion (no timeout)
    info!("Checking for immediate agent payload completion for request {}...", request_id);
    
    // Check if payload was already processed immediately (no wait needed)
    tokio::select! {
        _ = payload_completion_rx.recv() => {
            info!("Agent payload processing completed for request {}", request_id);
        }
        _ = tokio::time::sleep(std::time::Duration::from_millis(0)) => {
            // No wait - if payload arrives later, it will be caught by before-freeze check
            info!("No immediate agent payload for request {} - will check before freeze", request_id);
        }
    }
}

async fn process_any_stored_payloads_for_request(
    request_id: &str,
    invoked_function_arn: &str,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
    log_processor: &Arc<LogProcessor>,
) {
    // Check for any stored payloads and process them
    let payloads = get_and_clear_agent_payloads(request_id);
    let payload_count = payloads.len();
    
    if payload_count > 0 {
        info!("Processing {} stored agent payloads for request {}", payload_count, request_id);
    }
    
    for payload_bytes in payloads {
        process_and_send_agent_payload(
            &payload_bytes,
            request_id,
            invoked_function_arn,
            newrelic_client,
            config,
            log_processor,
        ).await;
    }
}

fn get_and_clear_agent_payloads(request_id: &str) -> Vec<Vec<u8>> {
    // Try per-request buffer first
    if let Ok(buffers) = REQUEST_AGENT_BUFFERS.lock() {
        if let Some(buffer) = buffers.get(request_id) {
            if let Ok(mut buf) = buffer.lock() {
                if !buf.is_empty() {
                    return std::mem::take(&mut *buf);
                }
            }
        }
    }
    
    // Fallback to global buffer
    if let Ok(mut global_buf) = AGENT_PAYLOAD_BUFFER.lock() {
        std::mem::take(&mut *global_buf)
    } else {
        Vec::new()
    }
}

/// Simplified warm start version - wait for channel coordination or timeout
async fn wait_for_function_execution_and_agent_payloads(
    request_id: &str,
    _invoked_function_arn: &str,
    _newrelic_client: &Arc<NewRelicClient>,
    _config: &Arc<config::ExtensionConfig>,
    _log_processor: &Arc<LogProcessor>,
) {
    info!("Waiting for agent payloads for request {}", request_id);
    
    // Create a receiver for this request to check for immediate agent payload completion
    let mut payload_completion_rx = create_payload_coordination_channel(request_id);
    
    // Check for immediate agent payload completion (no timeout)
    info!("Checking for immediate agent payload completion for request {}...", request_id);
    
    // Check if payload was already processed immediately (no wait needed)
    tokio::select! {
        _ = payload_completion_rx.recv() => {
            info!("Agent payload processing completed for request {}", request_id);
        }
    }
}

async fn process_previous_invocation_agent_payloads(
    previous_context: &Option<(String, String)>,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
    log_processor: &Arc<LogProcessor>,
) {
    if let Some((request_id, invoked_arn)) = previous_context {
        // First, flush any buffered logs with the previous context
        let previous_trace_id = {
            let invocation_context = log_processor.get_invocation_context();
            let context = invocation_context.lock().unwrap();
            context.trace_id.clone()
        };
        
        if let Err(e) = log_processor.flush_with_previous_context(
            request_id, 
            previous_trace_id.as_deref()
        ).await {
            error!("Error flushing logs with previous context: {}", e);
        }

        // Then process agent payloads
        process_agent_payloads_with_context(
            &Some(request_id.clone()), 
            &Some(invoked_arn.clone()), 
            newrelic_client, 
            config,
            log_processor
        ).await;
    }
}



/// Perform final telemetry flush during shutdown
async fn perform_final_telemetry_flush(
    log_processor: &Arc<LogProcessor>,
    platform_processor: &Arc<PlatformProcessor>,
) {
    if let Err(e) = log_processor.send_and_clear_batch_simple().await {
        error!("Error during final log flush: {}", e);
    }
    if let Err(e) = platform_processor.send_and_clear_batch_simple().await {
        error!("Error during final platform events flush: {}", e);
    }
}

/// Process final agent payloads during shutdown
async fn process_final_agent_payloads(
    previous_context: &Option<(String, String)>,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
    log_processor: &Arc<LogProcessor>,
) {
    if let Some((request_id, invoked_arn)) = previous_context {
        process_agent_payloads_with_context(
            &Some(request_id.clone()), 
            &Some(invoked_arn.clone()), 
            newrelic_client, 
            config,
            log_processor
        ).await;
    }
}

/// No-op event loop when extension is disabled
/// Equivalent to Go's noopLoop function
async fn execute_noop_event_loop(client: &Arc<Client>, extension_id: &str) {
    info!("Starting no-op mode, no telemetry will be sent");

    loop {
        let loop_start = std::time::Instant::now();
        match fetch_next_lambda_runtime_event(client, extension_id).await {
            Ok(LambdaRuntimeEvent::Shutdown { shutdown_reason: _ }) => {
                info!("Extension shutting down");
                break;
            }
            Ok(LambdaRuntimeEvent::Invoke { request_id, invoked_function_arn: _ }) => {
                // WARM START PATH: This is where no-op warm starts spend their time
                trace!("No-op mode invocation processed in {:?} (request_id: {})", loop_start.elapsed(), request_id);
            }
            Err(_) => { /* Ignore errors and continue polling */ }
        }
    }
}

/// Perform extension shutdown cleanup
async fn perform_extension_shutdown_cleanup(
    total_events_processed: u32,
    harvester_handle: tokio::task::JoinHandle<()>,
    extension_startup_time: std::time::Instant,
) {
    info!("New Relic Extension shutting down after {} events", total_events_processed);
    
    // Stop harvester background task
    harvester_handle.abort();
    
    let shutdown_at = std::time::Instant::now();
    let total_runtime = shutdown_at.duration_since(extension_startup_time);
    info!("Extension shutdown after {}ms", total_runtime.as_millis());
}

/// Drains the agent payload buffer and sends all pending payloads to New Relic.
/// Maintains all original functionality: trace ID extraction, error buffering, chunking
async fn process_agent_payloads_with_context(
    request_id_opt: &Option<String>,
    invoked_arn_opt: &Option<String>,
    client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
    log_processor: &Arc<LogProcessor>,
) {
    let payloads = if let (Some(request_id), Some(_)) = (request_id_opt, invoked_arn_opt) {
        // Try to get per-request buffer first
        let request_buffer = get_request_agent_buffer(request_id);
        let mut buf = match request_buffer.lock() {
            Ok(b) => b,
            Err(e) => {
                error!("Failed to lock per-request buffer for {}: {}", request_id, e);
                return;
            }
        };
        let request_payloads = std::mem::take(&mut *buf);
        
        // If no payloads in per-request buffer, check global buffer for backward compatibility
        if request_payloads.is_empty() {
            debug!("No payloads in per-request buffer for {}, checking global buffer", request_id);
            drop(buf); // Release per-request buffer lock
            
            let mut global_buf = match AGENT_PAYLOAD_BUFFER.lock() {
                Ok(b) => b,
                Err(e) => {
                    error!("Failed to lock global buffer: {}", e);
                    return;
                }
            };
            std::mem::take(&mut *global_buf)
        } else {
            debug!("Found {} payloads in per-request buffer for {}", request_payloads.len(), request_id);
            request_payloads
        }
    } else {
        // Fallback to global buffer when no request context
        let mut buf = match AGENT_PAYLOAD_BUFFER.lock() {
            Ok(b) => b,
            Err(e) => {
                error!("Failed to lock buffer: {}", e);
                return;
            }
        };
        std::mem::take(&mut *buf)
    };

    if payloads.is_empty() {
        if config.new_relic.collect_trace_id {
            if let Err(e) = log_processor.on_trace_id_extraction_failed().await {
                error!("Failed to handle no-payloads trace ID extraction failure: {}", e);
            }
        }
        return;
    }

    let (Some(request_id), Some(invoked_arn)) = (request_id_opt, invoked_arn_opt) else {
        error!("Payloads exist in buffer but no previous context is available. Discarding {} payloads.", payloads.len());
        return;
    };

    info!("Processing {} agent payloads", payloads.len());

    let function_name = invoked_arn.split(':').last().unwrap_or("");
    let log_group_name = format!("/aws/lambda/{}", function_name);
    
    let mut success_count = 0;
    let mut error_count = 0;
    let mut trace_id_found = false;

    // Step 1: Extract trace ID first if enabled (before sending any payloads)
    if config.new_relic.collect_trace_id {
        for payload_bytes in &payloads {
            if let Ok(Some(trace_id)) = trace::extract_trace_id_from_payload(payload_bytes) {
                trace_id_found = true;
                
                // Update per-request context if available, otherwise use global context
                let context_to_update = if let Some(req_id) = request_id_opt {
                    get_request_context(req_id)
                } else {
                    INVOCATION_CONTEXT.clone()
                };
                
                {
                    let mut context = match context_to_update.lock() {
                        Ok(ctx) => ctx,
                        Err(e) => {
                            error!("Failed to lock context for trace ID: {}", e);
                            break;
                        }
                    };
                    context.trace_id = Some(trace_id.clone());
                }
                
                info!("Extracted trace ID: {}, coordinating with logs before sending agent payload", trace_id);
                
                // CRITICAL: Update logs with trace ID and send them BEFORE agent payloads
                if let Err(e) = log_processor.on_trace_id_extracted(&trace_id).await {
                    error!("Failed to process trace ID extraction: {}", e);
                } else {
                    debug!("Successfully coordinated logs with trace ID: {}", trace_id);
                }
                break; // Only need one trace ID per invocation
            }
        }
    }

    // Step 2: Send agent payloads (logs with trace ID have already been sent if applicable)
    for payload_bytes in payloads {
        let wrapped_agent_data_json = create_wrapped_agent_payload_json(
            &payload_bytes,
            function_name,
            invoked_arn,
            &log_group_name,
            request_id,
        );

        match client.send_agent_payload(config, &wrapped_agent_data_json).await {
            Ok(_) => {
                success_count += 1;
                debug!("Successfully sent agent payload {} for request_id: {}", success_count, request_id);
            }
            Err(e) => {
                error_count += 1;
                error!("Failed to send agent payload: {}", e);
            }
        }
    }

    // Handle trace ID extraction failure (if enabled but no trace ID found)
    if !trace_id_found && config.new_relic.collect_trace_id {
        if let Err(e) = log_processor.on_trace_id_extraction_failed().await {
            error!("Failed to handle trace ID extraction failure: {}", e);
        }
    }

    info!("Agent payload processing complete: {} success, {} errors", success_count, error_count);
    
    // Clean up per-request resources after processing
    if let Some(request_id) = request_id_opt {
        cleanup_request_resources(request_id);
    }
}

/// Create wrapped agent payload JSON string
/// Create New Relic log format with agent data in message field
/// NOTE: Trace ID extraction is handled separately in process_and_send_agent_payload
fn create_wrapped_agent_payload_json(
    payload_bytes: &[u8],
    function_name: &str,
    invoked_function_arn: &str,
    log_group_name: &str,
    request_id: &str,
) -> String {
    debug!("Processing agent data of {} bytes for function: {}", payload_bytes.len(), function_name);
    
    // Create New Relic log event format with agent data as message
    create_newrelic_log_format(payload_bytes, function_name, invoked_function_arn, log_group_name, request_id)
}

/// Create New Relic format with Lambda context and stringified log events in entry field
/// Returns JSON with context and entry fields matching New Relic expected format
/// NOTE: This is for AGENT payload wrapping, not regular log processing
fn create_newrelic_log_format(
    agent_data: &[u8],
    function_name: &str,
    invoked_function_arn: &str,
    log_group_name: &str,
    request_id: &str,
) -> String {
    // Convert agent data to string (should be JSON array like [1,"NR_LAMBDA_MONITORING","compressed_data"])
    let agent_data_str = String::from_utf8_lossy(agent_data);
    debug!("Agent data to wrap in log format: {}", agent_data_str);

    // Generate timestamp in milliseconds
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Create the log events structure first (for agent payload wrapping)
    let log_events_payload = serde_json::json!({
        "logEvents": [{
            "id": request_id,
            "message": agent_data_str,
            "timestamp": timestamp
        }],
        "logGroup": log_group_name,
        "logStream": "",
        "messageType": "",
        "owner": ""
    });

    // Stringify the log events payload to put in entry field (this is required for agent payload format)
    let log_events_string = log_events_payload.to_string();

    // Create final payload with context and stringified entry
    let final_payload = serde_json::json!({
        "context": {
            "function_name": function_name,
            "invoked_function_arn": invoked_function_arn,
            "log_group_name": log_group_name,
            "log_stream_name": format!("{}:{}", EXTENSION_NAME, EXTENSION_VERSION)
        },
        "entry": log_events_string
    });

    // Convert to string and return
    final_payload.to_string()
}

/// Registers the extension with the Lambda Runtime API.
async fn register_extension_with_lambda_runtime(client: &Client) -> Result<(ExtensionRegistrationResponse, String), Box<dyn std::error::Error + Send + Sync>> {
    let runtime_api = env::var("AWS_LAMBDA_RUNTIME_API")
        .map_err(|_| "AWS_LAMBDA_RUNTIME_API not set")?;

    let url = format!("http://{}/2020-01-01/extension/register", runtime_api);
    
    let payload = serde_json::json!({
        "events": ["INVOKE", "SHUTDOWN"]
    });

    let response = client
        .post(&url)
        .header(EXTENSION_NAME_HEADER, EXTENSION_NAME)
        .json(&payload)
        .timeout(Duration::from_secs(30)) // 30 second timeout for registration
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_else(|_| "Failed to read response body".to_string());
        error!("Registration failed with status: {}, body: {}", status, body);
        return Err(format!("Registration failed with status: {}", status).into());
    }

    // Get extension ID from headers
    let extension_id = response
        .headers()
        .get(EXTENSION_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or("Missing extension ID in response headers")?
        .to_string();

    let registration: ExtensionRegistrationResponse = response
        .json()
        .await?;

    Ok((registration, extension_id))
}

/// Subscribes to the Lambda Telemetry API.
async fn subscribe_to_lambda_telemetry_api(client: &Client, ext_id: &str, port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let runtime_api = env::var("AWS_LAMBDA_RUNTIME_API")
        .map_err(|_| "AWS_LAMBDA_RUNTIME_API not set")?;

    let url = format!("http://{}/2022-07-01/telemetry", runtime_api);
    
    let payload = serde_json::json!({
        "schemaVersion": "2022-07-01",
        "types": ["platform", "function", "extension"],
        "buffering": {
            "maxBytes": 262144,
            "maxItems": 10000,
            "timeoutMs": 1000
        },
        "destination": {
            "protocol": "HTTP",
            "URI": format!("http://sandbox:{}/telemetry", port)
        }
    });

    let response = client
        .put(&url)
        .header(EXTENSION_ID_HEADER, ext_id)
        .json(&payload)
        .timeout(Duration::from_secs(30)) // 30 second timeout for telemetry subscription
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_else(|_| "Failed to read response body".to_string());
        error!("Telemetry subscription failed with status: {}, body: {}", status, body);
        return Err(format!("Telemetry subscription failed with status: {}", status).into());
    }

    Ok(())
}

/// Fetches the next event from the Lambda Runtime API.
async fn fetch_next_lambda_runtime_event(client: &Client, ext_id: &str) -> Result<LambdaRuntimeEvent, Box<dyn std::error::Error + Send + Sync>> {
    let runtime_api = env::var("AWS_LAMBDA_RUNTIME_API")
        .map_err(|_| "AWS_LAMBDA_RUNTIME_API not set")?;

    let url = format!("http://{}/2020-01-01/extension/event/next", runtime_api);

    // Retry logic for extension event polling - this is critical for reliability
    const MAX_RETRIES: u32 = 3;
    let mut retry_count = 0;

    loop {
        let response = client
            .get(&url)
            .header(EXTENSION_ID_HEADER, ext_id)
            .timeout(std::time::Duration::from_secs(300)) // 5 minute timeout for event polling
            .send()
            .await;

        match response {
            Ok(resp) => {
                // Success case - process the response
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_else(|_| "Failed to read response body".to_string());
                    error!("Next event request failed with status: {}, body: {}", status, body);
                    return Err(format!("Next event request failed with status: {}", status).into());
                }

                let event: LambdaRuntimeEvent = resp.json().await?;
                return Ok(event);
            },
            Err(e) => {
                // Handle timeout and connection errors with retry
                retry_count += 1;
                
                if e.is_timeout() {
                    warn!("Extension event polling timeout (attempt {}/{}): {}", retry_count, MAX_RETRIES, e);
                } else if e.is_connect() {
                    warn!("Extension event polling connection error (attempt {}/{}): {}", retry_count, MAX_RETRIES, e);
                } else {
                    warn!("Extension event polling error (attempt {}/{}): {}", retry_count, MAX_RETRIES, e);
                }

                if retry_count >= MAX_RETRIES {
                    error!("Extension event polling failed after {} retries, giving up", MAX_RETRIES);
                    return Err(e.into());
                }

                // Exponential backoff: 1s, 2s, 4s
                let delay = Duration::from_secs(2_u64.pow(retry_count - 1));
                warn!("Retrying extension event polling in {:?}...", delay);
                tokio::time::sleep(delay).await;
            }
        }
    }
}



