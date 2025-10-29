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
mod version;

#[cfg(debug_assertions)]
mod test_telemetry;

use std::{
    env,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::sync::mpsc;
use once_cell::sync::Lazy;
use dashmap::DashMap;

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



// --- CONCURRENT REQUEST HANDLING ---
// Per-request contexts to handle  Lambda invocations safely using DashMap for lock-free  access
static REQUEST_CONTEXTS: Lazy<Arc<DashMap<String, Arc<Mutex<InvocationContext>>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));
static REQUEST_AGENT_BUFFERS: Lazy<Arc<DashMap<String, Arc<Mutex<Vec<Vec<u8>>>>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

// Global coordination channels per request for agent payload processing
static PAYLOAD_COORDINATION: Lazy<Arc<DashMap<String, mpsc::UnboundedSender<()>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

// Per-request runtime.done signal channels (signaled by telemetry listener on platform.runtimeDone)
static RUNTIME_DONE_CHANNELS: Lazy<Arc<DashMap<String, mpsc::UnboundedSender<()>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

// Pending platform.report lines (stored when report arrives before agent is batched)
// Key: request_id, Value: report log line
static PENDING_REPORTS: Lazy<Arc<DashMap<String, String>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

// Per-request processing state management
static REQUEST_PROCESSORS: Lazy<Arc<DashMap<String, RequestProcessingState>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

// Failed agent payloads buffer for retry across invocations - using Mutex for Vec as DashMap is for key-value
static FAILED_AGENT_PAYLOADS: Lazy<Arc<Mutex<Vec<FailedAgentPayload>>>> =
    Lazy::new(|| Arc::new(Mutex::new(Vec::new())));

// --- AGENT PAYLOAD BATCHING ---
// Batch buffer for agent payloads with optional report lines (warm starts only)
static AGENT_BATCH_BUFFER: Lazy<Arc<DashMap<String, BatchedAgentPayload>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

// Batch metadata for tracking thresholds
struct BatchMetadata {
    agent_count: usize,
    oldest_timestamp: Option<chrono::DateTime<chrono::Utc>>,
}

static BATCH_META: Lazy<Arc<Mutex<BatchMetadata>>> =
    Lazy::new(|| Arc::new(Mutex::new(BatchMetadata {
        agent_count: 0,
        oldest_timestamp: None,
    })));

// Global current invocation context for telemetry processors
static CURRENT_INVOCATION_CONTEXT: Lazy<Arc<Mutex<InvocationContext>>> = Lazy::new(|| {
    Arc::new(Mutex::new(InvocationContext {
        request_id: "temp".to_string(),
        invoked_function_arn: "temp".to_string(),
        trace_id: None,
    }))
});

// Global flag to track if this is a warm start (for performance optimization)
static IS_WARM_START: Lazy<Arc<std::sync::atomic::AtomicBool>> = 
    Lazy::new(|| Arc::new(std::sync::atomic::AtomicBool::new(false)));

// --- PROCESSOR FACTORY FOR REQUEST-SCOPED PROCESSORS ---
#[derive(Debug, Clone)]
struct ProcessorFactory {
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<config::ExtensionConfig>,
}

impl ProcessorFactory {
    fn new(newrelic_client: Arc<NewRelicClient>, config: Arc<config::ExtensionConfig>) -> Self {
        Self { newrelic_client, config }
    }
    
    fn create_log_processor(&self, request_context: Arc<Mutex<InvocationContext>>) -> Arc<LogProcessor> {
        Arc::new(LogProcessor::new(
            Arc::clone(&self.newrelic_client),
            Arc::clone(&self.config),
            request_context,
        ))
    }
    
    fn create_platform_processor(&self, request_context: Arc<Mutex<InvocationContext>>) -> Arc<PlatformProcessor> {
        Arc::new(PlatformProcessor::new(
            Arc::clone(&self.newrelic_client),
            Arc::clone(&self.config),
            request_context,
        ))
    }
}

// --- PER-REQUEST PROCESSING STATE ---
#[derive(Debug)]
struct RequestProcessingState {
    request_id: String,
    context: Arc<Mutex<InvocationContext>>,
    platform_processor: Arc<PlatformProcessor>,
    agent_buffer: Arc<Mutex<Vec<Vec<u8>>>>,
    coordination_rx: Option<mpsc::UnboundedReceiver<()>>,
    runtime_done_rx: Option<mpsc::UnboundedReceiver<()>>,
}

// --- FAILED AGENT PAYLOAD FOR RETRY ---
#[derive(Debug, Clone)]
struct FailedAgentPayload {
    payload_bytes: Vec<u8>,
    request_id: String,
    invoked_function_arn: String,
    retry_count: usize,
    failed_at: chrono::DateTime<chrono::Utc>,
}

// --- BATCHED AGENT PAYLOAD ---
#[derive(Debug, Clone)]
struct BatchedAgentPayload {
    request_id: String,
    agent_payload_bytes: Arc<Vec<u8>>,  // Use Arc to avoid cloning large payloads
    report_line: Option<String>,
    invoked_function_arn: String,
    timestamp: chrono::DateTime<chrono::Utc>,
}

// Structure to hold all initialized components for warm starts
#[derive(Debug)]
struct ExtensionComponents {
    client: Arc<Client>,
    extension_id: String,
    processor_factory: Arc<ProcessorFactory>,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<config::ExtensionConfig>,
    runtime_done_rx: mpsc::UnboundedReceiver<()>,
    harvester: Arc<Harvester>,
    harvester_handle: tokio::task::JoinHandle<()>,
    global_log_processor: Arc<LogProcessor>,
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
        
        // Create noop processor factory
        let noop_newrelic_client = Arc::new(NewRelicClient::new_noop());
        let noop_processor_factory = Arc::new(ProcessorFactory::new(
            noop_newrelic_client.clone(),
            config.clone()
        ));
        
        // Create dummy processors for harvester
        let dummy_context = Arc::new(Mutex::new(InvocationContext {
            request_id: "noop".to_string(),
            invoked_function_arn: "noop".to_string(),
            trace_id: None,
        }));
        let noop_log_processor = noop_processor_factory.create_log_processor(dummy_context.clone());
        let noop_platform_processor = noop_processor_factory.create_platform_processor(dummy_context);
        
        return Ok(ExtensionComponents {
            client,
            extension_id,
            processor_factory: noop_processor_factory,
            newrelic_client: noop_newrelic_client,
            config: config.clone(),
            runtime_done_rx: mpsc::unbounded_channel::<()>().1,
            harvester: Arc::new(Harvester::new(vec![], Duration::from_secs(1), noop_log_processor.clone(), noop_platform_processor)),
            harvester_handle: tokio::spawn(async {}),
            global_log_processor: noop_log_processor,
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
        let noop_newrelic_client = Arc::new(NewRelicClient::new_noop());
        let noop_processor_factory = Arc::new(ProcessorFactory::new(
            noop_newrelic_client.clone(),
            config.clone()
        ));
        
        // Create dummy processors for harvester
        let dummy_context = Arc::new(Mutex::new(InvocationContext {
            request_id: "noop".to_string(),
            invoked_function_arn: "noop".to_string(),
            trace_id: None,
        }));
        let noop_log_processor = noop_processor_factory.create_log_processor(dummy_context.clone());
        let noop_platform_processor = noop_processor_factory.create_platform_processor(dummy_context);
        
        return Ok(ExtensionComponents {
            client,
            extension_id,
            processor_factory: noop_processor_factory,
            newrelic_client: noop_newrelic_client,
            config: config.clone(),
            runtime_done_rx: mpsc::unbounded_channel::<()>().1,
            harvester: Arc::new(Harvester::new(vec![], Duration::from_secs(1), noop_log_processor.clone(), noop_platform_processor)),
            harvester_handle: tokio::spawn(async {}),
            global_log_processor: noop_log_processor,
        });
    };

    // 3. Update config with validated license key
    let mut updated_config = (*config).clone();
    updated_config.new_relic.license_key = Some(license_key);
    let config = Arc::new(updated_config);
    
    info!("License key validated and extension registered - proceeding with full initialization");

    info!("NEW_RELIC_COLLECT_TRACE_ID setting: {}", config.new_relic.collect_trace_id);
    info!("NEW_RELIC_ADD_VERSION_DETAIL_TAGS setting: {}", config.new_relic.add_version_detail_tags);

    // Detect versions early if tagging is enabled (using async for AWS API calls)
    if config.new_relic.add_version_detail_tags {
        let version_info = version::VersionInfo::detect_async().await;
        info!("Version detection results:");
        info!("  Extension version: {}", version_info.extension_version);
        if let Some(ref agent_name) = version_info.agent_name {
            if let Some(ref agent_version) = version_info.agent_version {
                info!("  Agent: {} version {}", agent_name, agent_version);
            }
        } else {
            info!("  Agent: Not detected");
        }
        if let Some(ref layer_version) = version_info.layer_version {
            info!("  Layer: {}", layer_version);
        } else {
            info!("  Layer: Not detected (AWS API call may have failed)");
        }

    }

    info!("Log forwarding settings: send_function_logs={}, send_extension_logs={}",
          config.extension.send_function_logs,
          config.extension.send_extension_logs);

    // --- PHASE 2: PARALLEL INITIALIZATION (using already registered extension) ---

    // Update config with registration details first (needed for NewRelic client)
    let mut updated_config = (*config).clone();
    updated_config.aws.update_from_registration(
        registration.function_name,
        registration.function_version,
        registration.account_id,
    );
    let config = Arc::new(updated_config);

    // Note: Lambda function tagging will happen on first invocation when we have the real ARN
    // The constructed ARN here may have a placeholder account ID
    if config.new_relic.add_version_detail_tags {
        debug!("Version detail tagging enabled - will tag function on first invocation with actual ARN");
    }
    
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

    // Clean up very old failed payloads (older than 24 hours)
    cleanup_old_failed_payloads();

    // Start batch timeout background task (checks every 30 seconds for 5-minute timeout)
    start_batch_timeout_task(Arc::clone(&newrelic_client), Arc::clone(&config));

    // Create processor factory for request-scoped processors
    let processor_factory = Arc::new(ProcessorFactory::new(
        Arc::clone(&newrelic_client),
        Arc::clone(&config)
    ));
    
    // Setup telemetry listener (use global current context that gets updated per request)
    let global_context = Arc::clone(&CURRENT_INVOCATION_CONTEXT);
    let temp_log_processor = processor_factory.create_log_processor(global_context.clone());
    let temp_platform_processor = processor_factory.create_platform_processor(global_context);
    
    let telemetry_listener_address = setup_telemetry_listener(
        temp_log_processor.clone(),
        temp_platform_processor,
        Some(runtime_done_tx)
    ).await?;
    
    // 9. Subscribe to Lambda Telemetry API
    subscribe_to_lambda_telemetry_api(&client, &extension_id, telemetry_listener_address.port()).await?;

    // 10. Start harvester background task (will be populated with per-request processors)
    let (harvester, harvester_handle) = start_harvester_background_task(
        vec![], // Empty processors vector - will be populated per request
        config.new_relic.harvest_interval,
        &processor_factory,
    );

    // 11. Return initialized components directly
    Ok(ExtensionComponents {
        client,
        extension_id,
        processor_factory,
        newrelic_client,
        config,
        runtime_done_rx,
        harvester,
        harvester_handle,
        global_log_processor: temp_log_processor,
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

/// Start agent payload collector as background task with  request handling
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

/// Background task that checks every 30 seconds if batch should be sent due to 5-minute timeout
fn start_batch_timeout_task(
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<config::ExtensionConfig>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;

            // Check if oldest payload > 5 minutes
            let should_send = {
                if let Ok(meta) = BATCH_META.lock() {
                    if let Some(oldest) = meta.oldest_timestamp {
                        chrono::Utc::now() - oldest > chrono::Duration::seconds(300)
                    } else {
                        false
                    }
                } else {
                    false
                }
            }; // Lock is released here

            if should_send {
                info!("Batch timeout reached (5 minutes) - sending buffered payloads");
                send_batched_payloads(newrelic_client.clone(), config.clone()).await;
            }
        }
    });
}

/// Route agent payload to the correct per-request buffer
async fn route_payload_to_request_buffer(payload_bytes: Vec<u8>) {
    // Find the most recent active request using DashMap's  iteration
    // Note: DashMap doesn't guarantee insertion order, so we'll use the last entry we find
    let current_request_id = REQUEST_CONTEXTS.iter()
        .last()
        .map(|entry| entry.key().clone());

    if let Some(request_id) = current_request_id {
        // Store in request-specific buffer
        if let Some(request_buffer) = REQUEST_AGENT_BUFFERS.get(&request_id) {
            match request_buffer.lock() {
                Ok(mut buffer) => {
                    buffer.push(payload_bytes);
                    debug!("Stored agent payload in request buffer for {} (buffer size now {})",
                         request_id, buffer.len());

                    // Notify the request's coordination channel if available
                    if let Some(tx) = PAYLOAD_COORDINATION.get(&request_id) {
                        let _ = tx.send(());
                    }
                }
                Err(e) => {
                    error!("Failed to lock request buffer for {}: {} - payload lost!", request_id, e);
                }
            }
        } else {
            warn!("No buffer found for request: {} - payload lost!", request_id);
        }
    } else {
        warn!("No active requests found - agent payload lost!");
    }
}



// Channel coordination helper functions
fn create_payload_coordination_channel(request_id: &str) -> mpsc::UnboundedReceiver<()> {
    let (tx, rx) = mpsc::unbounded_channel();
    PAYLOAD_COORDINATION.insert(request_id.to_string(), tx);
    rx
}

fn cleanup_payload_coordination_channel(request_id: &str) {
    if PAYLOAD_COORDINATION.remove(request_id).is_some() {
        debug!("Cleaned up coordination channel for request {}", request_id);
    }
}

/// Buffer failed agent payload for retry across invocations
fn buffer_failed_agent_payload(
    payload_bytes: &[u8],
    request_id: &str,
    invoked_function_arn: &str,
) {
    let failed_payload = FailedAgentPayload {
        payload_bytes: payload_bytes.to_vec(),
        request_id: request_id.to_string(),
        invoked_function_arn: invoked_function_arn.to_string(),
        retry_count: 0,
        failed_at: chrono::Utc::now(),
    };
    
    if let Ok(mut failed_payloads) = FAILED_AGENT_PAYLOADS.lock() {
        failed_payloads.push(failed_payload);
        info!("Buffered failed agent payload for request {} (total failed: {})", 
             request_id, failed_payloads.len());
    } else {
        error!("Failed to lock FAILED_AGENT_PAYLOADS buffer - payload lost!");
    }
}

/// Process agent payload immediately when received from channel and notify completion
/// Process and send agent payload following our simple flow
async fn process_and_send_agent_payload(
    payload_bytes: &[u8],
    request_id: &str,
    invoked_function_arn: &str,
    log_processor: &Arc<LogProcessor>,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Extract trace ID only if enabled via config
    if config.new_relic.collect_trace_id {
        if let Ok(Some(trace_id)) = trace::extract_trace_id_from_payload(payload_bytes) {
            info!("Extracted trace ID: {}, coordinating with logs", trace_id);
            
            if let Err(e) = log_processor.on_trace_id_extracted(&trace_id).await {
                error!("Failed to coordinate logs with trace ID: {}", e);
            }
        } else {
            debug!("No trace ID found in agent payload or extraction failed");
        }
    } else {
        debug!("Trace ID collection disabled, skipping extraction");
    }
    
    // Actually send the agent payload to New Relic
    match send_agent_payload_to_newrelic(
        payload_bytes,
        request_id,
        invoked_function_arn,
        newrelic_client,
        config,
    ).await {
        Ok(_) => {
            info!("Agent payload processed and sent (size: {} bytes)", payload_bytes.len());
        }
        Err(e) => {
            error!("Failed to send agent payload for request {}: {}", request_id, e);
            
            // Buffer the failed payload for retry
            buffer_failed_agent_payload(
                payload_bytes,
                request_id,
                invoked_function_arn,
            );
            
            warn!("Agent payload buffered for retry (size: {} bytes)", payload_bytes.len());
        }
    }
    
    Ok(())
}

fn update_trace_id_in_context(request_id: &str, trace_id: &str) {
    if let Some(context) = get_request_context(request_id) {
        if let Ok(mut ctx) = context.lock() {
            ctx.trace_id = Some(trace_id.to_string());
            debug!("Updated trace ID {} for request {}", trace_id, request_id);
        } else {
            error!("Failed to lock context for trace ID update for request {}", request_id);
        }
    } else {
        error!("No context found for request {} during trace ID update", request_id);
    }
}

async fn send_agent_payload_to_newrelic(
    payload_bytes: &[u8],
    request_id: &str,
    invoked_function_arn: &str,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let function_name = invoked_function_arn.split(':').last().unwrap_or("");
    let log_group_name = format!("/aws/lambda/{}", function_name);

    let wrapped_payload = create_wrapped_agent_payload_json(
        payload_bytes,
        function_name,
        invoked_function_arn,
        &log_group_name,
        request_id,
        config,
    );
    
    match newrelic_client.send_agent_payload(config, &wrapped_payload).await {
        Ok(_) => {
            info!("Successfully sent agent payload for request {}", request_id);
            Ok(())
        }
        Err(e) => {
            error!("Failed to send agent payload for request {}: {}", request_id, e);
            Err(Box::new(e))
        }
    }
}

/// Start harvester as background task
fn start_harvester_background_task(
    processors: Vec<Arc<dyn Flush>>,
    harvest_interval: Duration,
    processor_factory: &Arc<ProcessorFactory>,
) -> (Arc<Harvester>, tokio::task::JoinHandle<()>) {
    // Create dummy processors for harvester (will be replaced by per-request processors)
    let dummy_context = Arc::new(Mutex::new(InvocationContext {
        request_id: "harvester".to_string(),
        invoked_function_arn: "harvester".to_string(),
        trace_id: None,
    }));
    let dummy_log_processor = processor_factory.create_log_processor(dummy_context.clone());
    let dummy_platform_processor = processor_factory.create_platform_processor(dummy_context);
    
    let harvester = Arc::new(Harvester::new(processors, harvest_interval, dummy_log_processor, dummy_platform_processor));
    let harvester_clone = Arc::clone(&harvester);
    let handle = tokio::spawn(async move {
        harvester_clone.run().await;
    });
    (harvester, handle)
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

    loop {
        debug!("mainLoop: waiting for next lambda invocation event...");

        // Fetch next Lambda runtime event
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
                let event_processing_start_time = std::time::Instant::now();

                // Tag Lambda function on first invocation (with real ARN)
                static TAGGING_DONE: std::sync::Once = std::sync::Once::new();
                let should_tag = components.config.new_relic.add_version_detail_tags;
                let arn_for_tagging = invoked_function_arn.clone();
                if should_tag {
                    TAGGING_DONE.call_once(|| {
                        info!("Spawning background task to tag Lambda function with version information");
                        let version_info = version::VersionInfo::get_or_detect();
                        version::tagging::tag_lambda_function_background(
                            version_info.extension_version.clone(),
                            version_info.agent_version.clone(),
                            version_info.layer_version.clone(),
                            arn_for_tagging,
                        );
                    });
                }

                // Update global context for telemetry processors
                if let Ok(mut global_context) = CURRENT_INVOCATION_CONTEXT.lock() {
                    global_context.request_id = request_id.clone();
                    global_context.invoked_function_arn = invoked_function_arn.clone();
                    global_context.trace_id = None; // Reset trace ID for new request
                }
                
                // Process any logs that were buffered waiting for request_id
                components.global_log_processor.process_buffered_logs_with_request_id(&request_id);
                
                // Create request-scoped processing state
                let request_state = create_request_processing_state(
                    &request_id,
                    &invoked_function_arn,
                    &components.processor_factory
                );

                // Update global log processor context for this request
                components.global_log_processor.update_invocation_context(request_state.context.clone());

                // Store request processing state
                REQUEST_PROCESSORS.insert(request_id.clone(), request_state);
                
                // Process the request concurrently but wait for completion
                let request_id_clone = request_id.clone();
                let invoked_function_arn_clone = invoked_function_arn.clone();
                let processor_factory_clone = components.processor_factory.clone();
                let newrelic_client_clone = components.newrelic_client.clone();
                let config_clone = components.config.clone();
                let global_log_processor_clone = components.global_log_processor.clone();

                let processing_handle = tokio::spawn(async move {
                    process_request_concurrently(
                        request_id_clone,
                        invoked_function_arn_clone,
                        processor_factory_clone,
                        newrelic_client_clone,
                        config_clone,
                        global_log_processor_clone,
                    ).await;
                });

                // ALWAYS wait for processing to complete (logs + agent must be sent before Lambda freeze)
                // Optimizations (no runtime.done wait + 50ms agent timeout) happen inside process_request_concurrently
                if let Err(e) = processing_handle.await {
                    error!("Error in request processing: {}", e);
                }

                let event_processing_time = event_processing_start_time.elapsed();

                if event_counter == 1 {
                    info!("COLD START: First invocation processed in {:?} (request_id: {})",
                          event_processing_time, request_id);
                    // Set warm start flag after first invocation completes
                    IS_WARM_START.store(true, std::sync::atomic::Ordering::Relaxed);
                } else {
                    info!("WARM START: Event {} processed in {:?} (request_id: {})",
                          event_counter, event_processing_time, request_id);
                }
            }
            LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
                info!("Extension shutting down: {}", shutdown_reason);

                // Wait for all  requests to complete and flush batched payloads
                wait_for_all_requests_completion(
                    components.newrelic_client.clone(),
                    components.config.clone()
                ).await;
                break;
            }
        }
    }

    event_counter
}

/// Concurrent request processing function
async fn process_request_concurrently(
    request_id: String,
    invoked_function_arn: String,
    _processor_factory: Arc<ProcessorFactory>,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<config::ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
) {
    debug!("Starting processing for request: {}", request_id);

    // Get request processing state
    let state = REQUEST_PROCESSORS.remove(&request_id).map(|(_k, v)| v);

    let Some(mut state) = state else {
        error!("No processing state found for request: {}", request_id);
        return;
    };

    // Set invocation start time using global log processor
    let invocation_start_time = chrono::Utc::now();
    global_log_processor.set_invocation_start_time(invocation_start_time);
    global_log_processor.reset_trace_id_state();
    state.platform_processor.process_invoke_event(&request_id, &invoked_function_arn);

    // STEP 1: Check if this is a cold or warm start
    let is_cold_start = !crate::IS_WARM_START.load(std::sync::atomic::Ordering::Relaxed);

    // For COLD STARTS ONLY: Wait for platform.runtimeDone event
    // For WARM STARTS: Skip this wait to avoid blocking the event loop (172ms → 32ms optimization)
    if is_cold_start {
        if let Some(ref mut runtime_done_rx) = state.runtime_done_rx {
            match runtime_done_rx.recv().await {
                Some(_) => {
                    info!("Runtime.done received for request: {} (COLD START)", request_id);
                }
                None => {
                    warn!("Runtime.done channel closed for request: {} - proceeding anyway", request_id);
                }
            }
        } else {
            warn!("No runtime.done channel for request: {} (shouldn't happen)", request_id);
        }
    } else {
        debug!("Skipping runtime.done wait for WARM START request: {} (performance optimization)", request_id);
    }

    // STEP 2: Wait for agent payload + optionally report line
    // Cold start: Wait longer (200ms) to ensure agent initialization completes
    // Warm start: Wait shorter (50ms) to minimize latency
    let agent_wait_timeout_ms = if is_cold_start { 200 } else { 50 };

    let payload_already_arrived = {
        if let Ok(buffer) = state.agent_buffer.lock() {
            !buffer.is_empty()
        } else {
            false
        }
    };

    if !payload_already_arrived {
        debug!("Waiting up to {}ms for agent payload for request: {}", agent_wait_timeout_ms, request_id);
        tokio::select! {
            _ = state.coordination_rx.as_mut().unwrap().recv() => {
                debug!("Agent payload received for request: {}", request_id);
            }
            _ = tokio::time::sleep(Duration::from_millis(agent_wait_timeout_ms)) => {
                debug!("Agent payload wait timeout ({}ms) for request: {}", agent_wait_timeout_ms, request_id);
            }
        }
    }

    // STEP 3: Extract agent payload from buffer
    let agent_payloads = {
        if let Ok(mut buffer) = state.agent_buffer.lock() {
            std::mem::take(&mut *buffer)
        } else {
            Vec::new()
        }
    };

    // STEP 4: Check if platform.report already available in PENDING_REPORTS (non-blocking)
    let report_line = PENDING_REPORTS.remove(&request_id).map(|(_, report)| {
        debug!("Found pending platform.report for request: {}", request_id);
        report
    });

    // STEP 5: Decide strategy based on cold/warm start and data availability
    let send_agent_task = if agent_payloads.is_empty() {
        // No agent payload - just continue
        info!("No agent payload for request: {}", request_id);
        None
    } else if is_cold_start {
        // COLD START: Send immediately with or without report
        info!("Cold start - sending agent payload immediately");
        let request_id_clone = request_id.clone();
        let invoked_function_arn_clone = invoked_function_arn.clone();
        let newrelic_client_clone = newrelic_client.clone();
        let config_clone = config.clone();
        let global_log_processor_clone = global_log_processor.clone();

        Some(tokio::spawn(async move {
            send_agent_with_report_immediately(
                request_id_clone,
                invoked_function_arn_clone,
                agent_payloads,
                report_line,
                newrelic_client_clone,
                config_clone,
                global_log_processor_clone,
            ).await;
        }))
    } else if report_line.is_some() {
        // WARM START + REPORT AVAILABLE: Send immediately in background
        debug!("Warm start - agent+report ready, sending in background");
        let request_id_clone = request_id.clone();
        let invoked_function_arn_clone = invoked_function_arn.clone();
        let newrelic_client_clone = newrelic_client.clone();
        let config_clone = config.clone();
        let global_log_processor_clone = global_log_processor.clone();

        Some(tokio::spawn(async move {
            send_agent_with_report_immediately(
                request_id_clone,
                invoked_function_arn_clone,
                agent_payloads,
                report_line,
                newrelic_client_clone,
                config_clone,
                global_log_processor_clone,
            ).await;
        }))
    } else {
        // WARM START + NO REPORT: Add to batch for later
        debug!("Warm start - batching agent payload for request: {}", request_id);
        for payload_bytes in agent_payloads {
            add_to_batch(
                request_id.clone(),
                payload_bytes,
                None,
                invoked_function_arn.clone(),
            );
        }

        // Check if should send batch
        if should_send_batch() {
            debug!("Batch threshold reached - sending batched payloads");
            let newrelic_client_clone = newrelic_client.clone();
            let config_clone = config.clone();

            Some(tokio::spawn(async move {
                send_batched_payloads(newrelic_client_clone, config_clone).await;
            }))
        } else {
            None
        }
    };

    // STEP 6: Flush logs, platform data, and agent send ALL IN PARALLEL
    let log_flushing = global_log_processor.flush();
    let platform_flushing = state.platform_processor.flush();
    let failed_retry = retry_failed_agent_payloads(&newrelic_client, &config);

    // Run ALL operations in parallel (including agent send)
    let (log_result, platform_result, _, agent_result) = tokio::join!(
        log_flushing,
        platform_flushing,
        failed_retry,
        async {
            if let Some(handle) = send_agent_task {
                handle.await
            } else {
                Ok(())
            }
        }
    );

    // Log any errors
    if let Err(e) = log_result {
        error!("Failed to flush global log processor for request {}: {}", request_id, e);
    }
    if let Err(e) = platform_result {
        error!("Failed to flush platform processor for request {}: {}", request_id, e);
    }
    if let Err(e) = agent_result {
        error!("Agent send task failed for request {}: {}", request_id, e);
    }
    
    // Cleanup request resources
    cleanup_request_processing_state(&request_id);
    
    debug!("Completed processing for request: {} (including final flush)", request_id);
}

/// Process agent payloads for specific request
async fn process_request_agent_payloads(
    request_id: &str,
    invoked_function_arn: &str,
    state: &RequestProcessingState,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
    global_log_processor: &Arc<LogProcessor>,
) {
    // Get payloads from request-specific buffer
    let payloads = {
        if let Ok(mut buffer) = state.agent_buffer.lock() {
            std::mem::take(&mut *buffer)
        } else {
            error!("Failed to lock agent buffer for request: {}", request_id);
            return;
        }
    };
    
    if payloads.is_empty() {
        debug!("No agent payloads to process for request: {}", request_id);
        return;
    }
    
    debug!("Processing {} agent payloads for request: {}", payloads.len(), request_id);
    
    // Process each payload using the global log processor
    for payload_bytes in payloads {
        if let Err(e) = process_and_send_agent_payload(
            &payload_bytes,
            request_id,
            invoked_function_arn,
            global_log_processor,
            newrelic_client,
            config,
        ).await {
            error!("Error processing agent payload for request {}: {}", request_id, e);
        }
    }
}

/// Wait for all  requests to complete and flush batched payloads
async fn wait_for_all_requests_completion(
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<config::ExtensionConfig>,
) {
    // Check if there are any pending requests
    let pending_count = REQUEST_PROCESSORS.len();

    if pending_count == 0 {
        debug!("No pending requests at shutdown - proceeding immediately");
    } else {
        info!("Waiting for {}  request(s) to complete...", pending_count);

        // Wait a reasonable time for requests to complete
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Force cleanup of any remaining requests
        let remaining_requests: Vec<String> = REQUEST_PROCESSORS.iter()
            .map(|entry| entry.key().clone())
            .collect();

        for request_id in remaining_requests {
            warn!("Force cleaning up request: {}", request_id);
            cleanup_request_processing_state(&request_id);
        }

        info!("All  requests completed");
    }

    // Phase 9: Flush any batched agent payloads before shutdown
    let batch_count = AGENT_BATCH_BUFFER.len();
    if batch_count > 0 {
        info!("Flushing {} batched agent payload(s) before shutdown", batch_count);
        send_batched_payloads(newrelic_client, config).await;
    }
}

/// Create per-request context for  request handling
/// Create per-request processing state for  request handling
fn create_request_processing_state(
    request_id: &str,
    invoked_function_arn: &str,
    processor_factory: &Arc<ProcessorFactory>
) -> RequestProcessingState {
    // Create context
    let context = Arc::new(Mutex::new(InvocationContext {
        request_id: request_id.to_string(),
        invoked_function_arn: invoked_function_arn.to_string(),
        trace_id: None,
    }));

    // Create only platform processor - log processor will be global
    let platform_processor = processor_factory.create_platform_processor(context.clone());

    // Create agent buffer
    let agent_buffer = Arc::new(Mutex::new(Vec::new()));

    // Create coordination channel for agent payload arrival
    let (payload_tx, payload_rx) = mpsc::unbounded_channel();
    PAYLOAD_COORDINATION.insert(request_id.to_string(), payload_tx);

    // Create runtime.done channel (telemetry listener will signal this)
    let (runtime_done_tx, runtime_done_rx) = mpsc::unbounded_channel();
    RUNTIME_DONE_CHANNELS.insert(request_id.to_string(), runtime_done_tx);

    let state = RequestProcessingState {
        request_id: request_id.to_string(),
        context: context.clone(),
        platform_processor,
        agent_buffer: agent_buffer.clone(),
        coordination_rx: Some(payload_rx),
        runtime_done_rx: Some(runtime_done_rx),
    };

    // Store in global DashMaps for  access
    REQUEST_CONTEXTS.insert(request_id.to_string(), context);
    REQUEST_AGENT_BUFFERS.insert(request_id.to_string(), agent_buffer);

    debug!("Created per-request processing state for {} (using global log processor)", request_id);
    state
}

/// Create per-request agent buffer for  request handling
fn create_request_agent_buffer(request_id: &str) -> Arc<Mutex<Vec<Vec<u8>>>> {
    let buffer = Arc::new(Mutex::new(Vec::new()));
    REQUEST_AGENT_BUFFERS.insert(request_id.to_string(), buffer.clone());
    info!("Created per-request agent buffer for {}", request_id);
    buffer
}

/// Get per-request context (for  requests)
fn get_request_context(request_id: &str) -> Option<Arc<Mutex<InvocationContext>>> {
    REQUEST_CONTEXTS.get(request_id).map(|entry| entry.value().clone())
}

/// Get per-request agent buffer (for  requests)
fn get_request_agent_buffer(request_id: &str) -> Option<Arc<Mutex<Vec<Vec<u8>>>>> {
    REQUEST_AGENT_BUFFERS.get(request_id).map(|entry| entry.value().clone())
}

/// Clean up per-request processing state after processing
fn cleanup_request_processing_state(request_id: &str) {
    // Clean up request processing state
    if REQUEST_PROCESSORS.remove(request_id).is_some() {
        debug!("Cleaned up request processing state for {}", request_id);
    }

    // Clean up context
    if REQUEST_CONTEXTS.remove(request_id).is_some() {
        debug!("Cleaned up context for request {}", request_id);
    }

    // Clean up agent buffer
    if REQUEST_AGENT_BUFFERS.remove(request_id).is_some() {
        debug!("Cleaned up agent buffer for request {}", request_id);
    }

    // Clean up payload coordination channel
    cleanup_payload_coordination_channel(request_id);

    // Clean up runtime.done channel
    if RUNTIME_DONE_CHANNELS.remove(request_id).is_some() {
        debug!("Cleaned up runtime.done channel for request {}", request_id);
    }

    // Clean up any pending report for this request
    if PENDING_REPORTS.remove(request_id).is_some() {
        debug!("Cleaned up pending platform.report for request {}", request_id);
    }
}

// --- BATCH MANAGEMENT HELPER FUNCTIONS ---

/// Add agent payload to batch buffer
fn add_to_batch(
    request_id: String,
    agent_bytes: Vec<u8>,
    report_line: Option<String>,
    arn: String,
) {
    let timestamp = chrono::Utc::now();

    AGENT_BATCH_BUFFER.insert(
        request_id.clone(),
        BatchedAgentPayload {
            request_id,
            agent_payload_bytes: Arc::new(agent_bytes),
            report_line,
            invoked_function_arn: arn,
            timestamp,
        }
    );

    // Update metadata
    let mut meta = BATCH_META.lock().unwrap();
    meta.agent_count += 1;
    if meta.oldest_timestamp.is_none() {
        meta.oldest_timestamp = Some(timestamp);
    }

    info!("Added agent payload to batch (total buffered: {})", meta.agent_count);
}

/// Check if batch should be sent based on thresholds
fn should_send_batch() -> bool {
    let meta = BATCH_META.lock().unwrap();

    // Condition 1: 3+ agent payloads
    if meta.agent_count >= 3 {
        debug!("Batch threshold reached: {} agents", meta.agent_count);
        return true;
    }

    // Condition 2: Oldest payload > 5 minutes
    if let Some(oldest) = meta.oldest_timestamp {
        let age = chrono::Utc::now() - oldest;
        if age > chrono::Duration::seconds(300) {
            debug!("Batch timeout reached: oldest payload is {:?} old", age);
            return true;
        }
    }

    false
}

/// Get all batched payloads and clear the buffer
fn get_and_clear_batch() -> Vec<BatchedAgentPayload> {
    let items: Vec<BatchedAgentPayload> = AGENT_BATCH_BUFFER
        .iter()
        .map(|entry| entry.value().clone())
        .collect();

    AGENT_BATCH_BUFFER.clear();

    // Reset metadata
    let mut meta = BATCH_META.lock().unwrap();
    meta.agent_count = 0;
    meta.oldest_timestamp = None;

    items
}

/// Send agent payload with optional report immediately (for cold start or when both ready)
async fn send_agent_with_report_immediately(
    request_id: String,
    invoked_function_arn: String,
    agent_payloads: Vec<Vec<u8>>,
    report_line: Option<String>,
    newrelic_client: Arc<crate::newrelic::client::NewRelicClient>,
    config: Arc<crate::config::ExtensionConfig>,
    _global_log_processor: Arc<crate::logs::processor::LogProcessor>,
) {
    let has_report = report_line.is_some();
    debug!("Sending agent payload immediately for {} (with report: {})", request_id, has_report);

    for payload_bytes in agent_payloads {
        // Build log events array with optional report
        let mut log_events = Vec::new();

        // Add agent payload FIRST (try UTF-8 first to avoid allocation)
        let agent_str = match std::str::from_utf8(&payload_bytes) {
            Ok(s) => s.to_string(),
            Err(_) => String::from_utf8_lossy(&payload_bytes).to_string(),
        };
        log_events.push(serde_json::json!({
            "id": request_id,
            "message": agent_str,
            "timestamp": chrono::Utc::now().timestamp_millis(),
        }));

        // Add report line SECOND (if available)
        if let Some(ref report) = report_line {
            log_events.push(serde_json::json!({
                "id": request_id,
                "message": report,
                "timestamp": chrono::Utc::now().timestamp_millis(),
            }));
        }

        // Wrap in New Relic format
        let entry = serde_json::json!({
            "logEvents": log_events,
            "logGroup": format!("/aws/lambda/{}", config.aws.function_name),
            "logStream": format!("newrelic-lambda-extension:{}", crate::EXTENSION_VERSION),
            "messageType": "",
            "owner": "",
        });

        let payload = serde_json::json!({
            "context": {
                "function_name": config.aws.function_name,
                "invoked_function_arn": invoked_function_arn,
                "log_group_name": format!("/aws/lambda/{}", config.aws.function_name),
                "log_stream_name": format!("newrelic-lambda-extension:{}", crate::EXTENSION_VERSION),
            },
            "entry": entry.to_string(),
        });

        let payload_json = payload.to_string();

        // Send to New Relic
        if let Err(e) = newrelic_client.send_agent_payload(&config, &payload_json).await {
            error!("Failed to send agent payload for {}: {}", request_id, e);
        }
    }
}

/// Send batched agent payloads (3+ payloads or timeout reached)
async fn send_batched_payloads(
    newrelic_client: Arc<crate::newrelic::client::NewRelicClient>,
    config: Arc<crate::config::ExtensionConfig>,
) {
    let batch_items = get_and_clear_batch();

    if batch_items.is_empty() {
        debug!("No batched payloads to send");
        return;
    }

    debug!("Sending batch of {} agent payloads", batch_items.len());

    // Build log events array from all batched items
    let mut log_events = Vec::new();

    for item in &batch_items {
        // Add agent payload FIRST (try UTF-8 first to avoid allocation)
        let agent_str = match std::str::from_utf8(&*item.agent_payload_bytes) {
            Ok(s) => s.to_string(),
            Err(_) => String::from_utf8_lossy(&*item.agent_payload_bytes).to_string(),
        };
        log_events.push(serde_json::json!({
            "id": item.request_id,
            "message": agent_str,
            "timestamp": item.timestamp.timestamp_millis(),
        }));

        // Add report line SECOND (if present)
        if let Some(ref report) = item.report_line {
            log_events.push(serde_json::json!({
                "id": item.request_id,
                "message": report,
                "timestamp": item.timestamp.timestamp_millis(),
            }));
        }
    }

    // Use most recent item's ARN for context
    let most_recent = batch_items.last().unwrap();

    // Wrap in New Relic format
    let entry = serde_json::json!({
        "logEvents": log_events,
        "logGroup": format!("/aws/lambda/{}", config.aws.function_name),
        "logStream": format!("newrelic-lambda-extension:{}", crate::EXTENSION_VERSION),
        "messageType": "",
        "owner": "",
    });

    let payload = serde_json::json!({
        "context": {
            "function_name": config.aws.function_name,
            "invoked_function_arn": most_recent.invoked_function_arn,
            "log_group_name": format!("/aws/lambda/{}", config.aws.function_name),
            "log_stream_name": format!("newrelic-lambda-extension:{}", crate::EXTENSION_VERSION),
        },
        "entry": entry.to_string(),
    });

    let payload_json = payload.to_string();

    // Send to New Relic
    if let Err(e) = newrelic_client.send_agent_payload(&config, &payload_json).await {
        error!("Failed to send batched payloads: {}", e);
    } else {
        info!("Successfully sent batch of {} payloads", batch_items.len());
    }
}

/// Check pending agent payloads for a given request context
/// Returns (request_buffer_size, global_buffer_size)
fn check_pending_agent_payloads(context: &Option<(String, String)>) -> (usize, usize) {
    if let Some((request_id, _)) = context {
        let request_buffer = get_request_agent_buffer(request_id);
        let request_size = if let Some(request_buffer) = request_buffer {
            if let Ok(buffer) = request_buffer.lock() {
                buffer.len()
            } else {
                0
            }
        } else {
            0
        };
        
        (request_size, 0)
    } else {
        (0, 0)
    }
}

/// Cleanup old failed payloads (called during initialization)
fn cleanup_old_failed_payloads() {
    if let Ok(mut failed_payloads) = FAILED_AGENT_PAYLOADS.lock() {
        let initial_count = failed_payloads.len();
        let now = chrono::Utc::now();
        
        failed_payloads.retain(|payload| {
            let age = now.signed_duration_since(payload.failed_at);
            age.num_hours() <= 24 // Keep payloads newer than 24 hours
        });
        
        let removed_count = initial_count - failed_payloads.len();
        if removed_count > 0 {
            debug!("Cleaned up {} old failed agent payloads (kept {} recent ones)",
                 removed_count, failed_payloads.len());
        }
    }
}

/// Retry failed agent payloads during final flush
async fn retry_failed_agent_payloads(
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
) {
    let mut retry_successful_count = 0;
    let mut retry_failed_count = 0;
    
    // Take all failed payloads for retry (this clears the buffer)
    let failed_payloads = {
        if let Ok(mut failed_payloads) = FAILED_AGENT_PAYLOADS.lock() {
            std::mem::take(&mut *failed_payloads)
        } else {
            error!("Failed to lock FAILED_AGENT_PAYLOADS for retry");
            return;
        }
    };
    
    if failed_payloads.is_empty() {
        debug!("No failed agent payloads to retry");
        return;
    }
    
    info!("Retrying {} failed agent payloads during final flush", failed_payloads.len());
    
    for mut failed_payload in failed_payloads {
        failed_payload.retry_count += 1;
        
        // Skip payloads that are too old (older than 24 hours)
        let age = chrono::Utc::now().signed_duration_since(failed_payload.failed_at);
        if age.num_hours() > 24 {
            warn!("Dropping agent payload that's too old ({} hours) for request {}", 
                 age.num_hours(), failed_payload.request_id);
            continue;
        }
        
        // Skip payloads that have been retried too many times
        if failed_payload.retry_count > 5 {
            warn!("Dropping agent payload after {} retries for request {}", 
                 failed_payload.retry_count, failed_payload.request_id);
            continue;
        }
        
        debug!("Retrying agent payload for request {} (attempt {})", 
               failed_payload.request_id, failed_payload.retry_count);
        
        match send_agent_payload_to_newrelic(
            &failed_payload.payload_bytes,
            &failed_payload.request_id,
            &failed_payload.invoked_function_arn,
            newrelic_client,
            config,
        ).await {
            Ok(_) => {
                retry_successful_count += 1;
                debug!("Successfully retried agent payload for request {}", failed_payload.request_id);
            }
            Err(_) => {
                retry_failed_count += 1;
                
                // Put it back in the buffer for next invocation if not too many retries
                if failed_payload.retry_count <= 5 {
                    if let Ok(mut failed_payloads) = FAILED_AGENT_PAYLOADS.lock() {
                        failed_payloads.push(failed_payload);
                    }
                }
            }
        }
    }
    
    if retry_successful_count > 0 || retry_failed_count > 0 {
        info!("Agent payload retry results: {} successful, {} still failed", 
             retry_successful_count, retry_failed_count);
    }
}

/// Comprehensive flush of all telemetry before freeze mode

/// Update global invocation context (legacy function, now also creates per-request context)











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



/// Create wrapped agent payload JSON string
/// Create New Relic log format with agent data in message field
/// NOTE: Trace ID extraction is handled separately in process_and_send_agent_payload
fn create_wrapped_agent_payload_json(
    payload_bytes: &[u8],
    function_name: &str,
    invoked_function_arn: &str,
    log_group_name: &str,
    request_id: &str,
    config: &Arc<config::ExtensionConfig>,
) -> String {
    debug!("Processing agent data of {} bytes for function: {}", payload_bytes.len(), function_name);

    // Create New Relic log event format with agent data as message
    create_newrelic_log_format(payload_bytes, function_name, invoked_function_arn, log_group_name, request_id, config)
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
    config: &Arc<config::ExtensionConfig>,
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

    // Create context object with base fields
    let mut context = serde_json::json!({
        "function_name": function_name,
        "invoked_function_arn": invoked_function_arn,
        "log_group_name": log_group_name,
        "log_stream_name": format!("{}:{}", EXTENSION_NAME, EXTENSION_VERSION)
    });

    // Add version detail tags to context if enabled
    if config.new_relic.add_version_detail_tags {
        // Use cached version info (already detected once during initialization)
        let version_info = version::VersionInfo::get_or_detect();
        let version_tags = version_info.as_tags();

        if let Some(context_obj) = context.as_object_mut() {
            for (key, value) in version_tags {
                context_obj.insert(key, serde_json::json!(value));
            }
            debug!("Added {} version detail tags to agent payload context", context_obj.len() - 4); // Subtract the 4 base fields
        }
    }

    // Create final payload with context and stringified entry
    let final_payload = serde_json::json!({
        "context": context,
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
            "timeoutMs": 100  // Reduced from 1000ms to 100ms for faster cold start log delivery
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

                // Standardized backoff: 200ms, 400ms, 900ms
                let delay = match retry_count {
                    1 => Duration::from_millis(200),
                    2 => Duration::from_millis(400),
                    _ => Duration::from_millis(900),
                };
                warn!("Retrying extension event polling in {:?}...", delay);
                tokio::time::sleep(delay).await;
            }
        }
    }
}



