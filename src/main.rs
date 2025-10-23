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
    sync::{Arc, Mutex, RwLock},
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



// --- CONCURRENT REQUEST HANDLING ---
// --- CONCURRENT REQUEST HANDLING ---
// Per-request contexts to handle concurrent Lambda invocations safely
// Using RwLock for read-heavy structures (many reads per telemetry event, few writes on invoke/cleanup)
static REQUEST_CONTEXTS: Lazy<Arc<RwLock<HashMap<String, Arc<Mutex<InvocationContext>>>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));
static REQUEST_AGENT_BUFFERS: Lazy<Arc<RwLock<HashMap<String, Arc<Mutex<Vec<Vec<u8>>>>>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

// Global coordination channels per request for agent payload processing
static PAYLOAD_COORDINATION: Lazy<Arc<RwLock<HashMap<String, mpsc::UnboundedSender<()>>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

// Per-request processing state management
static REQUEST_PROCESSORS: Lazy<Arc<RwLock<HashMap<String, RequestProcessingState>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

// Failed agent payloads buffer for retry across invocations
// Keep as Mutex - write-heavy (every failure writes)
static FAILED_AGENT_PAYLOADS: Lazy<Arc<Mutex<Vec<FailedAgentPayload>>>> =
    Lazy::new(|| Arc::new(Mutex::new(Vec::new())));

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
    let parallel_start = std::time::Instant::now();
    let (license_key_result, registration_result) = tokio::join!(
        resolve_license_key_with_aws_fallback(&config),
        initialize_lambda_runtime_client_and_register()
    );
    let parallel_duration = parallel_start.elapsed();
    info!("License key resolution + registration completed in {:?}", parallel_duration);

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

    // FAST PATH: If license key is directly provided, return immediately without AWS initialization
    if !credentials_config.license_key.is_empty() {
        debug!("License key provided directly via NEW_RELIC_LICENSE_KEY, skipping AWS credential resolution");
        return Ok(Some(credentials_config.license_key.clone()));
    }

    // SLOW PATH: Check if AWS services are actually needed for license key resolution
    // Only initialize AWS clients if user explicitly configured AWS-based credential sources
    let aws_services_required =
        std::env::var("NEW_RELIC_LICENSE_KEY_SECRET").is_ok() ||
        std::env::var("NEW_RELIC_LICENSE_KEY_SSM_PARAMETER_NAME").is_ok() ||
        !credentials_config.license_key_secret_id.is_empty() ||
        !credentials_config.license_key_ssm_parameter_name.is_empty();

    if aws_services_required {
        debug!("AWS credential sources configured, initializing AWS clients for license key resolution");
        match get_new_relic_license_key(&credentials_config).await {
            Ok(key) => {
                info!("Successfully obtained New Relic license key from AWS");
                Ok(Some(key))
            }
            Err(e) => {
                warn!("No license key found from AWS sources: {}. Extension will run in no-op mode.", e);
                Ok(None)
            }
        }
    } else {
        warn!("No license key available and no AWS credential sources configured. Extension will run in no-op mode.");
        Ok(None)
    }
}

/// Initialize HTTP client with appropriate timeout and connection settings
/// Matches Go's http.Client default behavior with connection pooling and keepalive
async fn initialize_http_client_with_timeout() -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
    Ok(Client::builder()
        // Remove global timeout - let individual requests set their own timeouts
        // Extension event polling needs 5+ minute timeouts, but other requests need shorter ones

        // Enable TCP keepalive to maintain persistent connections (matches Go's http.Client defaults)
        .tcp_keepalive(Some(Duration::from_secs(30)))

        // Enable connection pooling (keeps connections alive between requests)
        .pool_idle_timeout(Some(Duration::from_secs(90)))
        .pool_max_idle_per_host(10)

        // Enable HTTP/1.1 keepalive headers
        .http1_title_case_headers()

        // Don't fail on connection errors immediately - let retry logic handle it
        .connect_timeout(Duration::from_secs(10))

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

/// Route agent payload to the correct per-request buffer
async fn route_payload_to_request_buffer(payload_bytes: Vec<u8>) {
    // Find the most recent active request
    let current_request_id = {
        if let Ok(contexts) = REQUEST_CONTEXTS.read() {
            // Get the most recent request (assumes HashMap iteration order reflects insertion order)
            contexts.keys().last().cloned()
        } else {
            None
        }
    };
    
    if let Some(request_id) = current_request_id {
        // Store in request-specific buffer
        if let Some(request_buffer) = get_request_agent_buffer(&request_id) {
            match request_buffer.lock() {
                Ok(mut buffer) => {
                    buffer.push(payload_bytes);
                    info!("Stored agent payload in request buffer for {} (buffer size now {})", 
                         request_id, buffer.len());

                    // Notify the request's coordination channel if available
                    if let Ok(channels) = PAYLOAD_COORDINATION.read() {
                        if let Some(tx) = channels.get(&request_id) {
                            let _ = tx.send(());
                        }
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

    if let Ok(mut channels) = PAYLOAD_COORDINATION.write() {
        channels.insert(request_id.to_string(), tx);
    }

    rx
}

fn cleanup_payload_coordination_channel(request_id: &str) {
    if let Ok(mut channels) = PAYLOAD_COORDINATION.write() {
        if channels.remove(request_id).is_some() {
            debug!("Cleaned up coordination channel for request {}", request_id);
        }
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
            info!("Updated trace ID {} for request {}", trace_id, request_id);
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
    let mut probably_timeout = false;
    let mut last_request_id = String::new();

    loop {
        debug!("mainLoop: waiting for next lambda invocation event...");

        // Fetch next Lambda runtime event
        // CRITICAL: This call blocks until Lambda sends INVOKE or SHUTDOWN event
        let runtime_event = match fetch_next_lambda_runtime_event(&components.client, &components.extension_id).await {
            Ok(event) => event,
            Err(e) => {
                // Match Go implementation: log error, report to Extension API, and continue
                error!("NextEventError.Main: {}", e);

                // Report error to Lambda Extension API (critical for maintaining proper state)
                let error_ref: &dyn std::error::Error = &*e;
                if let Err(report_err) = report_exit_error(&components.client, &components.extension_id, "NextEventError.Main", error_ref).await {
                    error!("Failed to report exit error: {}", report_err);
                }

                continue;
            }
        };

        event_counter += 1;

        // Check if previous request timed out and process any late-arriving telemetry
        if probably_timeout && !last_request_id.is_empty() {
            info!("Checking for late telemetry after suspected timeout for request {}", last_request_id);
            // Process any telemetry that arrived late (non-blocking check)
            process_pending_agent_payloads_non_blocking(
                &last_request_id,
                &components.newrelic_client,
                &components.config,
                &components.global_log_processor,
            ).await;
            probably_timeout = false;
        }

        match runtime_event {
            LambdaRuntimeEvent::Invoke { request_id, invoked_function_arn } => {
                let event_processing_start_time = std::time::Instant::now();
                last_request_id = request_id.clone();

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
                let coordination_rx = if let Ok(mut processors) = REQUEST_PROCESSORS.write() {
                    let mut state = request_state;
                    let rx = state.coordination_rx.take();
                    processors.insert(request_id.clone(), state);
                    rx
                } else {
                    None
                };

                // CRITICAL: Wait for platform.runtimeDone, then wait 400ms for agent telemetry
                // This is the correct flow:
                // 1. Wait for EITHER platform.runtimeDone OR agent telemetry (whichever comes first)
                // 2. If runtimeDone comes first, wait additional 400ms for agent telemetry
                // 3. If agent telemetry comes first, process immediately
                // 4. Return to /next call

                if let Some(mut agent_rx) = coordination_rx {
                    // Wait for either runtime done or agent telemetry
                    tokio::select! {
                        _ = components.runtime_done_rx.recv() => {
                            info!("Received platform.runtimeDone for request {}, waiting 200ms for agent telemetry", request_id);

                            // Now wait 200ms for agent telemetry
                            let telemetry_timeout = Duration::from_millis(200);
                            tokio::select! {
                                _ = agent_rx.recv() => {
                                    info!("Agent telemetry received within 200ms after runtimeDone for request {}", request_id);
                                    process_agent_payloads_for_request(
                                        &request_id,
                                        &invoked_function_arn,
                                        &components.newrelic_client,
                                        &components.config,
                                        &components.global_log_processor,
                                    ).await;
                                    probably_timeout = false;
                                }
                                _ = tokio::time::sleep(telemetry_timeout) => {
                                    info!("No agent telemetry within 200ms after runtimeDone for request {}", request_id);
                                    probably_timeout = true;
                                }
                            }
                        }
                        _ = agent_rx.recv() => {
                            info!("Agent telemetry arrived before runtimeDone for request {}", request_id);
                            process_agent_payloads_for_request(
                                &request_id,
                                &invoked_function_arn,
                                &components.newrelic_client,
                                &components.config,
                                &components.global_log_processor,
                            ).await;
                            probably_timeout = false;

                            // Still wait for runtimeDone to ensure function completed
                            let _ = components.runtime_done_rx.recv().await;
                        }
                    }
                } else {
                    warn!("No coordination channel for request {} - just waiting for runtimeDone", request_id);
                    let _ = components.runtime_done_rx.recv().await;
                }

                // Flush logs and platform data before returning to /next
                if let Err(e) = components.global_log_processor.flush().await {
                    error!("Failed to flush logs for request {}: {}", request_id, e);
                }

                // Clean up request state
                cleanup_request_processing_state(&request_id);

                let event_processing_time = event_processing_start_time.elapsed();
                
                if event_counter == 1 {
                    info!("COLD START: First invocation processed in {:?} (request_id: {})", 
                          event_processing_time, request_id);
                } else {
                    // Set warm start flag for performance optimization
                    IS_WARM_START.store(true, std::sync::atomic::Ordering::Relaxed);
                    info!("WARM START: Event {} processed in {:?} (request_id: {})", 
                          event_counter, event_processing_time, request_id);
                }
            }
            LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
                info!("Extension shutting down: {}", shutdown_reason);
                
                // Wait for all concurrent requests to complete
                wait_for_all_requests_completion().await;
                break;
            }
        }
    }

    event_counter
}

/// Process agent payloads for a specific request (called after telemetry arrives)
async fn process_agent_payloads_for_request(
    request_id: &str,
    invoked_function_arn: &str,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
    global_log_processor: &Arc<LogProcessor>,
) {
    info!("Processing agent payloads for request: {}", request_id);

    // Get agent buffer and platform processor from state
    let (agent_buffer, platform_processor) = {
        if let Ok(processors) = REQUEST_PROCESSORS.read() {
            if let Some(state) = processors.get(request_id) {
                (Some(state.agent_buffer.clone()), Some(state.platform_processor.clone()))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        }
    };

    let Some(agent_buffer) = agent_buffer else {
        warn!("No processing state found for request: {}", request_id);
        return;
    };

    // Get payloads from request-specific buffer
    let payloads = {
        if let Ok(mut buffer) = agent_buffer.lock() {
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

    info!("Processing {} agent payloads for request: {}", payloads.len(), request_id);

    // Process each payload
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

    // Flush platform processor
    if let Some(platform_processor) = platform_processor {
        if let Err(e) = platform_processor.flush().await {
            error!("Failed to flush platform processor for request {}: {}", request_id, e);
        }
    }
}

/// Process any pending agent payloads non-blocking (for timeout recovery)
async fn process_pending_agent_payloads_non_blocking(
    request_id: &str,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
    global_log_processor: &Arc<LogProcessor>,
) {
    // Check if there are any payloads in the buffer
    let buffer = get_request_agent_buffer(request_id);

    if let Some(buffer) = buffer {
        let has_payloads = {
            if let Ok(buf) = buffer.lock() {
                !buf.is_empty()
            } else {
                false
            }
        };

        if has_payloads {
            info!("Processing late-arriving telemetry for request: {}", request_id);
            // We don't have invoked_function_arn here, but it's stored in the context
            let invoked_function_arn = {
                if let Some(context) = get_request_context(request_id) {
                    if let Ok(ctx) = context.lock() {
                        ctx.invoked_function_arn.clone()
                    } else {
                        String::new()
                    }
                } else {
                    String::new()
                }
            };

            if !invoked_function_arn.is_empty() {
                process_agent_payloads_for_request(
                    request_id,
                    &invoked_function_arn,
                    newrelic_client,
                    config,
                    global_log_processor,
                ).await;
            }
        }
    }
}


/// Wait for all concurrent requests to complete
async fn wait_for_all_requests_completion() {
    info!("Waiting for all concurrent requests to complete...");
    
    // Wait a reasonable time for requests to complete
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    // Force cleanup of any remaining requests
    let remaining_requests = {
        if let Ok(processors) = REQUEST_PROCESSORS.read() {
            processors.keys().cloned().collect::<Vec<_>>()
        } else {
            Vec::new()
        }
    };
    
    for request_id in remaining_requests {
        warn!("Force cleaning up request: {}", request_id);
        cleanup_request_processing_state(&request_id);
    }
    
    info!("All concurrent requests completed");
}

/// Create per-request context for concurrent request handling
/// Create per-request processing state for concurrent request handling
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

    // Create coordination channel
    let (tx, rx) = mpsc::unbounded_channel();

    // Store coordination sender
    if let Ok(mut channels) = PAYLOAD_COORDINATION.write() {
        channels.insert(request_id.to_string(), tx);
    }

    let state = RequestProcessingState {
        request_id: request_id.to_string(),
        context: context.clone(),
        platform_processor,
        agent_buffer: agent_buffer.clone(),
        coordination_rx: Some(rx),
    };

    // Store in global maps for backward compatibility
    if let Ok(mut contexts) = REQUEST_CONTEXTS.write() {
        contexts.insert(request_id.to_string(), context);
    }
    if let Ok(mut buffers) = REQUEST_AGENT_BUFFERS.write() {
        buffers.insert(request_id.to_string(), agent_buffer);
    }

    info!("Created per-request processing state for {} (using global log processor)", request_id);
    state
}

/// Create per-request agent buffer for concurrent request handling
fn create_request_agent_buffer(request_id: &str) -> Arc<Mutex<Vec<Vec<u8>>>> {
    let buffer = Arc::new(Mutex::new(Vec::new()));

    // Store in per-request buffers map
    if let Ok(mut buffers) = REQUEST_AGENT_BUFFERS.write() {
        buffers.insert(request_id.to_string(), buffer.clone());
        info!("Created per-request agent buffer for {}", request_id);
    } else {
        error!("Failed to lock REQUEST_AGENT_BUFFERS for request {}", request_id);
    }

    buffer
}

/// Get per-request context (for concurrent requests)
fn get_request_context(request_id: &str) -> Option<Arc<Mutex<InvocationContext>>> {
    if let Ok(contexts) = REQUEST_CONTEXTS.read() {
        return contexts.get(request_id).cloned();
    }
    None
}

/// Get per-request agent buffer (for concurrent requests)
fn get_request_agent_buffer(request_id: &str) -> Option<Arc<Mutex<Vec<Vec<u8>>>>> {
    if let Ok(buffers) = REQUEST_AGENT_BUFFERS.read() {
        return buffers.get(request_id).cloned();
    }
    None
}

/// Clean up per-request processing state after processing
fn cleanup_request_processing_state(request_id: &str) {
    // Clean up request processing state
    if let Ok(mut processors) = REQUEST_PROCESSORS.write() {
        if processors.remove(request_id).is_some() {
            debug!("Cleaned up request processing state for {}", request_id);
        }
    }

    // Clean up context
    if let Ok(mut contexts) = REQUEST_CONTEXTS.write() {
        if contexts.remove(request_id).is_some() {
            debug!("Cleaned up context for request {}", request_id);
        }
    }

    // Clean up agent buffer
    if let Ok(mut buffers) = REQUEST_AGENT_BUFFERS.write() {
        if buffers.remove(request_id).is_some() {
            debug!("Cleaned up agent buffer for request {}", request_id);
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
            info!("Cleaned up {} old failed agent payloads (kept {} recent ones)", 
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
                info!("Successfully retried agent payload for request {}", failed_payload.request_id);
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
            Err(e) => {
                // In no-op mode, if we get errors (especially 403), we need to handle gracefully
                warn!("No-op mode: Error fetching next event: {:?}", e);

                // Report error to Lambda Extension API to maintain proper state
                let error_ref: &dyn std::error::Error = &*e;
                if let Err(report_err) = report_exit_error(client, extension_id, "NextEventError.Noop", error_ref).await {
                    error!("No-op mode: Failed to report exit error: {}", report_err);
                }

                // If we get 403 errors even in no-op mode, the extension state is completely broken
                // Wait longer and keep trying - this prevents the container from being killed
                if e.to_string().contains("403") || e.to_string().contains("Forbidden") {
                    error!("No-op mode: Extension API state is broken, waiting 200ms before retry");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                } else {
                    // For other errors, wait 200ms
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
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

/// Reports an error to the Lambda Extension API
/// This is critical for maintaining proper state with the Lambda Extensions API
async fn report_exit_error(client: &Client, ext_id: &str, error_type: &str, error: &dyn std::error::Error) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let runtime_api = match env::var("AWS_LAMBDA_RUNTIME_API") {
        Ok(api) => api,
        Err(_) => {
            error!("AWS_LAMBDA_RUNTIME_API not set, cannot report error");
            return Ok(()); // Don't fail if we can't report
        }
    };

    let url = format!("http://{}/2020-01-01/extension/exit/error", runtime_api);

    let payload = serde_json::json!({
        "errorMessage": error.to_string(),
        "errorType": error_type
    });

    debug!("Reporting exit error to Lambda Extension API: {} - {}", error_type, error);

    // Best effort - don't fail if we can't report the error
    match client
        .post(&url)
        .header(EXTENSION_ID_HEADER, ext_id)
        .json(&payload)
        .timeout(Duration::from_secs(5))
        .send()
        .await
    {
        Ok(response) => {
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_else(|_| "Failed to read response body".to_string());
                warn!("Failed to report exit error with status: {}, body: {}", status, body);
            } else {
                debug!("Successfully reported exit error to Lambda Extension API");
            }
        }
        Err(e) => {
            warn!("Failed to send exit error report: {}", e);
        }
    }

    Ok(())
}

/// Fetches the next event from the Lambda Runtime API.
/// Handles transient connection errors that can occur after container freeze/thaw cycles
async fn fetch_next_lambda_runtime_event(client: &Client, ext_id: &str) -> Result<LambdaRuntimeEvent, Box<dyn std::error::Error + Send + Sync>> {
    let runtime_api = env::var("AWS_LAMBDA_RUNTIME_API")
        .map_err(|_| "AWS_LAMBDA_RUNTIME_API not set")?;

    let url = format!("http://{}/2020-01-01/extension/event/next", runtime_api);

    // Make a single /next call with retry ONLY for connection errors (not HTTP errors)
    // Connection errors can happen after Lambda freeze/thaw cycles when TCP connections go stale
    // We retry connection errors up to 3 times with exponential backoff
    // HTTP errors (like 403) are NOT retried to avoid concurrent /next calls
    const MAX_CONNECTION_RETRIES: u32 = 3;
    let mut last_error: Option<Box<dyn std::error::Error + Send + Sync>> = None;

    for attempt in 1..=MAX_CONNECTION_RETRIES {
        debug!("Calling /next API (attempt {})", attempt);

        match client
            .get(&url)
            .header(EXTENSION_ID_HEADER, ext_id)
            .timeout(std::time::Duration::from_secs(300)) // 5 minute timeout for event polling
            .send()
            .await
        {
            Ok(response) => {
                // Check if response was successful
                if !response.status().is_success() {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_else(|_| "Failed to read response body".to_string());
                    error!("Next event request failed with status: {}, body: {}", status, body);
                    // HTTP errors are NOT retried - return immediately
                    return Err(format!("Next event request failed with status: {}", status).into());
                }

                // Parse and return the event
                let event: LambdaRuntimeEvent = response.json().await?;
                if attempt > 1 {
                    info!("Successfully connected to /next API after {} attempts", attempt);
                }
                return Ok(event);
            }
            Err(e) => {
                // Check if this is a connection error (not an HTTP error)
                let is_connection_error = e.is_connect() || e.is_timeout() ||
                    e.to_string().contains("error sending request") ||
                    e.to_string().contains("connection") ||
                    e.to_string().contains("broken pipe");

                if is_connection_error && attempt < MAX_CONNECTION_RETRIES {
                    // Transient connection error - wait and retry
                    let backoff_ms = 100 * (2_u64.pow(attempt - 1)); // Exponential backoff: 100ms, 200ms, 400ms
                    warn!("Connection error on /next call (attempt {}): {}. Retrying in {}ms...",
                         attempt, e, backoff_ms);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    last_error = Some(Box::new(e));
                    continue;
                } else {
                    // Non-retryable error or max retries exceeded
                    if attempt >= MAX_CONNECTION_RETRIES {
                        error!("Failed to connect to /next API after {} attempts", MAX_CONNECTION_RETRIES);
                    }
                    return Err(Box::new(e));
                }
            }
        }
    }

    // Should never reach here, but handle it just in case
    Err(last_error.unwrap_or_else(|| "Failed to fetch next event after retries".into()))
}



