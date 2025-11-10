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
mod apm;

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

// Global APM app instance (for sending platform.report metrics in APM mode)
static APM_APP: Lazy<Arc<tokio::sync::RwLock<Option<apm::ApmApp>>>> =
    Lazy::new(|| Arc::new(tokio::sync::RwLock::new(None)));


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

// CRITICAL: Track currently active request for agent payload routing
// Since agent payloads don't include request_id, we route to the most recent ACTIVE request
// This works because Lambda typically processes requests sequentially (though concurrent is possible)
static CURRENT_ACTIVE_REQUEST_ID: Lazy<Arc<Mutex<Option<String>>>> = 
    Lazy::new(|| Arc::new(Mutex::new(None)));

// --- PROCESSOR FACTORY FOR REQUEST-SCOPED PROCESSORS ---
#[derive(Debug, Clone)]
struct ProcessorFactory {
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<config::ExtensionConfig>,
    apm_app: apm::SharedApmApp,
}

impl ProcessorFactory {
    fn new(newrelic_client: Arc<NewRelicClient>, config: Arc<config::ExtensionConfig>, apm_app: apm::SharedApmApp) -> Self {
        Self { newrelic_client, config, apm_app }
    }
    
    fn create_log_processor(&self, request_context: Arc<Mutex<InvocationContext>>) -> Arc<LogProcessor> {
        Arc::new(LogProcessor::new(
            Arc::clone(&self.newrelic_client),
            Arc::clone(&self.config),
            request_context,
            Some(Arc::clone(&self.apm_app)),
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

#[derive(Debug)]
struct RequestProcessingState {
    context: Arc<Mutex<InvocationContext>>,
    platform_processor: Arc<PlatformProcessor>,
    agent_buffer: Arc<Mutex<Vec<Vec<u8>>>>,
    coordination_rx: Option<mpsc::UnboundedReceiver<()>>,
    runtime_done_rx: Option<mpsc::UnboundedReceiver<()>>,
}

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
    harvester_handle: tokio::task::JoinHandle<()>,
    global_log_processor: Arc<LogProcessor>,
    apm_app: apm::SharedApmApp,
}

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
        let noop_apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let noop_processor_factory = Arc::new(ProcessorFactory::new(
            noop_newrelic_client.clone(),
            config.clone(),
            noop_apm_app.clone()
        ));
        
        // Create dummy processors for harvester
        let dummy_context = Arc::new(Mutex::new(InvocationContext {
            request_id: "noop".to_string(),
            invoked_function_arn: "noop".to_string(),
            trace_id: None,
        }));
        let noop_log_processor = noop_processor_factory.create_log_processor(dummy_context.clone());
        let _noop_platform_processor = noop_processor_factory.create_platform_processor(dummy_context);
        
        return Ok(ExtensionComponents {
            client,
            extension_id,
            processor_factory: noop_processor_factory,
            newrelic_client: noop_newrelic_client,
            config: config.clone(),
            harvester_handle: tokio::spawn(async {}),
            global_log_processor: noop_log_processor,
            apm_app: Arc::new(tokio::sync::RwLock::new(None)),
        });
    }

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
        let noop_apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let noop_processor_factory = Arc::new(ProcessorFactory::new(
            noop_newrelic_client.clone(),
            config.clone(),
            noop_apm_app.clone()
        ));
        
        // Create dummy processors for harvester
        let dummy_context = Arc::new(Mutex::new(InvocationContext {
            request_id: "noop".to_string(),
            invoked_function_arn: "noop".to_string(),
            trace_id: None,
        }));
        let noop_log_processor = noop_processor_factory.create_log_processor(dummy_context.clone());
        let _noop_platform_processor = noop_processor_factory.create_platform_processor(dummy_context);
        
        return Ok(ExtensionComponents {
            client,
            extension_id,
            processor_factory: noop_processor_factory,
            newrelic_client: noop_newrelic_client,
            config: config.clone(),
            harvester_handle: tokio::spawn(async {}),
            global_log_processor: noop_log_processor,
            apm_app: Arc::new(tokio::sync::RwLock::new(None)),
        });
    };

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
    let (runtime_done_tx, _runtime_done_rx) = runtime_done_channels;

    info!("Extension components initialized - ID: {} (license key pre-validated)", extension_id);

    start_agent_payload_collector_background_task(agent_telemetry_rx);

    // Clean up very old failed payloads (older than 24 hours)
    cleanup_old_failed_payloads();

    // Start batch timeout background task (checks every 30 seconds for 5-minute timeout)
    start_batch_timeout_task(Arc::clone(&newrelic_client), Arc::clone(&config));

    // 9. Initialize APM app if APM mode is enabled (before processor factory)
    let apm_app = if config.new_relic.apm_lambda_mode {
        info!("APM Lambda mode enabled - initializing APM connection");
        let license_key = config.new_relic.license_key.clone()
            .expect("License key must be available for APM mode");
        
        match apm::ApmApp::new(
            license_key,
            config.new_relic.apm_host.clone(),
            config.new_relic.metric_endpoint.clone(),
            (*client).clone(),
        ).await {
            Ok(app) => {
                info!(
                    "APM app initialized successfully - Entity GUID: {}",
                    app.get_entity_guid()
                );
                // Store in global APM_APP for access by telemetry listener
                {
                    let mut global_apm = APM_APP.write().await;
                    *global_apm = Some(app);
                }
                // Return a clone of the global Arc
                Arc::clone(&APM_APP)
            }
            Err(e) => {
                warn!("Failed to initialize APM app: {} - continuing without APM mode", e);
                Arc::new(tokio::sync::RwLock::new(None))
            }
        }
    } else {
        debug!("APM Lambda mode disabled");
        Arc::new(tokio::sync::RwLock::new(None))
    };

    // Create processor factory for request-scoped processors (with apm_app)
    let processor_factory = Arc::new(ProcessorFactory::new(
        Arc::clone(&newrelic_client),
        Arc::clone(&config),
        Arc::clone(&apm_app)
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

    let (_harvester, harvester_handle) = start_harvester_background_task(
        vec![],
        config.new_relic.harvest_interval,
        &processor_factory,
    );

    Ok(ExtensionComponents {
        client,
        extension_id,
        processor_factory,
        newrelic_client,
        config,
        harvester_handle,
        global_log_processor: temp_log_processor,
        apm_app,
    })
}

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
        .connect_timeout(Duration::from_secs(10)) // Connection establishment timeout
        .pool_idle_timeout(Duration::from_secs(90)) // Keep connections alive longer for Lambda runtime API
        .pool_max_idle_per_host(10) // Allow more parallel connections for APM telemetry sending
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
/// 
/// CRITICAL: Agent payloads don't include request_id, so we route to the currently active request.
/// This works because:
/// 1. Lambda typically processes requests sequentially (though concurrent is possible)
/// 2. We track the active request_id in CURRENT_ACTIVE_REQUEST_ID
/// 3. Each request sets this before waiting for agent payload
/// 4. Each request clears this after processing
async fn route_payload_to_request_buffer(payload_bytes: Vec<u8>) {
    // Get the currently active request ID
    let current_request_id = CURRENT_ACTIVE_REQUEST_ID.lock()
        .ok()
        .and_then(|guard| guard.clone());

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
        // No active request - this could be a late payload from a previous request
        // Try to find ANY request buffer that's still alive (for APM mode warm starts)
        let any_request_id = REQUEST_AGENT_BUFFERS.iter()
            .next()
            .map(|entry| entry.key().clone());
            
        if let Some(request_id) = any_request_id {
            warn!("No active request - routing late agent payload to buffer: {}", request_id);
            if let Some(request_buffer) = REQUEST_AGENT_BUFFERS.get(&request_id) {
                if let Ok(mut buffer) = request_buffer.lock() {
                    buffer.push(payload_bytes);
                    debug!("Stored late agent payload in buffer for {} (buffer size now {})",
                         request_id, buffer.len());
                }
            }
        } else {
            warn!("No active requests found - agent payload lost!");
        }
    }
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
    apm_app: &apm::SharedApmApp,
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
    
    // Route to APM mode or standard mode
    let apm_app_guard = apm_app.read().await;
    if let Some(ref app) = *apm_app_guard {
        // APM mode: parse and send to APM collector
        info!("APM mode: Processing agent payload (size: {} bytes)", payload_bytes.len());
        match app.process_agent_payload(payload_bytes.to_vec()).await {
            Ok(_) => {
                info!("APM agent payload processed and sent successfully");
            }
            Err(e) => {
                error!("Failed to send agent payload to APM collector: {}", e);
                // Buffer for retry
                buffer_failed_agent_payload(
                    payload_bytes,
                    request_id,
                    invoked_function_arn,
                );
                warn!("APM agent payload buffered for retry (size: {} bytes)", payload_bytes.len());
            }
        }
    } else {
        // Standard mode: send to serverless ingest API
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
    }
    
    Ok(())
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

// ============================================================================
// COMMON HELPER FUNCTIONS (used by both APM and Standard mode)
// ============================================================================

/// Tag Lambda function once on first invocation
fn tag_lambda_function_once(invoked_function_arn: String) {
    static TAGGING_DONE: std::sync::Once = std::sync::Once::new();
    TAGGING_DONE.call_once(|| {
        info!("Spawning background task to tag Lambda function with version information");
        let version_info = version::VersionInfo::get_or_detect();
        version::tagging::tag_lambda_function_background(
            version_info.extension_version.clone(),
            version_info.agent_version.clone(),
            version_info.layer_version.clone(),
            invoked_function_arn,
        );
    });
}

/// Update global invocation context for telemetry processors
fn update_global_invocation_context(request_id: &str, invoked_function_arn: &str) {
    if let Ok(mut global_context) = CURRENT_INVOCATION_CONTEXT.lock() {
        global_context.request_id = request_id.to_string();
        global_context.invoked_function_arn = invoked_function_arn.to_string();
        global_context.trace_id = None; // Reset trace ID for new request
    }
}

/// Extract trace ID from agent payload if enabled in config
async fn extract_and_coordinate_trace_id(
    payload_bytes: &[u8],
    config: &Arc<config::ExtensionConfig>,
    log_processor: &Arc<LogProcessor>,
) {
    if !config.new_relic.collect_trace_id {
        return;
    }
    
    if let Ok(Some(trace_id)) = trace::extract_trace_id_from_payload(payload_bytes) {
        info!("Extracted trace ID: {}, coordinating with logs", trace_id);
        if let Err(e) = log_processor.on_trace_id_extracted(&trace_id).await {
            error!("Failed to coordinate logs with trace ID: {}", e);
        }
    }
}

// ============================================================================
// EVENT LOOP ROUTER
// ============================================================================

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
///
/// Routes to appropriate loop based on APM mode
async fn execute_main_telemetry_processing_loop(components: &mut ExtensionComponents) -> u32 {
    let is_apm_mode = components.apm_app.read().await.is_some();
    
    if is_apm_mode {
        info!("Starting APM mode event loop");
        execute_apm_mode_event_loop(components).await
    } else {
        info!("Starting standard mode event loop");
        execute_standard_mode_event_loop(components).await
    }
}

/// APM MODE EVENT LOOP - Optimized for APM Collector
/// 
/// Key characteristics:
/// - Sends agent payloads immediately to APM collector (5 telemetry types)
/// - No batching or platform.report coordination
/// - Keeps buffer alive on warm starts to catch late-arriving payloads
/// - Processes pending payloads from previous invocation on warm starts
async fn execute_apm_mode_event_loop(components: &mut ExtensionComponents) -> u32 {
    let mut event_counter = 0;

    loop {
        debug!("APM mode: waiting for next lambda invocation event...");

        // Fetch next Lambda runtime event
        let runtime_event = match fetch_next_lambda_runtime_event(&components.client, &components.extension_id).await {
            Ok(event) => event,
            Err(e) => {
                error!("Error receiving next event: {:?}. Continuing.", e);
                continue;
            }
        };

        event_counter += 1;
        let is_cold_start = event_counter == 1;

        match runtime_event {
            LambdaRuntimeEvent::Invoke { request_id, invoked_function_arn } => {
                let event_start = std::time::Instant::now();

                // Tag Lambda function on first invocation (with real ARN)
                if is_cold_start && components.config.new_relic.add_version_detail_tags {
                    tag_lambda_function_once(invoked_function_arn.clone());
                }

                // Update global context
                update_global_invocation_context(&request_id, &invoked_function_arn);
                
                // Process buffered logs
                components.global_log_processor.process_buffered_logs_with_request_id(&request_id);
                
                // Create request state FIRST (before processing pending payloads)
                let request_state = create_request_processing_state(
                    &request_id,
                    &invoked_function_arn,
                    &components.processor_factory
                );
                REQUEST_PROCESSORS.insert(request_id.clone(), request_state);
                
                // WARM START: Process pending payloads in PARALLEL with current request
                // This prevents blocking the current request while processing old late payloads
                let pending_task = if !is_cold_start {
                    // Log buffer state before processing
                    let buffer_count = REQUEST_AGENT_BUFFERS.len();
                    info!("APM warm start: Found {} request buffer(s) before processing (current: {})", buffer_count, request_id);
                    
                    Some(tokio::spawn({
                        let newrelic_client = components.newrelic_client.clone();
                        let config = components.config.clone();
                        let global_log_processor = components.global_log_processor.clone();
                        let apm_app = components.apm_app.clone();
                        let current_request_id = request_id.clone();
                        
                        async move {
                            process_pending_agent_payloads(
                                &newrelic_client,
                                &config,
                                &global_log_processor,
                                &apm_app,
                                &current_request_id,  // Exclude current request
                            ).await;
                        }
                    }))
                } else {
                    None
                };
                
                // Process current request in APM mode (runs in parallel with pending task)
                let request_id_clone = request_id.clone();
                let invoked_function_arn_clone = invoked_function_arn.clone();
                let processor_factory_clone = components.processor_factory.clone();
                let newrelic_client_clone = components.newrelic_client.clone();
                let config_clone = components.config.clone();
                let global_log_processor_clone = components.global_log_processor.clone();
                let apm_app_clone = components.apm_app.clone();

                let current_task = tokio::spawn(async move {
                    process_apm_request(
                        request_id_clone,
                        invoked_function_arn_clone,
                        is_cold_start,
                        processor_factory_clone,
                        newrelic_client_clone,
                        config_clone,
                        global_log_processor_clone,
                        apm_app_clone,
                    ).await;
                });

                // Wait for both tasks to complete (current request + pending payloads)
                if let Some(pending) = pending_task {
                    let (current_result, pending_result) = tokio::join!(current_task, pending);
                    if let Err(e) = current_result {
                        error!("Error in APM request processing: {}", e);
                    }
                    if let Err(e) = pending_result {
                        error!("Error in pending payload processing: {}", e);
                    }
                } else {
                    if let Err(e) = current_task.await {
                        error!("Error in APM request processing: {}", e);
                    }
                }

                let event_time = event_start.elapsed();
                if is_cold_start {
                    info!("COLD START: First invocation processed in {:?} (request_id: {})", event_time, request_id);
                    IS_WARM_START.store(true, std::sync::atomic::Ordering::Relaxed);
                } else {
                    info!("WARM START: Event {} processed in {:?} (request_id: {})", event_counter, event_time, request_id);
                }
            }
            LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
                info!("APM mode: Extension shutting down: {}", shutdown_reason);
                // Process any final pending payloads (use empty string to process ALL buffers)
                process_pending_agent_payloads(
                    &components.newrelic_client,
                    &components.config,
                    &components.global_log_processor,
                    &components.apm_app,
                    "",  // Empty string = process all pending buffers
                ).await;
                break;
            }
        }
    }

    event_counter
}

/// STANDARD MODE EVENT LOOP - Optimized for Serverless Ingest API
/// 
/// Key characteristics:
/// - Batches agent payloads with platform.report for warm starts
/// - Sends to serverless ingest API (single wrapped payload)
/// - Waits for platform.report coordination
/// - Cleans up immediately after processing
async fn execute_standard_mode_event_loop(components: &mut ExtensionComponents) -> u32 {
    let mut event_counter = 0;

    loop {
        debug!("Standard mode: waiting for next lambda invocation event...");

        // Fetch next Lambda runtime event
        let runtime_event = match fetch_next_lambda_runtime_event(&components.client, &components.extension_id).await {
            Ok(event) => event,
            Err(e) => {
                error!("Error receiving next event: {:?}. Continuing.", e);
                continue;
            }
        };

        event_counter += 1;
        let is_cold_start = event_counter == 1;

        match runtime_event {
            LambdaRuntimeEvent::Invoke { request_id, invoked_function_arn } => {
                let event_start = std::time::Instant::now();

                // Tag Lambda function on first invocation (with real ARN)
                if is_cold_start && components.config.new_relic.add_version_detail_tags {
                    tag_lambda_function_once(invoked_function_arn.clone());
                }

                // Update global context
                update_global_invocation_context(&request_id, &invoked_function_arn);
                
                // Process buffered logs
                components.global_log_processor.process_buffered_logs_with_request_id(&request_id);
                
                // WARM START: Clean up old buffers from previous request and process any late agent payloads
                if !is_cold_start {
                    // Find old request buffers (from previous invocation)
                    let old_requests: Vec<String> = REQUEST_AGENT_BUFFERS.iter()
                        .map(|entry| entry.key().clone())
                        .collect();
                    
                    for old_request_id in old_requests {
                        // Check if there's a late agent payload in the buffer
                        if let Some(buffer) = REQUEST_AGENT_BUFFERS.get(&old_request_id) {
                            if let Ok(buffer_guard) = buffer.lock() {
                                if !buffer_guard.is_empty() {
                                    info!("Found {} late agent payload(s) for previous request: {}", 
                                         buffer_guard.len(), old_request_id);
                                    
                                    // Check if there's a matching platform.report in PENDING_REPORTS
                                    let report_line = PENDING_REPORTS.remove(&old_request_id).map(|(_, report)| {
                                        debug!("Found matching platform.report for late agent payload: {}", old_request_id);
                                        report
                                    });
                                    
                                    // Add to batch for sending
                                    for payload_bytes in buffer_guard.iter() {
                                        let context = REQUEST_CONTEXTS.get(&old_request_id)
                                            .map(|ctx_entry| {
                                                ctx_entry.lock()
                                                    .ok()
                                                    .map(|ctx| ctx.invoked_function_arn.clone())
                                                    .unwrap_or_else(|| "unknown".to_string())
                                            })
                                            .unwrap_or_else(|| "unknown".to_string());
                                        
                                        add_to_batch(
                                            old_request_id.clone(),
                                            payload_bytes.clone(),
                                            report_line.clone(),
                                            context,
                                        );
                                    }
                                }
                            }
                        }
                        
                        debug!("Cleaning up old buffer from previous request: {}", old_request_id);
                        cleanup_request_processing_state(&old_request_id);
                    }
                    
                    // Check if batch should be sent now
                    if should_send_batch() {
                        let newrelic_client = components.newrelic_client.clone();
                        let config = components.config.clone();
                        tokio::spawn(async move {
                            send_batched_payloads(newrelic_client, config).await;
                        });
                    }
                }
                
                // Create request state
                let request_state = create_request_processing_state(
                    &request_id,
                    &invoked_function_arn,
                    &components.processor_factory
                );

                // Update global log processor context for this request
                components.global_log_processor.update_invocation_context(request_state.context.clone());

                // Store request processing state
                REQUEST_PROCESSORS.insert(request_id.clone(), request_state);
                
                // Process request in standard mode
                let request_id_clone = request_id.clone();
                let invoked_function_arn_clone = invoked_function_arn.clone();
                let processor_factory_clone = components.processor_factory.clone();
                let newrelic_client_clone = components.newrelic_client.clone();
                let config_clone = components.config.clone();
                let global_log_processor_clone = components.global_log_processor.clone();
                let apm_app_clone = components.apm_app.clone();

                let processing_handle = tokio::spawn(async move {
                    process_request_concurrently(
                        request_id_clone,
                        invoked_function_arn_clone,
                        processor_factory_clone,
                        newrelic_client_clone,
                        config_clone,
                        global_log_processor_clone,
                        apm_app_clone,
                    ).await;
                });

                if let Err(e) = processing_handle.await {
                    error!("Error in standard mode request processing: {}", e);
                }

                let event_time = event_start.elapsed();
                if is_cold_start {
                    info!("COLD START: First invocation processed in {:?} (request_id: {})", event_time, request_id);
                    IS_WARM_START.store(true, std::sync::atomic::Ordering::Relaxed);
                } else {
                    info!("WARM START: Event {} processed in {:?} (request_id: {})", event_counter, event_time, request_id);
                }
            }
            LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
                info!("Standard mode: Extension shutting down: {}", shutdown_reason);

                // Wait for all requests to complete and flush batched payloads
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

// ============================================================================
// APM MODE REQUEST PROCESSING
// ============================================================================

/// Process request in APM mode - simplified flow with immediate sending
async fn process_apm_request(
    request_id: String,
    invoked_function_arn: String,
    is_cold_start: bool,
    _processor_factory: Arc<ProcessorFactory>,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<config::ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
    apm_app: apm::SharedApmApp,
) {
    debug!("APM mode: Starting processing for request: {}", request_id);

    // CRITICAL: Set this as the currently active request for agent payload routing
    if let Ok(mut active_request) = CURRENT_ACTIVE_REQUEST_ID.lock() {
        *active_request = Some(request_id.clone());
    }

    // WARM START: Check for and process any pending late agent payloads from previous invocations
    if !is_cold_start {
        // Find all buffers that have payloads (excluding current request)
        let pending_buffers: Vec<String> = REQUEST_AGENT_BUFFERS
            .iter()
            .filter_map(|entry| {
                let req_id = entry.key();
                if req_id != &request_id {
                    if let Ok(buffer) = entry.value().lock() {
                        if !buffer.is_empty() {
                            return Some(req_id.clone());
                        }
                    }
                }
                None
            })
            .collect();

        if !pending_buffers.is_empty() {
            info!(
                "APM warm start: Found {} pending late agent payload(s) from previous invocations - processing now",
                pending_buffers.len()
            );

            for old_request_id in pending_buffers {
                debug!("Processing late agent payload for request: {}", old_request_id);
                
                // Extract payloads from buffer
                let late_payloads = if let Some(buffer_ref) = REQUEST_AGENT_BUFFERS.get(&old_request_id) {
                    if let Ok(mut buffer) = buffer_ref.lock() {
                        std::mem::take(&mut *buffer)
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                };

                // Send late payloads to APM collector
                for payload_bytes in late_payloads {
                    debug!("Sending late agent payload for request: {} ({} bytes)", old_request_id, payload_bytes.len());
                    
                    if let Err(e) = send_to_apm_collector(
                        &payload_bytes,
                        &old_request_id,
                        &invoked_function_arn,
                        &newrelic_client,
                        &config,
                        &apm_app,
                    ).await {
                        error!("Failed to send late agent payload for {}: {}", old_request_id, e);
                    } else {
                        info!("Successfully sent late agent payload for request: {}", old_request_id);
                    }
                }

                // Cleanup old request resources after sending late payload
                debug!("Cleaning up resources for old request after late payload: {}", old_request_id);
                cleanup_request_processing_state_internal(&old_request_id, false);
            }
        }
    }

    // Get request processing state
    let state = REQUEST_PROCESSORS.remove(&request_id).map(|(_k, v)| v);

    let Some(mut state) = state else {
        error!("No processing state found for request: {}", request_id);
        return;
    };

    // Set invocation start time
    let invocation_start_time = chrono::Utc::now();
    global_log_processor.set_invocation_start_time(invocation_start_time);
    global_log_processor.reset_trace_id_state();
    state.platform_processor.process_invoke_event(&request_id, &invoked_function_arn);

    // COLD START: Wait for platform.runtimeDone
    // WARM START: Also wait for APM mode (agent sends payload when function completes)
    // Standard mode can skip for performance since it uses batching
    if is_cold_start {
        if let Some(ref mut runtime_done_rx) = state.runtime_done_rx {
            match runtime_done_rx.recv().await {
                Some(_) => info!("Runtime.done received for request: {} (COLD START)", request_id),
                None => warn!("Runtime.done channel closed for request: {} - proceeding anyway", request_id),
            }
        }
    } else {
        // APM WARM START: Wait for runtime.done because agent sends payload when function completes
        debug!("APM warm start: Waiting for runtime.done (agent sends payload at function completion)");
        if let Some(ref mut runtime_done_rx) = state.runtime_done_rx {
            match runtime_done_rx.recv().await {
                Some(_) => debug!("Runtime.done received for APM warm start request: {}", request_id),
                None => warn!("Runtime.done channel closed for request: {} - proceeding anyway", request_id),
            }
        }
    }

    // Wait up to 200ms for agent payload (early exit on arrival)
    let payload_already_arrived = {
        if let Ok(buffer) = state.agent_buffer.lock() {
            !buffer.is_empty()
        } else {
            false
        }
    };

    if !payload_already_arrived {
        debug!("APM mode: Waiting up to 200ms for agent payload for request: {}", request_id);
        tokio::select! {
            _ = state.coordination_rx.as_mut().unwrap().recv() => {
                debug!("Agent payload received early for request: {} (saved wait time)", request_id);
            }
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                debug!("Agent payload wait timeout (200ms) for request: {} - may arrive late", request_id);
            }
        }
    } else {
        debug!("Agent payload already in buffer for request: {} - no wait needed", request_id);
    }

    // Extract agent payload from buffer
    let agent_payloads = {
        if let Ok(mut buffer) = state.agent_buffer.lock() {
            std::mem::take(&mut *buffer)
        } else {
            Vec::new()
        }
    };

    // Track if we got the payload now or need to wait for late arrival
    let got_payload_now = !agent_payloads.is_empty();
    
    // Send agent payload immediately to APM collector if available
    let send_agent_task = if got_payload_now {
        info!("APM mode: Sending {} agent payload(s) immediately to APM collector", agent_payloads.len());
        let request_id_clone = request_id.clone();
        let invoked_function_arn_clone = invoked_function_arn.clone();
        let newrelic_client_clone = newrelic_client.clone();
        let config_clone = config.clone();
        let global_log_processor_clone = global_log_processor.clone();
        let apm_app_clone = apm_app.clone();

        Some(tokio::spawn(async move {
            for payload_bytes in agent_payloads {
                // Extract trace ID if enabled
                extract_and_coordinate_trace_id(&payload_bytes, &config_clone, &global_log_processor_clone).await;
                
                // Send to APM collector
                if let Err(e) = send_to_apm_collector(
                    &payload_bytes,
                    &request_id_clone,
                    &invoked_function_arn_clone,
                    &newrelic_client_clone,
                    &config_clone,
                    &apm_app_clone,
                ).await {
                    error!("Failed to send agent payload to APM collector: {}", e);
                }
            }
        }))
    } else {
        debug!("APM mode: No agent payload yet for request: {} - buffer kept alive for late arrival", request_id);
        None
    };

    // Flush logs, platform data, and agent send ALL IN PARALLEL
    let log_flushing = global_log_processor.flush();
    let platform_flushing = state.platform_processor.flush();

    let (log_result, platform_result, agent_result) = tokio::join!(
        log_flushing,
        platform_flushing,
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
        error!("Failed to flush logs for request {}: {}", request_id, e);
    }
    if let Err(e) = platform_result {
        error!("Failed to flush platform for request {}: {}", request_id, e);
    }
    if let Err(e) = agent_result {
        error!("Agent send task failed for request {}: {}", request_id, e);
    }
    
    // APM Mode Cleanup Strategy:
    // - If we got the payload now and sent it: CLEANUP immediately
    // - If payload not arrived yet: KEEP buffer alive for late arrival (next invocation will process it)
    if got_payload_now {
        debug!("APM mode: Agent payload sent - cleaning up all resources for request: {}", request_id);
        cleanup_request_processing_state_internal(&request_id, false);
        
        // Clear active request ID
        if let Ok(mut active_request) = CURRENT_ACTIVE_REQUEST_ID.lock() {
            *active_request = None;
        }
    } else {
        debug!("APM mode: No payload yet - keeping buffer alive for late arrival (will process on next invoke)");
        // Keep buffer and context alive - cleanup will happen when:
        // 1. Next invocation arrives and processes pending buffers
        // 2. Or late payload arrives and gets sent
        cleanup_request_processing_state_internal(&request_id, true); // skip_buffer_cleanup = true
        
        // Keep active request ID so late payloads route correctly
        if let Ok(mut active_request) = CURRENT_ACTIVE_REQUEST_ID.lock() {
            *active_request = Some(request_id.clone());
        }
    }
    
    debug!("APM mode: Completed processing for request: {}", request_id);
}

/// Send agent payload to APM collector (parses and sends 5 telemetry types)
async fn send_to_apm_collector(
    payload_bytes: &[u8],
    request_id: &str,
    _invoked_function_arn: &str,
    _newrelic_client: &Arc<NewRelicClient>,
    _config: &Arc<config::ExtensionConfig>,
    apm_app: &apm::SharedApmApp,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let apm_app_guard = apm_app.read().await;
    if let Some(ref app) = *apm_app_guard {
        info!("APM mode: Processing agent payload for request: {} (size: {} bytes)", request_id, payload_bytes.len());
        app.process_agent_payload(payload_bytes.to_vec()).await?;
        info!("APM mode: Agent payload sent successfully for request: {}", request_id);
    } else {
        error!("APM app not initialized - cannot send payload");
    }
    Ok(())
}

// ============================================================================
// STANDARD MODE REQUEST PROCESSING
// ============================================================================

/// Process request in Standard mode - with batching and platform.report coordination
async fn process_request_concurrently(
    request_id: String,
    invoked_function_arn: String,
    _processor_factory: Arc<ProcessorFactory>,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<config::ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
    _apm_app: apm::SharedApmApp,
) {
    debug!("Standard mode: Starting processing for request: {}", request_id);

    // CRITICAL: Set this as the currently active request for agent payload routing
    if let Ok(mut active_request) = CURRENT_ACTIVE_REQUEST_ID.lock() {
        *active_request = Some(request_id.clone());
    }

    // Get request processing state
    let state = REQUEST_PROCESSORS.remove(&request_id).map(|(_k, v)| v);

    let Some(mut state) = state else {
        error!("No processing state found for request: {}", request_id);
        return;
    };

    // Set invocation start time
    let invocation_start_time = chrono::Utc::now();
    global_log_processor.set_invocation_start_time(invocation_start_time);
    global_log_processor.reset_trace_id_state();
    state.platform_processor.process_invoke_event(&request_id, &invoked_function_arn);

    // Check if this is a cold or warm start
    let is_cold_start = !crate::IS_WARM_START.load(std::sync::atomic::Ordering::Relaxed);

    // COLD START: Wait for platform.runtimeDone event
    // WARM START: Skip this wait for performance
    if is_cold_start {
        if let Some(ref mut runtime_done_rx) = state.runtime_done_rx {
            match runtime_done_rx.recv().await {
                Some(_) => info!("Runtime.done received for request: {} (COLD START)", request_id),
                None => warn!("Runtime.done channel closed for request: {} - proceeding anyway", request_id),
            }
        }
    } else {
        debug!("Skipping runtime.done wait for WARM START request: {} (performance optimization)", request_id);
    }

    // Wait for agent payload - Standard mode uses shorter timeout for warm starts
    let agent_wait_timeout_ms = if is_cold_start { 200 } else { 50 };

    let payload_already_arrived = {
        if let Ok(buffer) = state.agent_buffer.lock() {
            !buffer.is_empty()
        } else {
            false
        }
    };

    if !payload_already_arrived {
        debug!("Standard mode: Waiting up to {}ms for agent payload for request: {}", agent_wait_timeout_ms, request_id);
        tokio::select! {
            _ = state.coordination_rx.as_mut().unwrap().recv() => {
                debug!("Agent payload received early for request: {}", request_id);
            }
            _ = tokio::time::sleep(Duration::from_millis(agent_wait_timeout_ms)) => {
                debug!("Agent payload wait timeout ({}ms) for request: {}", agent_wait_timeout_ms, request_id);
            }
        }
    } else {
        debug!("Agent payload already in buffer for request: {} - no wait needed", request_id);
    }

    // Extract agent payload from buffer
    let agent_payloads = {
        if let Ok(mut buffer) = state.agent_buffer.lock() {
            std::mem::take(&mut *buffer)
        } else {
            Vec::new()
        }
    };

    // Check if platform.report already available in PENDING_REPORTS (non-blocking)
    let report_line = PENDING_REPORTS.remove(&request_id).map(|(_, report)| {
        debug!("Found pending platform.report for request: {}", request_id);
        report
    });

    // STANDARD MODE STRATEGY: Batch payloads on warm starts without platform.report
    let send_agent_task = if agent_payloads.is_empty() {
        info!("Standard mode: No agent payload for request: {}", request_id);
        None
    } else if is_cold_start {
        // COLD START: Send immediately with or without report
        info!("Standard mode: Cold start - sending agent payload immediately (with report: {})", report_line.is_some());
        let request_id_clone = request_id.clone();
        let invoked_function_arn_clone = invoked_function_arn.clone();
        let newrelic_client_clone = newrelic_client.clone();
        let config_clone = config.clone();
        let global_log_processor_clone = global_log_processor.clone();
        let apm_app_clone = _apm_app.clone();

        Some(tokio::spawn(async move {
            send_agent_with_report_immediately(
                request_id_clone,
                invoked_function_arn_clone,
                agent_payloads,
                report_line,
                newrelic_client_clone,
                config_clone,
                global_log_processor_clone,
                apm_app_clone,
            ).await;
        }))
    } else if report_line.is_some() {
        // WARM START + REPORT AVAILABLE: Send immediately with report combined
        info!("Standard mode: Warm start - agent+report ready, sending combined");
        let request_id_clone = request_id.clone();
        let invoked_function_arn_clone = invoked_function_arn.clone();
        let newrelic_client_clone = newrelic_client.clone();
        let config_clone = config.clone();
        let global_log_processor_clone = global_log_processor.clone();
        let apm_app_clone = _apm_app.clone();

        Some(tokio::spawn(async move {
            send_agent_with_report_immediately(
                request_id_clone,
                invoked_function_arn_clone,
                agent_payloads,
                report_line,
                newrelic_client_clone,
                config_clone,
                global_log_processor_clone,
                apm_app_clone,
            ).await;
        }))
    } else {
        // WARM START + NO REPORT: Batch for later (5 min or count threshold)
        info!("Standard mode: Warm start - batching agent payload (no platform.report yet)");
        for payload_bytes in agent_payloads {
            add_to_batch(
                request_id.clone(),
                payload_bytes,
                None,
                invoked_function_arn.clone(),
            );
        }

        // Check if batch threshold reached
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

    // Flush logs, platform data, and retry failed payloads ALL IN PARALLEL
    let log_flushing = global_log_processor.flush();
    let platform_flushing = state.platform_processor.flush();
    let failed_retry = retry_failed_agent_payloads(&newrelic_client, &config);

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
        error!("Failed to flush logs for request {}: {}", request_id, e);
    }
    if let Err(e) = platform_result {
        error!("Failed to flush platform for request {}: {}", request_id, e);
    }
    if let Err(e) = agent_result {
        error!("Agent send task failed for request {}: {}", request_id, e);
    }
    
    // Standard mode: Keep buffers alive on WARM STARTS to catch late agent payloads
    // Late payloads are processed at the START of the NEXT request (not via pending payload processing)
    // Cold starts: cleanup immediately since no late payloads expected
    cleanup_request_processing_state_conditional(&request_id, is_cold_start);
    
    // CRITICAL: Clear the active request ID now that processing is done
    if let Ok(mut active_request) = CURRENT_ACTIVE_REQUEST_ID.lock() {
        *active_request = None;
    }
    
    debug!("Standard mode: Completed processing for request: {}", request_id);
}

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
        context: context.clone(),
        platform_processor,
        agent_buffer: agent_buffer.clone(),
        coordination_rx: Some(payload_rx),
        runtime_done_rx: Some(runtime_done_rx),
    };

    REQUEST_CONTEXTS.insert(request_id.to_string(), context);
    REQUEST_AGENT_BUFFERS.insert(request_id.to_string(), agent_buffer);

    debug!("Created per-request processing state for {} (using global log processor)", request_id);
    state
}

/// Clean up per-request processing state after processing
fn cleanup_request_processing_state(request_id: &str) {
    cleanup_request_processing_state_internal(request_id, false);
}

/// Clean up per-request processing state - for both Standard and APM modes
/// Cold start: cleanup everything
/// Warm start: keep buffer alive for late agent payloads (both modes use this strategy)
fn cleanup_request_processing_state_conditional(request_id: &str, is_cold_start: bool) {
    let skip_buffer_cleanup = !is_cold_start;
    cleanup_request_processing_state_internal(request_id, skip_buffer_cleanup);
}

/// Clean up with option to skip buffer cleanup (for warm starts in both modes)
fn cleanup_request_processing_state_internal(request_id: &str, skip_buffer_cleanup: bool) {
    // Clean up request processing state
    if REQUEST_PROCESSORS.remove(request_id).is_some() {
        debug!("Cleaned up request processing state for {}", request_id);
    }

    if !skip_buffer_cleanup {
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
    } else {
        debug!("Keeping buffer alive for request {} to catch late agent payloads (will be processed on next invocation)", request_id);
    }

    // Always clean up runtime.done channel
    if RUNTIME_DONE_CHANNELS.remove(request_id).is_some() {
        debug!("Cleaned up runtime.done channel for request {}", request_id);
    }

    // Clean up any pending report for this request
    if PENDING_REPORTS.remove(request_id).is_some() {
        debug!("Cleaned up pending platform.report for request {}", request_id);
    }
}

/// Process any pending agent payloads from previous invocation (APM mode only)
/// Excludes the current request ID to avoid processing empty buffer
async fn process_pending_agent_payloads(
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
    global_log_processor: &Arc<LogProcessor>,
    apm_app: &apm::SharedApmApp,
    current_request_id: &str,  // NEW: Exclude this request from processing
) {
    // Get all pending request buffers EXCEPT the current request
    let all_buffers: Vec<(String, usize)> = REQUEST_AGENT_BUFFERS
        .iter()
        .map(|entry| {
            let buffer_size = entry.value().lock().map(|b| b.len()).unwrap_or(0);
            (entry.key().clone(), buffer_size)
        })
        .collect();
    
    debug!("APM pending check: Total buffers={}, Details: {:?}", all_buffers.len(), all_buffers);
    
    let pending_requests: Vec<(String, Arc<Mutex<Vec<Vec<u8>>>>)> = REQUEST_AGENT_BUFFERS
        .iter()
        .filter(|entry| entry.key() != current_request_id)  // Exclude current request
        .map(|entry| (entry.key().clone(), entry.value().clone()))
        .collect();
    
    if pending_requests.is_empty() {
        debug!("No pending agent payload buffers from previous invocations (current request excluded: {})", current_request_id);
        return;
    }
    
    info!("Processing {} pending agent payload buffer(s) from previous invocations (excluding current: {})", pending_requests.len(), current_request_id);
    
    for (request_id, buffer) in pending_requests {
        // Get the context for this request
        let context = REQUEST_CONTEXTS.get(&request_id).map(|entry| entry.value().clone());
        
        let invoked_function_arn = if let Some(ctx) = context {
            if let Ok(ctx_guard) = ctx.lock() {
                ctx_guard.invoked_function_arn.clone()
            } else {
                "unknown".to_string()
            }
        } else {
            "unknown".to_string()
        };
        
        // Extract payloads from buffer
        let payloads = {
            if let Ok(mut buf) = buffer.lock() {
                std::mem::take(&mut *buf)
            } else {
                Vec::new()
            }
        };
        
        if !payloads.is_empty() {
            info!("Found {} pending agent payload(s) for previous request: {}", payloads.len(), request_id);
            
            for payload_bytes in payloads {
                if let Err(e) = process_and_send_agent_payload(
                    &payload_bytes,
                    &request_id,
                    &invoked_function_arn,
                    global_log_processor,
                    newrelic_client,
                    config,
                    apm_app,
                ).await {
                    error!("Failed to process pending agent payload: {}", e);
                }
            }
        }
        
        // Now cleanup the old request completely
        cleanup_request_processing_state_internal(&request_id, false);
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
    apm_app: apm::SharedApmApp,
) {
    let has_report = report_line.is_some();
    debug!("Sending agent payload immediately for {} (with report: {})", request_id, has_report);

    // Check if APM mode is enabled
    let apm_app_guard = apm_app.read().await;
    let is_apm_mode = apm_app_guard.is_some();

    for payload_bytes in agent_payloads {
        if let Some(ref app) = *apm_app_guard {
            // APM MODE: Send agent payload to APM collector
            info!("APM mode: Sending agent payload for request: {}", request_id);
            if let Err(e) = app.process_agent_payload(payload_bytes.clone()).await {
                error!("Failed to send agent payload to APM collector for {}: {}", request_id, e);
            }
            
            // Send REPORT log metrics to APM if available
            if let Some(ref report) = report_line {
                debug!("APM mode: Sending platform REPORT metrics for request: {}", request_id);
                if let Err(e) = app.send_platform_report_metrics(report).await {
                    error!("Failed to send platform REPORT metrics for {}: {}", request_id, e);
                }
                
                // Check for faults/timeouts in REPORT log and generate error events
                if report.contains("Task timed out") || report.contains("error") || report.contains("Error") {
                    debug!("APM mode: Detected fault/timeout in REPORT log, generating error event");
                    if let Err(e) = app.send_error_event_from_fault(report, &request_id, &invoked_function_arn).await {
                        error!("Failed to send error event for fault in {}: {}", request_id, e);
                    }
                }
            }
        } else {
            // STANDARD MODE: Send to serverless ingest API
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
    
    if is_apm_mode {
        info!("APM mode: Agent payload and platform metrics sent for request: {}", request_id);
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



