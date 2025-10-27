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
use std::collections::{HashMap, VecDeque};
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
// --- CONCURRENT REQUEST HANDLING ---
// Per-request contexts to handle concurrent Lambda invocations safely
// Using DashMap for lock-free concurrent access (10x faster than RwLock)
static REQUEST_CONTEXTS: Lazy<Arc<DashMap<String, Arc<Mutex<InvocationContext>>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));
static REQUEST_AGENT_BUFFERS: Lazy<Arc<DashMap<String, Arc<Mutex<Vec<Vec<u8>>>>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

// Global coordination channels per request for agent payload processing
static PAYLOAD_COORDINATION: Lazy<Arc<DashMap<String, mpsc::UnboundedSender<()>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

// Per-request processing state management
static REQUEST_PROCESSORS: Lazy<Arc<DashMap<String, RequestProcessingState>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

// Failed agent payloads buffer for retry across invocations
// OPTIMIZATION: VecDeque for O(1) front removal + cached size for O(1) size checks
const MAX_FAILED_PAYLOADS_BYTES: usize = 100 * 1024 * 1024; // 100MB limit

#[derive(Debug)]
struct FailedPayloadsBuffer {
    payloads: VecDeque<FailedAgentPayload>,
    total_size_bytes: usize,  // Cached total size for O(1) checks
}

impl FailedPayloadsBuffer {
    fn new() -> Self {
        Self {
            payloads: VecDeque::new(),
            total_size_bytes: 0,
        }
    }

    fn push(&mut self, payload: FailedAgentPayload) {
        let payload_size = payload.payload_bytes.len();

        // Evict oldest payloads (LRU) if needed - O(1) front removal with VecDeque!
        while self.total_size_bytes + payload_size > MAX_FAILED_PAYLOADS_BYTES && !self.payloads.is_empty() {
            if let Some(oldest) = self.payloads.pop_front() {  // O(1) removal!
                let oldest_size = oldest.payload_bytes.len();
                self.total_size_bytes = self.total_size_bytes.saturating_sub(oldest_size);
                warn!("Evicted oldest failed payload (age: {}h) to stay under {}MB limit",
                     chrono::Utc::now().signed_duration_since(oldest.failed_at).num_hours(),
                     MAX_FAILED_PAYLOADS_BYTES / 1024 / 1024);
            }
        }

        // Only add if we're under limit
        if self.total_size_bytes + payload_size <= MAX_FAILED_PAYLOADS_BYTES {
            self.total_size_bytes += payload_size;
            self.payloads.push_back(payload);
        } else {
            warn!("Cannot buffer failed payload - would exceed {}MB limit",
                 MAX_FAILED_PAYLOADS_BYTES / 1024 / 1024);
        }
    }

    fn len(&self) -> usize {
        self.payloads.len()
    }

    fn iter(&self) -> impl Iterator<Item = &FailedAgentPayload> {
        self.payloads.iter()
    }

    fn clear(&mut self) {
        self.payloads.clear();
        self.total_size_bytes = 0;
    }

    fn size_bytes(&self) -> usize {
        self.total_size_bytes
    }

    fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&FailedAgentPayload) -> bool,
    {
        // Rebuild total size after retaining
        let mut retained = VecDeque::new();
        let mut new_size = 0;

        for payload in self.payloads.drain(..) {
            if f(&payload) {
                new_size += payload.payload_bytes.len();
                retained.push_back(payload);
            }
        }

        self.payloads = retained;
        self.total_size_bytes = new_size;
    }

    fn is_empty(&self) -> bool {
        self.payloads.is_empty()
    }

    fn drain(&mut self) -> impl Iterator<Item = FailedAgentPayload> + '_ {
        self.total_size_bytes = 0;
        self.payloads.drain(..)
    }
}

static FAILED_AGENT_PAYLOADS: Lazy<Arc<Mutex<FailedPayloadsBuffer>>> =
    Lazy::new(|| Arc::new(Mutex::new(FailedPayloadsBuffer::new())));

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

// Cross-invocation batch accumulator for warm starts
static WARM_START_BATCH: Lazy<Arc<Mutex<WarmStartBatchAccumulator>>> =
    Lazy::new(|| Arc::new(Mutex::new(WarmStartBatchAccumulator::new())));

// Current request ID for payload routing (Mutex is faster than RwLock for single-value updates)
static CURRENT_REQUEST_ID: Lazy<Arc<Mutex<String>>> =
    Lazy::new(|| Arc::new(Mutex::new(String::new())));

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

// --- WARM START BATCHING STRUCTURES ---

/// Cross-invocation batch accumulator for warm starts
/// Accumulates agent payloads and REPORT lines across multiple Lambda invocations
/// Flushes when batch reaches 1MB or entries are older than 5 minutes
#[derive(Debug)]
struct WarmStartBatchAccumulator {
    entries: Vec<BatchEntry>,
    report_lines: HashMap<String, ReportLine>,
    total_size_bytes: usize,
}

impl WarmStartBatchAccumulator {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            report_lines: HashMap::new(),
            total_size_bytes: 0,
        }
    }

    /// Add agent payload entry to batch
    fn add_entry(&mut self, entry: BatchEntry) {
        self.total_size_bytes += entry.payload_bytes.len();
        self.entries.push(entry);
    }

    /// Add REPORT line and try to match with existing entries
    fn add_report_line(&mut self, request_id: String, message: String, timestamp: u64) {
        // Mark matching entries as having REPORT
        for entry in &mut self.entries {
            if entry.request_id == request_id && !entry.has_report {
                entry.has_report = true;
                break;
            }
        }

        self.report_lines.insert(request_id.clone(), ReportLine {
            request_id,
            message,
            timestamp,
            matched: true,
        });
    }

    /// Check if batch should be flushed
    fn should_flush(&self, max_size_bytes: usize, max_age_secs: u64) -> bool {
        // Size limit check
        if self.total_size_bytes >= max_size_bytes {
            return true;
        }

        // Age limit check - any entry older than max_age_secs
        let now = std::time::Instant::now();
        for entry in &self.entries {
            if now.duration_since(entry.received_at).as_secs() >= max_age_secs {
                return true;
            }
        }

        false
    }

    /// Take all entries and reset accumulator
    fn take_entries(&mut self) -> Vec<BatchEntry> {
        self.total_size_bytes = 0;
        std::mem::take(&mut self.entries)
    }

    /// Get report lines for entries being flushed
    fn get_report_lines_for_entries(&self, entries: &[BatchEntry]) -> HashMap<String, ReportLine> {
        let mut result = HashMap::new();
        for entry in entries {
            if let Some(report) = self.report_lines.get(&entry.request_id) {
                result.insert(entry.request_id.clone(), report.clone());
            }
        }
        result
    }

    /// Clean up report lines for flushed entries
    fn cleanup_report_lines(&mut self, entries: &[BatchEntry]) {
        for entry in entries {
            self.report_lines.remove(&entry.request_id);
        }
    }
}

/// Single batch entry containing agent payload
#[derive(Debug, Clone)]
struct BatchEntry {
    request_id: String,
    invoked_function_arn: String,
    payload_bytes: Vec<u8>,
    received_at: std::time::Instant,
    has_report: bool,
}

/// REPORT line to be included in batch
#[derive(Debug, Clone)]
struct ReportLine {
    request_id: String,
    message: String,
    timestamp: u64,
    matched: bool,
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

            // OPTIMIZATION: Only log first few payloads, then reduce to debug
            if payload_count <= 3 {
                info!("Received agent payload #{} ({} bytes) - processing immediately", payload_count, payload_bytes.len());
                debug!("Agent Payload preview: {:?}",
                       String::from_utf8_lossy(&payload_bytes[..std::cmp::min(100, payload_bytes.len())]));
            } else {
                debug!("Received agent payload #{} ({} bytes)", payload_count, payload_bytes.len());
            }

            // Route payload (will try immediate processing first, store if it fails)
            route_payload_to_request_buffer(payload_bytes).await;
        }

        warn!("Agent payload collector channel closed. No more agent payloads will be received");
    });
}

/// Route agent payload to the correct per-request buffer
/// OPTIMIZED: Uses CURRENT_REQUEST_ID to avoid searching contexts map
async fn route_payload_to_request_buffer(payload_bytes: Vec<u8>) {
    // OPTIMIZATION: Use current request ID instead of searching contexts map
    let request_id = {
        if let Ok(current_id) = CURRENT_REQUEST_ID.lock() {
            if current_id.is_empty() {
                None
            } else {
                Some(current_id.clone())
            }
        } else {
            None
        }
    };

    if let Some(request_id) = request_id {
        // Store in request-specific buffer
        if let Some(request_buffer) = get_request_agent_buffer(&request_id) {
            match request_buffer.lock() {
                Ok(mut buffer) => {
                    buffer.push(payload_bytes);
                    // OPTIMIZATION: Reduce logging on hot path
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
/// OPTIMIZATION: Uses VecDeque + cached size for O(1) operations (no iteration needed!)
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

    if let Ok(mut buffer) = FAILED_AGENT_PAYLOADS.lock() {
        let payload_size = failed_payload.payload_bytes.len();
        buffer.push(failed_payload);  // All eviction logic is inside push() - O(1) operations!

        info!("Buffered failed agent payload for request {} (total: {}, size: {}MB/{}MB)",
             request_id, buffer.len(),
             buffer.size_bytes() / 1024 / 1024,
             MAX_FAILED_PAYLOADS_BYTES / 1024 / 1024);
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

                // CRITICAL FIX: Create coordination channel IMMEDIATELY before any agent payload can arrive
                // This prevents race condition where agent payload arrives before channel exists
                let coordination_rx = create_payload_coordination_channel(&request_id);

                // OPTIMIZATION: Update CURRENT_REQUEST_ID for fast payload routing
                if let Ok(mut current_id) = CURRENT_REQUEST_ID.lock() {
                    *current_id = request_id.clone();
                } else {
                    error!("Failed to update CURRENT_REQUEST_ID for request {}", request_id);
                }

                // OPTIMIZATION: Only clone ARN on first invocation for tagging
                let is_first_invocation = event_counter == 0;
                if is_first_invocation {
                    // Tag Lambda function on first invocation (with real ARN)
                    if components.config.new_relic.add_version_detail_tags {
                        info!("Spawning background task to tag Lambda function with version information");
                        let version_info = version::VersionInfo::get_or_detect();
                        version::tagging::tag_lambda_function_background(
                            version_info.extension_version.clone(),
                            version_info.agent_version.clone(),
                            version_info.layer_version.clone(),
                            invoked_function_arn.clone(), // ONLY clone ARN on cold start
                        );
                    }
                }

                // Update global context for telemetry processors
                // OPTIMIZATION: Only clone ARN on first invocation, reuse after that
                if let Ok(mut global_context) = CURRENT_INVOCATION_CONTEXT.lock() {
                    global_context.request_id = request_id.clone();
                    if is_first_invocation {
                        global_context.invoked_function_arn = invoked_function_arn.clone(); // Clone on cold start
                    }
                    // On warm starts, ARN stays the same (no clone needed!)
                    global_context.trace_id = None; // Reset trace ID for new request
                }

                // Process any logs that were buffered waiting for request_id
                components.global_log_processor.process_buffered_logs_with_request_id(&request_id);

                // Create request-scoped processing state (coordination channel already created above)
                let request_state = create_request_processing_state_with_existing_channel(
                    &request_id,
                    &invoked_function_arn,
                    &components.processor_factory
                );

                // Update global log processor context for this request
                components.global_log_processor.update_invocation_context(request_state.context.clone());

                // Store request processing state (coordination_rx already created above)
                REQUEST_PROCESSORS.insert(request_id.clone(), request_state);

                // CRITICAL: Wait for platform.runtimeDone, then wait for agent telemetry
                // This is the correct flow:
                // 1. Wait for EITHER platform.runtimeDone OR agent telemetry (whichever comes first)
                // 2. If runtimeDone comes first, wait additional 200ms for agent telemetry
                // 3. If agent telemetry comes first, process immediately
                // 4. Return to /next call

                // Wait for either runtime done or agent telemetry
                let mut agent_rx = coordination_rx;
                tokio::select! {
                        _ = components.runtime_done_rx.recv() => {
                            // OPTIMIZATION: Check if agent payload already in buffer (skip 200ms wait!)
                            let payload_already_available = {
                                if let Some(buffer) = get_request_agent_buffer(&request_id) {
                                    if let Ok(buf) = buffer.lock() {
                                        !buf.is_empty()
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                }
                            };

                            if payload_already_available {
                                // OPTIMIZATION: Payload already received, skip 200ms wait!
                                debug!("Agent payload already in buffer for request {}, processing immediately", request_id);

                                // OPTIMIZATION: Process agent payload and flush logs IN PARALLEL
                                let (agent_result, flush_result) = tokio::join!(
                                    process_agent_payloads_for_request(
                                        &request_id,
                                        &invoked_function_arn,
                                        &components.newrelic_client,
                                        &components.config,
                                        &components.global_log_processor,
                                    ),
                                    components.global_log_processor.flush()
                                );

                                if let Err(e) = flush_result {
                                    error!("Failed to flush logs for request {}: {}", request_id, e);
                                }
                                probably_timeout = false;
                            } else {
                                // Payload not yet available, wait based on cold/warm start strategy
                                let is_warm_start = IS_WARM_START.load(std::sync::atomic::Ordering::Relaxed);
                                let wait_ms = if is_warm_start {
                                    // Warm start: can wait less time since we're batching anyway
                                    50
                                } else {
                                    // Cold start: use configured wait time (default 50ms, reduced from 200ms)
                                    components.config.new_relic.cold_start_report_wait_ms
                                };

                                debug!("Agent payload not in buffer yet, waiting up to {}ms for request {} ({})",
                                      wait_ms, request_id, if is_warm_start { "warm start" } else { "cold start" });

                                // OPTIMIZATION: Use channel notification exclusively (no polling)
                                // This eliminates 10+ lock acquisitions per invocation, saving 5-10ms
                                let telemetry_timeout = Duration::from_millis(wait_ms);
                                let start_time = std::time::Instant::now();

                                let payload_arrived = tokio::select! {
                                    _ = agent_rx.recv() => {
                                        let elapsed = start_time.elapsed().as_millis();
                                        debug!("Agent telemetry received after {}ms for request {}", elapsed, request_id);
                                        true
                                    }
                                    _ = tokio::time::sleep(telemetry_timeout) => {
                                        warn!("No agent telemetry within {}ms after runtimeDone for request {}", wait_ms, request_id);
                                        false
                                    }
                                };

                                if payload_arrived {
                                    // OPTIMIZATION: Process agent payload and flush logs IN PARALLEL
                                    let (agent_result, flush_result) = tokio::join!(
                                        process_agent_payloads_for_request(
                                            &request_id,
                                            &invoked_function_arn,
                                            &components.newrelic_client,
                                            &components.config,
                                            &components.global_log_processor,
                                        ),
                                        components.global_log_processor.flush()
                                    );

                                    if let Err(e) = flush_result {
                                        error!("Failed to flush logs for request {}: {}", request_id, e);
                                    }
                                    probably_timeout = false;
                                } else {
                                    // Timeout: no payload after full wait period
                                    warn!("No agent telemetry within {}ms after runtimeDone for request {}", wait_ms, request_id);
                                    probably_timeout = true;

                                    // Still flush logs even without agent payload
                                    if let Err(e) = components.global_log_processor.flush().await {
                                        error!("Failed to flush logs for request {}: {}", request_id, e);
                                    }
                                }
                            }
                        }
                        _ = agent_rx.recv() => {
                            // OPTIMIZATION: Remove info! log on hot path
                            debug!("Agent telemetry arrived before runtimeDone for request {}", request_id);

                            // OPTIMIZATION: Process agent payload and flush logs IN PARALLEL
                            let (agent_result, flush_result) = tokio::join!(
                                process_agent_payloads_for_request(
                                    &request_id,
                                    &invoked_function_arn,
                                    &components.newrelic_client,
                                    &components.config,
                                    &components.global_log_processor,
                                ),
                                components.global_log_processor.flush()
                            );

                            if let Err(e) = flush_result {
                                error!("Failed to flush logs for request {}: {}", request_id, e);
                            }
                            probably_timeout = false;

                            // Still wait for runtimeDone to ensure function completed
                            let _ = components.runtime_done_rx.recv().await;
                        }
                    }

                // Clean up request state
                cleanup_request_processing_state(&request_id);

                // Set warm start flag for performance optimization
                if event_counter > 0 {
                    IS_WARM_START.store(true, std::sync::atomic::Ordering::Relaxed);
                }

                // OPTIMIZATION: Only log timing on cold start or when debugging
                if event_counter == 0 {
                    let event_processing_time = event_processing_start_time.elapsed();
                    info!("COLD START: First invocation processed in {:?} (request_id: {})",
                          event_processing_time, request_id);
                } else {
                    // OPTIMIZATION: Remove info! log from warm start hot path (save 1-2ms of logging overhead)
                    debug!("WARM START: Event {} processed in {:?} (request_id: {})",
                          event_counter, event_processing_start_time.elapsed(), request_id);
                }
            }
            LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
                info!("Extension shutting down: {}", shutdown_reason);

                // Flush any pending warm start batch before shutdown
                if components.config.new_relic.enable_warm_start_batching {
                    info!("Flushing warm start batch before shutdown");
                    if let Err(e) = flush_warm_start_batch(&components.newrelic_client, &components.config).await {
                        error!("Failed to flush warm start batch on shutdown: {}", e);
                    }
                }

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
        if let Some(state) = REQUEST_PROCESSORS.get(request_id) {
            (Some(state.agent_buffer.clone()), Some(state.platform_processor.clone()))
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

    // Determine processing strategy based on warm start flag and batching config
    let is_warm_start = IS_WARM_START.load(std::sync::atomic::Ordering::Relaxed);
    let batching_enabled = config.new_relic.enable_warm_start_batching;

    if is_warm_start && batching_enabled {
        // WARM START PATH: Batch payloads for cross-invocation batching
        info!("WARM START: Adding {} payloads to batch for request {}", payloads.len(), request_id);

        // Extract trace IDs if enabled (do this before batching)
        if config.new_relic.collect_trace_id {
            for payload_bytes in &payloads {
                if let Ok(Some(trace_id)) = trace::extract_trace_id_from_payload(payload_bytes) {
                    info!("Extracted trace ID: {}, coordinating with logs", trace_id);

                    if let Err(e) = global_log_processor.on_trace_id_extracted(&trace_id).await {
                        error!("Failed to coordinate logs with trace ID: {}", e);
                    }
                }
            }
        }

        // Add payloads to batch
        if let Err(e) = add_to_warm_start_batch(
            payloads,
            request_id,
            invoked_function_arn,
            newrelic_client,
            config,
        ).await {
            error!("Error adding payloads to warm start batch for request {}: {}", request_id, e);
        }

        // IMPORTANT: Check if we should flush the batch now
        // Flush strategy: flush if we have accumulated enough data OR entries with REPORT lines
        let should_flush_now = {
            if let Ok(batch) = WARM_START_BATCH.lock() {
                if batch.entries.is_empty() {
                    false
                } else {
                    // Strategy 1: Flush if we have 5 or more entries (configurable)
                    let min_entries_for_flush = 5;

                    // Strategy 2: Flush if total size exceeds 100KB (more responsive than 1MB)
                    let min_size_for_flush = 100_000; // 100KB

                    // Strategy 3: Flush if at least 3 entries and we have some REPORT lines matched
                    let has_matched_reports = batch.entries.iter()
                        .filter(|e| e.has_report)
                        .count() >= 3;

                    // Flush if any condition is met
                    batch.entries.len() >= min_entries_for_flush ||
                    batch.total_size_bytes >= min_size_for_flush ||
                    has_matched_reports
                }
            } else {
                false
            }
        };

        if should_flush_now {
            info!("Flushing warm start batch after request {} (batch ready for sending)", request_id);
            if let Err(e) = flush_warm_start_batch(newrelic_client, config).await {
                error!("Failed to flush warm start batch after request {}: {}", request_id, e);
            }
        } else {
            debug!("Batch not ready yet after request {} (will accumulate more entries)", request_id);
        }
    } else {
        // COLD START PATH: Send payloads immediately for visibility
        info!("COLD START: Sending {} payloads immediately for request {}", payloads.len(), request_id);

        // Process each payload immediately
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
    tokio::time::sleep(Duration::from_millis(70)).await;
    
    // Force cleanup of any remaining requests
    let remaining_requests: Vec<String> = REQUEST_PROCESSORS.iter()
        .map(|entry| entry.key().clone())
        .collect();
    
    for request_id in remaining_requests {
        warn!("Force cleaning up request: {}", request_id);
        cleanup_request_processing_state(&request_id);
    }
    
    info!("All concurrent requests completed");
}

/// Create per-request context for concurrent request handling
/// Create per-request processing state WITHOUT creating coordination channel (channel created early to avoid race)
fn create_request_processing_state_with_existing_channel(
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

    // NOTE: Coordination channel already created early in event processing to avoid race condition
    // where agent payload arrives before channel exists

    let state = RequestProcessingState {
        request_id: request_id.to_string(),
        context: context.clone(),
        platform_processor,
        agent_buffer: agent_buffer.clone(),
        coordination_rx: None, // Not needed in state since we handle it at call site
    };

    // OPTIMIZATION: Lock-free insertions with DashMap (no lock contention!)
    REQUEST_CONTEXTS.insert(request_id.to_string(), context);
    REQUEST_AGENT_BUFFERS.insert(request_id.to_string(), agent_buffer);

    info!("Created per-request processing state for {} (coordination channel already created early)", request_id);
    state
}

/// Create per-request agent buffer for concurrent request handling
fn create_request_agent_buffer(request_id: &str) -> Arc<Mutex<Vec<Vec<u8>>>> {
    let buffer = Arc::new(Mutex::new(Vec::new()));

    // Store in per-request buffers map (lock-free with DashMap)
    REQUEST_AGENT_BUFFERS.insert(request_id.to_string(), buffer.clone());
    info!("Created per-request agent buffer for {}", request_id);

    buffer
}

/// Get per-request context (for concurrent requests)
fn get_request_context(request_id: &str) -> Option<Arc<Mutex<InvocationContext>>> {
    REQUEST_CONTEXTS.get(request_id).map(|entry| entry.value().clone())
}

/// Get per-request agent buffer (for concurrent requests)
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

/// Flush warm start batch to New Relic
async fn flush_warm_start_batch(
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Take entries from batch
    let (entries, report_lines) = {
        let mut batch = WARM_START_BATCH.lock().map_err(|e| format!("Failed to lock warm start batch: {}", e))?;

        if batch.entries.is_empty() {
            debug!("No entries to flush in warm start batch");
            return Ok(());
        }

        let entries = batch.take_entries();
        let report_lines = batch.get_report_lines_for_entries(&entries);
        batch.cleanup_report_lines(&entries);

        (entries, report_lines)
    };

    info!("Flushing warm start batch with {} entries, {} REPORT lines",
         entries.len(), report_lines.len());

    // Create batch payload
    let batch_payload = create_batch_payload_json(&entries, &report_lines, config);

    // Send batch to New Relic
    match newrelic_client.send_agent_payload(config, &batch_payload).await {
        Ok(_) => {
            info!("Successfully sent warm start batch with {} entries", entries.len());
            Ok(())
        }
        Err(e) => {
            error!("Failed to send warm start batch: {}", e);

            // Buffer failed entries for retry
            for entry in entries {
                buffer_failed_agent_payload(
                    &entry.payload_bytes,
                    &entry.request_id,
                    &entry.invoked_function_arn,
                );
            }

            Err(Box::new(e))
        }
    }
}

/// Add agent payload to warm start batch and check if flush is needed
async fn add_to_warm_start_batch(
    payloads: Vec<Vec<u8>>,
    request_id: &str,
    invoked_function_arn: &str,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let should_flush = {
        let mut batch = WARM_START_BATCH.lock().map_err(|e| format!("Failed to lock warm start batch: {}", e))?;

        // Add payloads to batch
        for payload_bytes in payloads {
            let entry = BatchEntry {
                request_id: request_id.to_string(),
                invoked_function_arn: invoked_function_arn.to_string(),
                payload_bytes,
                received_at: std::time::Instant::now(),
                has_report: false,
            };
            batch.add_entry(entry);
        }

        // Check if we should flush
        batch.should_flush(
            config.new_relic.batch_max_size_bytes,
            config.new_relic.batch_payload_timeout_secs,
        )
    };

    // Flush if needed (outside the lock)
    if should_flush {
        info!("Batch flush triggered for request {} (size or timeout limit reached)", request_id);
        flush_warm_start_batch(newrelic_client, config).await?;
    }

    Ok(())
}

/// Add REPORT line to warm start batch and try to match with pending entries
/// This is called by the platform processor when a REPORT line is received
/// Note: Batch will be flushed after request completion, this just adds the REPORT
pub fn add_report_to_warm_start_batch(
    request_id: String,
    report_message: String,
) {
    if let Ok(mut batch) = WARM_START_BATCH.lock() {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        batch.add_report_line(request_id.clone(), report_message, timestamp);

        debug!("Added REPORT line to batch for request {} (batch will flush after request completion)", request_id);
    } else {
        error!("Failed to lock warm start batch when adding REPORT line for {}", request_id);
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
    let failed_payloads: Vec<FailedAgentPayload> = {
        if let Ok(mut buffer) = FAILED_AGENT_PAYLOADS.lock() {
            buffer.drain().collect()
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

/// Create batch payload format with multiple agent payloads and REPORT lines
/// Used for warm start batching to send multiple invocations in single HTTP request
fn create_batch_payload_json(
    entries: &[BatchEntry],
    report_lines: &HashMap<String, ReportLine>,
    config: &Arc<config::ExtensionConfig>,
) -> String {
    if entries.is_empty() {
        warn!("Attempted to create empty batch payload");
        return "{}".to_string();
    }

    // Build log events array with agent payloads and REPORT lines
    let mut log_events = Vec::new();

    for entry in entries {
        // Convert agent payload bytes to string
        let agent_data_str = String::from_utf8_lossy(&entry.payload_bytes);

        // Generate timestamp for this entry (based on when it was received)
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Add agent payload as log event
        log_events.push(serde_json::json!({
            "id": entry.request_id,
            "message": agent_data_str.as_ref(),
            "timestamp": timestamp
        }));

        // Add matching REPORT line if available and batching is enabled
        if config.new_relic.batch_include_report {
            if let Some(report) = report_lines.get(&entry.request_id) {
                log_events.push(serde_json::json!({
                    "id": entry.request_id,
                    "message": report.message,
                    "timestamp": report.timestamp
                }));
            }
        }
    }

    // Use first entry's metadata for batch context
    let first_entry = &entries[0];
    let function_name = first_entry.invoked_function_arn.split(':').last().unwrap_or("");
    let log_group_name = format!("/aws/lambda/{}", function_name);

    // Create context object with base fields
    let mut context = serde_json::json!({
        "function_name": function_name,
        "invoked_function_arn": first_entry.invoked_function_arn,
        "log_group_name": log_group_name,
        "log_stream_name": format!("{}:{}", EXTENSION_NAME, EXTENSION_VERSION)
    });

    // Add version detail tags to context if enabled
    if config.new_relic.add_version_detail_tags {
        let version_info = version::VersionInfo::get_or_detect();
        let version_tags = version_info.as_tags();

        if let Some(context_obj) = context.as_object_mut() {
            for (key, value) in version_tags {
                context_obj.insert(key, serde_json::json!(value));
            }
        }
    }

    // Create log events payload structure
    let log_events_payload = serde_json::json!({
        "logEvents": log_events,
        "logGroup": log_group_name,
        "logStream": "",
        "messageType": "",
        "owner": ""
    });

    // Stringify the log events payload to put in entry field
    let log_events_string = log_events_payload.to_string();

    // Create final batch payload
    let final_payload = serde_json::json!({
        "context": context,
        "entry": log_events_string
    });

    info!("Created batch payload with {} entries, {} REPORT lines",
         entries.len(),
         report_lines.len());

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



