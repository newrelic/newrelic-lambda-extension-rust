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
mod runtime;
mod request;
mod event_loop;

#[cfg(debug_assertions)]
mod test_telemetry;

use std::{
    env,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::sync::mpsc;
use once_cell::sync::Lazy;

use tracing::{debug, error, info};
use reqwest::Client;

use crate::{
    context::InvocationContext,
    telemetry::listener::setup_telemetry_listener,
    newrelic::{
        client::NewRelicClient,
        harvester::Harvester,
        flush::Flush,
    },
    credentials::get_new_relic_license_key,
    request::{route_payload_to_request_buffer, ProcessorFactory},
    event_loop::{
        run_infinite_event_loop, ExtensionComponents,
        cleanup_old_failed_payloads, start_batch_timeout_task,
    },
};

const EXTENSION_NAME: &str = env!("CARGO_PKG_NAME");
const EXTENSION_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Global current invocation context for telemetry processors
static CURRENT_INVOCATION_CONTEXT: Lazy<Arc<Mutex<InvocationContext>>> = Lazy::new(|| {
    Arc::new(Mutex::new(InvocationContext {
        request_id: "temp".to_string(),
        invoked_function_arn: "temp".to_string(),
        trace_id: None,
    }))
});

/// Global flag to track if this is a warm start (for performance optimization)
static IS_WARM_START: Lazy<Arc<std::sync::atomic::AtomicBool>> =
    Lazy::new(|| Arc::new(std::sync::atomic::AtomicBool::new(false)));

/// Global APM app instance (for sending platform.report metrics in APM mode)
static APM_APP: Lazy<Arc<tokio::sync::RwLock<Option<apm::ApmApp>>>> =
    Lazy::new(|| Arc::new(tokio::sync::RwLock::new(None)));

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
        eprintln!(
            "[NR_EXT] ERROR Extension panic caught (Lambda will continue): {}",
            message
        );
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
            eprintln!(
                "[NR_EXT] ERROR Extension failed but continuing gracefully: {}",
                e
            );
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
    eprintln!(
        "[NR_EXT] INFO Extension running in NO-OP mode - Lambda function will continue normally"
    );

    // Even in no-op mode, we need to properly register with the Lambda Extensions API
    // and wait for INVOKE/SHUTDOWN events to follow the correct lifecycle
    let (client, extension_id, _registration) =
        initialize_lambda_runtime_client_and_register().await?;

    info!(
        "Extension registered in no-op mode with ID: {}",
        extension_id
    );

    // Follow proper Extension API lifecycle - wait for events but do nothing with them
    loop {
        match runtime::fetch_next_event(&client, &extension_id).await {
            Ok(runtime::LambdaRuntimeEvent::Invoke {
                request_id,
                invoked_function_arn: _,
            }) => {
                debug!(
                    "No-op mode: Received INVOKE event for request {}, doing nothing",
                    request_id
                );
                // Do absolutely nothing - no telemetry, no processing, no network calls
            }
            Ok(runtime::LambdaRuntimeEvent::Shutdown { shutdown_reason }) => {
                info!(
                    "No-op mode: Extension shutting down: {}",
                    shutdown_reason
                );
                break;
            }
            Err(e) => {
                error!(
                    "Error receiving next event in no-op mode: {:?}. Continuing.",
                    e
                );
            }
        }
    }

    Ok(())
}

/// Main extension logic following correct Lambda extension lifecycle
async fn run_extension() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let extension_startup_time = std::time::Instant::now();

    info!(
        "=== COLD START: Initializing version {} of the New Relic Lambda Extension ===",
        EXTENSION_VERSION
    );

    // PHASE 1: ONE-TIME COLD START INITIALIZATION (with true no-op fallback)
    let extension_components = match perform_one_time_initialization().await {
        Ok(components) => {
            info!("Extension initialization successful");
            Some(components)
        }
        Err(e) => {
            error!(
                "Extension initialization failed, entering true no-op mode: {}",
                e
            );
            eprintln!(
                "[NR_EXT] ERROR Initialization failed, entering true no-op mode (Lambda function will continue normally): {}",
                e
            );

            // Enter true no-op mode - do absolutely nothing
            run_noop_extension().await?;
            return Ok(()); // This return will never be reached since noop runs forever
        }
    };

    let extension_components =
        extension_components.expect("extension_components should exist after error handling");

    info!(
        "Cold start initialization complete (duration: {:?})",
        extension_startup_time.elapsed()
    );

    // PHASE 2: INFINITE EVENT LOOP (handles both first invoke and all warm starts)
    let (total_events_processed, harvester_handle) =
        run_infinite_event_loop(extension_components).await;

    // PHASE 3: CLEANUP ON SHUTDOWN (only happens once per container lifecycle)
    perform_extension_shutdown_cleanup(
        total_events_processed,
        harvester_handle,
        extension_startup_time,
    )
    .await;

    Ok(())
}

/// Perform all one-time initialization - called only once per container
async fn perform_one_time_initialization(
) -> Result<ExtensionComponents, Box<dyn std::error::Error + Send + Sync>> {
    // Initialize config first (this sets up logging)
    let config = config::init_config().clone();
    let config = Arc::new(config);

    // --- PHASE 1: PARALLEL CRITICAL OPERATIONS (maximum performance) ---

    // 1. Early exit for disabled extension (no-op mode)
    if !config.new_relic.extension_enabled {
        info!("Extension telemetry processing disabled - entering no-op mode");
        let (client, extension_id, _registration) =
            initialize_lambda_runtime_client_and_register().await?;

        // Create noop processor factory
        let noop_newrelic_client = Arc::new(NewRelicClient::new_noop());
        let noop_apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let noop_processor_factory = Arc::new(ProcessorFactory::new(
            noop_newrelic_client.clone(),
            config.clone(),
            noop_apm_app.clone(),
        ));

        // Create dummy processors for harvester
        let dummy_context = Arc::new(Mutex::new(InvocationContext {
            request_id: "noop".to_string(),
            invoked_function_arn: "noop".to_string(),
            trace_id: None,
        }));
        let noop_log_processor = noop_processor_factory.create_log_processor(dummy_context.clone());
        let _noop_platform_processor =
            noop_processor_factory.create_platform_processor(dummy_context);

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
        return handle_no_license_key(config, client, extension_id, registration).await;
    };

    let mut updated_config = (*config).clone();
    updated_config.new_relic.license_key = Some(license_key);
    let config = Arc::new(updated_config);

    info!("License key validated and extension registered - proceeding with full initialization");

    info!(
        "NEW_RELIC_COLLECT_TRACE_ID setting: {}",
        config.new_relic.collect_trace_id
    );
    info!(
        "NEW_RELIC_ADD_VERSION_DETAIL_TAGS setting: {}",
        config.new_relic.add_version_detail_tags
    );

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

    info!(
        "Log forwarding settings: send_function_logs={}, send_extension_logs={}",
        config.extension.send_function_logs, config.extension.send_extension_logs
    );

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
    let (agent_telemetry_rx_result, newrelic_client, runtime_done_channels) = tokio::join!(
        initialize_agent_telemetry_ipc_channel(),
        async { Arc::new(NewRelicClient::new(&config)) },
        async { mpsc::unbounded_channel::<()>() }
    );

    let agent_telemetry_rx = agent_telemetry_rx_result?;
    let (runtime_done_tx, _runtime_done_rx) = runtime_done_channels;

    info!(
        "Extension components initialized - ID: {} (license key pre-validated)",
        extension_id
    );

    start_agent_payload_collector_background_task(agent_telemetry_rx);

    // Clean up very old failed payloads (older than 24 hours)
    cleanup_old_failed_payloads();

    // Start batch timeout background task (checks every 30 seconds for 5-minute timeout)
    start_batch_timeout_task(Arc::clone(&newrelic_client), Arc::clone(&config));

    // 9. Initialize APM app if APM mode is enabled (before processor factory)
    let apm_app = if config.new_relic.apm_lambda_mode {
        info!("APM Lambda mode enabled - initializing APM connection");
        let license_key = config
            .new_relic
            .license_key
            .clone()
            .expect("License key must be available for APM mode");

        match apm::ApmApp::new(
            license_key,
            config.new_relic.apm_host.clone(),
            config.new_relic.metric_endpoint.clone(),
            (*client).clone(),
        )
        .await
        {
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
                info!(
                    "Failed to initialize APM app: {} - continuing without APM mode",
                    e
                );
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
        Arc::clone(&apm_app),
    ));

    // Setup telemetry listener (use global current context that gets updated per request)
    let global_context = Arc::clone(&CURRENT_INVOCATION_CONTEXT);
    let temp_log_processor = processor_factory.create_log_processor(global_context.clone());
    let temp_platform_processor = processor_factory.create_platform_processor(global_context);

    let telemetry_listener_address = setup_telemetry_listener(
        temp_log_processor.clone(),
        temp_platform_processor,
        Some(runtime_done_tx),
    )
    .await?;

    runtime::subscribe_to_telemetry(&client, &extension_id, telemetry_listener_address.port())
        .await?;

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

/// Handle case when no license key is available
async fn handle_no_license_key(
    config: Arc<config::ExtensionConfig>,
    client: Arc<Client>,
    extension_id: String,
    registration: runtime::ExtensionRegistrationResponse,
) -> Result<ExtensionComponents, Box<dyn std::error::Error + Send + Sync>> {
    info!("No license key available after checking all sources. Running in no-op mode.");

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
        noop_apm_app.clone(),
    ));

    // Create dummy processors for harvester
    let dummy_context = Arc::new(Mutex::new(InvocationContext {
        request_id: "noop".to_string(),
        invoked_function_arn: "noop".to_string(),
        trace_id: None,
    }));
    let noop_log_processor = noop_processor_factory.create_log_processor(dummy_context.clone());
    let _noop_platform_processor = noop_processor_factory.create_platform_processor(dummy_context);

    Ok(ExtensionComponents {
        client,
        extension_id,
        processor_factory: noop_processor_factory,
        newrelic_client: noop_newrelic_client,
        config: config.clone(),
        harvester_handle: tokio::spawn(async {}),
        global_log_processor: noop_log_processor,
        apm_app: Arc::new(tokio::sync::RwLock::new(None)),
    })
}

async fn resolve_license_key_with_aws_fallback(
    config: &Arc<config::ExtensionConfig>,
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    // Fix: Dereference the Arc to get &ExtensionConfig
    let credentials_config = config::Configuration::from(config.as_ref());

    // Check if AWS services are needed for license key resolution
    let aws_services_required = credentials_config.license_key.is_empty()
        && (std::env::var("NEW_RELIC_LICENSE_KEY_SECRET").is_ok()
            || std::env::var("NEW_RELIC_LICENSE_KEY_SSM_PARAMETER_NAME").is_ok()
            || !credentials_config.license_key_secret_id.is_empty()
            || !credentials_config.license_key_ssm_parameter_name.is_empty()
            || std::env::var("AWS_LAMBDA_RUNTIME_API").is_ok());

    if !credentials_config.license_key.is_empty() {
        Ok(Some(credentials_config.license_key.clone()))
    } else if aws_services_required {
        match get_new_relic_license_key(&credentials_config).await {
            Ok(key) => {
                debug!("Successfully obtained New Relic license key from AWS");
                Ok(Some(key))
            }
            Err(e) => {
                info!(
                    "No license key found from AWS sources: {}. Extension will run in no-op mode.",
                    e
                );
                Ok(None)
            }
        }
    } else {
        info!(
            "No license key available and not in AWS Lambda environment. Extension will run in no-op mode."
        );
        Ok(None)
    }
}

/// Initialize HTTP client with appropriate timeout
async fn initialize_http_client_with_timeout(
) -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
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
async fn initialize_lambda_runtime_client_and_register(
) -> Result<
    (
        Arc<Client>,
        String,
        runtime::ExtensionRegistrationResponse,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let client = Arc::new(initialize_http_client_with_timeout().await?);
    let (registration, extension_id) = runtime::register_extension(&client, EXTENSION_NAME).await?;
    Ok((client, extension_id, registration))
}

/// Initialize agent telemetry IPC channel
async fn initialize_agent_telemetry_ipc_channel(
) -> Result<mpsc::Receiver<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
    match agent::ipc::init_telemetry_channel().await {
        Ok(rx) => {
            info!(
                "Agent telemetry channel initialized, listening on pipe: {}",
                agent::ipc::TELEMETRY_NAMED_PIPE_PATH
            );
            Ok(rx)
        }
        Err(e) => {
            error!(
                "FATAL: Failed to initialize agent telemetry pipe: {}. Exiting.",
                e
            );
            Err(Box::new(e))
        }
    }
}

/// Start agent payload collector as background task with request handling
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

            info!(
                "Received agent payload #{} ({} bytes) - processing immediately",
                payload_count,
                payload_bytes.len()
            );

            if payload_count <= 5 {
                debug!(
                    "Agent Payload preview: {:?}",
                    String::from_utf8_lossy(
                        &payload_bytes[..std::cmp::min(100, payload_bytes.len())]
                    )
                );
            }

            // Route payload (will try immediate processing first, store if it fails)
            route_payload_to_request_buffer(payload_bytes).await;
        }

        info!("Agent payload collector channel closed. No more agent payloads will be received");
    });
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

    let harvester = Arc::new(Harvester::new(
        processors,
        harvest_interval,
        dummy_log_processor,
        dummy_platform_processor,
    ));
    let harvester_clone = Arc::clone(&harvester);
    let handle = tokio::spawn(async move {
        harvester_clone.run().await;
    });
    (harvester, handle)
}

/// Perform extension shutdown cleanup
async fn perform_extension_shutdown_cleanup(
    total_events_processed: u32,
    harvester_handle: tokio::task::JoinHandle<()>,
    extension_startup_time: std::time::Instant,
) {
    info!(
        "New Relic Extension shutting down after {} events",
        total_events_processed
    );

    // Stop harvester background task
    harvester_handle.abort();

    let shutdown_at = std::time::Instant::now();
    let total_runtime = shutdown_at.duration_since(extension_startup_time);
    info!(
        "Extension shutdown after {}ms",
        total_runtime.as_millis()
    );
}
