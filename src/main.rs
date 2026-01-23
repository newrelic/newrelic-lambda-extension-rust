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
mod error_synthesis;

#[cfg(debug_assertions)]
mod test_telemetry;

use std::{
    env,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use tokio::sync::mpsc;
use once_cell::sync::Lazy;

use tracing::{debug, error, info, warn};
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
        cleanup_old_failed_payloads,
    },
};

const EXTENSION_NAME: &str = env!("CARGO_PKG_NAME");
const EXTENSION_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Global current invocation context for telemetry processors
/// Following Go extension pattern: ARN starts empty, gets set by first INVOKE event
/// Uses RwLock for optimal concurrent read performance (multiple processors can read simultaneously)
static CURRENT_INVOCATION_CONTEXT: Lazy<Arc<RwLock<InvocationContext>>> = Lazy::new(|| {
    Arc::new(RwLock::new(InvocationContext {
        request_id: String::new(),
        invoked_function_arn: String::new(),
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
#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
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

        eprintln!(
            "[NR_EXT] ERROR Extension panic caught (Lambda will continue): {}",
            message
        );
        eprintln!("[NR_EXT] ERROR Panic location: {}", location);
    }));

    let extension_handle = tokio::spawn(async move {
        run_extension().await
    });

    match extension_handle.await {
        Ok(extension_result) => match extension_result {
            Ok(_) => {
                eprintln!("[NR_EXT] INFO Extension completed successfully");
                return Ok(());
            }
            Err(e) => {
                eprintln!(
                    "[NR_EXT] ERROR Extension failed but continuing gracefully: {}",
                    e
                );
                eprintln!("[NR_EXT] WARN Lambda function will continue without New Relic monitoring");
                return Ok(());
            }
        },
        // Task panicked - Lambda will continue but extension is inactive
        Err(join_error) => {
            eprintln!("[NR_EXT] CRITICAL Extension panicked but Lambda will continue");
            if join_error.is_panic() {
                eprintln!("[NR_EXT] CRITICAL Panic detected: {:?}", join_error);
            }
            eprintln!("[NR_EXT] INFO Lambda function will continue without New Relic monitoring");
            return Ok(());
        }
    }
}

/// Run the extension in true no-op mode - follows Extension API lifecycle but does nothing
/// Registers with Extension API and waits for INVOKE/SHUTDOWN events but processes nothing
async fn run_noop_extension() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    debug!("Extension running in NO-OP mode - no telemetry will be collected");
    eprintln!(
        "[NR_EXT] INFO Extension running in NO-OP mode - Lambda function will continue normally"
    );

    let (client, extension_id, _registration) =
        initialize_lambda_runtime_client_and_register().await?;

    debug!(
        "Extension registered in no-op mode with ID: {}",
        extension_id
    );

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
            }
            Ok(runtime::LambdaRuntimeEvent::Shutdown { shutdown_reason }) => {
                debug!(
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

            run_noop_extension().await?;
            return Ok(());
        }
    };

    let extension_components =
        extension_components.expect("extension_components should exist after error handling");

    info!(
        "Cold start initialization complete (duration: {:?})",
        extension_startup_time.elapsed()
    );

    let (total_events_processed, harvester_handle) =
        run_infinite_event_loop(extension_components).await;

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
    let config = config::init_config().clone();
    let config = Arc::new(config);

   

    if !config.new_relic.extension_enabled {
        debug!("Extension telemetry processing disabled - entering no-op mode");
        let (client, extension_id, _registration) =
            initialize_lambda_runtime_client_and_register().await?;

        let noop_newrelic_client = Arc::new(NewRelicClient::new_noop());
        let noop_apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let noop_processor_factory = Arc::new(ProcessorFactory::new(
            noop_newrelic_client.clone(),
            config.clone(),
            noop_apm_app.clone(),
        ));

        let dummy_context = Arc::new(Mutex::new(InvocationContext {
            request_id: "noop".to_string(),
            invoked_function_arn: "noop".to_string(),
            trace_id: None,
        }));
        let noop_log_processor = noop_processor_factory.create_log_processor(dummy_context.clone());
        let _noop_platform_processor =
            noop_processor_factory.create_platform_processor(dummy_context, noop_log_processor.clone());

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
    updated_config.new_relic.license_key = Some(license_key.clone());

    // Detect EU endpoints from license key prefix
    let license_key_prefix = license_key.get(0..2);

    if let Ok(host) = env::var("NEW_RELIC_HOST") {
        updated_config.new_relic.apm_host = host;
    } else if let Some("eu") = license_key_prefix {
        updated_config.new_relic.apm_host = "collector.eu01.nr-data.net".to_string();
    }

    if let Ok(endpoint) = env::var("NEW_RELIC_METRIC_ENDPOINT") {
        updated_config.new_relic.metric_endpoint = endpoint;
    } else if let Some("eu") = license_key_prefix {
        updated_config.new_relic.metric_endpoint = "https://metric-api.eu.newrelic.com/metric/v1".to_string();
    }

    if let Ok(endpoint) = env::var("NEW_RELIC_TELEMETRY_ENDPOINT") {
        updated_config.new_relic.telemetry_endpoint = endpoint;
    } else if let Some("eu") = license_key_prefix {
        updated_config.new_relic.telemetry_endpoint =
            "https://cloud-collector.eu01.nr-data.net/aws/lambda/v1".to_string();
    }

    if let Ok(endpoint) = env::var("NEW_RELIC_LOG_ENDPOINT") {
        updated_config.new_relic.log_endpoint = endpoint;
    } else if let Some("eu") = license_key_prefix {
        updated_config.new_relic.log_endpoint = "https://log-api.eu.newrelic.com/log/v1".to_string();
    }

    let config = Arc::new(updated_config);

    info!("License key validated and extension registered - proceeding with full initialization");
    
    // Construct registration fallback ARN once from registration response
    // This ARN is used for all telemetry before the first INVOKE event provides the actual invoked_function_arn
    // Also used as fallback when invoked_function_arn is not available
    let registration_fallback_arn = if let Some(ref account_id) = registration.account_id {
        let arn = format!(
            "arn:aws:lambda:{}:{}:function:{}",
            std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            account_id,
            registration.function_name
        );
        info!("Registration fallback ARN constructed: {}", arn);
        
        // Initialize global invocation context with fallback ARN
        if let Ok(mut global_context) = CURRENT_INVOCATION_CONTEXT.write() {
            global_context.invoked_function_arn = arn.clone();
            debug!("Initialized global context with registration fallback ARN");
        }
        
        Some(arn)
    } else {
        warn!("Account ID not provided by Lambda runtime (local testing?) - ARN will be populated from first INVOKE event");
        None
    };

    debug!(
        "NEW_RELIC_COLLECT_TRACE_ID setting: {}",
        config.new_relic.collect_trace_id
    );
    if config.new_relic.add_version_detail_tags {
        debug!("Version detail tagging enabled - will tag function on first invocation");
        debug!("  Extension version: {}", EXTENSION_VERSION);
        debug!("Version detection and tagging will happen lazily on first invocation to avoid AWS SDK initialization during INIT");
    }

    debug!(
        "Log forwarding settings: send_function_logs={}, send_extension_logs={}",
        config.extension.send_function_logs, config.extension.send_extension_logs
    );

   

    let mut updated_config = (*config).clone();
    updated_config.aws.update_from_registration(
        registration.function_name,
        registration.function_version,
        registration.account_id,
    );
    let config = Arc::new(updated_config);

    let (agent_telemetry_rx_result, newrelic_client, runtime_done_channels) = tokio::join!(
        initialize_agent_telemetry_ipc_channel(),
        async { Arc::new(NewRelicClient::new(&config)) },
        async { mpsc::unbounded_channel::<()>() }
    );

    let agent_telemetry_rx = agent_telemetry_rx_result?;
    let (runtime_done_tx, _runtime_done_rx) = runtime_done_channels;

    debug!(
        "Extension components initialized - ID: {} (license key pre-validated)",
        extension_id
    );

    start_agent_payload_collector_background_task(agent_telemetry_rx);

    cleanup_old_failed_payloads();

    // Smart conditional parallelization: only use tokio::join! when APM enabled
    // This avoids async overhead for standard mode (most common case)
    let (apm_app, processor_factory, temp_log_processor, telemetry_listener_address) =
        if config.new_relic.apm_lambda_mode {
            debug!("APM Lambda mode enabled - non-blocking connection strategy");

            // Spawn APM connection as background task - event loop starts immediately
            let apm_app = Arc::new(tokio::sync::RwLock::new(None));
            
            let license_key = config
                .new_relic
                .license_key
                .clone()
                .expect("License key must be available for APM mode");

            tokio::spawn({
                let license_key_clone = license_key.clone();
                let apm_host = config.new_relic.apm_host.clone();
                let metric_endpoint = config.new_relic.metric_endpoint.clone();
                let client_clone = (*client).clone();
                let function_name = config.aws.function_name.clone();
                let function_version = config.aws.function_version.clone().unwrap_or_else(|| "$LATEST".to_string());
                let account_id = config.aws.account_id.clone();
                let region = config.aws.region.clone();
                let apm_app_clone = Arc::clone(&apm_app);

                async move {
                    debug!("Background APM connection started...");
                    match apm::ApmApp::new(
                        license_key_clone,
                        apm_host,
                        metric_endpoint,
                        client_clone,
                        function_name,
                        function_version,
                        account_id,
                        region,
                    )
                    .await
                    {
                        Ok(app) => {
                            info!(
                                "APM app initialized successfully - Entity GUID: {}",
                                app.get_entity_guid()
                            );
                            let mut global_apm = apm_app_clone.write().await;
                            *global_apm = Some(app);
                            info!("APM connection complete - ready for agent payloads");
                        }
                        Err(e) => {
                            error!("CRITICAL: Failed to initialize APM app: {}", e);
                            error!("APM mode was explicitly enabled but connection failed");
                            error!("Extension will enter NO-OP mode - no telemetry will be processed");
                            warn!("Lambda function will continue but without New Relic monitoring");
                        }
                    }
                }
            });

            let processor_factory = Arc::new(ProcessorFactory::new(
                Arc::clone(&newrelic_client),
                Arc::clone(&config),
                Arc::clone(&apm_app),
            ));

            // Create a Mutex-wrapped context for processors (they expect Arc<Mutex<...>>)
            let temp_context = Arc::new(Mutex::new(InvocationContext::default()));
            let temp_log_processor = processor_factory.create_log_processor(temp_context.clone());
            let temp_platform_processor = processor_factory.create_platform_processor(temp_context, temp_log_processor.clone());

            // Set fallback ARN from registration for emergency shutdown before first INVOKE (if available)
            if let Some(ref arn) = registration_fallback_arn {
                temp_log_processor.set_fallback_arn(arn);
            }

            let telemetry_listener_address = setup_telemetry_listener(
                temp_log_processor.clone(),
                temp_platform_processor,
                Some(runtime_done_tx),
                config.new_relic.apm_lambda_mode,
            )
            .await?;

            (apm_app, processor_factory, temp_log_processor, telemetry_listener_address)
        } else {
            debug!("APM Lambda mode disabled - using sequential initialization");

            // Sequential path: simple flow with no async overhead
            let apm_app = Arc::new(tokio::sync::RwLock::new(None));

            let processor_factory = Arc::new(ProcessorFactory::new(
                Arc::clone(&newrelic_client),
                Arc::clone(&config),
                Arc::clone(&apm_app),
            ));

            // Create a Mutex-wrapped context for processors (they expect Arc<Mutex<...>>)
            let temp_context = Arc::new(Mutex::new(InvocationContext::default()));
            let temp_log_processor = processor_factory.create_log_processor(temp_context.clone());
            let temp_platform_processor = processor_factory.create_platform_processor(temp_context, temp_log_processor.clone());

            // Set fallback ARN from registration for emergency shutdown before first INVOKE (if available)
            if let Some(ref arn) = registration_fallback_arn {
                temp_log_processor.set_fallback_arn(arn);
            }

            let telemetry_listener_address = setup_telemetry_listener(
                temp_log_processor.clone(),
                temp_platform_processor,
                Some(runtime_done_tx),
                config.new_relic.apm_lambda_mode,
            )
            .await?;

            (apm_app, processor_factory, temp_log_processor, telemetry_listener_address)
        };

    runtime::subscribe_to_telemetry(&client, &extension_id, telemetry_listener_address.port())
        .await?;

    // Harvester enabled for periodic log flushing to reduce memory usage
    // Flushes function logs (if NEW_RELIC_EXTENSION_SEND_FUNCTION_LOGS=true),
    // extension logs (if NEW_RELIC_EXTENSION_SEND_EXTENSION_LOGS=true),
    // and platform logs (always, formatted as log lines except REPORT)
    let harvest_interval_secs = std::env::var("NEW_RELIC_HARVEST_INTERVAL_SECONDS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(5); // Default: 5 seconds for frequent log flushing
    
    debug!("Starting log harvester with {}s interval (function_logs={}, extension_logs={})",
        harvest_interval_secs,
        config.extension.send_function_logs,
        config.extension.send_extension_logs
    );
    
    let (_harvester, harvester_handle) = start_harvester_background_task(
        vec![], // No processors - only log/platform flushing
        Duration::from_secs(harvest_interval_secs),
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
    warn!("No license key available after checking all sources. Running in no-op mode.");

    let mut updated_config = (*config).clone();
    updated_config.aws.update_from_registration(
        registration.function_name,
        registration.function_version,
        registration.account_id,
    );
    let config = Arc::new(updated_config);

    let noop_newrelic_client = Arc::new(NewRelicClient::new_noop());
    let noop_apm_app = Arc::new(tokio::sync::RwLock::new(None));
    let noop_processor_factory = Arc::new(ProcessorFactory::new(
        noop_newrelic_client.clone(),
        config.clone(),
        noop_apm_app.clone(),
    ));

    let dummy_context = Arc::new(Mutex::new(InvocationContext {
        request_id: "noop".to_string(),
        invoked_function_arn: "noop".to_string(),
        trace_id: None,
    }));
    let noop_log_processor = noop_processor_factory.create_log_processor(dummy_context.clone());
    let _noop_platform_processor = noop_processor_factory.create_platform_processor(dummy_context, noop_log_processor.clone());

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
    let credentials_config = config::Configuration::from(config.as_ref());

    // Early return if license key is already in environment variable
    if !credentials_config.license_key.is_empty() {
        debug!("License key found in environment variable - skipping AWS SDK initialization for credentials");
        return Ok(Some(credentials_config.license_key.clone()));
    }

    // Always attempt to fetch from AWS sources (Secrets Manager, SSM, or fallback to default names)
    debug!("License key not in env var, attempting to fetch from AWS Secrets Manager or SSM Parameter Store");
    match get_new_relic_license_key(&credentials_config).await {
        Ok(key) => {
            info!("Successfully obtained New Relic license key from AWS");
            Ok(Some(key))
        }
        Err(e) => {
            debug!(
                "No license key found from AWS sources: {}. Extension will run in no-op mode.",
                e
            );
            Ok(None)
        }
    }
}

/// Initialize Lambda runtime client and register extension
/// This ensures /next polling is never affected by other HTTP operations
async fn initialize_lambda_runtime_client_and_register(
) -> Result<
    (
        Arc<Client>,
        String,
        runtime::ExtensionRegistrationResponse,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    // Only connect_timeout for TCP setup, NO timeout() for HTTP requests
    // This allows /next to block indefinitely waiting for INVOKE/SHUTDOWN events
    let lambda_runtime_client = Arc::new(Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .tcp_keepalive(Duration::from_secs(60))
        .pool_idle_timeout(Duration::from_secs(300))  // Keep connections alive 5 min
        .pool_max_idle_per_host(10)
        .build()?);

    let (registration, extension_id) = runtime::register_extension(&lambda_runtime_client, EXTENSION_NAME).await?;
    Ok((lambda_runtime_client, extension_id, registration))
}

/// Initialize agent telemetry IPC channel
async fn initialize_agent_telemetry_ipc_channel(
) -> Result<mpsc::Receiver<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
    match agent::ipc::init_telemetry_channel().await {
        Ok(rx) => {
            debug!(
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
        debug!("Agent payload collector started - continuously listening for agent payloads");
        let mut payload_count = 0;

        while let Some(payload_bytes) = receiver.recv().await {
            payload_count += 1;

            debug!(
                "Received agent payload #{} ({} bytes) - processing immediately",
                payload_count,
                payload_bytes.len()
            );

            if payload_count <= 5 {
                // Print complete payload with escaped newlines to prevent log corruption
                let full_payload = String::from_utf8_lossy(&payload_bytes);
                let sanitized = full_payload.replace('\n', "\\n").replace('\r', "\\r");
                debug!("Agent Payload (complete): {}", sanitized);
            }

            route_payload_to_request_buffer(payload_bytes).await;
        }

        debug!("Agent payload collector channel closed. No more agent payloads will be received");
    });
}

/// Start harvester as background task
fn start_harvester_background_task(
    processors: Vec<Arc<dyn Flush>>,
    harvest_interval: Duration,
    processor_factory: &Arc<ProcessorFactory>,
) -> (Arc<Harvester>, tokio::task::JoinHandle<()>) {
    let dummy_context = Arc::new(Mutex::new(InvocationContext {
        request_id: "harvester".to_string(),
        invoked_function_arn: "harvester".to_string(),
        trace_id: None,
    }));
    let dummy_log_processor = processor_factory.create_log_processor(dummy_context.clone());
    let dummy_platform_processor = processor_factory.create_platform_processor(dummy_context, dummy_log_processor.clone());

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

    harvester_handle.abort();

    let shutdown_at = std::time::Instant::now();
    let total_runtime = shutdown_at.duration_since(extension_startup_time);
    info!(
        "Extension shutdown after {}ms",
        total_runtime.as_millis()
    );
}
