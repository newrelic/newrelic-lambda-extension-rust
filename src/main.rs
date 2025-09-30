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
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::sync::mpsc;
use once_cell::sync::Lazy;

use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, trace, warn};
use reqwest::Client;
use base64::{Engine as _, engine::general_purpose};

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

// Extension name and version from Cargo.toml
const EXTENSION_NAME: &str = env!("CARGO_PKG_NAME");
const EXTENSION_VERSION: &str = env!("CARGO_PKG_VERSION");

// --- Extension Constants ---
const EXTENSION_NAME_HEADER: &str = "Lambda-Extension-Name";
const EXTENSION_ID_HEADER: &str = "Lambda-Extension-Identifier";

// --- Global state for warm starts (like Go's global vars) ---
static mut INVOKED_FUNCTION_ARN: Option<String> = None;
static mut LAST_REQUEST_ID: Option<String> = None;

// --- Lazy-initialized global components (initialized once during cold start) ---
static INVOCATION_CONTEXT: Lazy<Arc<Mutex<InvocationContext>>> = 
    Lazy::new(|| Arc::new(Mutex::new(InvocationContext::default())));
static AGENT_PAYLOAD_BUFFER: Lazy<Arc<Mutex<Vec<Vec<u8>>>>> = 
    Lazy::new(|| Arc::new(Mutex::new(Vec::new())));

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
        eprintln!("[NR_EXT]:ERROR:Extension panic caught (Lambda will continue): {}", message);
        eprintln!("[NR_EXT]:ERROR:Panic location: {}", location);
        
        // Don't re-panic - just log and continue
    }));
    
    // CRITICAL: Wrap everything in error handling to prevent Lambda crashes
    match run_extension_safely().await {
        Ok(_) => {
            eprintln!("[NR_EXT]:INFO:Extension completed successfully");
            Ok(())
        }
        Err(e) => {
            // Log error but don't propagate - this prevents Lambda from crashing
            eprintln!("[NR_EXT]:ERROR:Extension failed but continuing gracefully: {}", e);
            eprintln!("[NR_EXT]:WARN:Lambda function will continue without New Relic monitoring");
            
            // Return Ok to prevent Lambda crash - this is critical!
            Ok(())
        }
    }
}

/// Safe wrapper around the main extension logic
async fn run_extension_safely() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let extension_startup_time = std::time::Instant::now();
    
    // Initialize config first (this sets up logging)
    let config = config::init_config().clone();
    let config = Arc::new(config);
    
    info!("Initializing version {} of the New Relic Lambda Extension...", EXTENSION_VERSION);

    // --- COLD START PHASE: Maximum parallel initialization ---
    
    // 1. PARALLEL: License key resolution
    let license_key_result = resolve_license_key_with_aws_fallback(&config).await;
    let license_key_option = license_key_result?;

    // 2. Early exit for disabled extension (no-op mode)
    if !config.new_relic.extension_enabled {
        info!("Extension telemetry processing disabled");
        let (client, extension_id) = initialize_lambda_runtime_client_and_register().await?;
        execute_noop_event_loop(&client, &extension_id).await;
        return Ok(());
    }

    // 3. Early exit if no license key available
    let Some(license_key) = license_key_option else {
        warn!("No license key available. Running in no-op mode.");
        let (client, extension_id) = initialize_lambda_runtime_client_and_register().await?;
        execute_noop_event_loop(&client, &extension_id).await;
        return Ok(());
    };

    // 4. Update config with resolved license key
    let mut updated_config = (*config).clone();
    updated_config.new_relic.license_key = Some(license_key);
    let config = Arc::new(updated_config);

    info!("NEW_RELIC_COLLECT_TRACE_ID setting: {}", config.new_relic.collect_trace_id);

    // 5. PARALLEL: Initialize ALL core components simultaneously for maximum cold start performance
    let (
        client_result,
        registration_result,
        agent_telemetry_rx_result,
        newrelic_client,
        runtime_done_channels
    ) = tokio::join!(
        initialize_http_client_with_timeout(),
        async {
            let client = initialize_http_client_with_timeout().await?;
            register_extension_with_lambda_runtime(&client).await
        },
        initialize_agent_telemetry_ipc_channel(),
        async { Arc::new(NewRelicClient::new(&config)) },
        async { mpsc::unbounded_channel::<()>() }
    );

    let client = Arc::new(client_result?);
    let (_registration, extension_id) = registration_result?;
    let agent_telemetry_rx = agent_telemetry_rx_result?;
    let (runtime_done_tx, runtime_done_rx) = runtime_done_channels;

    info!("Extension registered with ID: {}", extension_id);

    // 6. Start agent payload collector (background task)
    start_agent_payload_collector_background_task(agent_telemetry_rx);

    // 7. PARALLEL: Initialize telemetry processors
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

    // 8. PARALLEL: Setup telemetry listener and subscribe to Lambda Telemetry API
    let (telemetry_listener_result, _) = tokio::join!(
        setup_telemetry_listener(
            log_processor.clone(), 
            platform_processor.clone(), 
            Some(runtime_done_tx)
        ),
        async { () } // Placeholder for potential future parallel initialization
    );

    let telemetry_listener_address = telemetry_listener_result?;
    
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

    info!("Extension ready (cold start duration: {:?})", extension_startup_time.elapsed());

    // 11. Enter main event loop - THIS IS WHERE WARM STARTS LIVE
    let total_events_processed = execute_main_telemetry_processing_loop(
        &client,
        &extension_id,
        runtime_done_rx,
        log_processor,
        platform_processor,
        newrelic_client,
        config,
    ).await;

    // 12. Cleanup on shutdown
    perform_extension_shutdown_cleanup(harvester_handle, total_events_processed, extension_startup_time).await;

    Ok(())
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
        .timeout(Duration::from_secs(10))
        .build()?)
}

/// Initialize Lambda runtime client and register extension
async fn initialize_lambda_runtime_client_and_register() -> Result<(Arc<Client>, String), Box<dyn std::error::Error + Send + Sync>> {
    let client = Arc::new(initialize_http_client_with_timeout().await?);
    let (_registration, extension_id) = register_extension_with_lambda_runtime(&client).await?;
    Ok((client, extension_id))
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

/// Start agent payload collector as background task
fn start_agent_payload_collector_background_task(agent_telemetry_rx: mpsc::Receiver<Vec<u8>>) {
    agent::processor::start_agent_payload_collector(agent_telemetry_rx, Arc::clone(&AGENT_PAYLOAD_BUFFER));
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

/// Main telemetry processing event loop - where warm starts spend their time
/// Equivalent to Go's mainLoop function
async fn execute_main_telemetry_processing_loop(
    client: &Arc<Client>,
    extension_id: &str,
    mut runtime_done_rx: mpsc::UnboundedReceiver<()>,
    log_processor: Arc<LogProcessor>,
    platform_processor: Arc<PlatformProcessor>,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<config::ExtensionConfig>,
) -> u32 {
    let mut event_counter = 0;
    let mut previous_invocation_context: Option<(String, String)> = None;

    loop {
        // WARM START CRITICAL PATH: This is the ONLY work done on warm starts
        debug!("mainLoop: waiting for next lambda invocation event...");
        let event_processing_start_time = std::time::Instant::now();
        
        // Process any remaining agent payloads from previous invocation
        process_previous_invocation_agent_payloads(
            &previous_invocation_context,
            &newrelic_client,
            &config,
            &log_processor
        ).await;
        
        // Clear request_id after processing previous invocation's payloads
        if previous_invocation_context.is_some() {
            flush_all_buffers_before_request_id_reset(&log_processor).await;
            log_processor.clear_request_id();
        }

        // Fetch next Lambda runtime event (WARM START: Only real network call)
        let runtime_event = match fetch_next_lambda_runtime_event(client, extension_id).await {
            Ok(event) => event,
            Err(e) => {
                error!("Error receiving next event: {:?}. Continuing.", e);
                continue;
            }
        };

        event_counter += 1;

        match runtime_event {
            LambdaRuntimeEvent::Invoke { request_id, invoked_function_arn } => {
                // WARM START: Process invocation event
                trace!("Invocation processed in {:?} (request_id: {})", event_processing_start_time.elapsed(), request_id);
                
                process_lambda_invocation_event(
                    &request_id,
                    &invoked_function_arn,
                    &log_processor,
                    &platform_processor,
                    &mut runtime_done_rx,
                    &newrelic_client,
                    &config,
                ).await;
                
                // Save context for next iteration
                previous_invocation_context = Some((request_id, invoked_function_arn));
            }
            LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
                info!("Extension shutting down: {}", shutdown_reason);
                
                // Final cleanup
                perform_final_telemetry_flush(&log_processor, &platform_processor).await;
                process_final_agent_payloads(&previous_invocation_context, &newrelic_client, &config, &log_processor).await;
                
                break;
            }
        }
    }

    event_counter
}

/// Process Lambda invocation event
async fn process_lambda_invocation_event(
    request_id: &str,
    invoked_function_arn: &str,
    log_processor: &Arc<LogProcessor>,
    platform_processor: &Arc<PlatformProcessor>,
    runtime_done_rx: &mut mpsc::UnboundedReceiver<()>,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
) {
    let invocation_start_time = chrono::Utc::now();
    
    // Update global invocation context
    update_global_invocation_context(request_id, invoked_function_arn, invocation_start_time);
    
    // Setup log processor for new invocation
    setup_log_processor_for_new_invocation(log_processor, invocation_start_time, request_id).await;
    
    // Process platform events
    platform_processor.process_invoke_event(request_id, invoked_function_arn);
    
    // Wait for runtime completion
    wait_for_lambda_runtime_completion(runtime_done_rx).await;
    
    // Process current invocation's agent payloads
    process_current_invocation_agent_payloads(
        request_id,
        invoked_function_arn,
        newrelic_client,
        config,
        log_processor
    ).await;
    
    // Flush any remaining buffered logs
    flush_remaining_buffered_logs_at_invocation_end(log_processor).await;
}

/// Update global invocation context
fn update_global_invocation_context(request_id: &str, invoked_function_arn: &str, invocation_start_time: chrono::DateTime<chrono::Utc>) {
    // Update global state (like Go's global vars)
    unsafe {
        INVOKED_FUNCTION_ARN = Some(invoked_function_arn.to_string());
        LAST_REQUEST_ID = Some(request_id.to_string());
    }

    // Update shared context
    if let Ok(mut context) = INVOCATION_CONTEXT.lock() {
        context.request_id = request_id.to_string();
        context.invoked_function_arn = invoked_function_arn.to_string();
        context.trace_id = None; // Reset trace ID for new invocation
    } else {
        error!("Failed to lock invocation_context");
    }
    
    info!("New invocation started - request_id: {}, timestamp: {}", request_id, invocation_start_time);
}

/// Setup log processor for new invocation
async fn setup_log_processor_for_new_invocation(
    log_processor: &Arc<LogProcessor>,
    invocation_start_time: chrono::DateTime<chrono::Utc>,
    request_id: &str,
) {
    log_processor.set_invocation_start_time(invocation_start_time);
    log_processor.reset_trace_id_state();
    
    // Retry any failed logs from previous invocations
    if let Err(e) = log_processor.retry_failed_logs_before_invocation().await {
        error!("Error retrying failed logs: {}", e);
    }
    
    // Process any logs that were buffered waiting for this request_id
    if let Err(e) = log_processor.on_request_id_available(request_id).await {
        error!("Error processing buffered logs with request_id: {}", e);
    }
}

/// Wait for Lambda runtime completion signal
async fn wait_for_lambda_runtime_completion(runtime_done_rx: &mut mpsc::UnboundedReceiver<()>) {
    // Wait for runtime done signal (no timeout - guaranteed by Telemetry API)
    if runtime_done_rx.recv().await.is_some() {
        // Wait additional buffer time for any final telemetry
        tokio::time::sleep(Duration::from_millis(400)).await;
    } else {
        warn!("Runtime done channel closed unexpectedly");
    }
}

/// Process current invocation's agent payloads
async fn process_current_invocation_agent_payloads(
    request_id: &str,
    invoked_function_arn: &str,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<config::ExtensionConfig>,
    log_processor: &Arc<LogProcessor>,
) {
    // Check if agent payloads are available
    let buffer_size = {
        if let Ok(buffer) = AGENT_PAYLOAD_BUFFER.lock() {
            buffer.len()
        } else {
            error!("Failed to lock agent_payload_buffer");
            return;
        }
    };
    
    if buffer_size == 0 {
        warn!("[agentsend] No agent payloads found, agent may not be initialized or runtime handler not set");
    }

    process_agent_payloads_with_context(
        &Some(request_id.to_string()), 
        &Some(invoked_function_arn.to_string()), 
        newrelic_client, 
        config,
        log_processor
    ).await;
}

/// Flush remaining buffered logs at invocation end
async fn flush_remaining_buffered_logs_at_invocation_end(log_processor: &Arc<LogProcessor>) {
    if let Err(e) = log_processor.flush_buffered_logs_at_invocation_end().await {
        error!("Error flushing buffered logs at invocation end: {}", e);
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

/// Flush all buffers before clearing request_id
async fn flush_all_buffers_before_request_id_reset(log_processor: &Arc<LogProcessor>) {
    if let Err(e) = log_processor.flush_all_buffers_before_clear().await {
        error!("Error flushing all buffers before clearing request_id: {}", e);
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
    harvester_handle: tokio::task::JoinHandle<()>,
    total_events_processed: u32,
    extension_startup_time: std::time::Instant,
) {
    info!("New Relic Extension shutting down after {} events", total_events_processed);
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
    let payloads = {
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

    for payload_bytes in payloads {
        // Trace ID extraction (if enabled)
        if config.new_relic.collect_trace_id && !trace_id_found {
            if let Ok(Some(trace_id)) = trace::extract_trace_id_from_payload(&payload_bytes) {
                trace_id_found = true;
                {
                    let mut context = match INVOCATION_CONTEXT.lock() {
                        Ok(ctx) => ctx,
                        Err(e) => {
                            error!("Failed to lock invocation_context for trace ID: {}", e);
                            continue;
                        }
                    };
                    context.trace_id = Some(trace_id.clone());
                }
                
                if let Err(e) = log_processor.on_trace_id_extracted(&trace_id).await {
                    error!("Failed to process trace ID extraction: {}", e);
                }
            }
        }

        let wrapped_payload_json = create_wrapped_agent_payload_json(
            &payload_bytes,
            function_name,
            invoked_arn,
            &log_group_name,
            request_id,
        );

        match client.send_agent_payload(config, &wrapped_payload_json).await {
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
}

/// Create wrapped payload for agent telemetry as JSON string
fn create_wrapped_agent_payload_json(
    payload_bytes: &[u8],
    function_name: &str,
    invoked_function_arn: &str,
    log_group_name: &str,
    request_id: &str,
) -> String {
    let context = serde_json::json!({
        "function_name": function_name,
        "invoked_function_arn": invoked_function_arn,
        "log_group_name": log_group_name,
        "log_stream_name": request_id
    });

    let wrapped_payload = serde_json::json!({
        "context": context,
        "entry": general_purpose::STANDARD.encode(payload_bytes)
    });

    wrapped_payload.to_string()
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

    let response = client
        .get(&url)
        .header(EXTENSION_ID_HEADER, ext_id)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_else(|_| "Failed to read response body".to_string());
        error!("Next event request failed with status: {}, body: {}", status, body);
        return Err(format!("Next event request failed with status: {}", status).into());
    }

    let event: LambdaRuntimeEvent = response
        .json()
        .await?;

    Ok(event)
}