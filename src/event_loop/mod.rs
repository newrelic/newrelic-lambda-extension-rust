pub mod apm;
pub mod payload;
pub mod standard;

use std::sync::Arc;
use reqwest::Client;
use tracing::{debug, error, info, trace};

use crate::{
    runtime,
    config::{self, ExtensionConfig},
    newrelic::client::NewRelicClient,
    logs::processor::LogProcessor,
    request::ProcessorFactory,
    version,
};

// Re-export public items so external callers don't break
pub use payload::{LAST_REQUEST_CONTEXT, cleanup_old_failed_payloads};

#[derive(Debug)]
pub struct ExtensionComponents {
    pub client: Arc<Client>,
    pub extension_id: String,
    pub processor_factory: Arc<ProcessorFactory>,
    pub newrelic_client: Arc<NewRelicClient>,
    pub config: Arc<ExtensionConfig>,
    pub harvester_handle: tokio::task::JoinHandle<()>,
    pub global_log_processor: Arc<LogProcessor>,
    pub apm_app: crate::apm::SharedApmApp,
    pub apm_mode_enabled: bool, // Actual mode after runtime detection (may differ from config for Java)
}

/// Event loop: handles cold start (first invoke) and warm starts (subsequent invokes)
pub async fn run_infinite_event_loop(
    mut extension_components: ExtensionComponents,
) -> (u32, tokio::task::JoinHandle<()>) {
    if !extension_components.config.new_relic.extension_enabled
        || extension_components.config.new_relic.license_key.is_none()
    {
        info!("Running in no-op mode");
        execute_noop_event_loop(&extension_components.client, &extension_components.extension_id)
            .await;
        return (0, extension_components.harvester_handle);
    }

    let total_events = execute_main_telemetry_processing_loop(&mut extension_components).await;
    (total_events, extension_components.harvester_handle)
}

/// Lambda extension pattern: GET /next (block) -> process INVOKE -> repeat until SHUTDOWN
/// Routes to APM or standard mode based on config (or runtime override for Java)
async fn execute_main_telemetry_processing_loop(components: &mut ExtensionComponents) -> u32 {
    let apm_mode_enabled = components.apm_mode_enabled;
    if apm_mode_enabled {
        info!("Starting APM mode event loop (connection may still be in progress)");
        apm::execute_apm_mode_event_loop(components).await
    } else {
        debug!("Starting standard mode event loop");
        standard::execute_standard_mode_event_loop(components).await
    }
}

pub async fn execute_noop_event_loop(client: &Arc<Client>, extension_id: &str) {
    info!("Starting no-op mode, no telemetry will be sent");

    loop {
        let loop_start = std::time::Instant::now();
        match runtime::fetch_next_event(client, extension_id).await {
            Ok(runtime::LambdaRuntimeEvent::Shutdown { shutdown_reason: _ }) => {
                debug!("Extension shutting down");
                break;
            }
            Ok(runtime::LambdaRuntimeEvent::Invoke {
                request_id,
                invoked_function_arn: _,
            }) => {
                trace!(
                    "No-op mode invocation processed in {:?} (request_id: {})",
                    loop_start.elapsed(),
                    request_id
                );
            }
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("403") || error_msg.contains("State transition") {
                    error!("Fatal extension state error (403 - Lambda shutting down): {:?}", e);
                    debug!("No-op mode exiting due to Lambda shutdown");
                    break;
                }
                error!("Error in no-op event loop: {:?}. Continuing.", e);
            }
        }
    }
}

/// Tag Lambda function once on first invocation
pub(crate) fn tag_lambda_function_once(invoked_function_arn: String, config: &config::ExtensionConfig) {
    static TAGGING_DONE: std::sync::Once = std::sync::Once::new();
    TAGGING_DONE.call_once(|| {
        debug!("Spawning background task to tag Lambda function with version information");
        let version_info = version::VersionInfo::get_or_detect(config.new_relic.layer_version.clone());
        let add_version_detail_tags = config.new_relic.add_version_detail_tags;
        let layer_version_from_config = config.new_relic.layer_version.clone();
        let function_name = config.aws.function_name.clone();
        version::tagging::tag_lambda_function_background(
            version_info.extension_version.clone(),
            version_info.agent_version.clone(),
            version_info.layer_version.clone(),
            invoked_function_arn,
            layer_version_from_config,
            add_version_detail_tags,
            function_name,
        );
    });
}

/// Update global invocation context for telemetry processors
pub(crate) fn update_global_invocation_context(request_id: &str, invoked_function_arn: &str) {
    if let Ok(mut global_context) = crate::CURRENT_INVOCATION_CONTEXT.write() {
        // Validate ARN before updating
        if invoked_function_arn.is_empty() {
            error!(
                "CRITICAL: Attempted to update global context with EMPTY invoked_function_arn for request_id: {}. Keeping previous ARN: {}",
                request_id,
                global_context.invoked_function_arn
            );
        } else {
            debug!(
                "Updating global context: request_id='{}', invoked_function_arn='{}' (previous ARN: '{}')",
                request_id,
                invoked_function_arn,
                global_context.invoked_function_arn
            );
            global_context.invoked_function_arn = invoked_function_arn.to_string();
        }
        global_context.request_id = request_id.to_string();
        global_context.trace_id = None;
    }
}

/// Extract trace ID from agent payload if enabled in config
pub(crate) async fn extract_and_coordinate_trace_id(
    payload_bytes: &[u8],
    config: &Arc<ExtensionConfig>,
    log_processor: &Arc<LogProcessor>,
) {
    if !config.new_relic.collect_trace_id {
        return;
    }

    if let Ok(Some(trace_id)) = crate::trace::extract_trace_id_from_payload(payload_bytes) {
        debug!("Extracted trace ID: {}, coordinating with logs", trace_id);
        if let Err(e) = log_processor.on_trace_id_extracted(&trace_id).await {
            error!("Failed to coordinate logs with trace ID: {}", e);
        }
    }
}
