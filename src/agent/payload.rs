//! Agent payload formatting and sending utilities
//!
//! This module handles:
//! - Wrapping agent payloads in New Relic log format
//! - Adding Lambda context metadata
//! - Version detail tagging (if enabled)
//! - Sending payloads to New Relic serverless ingest API

use std::sync::Arc;
use tracing::{debug, error};

use crate::{
    config::ExtensionConfig,
    newrelic::client::NewRelicClient,
    version,
    EXTENSION_NAME, EXTENSION_VERSION,
};

/// Send agent payload to New Relic serverless ingest API
pub async fn send_agent_payload_to_newrelic(
    payload_bytes: &[u8],
    request_id: &str,
    invoked_function_arn: &str,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<ExtensionConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let function_name = invoked_function_arn.split(':').next_back().unwrap_or("");
    let log_group_name = format!("/aws/lambda/{function_name}");

    let wrapped_payload = create_wrapped_agent_payload_json(
        payload_bytes,
        function_name,
        invoked_function_arn,
        &log_group_name,
        request_id,
        config,
    );

    match newrelic_client
        .send_agent_payload(config, &wrapped_payload)
        .await
    {
        Ok(()) => {
            debug!(
                "Successfully sent agent payload for request {}",
                request_id
            );
            Ok(())
        }
        Err(e) => {
            error!(
                "Failed to send agent payload for request {}: {}",
                request_id, e
            );
            Err(Box::new(e))
        }
    }
}

/// Create wrapped agent payload JSON string
/// Create New Relic log format with agent data in message field
/// NOTE: `Trace ID` extraction is handled separately in `process_and_send_agent_payload`
fn create_wrapped_agent_payload_json(
    payload_bytes: &[u8],
    function_name: &str,
    invoked_function_arn: &str,
    log_group_name: &str,
    request_id: &str,
    config: &Arc<ExtensionConfig>,
) -> String {
    debug!(
        "Processing agent data of {} bytes for function: {}",
        payload_bytes.len(),
        function_name
    );

    // Create New Relic log event format with agent data as message
    create_newrelic_log_format(
        payload_bytes,
        function_name,
        invoked_function_arn,
        log_group_name,
        request_id,
        config,
    )
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
    config: &Arc<ExtensionConfig>,
) -> String {
    // Convert agent data to string (should be JSON array like [1,"NR_LAMBDA_MONITORING","compressed_data"])
    let agent_data_str = String::from_utf8_lossy(agent_data);
    debug!("Agent data to wrap in log format: {}", agent_data_str);

    // Generate timestamp in milliseconds
    #[allow(clippy::cast_possible_truncation)]
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
            debug!(
                "Added {} version detail tags to agent payload context",
                context_obj.len() - 4
            ); // Subtract the 4 base fields
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
