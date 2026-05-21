// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Agent payload formatting and sending utilities
//!
//! This module handles:
//! - Wrapping agent payloads in New Relic log format
//! - Adding Lambda context metadata
//! - Version detail tagging (if enabled)
//! - Sending payloads to New Relic serverless ingest API

use std::sync::Arc;
use tracing::{debug, error, warn};

use crate::{
    config::ExtensionConfig,
    newrelic::client::NewRelicClient,
    version,
    EXTENSION_NAME, EXTENSION_VERSION,
};

/// Extract function name from Lambda ARN with proper validation
/// Lambda ARN format: arn:aws:lambda:{region}:{account}:function:{function-name}[:version]
/// Returns the function name or a fallback value from config
fn extract_function_name_from_arn<'a>(arn: &'a str, config_function_name: &'a str) -> &'a str {
    if arn.is_empty() {
        error!(
            "CRITICAL: invoked_function_arn is EMPTY. Using fallback from config: {}",
            config_function_name
        );
        return config_function_name;
    }

    let parts: Vec<&str> = arn.split(':').collect();
    
    // Standard ARN format: arn:aws:lambda:region:account:function:name[:version]
    // Positions:              0   1    2      3      4        5        6      7(optional)
    if parts.len() >= 7 && parts[0] == "arn" && parts[2] == "lambda" && parts[5] == "function" {
        debug!("Extracted function name '{}' from valid ARN", parts[6]);
        return parts[6];
    }
    
    // Fallback: try to use last segment ONLY if it looks like a valid function name (for malformed ARNs)
    if let Some(last_segment) = parts.last() {
        // Must be non-empty, not a keyword, and at least 3 chars long to be a plausible function name
        if !last_segment.is_empty() 
            && *last_segment != "function" 
            && *last_segment != "arn"
            && *last_segment != "aws"
            && *last_segment != "lambda"
            && last_segment.len() >= 3
        {
            warn!(
                "ARN format validation failed (expected 7+ parts, got {}). Using last segment '{}' as function name. Full ARN: {}",
                parts.len(),
                last_segment,
                arn
            );
            return last_segment;
        }
    }
    
    // Ultimate fallback: use config
    error!(
        "Failed to extract function name from malformed ARN '{}'. Using fallback from config: {}",
        arn,
        config_function_name
    );
    config_function_name
}

/// Send agent payload to New Relic serverless ingest API
pub async fn send_agent_payload_to_newrelic(
    payload_bytes: &[u8],
    request_id: &str,
    invoked_function_arn: &str,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<ExtensionConfig>,
    version_info: Option<&Arc<version::VersionInfo>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // ARN fallback is handled by extract_function_name_from_arn → config.aws.function_name
    let function_name = extract_function_name_from_arn(invoked_function_arn, &config.aws.function_name);

    debug!(
        "Agent payload context: request_id='{}', invoked_function_arn='{}', extracted_function_name='{}'",
        request_id,
        invoked_function_arn,
        function_name
    );
    let log_group_name = format!("/aws/lambda/{function_name}");

    let wrapped_payload = create_wrapped_agent_payload_json(
        payload_bytes,
        function_name,
        invoked_function_arn,
        &log_group_name,
        request_id,
        config,
        version_info,
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
    version_info: Option<&Arc<version::VersionInfo>>,
) -> String {
    debug!(
        "Processing agent data of {} bytes for function: {}",
        payload_bytes.len(),
        function_name
    );

    create_newrelic_log_format(
        payload_bytes,
        function_name,
        invoked_function_arn,
        log_group_name,
        request_id,
        config,
        version_info,
    )
}

/// Create New Relic format with Lambda context and stringified log events in entry field
/// Returns JSON with context and entry fields matching New Relic expected format
/// NOTE: This is for AGENT payload wrapping, not regular log processing
/// In serverless mode, appends version line as a second log event (like platform.report)
fn create_newrelic_log_format(
    agent_data: &[u8],
    function_name: &str,
    invoked_function_arn: &str,
    log_group_name: &str,
    request_id: &str,
    config: &Arc<ExtensionConfig>,
    version_info: Option<&Arc<version::VersionInfo>>,
) -> String {
    let agent_data_str = String::from_utf8_lossy(agent_data);
    debug!("Agent data to wrap in log format: {}", agent_data_str);

    #[allow(clippy::cast_possible_truncation)]
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Build log events array: agent payload first, then report line (if any), then version line (serverless mode only)
    let mut log_events = vec![
        serde_json::json!({
            "id": request_id,
            "message": agent_data_str,
            "timestamp": timestamp
        })
    ];

    // Note: report line is appended separately by caller if needed
    
    // Append version line as last log event in serverless mode (after agent payload and report line)
    if !config.new_relic.apm_lambda_mode {
        if let Some(version_info) = version_info {
            let version_line = version_info.format_version_line(request_id);
            debug!("Serverless mode - appending version line to agent payload: {}", version_line);
            
            log_events.push(serde_json::json!({
                "id": request_id,
                "message": version_line,
                "timestamp": timestamp
            }));
        }
    }

    let log_events_payload = serde_json::json!({
        "logEvents": log_events,
        "logGroup": log_group_name,
        "logStream": "",
        "messageType": "",
        "owner": ""
    });

    let log_events_string = log_events_payload.to_string();

    let mut context = serde_json::json!({
        "function_name": function_name,
        "invoked_function_arn": invoked_function_arn,
        "log_group_name": log_group_name,
        "log_stream_name": format!("{}:{}", EXTENSION_NAME, EXTENSION_VERSION)
    });

    if config.new_relic.add_version_detail_tags {
        let version_info = version::VersionInfo::get_or_detect(config.new_relic.layer_version.clone());
        let version_tags = version_info.as_tags();

        if let Some(context_obj) = context.as_object_mut() {
            for (key, value) in version_tags {
                context_obj.insert(key, serde_json::json!(value));
            }
            debug!(
                "Added {} version detail tags to agent payload context",
                context_obj.len() - 4
            );
        }
    }

    let final_payload = serde_json::json!({
        "context": context,
        "entry": log_events_string
    });

    final_payload.to_string()
}

