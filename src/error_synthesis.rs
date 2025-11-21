//! Error synthesis for Lambda timeout and platform faults
//!
//! This module synthesizes error messages for Lambda errors (timeout, platform faults, etc.)
//! and sends them to the New Relic telemetry endpoint (Vortex) similar to the Go extension.

use std::sync::Arc;
use tracing::{debug, error, info};
use crate::{
    config::ExtensionConfig,
    newrelic::client::NewRelicClient,
    EXTENSION_VERSION,
};

/// Synthesize and send timeout error to telemetry endpoint
pub async fn send_timeout_error(
    request_id: &str,
    invoked_function_arn: &str,
    timeout_seconds: Option<f64>,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<ExtensionConfig>,
) {
    let timeout_msg = if let Some(secs) = timeout_seconds {
        format!(
            "{} {} Task timed out after {:.2} seconds",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
            request_id,
            secs
        )
    } else {
        format!(
            "{} {} Task timed out",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
            request_id
        )
    };

    info!("Synthesizing timeout error for request {}: {}", request_id, timeout_msg);

    send_error_to_telemetry(
        &timeout_msg,
        request_id,
        invoked_function_arn,
        "LambdaTimeout",
        newrelic_client,
        config,
    )
    .await;
}

/// Synthesize and send platform fault error to telemetry endpoint
pub async fn send_platform_fault_error(
    request_id: &str,
    invoked_function_arn: &str,
    shutdown_reason: &str,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<ExtensionConfig>,
) {
    let fault_msg = format!(
        "RequestId: {} AWS Lambda platform fault caused a shutdown (reason: {})",
        request_id,
        shutdown_reason
    );

    info!("Synthesizing platform fault error for request {}: {}", request_id, fault_msg);

    send_error_to_telemetry(
        &fault_msg,
        request_id,
        invoked_function_arn,
        "LambdaPlatformFault",
        newrelic_client,
        config,
    )
    .await;
}

/// Synthesize and send generic Lambda error to telemetry endpoint
pub async fn send_lambda_error(
    error_message: &str,
    request_id: &str,
    invoked_function_arn: &str,
    error_type: &str,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<ExtensionConfig>,
) {
    info!("Synthesizing Lambda error for request {}: {} - {}", request_id, error_type, error_message);

    send_error_to_telemetry(
        error_message,
        request_id,
        invoked_function_arn,
        error_type,
        newrelic_client,
        config,
    )
    .await;
}

/// Core function to send error message to telemetry endpoint (Vortex/cloud-collector)
async fn send_error_to_telemetry(
    error_message: &str,
    request_id: &str,
    invoked_function_arn: &str,
    error_class: &str,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<ExtensionConfig>,
) {
    let timestamp = chrono::Utc::now().timestamp_millis();

    let log_event = serde_json::json!({
        "id": request_id,
        "message": error_message,
        "timestamp": timestamp,
    });

    let entry = serde_json::json!({
        "logEvents": [log_event],
        "logGroup": format!("/aws/lambda/{}", config.aws.function_name),
        "logStream": format!("newrelic-lambda-extension:{}", EXTENSION_VERSION),
        "messageType": "platform.error",
        "owner": "",
    });

    let payload = serde_json::json!({
        "context": {
            "function_name": config.aws.function_name,
            "invoked_function_arn": invoked_function_arn,
            "log_group_name": format!("/aws/lambda/{}", config.aws.function_name),
            "log_stream_name": format!("newrelic-lambda-extension:{}", EXTENSION_VERSION),
        },
        "entry": entry.to_string(),
    });

    let payload_json = payload.to_string();

    info!("Sending synthesized error ({}) to telemetry endpoint for request {}", error_class, request_id);

    match newrelic_client.send_agent_payload(config, &payload_json).await {
        Ok(()) => {
            debug!("Successfully sent synthesized error to telemetry endpoint for request: {}", request_id);
        }
        Err(e) => {
            error!("Failed to send synthesized error to telemetry endpoint for {}: {}", request_id, e);
        }
    }
}
