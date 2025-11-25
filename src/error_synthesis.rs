//! Error synthesis for Lambda timeout and platform faults
//!
//! This module synthesizes error messages for Lambda errors (timeout, platform faults, etc.)
//! and sends them to the New Relic telemetry endpoint (Vortex) similar to the Go extension.

use once_cell::sync::Lazy;
use std::sync::{Arc, Mutex};
use tracing::{debug, error, info};
use crate::{
    config::ExtensionConfig,
    newrelic::client::NewRelicClient,
    EXTENSION_VERSION,
};

/// Store platform metrics for error synthesis (from platform.report events)
#[derive(Debug, Clone)]
pub struct PlatformMetrics {
    pub request_id: String,
    pub duration_ms: Option<f64>,
    pub memory_size_mb: Option<u64>,
    pub max_memory_used_mb: Option<u64>,
}

/// Global storage for last platform metrics (for timeout error synthesis)
pub static LAST_PLATFORM_METRICS: Lazy<Arc<Mutex<Option<PlatformMetrics>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

/// Track sent errors to avoid duplicates (request_id, error_type)
/// Using error_type instead of full message to prevent duplicate errors from different sources
pub static SENT_ERRORS: Lazy<Arc<Mutex<std::collections::HashSet<(String, String)>>>> =
    Lazy::new(|| Arc::new(Mutex::new(std::collections::HashSet::new())));

/// Store failed error sends for retry on next invoke
/// Structure: (request_id, error_type, error_message, function_arn, error_class)
#[derive(Debug, Clone)]
pub struct FailedError {
    pub request_id: String,
    pub error_type: String,
    pub error_message: String,
    pub invoked_function_arn: String,
    pub error_class: String,
}

pub static FAILED_ERRORS: Lazy<Arc<Mutex<Vec<FailedError>>>> =
    Lazy::new(|| Arc::new(Mutex::new(Vec::new())));

/// Store the last detected error from function logs for more detailed platform fault messages
#[derive(Debug, Clone)]
pub struct LastDetectedError {
    pub request_id: String,
    pub error_message: String,
    pub error_type: String,
}

pub static LAST_DETECTED_ERROR: Lazy<Arc<Mutex<Option<LastDetectedError>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

/// Retry any failed errors from previous invocation before starting new one
/// Returns true if any retries were attempted
pub async fn retry_failed_errors(
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<ExtensionConfig>,
) -> bool {
    let failed_errors = if let Ok(mut guard) = FAILED_ERRORS.lock() {
        if guard.is_empty() {
            return false;
        }
        let errors = guard.clone();
        guard.clear(); // Clear immediately to avoid duplicate retries
        errors
    } else {
        return false;
    };

    if failed_errors.is_empty() {
        return false;
    }

    info!("Retrying {} failed error(s) from previous invocation(s)", failed_errors.len());

    for failed_error in failed_errors {
        debug!(
            "Retrying error send: {} for request {}",
            failed_error.error_class, failed_error.request_id
        );

        // Don't check SENT_ERRORS here - these are retries
        // Send directly to telemetry
        match send_error_to_telemetry_internal(
            &failed_error.error_message,
            &failed_error.request_id,
            &failed_error.invoked_function_arn,
            &failed_error.error_class,
            newrelic_client,
            config,
        )
        .await
        {
            Ok(()) => {
                debug!(
                    "Successfully retried error {} for request {}",
                    failed_error.error_class, failed_error.request_id
                );
                // Mark as sent on success
                if let Ok(mut sent_errors) = SENT_ERRORS.lock() {
                    sent_errors.insert((failed_error.request_id.clone(), failed_error.error_type.clone()));
                }
            }
            Err(e) => {
                error!(
                    "Retry failed for error {} (request {}): {} - will not retry again",
                    failed_error.error_class, failed_error.request_id, e
                );
                // Don't retry again - drop the error after one retry attempt
            }
        }
    }

    true
}

/// Clear all sent errors (call when starting new invocation)
/// This ensures each new invocation starts with a clean slate and prevents memory leaks
pub fn clear_sent_errors_for_request(request_id: &str) {
    if let Ok(mut sent_errors) = SENT_ERRORS.lock() {
        let prev_count = sent_errors.len();
        sent_errors.clear();
        if prev_count > 0 {
            debug!("Cleared {} sent error(s) for new invocation (request: {})", prev_count, request_id);
        }
    }

    // Also clear the last detected error for new invocation
    if let Ok(mut last_error) = LAST_DETECTED_ERROR.lock() {
        if last_error.is_some() {
            debug!("Cleared last detected error for new invocation (request: {})", request_id);
            *last_error = None;
        }
    }
}

/// Store platform metrics from platform.report event
pub fn store_platform_metrics(
    request_id: String,
    duration_ms: Option<f64>,
    memory_size_mb: Option<u64>,
    max_memory_used_mb: Option<u64>,
) {
    if let Ok(mut guard) = LAST_PLATFORM_METRICS.lock() {
        *guard = Some(PlatformMetrics {
            request_id,
            duration_ms,
            memory_size_mb,
            max_memory_used_mb,
        });
        debug!("Stored platform metrics for error synthesis");
    }
}

/// Synthesize and send timeout error to telemetry endpoint
/// Uses platform metrics if available, otherwise calculates from timeout_seconds
pub async fn send_timeout_error(
    request_id: &str,
    invoked_function_arn: &str,
    timeout_seconds: Option<f64>,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<ExtensionConfig>,
) {
    // Try to get actual duration from platform metrics first
    let actual_duration = if let Ok(guard) = LAST_PLATFORM_METRICS.lock() {
        if let Some(ref metrics) = *guard {
            if metrics.request_id == request_id {
                metrics.duration_ms.map(|ms| ms / 1000.0) // Convert ms to seconds
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    // Compose the error message using actual_duration or timeout_seconds
    let timeout_msg = if let Some(actual_secs) = actual_duration {
        // Use actual duration from platform.report
        format!(
            "{} {} Task timed out after {:.2} seconds",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
            request_id,
            actual_secs
        )
    } else if let Some(secs) = timeout_seconds {
        // Use provided timeout value
        format!(
            "{} {} Task timed out after {:.2} seconds",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
            request_id,
            secs
        )
    } else {
        // No timing information available
        format!(
            "{} {} Task timed out",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
            request_id
        )
    };

    // Check if we already sent a timeout error for this request (by error type, not exact message)
    if let Ok(sent_errors) = SENT_ERRORS.lock() {
        let key = (request_id.to_string(), "LambdaTimeout".to_string());
        if sent_errors.contains(&key) {
            debug!("Already sent timeout error for request {} (type dedup), skipping duplicate", request_id);
            return;
        }
    }
    info!("Synthesizing timeout error for request {}: {}", request_id, timeout_msg);

    // Attempt to send - only mark as sent on success
    match send_error_to_telemetry_internal(
        &timeout_msg,
        request_id,
        invoked_function_arn,
        "LambdaTimeout",
        newrelic_client,
        config,
    )
    .await
    {
        Ok(()) => {
            // Mark as successfully sent
            if let Ok(mut sent_errors) = SENT_ERRORS.lock() {
                sent_errors.insert((request_id.to_string(), "LambdaTimeout".to_string()));
            }
        }
        Err(e) => {
            error!("Failed to send timeout error for {}: {} - will retry on next invoke", request_id, e);
            // Store for retry on next invocation
            if let Ok(mut failed_errors) = FAILED_ERRORS.lock() {
                failed_errors.push(FailedError {
                    request_id: request_id.to_string(),
                    error_type: "LambdaTimeout".to_string(),
                    error_message: timeout_msg,
                    invoked_function_arn: invoked_function_arn.to_string(),
                    error_class: "LambdaTimeout".to_string(),
                });
            }
        }
    }
}

/// Synthesize and send platform fault error to telemetry endpoint
/// Includes memory information if available from platform metrics
pub async fn send_platform_fault_error(
    request_id: &str,
    invoked_function_arn: &str,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<ExtensionConfig>,
) {
    // Try to get memory info from platform metrics (useful for OOM faults)
    let memory_info = if let Ok(guard) = LAST_PLATFORM_METRICS.lock() {
        if let Some(ref metrics) = *guard {
            if metrics.request_id == request_id {
                match (metrics.max_memory_used_mb, metrics.memory_size_mb) {
                    (Some(used), Some(size)) => {
                        format!(" (Memory: {} MB used / {} MB limit)", used, size)
                    }
                    _ => String::new(),
                }
            } else {
                String::new()
            }
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    // Compose the fault message with memory info
    let fault_msg = format!(
        "RequestId: {} AWS Lambda platform fault caused a shutdown{}",
        request_id,
        memory_info
    );

    // Check if we already sent a platform fault error for this request (by error type, not exact message)
    if let Ok(sent_errors) = SENT_ERRORS.lock() {
        let key = (request_id.to_string(), "LambdaPlatformFault".to_string());
        if sent_errors.contains(&key) {
            debug!("Already sent platform fault error for request {} (type dedup), skipping duplicate", request_id);
            return;
        }
    }

    info!("Synthesizing platform fault error for request {}: {}", request_id, fault_msg);

    // Attempt to send - only mark as sent on success
    match send_error_to_telemetry_internal(
        &fault_msg,
        request_id,
        invoked_function_arn,
        "LambdaPlatformFault",
        newrelic_client,
        config,
    )
    .await
    {
        Ok(()) => {
            // Mark as successfully sent
            if let Ok(mut sent_errors) = SENT_ERRORS.lock() {
                sent_errors.insert((request_id.to_string(), "LambdaPlatformFault".to_string()));
            }
        }
        Err(e) => {
            error!("Failed to send platform fault error for {}: {} - will retry on next invoke", request_id, e);
            // Store for retry on next invocation
            if let Ok(mut failed_errors) = FAILED_ERRORS.lock() {
                failed_errors.push(FailedError {
                    request_id: request_id.to_string(),
                    error_type: "LambdaPlatformFault".to_string(),
                    error_message: fault_msg,
                    invoked_function_arn: invoked_function_arn.to_string(),
                    error_class: "LambdaPlatformFault".to_string(),
                });
            }
        }
    }
}

/// Synthesize and send generic Lambda error to telemetry endpoint
/// Note: error_message should already be in CloudWatch format (timestamp request-id message)
pub async fn send_lambda_error(
    error_message: &str,
    request_id: &str,
    invoked_function_arn: &str,
    error_type: &str,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<ExtensionConfig>,
) {
    // Check if we already sent this error type for this request (by error type, not exact message)
    if let Ok(sent_errors) = SENT_ERRORS.lock() {
        let key = (request_id.to_string(), error_type.to_string());
        if sent_errors.contains(&key) {
            debug!("Already sent {} error for request {} (type dedup), skipping duplicate", error_type, request_id);
            return;
        }
    }

    info!("Synthesizing Lambda error for request {}: {} - {}", request_id, error_type, error_message);

    // Attempt to send - only mark as sent on success
    match send_error_to_telemetry_internal(
        error_message,
        request_id,
        invoked_function_arn,
        error_type,
        newrelic_client,
        config,
    )
    .await
    {
        Ok(()) => {
            // Mark as successfully sent
            if let Ok(mut sent_errors) = SENT_ERRORS.lock() {
                sent_errors.insert((request_id.to_string(), error_type.to_string()));
            }
        }
        Err(e) => {
            error!("Failed to send Lambda error for {}: {} - will retry on next invoke", request_id, e);
            // Store for retry on next invocation
            if let Ok(mut failed_errors) = FAILED_ERRORS.lock() {
                failed_errors.push(FailedError {
                    request_id: request_id.to_string(),
                    error_type: error_type.to_string(),
                    error_message: error_message.to_string(),
                    invoked_function_arn: invoked_function_arn.to_string(),
                    error_class: error_type.to_string(),
                });
            }
        }
    }
}

/// Internal function to send error message to telemetry endpoint (Vortex/cloud-collector)
/// Returns Result to allow caller to handle success/failure
async fn send_error_to_telemetry_internal(
    error_message: &str,
    request_id: &str,
    invoked_function_arn: &str,
    error_class: &str,
    newrelic_client: &Arc<NewRelicClient>,
    config: &Arc<ExtensionConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

    newrelic_client.send_agent_payload(config, &payload_json).await?;

    debug!("Successfully sent synthesized error to telemetry endpoint for request: {}", request_id);
    Ok(())
}
