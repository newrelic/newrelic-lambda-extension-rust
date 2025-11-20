//! Agent payload batching logic
//!
//! This module handles batching of agent payloads for efficient sending to New Relic.
//! Batching strategies:
//! - Cold starts: Send immediately with `platform.report`
//! - Warm starts: Batch multiple payloads until threshold (3+ payloads or 5-minute timeout)
//!
//! Global state:
//! - `AGENT_BATCH_BUFFER`: Stores batched agent payloads with optional `platform.report`
//! - `BATCH_META`: Tracks batch metadata (count, oldest timestamp)

use std::sync::{Arc, Mutex};
use once_cell::sync::Lazy;
use dashmap::DashMap;
use tracing::{debug, error, info};

use crate::{
    config::ExtensionConfig,
    newrelic::client::NewRelicClient,
    EXTENSION_VERSION,
};

/// Batched agent payload with optional platform.report line
#[derive(Debug, Clone)]
pub struct BatchedAgentPayload {
    pub request_id: String,
    pub agent_payload_bytes: Arc<Vec<u8>>,
    pub report_line: Option<String>,
    pub invoked_function_arn: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Batch metadata for tracking thresholds
#[derive(Debug)]
pub struct BatchMetadata {
    pub agent_count: usize,
    pub oldest_timestamp: Option<chrono::DateTime<chrono::Utc>>,
}

/// Global batch buffer for agent payloads with optional report lines (warm starts only)
pub static AGENT_BATCH_BUFFER: Lazy<Arc<DashMap<String, BatchedAgentPayload>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

/// Global batch metadata for tracking thresholds
pub static BATCH_META: Lazy<Arc<Mutex<BatchMetadata>>> =
    Lazy::new(|| Arc::new(Mutex::new(BatchMetadata {
        agent_count: 0,
        oldest_timestamp: None,
    })));

/// Add agent payload to batch buffer
pub fn add_to_batch(
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

    if let Ok(mut meta) = BATCH_META.lock() {
        meta.agent_count += 1;
        if meta.oldest_timestamp.is_none() {
            meta.oldest_timestamp = Some(timestamp);
        }
        info!("Added agent payload to batch (total buffered: {})", meta.agent_count);
    }
}

/// Check if batch should be sent based on thresholds
///
/// Thresholds:
/// - 3+ agent payloads
/// - Oldest payload > 5 minutes
pub fn should_send_batch() -> bool {
    if let Ok(meta) = BATCH_META.lock() {
        if meta.agent_count >= 3 {
            debug!("Batch threshold reached: {} agents", meta.agent_count);
            return true;
        }

        if let Some(oldest) = meta.oldest_timestamp {
            let age = chrono::Utc::now() - oldest;
            if age > chrono::Duration::seconds(300) {
                debug!("Batch timeout reached: oldest payload is {:?} old", age);
                return true;
            }
        }
    }

    false
}

/// Get all batched payloads and clear the buffer
pub fn get_and_clear_batch() -> Vec<BatchedAgentPayload> {
    let items: Vec<BatchedAgentPayload> = AGENT_BATCH_BUFFER
        .iter()
        .map(|entry| entry.value().clone())
        .collect();

    AGENT_BATCH_BUFFER.clear();

    if let Ok(mut meta) = BATCH_META.lock() {
        meta.agent_count = 0;
        meta.oldest_timestamp = None;
    }

    items
}

/// Send agent payload with optional report immediately (for cold start or when both ready)
pub async fn send_agent_with_report_immediately(
    request_id: String,
    invoked_function_arn: String,
    agent_payloads: Vec<Vec<u8>>,
    report_line: Option<String>,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
    apm_app: crate::apm::SharedApmApp,
) {
    let has_report = report_line.is_some();
    debug!("Sending agent payload immediately for {} (with report: {})", request_id, has_report);

    let apm_app_guard = apm_app.read().await;
    let is_apm_mode = apm_app_guard.is_some();

    for payload_bytes in agent_payloads {
        if let Some(ref app) = *apm_app_guard {
            info!("APM mode: Sending agent payload for request: {}", request_id);
            if let Err(e) = app.process_agent_payload(payload_bytes.clone()).await {
                error!("Failed to send agent payload to APM collector for {}: {}", request_id, e);
            }
            
            if let Some(ref report) = report_line {
                debug!("APM mode: Sending platform REPORT metrics for request: {}", request_id);
                if let Err(e) = app.send_platform_report_metrics(report).await {
                    error!("Failed to send platform REPORT metrics for {}: {}", request_id, e);
                }
                
                if report.contains("Task timed out") || report.contains("error") || report.contains("Error") {
                    debug!("APM mode: Detected fault/timeout in REPORT log, generating error event");
                    if let Err(e) = app.send_error_event_from_fault(report, &request_id, &invoked_function_arn).await {
                        error!("Failed to send error event for fault in {}: {}", request_id, e);
                    }
                }
            }
        } else {
            let mut log_events = Vec::new();

            let agent_str = match std::str::from_utf8(&payload_bytes) {
                Ok(s) => s.to_string(),
                Err(_) => String::from_utf8_lossy(&payload_bytes).to_string(),
            };
            log_events.push(serde_json::json!({
                "id": request_id,
                "message": agent_str,
                "timestamp": chrono::Utc::now().timestamp_millis(),
            }));

            if let Some(ref report) = report_line {
                log_events.push(serde_json::json!({
                    "id": request_id,
                    "message": report,
                    "timestamp": chrono::Utc::now().timestamp_millis(),
                }));
            }

            let entry = serde_json::json!({
                "logEvents": log_events,
                "logGroup": format!("/aws/lambda/{}", config.aws.function_name),
                "logStream": format!("newrelic-lambda-extension:{}", EXTENSION_VERSION),
                "messageType": "",
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
pub async fn send_batched_payloads(
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
) {
    let batch_items = get_and_clear_batch();

    if batch_items.is_empty() {
        debug!("No batched payloads to send");
        return;
    }

    debug!("Sending batch of {} agent payloads", batch_items.len());

    let mut log_events = Vec::new();

    for item in &batch_items {
        let agent_str = match std::str::from_utf8(&item.agent_payload_bytes) {
            Ok(s) => s.to_string(),
            Err(_) => String::from_utf8_lossy(&item.agent_payload_bytes).to_string(),
        };
        log_events.push(serde_json::json!({
            "id": item.request_id,
            "message": agent_str,
            "timestamp": item.timestamp.timestamp_millis(),
        }));

        if let Some(ref report) = item.report_line {
            log_events.push(serde_json::json!({
                "id": item.request_id,
                "message": report,
                "timestamp": item.timestamp.timestamp_millis(),
            }));
        }
    }

    let most_recent = batch_items.last().expect("batch_items should not be empty");

    let entry = serde_json::json!({
        "logEvents": log_events,
        "logGroup": format!("/aws/lambda/{}", config.aws.function_name),
        "logStream": format!("newrelic-lambda-extension:{}", EXTENSION_VERSION),
        "messageType": "",
        "owner": "",
    });

    let payload = serde_json::json!({
        "context": {
            "function_name": config.aws.function_name,
            "invoked_function_arn": most_recent.invoked_function_arn,
            "log_group_name": format!("/aws/lambda/{}", config.aws.function_name),
            "log_stream_name": format!("newrelic-lambda-extension:{}", EXTENSION_VERSION),
        },
        "entry": entry.to_string(),
    });

    let payload_json = payload.to_string();

    if let Err(e) = newrelic_client.send_agent_payload(&config, &payload_json).await {
        error!("Failed to send batched payloads: {}", e);
    } else {
        info!("Successfully sent batch of {} payloads", batch_items.len());
    }
}
