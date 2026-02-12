//! Agent payload batching logic
//!
//! This module handles batching of agent payloads for efficient sending to New Relic.
//! Batching strategies:
//! - Cold starts: Send immediately with `platform.report`
//! - Warm starts: Batch multiple payloads until threshold (3+ payloads or 5-minute timeout)
//!
//! Global state:
//! - `AGENT_BATCH_BUFFER`: Stores batched agent payloads with optional `platform.report`

use std::sync::Arc;
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

/// Global batch buffer for agent payloads with optional report lines (warm starts only)
pub static AGENT_BATCH_BUFFER: Lazy<Arc<DashMap<String, BatchedAgentPayload>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

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

    debug!("Added agent payload to batch (total buffered: {})", AGENT_BATCH_BUFFER.len());
}

/// Check if batch threshold is reached (3+ payloads WITH report lines)
/// Only sends payloads with report lines when threshold is hit
pub fn should_send_batch_by_threshold() -> bool {
    let count_with_reports = AGENT_BATCH_BUFFER
        .iter()
        .filter(|entry| entry.value().report_line.is_some())
        .count();

    if count_with_reports >= 3 {
        debug!("Batch threshold reached: {} agent payloads with report lines", count_with_reports);
        return true;
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

    items
}

/// Get only batched payloads WITH report lines WITHOUT removing them from buffer
/// Used before sending to prevent data loss if send fails
pub fn get_batch_with_reports_only() -> Vec<BatchedAgentPayload> {
    AGENT_BATCH_BUFFER
        .iter()
        .filter(|entry| entry.value().report_line.is_some())
        .map(|entry| entry.value().clone())
        .collect()
}

/// Remove successfully sent payloads from buffer
/// Only call this after successful send to prevent data loss
pub(crate) fn remove_from_buffer(items: &[BatchedAgentPayload]) {
    for item in items {
        AGENT_BATCH_BUFFER.remove(&item.request_id);
    }
    debug!(
        "Removed {} payloads from batch (remaining in buffer: {})",
        items.len(),
        AGENT_BATCH_BUFFER.len()
    );
}

/// Build the New Relic JSON payload string from a slice of batch items.
/// Shared by all send paths to avoid duplicating the envelope structure.
pub(crate) fn build_batch_payload_json(
    items: &[BatchedAgentPayload],
    config: &ExtensionConfig,
    log_stream: &str,
    version_info: Option<&crate::version::VersionInfo>,
) -> String {
    let events_per_item = 2 + usize::from(version_info.is_some());
    let mut log_events = Vec::with_capacity(items.len() * events_per_item);

    for item in items {
        let agent_str = String::from_utf8_lossy(&item.agent_payload_bytes);
        log_events.push(serde_json::json!({
            "id": &item.request_id,
            "message": &*agent_str,
            "timestamp": item.timestamp.timestamp_millis(),
        }));

        if let Some(ref report) = item.report_line {
            log_events.push(serde_json::json!({
                "id": item.request_id,
                "message": report,
                "timestamp": item.timestamp.timestamp_millis(),
            }));
        }

        if let Some(vi) = version_info {
            let version_line = vi.format_version_line(&item.request_id);
            log_events.push(serde_json::json!({
                "id": item.request_id,
                "message": version_line,
                "timestamp": item.timestamp.timestamp_millis(),
            }));
        }
    }

    let most_recent = items.last().expect("items should not be empty");

    let entry = serde_json::json!({
        "logEvents": log_events,
        "logGroup": format!("/aws/lambda/{}", config.aws.function_name),
        "logStream": log_stream,
        "messageType": "",
        "owner": "",
    });

    serde_json::json!({
        "context": {
            "function_name": config.aws.function_name,
            "invoked_function_arn": most_recent.invoked_function_arn,
            "log_group_name": format!("/aws/lambda/{}", config.aws.function_name),
            "log_stream_name": format!("newrelic-lambda-extension:{}", EXTENSION_VERSION),
        },
        "entry": entry.to_string(),
    })
    .to_string()
}

/// Send only batched agent payloads WITH report lines (when threshold is hit)
/// Payloads without report lines remain in buffer for timeout/shutdown sending
/// DATA LOSS PREVENTION: Only removes payloads from buffer AFTER successful send
pub async fn send_batched_payloads_with_reports_only(
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
) {
    let batch_items = get_batch_with_reports_only();

    if batch_items.is_empty() {
        debug!("No batched payloads with report lines to send");
        return;
    }

    debug!(
        "Threshold reached: Sending batch of {} agent payloads WITH report lines (payloads without reports kept in buffer)",
        batch_items.len()
    );

    let version_info = if !config.new_relic.apm_lambda_mode {
        Some(crate::version::VersionInfo::get_or_detect(config.new_relic.layer_version.clone()))
    } else {
        None
    };

    let payload_json = build_batch_payload_json(
        &batch_items,
        &config,
        "",
        version_info.as_deref(),
    );

    match newrelic_client.send_agent_payload(&config, &payload_json).await {
        Ok(_) => {
            info!("Successfully sent batch of {} payloads with report lines", batch_items.len());
            remove_from_buffer(&batch_items);
        }
        Err(e) => {
            error!(
                "Failed to send batched payloads with reports after all retries: {} - Keeping {} payloads in buffer for next attempt",
                e,
                batch_items.len()
            );
        }
    }
}

/// Send all pending payloads on shutdown with 1MB chunking
/// Collects from: AGENT_BATCH_BUFFER, REQUEST_AGENT_BUFFERS, and matches with PENDING_REPORTS
/// Splits into 1MB chunks while keeping each payload + report together
pub async fn send_all_pending_payloads_on_shutdown(
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
) {
    use crate::request::{REQUEST_AGENT_BUFFERS, REQUEST_CONTEXTS, PENDING_REPORTS};

    debug!("Shutdown: Collecting all pending telemetry payloads");

    let mut all_payloads: Vec<BatchedAgentPayload> = Vec::new();

    // 1. Collect from AGENT_BATCH_BUFFER (already batched payloads)
    let batched_items = get_and_clear_batch();
    debug!("Shutdown: Found {} payloads in batch buffer", batched_items.len());
    all_payloads.extend(batched_items);

    // 2. Collect from REQUEST_AGENT_BUFFERS (late/unbatched payloads)
    let all_buffer_requests: Vec<String> = REQUEST_AGENT_BUFFERS
        .iter()
        .map(|entry| entry.key().clone())
        .collect();

    for request_id in all_buffer_requests {
        if let Some(buffer) = REQUEST_AGENT_BUFFERS.get(&request_id) {
            let payloads = if let Ok(mut buf) = buffer.lock() {
                std::mem::take(&mut *buf)
            } else {
                Vec::new()
            };

            if !payloads.is_empty() {
                debug!("Shutdown: Found {} unbatched payload(s) for request: {}", payloads.len(), request_id);

                let report_line = PENDING_REPORTS.remove(&request_id).map(|(_, report)| report);

                let arn = REQUEST_CONTEXTS
                    .get(&request_id)
                    .map(|ctx_entry| {
                        ctx_entry
                            .lock()
                            .ok()
                            .map(|ctx| ctx.invoked_function_arn.clone())
                            .unwrap_or_else(|| "unknown".to_string())
                    })
                    .unwrap_or_else(|| "unknown".to_string());

                for payload_bytes in payloads {
                    all_payloads.push(BatchedAgentPayload {
                        request_id: request_id.clone(),
                        agent_payload_bytes: Arc::new(payload_bytes),
                        report_line: report_line.clone(),
                        invoked_function_arn: arn.clone(),
                        timestamp: chrono::Utc::now(),
                    });
                }
            }
        }
    }

    if all_payloads.is_empty() {
        debug!("Shutdown: No pending payloads to send");
        return;
    }

    debug!("Shutdown: Total {} payload(s) to send", all_payloads.len());

    // 3. Split into 1MB chunks while keeping each payload + report together
    const MAX_CHUNK_SIZE: usize = 1_000_000; // 1MB

    let log_stream = format!("newrelic-lambda-extension:{EXTENSION_VERSION}");
    let chunks = split_into_chunks(all_payloads, MAX_CHUNK_SIZE, &config);

    debug!("Shutdown: Sending {} chunk(s)", chunks.len());

    for (idx, chunk_items) in chunks.iter().enumerate() {
        debug!("Shutdown: Sending chunk {} with {} payload(s)", idx + 1, chunk_items.len());

        let payload_json = build_batch_payload_json(chunk_items, &config, &log_stream, None);

        if let Err(e) = newrelic_client.send_agent_payload(&config, &payload_json).await {
            error!("Shutdown: Failed to send chunk {}: {}", idx + 1, e);
        } else {
            info!("Shutdown: Successfully sent chunk {} with {} payload(s)", idx + 1, chunk_items.len());
        }
    }

    info!("Shutdown: Completed sending all pending payloads");
}

/// Split payloads into chunks of max_size, keeping each payload + report together
pub(crate) fn split_into_chunks(
    payloads: Vec<BatchedAgentPayload>,
    max_size: usize,
    config: &Arc<ExtensionConfig>,
) -> Vec<Vec<BatchedAgentPayload>> {
    let mut chunks: Vec<Vec<BatchedAgentPayload>> = Vec::new();
    let mut current_chunk: Vec<BatchedAgentPayload> = Vec::new();
    let mut current_size: usize = 0;

    // Base overhead for the JSON structure (approximate)
    let base_overhead = estimate_base_overhead(config);

    for payload_item in payloads {
        let item_size = estimate_item_size(&payload_item);

        if current_size + item_size + base_overhead > max_size && !current_chunk.is_empty() {
            debug!("Splitting chunk at {} bytes with {} items", current_size, current_chunk.len());
            chunks.push(current_chunk);
            current_chunk = Vec::new();
            current_size = 0;
        }

        current_chunk.push(payload_item);
        current_size += item_size;
    }

    if !current_chunk.is_empty() {
        chunks.push(current_chunk);
    }

    chunks
}

/// Estimate the size of a single batched payload item
pub(crate) fn estimate_item_size(item: &BatchedAgentPayload) -> usize {
    let mut size = item.agent_payload_bytes.len();

    if let Some(ref report) = item.report_line {
        size += report.len();
    }

    // JSON overhead per log event (~150 bytes per event for structure + metadata)
    size += 150;
    if item.report_line.is_some() {
        size += 150;
    }

    size
}

/// Estimate the base overhead of the JSON structure
pub(crate) fn estimate_base_overhead(config: &Arc<ExtensionConfig>) -> usize {
    let function_name_len = config.aws.function_name.len();
    500 + (function_name_len * 3)
}

/// Cleanup old entries from AGENT_BATCH_BUFFER by sending them to New Relic first
/// Finds entries older than 5 minutes, sends them (even without report lines), then removes them
/// DATA LOSS PREVENTION: Only removes entries from buffer AFTER successful send
pub async fn cleanup_old_batch_entries(
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
) {
    let now = chrono::Utc::now();
    let threshold = chrono::Duration::minutes(5);

    let old_entries: Vec<BatchedAgentPayload> = AGENT_BATCH_BUFFER
        .iter()
        .filter(|entry| now.signed_duration_since(entry.value().timestamp) >= threshold)
        .map(|entry| entry.value().clone())
        .collect();

    if old_entries.is_empty() {
        return;
    }

    debug!("Periodic cleanup: Found {} old batch entries to send and remove", old_entries.len());

    let log_stream = format!("newrelic-lambda-extension:{EXTENSION_VERSION}");
    let payload_json = build_batch_payload_json(&old_entries, &config, &log_stream, None);

    match newrelic_client.send_agent_payload(&config, &payload_json).await {
        Ok(_) => {
            info!("Periodic cleanup: Successfully sent {} old batch entries", old_entries.len());
            remove_from_buffer(&old_entries);
        }
        Err(e) => {
            error!(
                "Periodic cleanup: Failed to send {} old batch entries: {} - keeping in buffer for next attempt",
                old_entries.len(), e
            );
        }
    }
}
