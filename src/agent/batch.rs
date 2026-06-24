// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

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
use tracing::{debug, error, info, warn};

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
        debug!("Added agent payload to batch (total buffered: {})", meta.agent_count);
    }
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

    if let Ok(mut meta) = BATCH_META.lock() {
        meta.agent_count = 0;
        meta.oldest_timestamp = None;
    }

    items
}

/// Get only batched payloads WITH report lines WITHOUT removing them from buffer
/// Used before sending to prevent data loss if send fails
pub fn get_batch_with_reports_only() -> Vec<BatchedAgentPayload> {
    let items_with_reports: Vec<BatchedAgentPayload> = AGENT_BATCH_BUFFER
        .iter()
        .filter(|entry| entry.value().report_line.is_some())
        .map(|entry| entry.value().clone())
        .collect();

    items_with_reports
}

/// Remove successfully sent payloads from buffer and update metadata
/// Only call this after successful send to prevent data loss
fn clear_batch_with_reports(items: &[BatchedAgentPayload]) {
    // Remove only items with report lines from buffer
    for item in items {
        AGENT_BATCH_BUFFER.remove(&item.request_id);
    }

    // Update metadata to reflect remaining items
    if let Ok(mut meta) = BATCH_META.lock() {
        let remaining_count = AGENT_BATCH_BUFFER.len();
        meta.agent_count = remaining_count;

        // Update oldest timestamp to the oldest remaining item
        if remaining_count == 0 {
            meta.oldest_timestamp = None;
        } else {
            meta.oldest_timestamp = AGENT_BATCH_BUFFER
                .iter()
                .map(|entry| entry.value().timestamp)
                .min();
        }

        debug!(
            "Removed {} payloads with report lines from batch (remaining in buffer: {})",
            items.len(),
            remaining_count
        );
    }
}

/// Send only batched agent payloads WITH report lines (when threshold is hit)
/// Payloads without report lines remain in buffer for timeout/shutdown sending
/// DATA LOSS PREVENTION: Only removes payloads from buffer AFTER successful send
pub async fn send_batched_payloads_with_reports_only(
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
) {
    // Get items WITHOUT removing them from buffer (prevent data loss on send failure)
    let batch_items = get_batch_with_reports_only();

    if batch_items.is_empty() {
        debug!("No batched payloads with report lines to send");
        return;
    }

    debug!(
        "Threshold reached: Sending batch of {} agent payloads WITH report lines (payloads without reports kept in buffer)",
        batch_items.len()
    );

    // Get version info once for appending to all payloads (serverless mode only)
    let version_info = if !config.new_relic.apm_lambda_mode {
        Some(crate::version::VersionInfo::get_or_detect(config.new_relic.layer_version.clone()))
    } else {
        None
    };

    // Pre-allocate capacity: each item needs 2-3 log events (agent + report + optional version)
    let mut log_events = Vec::with_capacity(batch_items.len() * 3);

    for item in &batch_items {
        // Avoid unnecessary string clones - use Cow to only allocate on invalid UTF-8
        let agent_str = String::from_utf8_lossy(&item.agent_payload_bytes);
        log_events.push(serde_json::json!({
            "id": &item.request_id,
            "message": &*agent_str,
            "timestamp": item.timestamp.timestamp_millis(),
        }));

        // All items in this batch have report lines (filtered) - append as second log event
        if let Some(ref report) = item.report_line {
            log_events.push(serde_json::json!({
                "id": item.request_id,
                "message": report,
                "timestamp": item.timestamp.timestamp_millis(),
            }));
        }

        // Append version line in serverless mode (third log event)
        if let Some(ref version_info) = version_info {
            let version_line = version_info.format_version_line(&item.request_id);
            log_events.push(serde_json::json!({
                "id": item.request_id,
                "message": version_line,
                "timestamp": item.timestamp.timestamp_millis(),
            }));
        }
    }

    let most_recent = batch_items.last().expect("batch_items should not be empty");

    let entry = serde_json::json!({
        "logEvents": log_events,
        "logGroup": format!("/aws/lambda/{}", config.aws.function_name),
        "logStream": "",
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

    // Use to_string() for final serialization (send_agent_payload expects &str)
    let payload_json = payload.to_string();

    // Send to New Relic with retries
    match newrelic_client.send_agent_payload(&config, &payload_json).await {
        Ok(_) => {
            info!("Successfully sent batch of {} payloads with report lines", batch_items.len());
            // Only remove from buffer AFTER successful send (prevent data loss)
            clear_batch_with_reports(&batch_items);
        }
        Err(e) => {
            warn!(
                "Failed to send batched payloads with reports after all retries: {} - Keeping {} payloads in buffer for next attempt",
                e,
                batch_items.len()
            );
            // Items remain in buffer - will be retried on next batch send or at shutdown
        }
    }
}

/// Send all pending payloads on shutdown with 1MB chunking
/// Collects from: AGENT_BATCH_BUFFER, REQUEST_DATA (agent buffers + pending reports)
/// Splits into 1MB chunks while keeping each payload + report together
pub async fn send_all_pending_payloads_on_shutdown(
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
    log_processor: Option<&Arc<crate::logs::processor::LogProcessor>>,
) {
    use crate::request::{REQUEST_DATA, get_request_context, remove_pending_report};

    debug!("Shutdown: Collecting all pending telemetry payloads");

    let mut all_payloads: Vec<BatchedAgentPayload> = Vec::new();

    // 1. Collect from AGENT_BATCH_BUFFER (already batched payloads)
    let batched_items = get_and_clear_batch();
    debug!("Shutdown: Found {} payloads in batch buffer", batched_items.len());
    all_payloads.extend(batched_items);

    // 2. Collect from REQUEST_DATA (late/unbatched payloads)
    let all_buffer_requests: Vec<String> = REQUEST_DATA
        .iter()
        .map(|entry| entry.key().clone())
        .collect();

    for request_id in all_buffer_requests {
        if let Some(buffer) = crate::request::get_agent_buffer(&request_id) {
            let payloads = if let Ok(mut buf) = buffer.lock() {
                std::mem::take(&mut *buf)
            } else {
                Vec::new()
            };

            if !payloads.is_empty() {
                debug!("Shutdown: Found {} unbatched payload(s) for request: {}", payloads.len(), request_id);

                // Get report line if available
                let report_line = remove_pending_report(&request_id);

                // Get context — cascade: per-request context → global registration ARN
                let arn = get_request_context(&request_id)
                    .and_then(|ctx_entry| {
                        ctx_entry
                            .lock()
                            .ok()
                            .map(|ctx| ctx.invoked_function_arn.clone())
                            .filter(|arn| !arn.is_empty())
                    })
                    .unwrap_or_else(crate::get_global_fallback_arn);

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

    // Last chance to stamp trace.id: a request whose payload never paired before
    // shutdown still has logs held in pending_logs. Extract its trace here so those
    // logs are stamped + flushed (by flush_pending_logs_unstamped after this) rather
    // than shipped untagged. Read-only on the payload; no effect on the send below.
    if let Some(lp) = log_processor {
        if config.new_relic.collect_trace_id {
            for item in &all_payloads {
                if let Ok(Some(trace_id)) =
                    crate::trace::extract_trace_id_from_payload(&item.agent_payload_bytes)
                {
                    let _ = lp
                        .on_trace_id_extracted(&item.request_id, &trace_id)
                        .await;
                }
            }
        }
    }

    // 3. Split into 1MB chunks while keeping each payload + report together
    const MAX_CHUNK_SIZE: usize = 1_000_000; // 1MB

    let chunks = split_into_chunks(all_payloads, MAX_CHUNK_SIZE, &config);

    debug!("Shutdown: Sending {} chunk(s)", chunks.len());

    // 4. Send each chunk
    for (idx, chunk_items) in chunks.iter().enumerate() {
        debug!("Shutdown: Sending chunk {} with {} payload(s)", idx + 1, chunk_items.len());

        // Pre-allocate capacity: each item needs 1-2 log events (agent + optional report)
        let mut log_events = Vec::with_capacity(chunk_items.len() * 2);

        for item in chunk_items {
            // Avoid unnecessary string clones - use Cow to only allocate on invalid UTF-8
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
        }

        let most_recent = chunk_items.last().expect("chunk should not be empty");

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
        // Estimate size of this item (agent payload + optional report + JSON overhead)
        let item_size = estimate_item_size(&payload_item);

        // Check if adding this item would exceed max_size
        if current_size + item_size + base_overhead > max_size && !current_chunk.is_empty() {
            // Start a new chunk
            debug!("Splitting chunk at {} bytes with {} items", current_size, current_chunk.len());
            chunks.push(current_chunk);
            current_chunk = Vec::new();
            current_size = 0;
        }

        // Add item to current chunk
        current_chunk.push(payload_item);
        current_size += item_size;
    }

    // Add the last chunk if not empty
    if !current_chunk.is_empty() {
        chunks.push(current_chunk);
    }

    chunks
}

/// Estimate the size of a single batched payload item
pub(crate) fn estimate_item_size(item: &BatchedAgentPayload) -> usize {
    let mut size = 0;

    // Agent payload size
    size += item.agent_payload_bytes.len();

    // Report line size (if present)
    if let Some(ref report) = item.report_line {
        size += report.len();
    }

    // JSON overhead per log event (~150 bytes per event for structure + metadata)
    size += 150;
    if item.report_line.is_some() {
        size += 150; // Second log event for report
    }

    size
}

/// Estimate the base overhead of the JSON structure
pub(crate) fn estimate_base_overhead(config: &Arc<ExtensionConfig>) -> usize {
    // Rough estimate: context object + entry wrapper + logEvents array
    let function_name_len = config.aws.function_name.len();
    let base = 500 + (function_name_len * 3); // Function name appears in multiple places
    base
}

/// Cleanup old entries from AGENT_BATCH_BUFFER by sending them to New Relic first
/// Finds entries older than 5 minutes, sends them (even without report lines), then removes them
pub async fn cleanup_old_batch_entries(
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
) {
    let now = chrono::Utc::now();
    let threshold = chrono::Duration::minutes(5);

    // Collect old entries that need to be sent
    let old_entries: Vec<BatchedAgentPayload> = AGENT_BATCH_BUFFER
        .iter()
        .filter(|entry| now.signed_duration_since(entry.value().timestamp) >= threshold)
        .map(|entry| entry.value().clone())
        .collect();

    if old_entries.is_empty() {
        return;
    }

    debug!("Periodic cleanup: Found {} old batch entries to send and remove", old_entries.len());

    // Send the old entries to New Relic (even without report lines - don't lose telemetry!)
    // Pre-allocate capacity: each item needs 1-2 log events (agent + optional report)
    let mut log_events = Vec::with_capacity(old_entries.len() * 2);

    for item in &old_entries {
        // Avoid unnecessary string clones - use Cow to only allocate on invalid UTF-8
        let agent_str = String::from_utf8_lossy(&item.agent_payload_bytes);
        log_events.push(serde_json::json!({
            "id": &item.request_id,
            "message": &*agent_str,
            "timestamp": item.timestamp.timestamp_millis(),
        }));

        // Include report line if available
        if let Some(ref report) = item.report_line {
            log_events.push(serde_json::json!({
                "id": item.request_id,
                "message": report,
                "timestamp": item.timestamp.timestamp_millis(),
            }));
        }
    }

    let most_recent = old_entries.last().expect("old_entries should not be empty");

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

    // Send to New Relic before removing
    if let Err(e) = newrelic_client.send_agent_payload(&config, &payload_json).await {
        error!("Periodic cleanup: Failed to send old batch entries: {}", e);
    } else {
        info!("Periodic cleanup: Successfully sent {} old batch entries", old_entries.len());
    }

    // Now remove the old entries from buffer
    for item in &old_entries {
        AGENT_BATCH_BUFFER.remove(&item.request_id);
    }

    // Update metadata
    if let Ok(mut meta) = BATCH_META.lock() {
        let final_count = AGENT_BATCH_BUFFER.len();
        meta.agent_count = final_count;
        meta.oldest_timestamp = if final_count == 0 {
            None
        } else {
            AGENT_BATCH_BUFFER.iter().map(|entry| entry.value().timestamp).min()
        };
        debug!("Periodic cleanup: Removed {} old entries (remaining in buffer: {})", old_entries.len(), final_count);
    }
}
