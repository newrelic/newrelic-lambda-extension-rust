//! Agent payload batching logic
//!
//! This module handles batching of agent payloads for efficient sending to New Relic.
//! Batching strategies:
//! - Cold starts: Send immediately with `platform.report`
//! - Warm starts: Batch multiple payloads until threshold (3+ payloads or 5-minute timeout)

use std::sync::Arc;
use dashmap::DashMap;
use tracing::{debug, error, info};
use once_cell::sync::Lazy;

use crate::{
    config::ExtensionConfig,
    newrelic::client::NewRelicClient,
    EXTENSION_VERSION,
};

/// Batched agent payload with optional platform.report line
#[derive(Debug, Clone)]
pub struct BatchedAgentPayload {
    pub request_id: String,
    pub agent_payload_bytes: Arc<[u8]>,
    pub report_line: Option<String>,
    pub invoked_function_arn: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Batch buffer for agent payloads with dependency injection
/// This allows for testable code without global state
#[derive(Debug, Clone)]
pub struct BatchBuffer {
    pub buffer: Arc<DashMap<String, BatchedAgentPayload>>,
}

/// Global default batch buffer instance
/// Using Lazy for better testability - tests can use their own BatchBuffer::new()
pub static DEFAULT_BATCH_BUFFER: Lazy<Arc<BatchBuffer>> = Lazy::new(|| Arc::new(BatchBuffer::new()));

impl BatchBuffer {
    /// Create a new batch buffer instance
    pub fn new() -> Self {
        Self {
            buffer: Arc::new(DashMap::new()),
        }
    }

    /// Add agent payload to batch buffer
    pub fn add_to_batch(
        &self,
        request_id: String,
        agent_bytes: Vec<u8>,
        report_line: Option<String>,
        arn: String,
    ) {
        let timestamp = chrono::Utc::now();

        self.buffer.insert(
            request_id.clone(),
            BatchedAgentPayload {
                request_id,
                agent_payload_bytes: Arc::from(agent_bytes),
                report_line,
                invoked_function_arn: arn,
                timestamp,
            }
        );

        #[cfg(not(test))]
        debug!("Added agent payload to batch (total buffered: {})", self.buffer.len());
    }

    /// Check if batch threshold is reached (3+ payloads WITH report lines)
    /// Short-circuits after finding 3 — avoids scanning the entire buffer
    pub fn should_send_batch_by_threshold(&self) -> bool {
        let reached = self.buffer
            .iter()
            .filter(|entry| entry.value().report_line.is_some())
            .take(3) // Short-circuit: stop counting after 3
            .count() >= 3;

        if reached {
            debug!("Batch threshold reached");
        }

        reached
    }

    /// Get all batched payloads and clear the buffer
    pub fn get_and_clear_batch(&self) -> Vec<BatchedAgentPayload> {
        let items: Vec<BatchedAgentPayload> = self.buffer
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

        self.buffer.clear();

        items
    }

    /// Get only batched payloads WITH report lines WITHOUT removing them from buffer
    /// Used before sending to prevent data loss if send fails
    pub fn get_batch_with_reports_only(&self) -> Vec<BatchedAgentPayload> {
        self.buffer
            .iter()
            .filter(|entry| entry.value().report_line.is_some())
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Remove successfully sent payloads from buffer
    /// Only call this after successful send to prevent data loss
    pub fn clear_batch_with_reports(&self, items: &[BatchedAgentPayload]) {
        for item in items {
            self.buffer.remove(&item.request_id);
        }

        debug!(
            "Removed {} payloads with report lines from batch (remaining in buffer: {})",
            items.len(),
            self.buffer.len()
        );
    }

    /// Send only batched agent payloads WITH report lines (when threshold is hit)
    /// Payloads without report lines remain in buffer for timeout/shutdown sending
    /// DATA LOSS PREVENTION: Only removes payloads from buffer AFTER successful send
    pub async fn send_batched_payloads_with_reports_only(
        &self,
        newrelic_client: Arc<NewRelicClient>,
        config: Arc<ExtensionConfig>,
    ) {
        // Get items WITHOUT removing them from buffer (prevent data loss on send failure)
        let batch_items = self.get_batch_with_reports_only();

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

        let payload_json = build_newrelic_payload(
            &batch_items,
            &config,
            version_info.as_deref(),
        );

        // Send to New Relic with retries
        match newrelic_client.send_agent_payload(&config, &payload_json).await {
            Ok(_) => {
                info!("Successfully sent batch of {} payloads with report lines", batch_items.len());
                // Only remove from buffer AFTER successful send (prevent data loss)
                self.clear_batch_with_reports(&batch_items);
            }
            Err(e) => {
                error!(
                    "Failed to send batched payloads with reports after all retries: {} - Keeping {} payloads in buffer for next attempt",
                    e,
                    batch_items.len()
                );
                // Items remain in buffer - will be retried on next batch send or at shutdown
            }
        }
    }

    /// Send all pending payloads on shutdown with 1MB chunking
    /// Collects from batch buffer, REQUEST_AGENT_BUFFERS, and matches with PENDING_REPORTS
    /// Splits into 1MB chunks while keeping each payload + report together
    pub async fn send_all_pending_payloads_on_shutdown(
        &self,
        newrelic_client: Arc<NewRelicClient>,
        config: Arc<ExtensionConfig>,
    ) {
        use crate::request::{REQUEST_AGENT_BUFFERS, REQUEST_CONTEXTS, PENDING_REPORTS};

        debug!("Shutdown: Collecting all pending telemetry payloads");

        let mut all_payloads: Vec<BatchedAgentPayload> = Vec::new();

        // 1. Collect from batch buffer (already batched payloads)
        let batched_items = self.get_and_clear_batch();
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

                    // Get report line if available
                    let report_line = PENDING_REPORTS.remove(&request_id).map(|(_, report)| report);

                    // Get context
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
                            agent_payload_bytes: Arc::from(payload_bytes),
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

        let chunks = split_into_chunks(all_payloads, MAX_CHUNK_SIZE, &config);

        debug!("Shutdown: Sending {} chunk(s)", chunks.len());

        // 4. Send each chunk
        for (idx, chunk_items) in chunks.iter().enumerate() {
            debug!("Shutdown: Sending chunk {} with {} payload(s)", idx + 1, chunk_items.len());

            let payload_json = build_newrelic_payload(chunk_items, &config, None);

            if let Err(e) = newrelic_client.send_agent_payload(&config, &payload_json).await {
                error!("Shutdown: Failed to send chunk {}: {}", idx + 1, e);
            } else {
                info!("Shutdown: Successfully sent chunk {} with {} payload(s)", idx + 1, chunk_items.len());
            }
        }

        info!("Shutdown: Completed sending all pending payloads");
    }

    /// Cleanup old entries from batch buffer by sending them to New Relic first
    /// Finds entries older than 5 minutes, sends them (even without report lines), then removes them
    pub async fn cleanup_old_batch_entries(
        &self,
        newrelic_client: Arc<NewRelicClient>,
        config: Arc<ExtensionConfig>,
    ) {
        let now = chrono::Utc::now();
        let threshold = chrono::Duration::minutes(5);

        // Collect old entries that need to be sent
        let old_entries: Vec<BatchedAgentPayload> = self.buffer
            .iter()
            .filter(|entry| now.signed_duration_since(entry.value().timestamp) >= threshold)
            .map(|entry| entry.value().clone())
            .collect();

        if old_entries.is_empty() {
            return;
        }

        debug!("Periodic cleanup: Found {} old batch entries to send and remove", old_entries.len());

        let payload_json = build_newrelic_payload(&old_entries, &config, None);

        // Send to New Relic — only remove from buffer on success (prevent data loss)
        if let Err(e) = newrelic_client.send_agent_payload(&config, &payload_json).await {
            error!(
                "Periodic cleanup: Failed to send old batch entries: {} - Keeping {} entries in buffer for next attempt",
                e,
                old_entries.len()
            );
            return;
        }

        info!("Periodic cleanup: Successfully sent {} old batch entries", old_entries.len());

        // Only remove entries AFTER successful send
        for item in &old_entries {
            self.buffer.remove(&item.request_id);
        }

        debug!("Periodic cleanup: Removed {} old entries (remaining in buffer: {})", old_entries.len(), self.buffer.len());
    }
}

/// Build New Relic payload JSON from batched agent payloads.
/// This is a pure function: no I/O, no side effects, easily testable.
pub fn build_newrelic_payload(
    items: &[BatchedAgentPayload],
    config: &ExtensionConfig,
    version_info: Option<&crate::version::VersionInfo>,
) -> String {
    // Pre-compute strings that are constant across all items — avoids 4x format! per call
    let log_group = format!("/aws/lambda/{}", config.aws.function_name);
    let log_stream = format!("newrelic-lambda-extension:{EXTENSION_VERSION}");

    let mut log_events = Vec::with_capacity(items.len() * 3);

    for item in items {
        let agent_str = String::from_utf8_lossy(&item.agent_payload_bytes);
        log_events.push(serde_json::json!({
            "id": &item.request_id,
            "message": &*agent_str,
            "timestamp": item.timestamp.timestamp_millis(),
        }));

        if let Some(ref report) = item.report_line {
            log_events.push(serde_json::json!({
                "id": &item.request_id,
                "message": report,
                "timestamp": item.timestamp.timestamp_millis(),
            }));
        }

        if let Some(vi) = version_info {
            let version_line = vi.format_version_line(&item.request_id);
            log_events.push(serde_json::json!({
                "id": &item.request_id,
                "message": version_line,
                "timestamp": item.timestamp.timestamp_millis(),
            }));
        }
    }

    let last_arn = items.last()
        .map(|i| i.invoked_function_arn.as_str())
        .unwrap_or("unknown");

    let entry = serde_json::json!({
        "logEvents": log_events,
        "logGroup": &log_group,
        "logStream": &log_stream,
        "messageType": "",
        "owner": "",
    });

    let payload = serde_json::json!({
        "context": {
            "function_name": &config.aws.function_name,
            "invoked_function_arn": last_arn,
            "log_group_name": &log_group,
            "log_stream_name": &log_stream,
        },
        "entry": entry.to_string(),
    });

    payload.to_string()
}

/// Split payloads into chunks of max_size, keeping each payload + report together
pub fn split_into_chunks(
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
#[inline]
pub fn estimate_item_size(item: &BatchedAgentPayload) -> usize {
    let report_size = item.report_line.as_ref().map_or(0, |r| r.len() + 150);
    item.agent_payload_bytes.len() + 150 + report_size
}

/// Estimate the base overhead of the JSON structure
#[inline]
fn estimate_base_overhead(config: &Arc<ExtensionConfig>) -> usize {
    // Rough estimate: context object + entry wrapper + logEvents array
    // Function name appears in multiple places
    500 + (config.aws.function_name.len() * 3)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Helper to create a test payload
    fn make_payload(request_id: &str, report: Option<&str>, arn: &str) -> BatchedAgentPayload {
        BatchedAgentPayload {
            request_id: request_id.to_string(),
            agent_payload_bytes: Arc::from(vec![1, 2, 3, 4]),
            report_line: report.map(|s| s.to_string()),
            invoked_function_arn: arn.to_string(),
            timestamp: chrono::Utc::now(),
        }
    }

    // ========================================================================
    // BatchBuffer core operations
    // ========================================================================

    #[test]
    fn test_batch_buffer_new_is_empty() {
        let buffer = BatchBuffer::new();
        assert!(buffer.buffer.is_empty());
        assert!(!buffer.should_send_batch_by_threshold());
    }

    #[test]
    fn test_add_to_batch_inserts() {
        let buffer = BatchBuffer::new();

        buffer.add_to_batch("req-1".to_string(), vec![10, 20], None, "arn:test".to_string());
        buffer.add_to_batch("req-2".to_string(), vec![30, 40], Some("REPORT".to_string()), "arn:test".to_string());

        assert!(buffer.buffer.contains_key("req-1"));
        assert!(buffer.buffer.contains_key("req-2"));

        // Verify report line
        let entry = buffer.buffer.get("req-2").expect("should exist");
        assert!(entry.report_line.is_some());
    }

    #[test]
    fn test_add_to_batch_overwrites_same_request_id() {
        let buffer = BatchBuffer::new();
        let key = "req-dup";

        buffer.add_to_batch(key.to_string(), vec![1], None, "arn:test".to_string());
        buffer.add_to_batch(key.to_string(), vec![2], Some("report".to_string()), "arn:test2".to_string());

        // DashMap overwrites on same key — buffer has exactly 1 entry
        assert_eq!(buffer.buffer.len(), 1);
        let entry = buffer.buffer.get(key).expect("should exist");
        assert!(entry.report_line.is_some());
        assert_eq!(&*entry.agent_payload_bytes, &[2u8]);
    }

    #[test]
    fn test_add_to_batch_empty_bytes() {
        let buffer = BatchBuffer::new();
        buffer.add_to_batch("req-empty".to_string(), vec![], None, "arn:test".to_string());

        let entry = buffer.buffer.get("req-empty").expect("should exist");
        assert!(entry.agent_payload_bytes.is_empty());
    }

    #[test]
    fn test_add_to_batch_empty_arn() {
        let buffer = BatchBuffer::new();
        buffer.add_to_batch("req-no-arn".to_string(), vec![1, 2], None, String::new());

        let entry = buffer.buffer.get("req-no-arn").expect("should exist");
        assert!(entry.invoked_function_arn.is_empty());
    }

    // ========================================================================
    // Threshold detection
    // ========================================================================

    #[test]
    fn test_threshold_counts_only_entries_with_reports() {
        let buffer = BatchBuffer::new();

        for i in 0..5 {
            buffer.add_to_batch(format!("no-{i}"), vec![1], None, "arn:test".to_string());
        }

        assert!(!buffer.should_send_batch_by_threshold());
    }

    #[test]
    fn test_should_send_batch_exactly_two_reports_boundary() {
        let buffer = BatchBuffer::new();

        buffer.add_to_batch("req-1".to_string(), vec![1], Some("r1".to_string()), "arn:test".to_string());
        buffer.add_to_batch("req-2".to_string(), vec![2], Some("r2".to_string()), "arn:test".to_string());

        // Exactly 2 reports — threshold is 3, so should NOT trigger
        assert!(!buffer.should_send_batch_by_threshold());
    }

    #[test]
    fn test_should_send_batch_reaches_threshold() {
        let buffer = BatchBuffer::new();

        buffer.add_to_batch("req-1".to_string(), vec![1], Some("report1".to_string()), "arn:test".to_string());
        buffer.add_to_batch("req-2".to_string(), vec![2], Some("report2".to_string()), "arn:test".to_string());
        buffer.add_to_batch("req-3".to_string(), vec![3], Some("report3".to_string()), "arn:test".to_string());

        assert!(buffer.should_send_batch_by_threshold());
    }

    // ========================================================================
    // Batch retrieval and clearing
    // ========================================================================

    #[test]
    fn test_get_and_clear_batch_returns_all_items() {
        let buffer = BatchBuffer::new();
        buffer.add_to_batch("req-1".to_string(), vec![1], None, "arn:test".to_string());
        buffer.add_to_batch("req-2".to_string(), vec![2], Some("report".to_string()), "arn:test".to_string());

        let items = buffer.get_and_clear_batch();
        assert_eq!(items.len(), 2);
        assert!(buffer.buffer.is_empty());
    }

    #[test]
    fn test_get_and_clear_batch_empty_buffer() {
        let buffer = BatchBuffer::new();
        let items = buffer.get_and_clear_batch();
        assert!(items.is_empty());
        assert!(buffer.buffer.is_empty());
    }

    #[test]
    fn test_get_batch_with_reports_filters_correctly() {
        let buffer = BatchBuffer::new();

        buffer.add_to_batch("req-a".to_string(), vec![1], None, "arn:test".to_string());
        buffer.add_to_batch("req-b".to_string(), vec![2], Some("report".to_string()), "arn:test".to_string());
        buffer.add_to_batch("req-c".to_string(), vec![3], None, "arn:test".to_string());

        let with_reports = buffer.get_batch_with_reports_only();

        assert_eq!(with_reports.len(), 1);
        assert_eq!(with_reports[0].request_id, "req-b");

        // Should NOT remove from buffer
        assert!(buffer.buffer.contains_key("req-a"));
        assert!(buffer.buffer.contains_key("req-b"));
        assert!(buffer.buffer.contains_key("req-c"));
    }

    #[test]
    fn test_clear_batch_with_reports_removes_only_specified() {
        let buffer = BatchBuffer::new();

        buffer.add_to_batch("req-1".to_string(), vec![1], Some("report1".to_string()), "arn:test".to_string());
        buffer.add_to_batch("req-2".to_string(), vec![2], None, "arn:test".to_string());
        buffer.add_to_batch("req-3".to_string(), vec![3], Some("report3".to_string()), "arn:test".to_string());

        let items_with_reports = buffer.get_batch_with_reports_only();
        assert_eq!(items_with_reports.len(), 2);

        buffer.clear_batch_with_reports(&items_with_reports);

        assert!(!buffer.buffer.contains_key("req-1"));
        assert!(buffer.buffer.contains_key("req-2"));
        assert!(!buffer.buffer.contains_key("req-3"));
    }

    // ========================================================================
    // build_newrelic_payload — pure function tests
    // ========================================================================

    #[test]
    fn test_build_newrelic_payload_single_item_no_report() {
        let config = ExtensionConfig::default();
        let items = vec![make_payload("req-1", None, "arn:aws:lambda:us-east-1:123:function:test")];
        let json_str = build_newrelic_payload(&items, &config, None);
        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("valid JSON");

        assert!(parsed["context"]["function_name"].is_string());
        assert!(parsed["entry"].is_string());

        let entry: serde_json::Value = serde_json::from_str(parsed["entry"].as_str().expect("entry is string")).expect("valid entry JSON");
        assert_eq!(entry["logEvents"].as_array().expect("logEvents array").len(), 1);
    }

    #[test]
    fn test_build_newrelic_payload_single_item_with_report() {
        let config = ExtensionConfig::default();
        let items = vec![make_payload("req-1", Some("REPORT Duration: 100 ms"), "arn:test")];
        let json_str = build_newrelic_payload(&items, &config, None);
        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("valid JSON");
        let entry: serde_json::Value = serde_json::from_str(parsed["entry"].as_str().expect("entry")).expect("valid entry");

        // Should have 2 log events: agent payload + report
        assert_eq!(entry["logEvents"].as_array().expect("array").len(), 2);
    }

    #[test]
    fn test_build_newrelic_payload_multiple_items() {
        let config = ExtensionConfig::default();
        let items = vec![
            make_payload("req-1", Some("REPORT 1"), "arn:test"),
            make_payload("req-2", None, "arn:test"),
            make_payload("req-3", Some("REPORT 3"), "arn:test"),
        ];
        let json_str = build_newrelic_payload(&items, &config, None);
        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("valid JSON");
        let entry: serde_json::Value = serde_json::from_str(parsed["entry"].as_str().expect("entry")).expect("valid entry");

        // req-1: 2 events, req-2: 1 event, req-3: 2 events = 5 total
        assert_eq!(entry["logEvents"].as_array().expect("array").len(), 5);
    }

    #[test]
    fn test_build_newrelic_payload_context_fields() {
        let mut config = ExtensionConfig::default();
        config.aws.function_name = "my-lambda".to_string();
        let items = vec![make_payload("req-1", None, "arn:aws:lambda:us-east-1:123:function:my-lambda")];
        let json_str = build_newrelic_payload(&items, &config, None);
        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("valid JSON");

        assert_eq!(parsed["context"]["function_name"], "my-lambda");
        assert_eq!(parsed["context"]["log_group_name"], "/aws/lambda/my-lambda");
        assert!(parsed["context"]["log_stream_name"].as_str().expect("string").starts_with("newrelic-lambda-extension:"));
    }

    #[test]
    fn test_build_newrelic_payload_empty_items() {
        let config = ExtensionConfig::default();
        let json_str = build_newrelic_payload(&[], &config, None);
        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("valid JSON");
        let entry: serde_json::Value = serde_json::from_str(parsed["entry"].as_str().expect("entry")).expect("valid entry");

        assert_eq!(entry["logEvents"].as_array().expect("array").len(), 0);
        assert_eq!(parsed["context"]["invoked_function_arn"], "unknown");
    }

    // ========================================================================
    // split_into_chunks — pure function tests
    // ========================================================================

    #[test]
    fn test_split_into_chunks_single_chunk() {
        let config = Arc::new(ExtensionConfig::default());
        let payloads = vec![
            make_payload("req-1", None, "arn:test"),
            make_payload("req-2", None, "arn:test"),
        ];

        let chunks = split_into_chunks(payloads, 1_000_000, &config);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 2);
    }

    #[test]
    fn test_split_into_chunks_forces_split_on_size() {
        let config = Arc::new(ExtensionConfig::default());

        let large_data = vec![0u8; 5000];
        let payloads: Vec<BatchedAgentPayload> = (0..10)
            .map(|i| BatchedAgentPayload {
                request_id: format!("req-{i}"),
                agent_payload_bytes: Arc::from(large_data.clone()),
                report_line: Some("REPORT Duration: 100 ms".to_string()),
                invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:test".to_string(),
                timestamp: chrono::Utc::now(),
            })
            .collect();

        let chunks = split_into_chunks(payloads, 15_000, &config);
        assert!(chunks.len() > 1, "Expected multiple chunks, got {}", chunks.len());

        let total_items: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total_items, 10);
    }

    #[test]
    fn test_split_into_chunks_empty() {
        let config = Arc::new(ExtensionConfig::default());
        let chunks = split_into_chunks(Vec::new(), 1_000_000, &config);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_into_chunks_single_oversized_item() {
        let config = Arc::new(ExtensionConfig::default());
        let large_data = vec![0u8; 2_000_000]; // 2MB single item
        let payloads = vec![BatchedAgentPayload {
            request_id: "req-big".to_string(),
            agent_payload_bytes: Arc::from(large_data),
            report_line: None,
            invoked_function_arn: "arn:test".to_string(),
            timestamp: chrono::Utc::now(),
        }];

        // Even though single item exceeds max_size, it's still in one chunk
        // (code adds to current_chunk when current_chunk is empty)
        let chunks = split_into_chunks(payloads, 1_000_000, &config);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 1);
    }

    // ========================================================================
    // estimate helpers
    // ========================================================================

    #[test]
    fn test_estimate_item_size_without_report() {
        let item = make_payload("req-1", None, "arn:test");
        let size = estimate_item_size(&item);
        assert_eq!(size, 4 + 150);
    }

    #[test]
    fn test_estimate_item_size_with_report() {
        let item = make_payload("req-1", Some("REPORT Duration: 123 ms"), "arn:test");
        let size = estimate_item_size(&item);
        assert_eq!(size, 4 + 23 + 150 + 150);
    }

    #[test]
    fn test_estimate_base_overhead_default_config() {
        let config = Arc::new(ExtensionConfig::default());
        let overhead = estimate_base_overhead(&config);
        assert_eq!(overhead, 500 + ("unknown".len() * 3));
    }

    #[test]
    fn test_estimate_base_overhead_long_function_name() {
        let mut config = ExtensionConfig::default();
        config.aws.function_name = "my-very-long-function-name-for-testing".to_string();
        let config = Arc::new(config);
        let overhead = estimate_base_overhead(&config);
        assert_eq!(overhead, 500 + (38 * 3));
    }

    // ========================================================================
    // Concurrency stress tests
    // ========================================================================

    #[test]
    fn test_concurrent_add_and_read_no_deadlock() {
        let buffer = Arc::new(BatchBuffer::new());

        let handles: Vec<_> = (0..5)
            .map(|thread_id| {
                let buffer = Arc::clone(&buffer);
                std::thread::spawn(move || {
                    for i in 0..10 {
                        let req_id = format!("t{thread_id}-r{i}");
                        let report = if i % 2 == 0 {
                            Some(format!("REPORT for {req_id}"))
                        } else {
                            None
                        };
                        buffer.add_to_batch(req_id, vec![1, 2, 3], report, "arn:test".to_string());
                    }

                    let _ = buffer.should_send_batch_by_threshold();
                    let _ = buffer.get_batch_with_reports_only();
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked — concurrency bug detected");
        }

        let final_count = buffer.buffer.len();
        assert!(final_count > 0, "Buffer should have entries after concurrent writes");
    }

    #[test]
    fn test_concurrent_add_and_clear_no_deadlock() {
        let buffer = Arc::new(BatchBuffer::new());

        let handles: Vec<_> = (0..5)
            .map(|thread_id| {
                let buffer = Arc::clone(&buffer);
                std::thread::spawn(move || {
                    for i in 0..20 {
                        let req_id = format!("t{thread_id}-r{i}");
                        buffer.add_to_batch(req_id, vec![1], Some("report".to_string()), "arn:test".to_string());

                        if i % 10 == 0 && thread_id == 0 {
                            let _ = buffer.get_and_clear_batch();
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked during concurrent add+clear");
        }
    }
}
