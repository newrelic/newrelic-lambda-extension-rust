
use tracing::{debug, error, warn};
use std::sync::Arc;

use crate::newrelic::payload;
use crate::util::SafeMutexOps;

use super::processor::LogProcessor;

/// A log entry that failed to send and is queued for retry
#[derive(Debug, Clone)]
pub(crate) struct FailedLogEntry {
    pub(crate) log_message: payload::LogMessage,
    pub(crate) original_request_id: String,
    pub(crate) retry_count: usize,
}

/// Configuration constants for batching and retry logic
pub(crate) const MAX_BATCH_SIZE: usize = 100;
pub(crate) const MAX_RETRIES: usize = 3;

/// Maximum number of failed logs to buffer for cross-invocation retry.
/// Production data shows these buffers are rarely used (inline retries
/// succeed). 100 entries caps worst-case memory at ~200KB.
pub(crate) const MAX_FAILED_LOGS: usize = 100;

/// Estimate log message size in bytes without full JSON serialization.
/// Uses byte counting on attribute keys/values instead of serde_json::to_string.
pub(crate) fn estimate_log_size(log: &payload::LogMessage) -> usize {
    // Base overhead: timestamp (8 bytes) + JSON structure (~20 bytes)
    let mut size = 28 + log.message.len();
    for (key, value) in &log.attributes {
        size += key.len() + 4; // key + quotes + colon + comma
        size += match value {
            serde_json::Value::String(s) => s.len() + 2,
            serde_json::Value::Null => 4,
            serde_json::Value::Bool(_) => 5,
            serde_json::Value::Number(n) => n.to_string().len(),
            // For nested objects, use to_string as fallback (rare in hot path)
            other => other.to_string().len(),
        };
    }
    size
}

/// Determines whether a failed log should be buffered for retry.
/// Function logs (customer data) are always retried.
/// Extension logs are only retried if they are ERROR level.
/// Platform/Unknown logs are retried to be safe.
pub(crate) fn should_retry_on_failure(log: &payload::LogMessage) -> bool {
    match log.log_source {
        payload::LogSource::Extension => {
            // Only retry extension ERROR logs -- drop info/debug to save memory
            log.attributes.get("level")
                .and_then(|v| v.as_str())
                .map(|level| level == "ERROR")
                .unwrap_or(false)
        }
        // Function (customer data), Platform, Unknown -- always retry
        payload::LogSource::Function | payload::LogSource::Platform | payload::LogSource::Unknown => true,
    }
}

impl LogProcessor {
    pub(crate) async fn send_buffered_logs_with_retry(&self, logs: Vec<payload::LogMessage>) -> std::io::Result<()> {
        if logs.is_empty() {
            return Ok(());
        }

        let client = Arc::clone(&self.newrelic_client);
        let config = Arc::clone(&self.config);
        let context = if let Some(ctx) = self.invocation_context.safe_lock() {
            ctx.clone()
        } else {
            warn!("Failed to acquire invocation context for buffered log send");
            return Ok(());
        };

        let chunks: Vec<Vec<payload::LogMessage>> = logs
            .chunks(MAX_BATCH_SIZE)
            .map(|chunk| chunk.to_vec())
            .collect();

        if chunks.len() > 1 {
            debug!("Chunking {} buffered logs into {} batches", logs.len(), chunks.len());
        }

        let mut failed_count = 0;
        let mut successful_chunks = 0;

        for chunk in chunks {
            match self.send_chunk_internal(&client, &config, chunk.clone(), &context.invoked_function_arn, false).await {
                Ok(()) => {
                    successful_chunks += 1;
                },
                Err(e) => {
                    error!("Buffered logs send failed: {}", e);
                    failed_count += chunk.len();
                }
            }
        }

        if successful_chunks > 0 {
            debug!("Successfully sent {} buffered log chunks", successful_chunks);
        }
        if failed_count > 0 {
            warn!("Dropped {} buffered logs due to send failures", failed_count);
        }

        Ok(())
    }
}
