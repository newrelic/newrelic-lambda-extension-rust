use crate::newrelic::payload;
use crate::telemetry::listener::TelemetryRecord;
use std::{collections::HashMap, sync::Mutex};

/// The LogProcessor is responsible for handling and transforming function and extension logs.
#[derive(Debug, Default)]
pub struct LogProcessor {
    log_batch: Mutex<Vec<payload::LogMessage>>,
}

impl LogProcessor {
    /// Creates a new LogProcessor.
    pub fn new() -> Self {
        Self::default()
    }

    /// Processes a single log telemetry record, adding it to the batch if valid.
    pub fn process_record(&self, record: TelemetryRecord) {
        if let Some(log_message) = self.to_log_message(record) {
            if let Ok(mut batch) = self.log_batch.lock() {
                batch.push(log_message);
            }
        }
    }

    /// Converts a TelemetryRecord into a LogMessage, if applicable.
    fn to_log_message(&self, record: TelemetryRecord) -> Option<payload::LogMessage> {
        let timestamp = record.time.timestamp_millis();
        // The message can be a string or a JSON object, so we handle both cases.
        let message = record
            .record
            .get("message")
            .map(|v| v.to_string())
            .unwrap_or_default();
        let mut attributes = HashMap::new();
        if let Some(request_id) = record.record.get("requestId").and_then(|v| v.as_str()) {
            attributes.insert("request_id".to_string(), request_id.into());
        }

        if let Some(span_id) = record.record.get("spanId").and_then(|v| v.as_str()) {
            attributes.insert("span.id".to_string(), span_id.into());
        }
        if let Some(trace_id) = record.record.get("traceId").and_then(|v| v.as_str()) {
            attributes.insert("trace.id".to_string(), trace_id.into());
        }

        Some(payload::LogMessage {
            timestamp,
            message,
            attributes,
        })
    }

    /// Returns a batch of logs if the buffer is full, leaving the rest.
    pub fn harvest(&self) -> Option<Vec<payload::LogMessage>> {
        let config = crate::config::get_config();
        let max_items = config.extension.max_batch_items as usize;

        if let Ok(mut batch) = self.log_batch.lock() {
            if batch.len() >= max_items {
                // Take the entire batch to send
                return Some(std::mem::take(&mut *batch));
            }
        }
        None
    }

    /// Drains all remaining logs from the batch for a final send.
    pub fn harvest_all(&self) -> Vec<payload::LogMessage> {
        if let Ok(mut batch) = self.log_batch.lock() {
            std::mem::take(&mut *batch)
        } else {
            Vec::new()
        }
    }
}

