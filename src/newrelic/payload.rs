//! Payloads
//!
//! This module defines the data structures for creating JSON payloads
//! that are sent to the New Relic Log API.

use serde::Serialize;
use std::collections::HashMap;

/// Represents a single log entry.
#[derive(Serialize, Debug, Clone)]
pub struct LogMessage {
    /// The timestamp of the log entry in milliseconds since the Unix epoch.
    pub timestamp: i64,
    /// The message content of the log.
    pub message: String,
    /// Additional attributes for the log entry.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub attributes: HashMap<String, serde_json::Value>,
}

/// Represents the common attributes block for a log payload.
#[derive(Serialize, Debug, Clone)]
pub struct Common {
    /// A map of common attributes to apply to all logs in this batch.
    pub attributes: HashMap<&'static str, serde_json::Value>,
}

/// The top-level structure for a log payload sent to New Relic.
#[derive(Serialize, Debug, Clone)]
pub struct DetailedLog {
    /// The common block of attributes.
    pub common: Common,
    /// A list of log entries.
    pub logs: Vec<LogMessage>,
}
