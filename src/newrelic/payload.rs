// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use serde::Serialize;
use serde_json::Map;

#[derive(Debug, Clone, Serialize)]
pub struct LogMessage {
    pub timestamp: i64,
    pub message: String,
    pub attributes: Map<String, serde_json::Value>,
}

impl LogMessage {
    /// Build a New Relic log event for one of the extension's OWN diagnostics
    /// (e.g. the shutdown "telemetry DROPPED" summary that is POSTed directly to
    /// the Log ingest). Uses the attribute keys New Relic's Logs UI expects —
    /// matching the normal log path (`level`, `_nr.logType`, `newrelic.source`) —
    /// so the line categorizes and renders like the customer's other logs.
    /// Common attributes (plugin, faas.name, faas.arn, NR_TAGS) are added by
    /// `NewRelicClient::send_logs` per-ARN, so they are not set here.
    pub fn diagnostic(level: &str, message: String) -> Self {
        let mut attributes = Map::new();
        attributes.insert("level".to_string(), serde_json::json!(level));
        attributes.insert("_nr.logType".to_string(), serde_json::json!("extension"));
        attributes.insert("newrelic.source".to_string(), serde_json::json!("api.logs"));
        Self {
            timestamp: chrono::Utc::now().timestamp_millis(),
            message,
            attributes,
        }
    }
}

#[cfg(test)]
#[path = "payload_tests.rs"]
mod tests;
