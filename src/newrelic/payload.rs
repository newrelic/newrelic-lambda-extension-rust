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
mod tests {
    use super::*;

    #[test]
    fn diagnostic_uses_nr_log_attribute_conventions() {
        let log = LogMessage::diagnostic("ERROR", "APM telemetry DROPPED at shutdown".to_string());
        assert_eq!(log.message, "APM telemetry DROPPED at shutdown");
        assert!(log.timestamp > 0, "timestamp must be set");
        assert_eq!(log.attributes["level"], serde_json::json!("ERROR"));
        // Must match the normal log path keys (src/logs/processor.rs) so NR Logs
        // categorizes it correctly — NOT the old "log_type" key.
        assert_eq!(log.attributes["_nr.logType"], serde_json::json!("extension"));
        assert_eq!(log.attributes["newrelic.source"], serde_json::json!("api.logs"));
        assert!(!log.attributes.contains_key("log_type"), "must not use the wrong key");
    }
}
