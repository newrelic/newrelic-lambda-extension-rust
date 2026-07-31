// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

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
