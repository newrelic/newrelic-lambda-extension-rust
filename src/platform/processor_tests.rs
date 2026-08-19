// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod tests {
    use crate::config::ExtensionConfig;
    use crate::context::InvocationContext;
    use crate::logs::processor::LogProcessor;
    use crate::newrelic::client::NewRelicClient;
    use crate::platform::processor::{normalize_platform_runtime_version, PlatformProcessor};
    use crate::telemetry::listener::TelemetryRecord;
    use std::sync::{Arc, Mutex};

    fn make_processor() -> PlatformProcessor {
        let client = Arc::new(NewRelicClient::new_noop());
        let config = Arc::new(ExtensionConfig::default());
        let context = Arc::new(Mutex::new(InvocationContext::default()));
        let log_processor = Arc::new(LogProcessor::new(
            client.clone(),
            config.clone(),
            context.clone(),
            None,
        ));
        PlatformProcessor::new(client, config, context, log_processor)
    }

    fn record(record_type: &str, fields: serde_json::Value) -> TelemetryRecord {
        TelemetryRecord {
            time: chrono::Utc::now(),
            record_type: record_type.to_string(),
            record: fields,
        }
    }

    #[test]
    fn convert_platform_report_to_log_line_formats_all_fields() {
        let processor = make_processor();
        let rec = record(
            "platform.report",
            serde_json::json!({
                "requestId": "req-1",
                "metrics": {
                    "durationMs": 123.45,
                    "billedDurationMs": 124,
                    "memorySizeMB": 128,
                    "maxMemoryUsedMB": 90,
                    "initDurationMs": 250.5
                }
            }),
        );

        let line = processor.convert_platform_report_to_log_line(&rec).expect("should format a REPORT line");

        assert!(line.starts_with("REPORT RequestId: req-1"));
        assert!(line.contains("Duration: 123.45 ms"));
        assert!(line.contains("Billed Duration: 124 ms"));
        assert!(line.contains("Memory Size: 128 MB"));
        assert!(line.contains("Max Memory Used: 90 MB"));
        assert!(line.contains("Init Duration: 250.50 ms"));
    }

    #[test]
    fn convert_platform_report_to_log_line_omits_init_duration_when_absent() {
        let processor = make_processor();
        let rec = record(
            "platform.report",
            serde_json::json!({
                "requestId": "req-2",
                "metrics": {
                    "durationMs": 10.0,
                    "billedDurationMs": 11,
                    "memorySizeMB": 128,
                    "maxMemoryUsedMB": 64
                }
            }),
        );

        let line = processor.convert_platform_report_to_log_line(&rec).expect("should format a REPORT line");

        assert!(!line.contains("Init Duration"));
    }

    #[test]
    fn convert_platform_report_to_log_line_returns_none_on_missing_required_fields() {
        let processor = make_processor();

        // No requestId at all.
        let rec = record("platform.report", serde_json::json!({"metrics": {"durationMs": 1.0}}));
        assert!(processor.convert_platform_report_to_log_line(&rec).is_none());

        // requestId present, metrics missing entirely.
        let rec = record("platform.report", serde_json::json!({"requestId": "req-3"}));
        assert!(processor.convert_platform_report_to_log_line(&rec).is_none());

        // metrics present but missing a required numeric field (billedDurationMs).
        let rec = record(
            "platform.report",
            serde_json::json!({"requestId": "req-4", "metrics": {"durationMs": 1.0}}),
        );
        assert!(processor.convert_platform_report_to_log_line(&rec).is_none());
    }

    #[test]
    fn create_platform_log_message_formats_start_and_end() {
        let processor = make_processor();

        let (msg, level) = processor.create_platform_log_message(&record(
            "platform.start",
            serde_json::json!({"requestId": "req-start"}),
        ));
        assert_eq!(msg, "START RequestId: req-start");
        assert_eq!(level, "INFO");

        let (msg, level) = processor.create_platform_log_message(&record(
            "platform.end",
            serde_json::json!({"requestId": "req-end"}),
        ));
        assert_eq!(msg, "END RequestId: req-end");
        assert_eq!(level, "INFO");
    }

    #[test]
    fn create_platform_log_message_formats_runtime_done_with_status() {
        let processor = make_processor();

        let (msg, level) = processor.create_platform_log_message(&record(
            "platform.runtimeDone",
            serde_json::json!({"requestId": "req-done", "status": "success"}),
        ));

        assert_eq!(msg, "RUNTIME DONE RequestId: req-done Status: success");
        assert_eq!(level, "INFO");
    }

    #[test]
    fn create_platform_log_message_falls_back_to_report_formatting_failed_when_fields_missing() {
        let processor = make_processor();

        let (msg, level) = processor.create_platform_log_message(&record("platform.report", serde_json::json!({})));

        assert_eq!(msg, "REPORT formatting failed - missing required fields");
        assert_eq!(level, "WARN");
    }

    #[test]
    fn create_platform_log_message_formats_unknown_event_type_generically() {
        let processor = make_processor();

        let (msg, level) = processor.create_platform_log_message(&record(
            "platform.somethingNew",
            serde_json::json!({"requestId": "req-unknown"}),
        ));

        assert!(msg.starts_with("PLATFORM EVENT PLATFORM.SOMETHINGNEW RequestId: req-unknown"));
        assert_eq!(level, "INFO");
    }

    #[test]
    fn extract_request_id_from_message_parses_report_line_with_tab() {
        let processor = make_processor();
        let msg = "REPORT RequestId: abc-123\tDuration: 1.00 ms";
        assert_eq!(processor.extract_request_id_from_message(msg), Some("abc-123".to_string()));
    }

    #[test]
    fn extract_request_id_from_message_parses_report_line_without_tab() {
        let processor = make_processor();
        let msg = "REPORT RequestId: abc-456";
        assert_eq!(processor.extract_request_id_from_message(msg), Some("abc-456".to_string()));
    }

    #[test]
    fn extract_request_id_from_message_returns_none_for_non_report_line() {
        let processor = make_processor();
        assert_eq!(processor.extract_request_id_from_message("START RequestId: abc-789"), None);
    }

    #[test]
    fn extract_log_level_from_message_detects_each_level() {
        let processor = make_processor();
        assert_eq!(processor.extract_log_level_from_message("an ERROR occurred"), "ERROR");
        assert_eq!(processor.extract_log_level_from_message("operation failed"), "ERROR");
        assert_eq!(processor.extract_log_level_from_message("Exception thrown"), "ERROR");
        assert_eq!(processor.extract_log_level_from_message("WARNING: low memory"), "WARNING");
        assert_eq!(processor.extract_log_level_from_message("debug trace here"), "DEBUG");
        assert_eq!(processor.extract_log_level_from_message("TRACE enabled"), "TRACE");
        assert_eq!(processor.extract_log_level_from_message("just a normal message"), "INFO");
    }

    #[test]
    fn test_normalize_platform_runtime_version() {
        // Node.js - use .x suffix
        assert_eq!(normalize_platform_runtime_version("nodejs:18.v98"), "nodejs18.x");
        assert_eq!(normalize_platform_runtime_version("nodejs:20.v15"), "nodejs20.x");
        assert_eq!(normalize_platform_runtime_version("nodejs:22.v2"), "nodejs22.x");

        // Python - keep major.minor
        assert_eq!(normalize_platform_runtime_version("python:3.13"), "python3.13");
        assert_eq!(normalize_platform_runtime_version("python:3.12.5"), "python3.12");

        // Ruby - keep major.minor
        assert_eq!(normalize_platform_runtime_version("ruby:3.3"), "ruby3.3");
        assert_eq!(normalize_platform_runtime_version("ruby:3.2.0"), "ruby3.2");

        // Java - keep major only
        assert_eq!(normalize_platform_runtime_version("java:17"), "java17");
        assert_eq!(normalize_platform_runtime_version("java:21"), "java21");

        // .NET - keep major only
        assert_eq!(normalize_platform_runtime_version("dotnet:8"), "dotnet8");
        assert_eq!(normalize_platform_runtime_version("dotnet:6"), "dotnet6");

        // No colon - return as-is
        assert_eq!(normalize_platform_runtime_version("unknown"), "unknown");
        assert_eq!(normalize_platform_runtime_version("go1.x"), "go1.x");
    }
}

/// NR-579360: platform.report -> log line under the stripped LMI report.
/// LMI omits billedDurationMs / memorySizeMB / maxMemoryUsedMB; the report must
/// still produce a REPORT line (not be dropped as "missing required fields").
/// Standard Lambda keeps the strict all-fields-required behavior verbatim.
#[cfg(test)]
mod report_log_line_tests {
    use crate::platform::processor::PlatformProcessor;
    use crate::config::deployment::{DeploymentContext, TelemetryMode};
    use crate::config::ExtensionConfig;
    use crate::context::InvocationContext;
    use crate::newrelic::client::NewRelicClient;
    use crate::logs::processor::LogProcessor;
    use crate::telemetry::listener::TelemetryRecord;
    use std::sync::{Arc, Mutex};

    fn processor(deployment: DeploymentContext) -> PlatformProcessor {
        let mut config = ExtensionConfig::default();
        config.deployment = deployment;
        let config = Arc::new(config);
        let client = Arc::new(NewRelicClient::new(&config));
        let ctx = Arc::new(Mutex::new(InvocationContext::default()));
        let log_processor = Arc::new(LogProcessor::new(
            Arc::clone(&client), Arc::clone(&config), Arc::clone(&ctx), None,
        ));
        PlatformProcessor::new(client, config, ctx, log_processor)
    }

    fn report_record(metrics: serde_json::Value) -> TelemetryRecord {
        TelemetryRecord {
            time: chrono::DateTime::from_timestamp(0, 0).expect("epoch"),
            record_type: "platform.report".to_string(),
            record: serde_json::json!({ "requestId": "req-1", "metrics": metrics }),
        }
    }

    // LMI: durationMs only (the stripped report) must still yield a REPORT line.
    #[test]
    fn lmi_report_with_only_duration_produces_line() {
        let p = processor(DeploymentContext::Lmi);
        let line = p
            .convert_platform_report_to_log_line(&report_record(serde_json::json!({ "durationMs": 140.0 })))
            .expect("LMI report with durationMs should still format");
        assert_eq!(line, "REPORT RequestId: req-1\tDuration: 140.00 ms");
        assert!(!line.contains("Billed Duration"));
        assert!(!line.contains("Memory Size"));
    }

    // LMI: any optional fields that ARE present get appended.
    #[test]
    fn lmi_report_appends_present_optional_fields() {
        let p = processor(DeploymentContext::Lmi);
        let line = p
            .convert_platform_report_to_log_line(&report_record(
                serde_json::json!({ "durationMs": 12.5, "maxMemoryUsedMB": 84 }),
            ))
            .expect("should format");
        assert_eq!(line, "REPORT RequestId: req-1\tDuration: 12.50 ms\tMax Memory Used: 84 MB");
    }

    // Normal: full metric set -> byte-identical to the original strict format.
    #[test]
    fn normal_report_full_is_unchanged() {
        let p = processor(DeploymentContext::Normal { mode: TelemetryMode::Serverless });
        let line = p
            .convert_platform_report_to_log_line(&report_record(serde_json::json!({
                "durationMs": 693.92, "billedDurationMs": 694, "memorySizeMB": 128,
                "maxMemoryUsedMB": 84, "initDurationMs": 397.68
            })))
            .expect("should format");
        assert_eq!(
            line,
            "REPORT RequestId: req-1\tDuration: 693.92 ms\tBilled Duration: 694 ms\tMemory Size: 128 MB\tMax Memory Used: 84 MB\tInit Duration: 397.68 ms"
        );
    }

    // Normal regression: a report missing the billed/memory fields still returns None,
    // exactly as before this change (strict behavior preserved for Standard Lambda).
    #[test]
    fn normal_report_missing_fields_returns_none() {
        let p = processor(DeploymentContext::Normal { mode: TelemetryMode::Apm });
        assert!(p
            .convert_platform_report_to_log_line(&report_record(serde_json::json!({ "durationMs": 140.0 })))
            .is_none());
    }
}
