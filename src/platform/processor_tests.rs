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
    fn create_platform_log_message_formats_init_runtime_done() {
        let processor = make_processor();

        let (msg, level) = processor.create_platform_log_message(&record(
            "platform.initRuntimeDone",
            serde_json::json!({"requestId": "req-init-done", "initializationType": "on-demand", "phase": "init", "status": "success"}),
        ));

        assert_eq!(msg, "INIT RUNTIME DONE RequestId: req-init-done Type: on-demand Phase: init Status: success");
        assert_eq!(level, "INFO");
    }

    #[test]
    fn create_platform_log_message_formats_init_report_without_metrics() {
        let processor = make_processor();

        // No "metrics" field at all -> duration_info stays empty (the else branch).
        let (msg, level) = processor.create_platform_log_message(&record(
            "platform.initReport",
            serde_json::json!({"requestId": "req-init-report", "initializationType": "on-demand", "phase": "init"}),
        ));

        assert_eq!(msg, "INIT REPORT RequestId: req-init-report Type: on-demand Phase: init");
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

    // LMI: billedDurationMs and memorySizeMB, when present, get appended too
    // (each is its own optional branch, separate from maxMemoryUsedMB below).
    #[test]
    fn lmi_report_appends_billed_duration_and_memory_size_when_present() {
        let p = processor(DeploymentContext::Lmi);
        let line = p
            .convert_platform_report_to_log_line(&report_record(
                serde_json::json!({ "durationMs": 12.5, "billedDurationMs": 13, "memorySizeMB": 256 }),
            ))
            .expect("should format");
        assert_eq!(line, "REPORT RequestId: req-1\tDuration: 12.50 ms\tBilled Duration: 13 ms\tMemory Size: 256 MB");
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

/// NR-569587: `platform_log_filter` gates which platform event types actually
/// reach the outbound log batch, on top of the existing `send_platform_logs` flag.
#[cfg(test)]
mod platform_log_filter_tests {
    use crate::config::ExtensionConfig;
    use crate::context::InvocationContext;
    use crate::logs::processor::LogProcessor;
    use crate::newrelic::client::NewRelicClient;
    use crate::platform::processor::PlatformProcessor;
    use crate::telemetry::listener::TelemetryRecord;
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    // Returns the processor plus our own handle to its LogProcessor, so tests can
    // check `test_log_batch_len()` without reaching into PlatformProcessor's
    // private field (processor_tests is a sibling module, not a descendant).
    fn processor_with_filter(send_platform_logs: bool, filter: HashSet<String>) -> (PlatformProcessor, Arc<LogProcessor>) {
        let mut config = ExtensionConfig::default();
        config.extension.send_platform_logs = send_platform_logs;
        config.extension.platform_log_filter = filter;
        let config = Arc::new(config);
        let client = Arc::new(NewRelicClient::new(&config));
        let ctx = Arc::new(Mutex::new(InvocationContext::default()));
        let log_processor = Arc::new(LogProcessor::new(
            Arc::clone(&client), Arc::clone(&config), Arc::clone(&ctx), None,
        ));
        let processor = PlatformProcessor::new(client, config, ctx, Arc::clone(&log_processor));
        (processor, log_processor)
    }

    fn record(record_type: &str) -> TelemetryRecord {
        TelemetryRecord {
            time: chrono::Utc::now(),
            record_type: record_type.to_string(),
            record: serde_json::json!({"requestId": "req-filter-test"}),
        }
    }

    #[test]
    fn no_filter_sends_every_type() {
        let (processor, log_processor) = processor_with_filter(true, HashSet::new());
        processor.process_record(record("platform.start"));
        processor.process_record(record("platform.report"));
        assert_eq!(log_processor.test_log_batch_len(), 2);
    }

    #[test]
    fn filter_allows_only_listed_types() {
        let mut filter = HashSet::new();
        filter.insert("platform.report".to_string());
        let (processor, log_processor) = processor_with_filter(true, filter);

        processor.process_record(record("platform.start"));
        assert_eq!(log_processor.test_log_batch_len(), 0);

        processor.process_record(record("platform.report"));
        assert_eq!(log_processor.test_log_batch_len(), 1);
    }

    #[test]
    fn filter_matches_case_insensitively() {
        // Filter stores lowercase (parsed from env); incoming record_type keeps AWS's
        // real casing. The gate lowercases record_type before comparing.
        let mut filter = HashSet::new();
        filter.insert("platform.start".to_string());
        let (processor, log_processor) = processor_with_filter(true, filter);

        processor.process_record(record("platform.start"));
        assert_eq!(log_processor.test_log_batch_len(), 1);
    }

    #[test]
    fn send_platform_logs_false_blocks_everything_regardless_of_filter() {
        let mut filter = HashSet::new();
        filter.insert("platform.report".to_string());
        let (processor, log_processor) = processor_with_filter(false, filter);

        processor.process_record(record("platform.report"));
        assert_eq!(log_processor.test_log_batch_len(), 0);
    }
}

/// `check_and_send_platform_errors` — detects error/failure/timeout status on the
/// 5 platform event types that can carry errors, and records the last-detected
/// error (read via `error_synthesis::LAST_DETECTED_ERROR`, a process-wide global —
/// hence `#[serial]`, matching the convention already used for other global/env
/// state in this crate's tests).
#[cfg(test)]
mod check_and_send_platform_errors_tests {
    use crate::config::ExtensionConfig;
    use crate::context::InvocationContext;
    use crate::error_synthesis::LAST_DETECTED_ERROR;
    use crate::logs::processor::LogProcessor;
    use crate::newrelic::client::NewRelicClient;
    use crate::platform::processor::PlatformProcessor;
    use crate::telemetry::listener::TelemetryRecord;
    use serial_test::serial;
    use std::sync::{Arc, Mutex};

    fn processor() -> PlatformProcessor {
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

    fn clear_last_error() {
        *LAST_DETECTED_ERROR.lock().unwrap() = None;
    }

    #[test]
    #[serial]
    fn non_error_capable_type_is_ignored() {
        clear_last_error();
        let p = processor();
        // platform.start/platform.report are not in the can-have-errors list at all.
        p.process_record(record("platform.start", serde_json::json!({"requestId": "r1", "status": "error"})));
        assert!(LAST_DETECTED_ERROR.lock().unwrap().is_none());
    }

    #[test]
    #[serial]
    fn error_capable_type_without_error_status_is_ignored() {
        clear_last_error();
        let p = processor();
        p.process_record(record("platform.runtimeDone", serde_json::json!({"requestId": "r2", "status": "success"})));
        assert!(LAST_DETECTED_ERROR.lock().unwrap().is_none());
    }

    #[test]
    #[serial]
    fn init_report_error_with_error_type_is_recorded() {
        clear_last_error();
        let p = processor();
        p.process_record(record(
            "platform.initReport",
            serde_json::json!({"requestId": "r3", "status": "error", "phase": "init", "errorType": "Runtime.ExitError"}),
        ));
        let last = LAST_DETECTED_ERROR.lock().unwrap().clone().expect("should record error");
        assert_eq!(last.request_id, "r3");
        assert_eq!(last.error_type, "Runtime.ExitError");
    }

    #[test]
    #[serial]
    fn init_runtime_done_failure_without_error_type_is_not_recorded() {
        clear_last_error();
        let p = processor();
        // No errorType field -> error message still built, but LAST_DETECTED_ERROR
        // is only updated when an errorType is present.
        p.process_record(record(
            "platform.initRuntimeDone",
            serde_json::json!({"requestId": "r4", "status": "failure", "phase": "init"}),
        ));
        assert!(LAST_DETECTED_ERROR.lock().unwrap().is_none());
    }

    #[test]
    #[serial]
    fn runtime_done_timeout_with_duration_is_recorded() {
        clear_last_error();
        let p = processor();
        p.process_record(record(
            "platform.runtimeDone",
            serde_json::json!({"requestId": "r5", "status": "timeout", "errorType": "Sandbox.Timedout", "metrics": {"durationMs": 3000.0}}),
        ));
        let last = LAST_DETECTED_ERROR.lock().unwrap().clone().expect("should record error");
        assert_eq!(last.error_type, "Sandbox.Timedout");
    }

    #[test]
    #[serial]
    fn runtime_done_timeout_without_duration_is_recorded() {
        clear_last_error();
        let p = processor();
        p.process_record(record(
            "platform.runtimeDone",
            serde_json::json!({"requestId": "r6", "status": "timeout", "errorType": "Sandbox.Timedout"}),
        ));
        let last = LAST_DETECTED_ERROR.lock().unwrap().clone().expect("should record error");
        assert_eq!(last.error_type, "Sandbox.Timedout");
    }

    #[test]
    #[serial]
    fn runtime_done_error_with_duration_and_error_type_is_recorded() {
        clear_last_error();
        let p = processor();
        p.process_record(record(
            "platform.runtimeDone",
            serde_json::json!({"requestId": "r7", "status": "error", "errorType": "Runtime.HandlerError", "metrics": {"durationMs": 42.0}}),
        ));
        let last = LAST_DETECTED_ERROR.lock().unwrap().clone().expect("should record error");
        assert_eq!(last.error_type, "Runtime.HandlerError");
    }

    #[test]
    #[serial]
    fn runtime_done_failure_without_error_type_or_duration_is_not_recorded() {
        clear_last_error();
        let p = processor();
        p.process_record(record(
            "platform.runtimeDone",
            serde_json::json!({"requestId": "r8", "status": "failure"}),
        ));
        assert!(LAST_DETECTED_ERROR.lock().unwrap().is_none());
    }

    #[test]
    #[serial]
    fn restore_runtime_done_error_with_error_type_is_recorded() {
        clear_last_error();
        let p = processor();
        p.process_record(record(
            "platform.restoreRuntimeDone",
            serde_json::json!({"requestId": "r9", "status": "error", "errorType": "Runtime.RestoreError"}),
        ));
        let last = LAST_DETECTED_ERROR.lock().unwrap().clone().expect("should record error");
        assert_eq!(last.error_type, "Runtime.RestoreError");
    }

    #[test]
    #[serial]
    fn restore_report_failure_without_error_type_is_not_recorded() {
        clear_last_error();
        let p = processor();
        p.process_record(record(
            "platform.restoreReport",
            serde_json::json!({"requestId": "r10", "status": "failure"}),
        ));
        assert!(LAST_DETECTED_ERROR.lock().unwrap().is_none());
    }

    #[test]
    #[serial]
    fn missing_request_id_falls_back_to_instance_id() {
        clear_last_error();
        let p = processor();
        // No "requestId" field at all; falls back to "instanceId" (init-prefixed, first 8 chars).
        p.process_record(record(
            "platform.initReport",
            serde_json::json!({"instanceId": "abcdefghijklmnop", "status": "error", "phase": "init", "errorType": "Runtime.InitError"}),
        ));
        let last = LAST_DETECTED_ERROR.lock().unwrap().clone().expect("should record error");
        assert_eq!(last.request_id, "init-abcdefgh");
    }

    #[test]
    #[serial]
    fn empty_request_id_falls_back_to_instance_id() {
        clear_last_error();
        let p = processor();
        // "requestId" present but empty -> same instanceId fallback path as the missing case.
        p.process_record(record(
            "platform.initReport",
            serde_json::json!({"requestId": "", "instanceId": "zzzzzzzzzzzz", "status": "error", "phase": "init", "errorType": "Runtime.InitError"}),
        ));
        let last = LAST_DETECTED_ERROR.lock().unwrap().clone().expect("should record error");
        assert_eq!(last.request_id, "init-zzzzzzzz");
    }

    #[test]
    #[serial]
    fn no_request_id_or_instance_id_generates_fallback_id() {
        clear_last_error();
        let p = processor();
        p.process_record(record(
            "platform.initReport",
            serde_json::json!({"status": "error", "phase": "init", "errorType": "Runtime.InitError"}),
        ));
        let last = LAST_DETECTED_ERROR.lock().unwrap().clone().expect("should record error");
        assert!(last.request_id.starts_with("init-"));
    }
}
