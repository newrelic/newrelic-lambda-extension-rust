#[cfg(test)]
mod tests {
    use crate::platform::processor::{normalize_platform_runtime_version, PlatformProcessor};
    use crate::telemetry::listener::TelemetryRecord;
    use crate::config::ExtensionConfig;
    use crate::context::InvocationContext;
    use crate::newrelic::client::NewRelicClient;
    use crate::request::ProcessorFactory;
    use std::sync::{Arc, Mutex};

    fn make_processor() -> PlatformProcessor {
        let config = Arc::new(ExtensionConfig::default());
        let client = Arc::new(NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let factory = ProcessorFactory::new(client.clone(), config.clone(), apm_app);
        let context = Arc::new(Mutex::new(InvocationContext {
            request_id: "req-test".to_string(),
            invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:test-fn".to_string(),
            trace_id: None,
        }));
        let log_processor = factory.create_log_processor(context.clone());
        PlatformProcessor::new(client, config, context, log_processor)
    }

    fn make_telemetry_record(record_type: &str, record: serde_json::Value) -> TelemetryRecord {
        TelemetryRecord {
            time: chrono::Utc::now(),
            record_type: record_type.to_string(),
            record,
        }
    }

    // ========================================================================
    // normalize_platform_runtime_version
    // ========================================================================

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

    // ========================================================================
    // convert_platform_report_to_log_line
    // ========================================================================

    #[test]
    fn test_convert_platform_report_basic() {
        let processor = make_processor();
        let record = make_telemetry_record("platform.report", serde_json::json!({
            "requestId": "abc-123",
            "metrics": {
                "durationMs": 123.45,
                "billedDurationMs": 124,
                "memorySizeMB": 512,
                "maxMemoryUsedMB": 256
            }
        }));

        let result = processor.convert_platform_report_to_log_line(&record);
        assert!(result.is_some());
        let line = result.expect("should produce report line");
        assert!(line.contains("REPORT RequestId: abc-123"));
        assert!(line.contains("Duration: 123.45 ms"));
        assert!(line.contains("Billed Duration: 124 ms"));
        assert!(line.contains("Memory Size: 512 MB"));
        assert!(line.contains("Max Memory Used: 256 MB"));
        assert!(!line.contains("Init Duration"));
    }

    #[test]
    fn test_convert_platform_report_with_init_duration() {
        let processor = make_processor();
        let record = make_telemetry_record("platform.report", serde_json::json!({
            "requestId": "abc-123",
            "metrics": {
                "durationMs": 100.0,
                "billedDurationMs": 101,
                "memorySizeMB": 256,
                "maxMemoryUsedMB": 128,
                "initDurationMs": 567.89
            }
        }));

        let result = processor.convert_platform_report_to_log_line(&record);
        let line = result.expect("should produce report line");
        assert!(line.contains("Init Duration: 567.89 ms"));
    }

    #[test]
    fn test_convert_platform_report_missing_metrics() {
        let processor = make_processor();
        let record = make_telemetry_record("platform.report", serde_json::json!({
            "requestId": "abc-123"
        }));

        let result = processor.convert_platform_report_to_log_line(&record);
        assert!(result.is_none(), "Missing metrics should return None");
    }

    #[test]
    fn test_convert_platform_report_missing_request_id() {
        let processor = make_processor();
        let record = make_telemetry_record("platform.report", serde_json::json!({
            "metrics": {
                "durationMs": 100.0,
                "billedDurationMs": 101,
                "memorySizeMB": 256,
                "maxMemoryUsedMB": 128
            }
        }));

        let result = processor.convert_platform_report_to_log_line(&record);
        assert!(result.is_none(), "Missing requestId should return None");
    }

    #[test]
    fn test_convert_platform_report_missing_required_metric_field() {
        let processor = make_processor();
        // Missing billedDurationMs
        let record = make_telemetry_record("platform.report", serde_json::json!({
            "requestId": "abc-123",
            "metrics": {
                "durationMs": 100.0,
                "memorySizeMB": 256,
                "maxMemoryUsedMB": 128
            }
        }));

        let result = processor.convert_platform_report_to_log_line(&record);
        assert!(result.is_none(), "Missing billedDurationMs should return None");
    }

    // ========================================================================
    // extract_request_id_from_record (via convert_platform_report_to_log_line)
    // ========================================================================

    #[test]
    fn test_extract_request_id_from_record_present() {
        let processor = make_processor();
        let record = make_telemetry_record("platform.report", serde_json::json!({
            "requestId": "my-req-id-42",
            "metrics": {
                "durationMs": 1.0,
                "billedDurationMs": 1,
                "memorySizeMB": 128,
                "maxMemoryUsedMB": 64
            }
        }));

        let line = processor.convert_platform_report_to_log_line(&record).expect("line");
        assert!(line.contains("my-req-id-42"));
    }

    // ========================================================================
    // create_platform_log_message (tested via record type routing)
    // ========================================================================

    #[test]
    fn test_create_log_message_platform_report() {
        let processor = make_processor();
        let record = make_telemetry_record("platform.report", serde_json::json!({
            "requestId": "req-1",
            "metrics": {
                "durationMs": 50.0,
                "billedDurationMs": 51,
                "memorySizeMB": 128,
                "maxMemoryUsedMB": 64
            }
        }));

        let (msg, level) = processor.create_platform_log_message(&record);
        assert_eq!(level, "INFO");
        assert!(msg.contains("REPORT RequestId: req-1"));
    }

    #[test]
    fn test_create_log_message_platform_report_missing_fields() {
        let processor = make_processor();
        let record = make_telemetry_record("platform.report", serde_json::json!({}));

        let (msg, level) = processor.create_platform_log_message(&record);
        assert_eq!(level, "WARN");
        assert!(msg.contains("formatting failed"));
    }

    #[test]
    fn test_create_log_message_platform_init_start() {
        let processor = make_processor();
        let record = make_telemetry_record("platform.initStart", serde_json::json!({
            "requestId": "req-init",
            "initializationType": "on-demand",
            "runtimeVersion": "python:3.13",
            "phase": "init"
        }));

        let (msg, level) = processor.create_platform_log_message(&record);
        assert_eq!(level, "INFO");
        assert!(msg.contains("INIT START"));
        assert!(msg.contains("on-demand"));
        assert!(msg.contains("python:3.13"));
        assert!(msg.contains("init"));
    }

    #[test]
    fn test_create_log_message_platform_init_runtime_done() {
        let processor = make_processor();
        let record = make_telemetry_record("platform.initRuntimeDone", serde_json::json!({
            "requestId": "req-init",
            "initializationType": "on-demand",
            "phase": "init",
            "status": "success"
        }));

        let (msg, level) = processor.create_platform_log_message(&record);
        assert_eq!(level, "INFO");
        assert!(msg.contains("INIT RUNTIME DONE"));
        assert!(msg.contains("success"));
    }

    #[test]
    fn test_create_log_message_platform_init_report() {
        let processor = make_processor();
        let record = make_telemetry_record("platform.initReport", serde_json::json!({
            "requestId": "req-init",
            "initializationType": "on-demand",
            "phase": "init",
            "metrics": { "durationMs": 456.78 }
        }));

        let (msg, level) = processor.create_platform_log_message(&record);
        assert_eq!(level, "INFO");
        assert!(msg.contains("INIT REPORT"));
        assert!(msg.contains("Duration: 456.78 ms"));
    }

    #[test]
    fn test_create_log_message_platform_start() {
        let processor = make_processor();
        let record = make_telemetry_record("platform.start", serde_json::json!({
            "requestId": "req-start"
        }));

        let (msg, level) = processor.create_platform_log_message(&record);
        assert_eq!(level, "INFO");
        assert!(msg.contains("START RequestId: req-start"));
    }

    #[test]
    fn test_create_log_message_platform_end() {
        let processor = make_processor();
        let record = make_telemetry_record("platform.end", serde_json::json!({
            "requestId": "req-end"
        }));

        let (msg, level) = processor.create_platform_log_message(&record);
        assert_eq!(level, "INFO");
        assert!(msg.contains("END RequestId: req-end"));
    }

    #[test]
    fn test_create_log_message_platform_runtime_done() {
        let processor = make_processor();
        let record = make_telemetry_record("platform.runtimeDone", serde_json::json!({
            "requestId": "req-done",
            "status": "success"
        }));

        let (msg, level) = processor.create_platform_log_message(&record);
        assert_eq!(level, "INFO");
        assert!(msg.contains("RUNTIME DONE RequestId: req-done"));
        assert!(msg.contains("Status: success"));
    }

    #[test]
    fn test_create_log_message_unknown_event_type() {
        let processor = make_processor();
        let record = make_telemetry_record("platform.newFeature", serde_json::json!({
            "requestId": "req-new",
            "data": "something"
        }));

        let (msg, level) = processor.create_platform_log_message(&record);
        assert_eq!(level, "INFO");
        assert!(msg.contains("PLATFORM EVENT"));
        assert!(msg.contains("PLATFORM.NEWFEATURE"));
    }

    #[test]
    fn test_create_log_message_missing_fields_defaults_to_unknown() {
        let processor = make_processor();
        let record = make_telemetry_record("platform.initStart", serde_json::json!({}));

        let (msg, _level) = processor.create_platform_log_message(&record);
        assert!(msg.contains("unknown")); // Missing fields default to "unknown"
    }

    // ========================================================================
    // process_invoke_event
    // ========================================================================

    #[test]
    fn test_process_invoke_event_updates_context() {
        let processor = make_processor();
        processor.process_invoke_event("new-req-id", "arn:aws:lambda:us-west-2:999:function:fn2");

        let context = processor.invocation_context.lock().expect("lock");
        assert_eq!(context.request_id, "new-req-id");
        assert_eq!(context.invoked_function_arn, "arn:aws:lambda:us-west-2:999:function:fn2");
    }

    // ========================================================================
    // extract_request_id_from_message
    // ========================================================================

    #[test]
    fn test_extract_request_id_from_message_with_tab() {
        let processor = make_processor();
        let msg = "REPORT RequestId: abc-123\tDuration: 100.00 ms";
        let result = processor.extract_request_id_from_message(msg);
        assert_eq!(result, Some("abc-123".to_string()));
    }

    #[test]
    fn test_extract_request_id_from_message_no_tab() {
        let processor = make_processor();
        let msg = "REPORT RequestId: def-456 extra-data";
        let result = processor.extract_request_id_from_message(msg);
        assert_eq!(result, Some("def-456".to_string()));
    }

    #[test]
    fn test_extract_request_id_from_message_not_report() {
        let processor = make_processor();
        let msg = "START RequestId: abc-123";
        let result = processor.extract_request_id_from_message(msg);
        assert!(result.is_none());
    }

    // ========================================================================
    // extract_log_level_from_message
    // ========================================================================

    #[test]
    fn test_extract_log_level_error() {
        let processor = make_processor();
        assert_eq!(processor.extract_log_level_from_message("Something ERROR happened"), "ERROR");
        assert_eq!(processor.extract_log_level_from_message("FAILURE detected"), "ERROR");
        assert_eq!(processor.extract_log_level_from_message("java.lang.NullPointerException"), "ERROR");
    }

    #[test]
    fn test_extract_log_level_warning() {
        let processor = make_processor();
        assert_eq!(processor.extract_log_level_from_message("WARNING: low memory"), "WARNING");
        assert_eq!(processor.extract_log_level_from_message("This is a warn message"), "WARNING");
    }

    #[test]
    fn test_extract_log_level_debug() {
        let processor = make_processor();
        assert_eq!(processor.extract_log_level_from_message("DEBUG entering function"), "DEBUG");
    }

    #[test]
    fn test_extract_log_level_trace() {
        let processor = make_processor();
        assert_eq!(processor.extract_log_level_from_message("TRACE detailed output"), "TRACE");
    }

    #[test]
    fn test_extract_log_level_info_default() {
        let processor = make_processor();
        assert_eq!(processor.extract_log_level_from_message("Normal processing complete"), "INFO");
    }

    // ========================================================================
    // PlatformProcessor construction and Debug
    // ========================================================================

    #[test]
    fn test_platform_processor_new_and_debug() {
        let processor = make_processor();
        let debug_str = format!("{processor:?}");
        assert!(debug_str.contains("PlatformProcessor"));
    }

    // ========================================================================
    // Flush trait
    // ========================================================================

    #[tokio::test]
    async fn test_platform_processor_flush_returns_ok() {
        use crate::newrelic::flush::Flush;
        let processor = make_processor();
        let result = processor.flush().await;
        assert!(result.is_ok(), "Flush should always return Ok");
    }
}
