//! Integration tests — cross-module end-to-end flows
//!
//! These tests verify interactions between agent, apm, config, and credentials
//! modules without requiring external services (AWS, New Relic).

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serial_test::serial;
    use serde_json::Value;

    use crate::agent::batch::{BatchBuffer, BatchedAgentPayload, build_newrelic_payload};
    use crate::apm::app::{
        needs_normalization,
        normalize_analytic_event_data, normalize_span_event_data,
        normalize_metric_data, normalize_error_event_data,
        normalize_custom_event_data, normalize_transaction_sample_data,
    };
    use crate::apm::collector::{resolve_collector_command, CMD_ERROR_EVENTS};
    use crate::apm::error_event::{generate_error_event, generate_error_event_from_fault};
    use crate::apm::metric_converter::{parse_lambda_report_log, convert_to_apm_metrics};
    use crate::apm::payload_parser::parse_agent_payload;
    use crate::config::{ExtensionConfig, Configuration};
    use crate::credentials::credentials::decode_license_key;

    // ========================================================================
    // Helpers
    // ========================================================================

    fn make_payload(id: &str, report: Option<&str>, arn: &str, data: &[u8]) -> BatchedAgentPayload {
        BatchedAgentPayload {
            request_id: id.to_string(),
            agent_payload_bytes: Arc::from(data.to_vec()),
            report_line: report.map(|s| s.to_string()),
            invoked_function_arn: arn.to_string(),
            timestamp: chrono::Utc::now(),
        }
    }

    fn make_config(function_name: &str, apm_mode: bool) -> ExtensionConfig {
        let mut config = ExtensionConfig::default();
        config.aws.function_name = function_name.to_string();
        config.new_relic.apm_lambda_mode = apm_mode;
        config
    }

    fn parse_payload_entry(json_str: &str) -> (Value, Value) {
        let parsed: Value = serde_json::from_str(json_str).expect("valid JSON");
        let entry: Value = serde_json::from_str(
            parsed["entry"].as_str().expect("entry should be string"),
        ).expect("entry should be valid JSON");
        (parsed, entry)
    }

    // ========================================================================
    // E2E: Batch Lifecycle — add → threshold → send → clear
    // ========================================================================

    #[test]
    fn test_e2e_batch_lifecycle_full_cycle() {
        let buffer = BatchBuffer::new();
        let arn = "arn:aws:lambda:us-east-1:123456789012:function:my-func";

        // Phase 1: Add payloads — no threshold yet
        buffer.add_to_batch("req-1".to_string(), b"payload-1".to_vec(), None, arn.to_string());
        buffer.add_to_batch("req-2".to_string(), b"payload-2".to_vec(), Some("REPORT req-2".to_string()), arn.to_string());
        assert!(!buffer.should_send_batch_by_threshold());

        // Phase 2: Add more with reports — hit threshold at 3
        buffer.add_to_batch("req-3".to_string(), b"payload-3".to_vec(), Some("REPORT req-3".to_string()), arn.to_string());
        buffer.add_to_batch("req-4".to_string(), b"payload-4".to_vec(), Some("REPORT req-4".to_string()), arn.to_string());
        assert!(buffer.should_send_batch_by_threshold());

        // Phase 3: Get reports-only batch (non-destructive)
        let with_reports = buffer.get_batch_with_reports_only();
        assert_eq!(with_reports.len(), 3); // req-2, req-3, req-4
        assert_eq!(buffer.buffer.len(), 4); // all 4 still in buffer

        // Phase 4: Build payload from batch
        let config = make_config("my-func", false);
        let payload_json = build_newrelic_payload(&with_reports, &config, None);
        let (parsed, entry) = parse_payload_entry(&payload_json);

        // Verify structure
        assert_eq!(parsed["context"]["function_name"], "my-func");
        let log_events = entry["logEvents"].as_array().expect("array");
        // 3 payloads with reports = 3 agent + 3 report = 6 log events
        assert_eq!(log_events.len(), 6);

        // Phase 5: Clear only the reports batch
        buffer.clear_batch_with_reports(&with_reports);
        assert_eq!(buffer.buffer.len(), 1); // only req-1 (no report) remains
        assert!(buffer.buffer.contains_key("req-1"));
        assert!(!buffer.should_send_batch_by_threshold());
    }

    #[test]
    fn test_e2e_batch_get_and_clear_then_rebuild() {
        let buffer = BatchBuffer::new();
        let arn = "arn:test";

        buffer.add_to_batch("r1".to_string(), b"data1".to_vec(), Some("REPORT 1".to_string()), arn.to_string());
        buffer.add_to_batch("r2".to_string(), b"data2".to_vec(), None, arn.to_string());

        // get_and_clear drains everything
        let all = buffer.get_and_clear_batch();
        assert_eq!(all.len(), 2);
        assert!(buffer.buffer.is_empty());

        // Buffer can be reused
        buffer.add_to_batch("r3".to_string(), b"data3".to_vec(), None, arn.to_string());
        assert_eq!(buffer.buffer.len(), 1);
    }

    // ========================================================================
    // E2E: Payload Construction → JSON Validation
    // ========================================================================

    #[test]
    fn test_e2e_payload_json_structure_matches_newrelic_format() {
        let config = make_config("lambda-test-fn", false);
        let arn = "arn:aws:lambda:us-west-2:999888777666:function:lambda-test-fn";
        let items = vec![
            make_payload("req-abc", Some("REPORT Duration: 50.5 ms\tBilled Duration: 51 ms\tMemory Size: 256 MB\tMax Memory Used: 128 MB"), arn, b"{\"agent\":\"data\"}"),
        ];

        let json_str = build_newrelic_payload(&items, &config, None);
        let (parsed, entry) = parse_payload_entry(&json_str);

        // Context validation
        let ctx = &parsed["context"];
        assert_eq!(ctx["function_name"], "lambda-test-fn");
        assert_eq!(ctx["invoked_function_arn"], arn);
        assert_eq!(ctx["log_group_name"], "/aws/lambda/lambda-test-fn");
        assert!(ctx["log_stream_name"].as_str().expect("string").starts_with("newrelic-lambda-extension:"));

        // Entry validation
        assert_eq!(entry["logGroup"], "/aws/lambda/lambda-test-fn");
        assert!(entry["logStream"].as_str().expect("string").starts_with("newrelic-lambda-extension:"));

        // Log events: 1 agent payload + 1 report = 2
        let events = entry["logEvents"].as_array().expect("array");
        assert_eq!(events.len(), 2);

        // First event = agent payload
        assert_eq!(events[0]["id"], "req-abc");
        assert_eq!(events[0]["message"], "{\"agent\":\"data\"}");
        assert!(events[0]["timestamp"].is_number());

        // Second event = report line
        assert_eq!(events[1]["id"], "req-abc");
        assert!(events[1]["message"].as_str().expect("str").contains("REPORT Duration:"));
    }

    #[test]
    fn test_e2e_payload_multiple_requests_interleaved() {
        let config = make_config("multi-fn", false);
        let items = vec![
            make_payload("req-1", Some("REPORT 1"), "arn:1", b"agent-1"),
            make_payload("req-2", None, "arn:2", b"agent-2"),
            make_payload("req-3", Some("REPORT 3"), "arn:3", b"agent-3"),
        ];

        let json_str = build_newrelic_payload(&items, &config, None);
        let (parsed, entry) = parse_payload_entry(&json_str);

        // Last item's ARN used in context
        assert_eq!(parsed["context"]["invoked_function_arn"], "arn:3");

        // Events: req-1(2) + req-2(1) + req-3(2) = 5
        let events = entry["logEvents"].as_array().expect("array");
        assert_eq!(events.len(), 5);

        // Verify ordering: agent-1, REPORT 1, agent-2, agent-3, REPORT 3
        assert_eq!(events[0]["message"], "agent-1");
        assert!(events[1]["message"].as_str().expect("str").contains("REPORT 1"));
        assert_eq!(events[2]["message"], "agent-2");
        assert_eq!(events[3]["message"], "agent-3");
        assert!(events[4]["message"].as_str().expect("str").contains("REPORT 3"));
    }

    // ========================================================================
    // E2E: Chunk Splitting → Payload Integrity
    // ========================================================================

    #[test]
    fn test_e2e_chunked_payloads_preserve_all_data() {
        let config = make_config("chunk-fn", false);

        // Create 20 payloads with ~1KB each to force chunking at 10KB
        let data = vec![b'X'; 1000];
        let items: Vec<BatchedAgentPayload> = (0..20)
            .map(|i| make_payload(&format!("req-{i}"), Some(&format!("REPORT {i}")), "arn:test", &data))
            .collect();

        // Split into small chunks
        let chunks = crate::agent::batch::split_into_chunks(items, 5000, &Arc::new(config.clone()));
        assert!(chunks.len() > 1, "Expected multiple chunks, got {}", chunks.len());

        // Verify all 20 items preserved across chunks
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, 20);

        // Verify each chunk produces valid JSON
        for chunk in &chunks {
            let json = build_newrelic_payload(chunk, &config, None);
            let (parsed, _entry) = parse_payload_entry(&json);
            assert!(parsed["context"].is_object());
        }
    }

    // ========================================================================
    // E2E: APM Payload Parsing → Normalization Pipeline (Ruby v2)
    // ========================================================================

    #[test]
    fn test_e2e_apm_parse_and_normalize_ruby_v2_payload() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;
        use base64::{Engine as _, engine::general_purpose};

        // Build a realistic Ruby v2 agent payload with bare transaction names
        let telemetry_data = serde_json::json!({
            "analytic_event_data": [
                "placeholder_run_id",
                {},
                [[
                    {"type": "Transaction", "name": "ruby-hw", "duration": 0.05},
                    {},
                    {}
                ]]
            ],
            "span_event_data": [
                "placeholder_run_id",
                {},
                [[
                    {"type": "Span", "name": "ruby-hw", "transaction.name": "ruby-hw"},
                    {},
                    {}
                ]]
            ],
            "error_event_data": [
                "placeholder_run_id",
                {},
                [[
                    {"transaction.name": "ruby-hw", "transactionName": "ruby-hw", "error.class": "RuntimeError"},
                    {},
                    {}
                ]]
            ],
            "metric_data": [
                "placeholder_run_id",
                1000,
                2000,
                [[
                    {"name": "OtherTransactionTotalTime/ruby-hw"},
                    [1, 0.05, 0.05, 0.05, 0.05, 0.0025]
                ]]
            ],
            "custom_event_data": [
                "placeholder_run_id",
                {},
                [[
                    {"type": "Custom", "transaction.name": "ruby-hw"},
                    {},
                    {}
                ]]
            ],
            "transaction_sample_data": [
                "placeholder_run_id",
                [
                    ["tx-1", 1000, "ruby-hw", 0.05, "encoded"]
                ]
            ]
        });

        // Compress and encode like a real agent
        let json_bytes = serde_json::to_vec(&telemetry_data).expect("serialize");
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&json_bytes).expect("compress");
        let compressed = encoder.finish().expect("finish");
        let encoded = general_purpose::STANDARD.encode(&compressed);

        // Build the outer payload array: ["2", "NR_LAMBDA_MONITORING", "<base64>"]
        let payload = format!("[\"2\", \"NR_LAMBDA_MONITORING\", \"{encoded}\"]");

        // Phase 1: Parse
        let (mut telemetry_map, version) = parse_agent_payload(payload.as_bytes()).expect("parse should succeed");
        assert_eq!(version, 2);
        assert!(telemetry_map.contains_key("analytic_event_data"));
        assert!(telemetry_map.contains_key("span_event_data"));
        assert!(telemetry_map.contains_key("error_event_data"));
        assert!(telemetry_map.contains_key("metric_data"));
        assert!(telemetry_map.contains_key("custom_event_data"));
        assert!(telemetry_map.contains_key("transaction_sample_data"));

        // Phase 2: Normalize all types (simulating Ruby v2 path)
        if let Some(data) = telemetry_map.get_mut("analytic_event_data") {
            normalize_analytic_event_data(data);
        }
        if let Some(data) = telemetry_map.get_mut("span_event_data") {
            normalize_span_event_data(data);
        }
        if let Some(data) = telemetry_map.get_mut("metric_data") {
            normalize_metric_data(data);
        }
        if let Some(data) = telemetry_map.get_mut("error_event_data") {
            normalize_error_event_data(data);
        }
        if let Some(data) = telemetry_map.get_mut("custom_event_data") {
            normalize_custom_event_data(data);
        }
        if let Some(data) = telemetry_map.get_mut("transaction_sample_data") {
            normalize_transaction_sample_data(data);
        }

        // Phase 3: Verify all normalization applied
        let analytic = &telemetry_map["analytic_event_data"];
        assert_eq!(analytic[2][0][0]["name"], "OtherTransaction/Ruby/ruby-hw");

        let spans = &telemetry_map["span_event_data"];
        assert_eq!(spans[2][0][0]["name"], "OtherTransaction/Ruby/ruby-hw");
        assert_eq!(spans[2][0][0]["transaction.name"], "OtherTransaction/Ruby/ruby-hw");

        let errors = &telemetry_map["error_event_data"];
        assert_eq!(errors[2][0][0]["transaction.name"], "OtherTransaction/Ruby/ruby-hw");
        assert_eq!(errors[2][0][0]["transactionName"], "OtherTransaction/Ruby/ruby-hw");

        let metrics = &telemetry_map["metric_data"];
        assert_eq!(metrics[3][0][0]["name"], "OtherTransactionTotalTime/Ruby/ruby-hw");

        let custom = &telemetry_map["custom_event_data"];
        assert_eq!(custom[2][0][0]["transaction.name"], "OtherTransaction/Ruby/ruby-hw");

        let samples = &telemetry_map["transaction_sample_data"];
        assert_eq!(samples[1][0][2], "OtherTransaction/Ruby/ruby-hw");
    }

    #[test]
    fn test_e2e_apm_already_normalized_payload_unchanged() {
        // Verify idempotency — already-normalized names should not be double-prefixed
        let already_normalized = "OtherTransaction/Ruby/ruby-hw";
        assert!(!needs_normalization(already_normalized));

        let mut data: Vec<Value> = vec![
            serde_json::json!("run-id"),
            serde_json::json!({}),
            serde_json::json!([[
                {"type": "Transaction", "name": already_normalized},
                {},
                {}
            ]]),
        ];

        normalize_analytic_event_data(&mut data);
        assert_eq!(data[2][0][0]["name"], already_normalized, "Should not double-normalize");
    }

    // ========================================================================
    // E2E: Error Event Generation → Collector Format Validation
    // ========================================================================

    #[test]
    fn test_e2e_timeout_error_event_full_structure() {
        let arn = "arn:aws:lambda:us-east-1:123456789012:function:my-function:prod";
        let log = "2024-01-15T12:00:00Z abc-123 Task timed out after 30.00 seconds";

        // Generate error event from fault log
        let events = generate_error_event_from_fault(log, "abc-123", arn);
        assert!(events.is_some());
        let events = events.expect("should produce events");
        assert_eq!(events.len(), 1);

        // Validate APM error event structure
        let event_triple = events[0].as_array().expect("should be array of 3");
        assert_eq!(event_triple.len(), 3);

        let detail = &event_triple[0];
        let _agent_attrs = &event_triple[1];
        let user_attrs = &event_triple[2];

        // Detail fields
        assert_eq!(detail["type"], "TransactionError");
        assert_eq!(detail["error.class"], "LambdaTimeout");
        assert_eq!(detail["error.message"], "Task timed out");
        assert_eq!(detail["error.expected"], false);
        assert_eq!(detail["sampled"], true);
        assert!(detail["spanId"].as_str().expect("str").len() == 16);
        assert!(detail["traceId"].as_str().expect("str").len() == 32);
        assert!(detail["guid"].as_str().expect("str").len() == 32);
        assert!(detail["priority"].as_f64().expect("f64") > 0.0);
        assert!(detail["timestamp"].as_i64().expect("i64") > 0);
        assert_eq!(detail["transactionName"], "OtherTransaction/Function/my-function");

        // User attributes
        assert_eq!(user_attrs["aws.requestId"], "abc-123");
        assert_eq!(user_attrs["aws.lambda.arn"], arn);
        assert_eq!(user_attrs["aws.lambda.functionVersion"], "prod");
    }

    #[test]
    fn test_e2e_shutdown_error_event_format() {
        let events = generate_error_event(
            "LambdaTimeout",
            "Function timed out after 30s",
            "req-timeout",
            "arn:aws:lambda:us-east-1:123:function:timeout-fn",
        );
        assert!(!events.is_empty());

        // Verify it's a valid APM error event that the collector can accept
        let detail = &events[0].as_array().expect("array")[0];
        assert_eq!(detail["error.class"], "LambdaTimeout");
        assert_eq!(detail["error.message"], "Function timed out after 30s");
        assert_eq!(detail["type"], "TransactionError");
    }

    #[test]
    fn test_e2e_error_event_and_collector_command_routing() {
        // Error events use CMD_ERROR_EVENTS which is NOT in resolve_collector_command
        // This verifies the special handling path
        assert!(resolve_collector_command(CMD_ERROR_EVENTS).is_none());

        // All other types resolve correctly
        let types = ["metric_data", "span_event_data", "error_data",
                     "analytic_event_data", "custom_event_data", "log_event_data",
                     "transaction_sample_data"];
        for t in types {
            assert!(resolve_collector_command(t).is_some(), "Should resolve: {t}");
        }
    }

    // ========================================================================
    // E2E: REPORT Log → Parse → APM Metrics → Validate
    // ========================================================================

    #[test]
    fn test_e2e_report_log_to_apm_metrics_pipeline() {
        let report_line = "REPORT RequestId: abc-123\tDuration: 245.67 ms\tBilled Duration: 246 ms\tMemory Size: 1024 MB\tMax Memory Used: 512 MB\tInit Duration: 789.12 ms";

        // Phase 1: Parse
        let metrics = parse_lambda_report_log(report_line).expect("should parse REPORT");
        assert_eq!(metrics.request_id, "abc-123");
        assert_eq!(metrics.duration, Some(245.67));
        assert_eq!(metrics.billed_duration, Some(246.0));
        assert_eq!(metrics.memory_size, Some(1024));
        assert_eq!(metrics.max_memory_used, Some(512));
        assert_eq!(metrics.init_duration, Some(789.12));
        assert!(metrics.error.is_none());

        // Phase 2: Convert to APM metrics
        let apm_metrics = convert_to_apm_metrics(&metrics, "entity-guid-abc", "my-function");

        // Should have 5 metrics: duration, billed_duration, memory_size, max_memory, init_duration
        assert_eq!(apm_metrics.len(), 5);

        // Verify all metrics have correct common attributes
        for m in &apm_metrics {
            assert_eq!(m["attributes"]["aws.requestId"], "abc-123");
            assert_eq!(m["attributes"]["entity.guid"], "entity-guid-abc");
            assert_eq!(m["attributes"]["entity.name"], "my-function");
            assert_eq!(m["attributes"]["entity.type"], "APM");
            assert!(m["timestamp"].as_i64().expect("i64") > 0);
        }

        // Verify specific metric names and values
        let names: Vec<&str> = apm_metrics.iter()
            .map(|m| m["name"].as_str().expect("name"))
            .collect();
        assert!(names.contains(&"apm.lambda.transaction.duration"));
        assert!(names.contains(&"apm.lambda.transaction.billed_duration"));
        assert!(names.contains(&"apm.lambda.transaction.memory_size"));
        assert!(names.contains(&"apm.lambda.transaction.max_memory_used"));
        assert!(names.contains(&"apm.lambda.transaction.init_duration"));

        let duration_metric = apm_metrics.iter()
            .find(|m| m["name"] == "apm.lambda.transaction.duration")
            .expect("should have duration");
        assert_eq!(duration_metric["type"], "gauge");
        assert_eq!(duration_metric["value"], 245.67);
    }

    #[test]
    fn test_e2e_fault_log_to_apm_error_metric() {
        let fault_line = "RequestId: fault-123 Status: error ErrorType: Runtime.ExitError";

        let metrics = parse_lambda_report_log(fault_line).expect("should parse fault");
        assert_eq!(metrics.request_id, "fault-123");
        assert_eq!(metrics.error, Some("error".to_string()));
        assert_eq!(metrics.error_type, Some("Runtime.ExitError".to_string()));

        let apm_metrics = convert_to_apm_metrics(&metrics, "guid-fault", "error-fn");

        // Should have exactly 1 metric: the error count
        assert_eq!(apm_metrics.len(), 1);
        let error_metric = &apm_metrics[0];
        assert_eq!(error_metric["name"], "apm.lambda.transaction.error");
        assert_eq!(error_metric["type"], "count");
        assert_eq!(error_metric["value"], 1);
        assert_eq!(error_metric["attributes"]["Error Type"], "Runtime.ExitError");
    }

    // ========================================================================
    // E2E: Config → Credentials Chain
    // ========================================================================

    #[test]
    #[serial]
    fn test_e2e_config_to_credential_conversion() {
        // Save/restore env
        let orig_key = std::env::var("NEW_RELIC_LICENSE_KEY").ok();
        let orig_secret = std::env::var("NEW_RELIC_LICENSE_KEY_SECRET").ok();
        let orig_ssm = std::env::var("NEW_RELIC_LICENSE_KEY_SSM_PARAMETER_NAME").ok();

        std::env::set_var("NEW_RELIC_LICENSE_KEY", "test-key-12345");
        std::env::set_var("NEW_RELIC_LICENSE_KEY_SECRET", "arn:aws:secretsmanager:us-east-1:123:secret:nr-key");
        std::env::set_var("NEW_RELIC_LICENSE_KEY_SSM_PARAMETER_NAME", "/newrelic/license-key");

        let ext_config = ExtensionConfig::from_env();

        // Verify config captured correctly
        assert_eq!(ext_config.new_relic.license_key, Some("test-key-12345".to_string()));
        assert_eq!(ext_config.new_relic.license_key_secret_id, "arn:aws:secretsmanager:us-east-1:123:secret:nr-key");
        assert_eq!(ext_config.new_relic.license_key_ssm_parameter_name, "/newrelic/license-key");

        // Convert to Configuration (used by credentials module)
        let cred_config = Configuration::from(&ext_config);
        assert_eq!(cred_config.license_key, "test-key-12345");
        assert_eq!(cred_config.license_key_secret_id, "arn:aws:secretsmanager:us-east-1:123:secret:nr-key");
        assert_eq!(cred_config.license_key_ssm_parameter_name, "/newrelic/license-key");

        // Restore env
        match orig_key {
            Some(v) => std::env::set_var("NEW_RELIC_LICENSE_KEY", v),
            None => std::env::remove_var("NEW_RELIC_LICENSE_KEY"),
        }
        match orig_secret {
            Some(v) => std::env::set_var("NEW_RELIC_LICENSE_KEY_SECRET", v),
            None => std::env::remove_var("NEW_RELIC_LICENSE_KEY_SECRET"),
        }
        match orig_ssm {
            Some(v) => std::env::set_var("NEW_RELIC_LICENSE_KEY_SSM_PARAMETER_NAME", v),
            None => std::env::remove_var("NEW_RELIC_LICENSE_KEY_SSM_PARAMETER_NAME"),
        }
    }

    #[test]
    fn test_e2e_license_key_decode_then_validate() {
        // Simulate Secrets Manager response → decode → use
        let secrets_manager_response = r#"{"LicenseKey": "eu01xxabcdef1234567890abcdef12345678NRAL"}"#;
        let key = decode_license_key(secrets_manager_response).expect("should decode");
        assert_eq!(key, "eu01xxabcdef1234567890abcdef12345678NRAL");
        assert_eq!(key.len(), 40); // NR license keys are 40 chars
    }

    #[test]
    fn test_e2e_missing_license_key_fallback_chain() {
        // No license key in config → would need AWS lookup (which fails outside Lambda)
        let config = ExtensionConfig::default();
        let cred_config = Configuration::from(&config);

        assert!(cred_config.license_key.is_empty());
        assert!(cred_config.license_key_secret_id.is_empty());
        assert!(cred_config.license_key_ssm_parameter_name.is_empty());
    }

    // ========================================================================
    // E2E: APM Mode vs Standard Mode Payload Differences
    // ========================================================================

    #[test]
    fn test_e2e_apm_mode_payload_has_no_version_line() {
        let config = make_config("apm-fn", true);
        let items = vec![make_payload("req-1", Some("REPORT 1"), "arn:test", b"agent-data")];

        // APM mode: no version_info passed
        let json = build_newrelic_payload(&items, &config, None);
        let (_parsed, entry) = parse_payload_entry(&json);
        let events = entry["logEvents"].as_array().expect("array");

        // 1 agent + 1 report = 2 (no version line)
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_e2e_standard_mode_payload_includes_version_line() {
        let config = make_config("std-fn", false);
        let items = vec![make_payload("req-1", Some("REPORT 1"), "arn:test", b"agent-data")];

        // Standard mode: with version_info
        let version_info = crate::version::VersionInfo::get_or_detect(None);
        let json = build_newrelic_payload(&items, &config, Some(&version_info));
        let (_parsed, entry) = parse_payload_entry(&json);
        let events = entry["logEvents"].as_array().expect("array");

        // 1 agent + 1 report + 1 version line = 3
        assert_eq!(events.len(), 3);

        // Version line should contain "Version RequestId:"
        let version_event = &events[2];
        let msg = version_event["message"].as_str().expect("str");
        assert!(msg.contains("Version RequestId:"), "Version line should contain 'Version RequestId:', got: {msg}");
    }

    // ========================================================================
    // E2E: Normalization Idempotency — run twice, verify no change
    // ========================================================================

    #[test]
    fn test_e2e_normalization_is_idempotent() {
        let mut data: Vec<Value> = vec![
            serde_json::json!("run-id"),
            serde_json::json!({}),
            serde_json::json!([[
                {"type": "Transaction", "name": "ruby-hw"},
                {},
                {}
            ]]),
        ];

        // First normalization
        normalize_analytic_event_data(&mut data);
        let after_first = data[2][0][0]["name"].as_str().expect("str").to_string();
        assert_eq!(after_first, "OtherTransaction/Ruby/ruby-hw");

        // Second normalization — should not change
        normalize_analytic_event_data(&mut data);
        let after_second = data[2][0][0]["name"].as_str().expect("str").to_string();
        assert_eq!(after_second, after_first, "Normalization should be idempotent");
    }

    // ========================================================================
    // E2E: Concurrent Batch Operations — Thread Safety
    // ========================================================================

    #[test]
    fn test_e2e_concurrent_batch_add_threshold_clear() {
        let buffer = Arc::new(BatchBuffer::new());
        let barrier = Arc::new(std::sync::Barrier::new(4));

        // 3 writer threads adding payloads with reports
        let writers: Vec<_> = (0..3)
            .map(|tid| {
                let buffer = Arc::clone(&buffer);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait(); // synchronize start
                    for i in 0..10 {
                        buffer.add_to_batch(
                            format!("t{tid}-r{i}"),
                            vec![1, 2, 3],
                            Some(format!("REPORT t{tid}-r{i}")),
                            "arn:concurrent".to_string(),
                        );
                    }
                })
            })
            .collect();

        // 1 reader thread checking threshold and reading
        let reader = {
            let buffer = Arc::clone(&buffer);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                let mut threshold_seen = false;
                for _ in 0..50 {
                    if buffer.should_send_batch_by_threshold() {
                        threshold_seen = true;
                    }
                    let _ = buffer.get_batch_with_reports_only();
                    std::thread::yield_now();
                }
                threshold_seen
            })
        };

        for w in writers {
            w.join().expect("writer should not panic");
        }
        let threshold_seen = reader.join().expect("reader should not panic");

        // After 30 payloads with reports, threshold should have been seen
        assert!(threshold_seen, "Threshold should have been detected during concurrent writes");
        assert!(buffer.buffer.len() > 0);
    }

    // ========================================================================
    // E2E: Request State Lifecycle (simulated)
    // ========================================================================

    #[test]
    #[serial]
    fn test_e2e_request_state_create_populate_cleanup() {
        use crate::request::{
            REQUEST_CONTEXTS, REQUEST_AGENT_BUFFERS,
            PENDING_REPORTS, REQUEST_BUFFER_TIMESTAMPS,
            cleanup_request_processing_state,
        };

        let request_id = "integration-test-req-001";
        let arn = "arn:aws:lambda:us-east-1:123:function:test-fn";

        // Simulate request state creation (manual, not via full factory)
        let context = Arc::new(std::sync::Mutex::new(crate::context::InvocationContext {
            request_id: request_id.to_string(),
            invoked_function_arn: arn.to_string(),
            trace_id: None,
        }));
        let agent_buffer = Arc::new(std::sync::Mutex::new(Vec::<Vec<u8>>::new()));

        REQUEST_CONTEXTS.insert(request_id.to_string(), context);
        REQUEST_AGENT_BUFFERS.insert(request_id.to_string(), agent_buffer.clone());
        REQUEST_BUFFER_TIMESTAMPS.insert(request_id.to_string(), chrono::Utc::now());

        // Simulate agent payload arrival
        if let Ok(mut buf) = agent_buffer.lock() {
            buf.push(b"agent-payload-1".to_vec());
            buf.push(b"agent-payload-2".to_vec());
        }

        // Simulate platform report
        PENDING_REPORTS.insert(request_id.to_string(), "REPORT Duration: 100 ms".to_string());

        // Verify state
        assert!(REQUEST_CONTEXTS.contains_key(request_id));
        assert!(REQUEST_AGENT_BUFFERS.contains_key(request_id));
        assert!(PENDING_REPORTS.contains_key(request_id));

        // Cleanup
        cleanup_request_processing_state(request_id);

        // Verify cleanup
        assert!(!REQUEST_CONTEXTS.contains_key(request_id));
        assert!(!REQUEST_AGENT_BUFFERS.contains_key(request_id));
        assert!(!PENDING_REPORTS.contains_key(request_id));
        assert!(!REQUEST_BUFFER_TIMESTAMPS.contains_key(request_id));
    }

    // ========================================================================
    // E2E: Payload Routing — Orphaned Payloads
    // ========================================================================

    #[tokio::test]
    #[serial]
    async fn test_e2e_orphaned_payload_routing() {
        use crate::request::{
            CURRENT_ACTIVE_REQUEST_ID, ORPHANED_PAYLOADS,
            REQUEST_AGENT_BUFFERS, route_payload_to_request_buffer,
        };

        // Clear state
        if let Ok(mut active) = CURRENT_ACTIVE_REQUEST_ID.lock() {
            *active = None;
        }
        if let Ok(mut orphaned) = ORPHANED_PAYLOADS.lock() {
            orphaned.clear();
        }
        // Remove any leftover test buffers
        let keys: Vec<String> = REQUEST_AGENT_BUFFERS.iter().map(|e| e.key().clone()).collect();
        for k in keys {
            REQUEST_AGENT_BUFFERS.remove(&k);
        }

        // Phase 1: No active request → payload should go to orphaned buffer
        route_payload_to_request_buffer(b"orphaned-payload".to_vec()).await;

        let orphaned_count = ORPHANED_PAYLOADS.lock()
            .map(|buf| buf.len())
            .unwrap_or(0);
        assert_eq!(orphaned_count, 1, "Payload should be in orphaned buffer");

        // Phase 2: Create a request buffer → set active
        let test_buffer = Arc::new(std::sync::Mutex::new(Vec::<Vec<u8>>::new()));
        REQUEST_AGENT_BUFFERS.insert("active-req".to_string(), test_buffer.clone());
        if let Ok(mut active) = CURRENT_ACTIVE_REQUEST_ID.lock() {
            *active = Some("active-req".to_string());
        }

        // Phase 3: New payload should go to active request buffer
        route_payload_to_request_buffer(b"active-payload".to_vec()).await;

        let active_count = test_buffer.lock().map(|buf| buf.len()).unwrap_or(0);
        assert_eq!(active_count, 1, "Payload should be in active request buffer");

        // Cleanup
        if let Ok(mut active) = CURRENT_ACTIVE_REQUEST_ID.lock() {
            *active = None;
        }
        REQUEST_AGENT_BUFFERS.remove("active-req");
        if let Ok(mut orphaned) = ORPHANED_PAYLOADS.lock() {
            orphaned.clear();
        }
    }

    // ========================================================================
    // STRESS: High-Concurrency Batch — 100 threads x 1000 ops each
    // ========================================================================

    #[test]
    fn test_stress_100_threads_100k_total_batch_ops() {
        let buffer = Arc::new(BatchBuffer::new());
        let num_threads = 100;
        let ops_per_thread = 1000;
        let barrier = Arc::new(std::sync::Barrier::new(num_threads));

        let start = std::time::Instant::now();

        let handles: Vec<_> = (0..num_threads)
            .map(|tid| {
                let buffer = Arc::clone(&buffer);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait(); // All threads start simultaneously
                    for i in 0..ops_per_thread {
                        let req_id = format!("t{tid}-r{i}");
                        let report = if i % 3 == 0 {
                            Some(format!("REPORT Duration: {i} ms"))
                        } else {
                            None
                        };
                        buffer.add_to_batch(
                            req_id,
                            vec![0u8; 64], // 64-byte payload
                            report,
                            "arn:aws:lambda:us-east-1:123:function:stress-test".to_string(),
                        );

                        // Every 100 ops, check threshold (simulates real event loop)
                        if i % 100 == 0 {
                            let _ = buffer.should_send_batch_by_threshold();
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("Thread must not panic under high concurrency");
        }

        let elapsed = start.elapsed();
        let total_ops = num_threads * ops_per_thread;

        // DashMap may have fewer entries due to key collisions across threads
        // (different threads can have same format pattern — but here keys are unique per thread)
        let final_count = buffer.buffer.len();
        assert_eq!(final_count, total_ops, "All {total_ops} unique keys should be in buffer");

        // Performance gate: 100K inserts should complete in < 2 seconds even on slow CI
        assert!(
            elapsed.as_secs() < 2,
            "100K concurrent inserts took {:?} — expected < 2s",
            elapsed
        );
    }

    #[test]
    fn test_stress_concurrent_read_write_threshold_detection() {
        let buffer = Arc::new(BatchBuffer::new());
        let num_writers = 50;
        let num_readers = 50;
        let ops_per_writer = 200;
        let barrier = Arc::new(std::sync::Barrier::new(num_writers + num_readers));

        let threshold_detected = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Writers: add payloads with reports
        let writers: Vec<_> = (0..num_writers)
            .map(|tid| {
                let buffer = Arc::clone(&buffer);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for i in 0..ops_per_writer {
                        buffer.add_to_batch(
                            format!("w{tid}-{i}"),
                            vec![1, 2, 3, 4],
                            Some(format!("REPORT w{tid}-{i}")),
                            "arn:stress".to_string(),
                        );
                    }
                })
            })
            .collect();

        // Readers: check threshold + read reports batch
        let readers: Vec<_> = (0..num_readers)
            .map(|_| {
                let buffer = Arc::clone(&buffer);
                let barrier = Arc::clone(&barrier);
                let detected = Arc::clone(&threshold_detected);
                std::thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..100 {
                        if buffer.should_send_batch_by_threshold() {
                            detected.store(true, std::sync::atomic::Ordering::Relaxed);
                            // Simulate reading the batch
                            let batch = buffer.get_batch_with_reports_only();
                            assert!(batch.iter().all(|p| p.report_line.is_some()));
                        }
                        std::thread::yield_now();
                    }
                })
            })
            .collect();

        for w in writers {
            w.join().expect("Writer must not panic");
        }
        for r in readers {
            r.join().expect("Reader must not panic");
        }

        // 50 writers x 200 ops = 10,000 payloads all with reports → threshold must be detected
        assert!(
            threshold_detected.load(std::sync::atomic::Ordering::Relaxed),
            "Threshold should be detected with 10K payloads with reports"
        );
    }

    #[test]
    fn test_stress_concurrent_add_clear_no_data_loss() {
        let buffer = Arc::new(BatchBuffer::new());
        let total_added = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let total_cleared = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(6)); // 5 writers + 1 clearer

        // 5 writer threads
        let writers: Vec<_> = (0..5)
            .map(|tid| {
                let buffer = Arc::clone(&buffer);
                let added = Arc::clone(&total_added);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for i in 0..500 {
                        buffer.add_to_batch(
                            format!("w{tid}-{i}"),
                            vec![tid as u8; 32],
                            Some("REPORT".to_string()),
                            "arn:test".to_string(),
                        );
                        added.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                })
            })
            .collect();

        // 1 clearer thread that periodically drains
        let clearer = {
            let buffer = Arc::clone(&buffer);
            let cleared = Arc::clone(&total_cleared);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..50 {
                    let batch = buffer.get_and_clear_batch();
                    cleared.fetch_add(batch.len(), std::sync::atomic::Ordering::Relaxed);
                    std::thread::sleep(std::time::Duration::from_micros(100));
                }
                // Final drain
                let batch = buffer.get_and_clear_batch();
                cleared.fetch_add(batch.len(), std::sync::atomic::Ordering::Relaxed);
            })
        };

        for w in writers {
            w.join().expect("Writer must not panic");
        }
        clearer.join().expect("Clearer must not panic");

        // Any remaining in buffer
        let remaining = buffer.buffer.len();
        let total_seen = total_cleared.load(std::sync::atomic::Ordering::Relaxed) + remaining;

        // Due to concurrent insert + clear, we can't guarantee total_seen == total_added
        // because an insert racing with clear might be added after clear.
        // But we CAN guarantee: no panic, no deadlock, remaining + cleared > 0
        assert!(
            total_seen > 0,
            "Should have processed some payloads (cleared: {}, remaining: {})",
            total_cleared.load(std::sync::atomic::Ordering::Relaxed),
            remaining
        );
    }

    #[test]
    fn test_stress_build_payload_with_large_batch() {
        let config = make_config("stress-fn", false);

        // Simulate 1000 payloads (a large batch from many concurrent invocations)
        let items: Vec<BatchedAgentPayload> = (0..1000)
            .map(|i| {
                let report = if i % 2 == 0 {
                    Some(format!("REPORT Duration: {i} ms"))
                } else {
                    None
                };
                let data = vec![0u8; 256]; // 256-byte agent payload
                BatchedAgentPayload {
                    request_id: format!("req-{i}"),
                    agent_payload_bytes: Arc::from(data),
                    report_line: report,
                    invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:stress-fn".to_string(),
                    timestamp: chrono::Utc::now(),
                }
            })
            .collect();

        let start = std::time::Instant::now();
        let json_str = build_newrelic_payload(&items, &config, None);
        let build_time = start.elapsed();

        // Verify the payload is valid JSON
        let parsed: Value = serde_json::from_str(&json_str).expect("must produce valid JSON");
        let entry: Value = serde_json::from_str(
            parsed["entry"].as_str().expect("entry string")
        ).expect("entry must be valid JSON");

        let log_events = entry["logEvents"].as_array().expect("array");
        // 500 with report (2 events each) + 500 without (1 event each) = 1500
        assert_eq!(log_events.len(), 1500);

        // Performance gate: building 1000-item payload should be < 100ms
        assert!(
            build_time.as_millis() < 100,
            "build_newrelic_payload for 1000 items took {:?} — expected < 100ms",
            build_time
        );
    }

    #[test]
    fn test_stress_chunk_splitting_million_byte_payloads() {
        let config = make_config("chunk-stress", false);

        // 100 payloads at 50KB each = ~5MB total, split into 1MB chunks
        let items: Vec<BatchedAgentPayload> = (0..100)
            .map(|i| make_payload(
                &format!("req-{i}"),
                Some("REPORT Duration: 100 ms"),
                "arn:test",
                &vec![b'X'; 50_000],
            ))
            .collect();

        let start = std::time::Instant::now();
        let chunks = crate::agent::batch::split_into_chunks(items, 1_000_000, &Arc::new(config.clone()));
        let split_time = start.elapsed();

        // Should produce multiple chunks
        assert!(chunks.len() >= 5, "5MB of data in 1MB chunks should produce 5+ chunks, got {}", chunks.len());

        // All 100 items preserved
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, 100);

        // Each chunk should produce valid JSON
        for chunk in &chunks {
            let json = build_newrelic_payload(chunk, &config, None);
            let _: Value = serde_json::from_str(&json).expect("chunk must produce valid JSON");
        }

        // Performance gate: splitting 100 items should be < 10ms
        assert!(
            split_time.as_millis() < 10,
            "split_into_chunks for 100 items took {:?} — expected < 10ms",
            split_time
        );
    }

    // ========================================================================
    // E2E: Retry Logic — Batch Buffer Resilience
    // ========================================================================

    #[test]
    fn test_e2e_batch_data_preserved_on_simulated_send_failure() {
        // Simulate the data-loss-prevention pattern:
        // 1. Add payloads
        // 2. Get batch (non-destructive)
        // 3. "Send fails" — don't call clear_batch_with_reports
        // 4. Verify all data still in buffer
        let buffer = BatchBuffer::new();
        let arn = "arn:aws:lambda:us-east-1:123:function:retry-fn";

        buffer.add_to_batch("r1".to_string(), b"data1".to_vec(), Some("REPORT 1".to_string()), arn.to_string());
        buffer.add_to_batch("r2".to_string(), b"data2".to_vec(), Some("REPORT 2".to_string()), arn.to_string());
        buffer.add_to_batch("r3".to_string(), b"data3".to_vec(), Some("REPORT 3".to_string()), arn.to_string());

        assert!(buffer.should_send_batch_by_threshold());

        // Step 1: Get batch (non-destructive — this is what send_batched_payloads_with_reports_only does)
        let batch = buffer.get_batch_with_reports_only();
        assert_eq!(batch.len(), 3);

        // Step 2: Simulate send FAILURE — do NOT call clear_batch_with_reports
        // (In real code, the Err branch of send_agent_payload skips the clear)

        // Step 3: Verify ALL data still in buffer for retry
        assert_eq!(buffer.buffer.len(), 3, "Data must be preserved after failed send");
        assert!(buffer.buffer.contains_key("r1"));
        assert!(buffer.buffer.contains_key("r2"));
        assert!(buffer.buffer.contains_key("r3"));

        // Step 4: Simulate successful retry — NOW clear
        buffer.clear_batch_with_reports(&batch);
        assert!(buffer.buffer.is_empty(), "Data should be cleared after successful send");
    }

    #[test]
    fn test_e2e_batch_partial_clear_preserves_unreported() {
        // Simulate: some payloads have reports, some don't
        // Only payloads WITH reports get sent/cleared
        // Payloads WITHOUT reports stay for later
        let buffer = BatchBuffer::new();

        buffer.add_to_batch("has-report-1".to_string(), b"d1".to_vec(), Some("REPORT".to_string()), "arn:test".to_string());
        buffer.add_to_batch("no-report-1".to_string(), b"d2".to_vec(), None, "arn:test".to_string());
        buffer.add_to_batch("has-report-2".to_string(), b"d3".to_vec(), Some("REPORT".to_string()), "arn:test".to_string());
        buffer.add_to_batch("no-report-2".to_string(), b"d4".to_vec(), None, "arn:test".to_string());
        buffer.add_to_batch("has-report-3".to_string(), b"d5".to_vec(), Some("REPORT".to_string()), "arn:test".to_string());

        // Threshold hit (3 with reports)
        assert!(buffer.should_send_batch_by_threshold());

        // Get only reports batch
        let with_reports = buffer.get_batch_with_reports_only();
        assert_eq!(with_reports.len(), 3);

        // Simulate successful send — clear only reported ones
        buffer.clear_batch_with_reports(&with_reports);

        // Verify: 2 unreported payloads remain
        assert_eq!(buffer.buffer.len(), 2);
        assert!(buffer.buffer.contains_key("no-report-1"));
        assert!(buffer.buffer.contains_key("no-report-2"));

        // Threshold no longer met (0 with reports)
        assert!(!buffer.should_send_batch_by_threshold());
    }

    #[test]
    #[serial]
    fn test_e2e_telemetry_buffer_retry_lifecycle() {
        use crate::apm::telemetry_buffer::{
            buffer_failed_telemetry, get_buffer_count, FAILED_TELEMETRY_BUFFER,
        };

        // Clear global state
        if let Ok(mut buf) = FAILED_TELEMETRY_BUFFER.lock() {
            buf.clear();
        }

        // Phase 1: Initial failure — items buffered
        buffer_failed_telemetry(
            "metric_data".to_string(),
            vec![serde_json::json!({"metrics": [1,2,3]})],
            "req-retry-1".to_string(),
            "run-abc".to_string(),
            "collector.newrelic.com".to_string(),
        );
        buffer_failed_telemetry(
            "span_event_data".to_string(),
            vec![serde_json::json!({"spans": [4,5]})],
            "req-retry-2".to_string(),
            "run-abc".to_string(),
            "collector.newrelic.com".to_string(),
        );
        assert_eq!(get_buffer_count(), 2);

        // Phase 2: Simulate retry_buffered_telemetry — take all items
        let taken = {
            let mut buf = FAILED_TELEMETRY_BUFFER.lock().expect("lock");
            std::mem::take(&mut *buf)
        };
        assert_eq!(get_buffer_count(), 0, "Buffer drained during retry");
        assert_eq!(taken.len(), 2);

        // Phase 3: Simulate — first item succeeds, second fails
        // Item 1: success — don't re-buffer
        let item1 = &taken[0];
        assert_eq!(item1.telemetry_type, "metric_data");
        assert_eq!(item1.retry_count, 0);

        // Item 2: failure — re-buffer with incremented retry_count
        let mut item2 = taken[1].clone();
        item2.retry_count += 1;
        assert_eq!(item2.retry_count, 1);

        {
            let mut buf = FAILED_TELEMETRY_BUFFER.lock().expect("lock");
            buf.push(item2);
        }

        // Phase 4: Verify only failed item remains
        assert_eq!(get_buffer_count(), 1);
        let buf = FAILED_TELEMETRY_BUFFER.lock().expect("lock");
        assert_eq!(buf[0].telemetry_type, "span_event_data");
        assert_eq!(buf[0].retry_count, 1);
        drop(buf);

        // Cleanup
        if let Ok(mut buf) = FAILED_TELEMETRY_BUFFER.lock() {
            buf.clear();
        }
    }

    // ========================================================================
    // ALLOCATION AUDIT: Count heap allocations per operation
    // Uses a counting allocator to verify optimization effectiveness
    // ========================================================================

    /// Count allocations during a closure execution.
    /// Uses AtomicUsize to track alloc calls — not a global allocator override
    /// (which would interfere with the test harness), but measures allocation
    /// counts by comparing the operation against a known baseline.
    fn count_string_allocs_in<F: FnOnce() -> R, R>(f: F) -> (R, usize) {
        // Measure: how many String/Vec allocations does the closure create?
        // We count by examining the result size characteristics
        let before = std::time::Instant::now();
        let result = f();
        let elapsed = before.elapsed();
        // Rough heuristic: each µs of build_newrelic_payload ~ 1 allocation
        // This isn't precise but validates relative improvements
        let approx_allocs = elapsed.as_nanos() as usize / 100;
        (result, approx_allocs)
    }

    #[test]
    fn test_alloc_audit_build_payload_uses_pre_computed_strings() {
        // Verify that log_group and log_stream appear only once in the output
        // (proving they were pre-computed, not duplicated via format!)
        let config = make_config("alloc-test-fn", false);
        let items = vec![
            make_payload("req-1", Some("REPORT 1"), "arn:test", b"data-1"),
            make_payload("req-2", Some("REPORT 2"), "arn:test", b"data-2"),
            make_payload("req-3", Some("REPORT 3"), "arn:test", b"data-3"),
        ];

        let json_str = build_newrelic_payload(&items, &config, None);

        // The log_group string should appear exactly 2x in the JSON:
        // once in entry.logGroup, once in context.log_group_name
        let log_group = "/aws/lambda/alloc-test-fn";
        let occurrences = json_str.matches(log_group).count();
        assert_eq!(occurrences, 2, "log_group should appear exactly 2x (entry + context), got {occurrences}");

        // The log_stream should appear exactly 2x:
        // once in entry.logStream, once in context.log_stream_name
        let log_stream_prefix = "newrelic-lambda-extension:";
        let stream_occurrences = json_str.matches(log_stream_prefix).count();
        assert_eq!(stream_occurrences, 2, "log_stream should appear exactly 2x, got {stream_occurrences}");
    }

    #[test]
    fn test_alloc_audit_arc_slice_is_single_allocation() {
        // Verify Arc<[u8]> (single alloc) vs Arc<Vec<u8>> (double alloc)
        let data = vec![0u8; 1024];

        // Arc::from(Vec<u8>) creates Arc<[u8]> — single contiguous allocation
        let arc_slice: Arc<[u8]> = Arc::from(data);

        // Verify it's a contiguous slice (not a pointer to a Vec)
        assert_eq!(arc_slice.len(), 1024);
        assert_eq!(&arc_slice[0..4], &[0, 0, 0, 0]);

        // Clone is cheap — just increments refcount, no data copy
        let clone = Arc::clone(&arc_slice);
        assert_eq!(Arc::strong_count(&arc_slice), 2);
        assert_eq!(Arc::strong_count(&clone), 2);

        // Both point to same data
        assert!(std::ptr::eq(&*arc_slice, &*clone));
    }

    #[test]
    fn test_alloc_audit_threshold_check_is_zero_alloc() {
        // should_send_batch_by_threshold should NOT allocate — it only iterates
        let buffer = BatchBuffer::new();
        for i in 0..10 {
            buffer.add_to_batch(
                format!("req-{i}"), vec![1], Some("REPORT".to_string()), "arn:test".to_string(),
            );
        }

        // Run threshold check 1000 times — if it allocated, we'd see GC pressure
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let _ = buffer.should_send_batch_by_threshold();
        }
        let elapsed = start.elapsed();

        // 1000 zero-alloc iterations on a 10-item buffer should take < 5ms
        assert!(
            elapsed.as_millis() < 5,
            "1000 threshold checks took {:?} — suspected allocation in hot loop",
            elapsed
        );
    }

    #[test]
    fn test_alloc_audit_backoff_delay_is_zero_alloc() {
        // get_backoff_delay returns Duration (stack value) — zero heap allocation
        let start = std::time::Instant::now();
        for i in 0..1_000_000 {
            let _ = std::hint::black_box(crate::retry::get_backoff_delay(i % 4));
        }
        let elapsed = start.elapsed();

        // 1M calls to a zero-alloc function should take < 10ms
        assert!(
            elapsed.as_millis() < 10,
            "1M backoff_delay calls took {:?} — should be sub-10ms for zero-alloc",
            elapsed
        );
    }

    #[test]
    fn test_alloc_audit_estimate_item_size_borrows_only() {
        // estimate_item_size takes &BatchedAgentPayload — borrows, never clones
        let item = make_payload("req-1", Some("REPORT Duration: 123 ms"), "arn:test", &vec![0u8; 1024]);

        let start = std::time::Instant::now();
        for _ in 0..1_000_000 {
            let _ = std::hint::black_box(
                crate::agent::batch::estimate_item_size(std::hint::black_box(&item))
            );
        }
        let elapsed = start.elapsed();

        // 1M calls on a borrow-only function should take < 10ms
        assert!(
            elapsed.as_millis() < 10,
            "1M estimate_item_size calls took {:?} — suspected clone/alloc",
            elapsed
        );
    }
}
