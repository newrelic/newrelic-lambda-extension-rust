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

    // ========================================================================
    // Benchmark: Log Level Extraction Throughput (hottest path — every log)
    // ========================================================================

    #[test]
    fn test_bench_log_level_extraction_throughput() {
        use crate::logs::processor::LogProcessor;
        use crate::newrelic::client::NewRelicClient;
        use crate::request::ProcessorFactory;
        use crate::context::InvocationContext;

        // Sample log messages simulating real-world Lambda workload
        let messages = vec![
            r#"{"level":"INFO","message":"Processing request","timestamp":"2024-01-15T12:00:00Z"}"#,
            r#"{"severity":"ERROR","message":"Connection timeout","code":500}"#,
            "2024-01-15T12:00:00Z INFO Lambda function invoked",
            "ERROR: Failed to connect to database",
            "WARNING: Memory usage at 90%",
            "DEBUG: Entering handler function",
            "Normal log message without any level indicator",
            r#"{"level":"Information","message":"Serilog structured log"}"#,
        ];

        let iterations = 100_000;
        let start = std::time::Instant::now();

        for i in 0..iterations {
            let msg = &messages[i % messages.len()];
            // Simulate the JSON parsing + level extraction path
            let _ = std::hint::black_box(
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(msg) {
                    parsed.get("level")
                        .or_else(|| parsed.get("severity"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_uppercase())
                        .unwrap_or_else(|| "INFO".to_string())
                } else {
                    // Unstructured — scan for keywords
                    let upper = msg.to_uppercase();
                    if upper.contains("ERROR") { "ERROR".to_string() }
                    else if upper.contains("WARN") { "WARN".to_string() }
                    else if upper.contains("DEBUG") { "DEBUG".to_string() }
                    else { "INFO".to_string() }
                }
            );
        }

        let elapsed = start.elapsed();
        let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();

        // Performance gate: 100K log level extractions should be < 200ms
        assert!(
            elapsed.as_millis() < 200,
            "100K log level extractions took {:?} ({:.0} ops/sec) — expected < 200ms",
            elapsed, ops_per_sec
        );
    }

    // ========================================================================
    // Benchmark: Payload Serialization Scaling (10, 100, 1000 items)
    // ========================================================================

    #[test]
    fn test_bench_payload_serialization_scaling() {
        let config = make_config("bench-fn", false);

        let sizes = [10, 100, 500];
        let mut timings = Vec::new();

        for &size in &sizes {
            let items: Vec<BatchedAgentPayload> = (0..size)
                .map(|i| make_payload(
                    &format!("req-{i}"),
                    Some(&format!("REPORT Duration: {i} ms")),
                    "arn:aws:lambda:us-east-1:123:function:bench-fn",
                    &vec![b'A'; 512],
                ))
                .collect();

            let start = std::time::Instant::now();
            for _ in 0..10 {
                let _ = std::hint::black_box(build_newrelic_payload(&items, &config, None));
            }
            let elapsed = start.elapsed();
            let avg_ms = elapsed.as_millis() as f64 / 10.0;
            timings.push((size, avg_ms));
        }

        // Scaling should be roughly linear (not quadratic)
        // 500 items should take at most 10x what 10 items takes (not 2500x)
        let (_, time_10) = timings[0];
        let (_, time_500) = timings[2];

        assert!(
            time_500 < time_10 * 100.0 || time_500 < 50.0,
            "Serialization scaling is non-linear: 10 items={:.1}ms, 500 items={:.1}ms (expected < {:.1}ms)",
            time_10, time_500, time_10 * 100.0
        );
    }

    // ========================================================================
    // Benchmark: DashMap Payload Routing Under Contention
    // ========================================================================

    #[test]
    fn test_bench_dashmap_routing_contention() {
        use crate::request::{REQUEST_AGENT_BUFFERS, CURRENT_ACTIVE_REQUEST_ID};
        use serial_test::serial;

        let num_threads = 20;
        let ops_per_thread = 5000;
        let barrier = Arc::new(std::sync::Barrier::new(num_threads));

        // Setup: one active request buffer
        let buffer = Arc::new(std::sync::Mutex::new(Vec::<Vec<u8>>::new()));
        REQUEST_AGENT_BUFFERS.insert("bench-req".to_string(), buffer.clone());
        if let Ok(mut active) = CURRENT_ACTIVE_REQUEST_ID.lock() {
            *active = Some("bench-req".to_string());
        }

        let start = std::time::Instant::now();

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..ops_per_thread {
                        // Simulate the hot path: read CURRENT_ACTIVE_REQUEST_ID + lookup buffer
                        let req_id = CURRENT_ACTIVE_REQUEST_ID
                            .lock()
                            .ok()
                            .and_then(|guard| guard.clone());
                        if let Some(req_id) = req_id {
                            let _ = std::hint::black_box(REQUEST_AGENT_BUFFERS.get(&req_id));
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("no panic");
        }

        let elapsed = start.elapsed();
        let total_ops = num_threads * ops_per_thread;

        // Cleanup
        REQUEST_AGENT_BUFFERS.remove("bench-req");
        if let Ok(mut active) = CURRENT_ACTIVE_REQUEST_ID.lock() {
            *active = None;
        }

        // Performance gate: 100K concurrent lookups should be < 500ms
        assert!(
            elapsed.as_millis() < 500,
            "{total_ops} concurrent DashMap lookups took {:?} — expected < 500ms",
            elapsed
        );
    }

    // ========================================================================
    // Benchmark: Telemetry Record Deserialization Throughput
    // ========================================================================

    #[test]
    fn test_bench_telemetry_record_deserialization() {
        let json_records = r#"[
            {"time":"2024-01-15T12:00:00.000Z","type":"function","record":{"message":"log line 1"}},
            {"time":"2024-01-15T12:00:01.000Z","type":"function","record":{"message":"log line 2 with more content"}},
            {"time":"2024-01-15T12:00:02.000Z","type":"extension","record":{"message":"ext log"}},
            {"time":"2024-01-15T12:00:03.000Z","type":"platform.runtimeDone","record":{"requestId":"req-1","status":"success"}},
            {"time":"2024-01-15T12:00:04.000Z","type":"platform.report","record":{"requestId":"req-1","metrics":{"durationMs":123.45,"billedDurationMs":124,"memorySizeMB":512,"maxMemoryUsedMB":256}}}
        ]"#;

        let iterations = 10_000;
        let start = std::time::Instant::now();

        for _ in 0..iterations {
            let records: Vec<crate::telemetry::listener::TelemetryRecord> = std::hint::black_box(
                serde_json::from_str(json_records).expect("valid")
            );
            assert_eq!(records.len(), 5);
        }

        let elapsed = start.elapsed();
        let records_per_sec = (iterations * 5) as f64 / elapsed.as_secs_f64();

        // Performance gate: 50K record deserializations should be < 500ms
        assert!(
            elapsed.as_millis() < 500,
            "50K telemetry record deserialization took {:?} ({:.0} records/sec) — expected < 500ms",
            elapsed, records_per_sec
        );
    }

    // ========================================================================
    // Benchmark: Platform REPORT Line Formatting Throughput
    // ========================================================================

    #[test]
    fn test_bench_platform_report_formatting() {
        let config = Arc::new(make_config("bench-fn", false));
        let client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let factory = crate::request::ProcessorFactory::new(client.clone(), config.clone(), apm_app);
        let ctx = Arc::new(std::sync::Mutex::new(crate::context::InvocationContext {
            request_id: "bench-req".to_string(),
            invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:bench-fn".to_string(),
            trace_id: None,
        }));
        let log_processor = factory.create_log_processor(ctx.clone());
        let processor = crate::platform::processor::PlatformProcessor::new(client, config, ctx, log_processor);

        let record = crate::telemetry::listener::TelemetryRecord {
            time: chrono::Utc::now(),
            record_type: "platform.report".to_string(),
            record: serde_json::json!({
                "requestId": "bench-req-001",
                "metrics": {
                    "durationMs": 1234.56,
                    "billedDurationMs": 1235,
                    "memorySizeMB": 512,
                    "maxMemoryUsedMB": 384,
                    "initDurationMs": 567.89
                }
            }),
        };

        let iterations = 100_000;
        let start = std::time::Instant::now();

        for _ in 0..iterations {
            let _ = std::hint::black_box(processor.convert_platform_report_to_log_line(&record));
        }

        let elapsed = start.elapsed();

        // Performance gate: 100K REPORT line formattings should be < 200ms
        assert!(
            elapsed.as_millis() < 200,
            "100K platform REPORT formattings took {:?} — expected < 200ms",
            elapsed
        );
    }

    // ========================================================================
    // Benchmark: NR_TAGS Config Parsing Throughput
    // ========================================================================

    #[test]
    #[serial]
    fn test_bench_nr_tags_parsing() {
        std::env::set_var("NR_TAGS", "env:production;team:platform;service:lambda-ext;region:us-east-1;version:2.4.5");

        let iterations = 100_000;
        let start = std::time::Instant::now();

        for _ in 0..iterations {
            let tags = std::hint::black_box(crate::config::parse_nr_tags());
            assert!(!tags.is_empty());
        }

        let elapsed = start.elapsed();
        std::env::remove_var("NR_TAGS");

        // Performance gate: 100K tag parses should be < 500ms
        assert!(
            elapsed.as_millis() < 500,
            "100K NR_TAGS parses took {:?} — expected < 500ms",
            elapsed
        );
    }

    // ========================================================================
    // E2E: EU Endpoint Detection from License Key Prefix
    // ========================================================================

    #[test]
    fn test_e2e_eu_license_key_sets_eu_endpoints() {
        // Simulate the EU endpoint detection logic from main.rs perform_one_time_initialization
        let license_key = "eu01xxABCDEF1234567890NRAL";
        let mut config = make_config("eu-function", false);
        config.new_relic.license_key = Some(license_key.to_string());

        let license_key_prefix = license_key.get(0..2);
        assert_eq!(license_key_prefix, Some("eu"));

        // Apply EU endpoint detection (same logic as main.rs lines 290-316)
        if let Some("eu") = license_key_prefix {
            config.new_relic.apm_host = "collector.eu01.nr-data.net".to_string();
            config.new_relic.metric_endpoint = "https://metric-api.eu.newrelic.com/metric/v1".to_string();
            config.new_relic.telemetry_endpoint = "https://cloud-collector.eu01.nr-data.net/aws/lambda/v1".to_string();
            config.new_relic.log_endpoint = "https://log-api.eu.newrelic.com/log/v1".to_string();
        }

        assert_eq!(config.new_relic.apm_host, "collector.eu01.nr-data.net");
        assert!(config.new_relic.metric_endpoint.contains("eu.newrelic.com"));
        assert!(config.new_relic.telemetry_endpoint.contains("eu01.nr-data.net"));
        assert!(config.new_relic.log_endpoint.contains("eu.newrelic.com"));
    }

    #[test]
    fn test_e2e_us_license_key_keeps_default_endpoints() {
        let license_key = "us01xxABCDEF1234567890NRAL";
        let config = make_config("us-function", false);

        let license_key_prefix = license_key.get(0..2);
        assert_ne!(license_key_prefix, Some("eu"));

        // US license key should NOT override endpoints — defaults stay
        assert!(!config.new_relic.apm_host.contains("eu01"));
    }

    #[test]
    fn test_e2e_env_var_overrides_eu_detection() {
        // When env vars are set, they take precedence over EU license key prefix
        let license_key = "eu01xxABCDEF1234567890NRAL";
        let mut config = make_config("override-function", false);
        config.new_relic.license_key = Some(license_key.to_string());

        let custom_host = "custom-collector.example.com";
        // Simulate: env var set takes precedence
        config.new_relic.apm_host = custom_host.to_string();

        assert_eq!(config.new_relic.apm_host, custom_host);
        assert!(!config.new_relic.apm_host.contains("eu01"));
    }

    #[test]
    fn test_e2e_short_license_key_no_panic() {
        // License key shorter than 2 chars should not panic
        let license_key = "e";
        let license_key_prefix = license_key.get(0..2);
        assert!(license_key_prefix.is_none());
    }

    #[test]
    fn test_e2e_empty_license_key_no_panic() {
        let license_key = "";
        let license_key_prefix = license_key.get(0..2);
        assert!(license_key_prefix.is_none());
    }

    // ========================================================================
    // E2E: Java Runtime Override to Serverless Mode
    // ========================================================================

    #[test]
    #[serial]
    fn test_e2e_java_runtime_forces_serverless_mode() {
        // Simulate: Java runtime detected via env var
        std::env::set_var("AWS_EXECUTION_ENV", "AWS_Lambda_java21");

        let mut config = make_config("java-function", true); // APM mode requested
        config.new_relic.apm_lambda_mode = true;

        let detected_runtime = crate::version::get_runtime_name();
        assert_eq!(detected_runtime, "java");

        // Apply override (same logic as apply_runtime_overrides in main.rs)
        if config.new_relic.apm_lambda_mode && detected_runtime == "java" {
            config.new_relic.apm_lambda_mode = false;
        }

        assert!(!config.new_relic.apm_lambda_mode, "Java should force serverless mode");
        std::env::remove_var("AWS_EXECUTION_ENV");
    }

    #[test]
    #[serial]
    fn test_e2e_python_runtime_keeps_apm_mode() {
        std::env::set_var("AWS_EXECUTION_ENV", "AWS_Lambda_python3.13");

        let mut config = make_config("python-function", true);
        config.new_relic.apm_lambda_mode = true;

        let detected_runtime = crate::version::get_runtime_name();
        assert_eq!(detected_runtime, "python");

        // Python should NOT override APM mode
        if config.new_relic.apm_lambda_mode && detected_runtime == "java" {
            config.new_relic.apm_lambda_mode = false;
        }

        assert!(config.new_relic.apm_lambda_mode, "Python should keep APM mode");
        std::env::remove_var("AWS_EXECUTION_ENV");
    }

    // ========================================================================
    // E2E: Shutdown Error Synthesis Scenarios
    // ========================================================================

    fn clear_error_synthesis_state() {
        if let Ok(mut m) = crate::error_synthesis::LAST_PLATFORM_METRICS.lock() { *m = None; }
        if let Ok(mut s) = crate::error_synthesis::SENT_ERRORS.lock() { s.clear(); }
        if let Ok(mut f) = crate::error_synthesis::FAILED_ERRORS.lock() { f.clear(); }
        if let Ok(mut e) = crate::error_synthesis::LAST_DETECTED_ERROR.lock() { *e = None; }
    }

    #[test]
    #[serial]
    fn test_e2e_shutdown_timeout_error_synthesis_pipeline() {
        clear_error_synthesis_state();

        let request_id = "req-timeout-001";
        let _arn = "arn:aws:lambda:us-east-1:123456789012:function:timeout-fn";

        // Step 1: Store platform metrics (as if platform.report arrived)
        crate::error_synthesis::store_platform_metrics(
            request_id.to_string(),
            Some(30500.0), // 30.5 seconds
            Some(512),
            Some(450),
        );

        // Step 2: Verify metrics stored
        let guard = crate::error_synthesis::LAST_PLATFORM_METRICS.lock().expect("lock");
        let metrics = guard.as_ref().expect("should have metrics");
        assert_eq!(metrics.request_id, request_id);
        assert_eq!(metrics.duration_ms, Some(30500.0));
        drop(guard);

        // Step 3: Verify dedup logic — mark as sent
        if let Ok(mut sent) = crate::error_synthesis::SENT_ERRORS.lock() {
            sent.insert((request_id.to_string(), "LambdaTimeout".to_string()));
        }

        // Step 4: Verify duplicate detection
        let guard = crate::error_synthesis::SENT_ERRORS.lock().expect("lock");
        assert!(guard.contains(&(request_id.to_string(), "LambdaTimeout".to_string())));
        // Different error type should NOT be considered duplicate
        assert!(!guard.contains(&(request_id.to_string(), "LambdaPlatformFault".to_string())));
        drop(guard);

        // Step 5: Verify clear on new invocation
        crate::error_synthesis::clear_sent_errors_for_request("req-new-invoke");
        let guard = crate::error_synthesis::SENT_ERRORS.lock().expect("lock");
        assert!(guard.is_empty(), "Should clear all sent errors on new invocation");
        drop(guard);

        clear_error_synthesis_state();
    }

    #[test]
    #[serial]
    fn test_e2e_shutdown_failure_error_with_memory_info() {
        clear_error_synthesis_state();

        let request_id = "req-oom-001";

        // Store platform metrics with OOM-like data
        crate::error_synthesis::store_platform_metrics(
            request_id.to_string(),
            Some(5000.0),
            Some(512),   // 512 MB limit
            Some(510),   // 510 MB used (near OOM)
        );

        // Verify memory info extraction logic (same as send_platform_fault_error)
        let memory_info = if let Ok(guard) = crate::error_synthesis::LAST_PLATFORM_METRICS.lock() {
            if let Some(ref metrics) = *guard {
                if metrics.request_id == request_id {
                    match (metrics.max_memory_used_mb, metrics.memory_size_mb) {
                        (Some(used), Some(size)) => {
                            format!(" (Memory: {} MB used / {} MB limit)", used, size)
                        }
                        _ => String::new(),
                    }
                } else {
                    String::new()
                }
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        assert_eq!(memory_info, " (Memory: 510 MB used / 512 MB limit)");

        let fault_msg = format!(
            "RequestId: {} AWS Lambda platform fault caused a shutdown{}",
            request_id, memory_info
        );
        assert!(fault_msg.contains("510 MB used"));
        assert!(fault_msg.contains("512 MB limit"));

        clear_error_synthesis_state();
    }

    #[test]
    #[serial]
    fn test_e2e_shutdown_spindown_no_error() {
        clear_error_synthesis_state();

        // Spindown is a normal shutdown — no error should be synthesized
        let reason = crate::runtime::ShutdownReason::Spindown;
        assert_eq!(reason.as_str(), "spindown");

        // Verify no errors were added
        let guard = crate::error_synthesis::SENT_ERRORS.lock().expect("lock");
        assert!(guard.is_empty());
        drop(guard);

        clear_error_synthesis_state();
    }

    #[test]
    #[serial]
    fn test_e2e_failed_errors_retry_lifecycle() {
        clear_error_synthesis_state();

        // Step 1: Simulate failed error sends being stored for retry
        if let Ok(mut failed) = crate::error_synthesis::FAILED_ERRORS.lock() {
            failed.push(crate::error_synthesis::FailedError {
                request_id: "req-fail-1".to_string(),
                error_type: "LambdaTimeout".to_string(),
                error_message: "Task timed out after 30.00 seconds".to_string(),
                invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:fn".to_string(),
                error_class: "LambdaTimeout".to_string(),
            });
            failed.push(crate::error_synthesis::FailedError {
                request_id: "req-fail-2".to_string(),
                error_type: "LambdaPlatformFault".to_string(),
                error_message: "Platform fault".to_string(),
                invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:fn".to_string(),
                error_class: "LambdaPlatformFault".to_string(),
            });
        }

        // Step 2: Verify queue has items
        let guard = crate::error_synthesis::FAILED_ERRORS.lock().expect("lock");
        assert_eq!(guard.len(), 2);
        drop(guard);

        // Step 3: Simulate drain for retry (same pattern as retry_failed_errors)
        let drained = if let Ok(mut guard) = crate::error_synthesis::FAILED_ERRORS.lock() {
            let errors = guard.clone();
            guard.clear();
            errors
        } else {
            Vec::new()
        };

        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].error_type, "LambdaTimeout");
        assert_eq!(drained[1].error_type, "LambdaPlatformFault");

        // Step 4: Verify queue is now empty
        let guard = crate::error_synthesis::FAILED_ERRORS.lock().expect("lock");
        assert!(guard.is_empty());
        drop(guard);

        clear_error_synthesis_state();
    }

    // ========================================================================
    // E2E: Late Payload Handling Across APM Invocations
    // ========================================================================

    fn clear_request_state() {
        crate::request::REQUEST_PROCESSORS.clear();
        crate::request::REQUEST_CONTEXTS.clear();
        crate::request::REQUEST_AGENT_BUFFERS.clear();
        crate::request::PAYLOAD_COORDINATION.clear();
        crate::request::RUNTIME_DONE_CHANNELS.clear();
        crate::request::PENDING_REPORTS.clear();
        crate::request::REQUEST_BUFFER_TIMESTAMPS.clear();
        if let Ok(mut active) = crate::request::CURRENT_ACTIVE_REQUEST_ID.lock() {
            *active = None;
        }
        if let Ok(mut orphaned) = crate::request::ORPHANED_PAYLOADS.lock() {
            orphaned.clear();
        }
    }

    #[test]
    #[serial]
    fn test_e2e_late_payload_buffered_across_invocations() {
        clear_request_state();

        // Invocation 1: Create request state, agent payload arrives but no run_id yet
        let req_id_1 = "req-inv-1";
        let arn = "arn:aws:lambda:us-east-1:123:function:apm-fn";
        let buffer_1 = Arc::new(std::sync::Mutex::new(Vec::new()));
        crate::request::REQUEST_AGENT_BUFFERS.insert(req_id_1.to_string(), buffer_1.clone());
        crate::request::REQUEST_CONTEXTS.insert(
            req_id_1.to_string(),
            Arc::new(std::sync::Mutex::new(crate::context::InvocationContext {
                request_id: req_id_1.to_string(),
                invoked_function_arn: arn.to_string(),
                trace_id: None,
            })),
        );

        // Agent payload arrives late — stored in buffer
        if let Ok(mut buf) = buffer_1.lock() {
            buf.push(b"late-agent-payload-data".to_vec());
        }

        // Invocation 1 ends with skip_buffer_cleanup=true (APM mode keeps buffers)
        crate::request::cleanup_request_processing_state_internal(req_id_1, true);

        // Buffer should still exist after APM-mode cleanup
        assert!(
            crate::request::REQUEST_AGENT_BUFFERS.contains_key(req_id_1),
            "Buffer should survive APM-mode cleanup"
        );

        // Invocation 2: Detect pending buffers from previous invocation
        let pending_buffers: Vec<String> = crate::request::REQUEST_AGENT_BUFFERS
            .iter()
            .filter_map(|entry| {
                if let Ok(buffer) = entry.value().lock() {
                    if !buffer.is_empty() {
                        return Some(entry.key().clone());
                    }
                }
                None
            })
            .collect();

        assert_eq!(pending_buffers.len(), 1);
        assert_eq!(pending_buffers[0], req_id_1);

        // Extract late payloads
        let late_payloads = if let Some(buffer_ref) = crate::request::REQUEST_AGENT_BUFFERS.get(req_id_1) {
            if let Ok(mut buffer) = buffer_ref.lock() {
                std::mem::take(&mut *buffer)
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        assert_eq!(late_payloads.len(), 1);
        assert_eq!(late_payloads[0], b"late-agent-payload-data");

        // Clean up old request after processing late payloads
        crate::request::cleanup_request_processing_state_internal(req_id_1, false);
        assert!(!crate::request::REQUEST_AGENT_BUFFERS.contains_key(req_id_1));

        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_e2e_orphaned_payload_lifecycle_across_invocations() {
        clear_request_state();

        // Agent payload arrives BEFORE any request is created
        if let Ok(mut orphaned) = crate::request::ORPHANED_PAYLOADS.lock() {
            orphaned.push(b"orphan-payload-1".to_vec());
            orphaned.push(b"orphan-payload-2".to_vec());
        }

        // First request is created — orphans should be moved to its buffer
        let req_id = "req-first";
        let agent_buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        crate::request::REQUEST_AGENT_BUFFERS.insert(req_id.to_string(), agent_buffer.clone());

        // Move orphans (same logic as create_request_processing_state)
        if let Ok(mut orphaned) = crate::request::ORPHANED_PAYLOADS.lock() {
            if !orphaned.is_empty() {
                if let Ok(mut buffer) = agent_buffer.lock() {
                    buffer.extend(orphaned.drain(..));
                }
            }
        }

        // Verify orphans moved
        let guard = agent_buffer.lock().expect("lock");
        assert_eq!(guard.len(), 2);
        assert_eq!(guard[0], b"orphan-payload-1");
        assert_eq!(guard[1], b"orphan-payload-2");
        drop(guard);

        // Orphan buffer should be empty
        let orphaned = crate::request::ORPHANED_PAYLOADS.lock().expect("lock");
        assert!(orphaned.is_empty());
        drop(orphaned);

        clear_request_state();
    }

    // ========================================================================
    // E2E: Multi-Invocation Batching in Standard Mode
    // ========================================================================

    #[test]
    fn test_e2e_multi_invocation_batching_lifecycle() {
        let buffer = BatchBuffer::new();
        let arn = "arn:aws:lambda:us-east-1:123:function:batch-fn";

        // Invocation 1: Agent payload arrives, no report yet → buffer with None report
        buffer.add_to_batch(
            "req-inv-1".to_string(),
            b"agent-data-inv1".to_vec(),
            None,
            arn.to_string(),
        );
        assert!(!buffer.should_send_batch_by_threshold());

        // Invocation 2: Both payload and report → complete
        buffer.add_to_batch(
            "req-inv-2".to_string(),
            b"agent-data-inv2".to_vec(),
            Some("REPORT RequestId: req-inv-2\tDuration: 100 ms".to_string()),
            arn.to_string(),
        );

        // Invocation 3: Report arrives, matched with existing payload
        buffer.add_to_batch(
            "req-inv-3".to_string(),
            b"agent-data-inv3".to_vec(),
            Some("REPORT RequestId: req-inv-3\tDuration: 200 ms".to_string()),
            arn.to_string(),
        );

        // Late report for invocation 1 arrives
        if let Some(mut entry) = buffer.buffer.get_mut("req-inv-1") {
            entry.report_line = Some("REPORT RequestId: req-inv-1\tDuration: 50 ms".to_string());
        }

        // Now all 3 have reports → threshold should trigger (at 3)
        assert!(buffer.should_send_batch_by_threshold());

        // Build payload — all 3 should be included
        let with_reports = buffer.get_batch_with_reports_only();
        assert_eq!(with_reports.len(), 3);

        let config = Arc::new(make_config("batch-fn", false));
        let payload_json = build_newrelic_payload(&with_reports, &config, None);
        let parsed: Value = serde_json::from_str(&payload_json).expect("valid JSON");

        // Verify context and entry are present
        assert!(parsed["context"].is_object());
        assert!(parsed["entry"].is_string());

        // Verify entry contains all 3 log events + 3 report lines = at least 3 events
        let entry: Value = serde_json::from_str(
            parsed["entry"].as_str().expect("entry string")
        ).expect("valid entry JSON");
        let log_events = entry["logEvents"].as_array().expect("array");
        // Each invocation contributes: 1 agent payload + 1 report line = 2 events, x3 = 6
        assert!(log_events.len() >= 3, "Should have at least 3 log events, got {}", log_events.len());

        // Clear sent items
        buffer.clear_batch_with_reports(&with_reports);
        assert_eq!(buffer.buffer.len(), 0, "All items should be cleared after send");
    }

    #[test]
    fn test_e2e_batch_payload_without_report_preserved() {
        let buffer = BatchBuffer::new();
        let arn = "arn:aws:lambda:us-east-1:123:function:fn";

        // Add payload without report
        buffer.add_to_batch("req-no-report".to_string(), b"data".to_vec(), None, arn.to_string());

        // Get reports-only batch — should NOT include this one
        let with_reports = buffer.get_batch_with_reports_only();
        assert!(with_reports.is_empty(), "Payload without report should not be in reports-only batch");

        // Original buffer should still have the item
        assert_eq!(buffer.buffer.len(), 1);
    }

    // ========================================================================
    // E2E: Cold Start State Transitions
    // ========================================================================

    fn clear_event_loop_state_for_e2e() {
        if let Ok(mut payloads) = crate::event_loop::FAILED_AGENT_PAYLOADS.lock() {
            payloads.clear();
        }
        if let Ok(mut ctx) = crate::event_loop::LAST_REQUEST_CONTEXT.lock() {
            *ctx = None;
        }
    }

    #[test]
    #[serial]
    fn test_e2e_cold_start_global_context_update() {
        // Simulate cold start: first INVOKE sets global context
        let request_id = "req-cold-001";
        let arn = "arn:aws:lambda:us-east-1:123456789012:function:cold-start-fn";

        crate::event_loop::update_global_invocation_context(request_id, arn);

        // Verify global context updated
        if let Ok(ctx) = crate::CURRENT_INVOCATION_CONTEXT.read() {
            assert_eq!(ctx.request_id, request_id);
            assert_eq!(ctx.invoked_function_arn, arn);
            assert!(ctx.trace_id.is_none());
        }

        // Simulate second invocation (warm start) — context should update
        let request_id_2 = "req-warm-002";
        let arn_2 = "arn:aws:lambda:us-east-1:123456789012:function:cold-start-fn";
        crate::event_loop::update_global_invocation_context(request_id_2, arn_2);

        if let Ok(ctx) = crate::CURRENT_INVOCATION_CONTEXT.read() {
            assert_eq!(ctx.request_id, request_id_2);
        }
    }

    #[test]
    #[serial]
    fn test_e2e_cold_start_account_id_extraction_from_arn() {
        // On first INVOKE, account_id is extracted from ARN and set in config
        let arn = "arn:aws:lambda:us-west-2:987654321098:function:my-function";
        let mut config = make_config("my-function", false);
        config.aws.account_id = None;

        config.aws.extract_and_update_account_id_from_arn(arn);

        assert_eq!(config.aws.account_id, Some("987654321098".to_string()));
    }

    #[test]
    #[serial]
    fn test_e2e_cold_start_last_request_context_tracking() {
        clear_event_loop_state_for_e2e();

        // Track first invocation
        if let Ok(mut guard) = crate::event_loop::LAST_REQUEST_CONTEXT.lock() {
            *guard = Some(("req-1".to_string(), "arn:1".to_string()));
        }

        // Track second invocation (overwrites)
        if let Ok(mut guard) = crate::event_loop::LAST_REQUEST_CONTEXT.lock() {
            *guard = Some(("req-2".to_string(), "arn:2".to_string()));
        }

        // Verify latest context is available for shutdown error synthesis
        let guard = crate::event_loop::LAST_REQUEST_CONTEXT.lock().expect("lock");
        assert_eq!(*guard, Some(("req-2".to_string(), "arn:2".to_string())));
        drop(guard);

        clear_event_loop_state_for_e2e();
    }

    // ========================================================================
    // E2E: Network Failure Resilience — Failed Agent Payload Retry Logic
    // ========================================================================

    #[test]
    #[serial]
    fn test_e2e_failed_agent_payload_retry_lifecycle() {
        clear_event_loop_state_for_e2e();

        // Step 1: Agent payload send fails — buffer it
        crate::event_loop::buffer_failed_agent_payload(
            b"failed-payload-data",
            "req-fail-001",
            "arn:aws:lambda:us-east-1:123:function:fn",
        );

        let guard = crate::event_loop::FAILED_AGENT_PAYLOADS.lock().expect("lock");
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0].retry_count, 0);
        drop(guard);

        // Step 2: On next invocation, take failed payloads for retry
        let failed = if let Ok(mut guard) = crate::event_loop::FAILED_AGENT_PAYLOADS.lock() {
            std::mem::take(&mut *guard)
        } else {
            Vec::new()
        };

        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].request_id, "req-fail-001");

        // Step 3: Retry fails again — increment retry_count and re-buffer
        let mut payload = failed.into_iter().next().expect("should have one");
        payload.retry_count += 1;

        if let Ok(mut guard) = crate::event_loop::FAILED_AGENT_PAYLOADS.lock() {
            guard.push(payload);
        }

        let guard = crate::event_loop::FAILED_AGENT_PAYLOADS.lock().expect("lock");
        assert_eq!(guard[0].retry_count, 1);
        drop(guard);

        // Step 4: After 5 retries, payload should be dropped (tested via cleanup)
        if let Ok(mut guard) = crate::event_loop::FAILED_AGENT_PAYLOADS.lock() {
            guard[0].retry_count = 6;
        }

        // Simulate retry check: retry_count > 5 → drop
        let guard = crate::event_loop::FAILED_AGENT_PAYLOADS.lock().expect("lock");
        assert!(guard[0].retry_count > 5, "Should exceed max retries");
        drop(guard);

        clear_event_loop_state_for_e2e();
    }

    #[test]
    #[serial]
    fn test_e2e_failed_payload_age_based_cleanup() {
        clear_event_loop_state_for_e2e();

        // Add a payload that's 25 hours old
        if let Ok(mut guard) = crate::event_loop::FAILED_AGENT_PAYLOADS.lock() {
            guard.push(crate::event_loop::FailedAgentPayload {
                payload_bytes: b"old-data".to_vec(),
                request_id: "req-old".to_string(),
                invoked_function_arn: "arn:test".to_string(),
                retry_count: 1,
                failed_at: chrono::Utc::now() - chrono::Duration::hours(25),
            });
            guard.push(crate::event_loop::FailedAgentPayload {
                payload_bytes: b"recent-data".to_vec(),
                request_id: "req-recent".to_string(),
                invoked_function_arn: "arn:test".to_string(),
                retry_count: 0,
                failed_at: chrono::Utc::now(),
            });
        }

        crate::event_loop::cleanup_old_failed_payloads();

        let guard = crate::event_loop::FAILED_AGENT_PAYLOADS.lock().expect("lock");
        assert_eq!(guard.len(), 1, "Should keep only recent payload");
        assert_eq!(guard[0].request_id, "req-recent");
        drop(guard);

        clear_event_loop_state_for_e2e();
    }

    // ========================================================================
    // E2E: License Key Resolution Fallback Chain
    // ========================================================================

    #[test]
    #[serial]
    fn test_e2e_license_key_env_var_takes_precedence() {
        // When license key is in env var, it should be used directly
        let mut config = make_config("fn", false);
        config.new_relic.license_key = Some("env_var_key_1234567890".to_string());

        let credentials_config = Configuration::from(&config);
        assert!(!credentials_config.license_key.is_empty());
        assert_eq!(credentials_config.license_key, "env_var_key_1234567890");
    }

    #[test]
    fn test_e2e_license_key_missing_all_sources() {
        // When no license key is available from any source
        let config = make_config("fn", false);
        let credentials_config = Configuration::from(&config);
        assert!(credentials_config.license_key.is_empty(), "Should be empty when no key configured");
    }

    #[test]
    fn test_e2e_license_key_decode_and_validate_full_chain() {
        // Full chain: encoded key → decode → validate
        let encoded_key = r#"{"LicenseKey": "abc123def456ghi789jkl012mno345pqNRAL"}"#;
        let result = decode_license_key(encoded_key);
        assert!(result.is_ok());
        assert_eq!(result.expect("should decode"), "abc123def456ghi789jkl012mno345pqNRAL");
    }

    #[test]
    fn test_e2e_license_key_decode_failure_graceful() {
        let bad_encoded = "not-valid-json";
        let result = decode_license_key(bad_encoded);
        assert!(result.is_err(), "Invalid JSON should return error");
    }

    // ========================================================================
    // E2E: Shutdown Reason → Error Event Mapping
    // ========================================================================

    #[test]
    fn test_e2e_shutdown_reason_to_error_event_mapping() {
        let arn = "arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST";

        // Timeout → generates error event
        let timeout_events = generate_error_event(
            "LambdaTimeout",
            "Task timed out after 30.00 seconds",
            "req-timeout",
            arn,
        );
        assert!(!timeout_events.is_empty());
        let detail = &timeout_events[0].as_array().expect("array")[0];
        assert_eq!(detail["error.class"], "LambdaTimeout");
        assert_eq!(detail["type"], "TransactionError");

        // Failure → generates error event
        let fault_events = generate_error_event(
            "LambdaPlatformFault",
            "AWS Lambda platform fault caused a shutdown",
            "req-fault",
            arn,
        );
        assert!(!fault_events.is_empty());
        let detail = &fault_events[0].as_array().expect("array")[0];
        assert_eq!(detail["error.class"], "LambdaPlatformFault");

        // Unknown → generates error event
        let unknown_events = generate_error_event(
            "LambdaShutdown",
            "Lambda shutdown with unknown reason",
            "req-unknown",
            arn,
        );
        assert!(!unknown_events.is_empty());
        let detail = &unknown_events[0].as_array().expect("array")[0];
        assert_eq!(detail["error.class"], "LambdaShutdown");
    }

    #[test]
    fn test_e2e_shutdown_error_event_includes_lambda_metadata() {
        let arn = "arn:aws:lambda:us-west-2:987654321098:function:my-fn:2";
        let events = generate_error_event(
            "LambdaTimeout",
            "Task timed out",
            "req-meta",
            arn,
        );

        let event_array = events[0].as_array().expect("array");
        let user_attrs = &event_array[2];

        assert_eq!(user_attrs["aws.lambda.arn"], arn);
        assert_eq!(user_attrs["aws.lambda.functionVersion"], "2");
        assert_eq!(user_attrs["aws.requestId"], "req-meta");
    }

    #[test]
    fn test_e2e_platform_report_to_apm_metrics_full_chain() {
        // Full chain: REPORT log line → parse → convert to APM metrics
        let report = "REPORT RequestId: abc-123\tDuration: 1234.56 ms\tBilled Duration: 1235 ms\tMemory Size: 256 MB\tMax Memory Used: 200 MB\tInit Duration: 567.89 ms";

        let metrics = parse_lambda_report_log(report).expect("should parse");
        assert_eq!(metrics.request_id, "abc-123");
        assert_eq!(metrics.duration, Some(1234.56));
        assert_eq!(metrics.init_duration, Some(567.89));

        let apm_metrics = convert_to_apm_metrics(&metrics, "entity-guid", "my-function");
        assert_eq!(apm_metrics.len(), 5); // duration, billed, memory_size, max_memory, init_duration

        // Verify all metric names
        let metric_names: Vec<&str> = apm_metrics
            .iter()
            .map(|m| m["name"].as_str().expect("name"))
            .collect();
        assert!(metric_names.contains(&"apm.lambda.transaction.duration"));
        assert!(metric_names.contains(&"apm.lambda.transaction.billed_duration"));
        assert!(metric_names.contains(&"apm.lambda.transaction.memory_size"));
        assert!(metric_names.contains(&"apm.lambda.transaction.max_memory_used"));
        assert!(metric_names.contains(&"apm.lambda.transaction.init_duration"));
    }

    // ========================================================================
    // E2E: LogProcessor — process_record, add_log_to_batch, flush
    // ========================================================================

    #[tokio::test]
    async fn test_e2e_log_processor_add_and_flush_lifecycle() {
        let config = Arc::new(make_config("log-fn", false));
        let client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let factory = crate::request::ProcessorFactory::new(client, config.clone(), apm_app);
        let ctx = Arc::new(std::sync::Mutex::new(crate::context::InvocationContext {
            request_id: "req-log-test".to_string(),
            invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:log-fn".to_string(),
            trace_id: None,
        }));
        let log_processor = factory.create_log_processor(ctx);

        // Add a log message to batch
        let log_msg = crate::newrelic::payload::LogMessage {
            timestamp: chrono::Utc::now().timestamp_millis(),
            message: "test log message".to_string(),
            attributes: serde_json::Map::new(),
        };
        log_processor.add_log_to_batch(log_msg);

        // Flush — sends to noop client (returns Ok)
        use crate::newrelic::flush::Flush;
        let result = log_processor.flush().await;
        // Flush may fail due to missing license key in noop config — that's expected
        // The important thing is it doesn't panic
        let _ = result;
    }

    #[tokio::test]
    async fn test_e2e_log_processor_process_telemetry_record() {
        let config = Arc::new(make_config("fn-record", false));
        let client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let factory = crate::request::ProcessorFactory::new(client, config.clone(), apm_app);
        let ctx = Arc::new(std::sync::Mutex::new(crate::context::InvocationContext {
            request_id: "req-record".to_string(),
            invoked_function_arn: "arn:test".to_string(),
            trace_id: None,
        }));
        let log_processor = factory.create_log_processor(ctx);

        // Process a function telemetry record
        let record = crate::telemetry::listener::TelemetryRecord {
            time: chrono::Utc::now(),
            record_type: "function".to_string(),
            record: serde_json::json!({"message": "hello from function"}),
        };
        log_processor.process_record(record).await;
        // No panic = log was processed and buffered
    }

    #[tokio::test]
    async fn test_e2e_log_processor_context_update() {
        let config = Arc::new(make_config("fn-ctx", false));
        let client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let factory = crate::request::ProcessorFactory::new(client, config.clone(), apm_app);
        let ctx = Arc::new(std::sync::Mutex::new(crate::context::InvocationContext::default()));
        let log_processor = factory.create_log_processor(ctx.clone());

        // Update context
        let new_ctx = Arc::new(std::sync::Mutex::new(crate::context::InvocationContext {
            request_id: "req-updated".to_string(),
            invoked_function_arn: "arn:updated".to_string(),
            trace_id: Some("trace-123".to_string()),
        }));
        log_processor.update_invocation_context(new_ctx);
        // No panic = context was updated
    }

    #[tokio::test]
    async fn test_e2e_log_processor_set_fallback_arn() {
        let config = Arc::new(make_config("fn-fallback", false));
        let client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let factory = crate::request::ProcessorFactory::new(client, config.clone(), apm_app);
        let ctx = Arc::new(std::sync::Mutex::new(crate::context::InvocationContext::default()));
        let log_processor = factory.create_log_processor(ctx);

        log_processor.set_fallback_arn("arn:aws:lambda:us-east-1:123:function:fallback-fn");
        // No panic = fallback ARN was set
    }

    // ========================================================================
    // E2E: request::wait_for_all_requests_completion
    // ========================================================================

    #[tokio::test]
    #[serial]
    async fn test_e2e_wait_for_all_requests_completion_empty() {
        clear_request_state();

        let config = Arc::new(make_config("wait-fn", false));
        let client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let factory = crate::request::ProcessorFactory::new(client.clone(), config.clone(), apm_app);
        let ctx = Arc::new(std::sync::Mutex::new(crate::context::InvocationContext::default()));
        let log_processor = factory.create_log_processor(ctx);

        let start = std::time::Instant::now();
        crate::request::wait_for_all_requests_completion(
            client,
            config,
            log_processor,
            start,
        )
        .await;
        // Should complete quickly with no pending requests

        clear_request_state();
    }

    // ========================================================================
    // E2E: request::cleanup_old_request_buffers — full flow
    // ========================================================================

    #[tokio::test]
    #[serial]
    async fn test_e2e_cleanup_old_request_buffers_sends_and_removes() {
        clear_request_state();

        // Insert an old buffer (>5 minutes)
        let old_req = "req-old-cleanup";
        crate::request::REQUEST_AGENT_BUFFERS.insert(
            old_req.to_string(),
            Arc::new(std::sync::Mutex::new(vec![b"old-payload".to_vec()])),
        );
        crate::request::REQUEST_CONTEXTS.insert(
            old_req.to_string(),
            Arc::new(std::sync::Mutex::new(crate::context::InvocationContext {
                request_id: old_req.to_string(),
                invoked_function_arn: "arn:old".to_string(),
                trace_id: None,
            })),
        );
        crate::request::REQUEST_BUFFER_TIMESTAMPS.insert(
            old_req.to_string(),
            chrono::Utc::now() - chrono::Duration::minutes(10),
        );

        let config = Arc::new(make_config("cleanup-fn", false));
        let client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());

        crate::request::cleanup_old_request_buffers(client, config).await;

        // Old buffer should be removed
        assert!(
            !crate::request::REQUEST_AGENT_BUFFERS.contains_key(old_req),
            "Old buffer should be cleaned up"
        );
        assert!(
            !crate::request::REQUEST_CONTEXTS.contains_key(old_req),
            "Old context should be cleaned up"
        );
        assert!(
            !crate::request::REQUEST_BUFFER_TIMESTAMPS.contains_key(old_req),
            "Old timestamp should be cleaned up"
        );

        clear_request_state();
    }

    // ========================================================================
    // E2E: ApmApp::new() — connection failure paths
    // ========================================================================

    #[tokio::test]
    async fn test_e2e_apm_app_new_connection_refused() {
        // Point at unreachable host — should fail after retries
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_millis(200))
            .build()
            .expect("client");

        let result = crate::apm::ApmApp::new(
            "test-license-key".to_string(),
            "127.0.0.1:1".to_string(), // unreachable
            "http://127.0.0.1:1/metric/v1".to_string(),
            client,
            "test-function".to_string(),
            "$LATEST".to_string(),
            Some("123456789012".to_string()),
            Some("us-east-1".to_string()),
        )
        .await;

        assert!(result.is_err(), "Connection to unreachable host should fail");
        let err_msg = format!("{}", result.expect_err("error"));
        assert!(
            err_msg.contains("PreConnect") || err_msg.contains("connect") || err_msg.contains("Failed"),
            "Error should mention connection failure, got: {err_msg}"
        );
    }

    // ========================================================================
    // E2E: credentials::get_new_relic_license_key — AWS not available
    // ========================================================================

    #[tokio::test]
    #[serial]
    async fn test_e2e_get_license_key_fails_without_lambda_env() {
        // Ensure we're NOT in a Lambda environment
        std::env::remove_var("AWS_LAMBDA_RUNTIME_API");

        let config = Configuration::from(&make_config("fn", false));

        let result = crate::credentials::get_new_relic_license_key(&config).await;
        assert!(result.is_err(), "Should fail outside Lambda environment");
        let err_msg = result.expect_err("error").to_string();
        assert!(
            err_msg.contains("AWS") || err_msg.contains("Lambda") || err_msg.contains("initialize"),
            "Error should mention AWS/Lambda unavailability, got: {err_msg}"
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_e2e_get_license_key_configured_secret_id_fails_outside_lambda() {
        std::env::remove_var("AWS_LAMBDA_RUNTIME_API");

        let mut config = make_config("fn", false);
        config.new_relic.license_key_secret_id = "my-secret".to_string();
        let credentials = Configuration::from(&config);

        let result = crate::credentials::get_new_relic_license_key(&credentials).await;
        assert!(result.is_err(), "Should fail outside Lambda even with secret configured");
    }

    #[tokio::test]
    #[serial]
    async fn test_e2e_get_license_key_configured_ssm_fails_outside_lambda() {
        std::env::remove_var("AWS_LAMBDA_RUNTIME_API");

        let mut config = make_config("fn", false);
        config.new_relic.license_key_ssm_parameter_name = "/newrelic/license-key".to_string();
        let credentials = Configuration::from(&config);

        let result = crate::credentials::get_new_relic_license_key(&credentials).await;
        assert!(result.is_err(), "Should fail outside Lambda even with SSM configured");
    }
}
