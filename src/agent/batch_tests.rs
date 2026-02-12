#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::agent::batch::{
        build_batch_payload_json, split_into_chunks,
        estimate_item_size, estimate_base_overhead,
        BatchedAgentPayload,
    };
    use crate::config::ExtensionConfig;

    /// Helper: create a test payload with the given request_id and optional report
    fn make_payload(request_id: &str, report: Option<&str>, payload: &[u8]) -> BatchedAgentPayload {
        BatchedAgentPayload {
            request_id: request_id.to_string(),
            agent_payload_bytes: Arc::new(payload.to_vec()),
            report_line: report.map(String::from),
            invoked_function_arn: "arn:aws:lambda:us-east-1:123456:function:test-fn".to_string(),
            timestamp: chrono::Utc::now(),
        }
    }

    // ========================================================================
    // estimate_item_size
    // ========================================================================

    #[test]
    fn test_estimate_item_size_without_report() {
        let item = make_payload("r1", None, b"hello world");
        // 11 bytes payload + 150 overhead = 161
        assert_eq!(estimate_item_size(&item), 11 + 150);
    }

    #[test]
    fn test_estimate_item_size_with_report() {
        let item = make_payload("r1", Some("REPORT duration: 100ms"), b"hello");
        // 5 bytes payload + 22 bytes report + 150 + 150 = 327
        assert_eq!(estimate_item_size(&item), 5 + 22 + 150 + 150);
    }

    #[test]
    fn test_estimate_item_size_empty_payload() {
        let item = make_payload("r1", None, b"");
        assert_eq!(estimate_item_size(&item), 150);
    }

    // ========================================================================
    // estimate_base_overhead
    // ========================================================================

    #[test]
    fn test_estimate_base_overhead() {
        let config = Arc::new(ExtensionConfig::default());
        let overhead = estimate_base_overhead(&config);
        let expected = 500 + config.aws.function_name.len() * 3;
        assert_eq!(overhead, expected);
    }

    #[test]
    fn test_estimate_base_overhead_long_name() {
        let mut config = ExtensionConfig::default();
        config.aws.function_name = "a-very-long-function-name-for-testing-purposes".to_string();
        let config = Arc::new(config);
        let overhead = estimate_base_overhead(&config);
        let expected = 500 + config.aws.function_name.len() * 3;
        assert_eq!(overhead, expected);
    }

    // ========================================================================
    // split_into_chunks
    // ========================================================================

    #[test]
    fn test_split_into_chunks_single_chunk() {
        let config = Arc::new(ExtensionConfig::default());
        let payloads = vec![
            make_payload("r1", None, b"small"),
            make_payload("r2", None, b"small"),
        ];

        let chunks = split_into_chunks(payloads, 1_000_000, &config);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 2);
    }

    #[test]
    fn test_split_into_chunks_forces_split() {
        let config = Arc::new(ExtensionConfig::default());
        let big_data = vec![0u8; 5000];

        let payloads = vec![
            make_payload("r1", None, &big_data),
            make_payload("r2", None, &big_data),
            make_payload("r3", None, &big_data),
        ];

        // Each item ~5000 + 150 overhead = ~5150, base ~500
        // Max 6000 should fit only 1 item per chunk
        let chunks = split_into_chunks(payloads, 6000, &config);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 1);
        assert_eq!(chunks[1].len(), 1);
        assert_eq!(chunks[2].len(), 1);
    }

    #[test]
    fn test_split_into_chunks_empty_input() {
        let config = Arc::new(ExtensionConfig::default());
        let chunks = split_into_chunks(Vec::new(), 1_000_000, &config);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_into_chunks_oversized_single_item() {
        let config = Arc::new(ExtensionConfig::default());
        let huge = vec![0u8; 2_000_000];
        let payloads = vec![make_payload("r1", None, &huge)];

        // Single item exceeds max_size — still placed in its own chunk
        let chunks = split_into_chunks(payloads, 1_000_000, &config);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 1);
    }

    #[test]
    fn test_split_into_chunks_preserves_order() {
        let config = Arc::new(ExtensionConfig::default());
        let payloads = vec![
            make_payload("r1", None, b"first"),
            make_payload("r2", None, b"second"),
            make_payload("r3", None, b"third"),
        ];

        let chunks = split_into_chunks(payloads, 1_000_000, &config);
        assert_eq!(chunks[0][0].request_id, "r1");
        assert_eq!(chunks[0][1].request_id, "r2");
        assert_eq!(chunks[0][2].request_id, "r3");
    }

    // ========================================================================
    // build_batch_payload_json
    // ========================================================================

    #[test]
    fn test_build_batch_payload_json_structure() {
        let config = ExtensionConfig::default();
        let items = vec![make_payload("req-1", Some("REPORT duration: 50ms"), b"agent-data")];

        let json_str = build_batch_payload_json(&items, &config, "my-log-stream", None);
        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("valid JSON");

        // Top-level keys
        assert!(parsed.get("context").is_some());
        assert!(parsed.get("entry").is_some());

        // Context fields
        let ctx = &parsed["context"];
        assert!(ctx["function_name"].is_string());
        assert!(ctx["invoked_function_arn"].is_string());
        assert!(ctx["log_group_name"].is_string());
        assert!(ctx["log_stream_name"].is_string());

        // Entry is stringified JSON
        let entry_str = parsed["entry"].as_str().expect("entry should be string");
        let entry: serde_json::Value = serde_json::from_str(entry_str).expect("valid entry JSON");
        assert_eq!(entry["logStream"], "my-log-stream");

        let events = entry["logEvents"].as_array().expect("logEvents array");
        // 1 agent payload + 1 report line = 2 events
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["id"], "req-1");
        assert_eq!(events[0]["message"], "agent-data");
        assert_eq!(events[1]["message"], "REPORT duration: 50ms");
    }

    #[test]
    fn test_build_batch_payload_json_without_report() {
        let config = ExtensionConfig::default();
        let items = vec![make_payload("req-1", None, b"payload-only")];

        let json_str = build_batch_payload_json(&items, &config, "", None);
        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("valid JSON");

        let entry_str = parsed["entry"].as_str().unwrap();
        let entry: serde_json::Value = serde_json::from_str(entry_str).unwrap();
        let events = entry["logEvents"].as_array().unwrap();
        // Only agent payload, no report
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_build_batch_payload_json_multiple_items() {
        let config = ExtensionConfig::default();
        let items = vec![
            make_payload("req-1", Some("rpt-1"), b"data-1"),
            make_payload("req-2", None, b"data-2"),
            make_payload("req-3", Some("rpt-3"), b"data-3"),
        ];

        let json_str = build_batch_payload_json(&items, &config, "", None);
        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("valid JSON");

        let entry_str = parsed["entry"].as_str().unwrap();
        let entry: serde_json::Value = serde_json::from_str(entry_str).unwrap();
        let events = entry["logEvents"].as_array().unwrap();
        // req-1: agent + report = 2, req-2: agent only = 1, req-3: agent + report = 2
        assert_eq!(events.len(), 5);
    }

    #[test]
    fn test_build_batch_payload_uses_last_item_arn() {
        let config = ExtensionConfig::default();
        let mut item1 = make_payload("req-1", None, b"d1");
        item1.invoked_function_arn = "arn:first".to_string();
        let mut item2 = make_payload("req-2", None, b"d2");
        item2.invoked_function_arn = "arn:last".to_string();

        let json_str = build_batch_payload_json(&[item1, item2], &config, "", None);
        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("valid JSON");

        assert_eq!(parsed["context"]["invoked_function_arn"], "arn:last");
    }

    #[test]
    fn test_build_batch_payload_log_group_matches_function() {
        let mut config = ExtensionConfig::default();
        config.aws.function_name = "my-special-fn".to_string();
        let items = vec![make_payload("r1", None, b"d")];

        let json_str = build_batch_payload_json(&items, &config, "", None);
        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("valid JSON");

        assert_eq!(parsed["context"]["log_group_name"], "/aws/lambda/my-special-fn");

        let entry_str = parsed["entry"].as_str().unwrap();
        let entry: serde_json::Value = serde_json::from_str(entry_str).unwrap();
        assert_eq!(entry["logGroup"], "/aws/lambda/my-special-fn");
    }

    #[test]
    fn test_build_batch_payload_timestamps_are_positive() {
        let config = ExtensionConfig::default();
        let items = vec![make_payload("r1", None, b"data")];

        let json_str = build_batch_payload_json(&items, &config, "", None);
        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("valid JSON");

        let entry_str = parsed["entry"].as_str().unwrap();
        let entry: serde_json::Value = serde_json::from_str(entry_str).unwrap();
        let ts = entry["logEvents"][0]["timestamp"].as_i64().expect("should be number");
        assert!(ts > 0, "timestamp should be positive");
    }
}
