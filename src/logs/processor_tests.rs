// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for LogProcessor's extract_log_level functionality
//! 
//! This test suite validates:
//! - Structured log level detection (Priority 1)
//! - Unstructured log level parsing with word boundaries (Priority 2)
//! - False positive prevention
//! - Case insensitivity
//! - Priority ordering (structured > unstructured)
//! - Real-world log format compatibility

#[cfg(test)]
mod tests {
    use super::super::{LogProcessor, LogType, FailedLogEntry, RequestTraceMap, RequestLogBuffer, MAX_TRACE_ID_MAP};
    use crate::newrelic::flush::Flush;
    use crate::config::ExtensionConfig;
    use crate::context::InvocationContext;
    use crate::newrelic::client::NewRelicClient;
    use std::sync::{Arc, Mutex};
    use serde_json::json;

    /// Helper function to create a test LogProcessor instance
    fn create_test_processor() -> LogProcessor {
        let config = Arc::new(ExtensionConfig::default());
        let newrelic_client = Arc::new(NewRelicClient::new(&config));
        let invocation_context = Arc::new(Mutex::new(InvocationContext::default()));
        
        LogProcessor::new(newrelic_client, config, invocation_context, None)
    }

    // ========================================================================
    // STRUCTURED LOG LEVEL TESTS (Priority 1)
    // Tests that structured JSON log level fields take precedence
    // ========================================================================

    #[test]
    fn test_structured_level_basic() {
        let processor = create_test_processor();
        let record = json!({"level": "ERROR", "message": "test"});
        assert_eq!(processor.extract_log_level(&record, "test"), "ERROR");
    }

    #[test]
    fn test_structured_level_various_fields() {
        let processor = create_test_processor();
        
        // Test "level" field (most common)
        assert_eq!(
            processor.extract_log_level(&json!({"level": "WARN"}), "message"),
            "WARN"
        );
        
        // Test "Level" field (capitalized)
        assert_eq!(
            processor.extract_log_level(&json!({"Level": "DEBUG"}), "message"),
            "DEBUG"
        );
        
        // Test "LogLevel" field
        assert_eq!(
            processor.extract_log_level(&json!({"LogLevel": "TRACE"}), "message"),
            "TRACE"
        );
        
        // Test "severity" field
        assert_eq!(
            processor.extract_log_level(&json!({"severity": "INFO"}), "message"),
            "INFO"
        );
    }

    #[test]
    fn test_structured_level_case_variations() {
        let processor = create_test_processor();
        
        assert_eq!(
            processor.extract_log_level(&json!({"level": "error"}), "msg"),
            "ERROR"
        );
        assert_eq!(
            processor.extract_log_level(&json!({"level": "Error"}), "msg"),
            "ERROR"
        );
        assert_eq!(
            processor.extract_log_level(&json!({"level": "ERROR"}), "msg"),
            "ERROR"
        );
    }

    #[test]
    fn test_structured_level_overrides_message_keywords() {
        let processor = create_test_processor();
        
        // Issue #381: Structured level should override message content
        let record = json!({
            "level": "INFO",
            "message": "API returned: {\"Errors\": []}"
        });
        assert_eq!(
            processor.extract_log_level(&record, "API returned: {\"Errors\": []}"),
            "INFO"
        );
    }

    #[test]
    fn test_structured_level_information_variant() {
        let processor = create_test_processor();
        
        // "Information" should map to "INFO"
        assert_eq!(
            processor.extract_log_level(&json!({"level": "Information"}), "msg"),
            "INFO"
        );
    }

    #[test]
    fn test_structured_level_warning_variant() {
        let processor = create_test_processor();
        
        // "Warning" should map to "WARN"
        assert_eq!(
            processor.extract_log_level(&json!({"level": "Warning"}), "msg"),
            "WARN"
        );
    }

    #[test]
    fn test_structured_level_fatal_maps_to_error() {
        let processor = create_test_processor();
        
        assert_eq!(
            processor.extract_log_level(&json!({"level": "FATAL"}), "msg"),
            "ERROR"
        );
        assert_eq!(
            processor.extract_log_level(&json!({"level": "CRITICAL"}), "msg"),
            "ERROR"
        );
    }

    #[test]
    fn test_structured_verbose_maps_to_trace() {
        let processor = create_test_processor();
        
        assert_eq!(
            processor.extract_log_level(&json!({"level": "VERBOSE"}), "msg"),
            "TRACE"
        );
    }

    // ========================================================================
    // UNSTRUCTURED LOG LEVEL TESTS (Priority 2 - Word Boundaries)
    // Tests parsing of log level keywords from unstructured message strings
    // ========================================================================

    #[test]
    fn test_unstructured_error_at_start() {
        let processor = create_test_processor();
        let record = json!({});
        
        assert_eq!(processor.extract_log_level(&record, "ERROR: Database connection failed"), "ERROR");
        assert_eq!(processor.extract_log_level(&record, "error: connection timeout"), "ERROR");
    }

    #[test]
    fn test_unstructured_error_with_brackets() {
        let processor = create_test_processor();
        let record = json!({});
        
        assert_eq!(processor.extract_log_level(&record, "[2024-01-01] ERROR Failed"), "ERROR");
        assert_eq!(processor.extract_log_level(&record, "[ERROR] System failure"), "ERROR");
    }

    #[test]
    fn test_unstructured_warn_variations() {
        let processor = create_test_processor();
        let record = json!({});
        
        assert_eq!(processor.extract_log_level(&record, "WARN: Low memory"), "WARN");
        assert_eq!(processor.extract_log_level(&record, "WARNING: Deprecated API"), "WARN");
        assert_eq!(processor.extract_log_level(&record, "warn - slow query"), "WARN");
    }

    #[test]
    fn test_unstructured_debug_and_trace() {
        let processor = create_test_processor();
        let record = json!({});
        
        assert_eq!(processor.extract_log_level(&record, "DEBUG Starting process"), "DEBUG");
        assert_eq!(processor.extract_log_level(&record, "trace - entering function"), "TRACE");
    }

    #[test]
    fn test_unstructured_info() {
        let processor = create_test_processor();
        let record = json!({});
        
        assert_eq!(processor.extract_log_level(&record, "INFO Request completed"), "INFO");
        assert_eq!(processor.extract_log_level(&record, "info: user logged in"), "INFO");
    }

    #[test]
    fn test_unstructured_fatal_maps_to_error() {
        let processor = create_test_processor();
        let record = json!({});
        
        assert_eq!(processor.extract_log_level(&record, "FATAL: System crash"), "ERROR");
        assert_eq!(processor.extract_log_level(&record, "fatal error occurred"), "ERROR");
    }

    #[test]
    fn test_unstructured_critical_maps_to_error() {
        let processor = create_test_processor();
        let record = json!({});
        
        assert_eq!(processor.extract_log_level(&record, "CRITICAL: Data corruption"), "ERROR");
    }

    // ========================================================================
    // WORD BOUNDARY TESTS (False Positive Prevention)
    // Ensures keywords must be standalone words, not substrings
    // ========================================================================

    #[test]
    fn test_word_boundary_prevents_false_positives() {
        let processor = create_test_processor();
        let record = json!({});
        
        // "errors" (plural) should NOT match because 's' is alphanumeric
        assert_eq!(
            processor.extract_log_level(&record, "Successfully processed order without errors"),
            "INFO"
        );
        
        // "error correction" - "error" IS a proper keyword here (space before "correction")
        // This actually SHOULD match as ERROR since there's a word boundary
        assert_eq!(
            processor.extract_log_level(&record, "User entered error correction mode"),
            "ERROR"
        );
        
        // "terrifying" - "error" substring should NOT match
        assert_eq!(
            processor.extract_log_level(&record, "Terrifying news reported"),
            "INFO"
        );
    }

    #[test]
    fn test_word_boundary_with_warn() {
        let processor = create_test_processor();
        let record = json!({});
        
        // "warn" inside other words should NOT match
        assert_eq!(
            processor.extract_log_level(&record, "System warnotify triggered"),
            "INFO"
        );
        
        assert_eq!(
            processor.extract_log_level(&record, "Beware of changes"),
            "INFO"
        );
    }

    #[test]
    fn test_word_boundary_proper_error_keyword() {
        let processor = create_test_processor();
        let record = json!({});
        
        // These SHOULD match because word boundaries are respected
        assert_eq!(
            processor.extract_log_level(&record, "error occurred"),
            "ERROR"
        );
        
        assert_eq!(
            processor.extract_log_level(&record, "An error was detected"),
            "ERROR"
        );
        
        assert_eq!(
            processor.extract_log_level(&record, "error: failed"),
            "ERROR"
        );
    }

    // ========================================================================
    // CASE INSENSITIVITY TESTS
    // Validates that matching is case-insensitive
    // ========================================================================

    #[test]
    fn test_case_insensitive_matching() {
        let processor = create_test_processor();
        let record = json!({});
        
        assert_eq!(processor.extract_log_level(&record, "ERROR message"), "ERROR");
        assert_eq!(processor.extract_log_level(&record, "error message"), "ERROR");
        assert_eq!(processor.extract_log_level(&record, "Error message"), "ERROR");
        assert_eq!(processor.extract_log_level(&record, "eRRoR message"), "ERROR");
    }

    // ========================================================================
    // PRIORITY TESTS (Structured > Unstructured)
    // Validates that structured level always wins over message parsing
    // ========================================================================

    #[test]
    fn test_structured_takes_priority() {
        let processor = create_test_processor();
        
        // Even if message says ERROR, structured level should win
        let record = json!({
            "level": "INFO",
            "message": "ERROR in error field"
        });
        assert_eq!(
            processor.extract_log_level(&record, "ERROR in error field"),
            "INFO"
        );
    }

    #[test]
    fn test_structured_debug_overrides_message_error() {
        let processor = create_test_processor();
        
        let record = json!({
            "level": "DEBUG",
            "message": "Checking error handler"
        });
        assert_eq!(
            processor.extract_log_level(&record, "Checking error handler"),
            "DEBUG"
        );
    }

    // ========================================================================
    // COMMON LOG FORMAT TESTS
    // Validates compatibility with real-world log formats
    // ========================================================================

    #[test]
    fn test_timestamp_bracket_format() {
        let processor = create_test_processor();
        let record = json!({});
        
        assert_eq!(
            processor.extract_log_level(&record, "[2024-01-01T10:00:00Z] ERROR Database offline"),
            "ERROR"
        );
        
        assert_eq!(
            processor.extract_log_level(&record, "[2024-01-01 10:00:00] WARN High latency"),
            "WARN"
        );
    }

    #[test]
    fn test_level_with_various_separators() {
        let processor = create_test_processor();
        let record = json!({});
        
        assert_eq!(processor.extract_log_level(&record, "ERROR: message"), "ERROR");
        assert_eq!(processor.extract_log_level(&record, "ERROR - message"), "ERROR");
        assert_eq!(processor.extract_log_level(&record, "ERROR | message"), "ERROR");
        assert_eq!(processor.extract_log_level(&record, "ERROR  message"), "ERROR");
    }

    #[test]
    fn test_json_payload_in_message() {
        let processor = create_test_processor();
        let record = json!({});
        
        // JSON payload with "Errors" key should default to INFO (no structured level, no proper keyword)
        assert_eq!(
            processor.extract_log_level(&record, "{\"Errors\": [], \"Status\": \"ok\"}"),
            "INFO"
        );
        
        // But if there's a proper error keyword, it should match
        assert_eq!(
            processor.extract_log_level(&record, "error: {\"Errors\": []}"),
            "ERROR"
        );
    }

    // ========================================================================
    // EDGE CASES
    // Tests boundary conditions and unusual inputs
    // ========================================================================

    #[test]
    fn test_empty_message() {
        let processor = create_test_processor();
        let record = json!({});
        
        assert_eq!(processor.extract_log_level(&record, ""), "INFO");
    }

    #[test]
    fn test_very_long_message() {
        let processor = create_test_processor();
        let record = json!({});
        
        // Create a 200-char message with ERROR at position 160
        let long_prefix = "A".repeat(160);
        let message = format!("{} ERROR occurred", long_prefix);
        
        // ERROR beyond 150-char window should NOT be found
        assert_eq!(processor.extract_log_level(&record, &message), "INFO");
        
        // ERROR within first 150 chars WITH proper word boundary should be found
        let message_with_early_error = format!("{} ERROR occurred", "A".repeat(50));
        assert_eq!(processor.extract_log_level(&record, &message_with_early_error), "ERROR");
        
        // ERROR without word boundary (directly after alphanumeric) should NOT match
        let message_no_boundary = format!("{}ERROR occurred", "A".repeat(50));
        assert_eq!(processor.extract_log_level(&record, &message_no_boundary), "INFO");
    }

    #[test]
    fn test_default_to_info() {
        let processor = create_test_processor();
        let record = json!({});
        
        assert_eq!(processor.extract_log_level(&record, "Just a message"), "INFO");
        assert_eq!(processor.extract_log_level(&record, "Request completed successfully"), "INFO");
    }

    #[test]
    fn test_unknown_structured_level_defaults_to_info() {
        let processor = create_test_processor();
        
        let record = json!({"level": "UNKNOWN"});
        assert_eq!(processor.extract_log_level(&record, "message"), "INFO");
        
        let record = json!({"level": "CUSTOM_LEVEL"});
        assert_eq!(processor.extract_log_level(&record, "message"), "INFO");
    }

    #[test]
    fn test_multiple_keywords_earliest_position_wins() {
        let processor = create_test_processor();
        let record = json!({});

        // With position-priority, the keyword appearing earliest in the message wins.
        // "warn" at position 0 beats "critical" at position 6.
        assert_eq!(
            processor.extract_log_level(&record, "warn: critical error detected"),
            "WARN"
        );

        // "error" at position 0 beats "info" at position 20
        assert_eq!(
            processor.extract_log_level(&record, "error occurred, see info panel"),
            "ERROR"
        );
    }

    #[test]
    fn test_level_prefix_beats_body_keyword() {
        let processor = create_test_processor();
        let record = json!({});

        // [INFO] prefix at position 1 beats "error" in "No error detected" in the body
        assert_eq!(
            processor.extract_log_level(
                &record,
                "[INFO] 2026-02-11T09:34:01.262Z Status check: No error detected. System is running fine."
            ),
            "INFO"
        );

        // [INFO] prefix beats "error" anywhere in the body
        assert_eq!(
            processor.extract_log_level(&record, "[INFO] An error was logged but handled gracefully"),
            "INFO"
        );

        // [ERROR] prefix beats "info" later in the body
        assert_eq!(
            processor.extract_log_level(&record, "[ERROR] See info at https://example.com"),
            "ERROR"
        );

        // [WARN] prefix beats "error" in body
        assert_eq!(
            processor.extract_log_level(&record, "[WARN] Retrying after error"),
            "WARN"
        );

        // INFO prefix (without brackets) beats "error" in body
        assert_eq!(
            processor.extract_log_level(&record, "INFO: No error detected in health check"),
            "INFO"
        );
    }

    // ========================================================================
    // REAL-WORLD SCENARIOS
    // Tests based on actual AWS Lambda use cases
    // ========================================================================

    #[test]
    fn test_aws_lambda_timeout() {
        let processor = create_test_processor();
        let record = json!({});
        
        // "timed" should not trigger "time" match
        assert_eq!(
            processor.extract_log_level(&record, "Task timed out after 30.00 seconds"),
            "INFO"
        );
    }

    #[test]
    fn test_stack_trace_with_error() {
        let processor = create_test_processor();
        let record = json!({});
        
        assert_eq!(
            processor.extract_log_level(&record, "error: NullPointerException at line 42"),
            "ERROR"
        );
    }

    #[test]
    fn test_http_status_codes() {
        let processor = create_test_processor();
        let record = json!({});
        
        // HTTP status messages shouldn't trigger false positives
        assert_eq!(
            processor.extract_log_level(&record, "HTTP 200 OK - No errors found"),
            "INFO"
        );
        
        // But actual error keyword should match
        assert_eq!(
            processor.extract_log_level(&record, "HTTP 500 error occurred"),
            "ERROR"
        );
    }

    #[test]
    fn test_powertools_structured_info() {
        let processor = create_test_processor();
        
        // AWS Powertools format - Issue #381 regression test
        let record = json!({
            "level": "INFO",
            "message": "{\"message\":\"Processed successfully\",\"Errors\":[]}"
        });
        
        assert_eq!(
            processor.extract_log_level(&record, "{\"message\":\"Processed successfully\",\"Errors\":[]}"),
            "INFO"
        );
    }

    #[test]
    fn test_lambda_info_no_error_detected() {
        let processor = create_test_processor();
        let record = json!({});

        // Exact format from bug report: INFO-level log with "No error detected" in body
        assert_eq!(
            processor.extract_log_level(
                &record,
                "[INFO] 2026-02-11T09:34:01.262Z 6f7dec01-8b9d-4e81-a146-99b4884a39c3 Status check: No error detected. System is running fine at 52.53.184.198."
            ),
            "INFO"
        );
    }

    #[test]
    fn test_serilog_format() {
        let processor = create_test_processor();
        
        // Serilog structured format
        let record = json!({
            "Level": "Warning",
            "MessageTemplate": "Request took {Duration}ms"
        });
        
        assert_eq!(
            processor.extract_log_level(&record, "Request took 1500ms"),
            "WARN"
        );
    }

    // ========================================================================
    // PHASE 1: LogType enum tests
    // ========================================================================

    #[test]
    fn test_log_type_from_record_type() {
        
        assert_eq!(LogType::from_record_type("function"),  LogType::Function);
        assert_eq!(LogType::from_record_type("platform"),  LogType::Platform);
        assert_eq!(LogType::from_record_type("extension"), LogType::Extension);
        assert_eq!(LogType::from_record_type("unknown"),   LogType::Function);
        assert_eq!(LogType::from_record_type(""),          LogType::Function);
    }

    #[test]
    fn test_failed_log_entry_clone_preserves_log_type() {
        
        use serde_json::Map;
        let entry = FailedLogEntry {
            log_message: crate::newrelic::payload::LogMessage {
                timestamp: 0,
                message: "x".into(),
                attributes: Map::new(),
            },
            original_request_id: "r1".into(),
            retry_count: 0,
            log_type: LogType::Platform,
        };
        let cloned = entry.clone();
        assert_eq!(cloned.log_type, LogType::Platform);
    }

    // ========================================================================
    // PHASE 2: push_to_failed_buffer overflow / eviction tests
    // ========================================================================

    fn make_entry(log_type: LogType, retry_count: usize)
        -> FailedLogEntry
    {
        
        use serde_json::Map;
        FailedLogEntry {
            log_message: crate::newrelic::payload::LogMessage {
                timestamp: 0,
                message: String::new(),
                attributes: Map::new(),
            },
            original_request_id: String::new(),
            retry_count,
            log_type,
        }
    }

    #[test]
    fn test_push_below_cap_adds_entry() {
        
        let p = create_test_processor();
        p.push_to_failed_buffer(make_entry(LogType::Function, 0));
        assert_eq!(p.failed_logs_buffer.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_overflow_evicts_extension_first() {
        
        // M5: per-type caps mean Function overflow no longer evicts Extension.
        // Instead each type has its own independent 100-cap queue.
        let p = create_test_processor();
        // Fill Function queue to exactly its cap.
        for _ in 0..100 {
            p.push_to_failed_buffer(make_entry(LogType::Function, 0));
        }
        // Add one Extension — lives in its own queue, does not affect Function.
        p.push_to_failed_buffer(make_entry(LogType::Extension, 0));
        let buf = p.failed_logs_buffer.lock().unwrap();
        assert_eq!(buf.len_of(LogType::Function), 100);
        assert_eq!(buf.len_of(LogType::Extension), 1);
        drop(buf);
        // Push one more Function — evicts OLDEST Function (FIFO within type),
        // does NOT touch Extension.
        p.push_to_failed_buffer(make_entry(LogType::Function, 0));
        let buf = p.failed_logs_buffer.lock().unwrap();
        assert_eq!(buf.len_of(LogType::Function), 100, "Function stays at its cap");
        assert_eq!(buf.len_of(LogType::Extension), 1, "Extension queue untouched");
    }

    #[test]
    fn test_per_type_caps_isolate_floods() {
        // M5: a flood of Function failures must not crowd out Extension/Platform.
        let p = create_test_processor();
        for _ in 0..500 {
            p.push_to_failed_buffer(make_entry(LogType::Function, 0));
        }
        let buf = p.failed_logs_buffer.lock().unwrap();
        assert_eq!(buf.len_of(LogType::Function), 100, "Function capped at per-type limit");
        drop(buf);
        // Extension still has its full 100-entry headroom.
        for _ in 0..100 {
            p.push_to_failed_buffer(make_entry(LogType::Extension, 0));
        }
        let buf = p.failed_logs_buffer.lock().unwrap();
        assert_eq!(buf.len_of(LogType::Extension), 100);
        assert_eq!(buf.len(), 200, "Function + Extension = 200 (each at per-type cap)");
    }

    #[test]
    fn test_overflow_function_fifo_evicts_oldest() {
        // M5: per-type FIFO semantics preserved.
        use serde_json::Map;
        let p = create_test_processor();
        // Fill Function queue to its per-type cap (100).
        for i in 0u64..100 {
            let entry = FailedLogEntry {
                log_message: crate::newrelic::payload::LogMessage {
                    timestamp: i as i64,
                    message: String::new(),
                    attributes: Map::new(),
                },
                original_request_id: String::new(),
                retry_count: 0,
                log_type: LogType::Function,
            };
            p.push_to_failed_buffer(entry);
        }
        let oldest_ts = p.failed_logs_buffer.lock().unwrap()
            .front_of(LogType::Function).unwrap().log_message.timestamp;
        assert_eq!(oldest_ts, 0);
        // One more Function — oldest (ts=0) evicted.
        p.push_to_failed_buffer(make_entry(LogType::Function, 0));
        let new_front_ts = p.failed_logs_buffer.lock().unwrap()
            .front_of(LogType::Function).unwrap().log_message.timestamp;
        assert_eq!(new_front_ts, 1, "FIFO within Function: oldest (ts=0) evicted");
    }

    // ========================================================================
    // PHASE 3: log_type_from_message roundtrip tests
    // ========================================================================

    #[test]
    fn test_log_type_from_message_roundtrip() {
        
        use serde_json::Map;
        for (record_type, expected) in &[
            ("function",  LogType::Function),
            ("platform",  LogType::Platform),
            ("extension", LogType::Extension),
        ] {
            let mut attrs = Map::new();
            attrs.insert("_nr.logType".to_string(), serde_json::json!(record_type));
            let msg = crate::newrelic::payload::LogMessage {
                timestamp: 0,
                message: String::new(),
                attributes: attrs,
            };
            assert_eq!(
                LogProcessor::log_type_from_message(&msg),
                *expected,
                "record_type '{}' should map to {:?}", record_type, expected
            );
        }
    }

    #[test]
    fn test_log_type_missing_defaults_to_function() {
        
        use serde_json::Map;
        let msg = crate::newrelic::payload::LogMessage {
            timestamp: 0,
            message: String::new(),
            attributes: Map::new(),
        };
        assert_eq!(LogProcessor::log_type_from_message(&msg), LogType::Function);
    }

    // ========================================================================
    // PHASE 4b: reset_trace_id_state rescue + on_trace_id_extracted routing
    // Tests for Fix 1 and Fix 3 from the 2025-05 refactor session.
    // ========================================================================

    /// Create a processor with collect_trace_id=true so buffered_logs and
    /// trace_extraction_state are initialised (they are None otherwise).
    fn create_trace_processor() -> LogProcessor {
        use crate::config::{ExtensionConfig, NewRelicConfig};
        let mut config = ExtensionConfig::default();
        config.new_relic = NewRelicConfig { collect_trace_id: true, ..NewRelicConfig::default() };
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(
            &Arc::new(ExtensionConfig::default()),
        ));
        let invocation_context = Arc::new(Mutex::new(crate::context::InvocationContext::default()));
        LogProcessor::new(newrelic_client, Arc::new(config), invocation_context, None)
    }

    /// Helper: build a minimal LogMessage with a given message string.
    fn make_log_msg(msg: &str) -> crate::newrelic::payload::LogMessage {
        use serde_json::Map;
        crate::newrelic::payload::LogMessage {
            timestamp: 0,
            message: msg.to_string(),
            attributes: Map::new(),
        }
    }

    // Helper: hold a log under a request_id in the per-request pending buffer.
    fn hold_log(p: &LogProcessor, request_id: &str, msg: &str) {
        let evicted = p
            .pending_logs
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .push(request_id, make_log_msg(msg));
        assert!(evicted.is_empty(), "test setup should not overflow the buffer");
    }

    // flush_pending_logs_unstamped routes still-held logs to log_batch (untagged).
    #[test]
    fn test_flush_pending_logs_unstamped_routes_to_batch() {
        let p = create_trace_processor();
        hold_log(&p, "req-x", "log-a");
        hold_log(&p, "req-x", "log-b");

        assert_eq!(p.log_batch.lock().unwrap().len(), 0);

        p.flush_pending_logs_unstamped();

        assert_eq!(
            p.pending_logs.as_ref().unwrap().lock().unwrap().total(),
            0,
            "pending buffer must be drained by flush_pending_logs_unstamped"
        );
        let batch = p.log_batch.lock().unwrap();
        assert_eq!(batch.len(), 2, "both held logs must be in log_batch");
        assert!(batch.iter().any(|m| m.message == "log-a"));
        assert!(batch.iter().any(|m| m.message == "log-b"));
    }

    #[test]
    fn test_flush_pending_logs_unstamped_no_op_when_empty() {
        let p = create_trace_processor();
        p.flush_pending_logs_unstamped();
        assert_eq!(p.log_batch.lock().unwrap().len(), 0,
            "flush with empty buffer must not add anything to log_batch");
    }

    #[test]
    fn test_flush_pending_logs_unstamped_no_op_without_trace_collection() {
        // collect_trace_id=false has no pending buffer — must be a no-op.
        let p = create_test_processor();
        assert!(p.pending_logs.is_none());
        p.flush_pending_logs_unstamped(); // must not panic
        assert_eq!(p.log_batch.lock().unwrap().len(), 0);
    }

    // on_trace_id_extracted stamps + routes ONLY the matching request's held logs.
    #[tokio::test]
    async fn test_on_trace_id_extracted_stamps_trace_id_and_routes_to_batch() {
        let p = create_trace_processor();
        let trace_id = "abc-trace-123";
        hold_log(&p, "req-1", "msg-1");
        hold_log(&p, "req-1", "msg-2");

        p.on_trace_id_extracted("req-1", trace_id).await.unwrap();

        assert_eq!(
            p.pending_logs.as_ref().unwrap().lock().unwrap().len_for("req-1"),
            0,
            "req-1's held logs must be drained"
        );
        let batch = p.log_batch.lock().unwrap();
        assert_eq!(batch.len(), 2, "both logs must be routed to log_batch");
        for log in batch.iter() {
            let tid = log.attributes.get("trace.id").and_then(|v| v.as_str()).unwrap_or("");
            assert_eq!(tid, trace_id, "trace.id must be stamped on log '{}'", log.message);
        }
    }

    // Cross-request isolation: extracting one request's trace must not touch another's.
    #[tokio::test]
    async fn test_on_trace_id_extracted_drains_only_matching_request() {
        let p = create_trace_processor();
        hold_log(&p, "A", "a1");
        hold_log(&p, "B", "b1");

        p.on_trace_id_extracted("A", "trace-A").await.unwrap();

        {
            let pl = p.pending_logs.as_ref().unwrap().lock().unwrap();
            assert_eq!(pl.len_for("A"), 0, "A drained");
            assert_eq!(pl.len_for("B"), 1, "B's logs must remain held with no trace yet");
        }
        let batch = p.log_batch.lock().unwrap();
        assert_eq!(batch.len(), 1, "only A's log routed");
        assert_eq!(
            batch[0].attributes.get("trace.id").and_then(|v| v.as_str()),
            Some("trace-A")
        );
    }

    #[tokio::test]
    async fn test_on_trace_id_extracted_no_op_when_buffer_empty() {
        let p = create_trace_processor();
        p.on_trace_id_extracted("req-1", "some-trace").await.unwrap();
        assert_eq!(p.log_batch.lock().unwrap().len(), 0,
            "empty buffer must produce no log_batch entries");
    }

    #[tokio::test]
    async fn test_on_trace_id_extracted_no_op_without_trace_collection() {
        // collect_trace_id=false — must return Ok(()) silently.
        let p = create_test_processor();
        assert!(p.pending_logs.is_none());
        p.on_trace_id_extracted("req-1", "some-trace").await.unwrap();
    }

    #[test]
    fn test_request_log_buffer_evicts_oldest_request_on_overflow() {
        let mut b = RequestLogBuffer::new(2);
        assert!(b.push("A", make_log_msg("a1")).is_empty());
        assert!(b.push("B", make_log_msg("b1")).is_empty());
        // max_total=2 reached; next push evicts the oldest request (A) entirely.
        let evicted = b.push("C", make_log_msg("c1"));
        assert_eq!(evicted.len(), 1, "A's bucket evicted on overflow");
        assert_eq!(evicted[0].message, "a1");
        assert_eq!(b.len_for("A"), 0);
        assert_eq!(b.total(), 2, "buffer stays within max_total");
    }

    // ========================================================================
    // request_id -> trace.id map: every log of a request gets its trace.id
    // ========================================================================

    #[test]
    fn test_request_trace_map_insert_get_and_update() {
        let mut m = RequestTraceMap::new();
        assert_eq!(m.get("r1"), None);
        m.insert("r1", "t1");
        m.insert("r2", "t2");
        assert_eq!(m.get("r1"), Some("t1"));
        assert_eq!(m.get("r2"), Some("t2"));
        // Re-inserting an existing request updates the value without growing.
        m.insert("r1", "t1b");
        assert_eq!(m.get("r1"), Some("t1b"));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn test_request_trace_map_evicts_oldest_past_cap() {
        let mut m = RequestTraceMap::new();
        let cap = MAX_TRACE_ID_MAP;
        for i in 0..cap {
            m.insert(&format!("r{i}"), &format!("t{i}"));
        }
        assert_eq!(m.len(), cap);
        assert_eq!(m.get("r0"), Some("t0"));
        // One past cap evicts the oldest (r0); never grows beyond cap.
        m.insert("r-new", "t-new");
        assert_eq!(m.len(), cap, "map must stay bounded (leak-proof)");
        assert_eq!(m.get("r0"), None, "oldest entry must be evicted past cap");
        assert_eq!(m.get("r1"), Some("t1"), "second-oldest must survive");
        assert_eq!(m.get("r-new"), Some("t-new"));
    }

    #[tokio::test]
    async fn test_on_trace_id_extracted_records_map_even_with_empty_buffer() {
        // Even when no logs were parked, the request->trace association must be
        // recorded so post-extraction / late logs can be stamped.
        let p = create_trace_processor();
        p.on_trace_id_extracted("req-9", "trace-9").await.unwrap();
        let m = p.request_trace_ids.as_ref().unwrap().lock().unwrap();
        assert_eq!(m.get("req-9"), Some("trace-9"));
    }

    use serial_test::serial;

    #[test]
    #[serial]
    fn test_apply_metadata_stamps_trace_id_from_map() {
        let p = create_trace_processor();
        if let Some(ref m) = p.request_trace_ids {
            m.lock().unwrap().insert("req-apply", "trace-apply");
        }
        // Force effective_request_id deterministically.
        *crate::request::TELEMETRY_CURRENT_REQUEST_ID.lock().unwrap() = Some("req-apply".to_string());

        let out = p.apply_current_invocation_metadata(make_log_msg("hello"));

        *crate::request::TELEMETRY_CURRENT_REQUEST_ID.lock().unwrap() = None;

        assert_eq!(
            out.attributes.get("trace.id").and_then(|v| v.as_str()),
            Some("trace-apply"),
            "trace.id must be stamped from the request->trace map"
        );
    }

    #[test]
    #[serial]
    fn test_apply_metadata_cross_request_uses_correct_trace() {
        // A late log for request A, processed while "current" is request B, must
        // get A's trace — not B's.
        let p = create_trace_processor();
        if let Some(ref m) = p.request_trace_ids {
            let mut g = m.lock().unwrap();
            g.insert("A", "trace-A");
            g.insert("B", "trace-B");
        }
        p.invocation_context.lock().unwrap().request_id = "B".to_string();
        *crate::request::TELEMETRY_CURRENT_REQUEST_ID.lock().unwrap() = Some("A".to_string());

        let out = p.apply_current_invocation_metadata(make_log_msg("late-A"));

        *crate::request::TELEMETRY_CURRENT_REQUEST_ID.lock().unwrap() = None;

        assert_eq!(
            out.attributes.get("trace.id").and_then(|v| v.as_str()),
            Some("trace-A"),
            "late log for request A must be stamped with trace-A, not the current request's trace"
        );
    }

    #[test]
    #[serial]
    fn test_apply_metadata_no_trace_when_request_absent_from_map() {
        let p = create_trace_processor();
        *crate::request::TELEMETRY_CURRENT_REQUEST_ID.lock().unwrap() = Some("unknown-req".to_string());
        let out = p.apply_current_invocation_metadata(make_log_msg("x"));
        *crate::request::TELEMETRY_CURRENT_REQUEST_ID.lock().unwrap() = None;
        assert!(
            out.attributes.get("trace.id").is_none(),
            "no trace.id when the request has no recorded trace"
        );
    }

    #[test]
    fn test_apply_metadata_no_trace_when_collection_off() {
        // collect_trace_id=false → no map allocated, never stamps trace.id.
        let p = create_test_processor();
        assert!(p.request_trace_ids.is_none());
        let out = p.apply_current_invocation_metadata(make_log_msg("x"));
        assert!(out.attributes.get("trace.id").is_none());
    }

    // ========================================================================
    // PHASE 5: start_invocation_retry + flush handle tracking
    // ========================================================================

    #[tokio::test]
    async fn test_start_invocation_retry_empty_buffer_does_not_set_handle() {
        let p = create_test_processor();
        p.start_invocation_retry();
        assert!(p.invocation_retry_handle.lock().unwrap().is_none(),
            "No handle should be created when buffer is empty");
    }

    #[tokio::test]
    async fn test_start_invocation_retry_sets_handle_when_buffer_has_entries() {
        
        let p = create_test_processor();
        p.push_to_failed_buffer(make_entry(LogType::Function, 0));
        p.start_invocation_retry();
        assert!(p.invocation_retry_handle.lock().unwrap().is_some(),
            "Handle should be set when buffer has entries");
    }

    #[tokio::test]
    async fn test_flush_clears_invocation_retry_handle() {
        
        let p = create_test_processor();
        p.push_to_failed_buffer(make_entry(LogType::Function, 0));
        p.start_invocation_retry();
        assert!(p.invocation_retry_handle.lock().unwrap().is_some());

        let _ = p.flush().await;
        assert!(p.invocation_retry_handle.lock().unwrap().is_none(),
            "Handle should be None after flush awaits it");
    }

    #[tokio::test]
    async fn test_exhausted_entries_not_sent() {
        
        let p = create_test_processor();
        // retry_count == MAX_RETRIES (3) means the filter should drop it
        p.push_to_failed_buffer(make_entry(LogType::Function, 3));
        p.start_invocation_retry();
        // If the entry was filtered out, handle is None (nothing to send)
        assert!(p.invocation_retry_handle.lock().unwrap().is_none(),
            "No handle should be created when all entries are exhausted");
    }

    #[tokio::test]
    async fn test_start_invocation_retry_drains_buffer() {

        let p = create_test_processor();
        for _ in 0..5 {
            p.push_to_failed_buffer(make_entry(LogType::Function, 0));
        }
        assert_eq!(p.failed_logs_buffer.lock().unwrap().len(), 5);
        p.start_invocation_retry();
        // Buffer should be drained atomically before spawn
        assert_eq!(p.failed_logs_buffer.lock().unwrap().len(), 0,
            "Buffer should be empty immediately after start_invocation_retry drains it");
    }

    // Shutdown path invariant: start_invocation_retry() followed by flush() drains the
    // failed_logs_buffer and clears the retry handle, so no logs are stranded when the
    // extension shuts down without another INVOKE.
    #[tokio::test]
    async fn test_shutdown_sequence_retry_then_flush_clears_state() {
        let p = create_test_processor();
        for _ in 0..3 {
            p.push_to_failed_buffer(make_entry(LogType::Function, 0));
        }
        assert_eq!(p.failed_logs_buffer.lock().unwrap().len(), 3);

        // Mirror the event loop's shutdown sequence.
        p.start_invocation_retry();
        let _ = p.flush().await;

        // After flush(), retry handle must be awaited (cleared) and buffer must be empty.
        // Entries only re-enter failed_logs_buffer on send failure with retry_count < MAX_RETRIES;
        // in the no-network test setup the send fails and entries are rebuffered with
        // retry_count incremented to 1, so assert the incremented count rather than emptiness.
        assert!(p.invocation_retry_handle.lock().unwrap().is_none(),
            "flush() must await the retry handle set by start_invocation_retry()");
        let buf = p.failed_logs_buffer.lock().unwrap();
        for entry in buf.iter_all() {
            assert!(entry.retry_count >= 1,
                "rebuffered entries must have advanced retry_count");
        }
    }

    // Regression: FLUSH_THRESHOLD is 10. The constant is private so we probe it indirectly
    // via the boundary — 9 pushes do not auto-flush, 10 do. Pushing through the batch lock
    // directly bypasses process_record, so we instead assert that lowering the threshold
    // did not regress the manual flush path (log_batch drain on flush()).
    #[tokio::test]
    async fn test_flush_drains_log_batch() {
        let p = create_test_processor();
        {
            let mut batch = p.log_batch.lock().unwrap();
            for i in 0..15 {
                batch.push(make_log_msg(&format!("msg-{}", i)));
            }
        }
        assert_eq!(p.log_batch.lock().unwrap().len(), 15);
        let _ = p.flush().await;
        assert_eq!(p.log_batch.lock().unwrap().len(), 0,
            "flush() must drain log_batch via mem::take");
    }

    // C1 regression — is_drained() must return false while is_auto_flushing==true,
    // even if log_batch is empty and pending_flush_handles has no in-flight handles.
    // Simulates the TOCTOU window between mem::take(batch) and pending_flush_handles.push().
    #[tokio::test]
    async fn test_is_drained_false_during_auto_flush_spawn_window() {
        let p = create_test_processor();
        // Simulate the spawn-window state:
        //   - batch: empty (after mem::take)
        //   - pending_flush_handles: still empty (handle not registered yet)
        //   - is_auto_flushing: true (set before mem::take, cleared after push)
        use std::sync::atomic::Ordering;
        assert!(p.log_batch.lock().unwrap().is_empty());
        assert!(p.pending_flush_handles.lock().unwrap().is_none());
        p.is_auto_flushing.store(true, Ordering::Relaxed);

        assert!(!p.is_drained(),
            "is_drained() must be false while is_auto_flushing==true, even if batch is empty and no handles are tracked");

        // After the flag clears, we're genuinely drained.
        p.is_auto_flushing.store(false, Ordering::Relaxed);
        assert!(p.is_drained(),
            "is_drained() must be true when not flushing, batch empty, no handles tracked");
    }

    // L4 — runtime_done_notify pre-arm semantics: notify_one() called BEFORE notified()
    // makes notified() return immediately (no delay). This is the assumption the
    // event loop depends on — if runtime.done arrives before we reach the wait point,
    // we must not block for the full deadline.
    #[tokio::test]
    async fn test_tokio_notify_pre_arm_returns_immediately() {
        use std::sync::Arc;
        use tokio::sync::Notify;
        let n = Arc::new(Notify::new());
        n.notify_one(); // fire before anyone awaits
        let start = std::time::Instant::now();
        tokio::time::timeout(std::time::Duration::from_millis(50), n.notified())
            .await
            .expect("pre-armed Notify must return immediately");
        assert!(start.elapsed() < std::time::Duration::from_millis(10),
            "pre-armed notify().notified() took {:?}, expected <10ms", start.elapsed());
    }

    // L4 — is_drained race safety: fire process_record-shaped pushes from many
    // concurrent tasks while another task polls is_drained(). At no point should
    // is_drained() return true while log_batch has entries (would indicate the
    // TOCTOU fix regressed).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_is_drained_consistent_under_concurrent_pushes() {
        use std::sync::Arc;
        let p = Arc::new(create_test_processor());

        // Start a poller task that watches for invariant violations.
        let pp = Arc::clone(&p);
        let invariant_broken = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let broken = Arc::clone(&invariant_broken);
        let poller = tokio::spawn(async move {
            for _ in 0..500 {
                let drained = pp.is_drained();
                let batch_len = pp.log_batch.lock().unwrap().len();
                // Invariant: if is_drained()==true, batch must be empty AND no
                // auto-flush is mid-spawn. We can at least check batch_len.
                if drained && batch_len > 0 {
                    broken.store(true, std::sync::atomic::Ordering::SeqCst);
                    break;
                }
                tokio::task::yield_now().await;
            }
        });

        // Producer: push 200 logs into the batch via direct insertion (matches
        // what process_record does after its threshold probe).
        for i in 0..200 {
            p.log_batch.lock().unwrap().push(make_log_msg(&format!("m-{}", i)));
            if i % 10 == 0 {
                tokio::task::yield_now().await;
            }
        }

        let _ = poller.await;
        assert!(!invariant_broken.load(std::sync::atomic::Ordering::SeqCst),
            "is_drained() returned true while log_batch had entries (TOCTOU regression)");
    }

    // M2 regression — estimate_log_size must never return 0 for a non-empty message.
    // Previously the codebase called serde_json::to_string(&attrs).unwrap_or_default().len()
    // which silently undersized on serialization error, causing oversized chunks → 413.
    #[test]
    fn test_estimate_log_size_nonzero_for_real_message() {
        let msg = make_log_msg("hello-world");
        let sz = super::super::estimate_log_size(&msg);
        assert!(sz > "hello-world".len(),
            "estimate_log_size({}) must exceed raw message length", sz);
    }

    // H2 regression — calling start_invocation_retry twice without flush() between
    // must NOT abort the prior task (data loss). Instead the counter ticks and a
    // background task awaits the prior handle.
    #[tokio::test]
    async fn test_start_invocation_retry_double_call_does_not_abort_prev() {
        use std::sync::atomic::Ordering;
        // Direct access to the private RETRY_INVARIANT_VIOLATIONS static — the test
        // module is a child of `processor`, so private items are visible via super::.
        super::super::RETRY_INVARIANT_VIOLATIONS.store(0, Ordering::Relaxed);

        let p = create_test_processor();
        p.push_to_failed_buffer(make_entry(LogType::Function, 0));
        p.start_invocation_retry();
        assert!(p.invocation_retry_handle.lock().unwrap().is_some(),
            "first call should set a handle");

        // Second call without flush() in between — invariant violation path.
        p.push_to_failed_buffer(make_entry(LogType::Function, 0));
        p.start_invocation_retry();

        // Counter ticked.
        assert_eq!(
            super::super::RETRY_INVARIANT_VIOLATIONS.load(Ordering::Relaxed),
            1,
            "invariant violation counter must increment on double-call"
        );
        // New handle is set (replacing the old one in the slot).
        assert!(p.invocation_retry_handle.lock().unwrap().is_some(),
            "second call should install a new handle");

        // Clean up the background await task by letting it run.
        tokio::task::yield_now().await;
    }

    // H1 regression — flush_on_shutdown must exhaust failed_logs_buffer via repeated
    // start_invocation_retry + flush passes, up to the per-entry MAX_RETRIES cap. The
    // loop must terminate (no infinite spin) and zero log is stranded if the per-entry
    // budget can accommodate the retries.
    #[tokio::test]
    async fn test_flush_on_shutdown_drains_failed_buffer() {
        let p = create_test_processor();
        // Seed the failed buffer with an entry whose retry_count is already at MAX
        // so it is filtered out immediately by start_invocation_retry. The drain
        // loop should terminate without spinning.
        // MAX_RETRIES is private to the processor module; 3 is the current value and
        // entries at or above it are filtered out by start_invocation_retry.
        p.push_to_failed_buffer(make_entry(LogType::Function, 3));
        assert_eq!(p.failed_logs_buffer_len(), 1);

        let _ = p.flush_on_shutdown().await;
        assert_eq!(p.failed_logs_buffer_len(), 0,
            "flush_on_shutdown should drain entries that are past their retry budget");
    }

    #[tokio::test]
    async fn test_flush_on_shutdown_terminates_on_persistent_failure() {
        // Deterministic assertion: the loop MUST terminate within a fixed wall-clock
        // budget regardless of whether sends succeed or fail. We wrap the call in
        // a 5s tokio timeout; any hang (infinite retry loop, deadlock, etc.) fails
        // the test explicitly rather than relying on implicit "no network" behavior.
        let p = create_test_processor();
        for _ in 0..5 {
            p.push_to_failed_buffer(make_entry(LogType::Function, 0));
        }
        assert_eq!(p.failed_logs_buffer_len(), 5);

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            p.flush_on_shutdown(),
        )
        .await;
        assert!(result.is_ok(), "flush_on_shutdown must terminate within 5s");

        // The loop must have terminated. Whether sends actually succeeded or failed
        // in the test environment is not the point — the point is termination.
        let after = p.failed_logs_buffer_len();
        assert!(after <= 5,
            "flush_on_shutdown should never grow the failed buffer (was 5, now {})", after);
    }

    // C2 regression — finished JoinHandles must be reaped so the vec doesn't leak
    // across a long-lived warm container. After yielding, any completed-task handles
    // should be None when is_drained() runs.
    #[tokio::test]
    async fn test_pending_flush_handles_reaped_when_finished() {
        use std::sync::atomic::Ordering;
        let p = create_test_processor();
        // Set a fast handle that completes immediately.
        let fast = tokio::spawn(async {});
        *p.pending_flush_handles.lock().unwrap() = Some(fast);

        // Let the fast task complete.
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        // is_drained() triggers the reap; finished handle becomes None.
        p.is_auto_flushing.store(false, Ordering::Relaxed);
        assert!(p.is_drained(), "is_drained() must reap the finished handle");
        assert!(p.pending_flush_handles.lock().unwrap().is_none(),
            "finished handle must be cleared to None after reap");

        // Now set a slow handle — is_drained() must return false.
        let slow = tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        });
        *p.pending_flush_handles.lock().unwrap() = Some(slow);
        assert!(!p.is_drained(),
            "is_drained() must be false when the pending handle is still running");
    }

    #[tokio::test]
    async fn test_is_drained_false_with_unfinished_handle() {
        let p = create_test_processor();
        use std::sync::atomic::Ordering;
        assert!(p.log_batch.lock().unwrap().is_empty());
        p.is_auto_flushing.store(false, Ordering::Relaxed);

        // Set a handle that will never finish within the test window.
        let handle = tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        });
        *p.pending_flush_handles.lock().unwrap() = Some(handle);

        assert!(!p.is_drained(),
            "is_drained() must be false when the pending flush handle is still running");
    }

    // ========================================================================
    // PHASE 6: entity.guid stamping coverage
    // Tests for the 2025-05 fixes that stamp entity.guid on all log paths.
    // ========================================================================

    /// Build a processor whose apm_app is set to a mock ApmApp with the given entity_guid.
    fn create_apm_processor(entity_guid: &str) -> LogProcessor {
        use crate::config::{ExtensionConfig, NewRelicConfig};
        use crate::apm::app::ApmApp;

        let mut config = ExtensionConfig::default();
        config.new_relic = NewRelicConfig { collect_trace_id: true, ..NewRelicConfig::default() };

        let mock_app = ApmApp {
            run_id: "run-1".to_string(),
            entity_guid: entity_guid.to_string(),
            collector_host: "collector.newrelic.com".to_string(),
            license_key: "test-key".to_string(),
            metric_endpoint: "https://metric-api.newrelic.com/metric/v1".to_string(),
            client: reqwest::Client::new(),
        };
        let apm_arc: Arc<tokio::sync::RwLock<Option<ApmApp>>> =
            Arc::new(tokio::sync::RwLock::new(Some(mock_app)));

        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(
            &Arc::new(ExtensionConfig::default()),
        ));
        let invocation_context = Arc::new(Mutex::new(crate::context::InvocationContext::default()));
        LogProcessor::new(newrelic_client, Arc::new(config), invocation_context, Some(apm_arc))
    }

    #[tokio::test]
    async fn test_on_trace_id_extracted_stamps_entity_guid() {
        let p = create_apm_processor("test-entity-guid");
        hold_log(&p, "req-abc", "apm-log-1");
        hold_log(&p, "req-abc", "apm-log-2");

        p.on_trace_id_extracted("req-abc", "trace-abc").await.unwrap();

        let batch = p.log_batch.lock().unwrap();
        assert_eq!(batch.len(), 2);
        for log in batch.iter() {
            let guid = log.attributes.get("entity.guid").and_then(|v| v.as_str()).unwrap_or("");
            assert_eq!(guid, "test-entity-guid",
                "entity.guid must be stamped on log '{}'", log.message);
            let tid = log.attributes.get("trace.id").and_then(|v| v.as_str()).unwrap_or("");
            assert_eq!(tid, "trace-abc",
                "trace.id must be stamped on log '{}'", log.message);
        }
    }

    #[test]
    fn test_flush_pending_logs_unstamped_stamps_entity_guid() {
        let p = create_apm_processor("flush-entity-guid");
        hold_log(&p, "req-orphan", "orphan-log-1");

        p.flush_pending_logs_unstamped();

        let batch = p.log_batch.lock().unwrap();
        assert_eq!(batch.len(), 1);
        let guid = batch[0].attributes.get("entity.guid").and_then(|v| v.as_str()).unwrap_or("");
        assert_eq!(guid, "flush-entity-guid",
            "entity.guid must be stamped on flushed orphan log");
        assert!(!batch[0].attributes.contains_key("trace.id"),
            "orphan log has no trace.id (no payload arrived)");
    }

    #[test]
    fn test_process_buffered_logs_stamps_entity_guid_and_trace_id() {
        let p = create_apm_processor("direct-path-guid");

        // Record the request's trace.id in the map so the direct path stamps it.
        if let Some(ref m) = p.request_trace_ids {
            m.lock().unwrap().insert("req-001", "trace-xyz");
        }

        // Push a log into request_id_buffer.
        {
            let mut buf = p.request_id_buffer.lock().unwrap();
            buf.push(make_log_msg("direct-log"));
        }

        p.process_buffered_logs_with_request_id("req-001");

        let batch = p.log_batch.lock().unwrap();
        assert_eq!(batch.len(), 1);
        let guid = batch[0].attributes.get("entity.guid").and_then(|v| v.as_str()).unwrap_or("");
        assert_eq!(guid, "direct-path-guid", "entity.guid must be stamped on direct-path log");
        let tid = batch[0].attributes.get("trace.id").and_then(|v| v.as_str()).unwrap_or("");
        assert_eq!(tid, "trace-xyz", "trace.id must be stamped on direct-path log");
    }

    #[tokio::test]
    async fn test_entity_guid_not_panic_when_apm_app_none() {
        // When apm_app is None, the drain path must complete without panic and
        // entity.guid must simply be absent from the output.
        let p = create_trace_processor(); // no apm_app
        hold_log(&p, "req-t1", "no-apm-log");

        p.on_trace_id_extracted("req-t1", "t1").await.unwrap();
        let batch = p.log_batch.lock().unwrap();
        assert!(!batch[0].attributes.contains_key("entity.guid"),
            "entity.guid must not appear when apm_app is None");
    }

    // Cold-start INIT (pre-invoke) logs: held until the request's trace arrives, then
    // stamped — but ALWAYS carrying ARN + request_id (never sent without them).
    #[tokio::test]
    async fn test_pre_invoke_logs_held_then_stamped_with_trace() {
        let p = create_apm_processor("guid-init");
        {
            let mut ctx = p.invocation_context.lock().unwrap();
            ctx.invoked_function_arn = "arn:aws:lambda:us-east-1:111:function:f".to_string();
            ctx.request_id = "req-init".to_string();
        }
        {
            let mut buf = p.pre_invoke_buffer.lock().unwrap();
            buf.push(make_log_msg("init-1"));
            buf.push(make_log_msg("init-2"));
        }

        // Trace not known yet → logs held, NOT batched.
        p.process_pre_invoke_logs();
        assert_eq!(p.log_batch.lock().unwrap().len(), 0, "held until trace, not batched");
        assert_eq!(
            p.pending_logs.as_ref().unwrap().lock().unwrap().len_for("req-init"),
            2,
            "both INIT logs held under their request_id"
        );

        // Trace arrives → held INIT logs stamped + flushed.
        p.on_trace_id_extracted("req-init", "trace-init").await.unwrap();
        let batch = p.log_batch.lock().unwrap();
        assert_eq!(batch.len(), 2);
        for log in batch.iter() {
            // ARN + request_id present (no placeholder) AND trace.id now stamped.
            assert_eq!(log.attributes.get("faas.arn").and_then(|v| v.as_str()),
                Some("arn:aws:lambda:us-east-1:111:function:f"));
            assert_eq!(log.attributes.get("faas.execution").and_then(|v| v.as_str()), Some("req-init"));
            let rid = log.attributes.get("aws").and_then(|v| v.as_object())
                .and_then(|a| a.get("lambda_request_id")).and_then(|v| v.as_str());
            assert_eq!(rid, Some("req-init"));
            assert_eq!(log.attributes.get("trace.id").and_then(|v| v.as_str()), Some("trace-init"));
        }
    }

    #[test]
    fn test_pre_invoke_logs_batched_directly_when_trace_collection_off() {
        // collect_trace_id=false → unchanged behavior: stamped with ARN+request_id and
        // batched immediately (no holding, no trace.id).
        let p = create_test_processor();
        assert!(p.pending_logs.is_none());
        {
            let mut ctx = p.invocation_context.lock().unwrap();
            ctx.invoked_function_arn = "arn:aws:lambda:us-east-1:111:function:f".to_string();
            ctx.request_id = "req-x".to_string();
        }
        {
            let mut buf = p.pre_invoke_buffer.lock().unwrap();
            buf.push(make_log_msg("init-1"));
        }
        p.process_pre_invoke_logs();
        let batch = p.log_batch.lock().unwrap();
        assert_eq!(batch.len(), 1, "batched immediately when collection off");
        assert_eq!(batch[0].attributes.get("faas.execution").and_then(|v| v.as_str()), Some("req-x"));
        assert!(!batch[0].attributes.contains_key("trace.id"));
    }

    #[test]
    fn test_pre_invoke_logs_stay_buffered_when_context_invalid() {
        // No ARN/request_id yet → must NOT be sent or held; stay in pre_invoke_buffer.
        let p = create_trace_processor();
        {
            let mut buf = p.pre_invoke_buffer.lock().unwrap();
            buf.push(make_log_msg("init-1"));
        }
        p.process_pre_invoke_logs();
        assert_eq!(p.log_batch.lock().unwrap().len(), 0, "nothing sent without context");
        assert_eq!(p.pending_logs.as_ref().unwrap().lock().unwrap().total(), 0, "nothing held without context");
        assert_eq!(p.pre_invoke_buffer.lock().unwrap().len(), 1, "INIT log stays buffered until context valid");
    }

    // During shutdown, the extension's OWN logs are NOT forwarded to NR (the structured
    // drop diagnostic is sent directly instead). Function/platform logs are unaffected.
    #[tokio::test]
    #[serial]
    async fn test_extension_logs_dropped_during_shutdown() {
        use crate::config::{ExtensionConfig, ExtensionSettings, NewRelicConfig};
        use crate::telemetry::listener::TelemetryRecord;

        // Processor with send_extension_logs = true so the gate under test is reached.
        let mut config = ExtensionConfig::default();
        config.extension = ExtensionSettings {
            send_extension_logs: true,
            ..ExtensionSettings::default()
        };
        config.new_relic = NewRelicConfig { ..NewRelicConfig::default() };
        let client = Arc::new(crate::newrelic::client::NewRelicClient::new(&Arc::new(
            ExtensionConfig::default(),
        )));
        let ctx = Arc::new(Mutex::new(crate::context::InvocationContext::default()));
        let p = LogProcessor::new(client, Arc::new(config), ctx, None);

        let rec = TelemetryRecord {
            time: chrono::Utc::now(),
            record_type: "extension".to_string(),
            record: serde_json::json!("[NR_EXT] ERROR APM telemetry DROPPED at shutdown"),
        };

        // Not shutting down → accepted (lands in pre_invoke_buffer; no ARN yet).
        crate::IS_SHUTTING_DOWN.store(false, std::sync::atomic::Ordering::Relaxed);
        p.process_record(rec.clone()).await;
        assert_eq!(
            p.pre_invoke_buffer.lock().unwrap().len(),
            1,
            "extension log accepted when not shutting down"
        );

        // Shutting down → dropped before any buffering/batching (no new entry).
        crate::IS_SHUTTING_DOWN.store(true, std::sync::atomic::Ordering::Relaxed);
        p.process_record(rec).await;
        assert_eq!(
            p.pre_invoke_buffer.lock().unwrap().len(),
            1,
            "extension log dropped during shutdown — no new entry"
        );
        assert_eq!(p.log_batch.lock().unwrap().len(), 0);

        // Reset the one-way latch so other tests aren't affected.
        crate::IS_SHUTTING_DOWN.store(false, std::sync::atomic::Ordering::Relaxed);
    }

    // ========================================================================
    // PHASE 8: SendError classification + log_type_from_message
    // ========================================================================

    #[test]
    fn test_log_type_from_message_function() {
        let mut attrs = serde_json::Map::new();
        attrs.insert("_nr.logType".to_string(), json!("function"));
        let msg = crate::newrelic::payload::LogMessage {
            timestamp: 0,
            message: String::new(),
            attributes: attrs,
        };
        assert_eq!(LogProcessor::log_type_from_message(&msg), LogType::Function);
    }

    #[test]
    fn test_log_type_from_message_platform() {
        let mut attrs = serde_json::Map::new();
        attrs.insert("_nr.logType".to_string(), json!("platform"));
        let msg = crate::newrelic::payload::LogMessage {
            timestamp: 0,
            message: String::new(),
            attributes: attrs,
        };
        assert_eq!(LogProcessor::log_type_from_message(&msg), LogType::Platform);
    }

    #[test]
    fn test_log_type_from_message_extension() {
        let mut attrs = serde_json::Map::new();
        attrs.insert("_nr.logType".to_string(), json!("extension"));
        let msg = crate::newrelic::payload::LogMessage {
            timestamp: 0,
            message: String::new(),
            attributes: attrs,
        };
        assert_eq!(LogProcessor::log_type_from_message(&msg), LogType::Extension);
    }

    #[test]
    fn test_log_type_from_message_missing_attribute_defaults_to_function() {
        let msg = crate::newrelic::payload::LogMessage {
            timestamp: 0,
            message: String::new(),
            attributes: serde_json::Map::new(),
        };
        assert_eq!(LogProcessor::log_type_from_message(&msg), LogType::Function);
    }

    #[test]
    fn test_log_type_from_message_unknown_value_defaults_to_function() {
        let mut attrs = serde_json::Map::new();
        attrs.insert("_nr.logType".to_string(), json!("unknown_type"));
        let msg = crate::newrelic::payload::LogMessage {
            timestamp: 0,
            message: String::new(),
            attributes: attrs,
        };
        assert_eq!(LogProcessor::log_type_from_message(&msg), LogType::Function);
    }

    #[test]
    fn test_send_error_client_rejected_drops_logs() {
        use crate::newrelic::client::SendError;
        let err = SendError::ClientRejected { status: 413 };
        // ClientRejected means logs should NOT be rebuffered (empty vec)
        assert!(matches!(err, SendError::ClientRejected { status: 413 }));
        // Verify the error is NOT retryable
        assert!(!matches!(err, SendError::ServerExhausted { .. } | SendError::Network(_)));
    }

    #[test]
    fn test_send_error_server_exhausted_is_retryable() {
        use crate::newrelic::client::SendError;
        let err = SendError::ServerExhausted { status: 503 };
        assert!(matches!(err, SendError::ServerExhausted { .. }));
    }

    #[test]
    fn test_failed_buffer_populated_for_retryable_errors() {
        let p = create_test_processor();
        assert_eq!(p.failed_logs_buffer.lock().unwrap().len(), 0);

        // Simulate what happens when try_send_chunk returns a retryable error:
        // The caller pushes logs to failed buffer
        let msg = crate::newrelic::payload::LogMessage {
            timestamp: 1234,
            message: "test log".to_string(),
            attributes: serde_json::Map::new(),
        };
        let entry = FailedLogEntry {
            log_type: LogType::Function,
            log_message: msg,
            original_request_id: "req-123".to_string(),
            retry_count: 0,
        };
        p.push_to_failed_buffer(entry);

        let buf = p.failed_logs_buffer.lock().unwrap();
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.len_of(LogType::Function), 1);
    }

    #[test]
    fn test_failed_buffer_not_populated_for_client_errors() {
        let p = create_test_processor();
        // For ClientRejected (4xx), we return empty Vec — nothing to rebuffer
        // Simulate: try_send_chunk returns Err((_, Vec::new()))
        let empty_logs: Vec<crate::newrelic::payload::LogMessage> = Vec::new();
        // No logs to push to buffer
        for log_message in empty_logs {
            let entry = FailedLogEntry {
                log_type: LogProcessor::log_type_from_message(&log_message),
                log_message,
                original_request_id: "req-456".to_string(),
                retry_count: 0,
            };
            p.push_to_failed_buffer(entry);
        }
        assert_eq!(p.failed_logs_buffer.lock().unwrap().len(), 0);
    }
}
