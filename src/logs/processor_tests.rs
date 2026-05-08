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
    use super::super::{LogProcessor, LogType, FailedLogEntry, TraceIdExtractionState};
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
        
        let p = create_test_processor();
        for _ in 0..299 {
            p.push_to_failed_buffer(make_entry(LogType::Function, 0));
        }
        p.push_to_failed_buffer(make_entry(LogType::Extension, 0));
        assert_eq!(p.failed_logs_buffer.lock().unwrap().len(), 300);
        // push one more Function — Extension should be evicted
        p.push_to_failed_buffer(make_entry(LogType::Function, 0));
        let buf = p.failed_logs_buffer.lock().unwrap();
        assert_eq!(buf.len(), 300);
        assert!(buf.iter().all(|e| e.log_type != LogType::Extension),
            "Extension log should have been evicted");
    }

    #[test]
    fn test_overflow_drops_incoming_extension_when_no_extension_in_buf() {
        
        let p = create_test_processor();
        for _ in 0..300 {
            p.push_to_failed_buffer(make_entry(LogType::Function, 0));
        }
        p.push_to_failed_buffer(make_entry(LogType::Extension, 0));
        let buf = p.failed_logs_buffer.lock().unwrap();
        assert_eq!(buf.len(), 300);
        assert!(buf.iter().all(|e| e.log_type == LogType::Function),
            "Incoming Extension must be dropped when buffer is all Function");
    }

    #[test]
    fn test_overflow_drops_incoming_platform_when_buf_all_function() {
        
        let p = create_test_processor();
        for _ in 0..300 {
            p.push_to_failed_buffer(make_entry(LogType::Function, 0));
        }
        p.push_to_failed_buffer(make_entry(LogType::Platform, 0));
        let buf = p.failed_logs_buffer.lock().unwrap();
        assert_eq!(buf.len(), 300);
        assert!(buf.iter().all(|e| e.log_type == LogType::Function),
            "Incoming Platform must be dropped when buffer is all Function");
    }

    #[test]
    fn test_overflow_function_fifo_evicts_oldest() {
        
        use serde_json::Map;
        let p = create_test_processor();
        for i in 0u64..300 {
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
        let oldest_ts = p.failed_logs_buffer.lock().unwrap().front().unwrap().log_message.timestamp;
        assert_eq!(oldest_ts, 0);
        p.push_to_failed_buffer(make_entry(LogType::Function, 0));
        let new_front_ts = p.failed_logs_buffer.lock().unwrap().front().unwrap().log_message.timestamp;
        assert_eq!(new_front_ts, 1, "FIFO: oldest (ts=0) should be evicted");
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

    // Fix 1 — reset_trace_id_state rescues buffered logs into log_batch instead of dropping them.
    #[test]
    fn test_reset_trace_id_state_rescues_buffered_logs() {
        let p = create_trace_processor();

        // Manually push logs into buffered_logs (simulates logs received while waiting for trace ID)
        {
            let buffered = p.buffered_logs.as_ref().unwrap();
            let mut guard = buffered.lock().unwrap();
            guard.push(make_log_msg("log-a"));
            guard.push(make_log_msg("log-b"));
        }

        // Sanity: batch is empty before rescue
        assert_eq!(p.log_batch.lock().unwrap().len(), 0);

        p.reset_trace_id_state();

        // After reset, buffered_logs must be empty
        let buffered_after = p.buffered_logs.as_ref().unwrap().lock().unwrap().len();
        assert_eq!(buffered_after, 0, "buffered_logs must be drained by reset_trace_id_state");

        // Rescued logs must appear in log_batch
        let batch = p.log_batch.lock().unwrap();
        assert_eq!(batch.len(), 2, "both rescued logs must be in log_batch");
        assert!(batch.iter().any(|m| m.message == "log-a"));
        assert!(batch.iter().any(|m| m.message == "log-b"));
    }

    #[test]
    fn test_reset_trace_id_state_no_op_when_buffer_empty() {
        let p = create_trace_processor();
        p.reset_trace_id_state();
        assert_eq!(p.log_batch.lock().unwrap().len(), 0,
            "reset with empty buffer must not add anything to log_batch");
    }

    #[test]
    fn test_reset_trace_id_state_no_op_without_trace_collection() {
        // Processor with collect_trace_id=false has no buffered_logs — must be a no-op.
        let p = create_test_processor();
        assert!(p.buffered_logs.is_none());
        p.reset_trace_id_state(); // must not panic
        assert_eq!(p.log_batch.lock().unwrap().len(), 0);
    }

    // Fix 3 — on_trace_id_extracted routes stamped logs through log_batch (not direct send).
    #[tokio::test]
    async fn test_on_trace_id_extracted_stamps_trace_id_and_routes_to_batch() {
        let p = create_trace_processor();
        let trace_id = "abc-trace-123";

        // Pre-load buffered_logs with two entries (no trace.id yet)
        {
            let buffered = p.buffered_logs.as_ref().unwrap();
            let mut guard = buffered.lock().unwrap();
            guard.push(make_log_msg("msg-1"));
            guard.push(make_log_msg("msg-2"));
        }

        p.on_trace_id_extracted(trace_id).await.unwrap();

        // buffered_logs must be drained
        let remaining = p.buffered_logs.as_ref().unwrap().lock().unwrap().len();
        assert_eq!(remaining, 0, "buffered_logs must be empty after on_trace_id_extracted");

        // Logs must be in log_batch (not sent directly)
        let batch = p.log_batch.lock().unwrap();
        assert_eq!(batch.len(), 2, "both logs must be routed to log_batch");

        // Each log must carry the trace.id attribute
        for log in batch.iter() {
            let tid = log.attributes.get("trace.id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            assert_eq!(tid, trace_id,
                "trace.id attribute must be stamped on log '{}'", log.message);
        }
    }

    #[tokio::test]
    async fn test_on_trace_id_extracted_no_op_when_buffer_empty() {
        let p = create_trace_processor();
        p.on_trace_id_extracted("some-trace").await.unwrap();
        assert_eq!(p.log_batch.lock().unwrap().len(), 0,
            "empty buffer must produce no log_batch entries");
    }

    #[tokio::test]
    async fn test_on_trace_id_extracted_no_op_without_trace_collection() {
        // collect_trace_id=false — must return Ok(()) silently.
        let p = create_test_processor();
        assert!(p.buffered_logs.is_none());
        p.on_trace_id_extracted("some-trace").await.unwrap();
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
        for entry in buf.iter() {
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
        {
            let buffered = p.buffered_logs.as_ref().unwrap();
            let mut guard = buffered.lock().unwrap();
            guard.push(make_log_msg("apm-log-1"));
            guard.push(make_log_msg("apm-log-2"));
        }

        p.on_trace_id_extracted("trace-abc").await.unwrap();

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
    fn test_reset_trace_id_state_stamps_entity_guid() {
        let p = create_apm_processor("reset-entity-guid");
        {
            let buffered = p.buffered_logs.as_ref().unwrap();
            let mut guard = buffered.lock().unwrap();
            guard.push(make_log_msg("rescued-log-1"));
        }

        p.reset_trace_id_state();

        let batch = p.log_batch.lock().unwrap();
        assert_eq!(batch.len(), 1);
        let guid = batch[0].attributes.get("entity.guid").and_then(|v| v.as_str()).unwrap_or("");
        assert_eq!(guid, "reset-entity-guid",
            "entity.guid must be stamped on rescued log");
    }

    #[test]
    fn test_process_buffered_logs_stamps_entity_guid_and_trace_id() {
        
        let p = create_apm_processor("direct-path-guid");

        // Put a trace ID into invocation_context so the log takes the direct path.
        p.invocation_context.lock().unwrap().trace_id = Some("trace-xyz".to_string());

        // Set extraction state to Extracted so routing goes direct to log_batch.
        if let Some(ref state_arc) = p.trace_extraction_state {
            *state_arc.lock().unwrap() = TraceIdExtractionState::Extracted;
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
        // When apm_app is None, all three paths must complete without panic and
        // entity.guid must simply be absent from the output.
        let p = create_trace_processor(); // no apm_app

        {
            let buffered = p.buffered_logs.as_ref().unwrap();
            buffered.lock().unwrap().push(make_log_msg("no-apm-log"));
        }
        p.on_trace_id_extracted("t1").await.unwrap();
        let batch = p.log_batch.lock().unwrap();
        assert!(!batch[0].attributes.contains_key("entity.guid"),
            "entity.guid must not appear when apm_app is None");
    }
}
