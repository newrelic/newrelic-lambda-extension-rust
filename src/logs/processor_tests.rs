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
    use crate::logs::processor::LogProcessor;
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
}
