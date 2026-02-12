#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::agent::payload::{extract_function_name_from_arn, create_newrelic_log_format};
    use crate::config::ExtensionConfig;

    // ========================================================================
    // extract_function_name_from_arn
    // ========================================================================

    #[test]
    fn test_valid_arn_extracts_function_name() {
        let result = extract_function_name_from_arn(
            "arn:aws:lambda:us-east-1:123456789012:function:my-function",
            "fallback",
        );
        assert_eq!(result, "my-function");
    }

    #[test]
    fn test_arn_with_version_qualifier() {
        // Position 6 is still the function name; version is position 7
        let result = extract_function_name_from_arn(
            "arn:aws:lambda:us-east-1:123456789012:function:my-function:$LATEST",
            "fallback",
        );
        assert_eq!(result, "my-function");
    }

    #[test]
    fn test_empty_arn_returns_fallback() {
        let result = extract_function_name_from_arn("", "my-fallback");
        assert_eq!(result, "my-fallback");
    }

    #[test]
    fn test_short_arn_returns_fallback() {
        let result = extract_function_name_from_arn("arn:aws:lambda", "my-fallback");
        assert_eq!(result, "my-fallback");
    }

    #[test]
    fn test_arn_with_empty_function_name_returns_fallback() {
        // Position 6 is empty string
        let result = extract_function_name_from_arn(
            "arn:aws:lambda:us-east-1:123456:function:",
            "my-fallback",
        );
        assert_eq!(result, "my-fallback");
    }

    #[test]
    fn test_non_arn_string_returns_fallback() {
        let result = extract_function_name_from_arn("not-an-arn-at-all", "my-fallback");
        assert_eq!(result, "my-fallback");
    }

    #[test]
    fn test_arn_with_special_characters_in_name() {
        let result = extract_function_name_from_arn(
            "arn:aws:lambda:eu-west-1:999:function:my_func-v2.1",
            "fallback",
        );
        assert_eq!(result, "my_func-v2.1");
    }

    // ========================================================================
    // create_newrelic_log_format
    // ========================================================================

    #[test]
    fn test_create_newrelic_log_format_basic_structure() {
        let config = Arc::new(ExtensionConfig::default());
        let payload = b"test-agent-payload";

        let result = create_newrelic_log_format(
            payload,
            "my-function",
            "arn:aws:lambda:us-east-1:123:function:my-function",
            "/aws/lambda/my-function",
            "req-123",
            &config,
            None,
        );

        let parsed: serde_json::Value = serde_json::from_str(&result).expect("valid JSON");

        // Context
        let ctx = &parsed["context"];
        assert_eq!(ctx["function_name"], "my-function");
        assert_eq!(ctx["invoked_function_arn"], "arn:aws:lambda:us-east-1:123:function:my-function");
        assert_eq!(ctx["log_group_name"], "/aws/lambda/my-function");
        assert!(ctx["log_stream_name"].as_str().unwrap().contains("newrelic-lambda-extension"));

        // Entry is stringified JSON
        let entry_str = parsed["entry"].as_str().expect("entry should be string");
        let entry: serde_json::Value = serde_json::from_str(entry_str).expect("valid entry JSON");

        assert_eq!(entry["logGroup"], "/aws/lambda/my-function");

        let events = entry["logEvents"].as_array().expect("logEvents array");
        // Without version_info, only 1 event (the agent payload)
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["id"], "req-123");
        assert_eq!(events[0]["message"], "test-agent-payload");
        assert!(events[0]["timestamp"].is_number());
    }

    #[test]
    fn test_create_newrelic_log_format_with_utf8_lossy() {
        let config = Arc::new(ExtensionConfig::default());
        // Invalid UTF-8 bytes — should not panic, uses lossy conversion
        let payload = &[0xFF, 0xFE, 0x48, 0x65, 0x6C, 0x6C, 0x6F];

        let result = create_newrelic_log_format(
            payload,
            "fn",
            "arn",
            "/aws/lambda/fn",
            "req-1",
            &config,
            None,
        );

        let parsed: serde_json::Value = serde_json::from_str(&result).expect("valid JSON");
        // Should produce valid JSON even with lossy conversion
        assert!(parsed.get("entry").is_some());
    }

    #[test]
    fn test_create_newrelic_log_format_empty_payload() {
        let config = Arc::new(ExtensionConfig::default());

        let result = create_newrelic_log_format(
            b"",
            "fn",
            "arn",
            "/aws/lambda/fn",
            "req-1",
            &config,
            None,
        );

        let parsed: serde_json::Value = serde_json::from_str(&result).expect("valid JSON");
        let entry_str = parsed["entry"].as_str().unwrap();
        let entry: serde_json::Value = serde_json::from_str(entry_str).unwrap();
        let events = entry["logEvents"].as_array().unwrap();
        assert_eq!(events[0]["message"], "");
    }

    #[test]
    fn test_create_newrelic_log_format_apm_mode_skips_version() {
        let mut config = ExtensionConfig::default();
        config.new_relic.apm_lambda_mode = true;
        let config = Arc::new(config);

        let result = create_newrelic_log_format(
            b"data",
            "fn",
            "arn",
            "/aws/lambda/fn",
            "req-1",
            &config,
            None,
        );

        let parsed: serde_json::Value = serde_json::from_str(&result).expect("valid JSON");
        let entry_str = parsed["entry"].as_str().unwrap();
        let entry: serde_json::Value = serde_json::from_str(entry_str).unwrap();
        let events = entry["logEvents"].as_array().unwrap();
        // In APM mode with no version_info, only the agent payload event
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_create_newrelic_log_format_timestamps_are_positive() {
        let config = Arc::new(ExtensionConfig::default());

        let result = create_newrelic_log_format(
            b"data",
            "fn",
            "arn",
            "/aws/lambda/fn",
            "req-1",
            &config,
            None,
        );

        let parsed: serde_json::Value = serde_json::from_str(&result).expect("valid JSON");
        let entry_str = parsed["entry"].as_str().unwrap();
        let entry: serde_json::Value = serde_json::from_str(entry_str).unwrap();
        let ts = entry["logEvents"][0]["timestamp"].as_u64().expect("timestamp should be u64");
        assert!(ts > 0, "timestamp should be positive");
    }
}
