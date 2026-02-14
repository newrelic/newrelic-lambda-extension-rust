#[cfg(test)]
mod tests {
    use crate::credentials::credentials::decode_license_key;
    use crate::credentials::get_new_relic_license_key;
    use crate::config::Configuration;

    // ========================================================================
    // decode_license_key
    // ========================================================================

    #[test]
    fn test_decode_license_key_valid_json() {
        let raw = r#"{"LicenseKey": "abc123def456"}"#;
        let result = decode_license_key(raw);
        assert!(result.is_ok());
        assert_eq!(result.expect("should succeed"), "abc123def456");
    }

    #[test]
    fn test_decode_license_key_empty_key_returns_error() {
        let raw = r#"{"LicenseKey": ""}"#;
        let result = decode_license_key(raw);
        assert!(result.is_err());
        let err_msg = result.expect_err("should fail").to_string();
        assert!(err_msg.contains("malformed license key secret"));
    }

    #[test]
    fn test_decode_license_key_missing_field_returns_error() {
        let raw = r#"{"SomeOtherKey": "value"}"#;
        let result = decode_license_key(raw);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_license_key_invalid_json_returns_error() {
        let raw = "not-json-at-all";
        let result = decode_license_key(raw);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_license_key_empty_string_returns_error() {
        let raw = "";
        let result = decode_license_key(raw);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_license_key_case_sensitive_field_name() {
        // Field must be "LicenseKey" (PascalCase), not "licenseKey"
        let raw = r#"{"licenseKey": "abc123"}"#;
        let result = decode_license_key(raw);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_license_key_extra_fields_ignored() {
        let raw = r#"{"LicenseKey": "abc123", "extra": "ignored"}"#;
        let result = decode_license_key(raw);
        assert!(result.is_ok());
        assert_eq!(result.expect("should succeed"), "abc123");
    }

    #[test]
    fn test_decode_license_key_whitespace_only_key() {
        // Whitespace-only key should succeed (it's non-empty per the check)
        let raw = r#"{"LicenseKey": "   "}"#;
        let result = decode_license_key(raw);
        assert!(result.is_ok());
        assert_eq!(result.expect("should succeed"), "   ");
    }

    #[test]
    fn test_decode_license_key_numeric_value_returns_error() {
        // LicenseKey must be a string, not a number
        let raw = r#"{"LicenseKey": 12345}"#;
        let result = decode_license_key(raw);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_license_key_null_value_returns_error() {
        // null is not a valid String in serde — deserialization fails
        let raw = r#"{"LicenseKey": null}"#;
        let result = decode_license_key(raw);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_license_key_unicode_value() {
        let raw = r#"{"LicenseKey": "ライセンス-key-123"}"#;
        let result = decode_license_key(raw);
        assert!(result.is_ok());
        assert_eq!(result.expect("should succeed"), "ライセンス-key-123");
    }

    #[test]
    fn test_decode_license_key_nested_object_returns_error() {
        let raw = r#"{"LicenseKey": {"nested": true}}"#;
        let result = decode_license_key(raw);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_license_key_array_value_returns_error() {
        let raw = r#"{"LicenseKey": ["abc"]}"#;
        let result = decode_license_key(raw);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_license_key_boolean_value_returns_error() {
        let raw = r#"{"LicenseKey": true}"#;
        let result = decode_license_key(raw);
        assert!(result.is_err());
    }

    // ========================================================================
    // get_new_relic_license_key — outside Lambda environment
    // ========================================================================

    #[tokio::test]
    async fn test_get_license_key_fails_outside_lambda() {
        // AWS_LAMBDA_RUNTIME_API is not set in test environment,
        // so initialize_aws_clients() returns an error immediately
        let conf = Configuration {
            license_key: String::new(),
            license_key_secret_id: String::new(),
            license_key_ssm_parameter_name: String::new(),
        };

        let result = get_new_relic_license_key(&conf).await;
        assert!(result.is_err());
        let err_msg = result.expect_err("should fail").to_string();
        assert!(err_msg.contains("Failed to initialize AWS clients"));
    }

    #[tokio::test]
    async fn test_get_license_key_fails_even_with_config_values() {
        // Even if config has secret_id/ssm values, should still fail
        // because AWS clients can't initialize outside Lambda
        let conf = Configuration {
            license_key: String::new(),
            license_key_secret_id: "my-secret".to_string(),
            license_key_ssm_parameter_name: "my-param".to_string(),
        };

        let result = get_new_relic_license_key(&conf).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_license_key_very_long_valid_key() {
        let long_key = "a".repeat(1000);
        let raw = format!(r#"{{"LicenseKey": "{}"}}"#, long_key);
        let result = decode_license_key(&raw);
        assert!(result.is_ok());
        assert_eq!(result.expect("should succeed").len(), 1000);
    }

    #[tokio::test]
    async fn test_get_license_key_error_message_content() {
        let conf = Configuration {
            license_key: String::new(),
            license_key_secret_id: "my-secret".to_string(),
            license_key_ssm_parameter_name: "my-param".to_string(),
        };

        let result = get_new_relic_license_key(&conf).await;
        assert!(result.is_err());
        let err_msg = result.expect_err("should fail").to_string();
        assert!(
            err_msg.contains("Failed to initialize AWS clients"),
            "Expected error about AWS clients, got: {}",
            err_msg
        );
    }
}
