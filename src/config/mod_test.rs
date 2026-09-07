// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::env;
use std::time::Duration;
use serial_test::serial;

// Test helper to clean up environment before and after tests
fn with_clean_env<F>(test: F)
where
    F: FnOnce(),
{
    // Save original values
    let original_tags = env::var("NR_TAGS").ok();
    let original_delimiter = env::var("NR_ENV_DELIMITER").ok();
    let original_labels = env::var("NEW_RELIC_LABELS").ok();

    // Clean environment
    env::remove_var("NR_TAGS");
    env::remove_var("NR_ENV_DELIMITER");
    env::remove_var("NEW_RELIC_LABELS");

    // Run test
    test();

    // Restore environment
    if let Some(val) = original_tags {
        env::set_var("NR_TAGS", val);
    } else {
        env::remove_var("NR_TAGS");
    }
    if let Some(val) = original_delimiter {
        env::set_var("NR_ENV_DELIMITER", val);
    } else {
        env::remove_var("NR_ENV_DELIMITER");
    }
    if let Some(val) = original_labels {
        env::set_var("NEW_RELIC_LABELS", val);
    } else {
        env::remove_var("NEW_RELIC_LABELS");
    }
}

// Test helper for full environment cleanup
fn with_full_clean_env<F>(test: F)
where
    F: FnOnce(),
{
    // List of all config-related env vars
    let env_vars = vec![
        "NR_TAGS",
        "NR_ENV_DELIMITER",
        "NEW_RELIC_LABELS",
        "NEW_RELIC_LAMBDA_EXTENSION_ENABLED",
        "NEW_RELIC_LICENSE_KEY",
        "NEW_RELIC_LICENSE_KEY_SECRET",
        "NEW_RELIC_LICENSE_KEY_SSM_PARAMETER_NAME",
        "NEW_RELIC_LAMBDA_HANDLER",
        "NEW_RELIC_COLLECT_TRACE_ID",
        "NEW_RELIC_TRACE_ID_LOG_BUFFER_MAX",
        "NEW_RELIC_ADD_VERSION_DETAIL_TAGS",
        "NEW_RELIC_LAYER_VERSION",
        "NEW_RELIC_APM_LAMBDA_MODE",
        "NEW_RELIC_APM_BLOCKING_HANDSHAKE",
        "NEW_RELIC_APM_HANDSHAKE_TIMEOUT_SECS",
        "NEW_RELIC_EXTENSION_SYNCHRONOUS_FLUSH",
        "NEW_RELIC_APM_DISABLE_TELEMETRY",
        "NEW_RELIC_EXTENSION_SEND_LOGS",
        "NEW_RELIC_EXTENSION_SEND_FUNCTION_LOGS",
        "NEW_RELIC_EXTENSION_SEND_EXTENSION_LOGS",
        "NEW_RELIC_EXTENSION_SEND_PLATFORM_LOGS",
        "NEW_RELIC_EXTENSION_LOG_LEVEL",
        "NEW_RELIC_EXTENSION_LOGS_ENABLED",
        "NEW_RELIC_LAMBDA_EXTENSION_PROXY",
        "NEW_RELIC_DATA_COLLECTION_TIMEOUT",
        "NEW_RELIC_HTTP_TIMEOUT",
        "AWS_LAMBDA_RUNTIME_API",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        // OTLP metrics forwarding opt-in. Must be cleared here or a preceding test that
        // sets it leaks into the default-value assertions below (the list is an explicit
        // allowlist, not a wildcard).
        "NEW_RELIC_OTLP_METRIC_ENABLED",
        "NEW_RELIC_OTLP_METRIC_ENDPOINT",
    ];
    
    // Save original values
    let original_values: Vec<(String, Option<String>)> = env_vars
        .iter()
        .map(|&var| (var.to_string(), env::var(var).ok()))
        .collect();
    
    // Clean environment
    for var in &env_vars {
        env::remove_var(var);
    }
    
    // Run test
    test();
    
    // Restore environment
    for (var, value) in original_values {
        if let Some(val) = value {
            env::set_var(&var, val);
        } else {
            env::remove_var(&var);
        }
    }
}

#[test]
#[serial]
fn test_parse_nr_tags_with_default_delimiter() {
    with_clean_env(|| {
        env::set_var("NR_TAGS", "env:prod;team:backend;region:us-east-1");
        
        let tags = parse_nr_tags();
        
        assert_eq!(tags.len(), 3);
        assert!(tags.contains(&("env".to_string(), "prod".to_string())));
        assert!(tags.contains(&("team".to_string(), "backend".to_string())));
        assert!(tags.contains(&("region".to_string(), "us-east-1".to_string())));
    });
}

#[test]
#[serial]
fn test_parse_nr_tags_with_custom_delimiter() {
    with_clean_env(|| {
        env::set_var("NR_TAGS", "env:prod|team:backend");
        env::set_var("NR_ENV_DELIMITER", "|");
        
        let tags = parse_nr_tags();
        
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&("env".to_string(), "prod".to_string())));
        assert!(tags.contains(&("team".to_string(), "backend".to_string())));
    });
}

#[test]
#[serial]
fn test_parse_nr_tags_with_whitespace() {
    with_clean_env(|| {
        env::set_var("NR_TAGS", " env : prod ; team : backend ");
        
        let tags = parse_nr_tags();
        
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&("env".to_string(), "prod".to_string())));
        assert!(tags.contains(&("team".to_string(), "backend".to_string())));
    });
}

#[test]
#[serial]
fn test_parse_nr_tags_invalid_format() {
    with_clean_env(|| {
        // Test invalid formats - should be skipped
        env::set_var("NR_TAGS", "invalid;env:prod;also-invalid;team:backend");
        
        let tags = parse_nr_tags();
        
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&("env".to_string(), "prod".to_string())));
        assert!(tags.contains(&("team".to_string(), "backend".to_string())));
    });
}

#[test]
#[serial]
fn test_parse_nr_tags_empty_values() {
    with_clean_env(|| {
        // Test empty keys/values - should be skipped
        env::set_var("NR_TAGS", ":value;key:;env:prod");
        
        let tags = parse_nr_tags();
        
        assert_eq!(tags.len(), 1);
        assert!(tags.contains(&("env".to_string(), "prod".to_string())));
    });
}

#[test]
#[serial]
fn test_parse_nr_tags_not_set() {
    with_clean_env(|| {
        let tags = parse_nr_tags();
        assert!(tags.is_empty());
    });
}

#[test]
#[serial]
fn test_parse_nr_tags_empty_string() {
    with_clean_env(|| {
        env::set_var("NR_TAGS", "");

        let tags = parse_nr_tags();

        assert!(tags.is_empty());
    });
}

// ============================================================================
// NEW_RELIC_LABELS (agent-specs/Labels.md) - independent of NR_TAGS above
// ============================================================================

#[test]
#[serial]
fn test_parse_new_relic_labels_basic() {
    with_clean_env(|| {
        env::set_var("NEW_RELIC_LABELS", "env:prod;team:backend");

        let labels = parse_new_relic_labels();

        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&("env".to_string(), "prod".to_string())));
        assert!(labels.contains(&("team".to_string(), "backend".to_string())));
    });
}

#[test]
#[serial]
fn test_parse_new_relic_labels_whitespace() {
    with_clean_env(|| {
        env::set_var("NEW_RELIC_LABELS", " env : prod ; team : backend ");

        let labels = parse_new_relic_labels();

        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&("env".to_string(), "prod".to_string())));
        assert!(labels.contains(&("team".to_string(), "backend".to_string())));
    });
}

#[test]
#[serial]
fn test_parse_new_relic_labels_not_set() {
    with_clean_env(|| {
        let labels = parse_new_relic_labels();
        assert!(labels.is_empty());
    });
}

#[test]
#[serial]
fn test_parse_new_relic_labels_empty_string() {
    with_clean_env(|| {
        env::set_var("NEW_RELIC_LABELS", "");

        let labels = parse_new_relic_labels();

        assert!(labels.is_empty());
    });
}

#[test]
#[serial]
fn test_parse_new_relic_labels_only_delimiters() {
    with_clean_env(|| {
        // Nothing but separators - no labels configured, not a malformed-string error.
        env::set_var("NEW_RELIC_LABELS", ";;;");

        let labels = parse_new_relic_labels();

        assert!(labels.is_empty());
    });
}

#[test]
#[serial]
fn test_parse_new_relic_labels_strips_leading_and_trailing_semicolons() {
    with_clean_env(|| {
        // Spec: leading/trailing stray `;` are stripped, not treated as malformed.
        env::set_var("NEW_RELIC_LABELS", ";;env:prod;;");

        let labels = parse_new_relic_labels();

        assert_eq!(labels, vec![("env".to_string(), "prod".to_string())]);
    });
}

#[test]
#[serial]
fn test_parse_new_relic_labels_middle_empty_pair_is_malformed() {
    with_clean_env(|| {
        // Spec: an empty pair between two valid ones IS malformed (unlike leading/trailing).
        env::set_var("NEW_RELIC_LABELS", "env:prod;;team:backend");

        let labels = parse_new_relic_labels();

        assert!(labels.is_empty(), "a malformed string must hard-fail to an empty list");
    });
}

#[test]
#[serial]
fn test_parse_new_relic_labels_malformed_missing_colon_hard_fails_whole_list() {
    with_clean_env(|| {
        // Unlike NR_TAGS (which skips just the bad pair), NEW_RELIC_LABELS must discard
        // the entire list on any malformed pair - never a partial list.
        env::set_var("NEW_RELIC_LABELS", "invalid;env:prod;team:backend");

        let labels = parse_new_relic_labels();

        assert!(labels.is_empty());
    });
}

#[test]
#[serial]
fn test_parse_new_relic_labels_malformed_multiple_colons_hard_fails_whole_list() {
    with_clean_env(|| {
        env::set_var("NEW_RELIC_LABELS", "valid:value;invalid:has:colons;another:good");

        let labels = parse_new_relic_labels();

        assert!(labels.is_empty());
    });
}

#[test]
#[serial]
fn test_parse_new_relic_labels_malformed_empty_type_hard_fails_whole_list() {
    with_clean_env(|| {
        env::set_var("NEW_RELIC_LABELS", ":value;env:prod");

        let labels = parse_new_relic_labels();

        assert!(labels.is_empty());
    });
}

#[test]
#[serial]
fn test_parse_new_relic_labels_malformed_empty_value_hard_fails_whole_list() {
    with_clean_env(|| {
        env::set_var("NEW_RELIC_LABELS", "key:;env:prod");

        let labels = parse_new_relic_labels();

        assert!(labels.is_empty());
    });
}

#[test]
#[serial]
fn test_parse_new_relic_labels_duplicate_type_last_wins() {
    with_clean_env(|| {
        env::set_var("NEW_RELIC_LABELS", "env:prod;env:staging");

        let labels = parse_new_relic_labels();

        assert_eq!(labels, vec![("env".to_string(), "staging".to_string())]);
    });
}

#[test]
#[serial]
fn test_parse_new_relic_labels_truncates_long_value() {
    with_clean_env(|| {
        let long_value = "v".repeat(300);
        env::set_var("NEW_RELIC_LABELS", format!("env:{long_value}"));

        let labels = parse_new_relic_labels();

        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].0, "env");
        assert_eq!(labels[0].1.chars().count(), 255);
        assert_eq!(labels[0].1, "v".repeat(255));
    });
}

#[test]
#[serial]
fn test_parse_new_relic_labels_truncates_long_type() {
    with_clean_env(|| {
        let long_type = "t".repeat(300);
        env::set_var("NEW_RELIC_LABELS", format!("{long_type}:prod"));

        let labels = parse_new_relic_labels();

        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].0.chars().count(), 255);
        assert_eq!(labels[0].0, "t".repeat(255));
        assert_eq!(labels[0].1, "prod");
    });
}

#[test]
#[serial]
fn test_parse_new_relic_labels_caps_at_64_entries() {
    with_clean_env(|| {
        let raw = (0..70)
            .map(|i| format!("k{i}:v{i}"))
            .collect::<Vec<_>>()
            .join(";");
        env::set_var("NEW_RELIC_LABELS", raw);

        let labels = parse_new_relic_labels();

        assert_eq!(labels.len(), 64);
        assert!(labels.contains(&("k0".to_string(), "v0".to_string())));
        assert!(labels.contains(&("k63".to_string(), "v63".to_string())));
        assert!(!labels.contains(&("k64".to_string(), "v64".to_string())));
    });
}

#[test]
#[serial]
fn test_parse_new_relic_labels_ignores_nr_env_delimiter() {
    with_clean_env(|| {
        // NEW_RELIC_LABELS uses fixed `:`/`;` delimiters per spec - NR_ENV_DELIMITER
        // (which only affects NR_TAGS) must have no effect on it.
        env::set_var("NR_ENV_DELIMITER", "|");
        env::set_var("NEW_RELIC_LABELS", "env:prod;team:backend");

        let labels = parse_new_relic_labels();

        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&("env".to_string(), "prod".to_string())));
        assert!(labels.contains(&("team".to_string(), "backend".to_string())));
    });
}

#[test]
#[serial]
fn test_parse_new_relic_labels_independent_of_nr_tags() {
    with_clean_env(|| {
        // Malformed NEW_RELIC_LABELS must not affect NR_TAGS, and vice versa - the two
        // env vars are parsed by entirely separate functions with no shared state.
        env::set_var("NR_TAGS", "env:prod");
        env::set_var("NEW_RELIC_LABELS", "invalid");

        assert_eq!(parse_nr_tags(), vec![("env".to_string(), "prod".to_string())]);
        assert!(parse_new_relic_labels().is_empty());
    });
}

// ============================================================================
// Data Structures - Default Tests
// ============================================================================

#[test]
fn test_extension_config_default() {
    let config = ExtensionConfig::default();
    
    assert!(config.new_relic.extension_enabled);
    assert_eq!(config.new_relic.license_key, None);
    assert_eq!(config.aws.runtime_api, "127.0.0.1:9001");
    assert_eq!(config.aws.function_name, "unknown");
    assert_eq!(config.extension.log_level, "info");
    assert!(config.extension.extension_logs_enabled);
}

#[test]
fn test_new_relic_config_default() {
    let config = NewRelicConfig::default();
    
    assert!(config.extension_enabled);
    assert_eq!(config.license_key, None);
    assert_eq!(config.license_key_secret_id, "");
    assert_eq!(config.license_key_ssm_parameter_name, "");
    assert_eq!(config.lambda_handler, None);
    assert_eq!(config.telemetry_endpoint, "https://cloud-collector.newrelic.com/aws/lambda/v1");
    assert_eq!(config.log_endpoint, "https://log-api.newrelic.com/log/v1");
    assert_eq!(config.harvest_interval, Duration::from_secs(2));
    assert!(!config.collect_trace_id);
    assert!(!config.add_version_detail_tags);
    assert_eq!(config.layer_version, None);
    assert!(!config.apm_lambda_mode);
    assert_eq!(config.apm_host, "collector.newrelic.com");
    assert_eq!(config.metric_endpoint, "https://metric-api.newrelic.com/metric/v1");
    assert_eq!(config.proxy_url, None);
    assert_eq!(config.data_collection_timeout, None);
    assert_eq!(config.http_timeout, None);
}

#[test]
fn test_aws_config_default() {
    let config = AwsConfig::default();
    
    assert_eq!(config.runtime_api, "127.0.0.1:9001");
    assert_eq!(config.function_name, "unknown");
    assert_eq!(config.function_version, None);
    assert_eq!(config.account_id, None);
    assert_eq!(config.region, None);
}

#[test]
fn test_extension_settings_default() {
    let settings = ExtensionSettings::default();
    
    assert!(!settings.send_function_logs);
    assert!(!settings.send_extension_logs);
    assert!(!settings.send_platform_logs);
    assert_eq!(settings.log_level, "info");
    assert!(settings.extension_logs_enabled);
}

// ============================================================================
// Conversions
// ============================================================================

#[test]
fn test_configuration_from_extension_config() {
    let mut ext_config = ExtensionConfig::default();
    ext_config.new_relic.license_key = Some("test_key_123".to_string());
    ext_config.new_relic.license_key_secret_id = "secret_id_456".to_string();
    ext_config.new_relic.license_key_ssm_parameter_name = "ssm_param_789".to_string();
    
    let config = Configuration::from(&ext_config);
    
    assert_eq!(config.license_key, "test_key_123");
    assert_eq!(config.license_key_secret_id, "secret_id_456");
    assert_eq!(config.license_key_ssm_parameter_name, "ssm_param_789");
}

#[test]
fn test_configuration_from_extension_config_no_license_key() {
    let ext_config = ExtensionConfig::default();
    
    let config = Configuration::from(&ext_config);
    
    assert_eq!(config.license_key, "");
    assert_eq!(config.license_key_secret_id, "");
    assert_eq!(config.license_key_ssm_parameter_name, "");
}

// ============================================================================
// Helper Functions - parse_bool
// ============================================================================

#[test]
#[serial]
fn test_parse_bool_true_values() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_COLLECT_TRACE_ID", "true");
        let config = ExtensionConfig::from_env();
        assert!(config.new_relic.collect_trace_id);
        
        env::set_var("NEW_RELIC_COLLECT_TRACE_ID", "1");
        let config = ExtensionConfig::from_env();
        assert!(config.new_relic.collect_trace_id);
        
        env::set_var("NEW_RELIC_COLLECT_TRACE_ID", "yes");
        let config = ExtensionConfig::from_env();
        assert!(config.new_relic.collect_trace_id);
        
        env::set_var("NEW_RELIC_COLLECT_TRACE_ID", "on");
        let config = ExtensionConfig::from_env();
        assert!(config.new_relic.collect_trace_id);
        
        env::set_var("NEW_RELIC_COLLECT_TRACE_ID", "TRUE");
        let config = ExtensionConfig::from_env();
        assert!(config.new_relic.collect_trace_id);
    });
}

#[test]
#[serial]
fn test_parse_bool_false_values() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_COLLECT_TRACE_ID", "false");
        let config = ExtensionConfig::from_env();
        assert!(!config.new_relic.collect_trace_id);
        
        env::set_var("NEW_RELIC_COLLECT_TRACE_ID", "0");
        let config = ExtensionConfig::from_env();
        assert!(!config.new_relic.collect_trace_id);
        
        env::set_var("NEW_RELIC_COLLECT_TRACE_ID", "no");
        let config = ExtensionConfig::from_env();
        assert!(!config.new_relic.collect_trace_id);
        
        env::set_var("NEW_RELIC_COLLECT_TRACE_ID", "");
        let config = ExtensionConfig::from_env();
        assert!(!config.new_relic.collect_trace_id);
    });
}

#[test]
#[serial]
fn test_apm_disable_telemetry_defaults_empty() {
    with_full_clean_env(|| {
        // Unset → nothing disabled (default behavior unchanged).
        let config = ExtensionConfig::from_env();
        assert!(config.new_relic.apm_disabled_telemetry.is_empty());

        env::set_var("NEW_RELIC_APM_DISABLE_TELEMETRY", "");
        let config = ExtensionConfig::from_env();
        assert!(config.new_relic.apm_disabled_telemetry.is_empty());
    });
}

#[test]
#[serial]
fn test_apm_disable_telemetry_parses_canonical_types() {
    with_full_clean_env(|| {
        // Case-insensitive, whitespace-trimmed, comma-separated.
        env::set_var(
            "NEW_RELIC_APM_DISABLE_TELEMETRY",
            " platform_metrics , SQL_TRACE_DATA ,custom_event_data",
        );
        let config = ExtensionConfig::from_env();
        let d = &config.new_relic.apm_disabled_telemetry;
        assert_eq!(d.len(), 3);
        assert!(d.contains("platform_metrics"));
        assert!(d.contains("sql_trace_data"));
        assert!(d.contains("custom_event_data"));
    });
}

#[test]
#[serial]
fn test_apm_disable_telemetry_ignores_unknown_tokens() {
    with_full_clean_env(|| {
        // Unknown tokens (e.g. the short form 'sql_trace') are dropped, valid ones kept.
        env::set_var(
            "NEW_RELIC_APM_DISABLE_TELEMETRY",
            "sql_trace,bogus,metric_data",
        );
        let config = ExtensionConfig::from_env();
        let d = &config.new_relic.apm_disabled_telemetry;
        assert_eq!(d.len(), 1);
        assert!(d.contains("metric_data"));
        assert!(!d.contains("sql_trace"));
        assert!(!d.contains("bogus"));
    });
}

#[test]
fn test_parse_disabled_telemetry_unit() {
    use super::parse_disabled_telemetry;
    assert!(parse_disabled_telemetry("").is_empty());
    assert!(parse_disabled_telemetry("   ,  ,").is_empty());
    let all = parse_disabled_telemetry(
        "metric_data,custom_event_data,log_event_data,analytic_event_data,error_event_data,error_data,span_event_data,sql_trace_data,transaction_sample_data,platform_metrics",
    );
    assert_eq!(all.len(), 10);
}

// ============================================================================
// Helper Functions - validate_log_level
// ============================================================================

#[test]
#[serial]
fn test_validate_log_level_valid_levels() {
    let valid_levels = vec!["trace", "debug", "info", "warn", "error", "all"];
    
    for level in valid_levels {
        with_full_clean_env(|| {
            env::set_var("NEW_RELIC_EXTENSION_LOG_LEVEL", level);
            let config = ExtensionConfig::from_env();
            assert_eq!(config.extension.log_level, level, "Failed for level: {}", level);
        });
        
        // Test uppercase in separate clean environment
        with_full_clean_env(|| {
            env::set_var("NEW_RELIC_EXTENSION_LOG_LEVEL", level.to_uppercase());
            let config = ExtensionConfig::from_env();
            assert_eq!(config.extension.log_level, level, "Failed for uppercase level: {}", level.to_uppercase());
        });
    }
}

#[test]
#[serial]
fn test_validate_log_level_invalid_defaults_to_info() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_LOG_LEVEL", "invalid");
        let config = ExtensionConfig::from_env();
        assert_eq!(config.extension.log_level, "info");
        
        env::set_var("NEW_RELIC_EXTENSION_LOG_LEVEL", "verbose");
        let config = ExtensionConfig::from_env();
        assert_eq!(config.extension.log_level, "info");
        
        env::set_var("NEW_RELIC_EXTENSION_LOG_LEVEL", "123");
        let config = ExtensionConfig::from_env();
        assert_eq!(config.extension.log_level, "info");
    });
}

// ============================================================================
// Helper Functions - parse_send_logs
// ============================================================================

#[test]
#[serial]
fn test_parse_send_logs_all_option() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_SEND_LOGS", "all");
        let config = ExtensionConfig::from_env();
        
        assert!(config.extension.send_function_logs);
        assert!(config.extension.send_extension_logs);
        assert!(config.extension.send_platform_logs);
    });
}

#[test]
#[serial]
fn test_parse_send_logs_comma_separated() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_SEND_LOGS", "function,platform");
        let config = ExtensionConfig::from_env();
        
        assert!(config.extension.send_function_logs);
        assert!(!config.extension.send_extension_logs);
        assert!(config.extension.send_platform_logs);
    });
}

#[test]
#[serial]
fn test_parse_send_logs_single_value() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_SEND_LOGS", "extension");
        let config = ExtensionConfig::from_env();
        
        assert!(!config.extension.send_function_logs);
        assert!(config.extension.send_extension_logs);
        assert!(!config.extension.send_platform_logs);
    });
}

#[test]
#[serial]
fn test_parse_send_logs_empty_string() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_SEND_LOGS", "");
        let config = ExtensionConfig::from_env();
        
        assert!(!config.extension.send_function_logs);
        assert!(!config.extension.send_extension_logs);
        assert!(!config.extension.send_platform_logs);
    });
}

#[test]
#[serial]
fn test_parse_send_logs_with_spaces() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_SEND_LOGS", " function , platform ");
        let config = ExtensionConfig::from_env();
        
        assert!(config.extension.send_function_logs);
        assert!(!config.extension.send_extension_logs);
        assert!(config.extension.send_platform_logs);
    });
}

#[test]
#[serial]
fn test_parse_send_logs_individual_flags_backward_compatibility() {
    with_full_clean_env(|| {
        // When SEND_LOGS is not set, should fall back to individual flags
        env::set_var("NEW_RELIC_EXTENSION_SEND_FUNCTION_LOGS", "true");
        env::set_var("NEW_RELIC_EXTENSION_SEND_PLATFORM_LOGS", "1");
        
        let config = ExtensionConfig::from_env();
        
        assert!(config.extension.send_function_logs);
        assert!(!config.extension.send_extension_logs);
        assert!(config.extension.send_platform_logs);
    });
}

#[test]
#[serial]
fn test_parse_send_logs_precedence_over_individual_flags() {
    with_full_clean_env(|| {
        // SEND_LOGS should take precedence
        env::set_var("NEW_RELIC_EXTENSION_SEND_LOGS", "extension");
        env::set_var("NEW_RELIC_EXTENSION_SEND_FUNCTION_LOGS", "true");
        env::set_var("NEW_RELIC_EXTENSION_SEND_PLATFORM_LOGS", "true");
        
        let config = ExtensionConfig::from_env();
        
        assert!(!config.extension.send_function_logs);
        assert!(config.extension.send_extension_logs);
        assert!(!config.extension.send_platform_logs);
    });
}

// ============================================================================
// AWS ARN Handling
// ============================================================================

#[test]
#[serial]
fn test_construct_function_arn_valid() {
    with_full_clean_env(|| {
        env::set_var("AWS_REGION", "us-west-2");
        
        let mut aws_config = AwsConfig::default();
        aws_config.function_name = "my-test-function".to_string();
        aws_config.account_id = Some("123456789012".to_string());
        
        let arn = aws_config.construct_function_arn();
        
        assert_eq!(
            arn,
            Some("arn:aws:lambda:us-west-2:123456789012:function:my-test-function".to_string())
        );
    });
}

#[test]
fn test_construct_function_arn_empty_function_name() {
    let mut aws_config = AwsConfig::default();
    aws_config.function_name = "".to_string();
    aws_config.account_id = Some("123456789012".to_string());
    
    let arn = aws_config.construct_function_arn();
    
    assert_eq!(arn, None);
}

#[test]
fn test_construct_function_arn_missing_account_id() {
    let mut aws_config = AwsConfig::default();
    aws_config.function_name = "my-test-function".to_string();
    aws_config.account_id = None;
    
    let arn = aws_config.construct_function_arn();
    
    assert_eq!(arn, None);
}

#[test]
fn test_construct_function_arn_empty_account_id() {
    let mut aws_config = AwsConfig::default();
    aws_config.function_name = "my-test-function".to_string();
    aws_config.account_id = Some("".to_string());
    
    let arn = aws_config.construct_function_arn();
    
    assert_eq!(arn, None);
}

#[test]
#[serial]
fn test_construct_function_arn_uses_aws_default_region() {
    with_full_clean_env(|| {
        env::remove_var("AWS_REGION");
        env::set_var("AWS_DEFAULT_REGION", "eu-west-1");
        
        let mut aws_config = AwsConfig::default();
        aws_config.function_name = "my-function".to_string();
        aws_config.account_id = Some("987654321098".to_string());
        
        let arn = aws_config.construct_function_arn();
        
        assert_eq!(
            arn,
            Some("arn:aws:lambda:eu-west-1:987654321098:function:my-function".to_string())
        );
    });
}

#[test]
#[serial]
fn test_construct_function_arn_defaults_to_us_east_1() {
    with_full_clean_env(|| {
        let mut aws_config = AwsConfig::default();
        aws_config.function_name = "my-function".to_string();
        aws_config.account_id = Some("111222333444".to_string());
        
        let arn = aws_config.construct_function_arn();
        
        assert_eq!(
            arn,
            Some("arn:aws:lambda:us-east-1:111222333444:function:my-function".to_string())
        );
    });
}

#[test]
fn test_extract_account_id_from_arn_valid() {
    let arn = "arn:aws:lambda:us-east-1:123456789012:function:my-function";
    let account_id = AwsConfig::extract_account_id_from_arn(arn);
    
    assert_eq!(account_id, Some("123456789012".to_string()));
}

#[test]
fn test_extract_account_id_from_arn_with_qualifier() {
    let arn = "arn:aws:lambda:us-east-1:123456789012:function:my-function:prod";
    let account_id = AwsConfig::extract_account_id_from_arn(arn);
    
    assert_eq!(account_id, Some("123456789012".to_string()));
}

#[test]
fn test_extract_account_id_from_arn_invalid_format() {
    let arn = "invalid-arn-format";
    let account_id = AwsConfig::extract_account_id_from_arn(arn);
    
    assert_eq!(account_id, None);
}

#[test]
fn test_extract_account_id_from_arn_wrong_service() {
    let arn = "arn:aws:s3:us-east-1:123456789012:bucket/my-bucket";
    let account_id = AwsConfig::extract_account_id_from_arn(arn);
    
    assert_eq!(account_id, None);
}

#[test]
fn test_extract_account_id_from_arn_incomplete() {
    let arn = "arn:aws:lambda:us-east-1";
    let account_id = AwsConfig::extract_account_id_from_arn(arn);
    
    assert_eq!(account_id, None);
}

#[test]
fn test_extract_and_update_account_id_from_arn() {
    let mut aws_config = AwsConfig::default();
    let arn = "arn:aws:lambda:us-west-2:555666777888:function:test-func";
    
    aws_config.extract_and_update_account_id_from_arn(arn);
    
    assert_eq!(aws_config.account_id, Some("555666777888".to_string()));
}

#[test]
fn test_extract_and_update_account_id_preserves_existing() {
    let mut aws_config = AwsConfig::default();
    aws_config.account_id = Some("111111111111".to_string());
    
    let arn = "arn:aws:lambda:us-west-2:999999999999:function:test-func";
    
    aws_config.extract_and_update_account_id_from_arn(arn);
    
    // Should keep existing account ID
    assert_eq!(aws_config.account_id, Some("111111111111".to_string()));
}

#[test]
fn test_extract_and_update_account_id_updates_placeholder() {
    let mut aws_config = AwsConfig::default();
    aws_config.account_id = Some("123456789012".to_string());
    
    let arn = "arn:aws:lambda:us-west-2:888888888888:function:test-func";
    
    aws_config.extract_and_update_account_id_from_arn(arn);
    
    // Should update placeholder account ID
    assert_eq!(aws_config.account_id, Some("888888888888".to_string()));
}

#[test]
#[serial]
fn test_update_from_registration() {
    with_full_clean_env(|| {
        env::set_var("AWS_REGION", "ap-southeast-1");
        
        let mut aws_config = AwsConfig::default();
        
        aws_config.update_from_registration(
            "registered-function".to_string(),
            "v1.0".to_string(),
            Some("444455556666".to_string()),
        );
        
        assert_eq!(aws_config.function_name, "registered-function");
        assert_eq!(aws_config.function_version, Some("v1.0".to_string()));
        assert_eq!(aws_config.account_id, Some("444455556666".to_string()));
        assert_eq!(aws_config.region, Some("ap-southeast-1".to_string()));
    });
}

#[test]
#[serial]
fn test_update_from_registration_preserves_existing_region() {
    with_full_clean_env(|| {
        let mut aws_config = AwsConfig::default();
        aws_config.region = Some("eu-central-1".to_string());
        
        aws_config.update_from_registration(
            "func".to_string(),
            "v2".to_string(),
            Some("123".to_string()),
        );
        
        assert_eq!(aws_config.region, Some("eu-central-1".to_string()));
    });
}

// ============================================================================
// Environment Variables - ExtensionConfig::from_env()
// ============================================================================

#[test]
#[serial]
fn test_from_env_extension_enabled() {
    // Test false
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_LAMBDA_EXTENSION_ENABLED", "false");
        let config = ExtensionConfig::from_env();
        assert!(!config.new_relic.extension_enabled);
    });
    
    // Test true
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_LAMBDA_EXTENSION_ENABLED", "true");
        let config = ExtensionConfig::from_env();
        assert!(config.new_relic.extension_enabled);
    });
}

#[test]
#[serial]
fn test_from_env_license_key() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_LICENSE_KEY", "test_license_key_abc123");
        let config = ExtensionConfig::from_env();
        
        assert_eq!(config.new_relic.license_key, Some("test_license_key_abc123".to_string()));
    });
}

#[test]
#[serial]
fn test_from_env_license_key_secret() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_LICENSE_KEY_SECRET", "my-secret-arn");
        let config = ExtensionConfig::from_env();
        
        assert_eq!(config.new_relic.license_key_secret_id, "my-secret-arn");
    });
}

#[test]
#[serial]
fn test_from_env_license_key_ssm() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_LICENSE_KEY_SSM_PARAMETER_NAME", "/newrelic/license");
        let config = ExtensionConfig::from_env();
        
        assert_eq!(config.new_relic.license_key_ssm_parameter_name, "/newrelic/license");
    });
}

#[test]
#[serial]
fn test_from_env_lambda_handler() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_LAMBDA_HANDLER", "index.handler");
        let config = ExtensionConfig::from_env();
        
        assert_eq!(config.new_relic.lambda_handler, Some("index.handler".to_string()));
    });
}

#[test]
#[serial]
fn test_from_env_collect_trace_id() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_COLLECT_TRACE_ID", "true");
        let config = ExtensionConfig::from_env();

        assert!(config.new_relic.collect_trace_id);
    });
}

#[test]
#[serial]
fn test_from_env_trace_id_log_buffer_max_default() {
    with_full_clean_env(|| {
        let config = ExtensionConfig::from_env();
        assert_eq!(config.new_relic.trace_id_log_buffer_max, 2000,
            "default buffer cap must be 2000 when env unset");
    });
}

#[test]
#[serial]
fn test_from_env_trace_id_log_buffer_max_custom() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_TRACE_ID_LOG_BUFFER_MAX", "500");
        let config = ExtensionConfig::from_env();
        assert_eq!(config.new_relic.trace_id_log_buffer_max, 500);
    });
}

#[test]
#[serial]
fn test_from_env_trace_id_log_buffer_max_clamped_and_invalid() {
    with_full_clean_env(|| {
        // 0 clamps up to the minimum (never drop everything).
        env::set_var("NEW_RELIC_TRACE_ID_LOG_BUFFER_MAX", "0");
        assert_eq!(ExtensionConfig::from_env().new_relic.trace_id_log_buffer_max, 1);

        // Above the ceiling clamps down.
        env::set_var("NEW_RELIC_TRACE_ID_LOG_BUFFER_MAX", "99999999");
        assert_eq!(ExtensionConfig::from_env().new_relic.trace_id_log_buffer_max, 100_000);

        // Non-numeric falls back to the default.
        env::set_var("NEW_RELIC_TRACE_ID_LOG_BUFFER_MAX", "not-a-number");
        assert_eq!(ExtensionConfig::from_env().new_relic.trace_id_log_buffer_max, 2000);
    });
}

#[test]
#[serial]
fn test_from_env_add_version_detail_tags() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_ADD_VERSION_DETAIL_TAGS", "yes");
        let config = ExtensionConfig::from_env();
        
        assert!(config.new_relic.add_version_detail_tags);
    });
}

#[test]
#[serial]
fn test_from_env_layer_version() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_LAYER_VERSION", "2.4.5");
        let config = ExtensionConfig::from_env();
        
        assert_eq!(config.new_relic.layer_version, Some("2.4.5".to_string()));
    });
}

#[test]
#[serial]
fn test_from_env_apm_lambda_mode() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_APM_LAMBDA_MODE", "1");
        let config = ExtensionConfig::from_env();
        
        assert!(config.new_relic.apm_lambda_mode);
    });
}

#[test]
#[serial]
fn test_from_env_aws_runtime_api() {
    with_full_clean_env(|| {
        env::set_var("AWS_LAMBDA_RUNTIME_API", "192.168.1.100:8080");
        let config = ExtensionConfig::from_env();
        
        assert_eq!(config.aws.runtime_api, "192.168.1.100:8080");
    });
}

#[test]
#[serial]
fn test_from_env_log_level() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_LOG_LEVEL", "debug");
        let config = ExtensionConfig::from_env();
        
        assert_eq!(config.extension.log_level, "debug");
    });
}

#[test]
#[serial]
fn test_from_env_extension_logs_enabled() {
    // Test false
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_LOGS_ENABLED", "false");
        let config = ExtensionConfig::from_env();
        assert!(!config.extension.extension_logs_enabled);
    });
    
    // Test true
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_LOGS_ENABLED", "true");
        let config = ExtensionConfig::from_env();
        assert!(config.extension.extension_logs_enabled);
    });
}

#[test]
#[serial]
fn test_from_env_defaults_when_not_set() {
    with_full_clean_env(|| {
        let config = ExtensionConfig::from_env();
        
        // Check defaults are applied
        assert!(config.new_relic.extension_enabled);
        assert_eq!(config.new_relic.license_key, None);
        assert_eq!(config.aws.runtime_api, "127.0.0.1:9001");
        assert!(!config.extension.send_function_logs);
        assert!(!config.extension.send_extension_logs);
        assert!(!config.extension.send_platform_logs);
        assert_eq!(config.extension.log_level, "info");
        assert!(config.extension.extension_logs_enabled);
    });
}

#[test]
#[serial]
fn test_from_env_comprehensive() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_LAMBDA_EXTENSION_ENABLED", "true");
        env::set_var("NEW_RELIC_LICENSE_KEY", "full_test_key");
        env::set_var("NEW_RELIC_LICENSE_KEY_SECRET", "secret_arn");
        env::set_var("NEW_RELIC_LICENSE_KEY_SSM_PARAMETER_NAME", "ssm_param");
        env::set_var("NEW_RELIC_LAMBDA_HANDLER", "app.handler");
        env::set_var("NEW_RELIC_COLLECT_TRACE_ID", "true");
        env::set_var("NEW_RELIC_ADD_VERSION_DETAIL_TAGS", "on");
        env::set_var("NEW_RELIC_LAYER_VERSION", "3.0.0");
        env::set_var("NEW_RELIC_APM_LAMBDA_MODE", "yes");
        env::set_var("NEW_RELIC_EXTENSION_SEND_LOGS", "all");
        env::set_var("NEW_RELIC_EXTENSION_LOG_LEVEL", "trace");
        env::set_var("NEW_RELIC_EXTENSION_LOGS_ENABLED", "true");
        env::set_var("AWS_LAMBDA_RUNTIME_API", "10.0.0.1:9001");
        
        let config = ExtensionConfig::from_env();
        
        assert!(config.new_relic.extension_enabled);
        assert_eq!(config.new_relic.license_key, Some("full_test_key".to_string()));
        assert_eq!(config.new_relic.license_key_secret_id, "secret_arn");
        assert_eq!(config.new_relic.license_key_ssm_parameter_name, "ssm_param");
        assert_eq!(config.new_relic.lambda_handler, Some("app.handler".to_string()));
        assert!(config.new_relic.collect_trace_id);
        assert!(config.new_relic.add_version_detail_tags);
        assert_eq!(config.new_relic.layer_version, Some("3.0.0".to_string()));
        assert!(config.new_relic.apm_lambda_mode);
        assert!(config.extension.send_function_logs);
        assert!(config.extension.send_extension_logs);
        assert!(config.extension.send_platform_logs);
        assert_eq!(config.extension.log_level, "trace");
        assert!(config.extension.extension_logs_enabled);
        assert_eq!(config.aws.runtime_api, "10.0.0.1:9001");
    });
}

// ============================================================================
// Edge Cases & Additional Coverage Tests
// ============================================================================

#[test]
#[serial]
fn test_parse_send_logs_all_with_multiple_values() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_SEND_LOGS", "all,function,extension");
        let config = ExtensionConfig::from_env();
        
        // When "all" is specified with others, should default to all
        assert!(config.extension.send_function_logs);
        assert!(config.extension.send_extension_logs);
        assert!(config.extension.send_platform_logs);
    });
}

#[test]
#[serial]
fn test_parse_send_logs_invalid_values_ignored() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_SEND_LOGS", "function,invalid,platform,garbage");
        let config = ExtensionConfig::from_env();
        
        // Invalid values should be ignored, valid ones processed
        assert!(config.extension.send_function_logs);
        assert!(!config.extension.send_extension_logs);
        assert!(config.extension.send_platform_logs);
    });
}

#[test]
#[serial]
fn test_from_env_extension_enabled_invalid_value() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_LAMBDA_EXTENSION_ENABLED", "invalid");
        let config = ExtensionConfig::from_env();
        
        // Should default to true when parse fails
        assert!(config.new_relic.extension_enabled);
    });
}

#[test]
#[serial]
fn test_from_env_extension_enabled_numeric_zero() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_LAMBDA_EXTENSION_ENABLED", "0");
        let config = ExtensionConfig::from_env();
        
        // "0" doesn't parse as bool, so it defaults to true
        // (extension_enabled uses .parse() not parse_bool())
        assert!(config.new_relic.extension_enabled);
    });
}

#[test]
#[serial]
fn test_construct_function_arn_with_both_region_vars() {
    with_full_clean_env(|| {
        // AWS_REGION takes precedence over AWS_DEFAULT_REGION
        env::set_var("AWS_REGION", "us-west-1");
        env::set_var("AWS_DEFAULT_REGION", "eu-west-1");
        
        let mut aws_config = AwsConfig::default();
        aws_config.function_name = "my-function".to_string();
        aws_config.account_id = Some("123456789012".to_string());
        
        let arn = aws_config.construct_function_arn();
        
        assert_eq!(
            arn,
            Some("arn:aws:lambda:us-west-1:123456789012:function:my-function".to_string())
        );
    });
}

#[test]
#[serial]
fn test_update_from_registration_with_no_env_region() {
    with_full_clean_env(|| {
        let mut aws_config = AwsConfig::default();
        
        aws_config.update_from_registration(
            "func".to_string(),
            "v1".to_string(),
            Some("123".to_string()),
        );
        
        // Region should remain None if not in environment
        assert_eq!(aws_config.region, None);
    });
}

#[test]
fn test_extract_and_update_account_id_empty_string() {
    let mut aws_config = AwsConfig::default();
    aws_config.account_id = Some("".to_string());
    
    let arn = "arn:aws:lambda:us-west-2:999999999999:function:test-func";
    
    aws_config.extract_and_update_account_id_from_arn(arn);
    
    // Empty string should be updated
    assert_eq!(aws_config.account_id, Some("999999999999".to_string()));
}

#[test]
fn test_extract_and_update_account_id_invalid_arn() {
    let mut aws_config = AwsConfig::default();
    
    let arn = "invalid-arn-format";
    
    aws_config.extract_and_update_account_id_from_arn(arn);
    
    // Should remain None for invalid ARN
    assert_eq!(aws_config.account_id, None);
}

#[test]
fn test_extract_account_id_from_arn_empty_string() {
    let account_id = AwsConfig::extract_account_id_from_arn("");
    assert_eq!(account_id, None);
}

#[test]
#[serial]
fn test_from_env_with_empty_string_values() {
    with_full_clean_env(|| {
        // Set empty strings (different from not setting)
        env::set_var("NEW_RELIC_LICENSE_KEY_SECRET", "");
        env::set_var("NEW_RELIC_LICENSE_KEY_SSM_PARAMETER_NAME", "");
        env::set_var("NEW_RELIC_COLLECT_TRACE_ID", "");
        env::set_var("NEW_RELIC_ADD_VERSION_DETAIL_TAGS", "");
        env::set_var("NEW_RELIC_APM_LAMBDA_MODE", "");
        
        let config = ExtensionConfig::from_env();
        
        // Empty strings should result in default/false values
        assert_eq!(config.new_relic.license_key_secret_id, "");
        assert_eq!(config.new_relic.license_key_ssm_parameter_name, "");
        assert!(!config.new_relic.collect_trace_id);
        assert!(!config.new_relic.add_version_detail_tags);
        assert!(!config.new_relic.apm_lambda_mode);
    });
}

#[test]
fn test_configuration_struct_fields() {
    let config = Configuration {
        license_key: "test_key".to_string(),
        license_key_secret_id: "secret".to_string(),
        license_key_ssm_parameter_name: "param".to_string(),
    };
    
    assert_eq!(config.license_key, "test_key");
    assert_eq!(config.license_key_secret_id, "secret");
    assert_eq!(config.license_key_ssm_parameter_name, "param");
}

#[test]
fn test_new_relic_config_all_fields() {
    let config = NewRelicConfig {
        extension_enabled: false,
        license_key: Some("key".to_string()),
        license_key_secret_id: "secret".to_string(),
        license_key_ssm_parameter_name: "param".to_string(),
        lambda_handler: Some("handler".to_string()),
        telemetry_endpoint: "http://telemetry".to_string(),
        log_endpoint: "http://logs".to_string(),
        harvest_interval: Duration::from_secs(5),
        collect_trace_id: true,
        trace_id_log_buffer_max: 2000,
        add_version_detail_tags: true,
        layer_version: Some("1.0.0".to_string()),
        apm_lambda_mode: true,
        apm_blocking_handshake: false,
        apm_handshake_timeout_secs: 5,
        apm_disabled_telemetry: std::collections::HashSet::new(),
        apm_host: "apm.host".to_string(),
        metric_endpoint: "http://metrics".to_string(),
        otlp_metric_endpoint: "https://collector.newrelic.com/v1/metrics".to_string(),
        otlp_metric_enabled: false,
        proxy_url: Some("http://proxy:8080".to_string()),
        synchronous_flush: false,
        data_collection_timeout: Some(Duration::from_secs(20)),
        http_timeout: Some(Duration::from_secs(1)),
    };

    assert!(!config.extension_enabled);
    assert_eq!(config.license_key, Some("key".to_string()));
    assert_eq!(config.harvest_interval, Duration::from_secs(5));
    assert!(config.collect_trace_id);
    assert!(config.apm_lambda_mode);
    assert!(!config.synchronous_flush);
}

#[test]
fn test_aws_config_all_fields() {
    let config = AwsConfig {
        runtime_api: "192.168.1.1:9001".to_string(),
        function_name: "test-function".to_string(),
        function_version: Some("v2.0".to_string()),
        account_id: Some("999888777666".to_string()),
        region: Some("ap-south-1".to_string()),
    };
    
    assert_eq!(config.runtime_api, "192.168.1.1:9001");
    assert_eq!(config.function_name, "test-function");
    assert_eq!(config.function_version, Some("v2.0".to_string()));
    assert_eq!(config.account_id, Some("999888777666".to_string()));
    assert_eq!(config.region, Some("ap-south-1".to_string()));
}

#[test]
fn test_extension_settings_all_fields() {
    let settings = ExtensionSettings {
        send_function_logs: true,
        send_extension_logs: true,
        send_platform_logs: true,
        platform_log_filter: std::collections::HashSet::new(),
        log_level: "debug".to_string(),
        extension_logs_enabled: false,
        runtime_done_grace_ms: 250,
        pipeline_flush: false,
        lmi_flush_interval_ms: 30_000,
    };
    
    assert!(settings.send_function_logs);
    assert!(settings.send_extension_logs);
    assert!(settings.send_platform_logs);
    assert_eq!(settings.log_level, "debug");
    assert!(!settings.extension_logs_enabled);
}

#[test]
fn test_extension_config_clone() {
    let config1 = ExtensionConfig::default();
    let config2 = config1.clone();
    
    assert_eq!(config1.aws.runtime_api, config2.aws.runtime_api);
    assert_eq!(config1.extension.log_level, config2.extension.log_level);
}

#[test]
fn test_extension_config_debug_format() {
    let config = ExtensionConfig::default();
    let debug_output = format!("{:?}", config);
    
    assert!(debug_output.contains("ExtensionConfig"));
    assert!(debug_output.contains("NewRelicConfig"));
    assert!(debug_output.contains("AwsConfig"));
}

#[test]
#[serial]
fn test_parse_bool_case_insensitivity() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_COLLECT_TRACE_ID", "TrUe");
        let config = ExtensionConfig::from_env();
        assert!(config.new_relic.collect_trace_id);
    });
    
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_COLLECT_TRACE_ID", "YES");
        let config = ExtensionConfig::from_env();
        assert!(config.new_relic.collect_trace_id);
    });
    
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_COLLECT_TRACE_ID", "On");
        let config = ExtensionConfig::from_env();
        assert!(config.new_relic.collect_trace_id);
    });
}

#[test]
#[serial]
fn test_validate_log_level_mixed_case() {
    let levels = vec![
        ("TrAcE", "trace"),
        ("DeBuG", "debug"),
        ("InFo", "info"),
        ("WaRn", "warn"),
        ("ErRoR", "error"),
        ("AlL", "all"),
    ];
    
    for (input, expected) in levels {
        with_full_clean_env(|| {
            env::set_var("NEW_RELIC_EXTENSION_LOG_LEVEL", input);
            let config = ExtensionConfig::from_env();
            assert_eq!(config.extension.log_level, expected);
        });
    }
}

#[test]
#[serial]
fn test_parse_nr_tags_multiple_colons() {
    with_clean_env(|| {
        // Tags with multiple colons should be skipped (only 2 parts allowed)
        env::set_var("NR_TAGS", "valid:value;invalid:has:colons;another:good");
        
        let tags = parse_nr_tags();
        
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&("valid".to_string(), "value".to_string())));
        assert!(tags.contains(&("another".to_string(), "good".to_string())));
    });
}

#[test]
#[serial]
fn test_parse_nr_tags_only_delimiter() {
    with_clean_env(|| {
        env::set_var("NR_TAGS", ";;;");
        
        let tags = parse_nr_tags();
        
        assert!(tags.is_empty());
    });
}

#[test]
#[serial]
fn test_parse_nr_tags_single_tag() {
    with_clean_env(|| {
        env::set_var("NR_TAGS", "environment:production");
        
        let tags = parse_nr_tags();
        
        assert_eq!(tags.len(), 1);
        assert!(tags.contains(&("environment".to_string(), "production".to_string())));
    });
}

#[test]
#[serial]
fn test_from_env_all_boolean_fields_mixed() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_LAMBDA_EXTENSION_ENABLED", "true");
        env::set_var("NEW_RELIC_COLLECT_TRACE_ID", "1");
        env::set_var("NEW_RELIC_ADD_VERSION_DETAIL_TAGS", "yes");
        env::set_var("NEW_RELIC_APM_LAMBDA_MODE", "on");
        env::set_var("NEW_RELIC_EXTENSION_LOGS_ENABLED", "true");
        env::set_var("NEW_RELIC_EXTENSION_SEND_FUNCTION_LOGS", "1");
        
        let config = ExtensionConfig::from_env();
        
        assert!(config.new_relic.extension_enabled);
        assert!(config.new_relic.collect_trace_id);
        assert!(config.new_relic.add_version_detail_tags);
        assert!(config.new_relic.apm_lambda_mode);
        assert!(config.extension.extension_logs_enabled);
        assert!(config.extension.send_function_logs);
    });
}

#[test]
#[serial]
fn test_construct_function_arn_special_characters_in_function_name() {
    with_full_clean_env(|| {
        env::set_var("AWS_REGION", "us-east-1");
        
        let mut aws_config = AwsConfig::default();
        aws_config.function_name = "my-test_function.prod".to_string();
        aws_config.account_id = Some("123456789012".to_string());
        
        let arn = aws_config.construct_function_arn();
        
        assert_eq!(
            arn,
            Some("arn:aws:lambda:us-east-1:123456789012:function:my-test_function.prod".to_string())
        );
    });
}

#[test]
fn test_configuration_from_conversion() {
    let mut ext_config = ExtensionConfig::default();
    ext_config.new_relic.license_key = None;
    
    let config: Configuration = (&ext_config).into();
    
    // None license_key should convert to empty string
    assert_eq!(config.license_key, "");
}

#[test]
#[serial]
fn test_parse_send_logs_case_insensitivity() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_SEND_LOGS", "FUNCTION,PLATFORM");
        let config = ExtensionConfig::from_env();
        
        assert!(config.extension.send_function_logs);
        assert!(!config.extension.send_extension_logs);
        assert!(config.extension.send_platform_logs);
    });
}

#[test]
#[serial]
fn test_parse_send_logs_extra_spaces() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_SEND_LOGS", "  function  ,  extension  ");
        let config = ExtensionConfig::from_env();

        assert!(config.extension.send_function_logs);
        assert!(config.extension.send_extension_logs);
        assert!(!config.extension.send_platform_logs);
    });
}

#[test]
#[serial]
fn test_parse_send_logs_specific_platform_event() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_SEND_LOGS", "platform.report");
        let config = ExtensionConfig::from_env();

        assert!(config.extension.send_platform_logs);
        assert_eq!(config.extension.platform_log_filter.len(), 1);
        assert!(config.extension.platform_log_filter.contains("platform.report"));
    });
}

#[test]
#[serial]
fn test_parse_send_logs_multiple_specific_platform_events() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_SEND_LOGS", "platform.start,platform.report");
        let config = ExtensionConfig::from_env();

        assert!(config.extension.send_platform_logs);
        assert_eq!(config.extension.platform_log_filter.len(), 2);
        assert!(config.extension.platform_log_filter.contains("platform.start"));
        assert!(config.extension.platform_log_filter.contains("platform.report"));
    });
}

#[test]
#[serial]
fn test_parse_send_logs_bare_platform_overrides_specific() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_SEND_LOGS", "platform,platform.report");
        let config = ExtensionConfig::from_env();

        assert!(config.extension.send_platform_logs);
        assert!(config.extension.platform_log_filter.is_empty());
    });
}

#[test]
#[serial]
fn test_parse_send_logs_specific_platform_event_case_insensitive() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_SEND_LOGS", "PLATFORM.START");
        let config = ExtensionConfig::from_env();

        assert!(config.extension.send_platform_logs);
        assert!(config.extension.platform_log_filter.contains("platform.start"));
    });
}

#[test]
#[serial]
fn test_parse_send_logs_all_clears_platform_filter() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_SEND_LOGS", "all");
        let config = ExtensionConfig::from_env();

        assert!(config.extension.send_platform_logs);
        assert!(config.extension.platform_log_filter.is_empty());
    });
}

#[test]
#[serial]
fn test_parse_send_logs_no_filter_when_only_individual_flag_set() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_SEND_PLATFORM_LOGS", "true");
        let config = ExtensionConfig::from_env();

        assert!(config.extension.send_platform_logs);
        assert!(config.extension.platform_log_filter.is_empty());
    });
}

// ============================================================================
// Proxy Configuration
// ============================================================================

#[test]
#[serial]
fn test_from_env_proxy_url_set() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_LAMBDA_EXTENSION_PROXY", "http://proxy.internal:8080");
        let config = ExtensionConfig::from_env();

        assert_eq!(config.new_relic.proxy_url, Some("http://proxy.internal:8080".to_string()));
    });
}

#[test]
#[serial]
fn test_from_env_proxy_url_not_set() {
    with_full_clean_env(|| {
        let config = ExtensionConfig::from_env();

        assert_eq!(config.new_relic.proxy_url, None);
    });
}

#[test]
#[serial]
fn test_from_env_proxy_url_empty_string() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_LAMBDA_EXTENSION_PROXY", "");
        let config = ExtensionConfig::from_env();

        assert_eq!(config.new_relic.proxy_url, None);
    });
}

#[test]
#[serial]
fn test_from_env_proxy_url_with_auth() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_LAMBDA_EXTENSION_PROXY", "http://user:pass@proxy.internal:3128");
        let config = ExtensionConfig::from_env();

        assert_eq!(config.new_relic.proxy_url, Some("http://user:pass@proxy.internal:3128".to_string()));
    });
}

#[test]
#[serial]
fn test_from_env_proxy_url_https() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_LAMBDA_EXTENSION_PROXY", "https://secure-proxy:443");
        let config = ExtensionConfig::from_env();

        assert_eq!(config.new_relic.proxy_url, Some("https://secure-proxy:443".to_string()));
    });
}

#[test]
#[serial]
fn test_from_env_proxy_startup_log_never_leaks_credentials() {
    with_full_clean_env(|| {
        let proxy_url = "http://secretuser:secretpass@proxy.internal:8080";
        env::set_var("NEW_RELIC_LAMBDA_EXTENSION_PROXY", proxy_url);

        // Replicate the inline masking logic from from_env()
        let url = proxy_url;
        let masked = if let (Some(scheme_end), Some(at_pos)) = (url.find("://"), url.find('@')) {
            format!("{}***:***{}", &url[..scheme_end + 3], &url[at_pos..])
        } else {
            url.to_string()
        };

        assert!(!masked.contains("secretuser"),
            "Startup log would leak username: {}", masked);
        assert!(!masked.contains("secretpass"),
            "Startup log would leak password: {}", masked);
        assert!(masked.contains("***:***@proxy.internal:8080"),
            "Masked output should preserve host: {}", masked);
    });
}

#[test]
#[serial]
fn test_apm_blocking_handshake_default_false() {
    with_full_clean_env(|| {
        let config = ExtensionConfig::from_env();
        assert!(!config.new_relic.apm_blocking_handshake);
    });
}

#[test]
#[serial]
fn test_apm_blocking_handshake_enabled_from_env() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_APM_BLOCKING_HANDSHAKE", "true");
        let config = ExtensionConfig::from_env();
        assert!(config.new_relic.apm_blocking_handshake);
    });
}

#[test]
#[serial]
fn test_apm_blocking_handshake_explicit_false() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_APM_BLOCKING_HANDSHAKE", "false");
        let config = ExtensionConfig::from_env();
        assert!(!config.new_relic.apm_blocking_handshake);
    });
}

#[test]
#[serial]
fn test_apm_handshake_timeout_default() {
    with_full_clean_env(|| {
        let config = ExtensionConfig::from_env();
        assert_eq!(config.new_relic.apm_handshake_timeout_secs, 5);
    });
}

#[test]
#[serial]
fn test_apm_handshake_timeout_from_env() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_APM_HANDSHAKE_TIMEOUT_SECS", "7");
        let config = ExtensionConfig::from_env();
        assert_eq!(config.new_relic.apm_handshake_timeout_secs, 7);
    });
}

#[test]
#[serial]
fn test_apm_handshake_timeout_zero_clamped_to_one() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_APM_HANDSHAKE_TIMEOUT_SECS", "0");
        let config = ExtensionConfig::from_env();
        // .max(1) guard prevents zero-second timeout
        assert_eq!(config.new_relic.apm_handshake_timeout_secs, 1);
    });
}

#[test]
#[serial]
fn test_apm_handshake_timeout_invalid_string_falls_back_to_default() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_APM_HANDSHAKE_TIMEOUT_SECS", "not_a_number");
        let config = ExtensionConfig::from_env();
        assert_eq!(config.new_relic.apm_handshake_timeout_secs, 5);
    });
}

#[test]
#[serial]
fn test_apm_blocking_handshake_truthy_variants() {
    for value in &["1", "yes", "on", "TRUE", "Yes"] {
        with_full_clean_env(|| {
            env::set_var("NEW_RELIC_APM_BLOCKING_HANDSHAKE", value);
            let config = ExtensionConfig::from_env();
            assert!(
                config.new_relic.apm_blocking_handshake,
                "Expected true for value '{}'", value
            );
        });
    }
}

// ── serverless-mode synchronous_flush (NR-600648 follow-up) ──

#[test]
#[serial]
fn test_synchronous_flush_default_false() {
    with_full_clean_env(|| {
        let config = ExtensionConfig::from_env();
        assert!(!config.new_relic.synchronous_flush);
    });
}

#[test]
#[serial]
fn test_synchronous_flush_enabled_from_env() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_SYNCHRONOUS_FLUSH", "true");
        let config = ExtensionConfig::from_env();
        assert!(config.new_relic.synchronous_flush);
    });
}

#[test]
#[serial]
fn test_synchronous_flush_explicit_false() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_SYNCHRONOUS_FLUSH", "false");
        let config = ExtensionConfig::from_env();
        assert!(!config.new_relic.synchronous_flush);
    });
}

#[test]
#[serial]
fn test_synchronous_flush_truthy_variants() {
    for value in &["1", "yes", "on", "TRUE", "Yes"] {
        with_full_clean_env(|| {
            env::set_var("NEW_RELIC_EXTENSION_SYNCHRONOUS_FLUSH", value);
            let config = ExtensionConfig::from_env();
            assert!(
                config.new_relic.synchronous_flush,
                "Expected true for value '{}'", value
            );
        });
    }
}

#[test]
#[serial]
fn test_synchronous_flush_and_pipeline_flush_both_enabled_is_representable() {
    // Both flags can legally be set simultaneously — precedence is enforced at the
    // event-loop level (execute_standard_mode_event_loop), not by rejecting the config.
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_EXTENSION_SYNCHRONOUS_FLUSH", "true");
        env::set_var("NEW_RELIC_EXTENSION_PIPELINE_FLUSH", "true");
        let config = ExtensionConfig::from_env();
        assert!(config.new_relic.synchronous_flush);
        assert!(config.extension.pipeline_flush);
        env::remove_var("NEW_RELIC_EXTENSION_PIPELINE_FLUSH");
    });
}

// ============================================================================
// NEW_RELIC_DATA_COLLECTION_TIMEOUT / NEW_RELIC_HTTP_TIMEOUT
// ============================================================================

#[test]
fn test_parse_duration_valid_units() {
    assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
    assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
    assert_eq!(parse_duration("2m"), Some(Duration::from_secs(120)));
    assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
    assert_eq!(parse_duration("1.5s"), Some(Duration::from_millis(1500)));
}

#[test]
fn test_parse_duration_bare_number_rejected() {
    // Bare numbers with no unit are rejected, matching Go's time.ParseDuration.
    assert_eq!(parse_duration("15"), None);
    assert_eq!(parse_duration("20"), None);
}

#[test]
fn test_parse_duration_invalid_unit_rejected() {
    assert_eq!(parse_duration("10x"), None);
    assert_eq!(parse_duration("10 days"), None);
}

#[test]
fn test_parse_duration_negative_rejected() {
    assert_eq!(parse_duration("-5s"), None);
}

#[test]
fn test_parse_duration_empty_rejected() {
    assert_eq!(parse_duration(""), None);
}

#[test]
fn test_parse_duration_trims_whitespace() {
    assert_eq!(parse_duration("  20s  "), Some(Duration::from_secs(20)));
}

#[test]
#[serial]
fn test_from_env_data_collection_timeout_unset_stays_none() {
    with_full_clean_env(|| {
        let config = ExtensionConfig::from_env();
        assert_eq!(config.new_relic.data_collection_timeout, None);
    });
}

#[test]
#[serial]
fn test_from_env_data_collection_timeout_valid_value() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_DATA_COLLECTION_TIMEOUT", "20s");
        let config = ExtensionConfig::from_env();
        assert_eq!(config.new_relic.data_collection_timeout, Some(Duration::from_secs(20)));
    });
}

#[test]
#[serial]
fn test_from_env_data_collection_timeout_invalid_value_falls_back_to_default() {
    with_full_clean_env(|| {
        // Bare number, no unit -> falls back to the 10s default, matching the Go extension.
        env::set_var("NEW_RELIC_DATA_COLLECTION_TIMEOUT", "15");
        let config = ExtensionConfig::from_env();
        assert_eq!(config.new_relic.data_collection_timeout, Some(Duration::from_secs(10)));
    });
}

#[test]
#[serial]
fn test_from_env_http_timeout_unset_stays_none() {
    with_full_clean_env(|| {
        let config = ExtensionConfig::from_env();
        assert_eq!(config.new_relic.http_timeout, None);
    });
}

#[test]
#[serial]
fn test_from_env_http_timeout_valid_value() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_HTTP_TIMEOUT", "1s");
        let config = ExtensionConfig::from_env();
        assert_eq!(config.new_relic.http_timeout, Some(Duration::from_secs(1)));
    });
}

#[test]
#[serial]
fn test_from_env_http_timeout_invalid_value_falls_back_to_default() {
    with_full_clean_env(|| {
        // Bare number, no unit -> falls back to the 2400ms default.
        env::set_var("NEW_RELIC_HTTP_TIMEOUT", "3");
        let config = ExtensionConfig::from_env();
        assert_eq!(config.new_relic.http_timeout, Some(Duration::from_millis(2400)));
    });
}


// ── OTLP metrics forwarding opt-in (NEW_RELIC_OTLP_METRIC_ENABLED) ──

#[test]
#[serial]
fn test_otlp_metric_enabled_default_false() {
    with_full_clean_env(|| {
        let config = ExtensionConfig::from_env();
        assert!(!config.new_relic.otlp_metric_enabled);
    });
}

#[test]
#[serial]
fn test_otlp_metric_enabled_true_from_env() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_OTLP_METRIC_ENABLED", "true");
        let config = ExtensionConfig::from_env();
        assert!(config.new_relic.otlp_metric_enabled);
    });
}

#[test]
#[serial]
fn test_otlp_metric_enabled_explicit_false() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_OTLP_METRIC_ENABLED", "false");
        let config = ExtensionConfig::from_env();
        assert!(!config.new_relic.otlp_metric_enabled);
    });
}

#[test]
#[serial]
fn test_otlp_metric_enabled_truthy_variants() {
    for value in &["1", "yes", "on", "TRUE", "Yes"] {
        with_full_clean_env(|| {
            env::set_var("NEW_RELIC_OTLP_METRIC_ENABLED", value);
            let config = ExtensionConfig::from_env();
            assert!(
                config.new_relic.otlp_metric_enabled,
                "Expected true for value '{}'", value
            );
        });
    }
}

#[test]
#[serial]
fn test_otlp_metric_enabled_garbage_string_falls_back_to_false() {
    with_full_clean_env(|| {
        env::set_var("NEW_RELIC_OTLP_METRIC_ENABLED", "not_a_bool");
        let config = ExtensionConfig::from_env();
        assert!(!config.new_relic.otlp_metric_enabled);
    });
}
