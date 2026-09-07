// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use serial_test::serial;

#[test]
fn retryable_statuses_are_transient_only() {
    for code in [408, 429, 500, 502, 503, 504] {
        assert!(is_retryable_status(code), "{code} should be retryable");
    }
    // Permanent / success / restart / disconnect must NOT be classified retryable.
    for code in [200, 202, 400, 401, 403, 404, 409, 410, 413] {
        assert!(!is_retryable_status(code), "{code} must not be retryable");
    }
}

#[test]
fn metric_api_error_classification() {
    let retr = MetricApiError::Retryable {
        status: 503,
        retry_after: Some(std::time::Duration::from_secs(7)),
    };
    assert!(!retr.is_permanent());
    assert_eq!(retr.retry_after(), Some(std::time::Duration::from_secs(7)));

    let perm = MetricApiError::Permanent { status: 400 };
    assert!(perm.is_permanent());
    assert_eq!(perm.retry_after(), None);

    let net = MetricApiError::Network(anyhow::anyhow!("boom"));
    assert!(!net.is_permanent());
    assert_eq!(net.retry_after(), None);
}

#[test]
#[serial]
fn reconnect_flag_is_one_shot() {
    // Drain any pre-existing state.
    let _ = take_reconnect_needed();
    assert!(!take_reconnect_needed(), "should start clear");
    signal_reconnect_needed();
    assert!(take_reconnect_needed(), "first take observes the signal");
    assert!(!take_reconnect_needed(), "second take is cleared");
}

#[test]
#[serial]
fn disabled_telemetry_roundtrips() {
    let mut set = std::collections::HashSet::new();
    set.insert("platform_metrics".to_string());
    set.insert("sql_trace_data".to_string());
    set_disabled_telemetry(set);
    assert!(is_telemetry_disabled("platform_metrics"));
    assert!(is_telemetry_disabled("sql_trace_data"));
    assert!(!is_telemetry_disabled("metric_data"));
    // Reset so other serial tests see a clean state.
    set_disabled_telemetry(std::collections::HashSet::new());
    assert!(!is_telemetry_disabled("platform_metrics"));
}

#[test]
fn known_telemetry_types_complete() {
    // The 9 agent-payload types + platform_metrics.
    assert_eq!(KNOWN_TELEMETRY_TYPES.len(), 10);
    assert!(KNOWN_TELEMETRY_TYPES.contains(&"platform_metrics"));
    assert!(KNOWN_TELEMETRY_TYPES.contains(&"sql_trace_data"));
}

fn restart_log_level(status_code: u16) -> &'static str {
    if status_code == 409 { "INFO" } else { "WARN" }
}

#[test]
fn log_level_409_is_info_401_is_warn() {
    assert_eq!(restart_log_level(409), "INFO", "409 (routine session refresh) must log at INFO");
    assert_eq!(restart_log_level(401), "WARN", "401 (auth failure) must log at WARN");
}

#[test]
fn disconnect_is_not_restart_exception() {
    // 410 returns CollectorError::Disconnect, not RestartException. This is
    // intentional: telemetry_buffer::retry_buffered_telemetry only skips
    // retry_count for RestartException (409/401). A 410 is a hard disconnect
    // and must consume a retry slot like any other non-session error.
    let restart = anyhow::Error::new(CollectorError::RestartException);
    let disconnect = anyhow::Error::new(CollectorError::Disconnect);

    let is_restart = |e: &anyhow::Error| {
        e.downcast_ref::<CollectorError>()
            .map(|ce| matches!(ce, CollectorError::RestartException))
            .unwrap_or(false)
    };

    assert!(is_restart(&restart), "RestartException (409/401) must be detected");
    assert!(!is_restart(&disconnect), "Disconnect (410) must NOT be treated as restart");
}

// ---------------------------------------------------------------------------
// OTLP metrics forwarding gate
// ---------------------------------------------------------------------------

#[test]
#[serial]
fn otlp_metric_enabled_defaults_to_false_and_roundtrips() {
    // Process-wide static: don't assume a fresh false here (another test may have
    // run first), but do confirm the flag actually flips both ways.
    set_otlp_metric_enabled(false);
    assert!(!is_otlp_metric_enabled());
    set_otlp_metric_enabled(true);
    assert!(is_otlp_metric_enabled());
    // Reset so other serial tests see a clean state.
    set_otlp_metric_enabled(false);
    assert!(!is_otlp_metric_enabled());
}

/// Pins the full truth table for the effective OTLP gate, exercising the REAL
/// production function (`ExtensionConfig::otlp_metric_forwarding_active`) that main.rs
/// feeds into `set_otlp_metric_enabled` — not a re-implementation of the `&&`. Guards
/// against regressing to mirroring the env var alone, which would leave the flag "on"
/// in serverless mode where the OTLP send path does not exist.
#[test]
#[serial]
fn otlp_metric_enabled_requires_both_env_var_and_apm_mode() {
    for (env_var, apm_mode, expected) in [
        (true, true, true),    // both on -> OTLP sends
        (true, false, false),  // flag set but serverless -> no send path exists
        (false, true, false),  // APM mode but flag off -> opted out
        (false, false, false), // neither
    ] {
        let mut config = crate::config::ExtensionConfig::default();
        config.new_relic.otlp_metric_enabled = env_var;
        config.new_relic.apm_lambda_mode = apm_mode;

        assert_eq!(
            config.otlp_metric_forwarding_active(),
            expected,
            "otlp_metric_forwarding_active: env_var={env_var}, apm_mode={apm_mode}"
        );

        // And confirm it round-trips through the process-wide gate main.rs sets.
        set_otlp_metric_enabled(config.otlp_metric_forwarding_active());
        assert_eq!(
            is_otlp_metric_enabled(),
            expected,
            "is_otlp_metric_enabled: env_var={env_var}, apm_mode={apm_mode}"
        );
    }

    // Reset so other serial tests see a clean state.
    set_otlp_metric_enabled(false);
    assert!(!is_otlp_metric_enabled());
}
