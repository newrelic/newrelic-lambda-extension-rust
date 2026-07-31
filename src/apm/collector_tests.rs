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
