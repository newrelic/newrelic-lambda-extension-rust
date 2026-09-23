// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use serial_test::serial;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

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

#[test]
fn collector_error_display_messages() {
    assert_eq!(CollectorError::Disconnect.to_string(), "Collector disconnected (410)");
    assert_eq!(CollectorError::RestartException.to_string(), "Collector restart exception (401/409)");
}

#[test]
fn metric_api_error_display_messages() {
    let retr = MetricApiError::Retryable { status: 503, retry_after: None };
    assert_eq!(retr.to_string(), "Metric API transient error (status 503)");

    let perm = MetricApiError::Permanent { status: 400 };
    assert_eq!(perm.to_string(), "Metric API permanent error (status 400)");

    let net = MetricApiError::Network(anyhow::anyhow!("connection reset"));
    assert_eq!(net.to_string(), "Metric API network error: connection reset");
}

#[test]
fn parse_retry_after_parses_valid_seconds_header() {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(reqwest::header::RETRY_AFTER, "30".parse().unwrap());
    assert_eq!(parse_retry_after(&headers), Some(std::time::Duration::from_secs(30)));
}

#[test]
fn parse_retry_after_ignores_http_date_form() {
    // New Relic only ever emits delta-seconds; the HTTP-date form must be
    // ignored (parsed as u64 fails) rather than panicking or guessing.
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(reqwest::header::RETRY_AFTER, "Wed, 21 Oct 2026 07:28:00 GMT".parse().unwrap());
    assert_eq!(parse_retry_after(&headers), None);
}

#[test]
fn parse_retry_after_missing_header_returns_none() {
    let headers = reqwest::header::HeaderMap::new();
    assert_eq!(parse_retry_after(&headers), None);
}

// ── send_error_events ─────────────────────────────────────────────────────────

#[tokio::test]
async fn send_error_events_empty_slice_is_noop() {
    // No HTTP call is made — function returns Ok(()) immediately.
    let client = reqwest::Client::new();
    let result = send_error_events(&client, "key", "127.0.0.1:1", "run-1", &[]).await;
    assert!(result.is_ok());
}

#[tokio::test]
#[serial]
async fn send_error_events_connection_refused_returns_error() {
    let _ = take_reconnect_needed();
    let client = reqwest::Client::new();
    let events = vec![serde_json::json!({"type": "TransactionError"})];
    let result = send_error_events(&client, "key", "127.0.0.1:1", "run-1", &events).await;
    assert!(result.is_err());
    // A network error must NOT trigger a reconnect signal.
    assert!(!take_reconnect_needed());
}

// ── send_apm_telemetry ────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn send_apm_telemetry_connection_refused_returns_error() {
    let _ = take_reconnect_needed();
    let client = reqwest::Client::new();
    let data = vec![serde_json::json!(null), serde_json::json!({})];
    let result = send_apm_telemetry(&client, "key", "127.0.0.1:1", "run-1", CMD_METRICS, &data).await;
    assert!(result.is_err());
    assert!(!take_reconnect_needed());
}

// ── send_platform_metrics (metric_endpoint is a parameter → wiremock works) ──

#[tokio::test]
async fn send_platform_metrics_empty_is_noop() {
    // No HTTP call — returns Ok(()) immediately without touching port 1.
    let client = reqwest::Client::new();
    let result = send_platform_metrics(&client, "key", "http://127.0.0.1:1/metrics", &[]).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn send_platform_metrics_success_202() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let metrics = vec![serde_json::json!({"name": "apm.lambda.duration", "value": 1.0})];
    let result = send_platform_metrics(&client, "test-key", &server.uri(), &metrics).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn send_platform_metrics_200_is_success() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let metrics = vec![serde_json::json!({"name": "apm.lambda.billed_duration", "value": 100.0})];
    let result = send_platform_metrics(&client, "test-key", &server.uri(), &metrics).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn send_platform_metrics_503_is_retryable() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503).set_body_string("Service Unavailable"))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let metrics = vec![serde_json::json!({"name": "m", "value": 1.0})];
    let err = send_platform_metrics(&client, "test-key", &server.uri(), &metrics).await.unwrap_err();

    assert!(!err.is_permanent());
    assert!(matches!(err, MetricApiError::Retryable { status: 503, .. }));
}

#[tokio::test]
async fn send_platform_metrics_429_is_retryable_with_retry_after() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("Retry-After", "60"),
        )
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let metrics = vec![serde_json::json!({"name": "m", "value": 1.0})];
    let err = send_platform_metrics(&client, "test-key", &server.uri(), &metrics).await.unwrap_err();

    assert!(!err.is_permanent());
    assert_eq!(err.retry_after(), Some(std::time::Duration::from_secs(60)));
}

#[tokio::test]
async fn send_platform_metrics_400_is_permanent() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_string("Bad Request"))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let metrics = vec![serde_json::json!({"name": "m", "value": 1.0})];
    let err = send_platform_metrics(&client, "test-key", &server.uri(), &metrics).await.unwrap_err();

    assert!(err.is_permanent());
    assert!(matches!(err, MetricApiError::Permanent { status: 400 }));
}

#[tokio::test]
async fn send_platform_metrics_403_is_permanent() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let metrics = vec![serde_json::json!({"name": "m", "value": 1.0})];
    let err = send_platform_metrics(&client, "test-key", &server.uri(), &metrics).await.unwrap_err();

    assert!(err.is_permanent());
}

#[tokio::test]
async fn send_platform_metrics_network_error_is_retryable() {
    // Port 1 → ECONNREFUSED → MetricApiError::Network.
    let client = reqwest::Client::new();
    let metrics = vec![serde_json::json!({"name": "m", "value": 1.0})];
    let err = send_platform_metrics(&client, "test-key", "http://127.0.0.1:1/metrics", &metrics)
        .await
        .unwrap_err();

    assert!(!err.is_permanent());
    assert!(matches!(err, MetricApiError::Network(_)));
}
