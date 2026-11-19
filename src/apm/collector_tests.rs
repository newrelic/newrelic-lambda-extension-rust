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
    let client = crate::newrelic::tls_fallback::test_client();
    let result = send_error_events(&client, "key", "127.0.0.1:1", "run-1", &[], "req-1").await;
    assert!(result.is_ok());
}

#[tokio::test]
#[serial]
async fn send_error_events_connection_refused_returns_error() {
    let _ = take_reconnect_needed();
    let client = crate::newrelic::tls_fallback::test_client();
    let events = vec![serde_json::json!({"type": "TransactionError"})];
    let result = send_error_events(&client, "key", "127.0.0.1:1", "run-1", &events, "req-1").await;
    assert!(result.is_err());
    // A network error must NOT trigger a reconnect signal.
    assert!(!take_reconnect_needed());
}

// ── send_apm_telemetry ────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn send_apm_telemetry_connection_refused_returns_error() {
    let _ = take_reconnect_needed();
    let client = crate::newrelic::tls_fallback::test_client();
    let data = vec![serde_json::json!(null), serde_json::json!({})];
    let result = send_apm_telemetry(&client, "key", "127.0.0.1:1", "run-1", CMD_METRICS, &data, "req-1").await;
    assert!(result.is_err());
    assert!(!take_reconnect_needed());
}

// ── send_platform_metrics (metric_endpoint is a parameter → wiremock works) ──

#[tokio::test]
async fn send_platform_metrics_empty_is_noop() {
    // No HTTP call — returns Ok(()) immediately without touching port 1.
    let client = crate::newrelic::tls_fallback::test_client();
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

    let client = crate::newrelic::tls_fallback::test_client();
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

    let client = crate::newrelic::tls_fallback::test_client();
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

    let client = crate::newrelic::tls_fallback::test_client();
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

    let client = crate::newrelic::tls_fallback::test_client();
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

    let client = crate::newrelic::tls_fallback::test_client();
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

    let client = crate::newrelic::tls_fallback::test_client();
    let metrics = vec![serde_json::json!({"name": "m", "value": 1.0})];
    let err = send_platform_metrics(&client, "test-key", &server.uri(), &metrics).await.unwrap_err();

    assert!(err.is_permanent());
}

#[tokio::test]
async fn send_platform_metrics_network_error_is_retryable() {
    // Port 1 → ECONNREFUSED → MetricApiError::Network.
    let client = crate::newrelic::tls_fallback::test_client();
    let metrics = vec![serde_json::json!({"name": "m", "value": 1.0})];
    let err = send_platform_metrics(&client, "test-key", "http://127.0.0.1:1/metrics", &metrics)
        .await
        .unwrap_err();

    assert!(!err.is_permanent());
    assert!(matches!(err, MetricApiError::Network(_)));
}

// ── splitting metric data points into Metric API requests (1 MB limit) ────────────────────

#[test]
fn split_by_payload_size_with_nothing_is_empty() {
    assert!(split_by_payload_size(&[], 100).is_empty());
}

#[test]
fn split_by_payload_size_keeps_small_items_together() {
    assert_eq!(split_by_payload_size(&[10, 20, 30], 1_000), vec![0..3]);
}

#[test]
fn split_by_payload_size_starts_a_new_request_when_the_next_item_would_not_fit() {
    // Overhead per request is reserved, so use sizes well clear of the boundary.
    let max = 1_000;
    let groups = split_by_payload_size(&[400, 400, 400, 400, 400], max);
    assert_eq!(groups, vec![0..2, 2..4, 4..5]);
}

#[test]
fn split_by_payload_size_puts_an_oversized_item_alone() {
    let groups = split_by_payload_size(&[100, 5_000, 100], 1_000);
    assert_eq!(groups, vec![0..1, 1..2, 2..3]);
}

#[test]
fn split_by_payload_size_covers_every_item_in_order() {
    let sizes: Vec<usize> = (1..=57).map(|i| i * 13).collect();
    let groups = split_by_payload_size(&sizes, 900);
    let mut next = 0;
    for g in &groups {
        assert_eq!(g.start, next, "no gap and no overlap");
        assert!(g.end > g.start);
        next = g.end;
    }
    assert_eq!(next, sizes.len());
}

// ── Metric API body: attributes shared by every data point move to `common` ───────────────

fn metric(name: &str, ts: i64, attrs: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"name": name, "type": "gauge", "value": 1.5, "timestamp": ts, "attributes": attrs})
}

fn attrs_of(m: &serde_json::Value) -> serde_json::Value {
    m.get("attributes").cloned().unwrap_or(serde_json::Value::Null)
}

#[test]
fn shared_attributes_move_to_common_but_per_request_ones_stay_on_each_metric() {
    let shared = |rid: &str| serde_json::json!({"entity.guid": "g1", "aws.lambda.arn": "arn:x", "aws.requestId": rid});
    let metrics = vec![
        metric("duration", 100, shared("req-1")),
        metric("billed", 100, shared("req-1")),
        metric("duration", 200, shared("req-2")),
        metric("billed", 200, shared("req-2")),
    ];

    let payload = build_metric_payload(&metrics);

    let map = &payload[0];
    assert_eq!(map["common"]["attributes"], serde_json::json!({"entity.guid": "g1", "aws.lambda.arn": "arn:x"}));
    let out = map["metrics"].as_array().unwrap();
    assert_eq!(attrs_of(&out[0]), serde_json::json!({"aws.requestId": "req-1"}));
    assert_eq!(attrs_of(&out[1]), serde_json::json!({"aws.requestId": "req-1"}));
    assert_eq!(attrs_of(&out[2]), serde_json::json!({"aws.requestId": "req-2"}), "a different request keeps its own id");
    assert_eq!(attrs_of(&out[3]), serde_json::json!({"aws.requestId": "req-2"}));
}

#[test]
fn expanding_common_gives_back_exactly_the_original_metrics() {
    let metrics = vec![
        metric("a", 1, serde_json::json!({"k": "same", "only_here": true, "n": 7})),
        metric("b", 2, serde_json::json!({"k": "same", "n": 8})),
        metric("c", 3, serde_json::json!({"k": "same", "n": 7, "extra": "x"})),
    ];
    let payload = build_metric_payload(&metrics);
    assert_eq!(expand_common(&payload), metrics, "nothing added, nothing lost, same order");
}

#[test]
fn a_single_request_hoists_everything_it_shares() {
    let attrs = serde_json::json!({"entity.guid": "g", "aws.requestId": "only"});
    let metrics = vec![metric("a", 5, attrs.clone()), metric("b", 5, attrs.clone()), metric("c", 5, attrs)];
    let payload = build_metric_payload(&metrics);
    assert_eq!(payload[0]["common"]["attributes"]["aws.requestId"], "only");
    assert!(payload[0]["metrics"][0].get("attributes").is_none(), "nothing left to repeat");
    assert_eq!(expand_common(&payload), metrics);
}

#[test]
fn an_attribute_missing_from_one_metric_is_not_hoisted() {
    let metrics = vec![
        metric("a", 1, serde_json::json!({"k": "v", "both": 1})),
        metric("b", 1, serde_json::json!({"k": "v"})),
    ];
    let payload = build_metric_payload(&metrics);
    assert_eq!(payload[0]["common"]["attributes"], serde_json::json!({"k": "v"}));
    assert_eq!(attrs_of(&payload[0]["metrics"][0]), serde_json::json!({"both": 1}));
    assert_eq!(expand_common(&payload), metrics);
}

#[test]
fn the_same_key_with_a_different_value_or_type_is_not_hoisted() {
    let metrics = vec![
        metric("a", 1, serde_json::json!({"mem": 128, "flag": true})),
        metric("b", 1, serde_json::json!({"mem": "128", "flag": true})),
    ];
    let payload = build_metric_payload(&metrics);
    assert_eq!(payload[0]["common"]["attributes"], serde_json::json!({"flag": true}), "128 and \"128\" differ");
    assert_eq!(expand_common(&payload), metrics);
}

#[test]
fn one_metric_or_metrics_without_attributes_get_no_common_block() {
    let one = vec![metric("a", 1, serde_json::json!({"k": "v"}))];
    assert!(build_metric_payload(&one)[0].get("common").is_none(), "nothing to share with one metric");
    assert_eq!(expand_common(&build_metric_payload(&one)), one);

    let bare = vec![serde_json::json!({"name": "a", "value": 1}), serde_json::json!({"name": "b", "value": 2})];
    let payload = build_metric_payload(&bare);
    assert!(payload[0].get("common").is_none());
    assert_eq!(payload[0]["metrics"], serde_json::json!(bare));
}

#[test]
fn only_attributes_are_hoisted_never_names_values_or_timestamps() {
    let metrics = vec![
        metric("a", 111, serde_json::json!({"k": "v"})),
        metric("b", 222, serde_json::json!({"k": "v"})),
    ];
    let out = build_metric_payload(&metrics);
    let out = out[0]["metrics"].as_array().unwrap();
    assert_eq!((out[0]["name"].as_str(), out[0]["timestamp"].as_i64()), (Some("a"), Some(111)));
    assert_eq!((out[1]["name"].as_str(), out[1]["timestamp"].as_i64()), (Some("b"), Some(222)));
    assert!(out.iter().all(|m| m["type"] == "gauge" && m["value"] == 1.5));
}

#[tokio::test]
async fn send_platform_metrics_posts_a_gzip_body_with_the_common_block() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(202)).mount(&server).await;
    let client = crate::newrelic::tls_fallback::test_client();
    let attrs = |rid: &str| serde_json::json!({"entity.guid": "g", "aws.requestId": rid});
    let metrics = vec![metric("a", 1, attrs("r1")), metric("a", 2, attrs("r2"))];

    send_platform_metrics(&client, "test-key", &server.uri(), &metrics).await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.headers.get("content-encoding").map(|v| v.to_str().unwrap()), Some("gzip"));
    assert_eq!(request.headers.get("content-type").map(|v| v.to_str().unwrap()), Some("application/json"));
    assert_eq!(request.headers.get("api-key").map(|v| v.to_str().unwrap()), Some("test-key"));
    let payload = decode_metric_request(&request.body);
    assert_eq!(payload[0]["common"]["attributes"], serde_json::json!({"entity.guid": "g"}));
    assert_eq!(expand_common(&payload), metrics, "the receiver sees exactly the metrics we were given");
}

#[test]
fn gzip_shrinks_a_repetitive_metric_body() {
    let attrs = serde_json::json!({"entity.guid": "g", "aws.lambda.arn": "arn:aws:lambda:ap-south-1:123456789012:function:f"});
    let metrics: Vec<_> = (0..40).map(|i| metric("apm.lambda.transaction.duration", i, attrs.clone())).collect();
    let raw = serde_json::to_vec(&serde_json::json!([{"metrics": metrics}])).unwrap();
    let sent = gzip_bytes(&serde_json::to_vec(&build_metric_payload(&metrics)).unwrap()).unwrap();
    assert!(sent.len() * 5 < raw.len(), "{} bytes sent for {} raw", sent.len(), raw.len());
}
