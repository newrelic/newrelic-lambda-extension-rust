// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for `runtime/mod.rs`
//!
//! Covers:
//! - `ShutdownReason`: all four variants via `as_str()` and `Display`
//! - `fetch_next_event`: missing env-var, INVOKE parse, SHUTDOWN parse, retry loop

use serial_test::serial;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{fetch_next_event, LambdaRuntimeEvent, ShutdownReason};

const RUNTIME_API_ENV: &str = "AWS_LAMBDA_RUNTIME_API";
const EXT_ID_HEADER: &str = "Lambda-Extension-Identifier";
const TEST_EXT_ID: &str = "test-mod-ext-id";

/// Set `AWS_LAMBDA_RUNTIME_API` to `server`'s address for the duration of `f`,
/// then restore the previous value (or remove the var if it was unset).
async fn with_runtime_api<F, Fut, T>(server: &MockServer, f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let prev = std::env::var(RUNTIME_API_ENV).ok();
    std::env::set_var(RUNTIME_API_ENV, server.address().to_string());
    let out = f().await;
    match prev {
        Some(v) => std::env::set_var(RUNTIME_API_ENV, v),
        None => std::env::remove_var(RUNTIME_API_ENV),
    }
    out
}

// ── ShutdownReason ───────────────────────────────────────────────────────────

#[test]
fn shutdown_reason_as_str_all_variants() {
    assert_eq!(ShutdownReason::Spindown.as_str(), "spindown");
    assert_eq!(ShutdownReason::Timeout.as_str(), "timeout");
    assert_eq!(ShutdownReason::Failure.as_str(), "failure");
    assert_eq!(ShutdownReason::Unknown.as_str(), "unknown");
}

#[test]
fn shutdown_reason_display_matches_as_str() {
    for variant in [
        ShutdownReason::Spindown,
        ShutdownReason::Timeout,
        ShutdownReason::Failure,
        ShutdownReason::Unknown,
    ] {
        assert_eq!(format!("{variant}"), variant.as_str());
    }
}

// ── fetch_next_event ─────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn fetch_next_event_returns_error_when_runtime_api_missing() {
    let prev = std::env::var(RUNTIME_API_ENV).ok();
    std::env::remove_var(RUNTIME_API_ENV);

    let client = reqwest::Client::new();
    let result = fetch_next_event(&client, TEST_EXT_ID).await;

    match prev {
        Some(v) => std::env::set_var(RUNTIME_API_ENV, v),
        None => std::env::remove_var(RUNTIME_API_ENV),
    }

    assert!(result.is_err());
}

#[tokio::test]
#[serial]
async fn fetch_next_event_parses_invoke_response() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/2020-01-01/extension/event/next"))
        .and(header(EXT_ID_HEADER, TEST_EXT_ID))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "eventType": "INVOKE",
            "requestId": "req-mod-01",
            "invokedFunctionArn": "arn:aws:lambda:us-east-1:123:function:fn",
            "deadlineMs": 9_999_999_999_i64
        })))
        .expect(1)
        .mount(&server)
        .await;

    let result = with_runtime_api(&server, || async {
        let client = reqwest::Client::new();
        fetch_next_event(&client, TEST_EXT_ID).await
    })
    .await;

    match result.expect("should parse INVOKE") {
        LambdaRuntimeEvent::Invoke { request_id, .. } => {
            assert_eq!(request_id, "req-mod-01");
        }
        other => panic!("expected Invoke, got {other:?}"),
    }
}

#[tokio::test]
#[serial]
async fn fetch_next_event_parses_shutdown_with_timeout_reason() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/2020-01-01/extension/event/next"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "eventType": "SHUTDOWN",
            "shutdownReason": "timeout"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let result = with_runtime_api(&server, || async {
        let client = reqwest::Client::new();
        fetch_next_event(&client, TEST_EXT_ID).await
    })
    .await;

    match result.expect("should parse SHUTDOWN(timeout)") {
        LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
            assert_eq!(shutdown_reason, ShutdownReason::Timeout);
        }
        other => panic!("expected Shutdown, got {other:?}"),
    }
}

#[tokio::test]
#[serial]
async fn fetch_next_event_retries_connection_errors_then_fails() {
    // Port 1 always refuses connections; MAX_RETRIES=3 means 3 attempts
    // with 200 ms + 400 ms sleep between them (~600 ms total).
    let prev = std::env::var(RUNTIME_API_ENV).ok();
    std::env::set_var(RUNTIME_API_ENV, "127.0.0.1:1");

    let client = reqwest::Client::new();
    let result = fetch_next_event(&client, TEST_EXT_ID).await;

    match prev {
        Some(v) => std::env::set_var(RUNTIME_API_ENV, v),
        None => std::env::remove_var(RUNTIME_API_ENV),
    }

    assert!(result.is_err(), "should fail after exhausting retries");
}

// The connection-refused tests above cover `e.is_connect()` — this test covers the
// sibling `e.is_timeout()` branch at runtime/mod.rs:292 by using a wiremock server
// that delays its response beyond the client's total request timeout.
#[tokio::test]
#[serial]
async fn fetch_next_event_timeout_branch_is_retried_then_fails() {
    let server = MockServer::start().await;

    // Each request is accepted but the response is held for 500 ms — far longer than
    // the 50 ms client timeout — so every attempt returns a reqwest timeout error.
    Mock::given(method("GET"))
        .and(path("/2020-01-01/extension/event/next"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(std::time::Duration::from_millis(500))
                .set_body_json(serde_json::json!({
                    "eventType": "INVOKE",
                    "requestId": "req-timeout-test",
                    "invokedFunctionArn": "arn:test",
                    "deadlineMs": 9_999_999_999_i64
                })),
        )
        .mount(&server)
        .await;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(50))
        .build()
        .unwrap();

    let result = with_runtime_api(&server, || async {
        fetch_next_event(&client, TEST_EXT_ID).await
    })
    .await;

    assert!(result.is_err(), "must fail after exhausting retries on timeout");
}
