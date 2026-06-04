// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for `subscribe_to_telemetry` covering the schema
//! negotiation: try the `2025-01-29` body schema first, fall back to
//! `2022-07-01` on `HTTP 400` or `404`, and surface every other error
//! verbatim.
//!
//! Both attempts PUT to the **fixed** Telemetry API endpoint version
//! ([`TELEMETRY_API_VERSION`] = `2022-07-01`) — only the body `schemaVersion`
//! differs between them. The mocks therefore key off the request *body*, not
//! the URL path. (Routing the schema version into the URL path is exactly the
//! bug that produced the production `404 page not found`; see
//! `subscribe_always_targets_fixed_api_path` for the regression guard.)
//!
//! The wiremock harness mirrors the registration tests
//! ([registration_test.rs](super::registration_test)) — same `with_runtime_api`
//! helper pattern, same `serial_test` discipline because we mutate
//! `AWS_LAMBDA_RUNTIME_API` at the process level.

use super::{
    subscribe_to_telemetry, TelemetrySchema, TelemetrySubscriptionError, TELEMETRY_API_VERSION,
};
use serial_test::serial;
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const RUNTIME_API_ENV: &str = "AWS_LAMBDA_RUNTIME_API";
const EXT_ID_HEADER: &str = "Lambda-Extension-Identifier";
const TEST_EXT_ID: &str = "test-ext-id";
const LISTENER_PORT: u16 = 4242;

/// The single, fixed path every subscription attempt must hit.
const TELEMETRY_PATH: &str = "/2022-07-01/telemetry";

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

/// Run `subscribe_to_telemetry` against `server` with the runtime API env set.
async fn subscribe(server: &MockServer) -> Result<TelemetrySchema, TelemetrySubscriptionError> {
    with_runtime_api(server, || async {
        let client = reqwest::Client::new();
        subscribe_to_telemetry(&client, TEST_EXT_ID, LISTENER_PORT).await
    })
    .await
}

// ---------------------------------------------------------------------------
// Happy path: AWS accepts the preferred body schema on the first try.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn subscribe_succeeds_on_2025_first_try() {
    let server = MockServer::start().await;

    // Exactly one PUT to the fixed path with the 2025 body schema.
    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(header(EXT_ID_HEADER, TEST_EXT_ID))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2025-01-29"
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    // The 2022 fallback body must never be sent on a first-try success.
    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2022-07-01"
        })))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;

    let result = subscribe(&server).await;

    assert_eq!(
        result.expect("subscription should succeed"),
        TelemetrySchema::V2025_01_29
    );
}

// ---------------------------------------------------------------------------
// Regression: the schema version must NEVER appear in the URL path. This is
// the bug that took down LMI in production — the runtime API returned
// `404 page not found` for `/2025-01-29/telemetry`.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn subscribe_always_targets_fixed_api_path() {
    let server = MockServer::start().await;

    // The schema-versioned path is the regression: if the code ever routes the
    // schema into the URL path again, this mock fires (expect 0 fails it).
    Mock::given(method("PUT"))
        .and(path("/2025-01-29/telemetry"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let result = subscribe(&server).await;

    assert_eq!(
        result.expect("subscription should succeed on the fixed path"),
        TelemetrySchema::V2025_01_29
    );
    // Belt and suspenders: the const the code uses is the fixed API version.
    assert_eq!(TELEMETRY_API_VERSION, "2022-07-01");
}

// ---------------------------------------------------------------------------
// Fallback path: 400 on the 2025 body → retry once with the 2022 body.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn subscribe_falls_back_to_2022_on_400() {
    let server = MockServer::start().await;

    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2025-01-29"
        })))
        .respond_with(
            ResponseTemplate::new(400).set_body_string("Schema 2025-01-29 not yet supported"),
        )
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2022-07-01"
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let result = subscribe(&server).await;

    assert_eq!(
        result.expect("fallback subscription should succeed"),
        TelemetrySchema::V2022_07_01
    );
}

// ---------------------------------------------------------------------------
// Fallback path: 404 on the 2025 body → retry once with the 2022 body. This
// mirrors the real LMI symptom (runtime that does not serve the newer schema
// answered with a 404) and must degrade gracefully rather than go no-op.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn subscribe_falls_back_to_2022_on_404() {
    let server = MockServer::start().await;

    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2025-01-29"
        })))
        .respond_with(ResponseTemplate::new(404).set_body_string("404 page not found"))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2022-07-01"
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let result = subscribe(&server).await;

    assert_eq!(
        result.expect("404 on preferred schema should fall back, not fail"),
        TelemetrySchema::V2022_07_01
    );
}

// ---------------------------------------------------------------------------
// Negative paths: non-400/404 errors must NOT trigger fallback.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn subscribe_does_not_fall_back_on_500() {
    let server = MockServer::start().await;

    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2025-01-29"
        })))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;

    // The 2022 fallback body must NOT be sent on 500 — that would mask
    // transient AWS errors as "fallback worked" and silently downgrade.
    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2022-07-01"
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let result = subscribe(&server).await;

    match result {
        Err(TelemetrySubscriptionError::Rejected { status, schema, .. }) => {
            assert_eq!(status, 500);
            assert_eq!(schema, TelemetrySchema::V2025_01_29);
        }
        other => panic!("expected Rejected(500, V2025_01_29), got {other:?}"),
    }
}

#[tokio::test]
#[serial]
async fn subscribe_does_not_fall_back_on_403() {
    let server = MockServer::start().await;

    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2025-01-29"
        })))
        .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2022-07-01"
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let result = subscribe(&server).await;

    assert!(
        matches!(
            result,
            Err(TelemetrySubscriptionError::Rejected {
                status: 403,
                schema: TelemetrySchema::V2025_01_29,
                ..
            })
        ),
        "403 must propagate, never trigger fallback"
    );
}

#[tokio::test]
#[serial]
async fn subscribe_does_not_fall_back_when_2022_also_400() {
    // If both schemas reject with 400, the second error must surface — we
    // must not loop indefinitely, and we must report the 2022 status.
    let server = MockServer::start().await;

    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2025-01-29"
        })))
        .respond_with(ResponseTemplate::new(400))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2022-07-01"
        })))
        .respond_with(ResponseTemplate::new(400).set_body_string("legacy schema also rejected"))
        .expect(1)
        .mount(&server)
        .await;

    let result = subscribe(&server).await;

    match result {
        Err(TelemetrySubscriptionError::Rejected {
            status, schema, body, ..
        }) => {
            assert_eq!(status, 400);
            assert_eq!(schema, TelemetrySchema::V2022_07_01);
            assert!(body.contains("legacy schema also rejected"));
        }
        other => panic!("expected Rejected(400, V2022_07_01) on second failure, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Payload shape: assert what AWS sees on the wire matches AWS docs.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn subscribe_payload_includes_required_fields_2025() {
    let server = MockServer::start().await;

    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2025-01-29",
            "types": ["platform", "function", "extension"],
            "destination": {
                "protocol": "HTTP",
                "URI": "http://sandbox:4242/telemetry"
            }
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let result = subscribe(&server).await;

    assert!(result.is_ok(), "payload shape mismatch: {result:?}");
}

#[tokio::test]
#[serial]
async fn subscribe_payload_includes_required_fields_2022_after_fallback() {
    let server = MockServer::start().await;

    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2025-01-29"
        })))
        .respond_with(ResponseTemplate::new(400))
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2022-07-01",
            "types": ["platform", "function", "extension"],
            "destination": {
                "protocol": "HTTP",
                "URI": "http://sandbox:4242/telemetry"
            }
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let result = subscribe(&server).await;

    assert_eq!(
        result.expect("fallback should succeed"),
        TelemetrySchema::V2022_07_01
    );
}

// ---------------------------------------------------------------------------
// Environment errors.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn subscribe_returns_typed_error_when_runtime_api_unset() {
    let prev = std::env::var(RUNTIME_API_ENV).ok();
    std::env::remove_var(RUNTIME_API_ENV);

    let client = reqwest::Client::new();
    let result = subscribe_to_telemetry(&client, TEST_EXT_ID, LISTENER_PORT).await;

    if let Some(v) = prev {
        std::env::set_var(RUNTIME_API_ENV, v);
    }

    assert!(matches!(
        result,
        Err(TelemetrySubscriptionError::MissingRuntimeApi)
    ));
}
