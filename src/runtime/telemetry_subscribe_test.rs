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
async fn subscribe_retries_500_then_gives_up_without_falling_back() {
    // 500 is transient, so it is retried on the SAME schema up to MAX_ATTEMPTS
    // (3) — never falling back to 2022, which would mask a transient AWS error
    // as "fallback worked" and silently downgrade the schema.
    let server = MockServer::start().await;

    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2025-01-29"
        })))
        .respond_with(ResponseTemplate::new(500))
        .expect(3) // initial attempt + 2 retries, all on the 2025 schema
        .mount(&server)
        .await;

    // The 2022 fallback body must NEVER be sent on 500.
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
        other => panic!("expected Rejected(500, V2025_01_29) after retries, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Retry path: a transient 503 on the preferred schema is retried on the SAME
// schema and succeeds — no fallback, no startup abort.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn subscribe_retries_on_503_then_succeeds_same_schema() {
    let server = MockServer::start().await;

    // First two attempts: 503 (transient). Higher priority + capped so it is
    // consumed first, then yields to the success mock.
    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2025-01-29"
        })))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(2)
        .with_priority(1)
        .expect(2)
        .mount(&server)
        .await;

    // Third attempt: 200 on the SAME (preferred) schema.
    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2025-01-29"
        })))
        .respond_with(ResponseTemplate::new(200))
        .with_priority(2)
        .expect(1)
        .mount(&server)
        .await;

    // Fallback must never fire — the preferred schema ultimately succeeded.
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

    assert_eq!(
        result.expect("transient 503 should be retried, not fatal"),
        TelemetrySchema::V2025_01_29
    );
}

// ---------------------------------------------------------------------------
// Retry path interacts correctly with the fallback: a 503 on the preferred
// schema is retried (not a fallback signal); a 400 IS the fallback signal and
// is NOT retried. Here the preferred schema 503s persistently → give up,
// surface the 5xx, never fall back. (Companion to the 400/404 fallback tests.)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn subscribe_does_not_retry_400_but_falls_back_immediately() {
    let server = MockServer::start().await;

    // 400 is terminal for retry (it is the fallback signal): exactly ONE 2025
    // attempt, then fall back.
    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2025-01-29"
        })))
        .respond_with(ResponseTemplate::new(400).set_body_string("schema not supported"))
        .expect(1) // NOT retried — proves 400 is terminal for the retry layer
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
        result.expect("400 should fall back, not retry"),
        TelemetrySchema::V2022_07_01
    );
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
// Retry + fallback compose: the fallback (2022) schema carries its OWN retry
// budget. A 400 on 2025 falls back (no retry), then a transient 503 on 2022 is
// retried on 2022 and succeeds. Worst case is 1 (V2025) + up to 3 (V2022) calls.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn subscribe_falls_back_then_retries_2022_on_503() {
    let server = MockServer::start().await;

    // V2025: 400 → terminal for retry, triggers exactly one fallback.
    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2025-01-29"
        })))
        .respond_with(ResponseTemplate::new(400).set_body_string("schema not supported"))
        .expect(1)
        .mount(&server)
        .await;

    // V2022: first attempt 503 (transient), then 200 — proving the fallback
    // schema retries on its own budget rather than giving up.
    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2022-07-01"
        })))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .with_priority(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path(TELEMETRY_PATH))
        .and(body_partial_json(serde_json::json!({
            "schemaVersion": "2022-07-01"
        })))
        .respond_with(ResponseTemplate::new(200))
        .with_priority(2)
        .expect(1)
        .mount(&server)
        .await;

    let result = subscribe(&server).await;

    assert_eq!(
        result.expect("fallback schema should retry a transient 503 and succeed"),
        TelemetrySchema::V2022_07_01
    );
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
