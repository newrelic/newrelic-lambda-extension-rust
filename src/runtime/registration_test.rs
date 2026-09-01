// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::{
    register_extension, schema_for, ManagedSchema, RegistrationError, RegistrationSchema,
    StandardSchema,
};
use crate::config::deployment::{DeploymentContext, TelemetryMode};
use serial_test::serial;
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const RUNTIME_API_ENV: &str = "AWS_LAMBDA_RUNTIME_API";

/// Helper: point the extension at a wiremock server for the duration of `f`,
/// then restore the previous env var. Tests using this MUST be `#[serial]`.
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

fn ok_response() -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("Lambda-Extension-Identifier", "test-ext-id-abc123")
        .set_body_json(serde_json::json!({
            "functionName": "my-fn",
            "functionVersion": "$LATEST",
            "accountId": "123456789012"
        }))
}

#[test]
fn standard_schema_subscribes_to_invoke_and_shutdown() {
    let s = StandardSchema;
    assert_eq!(s.events(), &["INVOKE", "SHUTDOWN"]);
    assert_eq!(s.name(), "standard");
}

#[test]
fn managed_schema_subscribes_to_shutdown_only() {
    // Per AWS docs: "Extensions for Lambda Managed Instances functions can
    // only register for the SHUTDOWN event. Attempting to register for the
    // INVOKE event will result in an error."
    let s = ManagedSchema;
    assert_eq!(s.events(), &["SHUTDOWN"]);
    assert_eq!(s.name(), "managed");
    assert!(!s.events().contains(&"INVOKE"));
}

#[test]
fn dispatcher_normal_serverless_picks_standard() {
    let ctx = DeploymentContext::Normal {
        mode: TelemetryMode::Serverless,
    };
    assert_eq!(schema_for(ctx).name(), "standard");
}

#[test]
fn dispatcher_normal_apm_picks_standard() {
    let ctx = DeploymentContext::Normal {
        mode: TelemetryMode::Apm,
    };
    assert_eq!(schema_for(ctx).name(), "standard");
}

#[test]
fn dispatcher_lmi_picks_managed() {
    assert_eq!(schema_for(DeploymentContext::Lmi).name(), "managed");
}

#[test]
fn registration_error_display_is_actionable() {
    let cases = [
        (RegistrationError::MissingRuntimeApi, "AWS_LAMBDA_RUNTIME_API"),
        (
            RegistrationError::MissingExtensionId,
            "Lambda-Extension-Identifier",
        ),
        (
            RegistrationError::Rejected {
                status: 403,
                body: "forbidden".to_string(),
            },
            "status=403",
        ),
    ];
    for (err, expected_substring) in cases {
        let msg = format!("{err}");
        assert!(
            msg.contains(expected_substring),
            "error message {msg:?} should contain {expected_substring:?}"
        );
    }
}

#[test]
fn registration_error_rejected_includes_body() {
    let err = RegistrationError::Rejected {
        status: 400,
        body: "Invalid event INVOKE for Lambda Managed Instances".to_string(),
    };
    let msg = format!("{err}");
    assert!(msg.contains("Invalid event INVOKE"));
    assert!(msg.contains("status=400"));
}

#[test]
fn payload_serializes_events_array_correctly() {
    // Asserts the exact JSON shape AWS expects, for both schemas.
    let standard_payload = serde_json::json!({ "events": StandardSchema.events() });
    assert_eq!(
        standard_payload.to_string(),
        r#"{"events":["INVOKE","SHUTDOWN"]}"#
    );

    let managed_payload = serde_json::json!({ "events": ManagedSchema.events() });
    assert_eq!(
        managed_payload.to_string(),
        r#"{"events":["SHUTDOWN"]}"#
    );
}

// ---------------------------------------------------------------------------
// Wiremock integration tests: assert the actual HTTP request shape AWS sees.
// These prove that what we send on the wire matches the AWS-documented schema
// for each deployment context — not just that our local payload functions
// return the right value.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn standard_schema_sends_invoke_and_shutdown_on_the_wire() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/2020-01-01/extension/register"))
        .and(header("Lambda-Extension-Name", "test-ext"))
        .and(header("Lambda-Extension-Accept-Feature", "accountId"))
        .and(body_partial_json(serde_json::json!({
            "events": ["INVOKE", "SHUTDOWN"]
        })))
        .respond_with(ok_response())
        .expect(1) // one and only one matching request
        .mount(&server)
        .await;

    let result = with_runtime_api(&server, || async {
        let client = reqwest::Client::new();
        register_extension(&client, "test-ext", &StandardSchema).await
    })
    .await;

    let (resp, ext_id) = result.expect("registration should succeed");
    assert_eq!(ext_id, "test-ext-id-abc123");
    assert_eq!(resp.function_name, "my-fn");
    assert_eq!(resp.function_version, "$LATEST");
    assert_eq!(resp.account_id.as_deref(), Some("123456789012"));
}

#[tokio::test]
#[serial]
async fn managed_schema_sends_shutdown_only_on_the_wire() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/2020-01-01/extension/register"))
        .and(header("Lambda-Extension-Name", "test-ext"))
        .and(body_partial_json(serde_json::json!({
            "events": ["SHUTDOWN"]
        })))
        .respond_with(ok_response())
        .expect(1)
        .mount(&server)
        .await;

    let result = with_runtime_api(&server, || async {
        let client = reqwest::Client::new();
        register_extension(&client, "test-ext", &ManagedSchema).await
    })
    .await;

    assert!(
        result.is_ok(),
        "managed registration should succeed: {result:?}"
    );
}

#[tokio::test]
#[serial]
async fn dispatcher_drives_the_correct_payload_end_to_end() {
    // Full path: detect-style ctx → schema_for → register_extension → wire body.
    // We inject the LMI context directly (detect() reads env vars and we don't
    // want to mutate AWS_LAMBDA_INITIALIZATION_TYPE inside an async test).
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/2020-01-01/extension/register"))
        .and(body_partial_json(serde_json::json!({
            "events": ["SHUTDOWN"]
        })))
        .respond_with(ok_response())
        .expect(1)
        .mount(&server)
        .await;

    let ctx = DeploymentContext::Lmi;
    let schema = schema_for(ctx);
    assert_eq!(schema.name(), "managed");

    let result = with_runtime_api(&server, || async {
        let client = reqwest::Client::new();
        register_extension(&client, "test-ext", schema).await
    })
    .await;

    assert!(result.is_ok());
}

#[tokio::test]
#[serial]
async fn registration_returns_typed_error_on_4xx() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/2020-01-01/extension/register"))
        .respond_with(ResponseTemplate::new(400).set_body_string(
            "Invalid event INVOKE for Lambda Managed Instances",
        ))
        .mount(&server)
        .await;

    let result = with_runtime_api(&server, || async {
        let client = reqwest::Client::new();
        register_extension(&client, "test-ext", &StandardSchema).await
    })
    .await;

    match result {
        Err(RegistrationError::Rejected { status, body }) => {
            assert_eq!(status, 400);
            assert!(body.contains("Invalid event INVOKE"));
        }
        other => panic!("expected Rejected, got {other:?}"),
    }
}

#[tokio::test]
#[serial]
async fn registration_returns_typed_error_when_extension_id_header_missing() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/2020-01-01/extension/register"))
        .respond_with(
            ResponseTemplate::new(200)
                // intentionally NO Lambda-Extension-Identifier header
                .set_body_json(serde_json::json!({
                    "functionName": "f",
                    "functionVersion": "1"
                })),
        )
        .mount(&server)
        .await;

    let result = with_runtime_api(&server, || async {
        let client = reqwest::Client::new();
        register_extension(&client, "test-ext", &StandardSchema).await
    })
    .await;

    assert!(matches!(result, Err(RegistrationError::MissingExtensionId)));
}

#[tokio::test]
#[serial]
async fn registration_returns_typed_error_when_runtime_api_unset() {
    let prev = std::env::var(RUNTIME_API_ENV).ok();
    std::env::remove_var(RUNTIME_API_ENV);

    let client = reqwest::Client::new();
    let result = register_extension(&client, "test-ext", &StandardSchema).await;

    if let Some(v) = prev {
        std::env::set_var(RUNTIME_API_ENV, v);
    }

    assert!(matches!(result, Err(RegistrationError::MissingRuntimeApi)));
}

// ---------------------------------------------------------------------------
// Retry behavior: registration is a once-per-environment cold-start call, so a
// transient failure is retried with backoff rather than aborting startup.
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn registration_retries_on_503_then_succeeds() {
    let server = MockServer::start().await;

    // First two attempts: 503 (transient). Capped + higher priority so it is
    // consumed first, then yields to the success mock for the third attempt.
    Mock::given(method("POST"))
        .and(path("/2020-01-01/extension/register"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(2)
        .with_priority(1)
        .expect(2)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/2020-01-01/extension/register"))
        .respond_with(ok_response())
        .with_priority(2)
        .expect(1)
        .mount(&server)
        .await;

    let result = with_runtime_api(&server, || async {
        let client = reqwest::Client::new();
        register_extension(&client, "test-ext", &StandardSchema).await
    })
    .await;

    let (_, ext_id) = result.expect("transient 503 should be retried, not fatal");
    assert_eq!(ext_id, "test-ext-id-abc123");
}

#[tokio::test]
#[serial]
async fn registration_gives_up_after_max_attempts_on_persistent_503() {
    let server = MockServer::start().await;

    // Persistent 503: exactly MAX_ATTEMPTS (3) requests, then surface the error.
    Mock::given(method("POST"))
        .and(path("/2020-01-01/extension/register"))
        .respond_with(ResponseTemplate::new(503).set_body_string("service unavailable"))
        .expect(3)
        .mount(&server)
        .await;

    let result = with_runtime_api(&server, || async {
        let client = reqwest::Client::new();
        register_extension(&client, "test-ext", &StandardSchema).await
    })
    .await;

    match result {
        Err(RegistrationError::Rejected { status, .. }) => assert_eq!(status, 503),
        other => panic!("expected Rejected(503) after retries, got {other:?}"),
    }
}

#[tokio::test]
#[serial]
async fn registration_does_not_retry_on_4xx() {
    let server = MockServer::start().await;

    // 4xx is a terminal client error — it must NOT be retried (exactly 1 call).
    Mock::given(method("POST"))
        .and(path("/2020-01-01/extension/register"))
        .respond_with(
            ResponseTemplate::new(400).set_body_string("Invalid event INVOKE for Managed Instances"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let result = with_runtime_api(&server, || async {
        let client = reqwest::Client::new();
        register_extension(&client, "test-ext", &StandardSchema).await
    })
    .await;

    assert!(
        matches!(result, Err(RegistrationError::Rejected { status: 400, .. })),
        "4xx must be terminal, got {result:?}"
    );
}

#[test]
fn registration_error_retry_classification() {
    // 5xx / 429 are transient → retryable.
    for status in [500, 502, 503, 504, 429] {
        assert!(
            RegistrationError::Rejected {
                status,
                body: String::new()
            }
            .is_retryable(),
            "status {status} should be retryable"
        );
    }
    // Other 4xx and the typed env/header errors are terminal.
    for status in [400, 401, 403, 404, 409] {
        assert!(
            !RegistrationError::Rejected {
                status,
                body: String::new()
            }
            .is_retryable(),
            "status {status} must be terminal"
        );
    }
    assert!(!RegistrationError::MissingRuntimeApi.is_retryable());
    assert!(!RegistrationError::MissingExtensionId.is_retryable());
}

#[tokio::test]
async fn registration_transport_error_is_retryable() {
    // A real connection-refused error from reqwest must classify as retryable.
    let client = reqwest::Client::new();
    let err = client
        .get("http://127.0.0.1:1/")
        .send()
        .await
        .expect_err("connection to a closed port must fail");
    assert!(RegistrationError::Transport(err).is_retryable());
}

#[tokio::test]
#[serial]
async fn account_id_is_optional_in_response() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/2020-01-01/extension/register"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Lambda-Extension-Identifier", "id-1")
                .set_body_json(serde_json::json!({
                    "functionName": "f",
                    "functionVersion": "1"
                    // no accountId
                })),
        )
        .mount(&server)
        .await;

    let result = with_runtime_api(&server, || async {
        let client = reqwest::Client::new();
        register_extension(&client, "test-ext", &StandardSchema).await
    })
    .await;

    let (resp, _) = result.expect("registration should succeed without accountId");
    assert!(resp.account_id.is_none());
}
