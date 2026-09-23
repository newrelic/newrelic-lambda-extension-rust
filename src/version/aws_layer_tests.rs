// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use serial_test::serial;

#[test]
fn test_parse_layer_arn() {
    let arn = "arn:aws:lambda:us-east-1:123456789012:layer:NewRelicPython313X86:93";
    let result = parse_layer_arn(arn);
    assert_eq!(result, Some("NewRelicPython313X86:93".to_string()));
}

#[test]
fn test_parse_invalid_arn() {
    let arn = "invalid-arn";
    let result = parse_layer_arn(arn);
    assert_eq!(result, None);
}

// =============================================================================
// fetch_layer_info_from_aws — wiremock intercepts Lambda GetFunctionConfiguration.
// aws_config::load_defaults is called fresh per invocation (no OnceLock), so
// each test is fully independent.
// =============================================================================

fn set_aws_env(endpoint: &str) {
    std::env::set_var("AWS_ENDPOINT_URL", endpoint);
    std::env::set_var("AWS_ACCESS_KEY_ID", "test-key");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test-secret");
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
    // Provide a CA bundle so the TLS trust store can initialize without
    // relying on native OS roots (which can fail in test environments).
    for path in &["/etc/ssl/cert.pem", "/etc/ssl/certs/ca-certificates.crt", "/etc/pki/tls/certs/ca-bundle.crt"] {
        if std::path::Path::new(path).exists() {
            std::env::set_var("AWS_CA_BUNDLE", path);
            break;
        }
    }
}

fn clear_aws_env() {
    std::env::remove_var("AWS_ENDPOINT_URL");
    std::env::remove_var("AWS_ACCESS_KEY_ID");
    std::env::remove_var("AWS_SECRET_ACCESS_KEY");
    std::env::remove_var("AWS_DEFAULT_REGION");
    std::env::remove_var("AWS_CA_BUNDLE");
}

/// NewRelic layer present — should return parsed "name:version".
/// Covers: Ok arm, loop, arn.contains("newrelic") true, parse_layer_arn Some,
/// return Some(layer_info) (lines 30-47).
#[tokio::test]
#[serial]
async fn fetch_layer_info_returns_newrelic_layer() {
    use wiremock::{Mock, MockServer, ResponseTemplate};
    use wiremock::matchers::method;

    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/json")
                .set_body_string(
                    r#"{"FunctionName":"my-fn","Layers":[{"Arn":"arn:aws:lambda:us-east-1:123456789012:layer:NewRelicPython313X86:93","CodeSize":28973637}]}"#,
                ),
        )
        .mount(&mock)
        .await;

    set_aws_env(&mock.uri());
    let result = fetch_layer_info_from_aws("my-fn".to_string()).await;
    clear_aws_env();

    assert_eq!(result, Some("NewRelicPython313X86:93".to_string()));
}

/// Non-NewRelic layer — loop finds no NewRelic match, falls back to first layer.
/// Covers: arn.contains("newrelic") false, fallback block (lines 52-58).
#[tokio::test]
#[serial]
async fn fetch_layer_info_fallback_to_first_layer() {
    use wiremock::{Mock, MockServer, ResponseTemplate};
    use wiremock::matchers::method;

    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/json")
                .set_body_string(
                    r#"{"FunctionName":"my-fn","Layers":[{"Arn":"arn:aws:lambda:us-east-1:123456789012:layer:SomeOtherLayer:1","CodeSize":1234}]}"#,
                ),
        )
        .mount(&mock)
        .await;

    set_aws_env(&mock.uri());
    let result = fetch_layer_info_from_aws("my-fn".to_string()).await;
    clear_aws_env();

    assert_eq!(result, Some("SomeOtherLayer:1".to_string()));
}

/// No layers in the response — should return None.
/// Covers: layers.is_empty() true branch (lines 62-64).
#[tokio::test]
#[serial]
async fn fetch_layer_info_no_layers_returns_none() {
    use wiremock::{Mock, MockServer, ResponseTemplate};
    use wiremock::matchers::method;

    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/json")
                .set_body_string(r#"{"FunctionName":"my-fn","Layers":[]}"#),
        )
        .mount(&mock)
        .await;

    set_aws_env(&mock.uri());
    let result = fetch_layer_info_from_aws("my-fn".to_string()).await;
    clear_aws_env();

    assert_eq!(result, None);
}

/// Lambda API returns 403 — should warn and return None.
/// Covers: Err arm, warn! line, returns None (lines 66-72).
#[tokio::test]
#[serial]
async fn fetch_layer_info_api_error_returns_none() {
    use wiremock::{Mock, MockServer, ResponseTemplate};
    use wiremock::matchers::method;

    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(403)
                .insert_header("Content-Type", "application/json")
                .set_body_string(
                    r#"{"__type":"AccessDeniedException","message":"not authorized to get function"}"#,
                ),
        )
        .mount(&mock)
        .await;

    set_aws_env(&mock.uri());
    let result = fetch_layer_info_from_aws("my-fn".to_string()).await;
    clear_aws_env();

    assert_eq!(result, None);
}
