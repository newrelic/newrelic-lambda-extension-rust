//! APM collector connection (PreConnect and Connect)
//!
//! Based on connect.go PreConnect() and Connect()

use anyhow::{Result, anyhow};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::time::Duration;
use tracing::{debug, error, info};
use flate2::write::GzEncoder;
use flate2::Compression;

/// Connection timeouts - MORE AGGRESSIVE than Go for Lambda cold start
const PRECONNECT_TIMEOUT_SECS: u64 = 20;
const CONNECT_TIMEOUT_SECS: u64 = 20;
const EXTENSION_VERSION: &str = env!("CARGO_PKG_VERSION");

fn get_user_agent() -> String {
    format!("NewRelic-Rust-Lambda-Extension/{EXTENSION_VERSION}")
}

/// OPTIMIZATION: Inline compression (no spawn_blocking overhead)
fn compress_inline(data: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(data)?;
    encoder.finish().map_err(|e| anyhow!("Compression failed: {}", e))
}

/// PreConnect request payload
#[derive(Debug, Serialize)]
pub struct PreconnectRequest {
    pub security_policies_token: String,
    pub high_security: bool,
}

/// PreConnect response
#[derive(Debug, Deserialize)]
pub struct PreconnectResponse {
    pub return_value: PreconnectReturnValue,
}

#[derive(Debug, Deserialize)]
pub struct PreconnectReturnValue {
    pub redirect_host: String,
}

/// Connect request payload
#[derive(Debug, Serialize)]
pub struct ConnectRequest {
    pub pid: u32,
    pub language: String,
    pub agent_version: String,
    pub host: String,
    pub display_host: String,
    pub app_name: Vec<String>,
    pub identifier: String,
    pub utilization: Utilization,
    pub labels: Vec<Label>,
}

#[derive(Debug, Serialize)]
pub struct Utilization {
    pub vendors: Vendors,
}

#[derive(Debug, Serialize)]
pub struct Vendors {
    #[serde(rename = "awslambda")]
    pub aws_lambda: AwsLambdaInfo,
}

#[derive(Debug, Serialize)]
pub struct AwsLambdaInfo {
    #[serde(rename = "aws.arn")]
    pub arn: String,
    #[serde(rename = "aws.region")]
    pub region: String,
    #[serde(rename = "aws.accountId")]
    pub account_id: String,
    #[serde(rename = "aws.functionName")]
    pub function_name: String,
}

#[derive(Debug, Serialize)]
pub struct Label {
    pub label_type: String,
    pub label_value: String,
}

/// Connect response
#[derive(Debug, Deserialize)]
pub struct ConnectResponse {
    pub return_value: ConnectReturnValue,
}

#[derive(Debug, Deserialize)]
pub struct ConnectReturnValue {
    pub agent_run_id: String,
    pub entity_guid: Option<String>,
}

/// Execute PreConnect to get regional collector host
pub async fn preconnect(
    client: &Client,
    license_key: &str,
    base_host: &str,
) -> Result<String> {
    let url = format!(
        "https://{base_host}/agent_listener/invoke_raw_method?marshal_format=json&protocol_version=17&method=preconnect&license_key={license_key}"
    );

    let preconnect_req = vec![PreconnectRequest {
        security_policies_token: String::new(),
        high_security: false,
    }];

    let body = serde_json::to_vec(&preconnect_req)?;

    // OPTIMIZATION: Inline compression for small payloads (Go-style - no spawn_blocking overhead)
    let compressed_body = compress_inline(&body)?;

    debug!("PreConnect request to collector (timeout: {}s)", PRECONNECT_TIMEOUT_SECS);

    // OPTIMIZATION: 30s timeout for Lambda cold start (network can be slow)
    let response = client
        .post(&url)
        .header("Content-Type", "application/octet-stream")
        .header("Content-Encoding", "gzip")
        .header("User-Agent", get_user_agent())
        .header("Accept-Encoding", "identity, deflate")
        .body(compressed_body)
        .timeout(Duration::from_secs(PRECONNECT_TIMEOUT_SECS))
        .send()
        .await
        .map_err(|e| {
            if e.is_timeout() {
                error!("PreConnect TIMEOUT after {}s - Lambda cold start network may be slow", PRECONNECT_TIMEOUT_SECS);
            } else if e.is_connect() {
                error!("PreConnect CONNECTION ERROR - Cannot reach collector at {}", base_host);
            } else if e.is_request() {
                error!("PreConnect REQUEST ERROR - Invalid request format or parameters");
            } else {
                error!("PreConnect HTTP request failed: {}", e);
            }
            e
        })?;

    let status = response.status();
    if !status.is_success() {
        let error_body = response.text().await.unwrap_or_else(|_| "Unable to read response body".to_string());
        error!("PreConnect FAILED - HTTP Status: {}, Response Body: {}", status, error_body);
        error!("This usually means: 1) Invalid license key, 2) Network connectivity issue, 3) Collector endpoint unreachable");
        return Err(anyhow!("PreConnect failed with HTTP {} - {}", status, error_body));
    }

    let preconnect_resp: PreconnectResponse = response.json().await?;
    let redirect_host = preconnect_resp.return_value.redirect_host;

    info!("PreConnect successful, redirect host: {}", redirect_host);
    Ok(redirect_host)
}

/// Execute Connect to get Run ID and Entity GUID
pub async fn connect(
    client: &Client,
    license_key: &str,
    collector_host: &str,
    function_name: &str,
    function_arn: &str,
    account_id: &str,
    region: &str,
    _function_version: &str,
    runtime: &str,
    agent_version: &str,
) -> Result<ConnectResponse> {
    let url = format!(
        "https://{collector_host}/agent_listener/invoke_raw_method?marshal_format=json&protocol_version=17&method=connect&license_key={license_key}"
    );

    let connect_req = vec![ConnectRequest {
        pid: std::process::id(),
        language: runtime.to_string(),
        agent_version: agent_version.to_string(),
        host: function_arn.to_string(),
        display_host: function_name.to_string(),
        app_name: vec![function_name.to_string()],
        identifier: function_name.to_string(),
        utilization: Utilization {
            vendors: Vendors {
                aws_lambda: AwsLambdaInfo {
                    arn: function_arn.to_string(),
                    region: region.to_string(),
                    account_id: account_id.to_string(),
                    function_name: function_name.to_string(),
                },
            },
        },
        labels: get_labels(function_arn, runtime),
    }];

    let body = serde_json::to_vec(&connect_req)?;

    // OPTIMIZATION: Inline compression for small payloads (Go-style - no spawn_blocking overhead)
    let compressed_body = compress_inline(&body)?;

    debug!("Connect request to collector (timeout: {}s)", CONNECT_TIMEOUT_SECS);

    // OPTIMIZATION: 30s timeout for Lambda cold start
    let response = client
        .post(&url)
        .header("Content-Type", "application/octet-stream")
        .header("Content-Encoding", "gzip")
        .header("User-Agent", get_user_agent())
        .header("Accept-Encoding", "identity, deflate")
        .body(compressed_body)
        .timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
        .send()
        .await
        .map_err(|e| {
            if e.is_timeout() {
                error!("Connect TIMEOUT after {}s", CONNECT_TIMEOUT_SECS);
            } else if e.is_connect() {
                error!("Connect CONNECTION ERROR - Cannot reach collector at {}", collector_host);
            } else {
                error!("Connect HTTP request failed: {}", e);
            }
            e
        })?;

    let status = response.status();
    if !status.is_success() {
        let error_body = response.text().await.unwrap_or_else(|_| "Unable to read response body".to_string());
        error!("Connect FAILED - HTTP Status: {}, Response Body: {}", status, error_body);
        return Err(anyhow!("Connect failed with HTTP {} - {}", status, error_body));
    }

    let connect_resp: ConnectResponse = response.json().await?;

    info!(
        "Connect successful, Run ID: {}, Entity GUID: {:?}",
        connect_resp.return_value.agent_run_id,
        connect_resp.return_value.entity_guid
    );

    Ok(connect_resp)
}

// Note: parse_nr_tags() is now defined in config::mod for shared use

/// Get labels for Connect request
fn get_labels(function_arn: &str, runtime: &str) -> Vec<Label> {
    let runtime_version = crate::version::get_runtime_version();
    let extension_version = env!("CARGO_PKG_VERSION");
    
    let mut labels = vec![
        Label {
            label_type: "aws.arn".to_string(),
            label_value: function_arn.to_string(),
        },
        Label {
            label_type: "isLambdaFunction".to_string(),
            label_value: "true".to_string(),
        },
        Label {
            label_type: "newrelic.extension.version".to_string(),
            label_value: extension_version.to_string(),
        },
    ];

    // Only send runtime version if we have actual version info
    if runtime != "unknown" 
        && !runtime_version.contains("unknown") 
        && runtime_version != runtime  
        && runtime_version.len() > runtime.len()
    {
        labels.push(Label {
            label_type: "lambda.runtime.version".to_string(),
            label_value: runtime_version,
        });
    }

    for (key, value) in crate::config::parse_nr_tags() {
        labels.push(Label {
            label_type: key,
            label_value: value,
        });
    }

    labels
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::io::Read;

    // ========================================================================
    // compress_inline
    // ========================================================================

    #[test]
    fn test_compress_inline_roundtrip() {
        let original = b"Hello, this is test data for compression roundtrip!";
        let compressed = compress_inline(original).expect("compression should succeed");

        let mut decoder = GzDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).expect("decompression should succeed");

        assert_eq!(&decompressed, original);
    }

    #[test]
    fn test_compress_inline_empty_input() {
        let compressed = compress_inline(b"").expect("compression of empty should succeed");

        let mut decoder = GzDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).expect("decompression should succeed");

        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_compress_inline_produces_smaller_output_for_large_input() {
        let large_data = vec![b'A'; 10000];
        let compressed = compress_inline(&large_data).expect("compression should succeed");
        assert!(compressed.len() < large_data.len());
    }

    // ========================================================================
    // get_labels
    // ========================================================================

    #[test]
    fn test_get_labels_mandatory_labels_present() {
        let labels = get_labels("arn:aws:lambda:us-east-1:123:function:test", "nodejs");

        let label_types: Vec<&str> = labels.iter().map(|l| l.label_type.as_str()).collect();
        assert!(label_types.contains(&"aws.arn"));
        assert!(label_types.contains(&"isLambdaFunction"));
        assert!(label_types.contains(&"newrelic.extension.version"));
    }

    #[test]
    fn test_get_labels_extension_version_matches_cargo_pkg() {
        let labels = get_labels("arn:test", "python");
        let ext_label = labels.iter()
            .find(|l| l.label_type == "newrelic.extension.version")
            .expect("should have extension version label");
        assert_eq!(ext_label.label_value, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn test_get_labels_is_lambda_function_true() {
        let labels = get_labels("arn:test", "ruby");
        let lambda_label = labels.iter()
            .find(|l| l.label_type == "isLambdaFunction")
            .expect("should have isLambdaFunction label");
        assert_eq!(lambda_label.label_value, "true");
    }

    // ========================================================================
    // get_user_agent
    // ========================================================================

    #[test]
    fn test_get_user_agent_format() {
        let ua = get_user_agent();
        assert!(ua.starts_with("NewRelic-Rust-Lambda-Extension/"));
        assert!(ua.contains(env!("CARGO_PKG_VERSION")));
    }

    // ========================================================================
    // Serialization
    // ========================================================================

    #[test]
    fn test_preconnect_request_serialization() {
        let req = PreconnectRequest {
            security_policies_token: String::new(),
            high_security: false,
        };
        let json = serde_json::to_string(&req).expect("serialization should succeed");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");

        assert_eq!(parsed["security_policies_token"], "");
        assert_eq!(parsed["high_security"], false);
    }

    #[test]
    fn test_connect_request_serialization() {
        let req = ConnectRequest {
            pid: 12345,
            language: "ruby".to_string(),
            agent_version: "9.5.0".to_string(),
            host: "arn:test".to_string(),
            display_host: "my-function".to_string(),
            app_name: vec!["my-function".to_string()],
            identifier: "my-function".to_string(),
            utilization: Utilization {
                vendors: Vendors {
                    aws_lambda: AwsLambdaInfo {
                        arn: "arn:test".to_string(),
                        region: "us-east-1".to_string(),
                        account_id: "123456".to_string(),
                        function_name: "my-function".to_string(),
                    },
                },
            },
            labels: vec![],
        };
        let json = serde_json::to_string(&req).expect("serialization should succeed");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");

        assert_eq!(parsed["pid"], 12345);
        assert_eq!(parsed["language"], "ruby");
        assert_eq!(parsed["utilization"]["vendors"]["awslambda"]["aws.arn"], "arn:test");
        assert_eq!(parsed["utilization"]["vendors"]["awslambda"]["aws.region"], "us-east-1");
        assert_eq!(parsed["utilization"]["vendors"]["awslambda"]["aws.accountId"], "123456");
        assert_eq!(parsed["utilization"]["vendors"]["awslambda"]["aws.functionName"], "my-function");
    }

    // ========================================================================
    // Deserialization
    // ========================================================================

    #[test]
    fn test_preconnect_response_deserialization() {
        let json = r#"{"return_value": {"redirect_host": "collector-123.newrelic.com"}}"#;
        let resp: PreconnectResponse = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(resp.return_value.redirect_host, "collector-123.newrelic.com");
    }

    #[test]
    fn test_connect_response_deserialization() {
        let json = r#"{"return_value": {"agent_run_id": "run-123", "entity_guid": "ABCDEF123"}}"#;
        let resp: ConnectResponse = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(resp.return_value.agent_run_id, "run-123");
        assert_eq!(resp.return_value.entity_guid, Some("ABCDEF123".to_string()));
    }

    #[test]
    fn test_connect_response_missing_entity_guid() {
        let json = r#"{"return_value": {"agent_run_id": "run-456", "entity_guid": null}}"#;
        let resp: ConnectResponse = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(resp.return_value.agent_run_id, "run-456");
        assert_eq!(resp.return_value.entity_guid, None);
    }
}
