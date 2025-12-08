//! APM collector connection (PreConnect and Connect)
//!
//! Based on connect.go PreConnect() and Connect()

use anyhow::{Result, anyhow};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::env;
use std::io::Write;
use std::time::Duration;
use tracing::{debug, error, info};
use once_cell::sync::Lazy;
use flate2::write::GzEncoder;
use flate2::Compression;

/// Connection timeouts - MORE AGGRESSIVE than Go for Lambda cold start
const PRECONNECT_TIMEOUT_SECS: u64 = 20;
const CONNECT_TIMEOUT_SECS: u64 = 20;

/// OPTIMIZATION: Cache runtime detection (runs once per container lifetime)
static CACHED_RUNTIME: Lazy<String> = Lazy::new(|| detect_runtime_internal());

/// OPTIMIZATION: Cache agent version detection (runs once per container lifetime)
static CACHED_AGENT_VERSION: Lazy<String> = Lazy::new(|| {
    let runtime = CACHED_RUNTIME.as_str();
    detect_agent_version_internal(runtime)
});

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

    debug!("PreConnect request to: {} (timeout: {}s)", url, PRECONNECT_TIMEOUT_SECS);

    // OPTIMIZATION: 30s timeout for Lambda cold start (network can be slow)
    let response = client
        .post(&url)
        .header("Content-Type", "application/octet-stream")
        .header("Content-Encoding", "gzip")
        .header("User-Agent", "NewRelic-Rust-Lambda-Extension/0.1.0")
        .header("Accept-Encoding", "identity, deflate")
        .body(compressed_body)
        .timeout(Duration::from_secs(PRECONNECT_TIMEOUT_SECS))
        .send()
        .await
        .map_err(|e| {
            error!("PreConnect HTTP request failed: {:?}", e);
            if e.is_timeout() {
                error!("PreConnect TIMEOUT after {}s - Lambda cold start network may be slow", PRECONNECT_TIMEOUT_SECS);
            } else if e.is_connect() {
                error!("PreConnect CONNECTION ERROR - Cannot reach collector at {}", base_host);
            } else if e.is_request() {
                error!("PreConnect REQUEST ERROR - Invalid request format or parameters");
            }
            e
        })?;

    let status = response.status();
    if !status.is_success() {
        let error_body = response.text().await.unwrap_or_else(|_| "Unable to read response body".to_string());
        error!("PreConnect FAILED - HTTP Status: {}, Response Body: {}", status, error_body);
        error!("PreConnect URL was: {}", url);
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
        labels: get_labels(function_arn),
    }];

    let body = serde_json::to_vec(&connect_req)?;

    // OPTIMIZATION: Inline compression for small payloads (Go-style - no spawn_blocking overhead)
    let compressed_body = compress_inline(&body)?;

    debug!("Connect request to: {} (timeout: {}s)", url, CONNECT_TIMEOUT_SECS);

    // OPTIMIZATION: 30s timeout for Lambda cold start
    let response = client
        .post(&url)
        .header("Content-Type", "application/octet-stream")
        .header("Content-Encoding", "gzip")
        .header("User-Agent", "NewRelic-Rust-Lambda-Extension/0.1.0")
        .header("Accept-Encoding", "identity, deflate")
        .body(compressed_body)
        .timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
        .send()
        .await
        .map_err(|e| {
            error!("Connect HTTP request failed: {:?}", e);
            if e.is_timeout() {
                error!("Connect TIMEOUT after {}s", CONNECT_TIMEOUT_SECS);
            } else if e.is_connect() {
                error!("Connect CONNECTION ERROR - Cannot reach collector at {}", collector_host);
            }
            e
        })?;

    let status = response.status();
    if !status.is_success() {
        let error_body = response.text().await.unwrap_or_else(|_| "Unable to read response body".to_string());
        error!("Connect FAILED - HTTP Status: {}, Response Body: {}", status, error_body);
        error!("Connect URL was: {}", url);
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

/// Get cached runtime (detected once per container)
/// OPTIMIZATION: Returns cached value - detection happens only once
pub fn detect_runtime() -> &'static str {
    CACHED_RUNTIME.as_str()
}

/// Detect Lambda runtime from /var/lang/bin (internal, called once)
fn detect_runtime_internal() -> String {
    // OPTIMIZATION: Check most common runtimes first (Node, Python)
    // Using direct path construction without heap allocation
    if std::path::Path::new("/var/lang/bin/node").exists() {
        return "nodejs".to_string(); // BUGFIX: Collector expects "nodejs", not "node"
    }
    if std::path::Path::new("/var/lang/bin/python").exists() {
        return "python".to_string();
    }
    if std::path::Path::new("/var/lang/bin/ruby").exists() {
        return "ruby".to_string();
    }
    if std::path::Path::new("/var/lang/bin/dotnet").exists() {
        return "dotnet".to_string();
    }

    debug!("No specific runtime detected, defaulting to go");
    "go".to_string()
}

/// Get cached agent version (detected once per container)
/// OPTIMIZATION: Returns cached value - detection happens only once
pub fn detect_agent_version(_runtime: &str) -> &'static str {
    CACHED_AGENT_VERSION.as_str()
}

/// Get agent version from layer paths (internal, called once)
fn detect_agent_version_internal(runtime: &str) -> String {
    match runtime {
        "nodejs" => { // BUGFIX: Changed from "node" to match runtime string
            let paths = vec![
                "/opt/nodejs/node_modules/newrelic/package.json",
                "/var/task/node_modules/newrelic/package.json",
            ];

            for path in paths {
                if let Ok(content) = std::fs::read_to_string(path) {
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) {
                        if let Some(version) = json.get("version").and_then(|v| v.as_str()) {
                            debug!("Detected Node.js agent version: {}", version);
                            return version.to_string();
                        }
                    }
                }
            }
        }
        "python" => {
            let paths = vec![
                "/opt/python/newrelic/version.txt",
                "/var/task/newrelic/version.txt",
            ];

            for path in paths {
                if let Ok(content) = std::fs::read_to_string(path) {
                    let version = content.trim().to_string();
                    debug!("Detected Python agent version: {}", version);
                    return version;
                }
            }
        }
        "dotnet" | "ruby" => {
            let paths = vec![
                format!("/opt/{}/newrelic/version.txt", runtime),
                format!("/var/task/newrelic/version.txt"),
            ];

            for path in paths {
                if let Ok(content) = std::fs::read_to_string(&path) {
                    let version = content.trim().to_string();
                    debug!("Detected {} agent version: {}", runtime, version);
                    return version;
                }
            }
        }
        _ => {}
    }

    debug!("Could not detect agent version, using default");
    "unknown".to_string()
}

/// Parse NR_TAGS environment variable into key-value pairs
/// Format: "key1:value1;key2:value2" (delimiter can be customized via NR_ENV_DELIMITER)
fn parse_nr_tags() -> Vec<(String, String)> {
    let nr_tags = match env::var("NR_TAGS") {
        Ok(tags) if !tags.is_empty() => tags,
        _ => return Vec::new(),
    };

    let delimiter = env::var("NR_ENV_DELIMITER").unwrap_or_else(|_| ";".to_string());

    nr_tags
        .split(&delimiter)
        .filter_map(|tag| {
            let parts: Vec<&str> = tag.split(':').collect();
            if parts.len() == 2 {
                Some((parts[0].to_string(), parts[1].to_string()))
            } else {
                None
            }
        })
        .collect()
}

/// Get labels for Connect request, including aws.arn, isLambdaFunction, and NR_TAGS
fn get_labels(function_arn: &str) -> Vec<Label> {
    let mut labels = vec![
        Label {
            label_type: "aws.arn".to_string(),
            label_value: function_arn.to_string(),
        },
        Label {
            label_type: "isLambdaFunction".to_string(),
            label_value: "true".to_string(),
        },
    ];

    for (key, value) in parse_nr_tags() {
        debug!("Added custom label from NR_TAGS: {}={}", key, value);
        labels.push(Label {
            label_type: key,
            label_value: value,
        });
    }

    labels
}
