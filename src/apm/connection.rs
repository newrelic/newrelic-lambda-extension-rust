// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! APM collector connection (PreConnect and Connect)
//!
//! Based on connect.go PreConnect() and Connect()

use anyhow::{Result, anyhow};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tracing::{debug, info};
use flate2::write::GzEncoder;
use flate2::Compression;

use crate::newrelic::client::redact_url;

/// Permanent APM handshake rejection (HTTP 401/403): the license key is invalid
/// or the account lacks permission. Retrying cannot fix this, so the caller
/// latches APM off for the life of the container instead of looping every invoke.
#[derive(Debug)]
pub struct PermanentAuthError {
    pub status: u16,
}

impl std::fmt::Display for PermanentAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "APM handshake rejected (HTTP {}): invalid license key or insufficient permissions",
            self.status
        )
    }
}

impl std::error::Error for PermanentAuthError {}

/// Return the HTTP status if a [`PermanentAuthError`] appears anywhere in the
/// error chain (errors are wrapped with `.context(...)` by callers).
pub fn is_permanent_auth_error(err: &anyhow::Error) -> Option<u16> {
    err.chain()
        .find_map(|cause| cause.downcast_ref::<PermanentAuthError>().map(|e| e.status))
}

/// Latched once when the handshake hits a permanent auth failure (401/403).
/// While set, the extension stops attempting APM handshakes for this container.
static HANDSHAKE_FATAL: AtomicBool = AtomicBool::new(false);

/// Mark the APM handshake as permanently failed (invalid credentials).
pub fn signal_handshake_fatal() {
    HANDSHAKE_FATAL.store(true, Ordering::Relaxed);
}

/// Whether APM handshakes have been permanently disabled for this container.
pub fn is_handshake_fatal() -> bool {
    HANDSHAKE_FATAL.load(Ordering::Relaxed)
}

#[cfg(test)]
pub fn reset_handshake_fatal_for_test() {
    HANDSHAKE_FATAL.store(false, Ordering::Relaxed);
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
    timeout_secs: u64,
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

    // Log only the redacted endpoint (the URL query carries the license key).
    debug!("PreConnect request to {} (timeout: {}s)", redact_url(&url), timeout_secs);

    let response = client
        .post(&url)
        .header("Content-Type", "application/octet-stream")
        .header("Content-Encoding", "gzip")
        .header("User-Agent", "NewRelic-Rust-Lambda-Extension/0.1.0")
        .header("Accept-Encoding", "identity, deflate")
        .body(compressed_body)
        .timeout(Duration::from_secs(timeout_secs))
        .send()
        .await
        // Strip the request URL from the error BEFORE it is logged or propagated:
        // reqwest's error Display embeds the URL, which contains `license_key`.
        // These are transient and retried by the caller, so log at debug, not error.
        .map_err(|e| {
            let sanitized = e.without_url();
            if sanitized.is_timeout() {
                debug!("PreConnect timeout after {}s - sandbox may have been frozen during handshake", timeout_secs);
            } else if sanitized.is_connect() {
                debug!("PreConnect connection error - cannot reach collector at {}", base_host);
            } else {
                debug!("PreConnect request failed: {}", sanitized);
            }
            sanitized
        })?;

    let status = response.status();
    if !status.is_success() {
        let code = status.as_u16();
        let error_body = response.text().await.unwrap_or_else(|_| "Unable to read response body".to_string());
        // 401/403 are permanent (bad license key / no permission) — do not retry.
        if code == 401 || code == 403 {
            return Err(anyhow::Error::new(PermanentAuthError { status: code }));
        }
        debug!("PreConnect failed - HTTP {}: {}", code, error_body);
        return Err(anyhow!("PreConnect failed with HTTP {} - {}", code, error_body));
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
    timeout_secs: u64,
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

    // Log only the redacted endpoint (the URL query carries the license key).
    debug!("Connect request to {} (timeout: {}s)", redact_url(&url), timeout_secs);

    let response = client
        .post(&url)
        .header("Content-Type", "application/octet-stream")
        .header("Content-Encoding", "gzip")
        .header("User-Agent", "NewRelic-Rust-Lambda-Extension/0.1.0")
        .header("Accept-Encoding", "identity, deflate")
        .body(compressed_body)
        .timeout(Duration::from_secs(timeout_secs))
        .send()
        .await
        // Strip the request URL (contains `license_key`) before log/propagation.
        // Transient failure retried by the caller, so log at debug, not error.
        .map_err(|e| {
            let sanitized = e.without_url();
            if sanitized.is_timeout() {
                debug!("Connect timeout after {}s", timeout_secs);
            } else if sanitized.is_connect() {
                debug!("Connect connection error - cannot reach collector at {}", collector_host);
            } else {
                debug!("Connect request failed: {}", sanitized);
            }
            sanitized
        })?;

    let status = response.status();
    if !status.is_success() {
        let code = status.as_u16();
        let error_body = response.text().await.unwrap_or_else(|_| "Unable to read response body".to_string());
        // 401/403 are permanent (bad license key / no permission) — do not retry.
        if code == 401 || code == 403 {
            return Err(anyhow::Error::new(PermanentAuthError { status: code }));
        }
        debug!("Connect failed - HTTP {}: {}", code, error_body);
        return Err(anyhow!("Connect failed with HTTP {} - {}", code, error_body));
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

    for (key, value) in crate::config::get_nr_tags() {
        labels.push(Label {
            label_type: key.clone(),
            label_value: value.clone(),
        });
    }

    labels
}

#[cfg(test)]
mod connection_tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn permanent_auth_error_detected_through_context_chain() {
        // Mirrors how try_connect wraps the error: `.context("PreConnect failed")`.
        let err = anyhow::Error::new(PermanentAuthError { status: 401 })
            .context("PreConnect failed");
        assert_eq!(is_permanent_auth_error(&err), Some(401));
    }

    #[test]
    fn transient_error_is_not_permanent() {
        let err = anyhow!("Connect failed with HTTP 503 - service unavailable");
        assert_eq!(is_permanent_auth_error(&err), None);
    }

    #[test]
    fn permanent_auth_error_display_has_no_secret() {
        let msg = PermanentAuthError { status: 403 }.to_string();
        assert!(msg.contains("403"));
        assert!(!msg.contains("license_key"));
    }

    #[test]
    #[serial]
    fn handshake_fatal_latch_roundtrips() {
        reset_handshake_fatal_for_test();
        assert!(!is_handshake_fatal());
        signal_handshake_fatal();
        assert!(is_handshake_fatal());
        reset_handshake_fatal_for_test();
        assert!(!is_handshake_fatal());
    }
}
