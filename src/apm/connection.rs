// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! APM collector connection (PreConnect and Connect)
//!
//! Based on connect.go PreConnect() and Connect()

use anyhow::{Result, anyhow};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, info};
use flate2::write::GzEncoder;
use flate2::Compression;

use crate::config::deployment::DeploymentContext;
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

// ── Handshake diagnostics ────────────────────────────────────────────────────
// Track how hard we tried to connect and why we last failed, so the shutdown
// summary can tell the customer (and us) exactly what happened — e.g. "30 total
// attempts across 12 reconnect cycles, last failure: HTTP 503". All reset on a
// successful connect via `reset_connect_stats()`.

/// Total individual PreConnect/Connect attempts that have failed since the last
/// successful connect (3 per `ApmApp::new` burst).
static CONNECT_ATTEMPTS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Number of reconnect cycles (one per `ApmApp::new` call: startup, per-invoke, shutdown).
static CONNECT_CYCLES: AtomicU64 = AtomicU64::new(0);
/// Concise reason for the most recent handshake failure, including the HTTP
/// status when the collector responded (e.g. "HTTP 503"), or the network cause
/// otherwise (e.g. "timeout after 5s", "connection error").
static LAST_HANDSHAKE_FAILURE: Mutex<Option<String>> = Mutex::new(None);

/// Record one failed handshake attempt (call per PreConnect/Connect try).
pub fn record_connect_attempt() {
    CONNECT_ATTEMPTS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Record one reconnect cycle (call once per `ApmApp::new`).
pub fn record_connect_cycle() {
    CONNECT_CYCLES.fetch_add(1, Ordering::Relaxed);
}

/// Store the most recent handshake failure reason (set at the failure site so
/// the HTTP status is preserved).
pub fn record_failure_reason(reason: impl Into<String>) {
    if let Ok(mut g) = LAST_HANDSHAKE_FAILURE.lock() {
        *g = Some(reason.into());
    }
}

pub fn connect_attempts_total() -> u64 {
    CONNECT_ATTEMPTS_TOTAL.load(Ordering::Relaxed)
}

pub fn connect_cycles() -> u64 {
    CONNECT_CYCLES.load(Ordering::Relaxed)
}

pub fn last_failure_reason() -> Option<String> {
    LAST_HANDSHAKE_FAILURE.lock().ok().and_then(|g| g.clone())
}

/// Reset all handshake diagnostics — called after a successful connect so the
/// counters reflect only the current disconnected streak.
pub fn reset_connect_stats() {
    CONNECT_ATTEMPTS_TOTAL.store(0, Ordering::Relaxed);
    CONNECT_CYCLES.store(0, Ordering::Relaxed);
    if let Ok(mut g) = LAST_HANDSHAKE_FAILURE.lock() {
        *g = None;
    }
}

/// Build a failure reason from an HTTP status + the collector's response body.
/// Uses the collector's actual error message (not a hardcoded phrase); falls
/// back to just the code when the body is empty. Body is trimmed/truncated so a
/// verbose response can't bloat the log line.
pub(crate) fn http_failure_reason(code: u16, body: &str) -> String {
    let body = body.trim();
    if body.is_empty() {
        format!("HTTP {code}")
    } else {
        let truncated: String = body.chars().take(300).collect();
        format!("HTTP {code}: {truncated}")
    }
}

/// OPTIMIZATION: Inline compression (no spawn_blocking overhead)
pub(crate) fn compress_inline(data: &[u8]) -> Result<Vec<u8>> {
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
    /// LMI instance identifier from `platform.initStart`. Absent on Standard Lambda.
    #[serde(rename = "aws.lambda.managedInstance.instanceId", skip_serializing_if = "Option::is_none")]
    pub managed_instance_id: Option<String>,
    /// LMI maximum memory (raw AWS uint64, bytes). Absent on Standard Lambda or when AWS omits it.
    #[serde(rename = "aws.lambda.managedInstance.instanceMaxMemory", skip_serializing_if = "Option::is_none")]
    pub managed_instance_max_memory: Option<u64>,
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
        .header("User-Agent", crate::version::user_agent())
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
            let reason = if sanitized.is_timeout() {
                format!("timeout after {timeout_secs}s")
            } else if sanitized.is_connect() {
                "connection error".to_string()
            } else {
                "network error".to_string()
            };
            debug!("PreConnect {} - cannot reach collector at {}", reason, base_host);
            record_failure_reason(format!("PreConnect {reason}"));
            sanitized
        })?;

    let status = response.status();
    if !status.is_success() {
        let code = status.as_u16();
        let error_body = response.text().await.unwrap_or_else(|_| "Unable to read response body".to_string());
        // Record the collector's actual error response (not a hardcoded phrase).
        record_failure_reason(http_failure_reason(code, &error_body));
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
#[allow(clippy::too_many_arguments)]
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
    lmi_metadata: Option<crate::telemetry::managed_instance::ManagedInstanceMetadata>,
    deployment: DeploymentContext,
) -> Result<ConnectResponse> {
    let url = format!(
        "https://{collector_host}/agent_listener/invoke_raw_method?marshal_format=json&protocol_version=17&method=connect&license_key={license_key}"
    );

    let (managed_instance_id, managed_instance_max_memory) = match lmi_metadata {
        Some(meta) => (Some(meta.instance_id), meta.instance_max_memory),
        None => (None, None),
    };

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
                    managed_instance_id,
                    managed_instance_max_memory,
                },
            },
        },
        labels: get_labels(function_arn, runtime, deployment),
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
        .header("User-Agent", crate::version::user_agent())
        .header("Accept-Encoding", "identity, deflate")
        .body(compressed_body)
        .timeout(Duration::from_secs(timeout_secs))
        .send()
        .await
        // Strip the request URL (contains `license_key`) before log/propagation.
        // Transient failure retried by the caller, so log at debug, not error.
        .map_err(|e| {
            let sanitized = e.without_url();
            let reason = if sanitized.is_timeout() {
                format!("timeout after {timeout_secs}s")
            } else if sanitized.is_connect() {
                "connection error".to_string()
            } else {
                "network error".to_string()
            };
            debug!("Connect {} - cannot reach collector at {}", reason, collector_host);
            record_failure_reason(format!("Connect {reason}"));
            sanitized
        })?;

    let status = response.status();
    if !status.is_success() {
        let code = status.as_u16();
        let error_body = response.text().await.unwrap_or_else(|_| "Unable to read response body".to_string());
        // Record the collector's actual error response (not a hardcoded phrase).
        record_failure_reason(http_failure_reason(code, &error_body));
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
fn get_labels(function_arn: &str, runtime: &str, deployment: DeploymentContext) -> Vec<Label> {
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

    // Only present on Lambda Managed Instances — absent (not "false") on Normal Lambda.
    if deployment.is_lmi() {
        labels.push(Label {
            label_type: "isLMI".to_string(),
            label_value: "true".to_string(),
        });
    }

    for (key, value) in crate::config::get_nr_tags() {
        labels.push(Label {
            label_type: key.clone(),
            label_value: value.clone(),
        });
    }

    // NEW_RELIC_LABELS (agent-specs/Labels.md) - additive, alongside NR_TAGS above.
    // Sent unprefixed here, matching the connect payload's `label_type`/`label_value`
    // shape - confirmed against the official Python agent (agent_protocol.py's
    // _connect_payload sends settings["labels"] verbatim, built by config.py's
    // _process_labels_setting as {"label_type": key, "label_value": value}, no
    // prefix). The `tags.` prefix only applies to the log-forwarding path (client.rs),
    // matching data_collector.py's `f"tags.{label['label_type']}"` construction there.
    for (key, value) in crate::config::get_new_relic_labels() {
        labels.push(Label {
            label_type: key.clone(),
            label_value: value.clone(),
        });
    }

    labels
}

#[cfg(test)]
#[path = "connection_tests.rs"]
mod connection_tests;
