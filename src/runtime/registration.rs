// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Lambda Extensions API: registration.
//!
//! Standard Lambda and Lambda Managed Instances (LMI) hit the same `POST
//! /2020-01-01/extension/register` endpoint, but the accepted `events` array
//! differs:
//!
//! - **Standard:** `["INVOKE", "SHUTDOWN"]`
//! - **LMI:** `["SHUTDOWN"]` — INVOKE is rejected, per
//!   <https://docs.aws.amazon.com/lambda/latest/dg/runtimes-extensions-api.html>:
//!   *"Extensions for Lambda Managed Instances functions can only register for
//!   the SHUTDOWN event. Attempting to register for the INVOKE event will
//!   result in an error."*
//!
//! That single difference is captured by the [`RegistrationSchema`] trait. The
//! HTTP transport, headers, response parsing, and error handling are shared.
//! See `LMI_SUPPORT.md` §3 for the full design rationale.

use std::env;
use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;
use tracing::{error, info, warn};

use crate::config::deployment::DeploymentContext;

use super::{retry, EXTENSION_ID_HEADER};

const EXTENSION_NAME_HEADER: &str = "Lambda-Extension-Name";
const ACCEPT_FEATURE_HEADER: &str = "Lambda-Extension-Accept-Feature";
const REGISTER_PATH: &str = "/2020-01-01/extension/register";
const REGISTER_TIMEOUT: Duration = Duration::from_secs(30);

/// Response body from a successful registration.
///
/// `accountId` is only populated when the caller sends
/// `Lambda-Extension-Accept-Feature: accountId`.
#[derive(Deserialize, Debug)]
pub struct ExtensionRegistrationResponse {
    #[serde(rename = "functionName")]
    pub function_name: String,
    #[serde(rename = "functionVersion")]
    pub function_version: String,
    #[serde(rename = "accountId", default)]
    pub account_id: Option<String>,
}

/// Typed errors from `register_extension`.
///
/// Replaces the previous `Box<dyn std::error::Error + Send + Sync>` per the
/// project's Zero-Panic policy (CLAUDE.md §2): structured errors at every
/// boundary so callers can inspect failures without string-matching.
#[derive(Debug)]
pub enum RegistrationError {
    /// `AWS_LAMBDA_RUNTIME_API` env var not set — extension launched outside
    /// of the Lambda Extensions environment.
    MissingRuntimeApi,
    /// Network/IO failure talking to the runtime API.
    Transport(reqwest::Error),
    /// Runtime API returned non-2xx. `status` is the HTTP code, `body` is the
    /// raw response payload (truncated by reqwest if very large).
    Rejected { status: u16, body: String },
    /// Response was 2xx but did not include the
    /// `Lambda-Extension-Identifier` header — the extension cannot make any
    /// further calls without it.
    MissingExtensionId,
    /// Response body did not deserialize to [`ExtensionRegistrationResponse`].
    Deserialize(reqwest::Error),
}

impl std::fmt::Display for RegistrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingRuntimeApi => {
                write!(f, "AWS_LAMBDA_RUNTIME_API not set")
            }
            Self::Transport(e) => write!(f, "HTTP transport failure: {e}"),
            Self::Rejected { status, body } => {
                write!(f, "Registration rejected: status={status}, body={body}")
            }
            Self::MissingExtensionId => write!(
                f,
                "Missing Lambda-Extension-Identifier header in response"
            ),
            Self::Deserialize(e) => {
                write!(f, "Failed to deserialize registration response: {e}")
            }
        }
    }
}

impl std::error::Error for RegistrationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(e) | Self::Deserialize(e) => Some(e),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for RegistrationError {
    fn from(e: reqwest::Error) -> Self {
        if e.is_decode() {
            Self::Deserialize(e)
        } else {
            Self::Transport(e)
        }
    }
}

impl RegistrationError {
    /// Whether this failure is transient and worth retrying (see
    /// [`crate::runtime::retry`]).
    ///
    /// Transport failures and `5xx`/`429` responses are retried. `Rejected`
    /// with any other `4xx`, a missing identifier header, a deserialize
    /// failure, and a missing env var are all terminal — none will resolve on
    /// a retry.
    #[must_use]
    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Rejected { status, .. } => retry::status_is_retryable(*status),
            Self::MissingRuntimeApi | Self::MissingExtensionId | Self::Deserialize(_) => false,
        }
    }
}

/// The shape of an extension registration request.
///
/// Implementations differ only in `events()` — everything else (URL, headers,
/// response parsing) is shared by [`register_extension`].
pub trait RegistrationSchema: Sync {
    /// Lifecycle events to subscribe to. AWS validates this list and rejects
    /// mismatches (e.g., LMI rejects `INVOKE`).
    fn events(&self) -> &'static [&'static str];

    /// Display name for logging (e.g., "standard", "managed").
    fn name(&self) -> &'static str;
}

/// Registration shape for standard AWS Lambda functions.
#[derive(Debug)]
pub struct StandardSchema;

impl RegistrationSchema for StandardSchema {
    fn events(&self) -> &'static [&'static str] {
        &["INVOKE", "SHUTDOWN"]
    }
    fn name(&self) -> &'static str {
        "standard"
    }
}

/// Registration shape for Lambda Managed Instances (LMI).
///
/// Subscribes to `SHUTDOWN` only — AWS rejects `INVOKE` registration on LMI
/// because LMI supports concurrent invocations within a single execution
/// environment. Per-invocation tracking moves to the Telemetry API's
/// `platform.report` event.
#[derive(Debug)]
pub struct ManagedSchema;

impl RegistrationSchema for ManagedSchema {
    fn events(&self) -> &'static [&'static str] {
        &["SHUTDOWN"]
    }
    fn name(&self) -> &'static str {
        "managed"
    }
}

/// Pick the registration schema for a given deployment context.
///
/// The match is intentionally exhaustive (no `_` arm) so adding a new
/// `DeploymentContext` variant in the future is a compile error here — forcing
/// an explicit decision instead of silently routing through the standard path.
#[must_use]
pub fn schema_for(ctx: DeploymentContext) -> &'static dyn RegistrationSchema {
    match ctx {
        DeploymentContext::Normal { .. } => &StandardSchema,
        DeploymentContext::Lmi => &ManagedSchema,
    }
}

/// Register the extension with the Lambda Runtime API, with bounded retry on
/// transient failures.
///
/// `schema` controls the lifecycle events the extension subscribes to. Use
/// [`schema_for`] to dispatch from a [`DeploymentContext`].
///
/// Wraps [`register_once`] in the shared cold-start retry policy ([`retry`]):
/// transport errors and `5xx`/`429` responses are retried up to
/// [`retry::MAX_ATTEMPTS`] times with escalating backoff. Registration is a
/// once-per-execution-environment call, so a transient blip here would
/// otherwise leave the environment running blind for its whole lifetime.
///
/// # Errors
///
/// Returns [`RegistrationError`] for any failure — env-var missing, network
/// error, non-2xx response, missing identifier header, or response that fails
/// to deserialize. Terminal (non-retryable) errors are returned on the first
/// attempt.
pub async fn register_extension(
    client: &Client,
    extension_name: &str,
    schema: &dyn RegistrationSchema,
) -> Result<(ExtensionRegistrationResponse, String), RegistrationError> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        match register_once(client, extension_name, schema).await {
            Ok(v) => return Ok(v),
            Err(e) if attempt < retry::MAX_ATTEMPTS && e.is_retryable() => {
                let delay = retry::backoff(attempt);
                warn!(
                    "[NR_EXT] registration schema={} attempt {attempt}/{} failed: {e} — retrying in {delay:?}",
                    schema.name(),
                    retry::MAX_ATTEMPTS
                );
                tokio::time::sleep(delay).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// A single registration attempt. Retry/backoff lives in the caller
/// [`register_extension`].
async fn register_once(
    client: &Client,
    extension_name: &str,
    schema: &dyn RegistrationSchema,
) -> Result<(ExtensionRegistrationResponse, String), RegistrationError> {
    let runtime_api =
        env::var("AWS_LAMBDA_RUNTIME_API").map_err(|_| RegistrationError::MissingRuntimeApi)?;

    let url = format!("http://{runtime_api}{REGISTER_PATH}");
    let payload = serde_json::json!({ "events": schema.events() });

    info!(
        "[NR_EXT] registering extension name={} schema={} events={:?}",
        extension_name,
        schema.name(),
        schema.events()
    );

    let response = client
        .post(&url)
        .header(EXTENSION_NAME_HEADER, extension_name)
        .header(ACCEPT_FEATURE_HEADER, "accountId")
        .json(&payload)
        .timeout(REGISTER_TIMEOUT)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<failed to read response body>".to_string());
        error!(
            "[NR_EXT] registration failed schema={} status={} body={}",
            schema.name(),
            status,
            body
        );
        return Err(RegistrationError::Rejected { status, body });
    }

    let extension_id = response
        .headers()
        .get(EXTENSION_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or(RegistrationError::MissingExtensionId)?
        .to_string();

    let registration: ExtensionRegistrationResponse = response.json().await?;
    Ok((registration, extension_id))
}

#[cfg(test)]
#[path = "registration_test.rs"]
mod registration_test;
