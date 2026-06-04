// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! AWS Lambda Extensions API integration
//! Handles extension registration, telemetry subscription, and event polling.
//!
//! Registration lives in the [`registration`] submodule (Standard + Managed
//! schemas). Telemetry subscription and event polling stay in this file.

pub mod registration;
pub mod telemetry_schema;

// Re-export the call-site API. The trait and concrete schemas stay scoped to
// `registration::` — callers should reach into the submodule explicitly when
// they need them, keeping the runtime root API minimal.
pub use registration::{register_extension, schema_for, ExtensionRegistrationResponse};
pub use telemetry_schema::{TelemetrySchema, TelemetrySubscriptionError};

use std::{env, time::Duration};
use reqwest::Client;
use serde::Deserialize;
use tracing::{debug, error, info, warn};

/// Header used by registration, telemetry subscription, and event polling.
/// `pub(crate)` so the `registration` submodule can share it.
pub(crate) const EXTENSION_ID_HEADER: &str = "Lambda-Extension-Identifier";

/// Fixed Telemetry API endpoint version — the `<version>` segment in
/// `PUT /<version>/telemetry`.
///
/// This identifies the API, **not** the event schema: it never varies with
/// [`TelemetrySchema`], which only sets the body `schemaVersion`. It is the
/// sibling of the `2020-01-01` Extensions API path used for registration and
/// `/event/next`. Routing the schema version (e.g. `2025-01-29`) into this
/// path segment is what produced the LMI subscription `404 page not found`.
pub const TELEMETRY_API_VERSION: &str = "2022-07-01";

/// Shutdown reasons from AWS Lambda (matching Go extension implementation)
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ShutdownReason {
    Spindown,  // Normal shutdown
    Timeout,   // Lambda timeout
    Failure,   // Lambda failure/fault
    #[serde(other)]
    Unknown,   // Any other reason
}

impl ShutdownReason {
    /// Convert to string representation
    pub fn as_str(&self) -> &str {
        match self {
            ShutdownReason::Spindown => "spindown",
            ShutdownReason::Timeout => "timeout",
            ShutdownReason::Failure => "failure",
            ShutdownReason::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for ShutdownReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Deserialize, Debug)]
#[serde(tag = "eventType")]
pub enum LambdaRuntimeEvent {
    #[serde(rename(deserialize = "INVOKE"))]
    Invoke {
        #[serde(rename(deserialize = "requestId"))]
        request_id: String,
        #[serde(rename(deserialize = "invokedFunctionArn"))]
        invoked_function_arn: String,
        /// Epoch milliseconds at which the function will time out. Used by the event
        /// loop as the upper bound when waiting for platform.runtimeDone so the wait
        /// can never outlive the function's own timeout.
        #[serde(rename(deserialize = "deadlineMs"), default)]
        deadline_ms: i64,
    },
    #[serde(rename(deserialize = "SHUTDOWN"))]
    Shutdown {
        #[serde(rename(deserialize = "shutdownReason"))]
        shutdown_reason: ShutdownReason,
    },
}

/// Subscribe to the Lambda Telemetry API.
///
/// Both attempts PUT to the fixed [`TELEMETRY_API_VERSION`] endpoint; only the
/// body `schemaVersion` differs. Tries [`TelemetrySchema::V2025_01_29`] first;
/// on `HTTP 400` or `404`, retries once with [`TelemetrySchema::V2022_07_01`]
/// (Standard Lambda runtimes that have not yet been upgraded to the newer
/// schema reject the body with 400; a 404 from a runtime that does not serve
/// the newer schema is treated the same way rather than failing hard).
///
/// Returns the schema actually accepted by AWS so callers can gate
/// schema-specific record parsing (e.g., host-level metadata that only appears
/// under `2025-01-29`).
///
/// # Errors
///
/// - [`TelemetrySubscriptionError::MissingRuntimeApi`] if `AWS_LAMBDA_RUNTIME_API`
///   is not set.
/// - [`TelemetrySubscriptionError::Transport`] for network/IO failures.
/// - [`TelemetrySubscriptionError::Rejected`] for any non-2xx response that is
///   *not* a 400/404 on the preferred schema (those trigger the fallback
///   rather than returning).
pub async fn subscribe_to_telemetry(
    client: &Client,
    ext_id: &str,
    port: u16,
) -> Result<TelemetrySchema, TelemetrySubscriptionError> {
    match try_subscribe(client, ext_id, port, TelemetrySchema::V2025_01_29).await {
        Ok(()) => {
            info!(
                "[NR_EXT] subscribed to Telemetry API schema={}",
                TelemetrySchema::V2025_01_29.name()
            );
            Ok(TelemetrySchema::V2025_01_29)
        }
        Err(TelemetrySubscriptionError::Rejected {
            status: status @ (400 | 404),
            body,
            schema: TelemetrySchema::V2025_01_29,
        }) => {
            warn!(
                "[NR_EXT] Telemetry API {} rejected with {status} (body: {body}) — falling back to {}",
                TelemetrySchema::V2025_01_29.name(),
                TelemetrySchema::V2022_07_01.name()
            );
            try_subscribe(client, ext_id, port, TelemetrySchema::V2022_07_01).await?;
            info!(
                "[NR_EXT] subscribed to Telemetry API schema={} (after fallback)",
                TelemetrySchema::V2022_07_01.name()
            );
            Ok(TelemetrySchema::V2022_07_01)
        }
        Err(e) => Err(e),
    }
}

/// Single subscription attempt against one schema. Used both as the first
/// try (`2025-01-29`) and the fallback (`2022-07-01`) — same wire shape, same
/// headers, same timeout.
async fn try_subscribe(
    client: &Client,
    ext_id: &str,
    port: u16,
    schema: TelemetrySchema,
) -> Result<(), TelemetrySubscriptionError> {
    let runtime_api =
        env::var("AWS_LAMBDA_RUNTIME_API").map_err(|_| TelemetrySubscriptionError::MissingRuntimeApi)?;

    // The URL path version is the fixed Telemetry API endpoint version — never
    // the event schema. The schema is carried only in the body `schemaVersion`.
    let url = format!("http://{runtime_api}/{TELEMETRY_API_VERSION}/telemetry");

    let payload = serde_json::json!({
        "schemaVersion": schema.schema_version(),
        "types": ["platform", "function", "extension"],
        "buffering": {
            "maxBytes": 262144,
            "maxItems": 1000,
            // Minimum value to ensure platform.report events are delivered in
            // the current invocation rather than after the freeze.
            "timeoutMs": 25
        },
        "destination": {
            "protocol": "HTTP",
            "URI": format!("http://sandbox:{port}/telemetry")
        }
    });

    debug!(
        "[NR_EXT] attempting Telemetry API subscription schema={} url={url}",
        schema.name()
    );

    let response = client
        .put(&url)
        .header(EXTENSION_ID_HEADER, ext_id)
        .json(&payload)
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(TelemetrySubscriptionError::Transport)?;

    if !response.status().is_success() {
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<failed to read response body>".to_string());
        error!(
            "[NR_EXT] Telemetry subscription failed schema={} status={status} body={body}",
            schema.name()
        );
        return Err(TelemetrySubscriptionError::Rejected {
            status,
            body,
            schema,
        });
    }

    Ok(())
}

pub async fn fetch_next_event(
    client: &Client,
    ext_id: &str,
) -> Result<LambdaRuntimeEvent, Box<dyn std::error::Error + Send + Sync>> {
    let runtime_api = env::var("AWS_LAMBDA_RUNTIME_API")
        .map_err(|_| "AWS_LAMBDA_RUNTIME_API not set")?;

    let url = format!("http://{}/2020-01-01/extension/event/next", runtime_api);

    const MAX_RETRIES: u32 = 3;
    let mut retry_count = 0;

    loop {
        debug!("About to call /next API (attempt {}/{})", retry_count + 1, MAX_RETRIES);
        let call_start = std::time::Instant::now();

        let response = client
            .get(&url)
            .header(EXTENSION_ID_HEADER, ext_id)
            .send()
            .await;

        let call_duration = call_start.elapsed();

        match response {
            Ok(resp) => {
                debug!("/next response received after {:?}", call_duration);

                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_else(|_| "Failed to read response body".to_string());
                    error!("/next request failed with status: {}, body: {}", status, body);
                    return Err(format!("Next event request failed with status: {}", status).into());
                }

                debug!("Parsing /next response JSON...");
                let event: LambdaRuntimeEvent = resp.json().await?;
                let event_type_str = match &event {
                    LambdaRuntimeEvent::Invoke { .. } => "INVOKE",
                    LambdaRuntimeEvent::Shutdown { .. } => "SHUTDOWN",
                };
                debug!("/next event parsed successfully: eventType={}", event_type_str);
                return Ok(event);
            },
            Err(e) => {
                error!("/next call failed after {:?}: {}", call_duration, e);
                retry_count += 1;

                if e.is_timeout() {
                    warn!("Event polling timeout (attempt {}/{})", retry_count, MAX_RETRIES);
                } else if e.is_connect() {
                    warn!("Event polling connection error (attempt {}/{})", retry_count, MAX_RETRIES);
                } else {
                    warn!("Event polling error (attempt {}/{}): {}", retry_count, MAX_RETRIES, e);
                }

                if retry_count >= MAX_RETRIES {
                    error!("Event polling failed after {} retries", MAX_RETRIES);
                    return Err(e.into());
                }

                let delay = match retry_count {
                    1 => Duration::from_millis(200),
                    2 => Duration::from_millis(400),
                    _ => Duration::from_millis(900),
                };
                warn!("Retrying in {:?}...", delay);
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
#[path = "telemetry_subscribe_test.rs"]
mod telemetry_subscribe_test;
