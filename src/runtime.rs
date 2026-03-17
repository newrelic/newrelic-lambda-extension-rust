//! AWS Lambda Extensions API integration
//! Handles extension registration, telemetry subscription, and event polling

use std::{env, time::Duration};
use reqwest::Client;
use serde::Deserialize;
use tracing::{debug, error, warn};

/// Cached Lambda Runtime API address — immutable for container lifetime, avoids
/// per-invocation env::var lookup + String allocation in the hot polling loop.
static RUNTIME_API: once_cell::sync::OnceCell<String> = once_cell::sync::OnceCell::new();

fn get_runtime_api() -> Result<&'static str, Box<dyn std::error::Error + Send + Sync>> {
    RUNTIME_API
        .get_or_try_init(|| env::var("AWS_LAMBDA_RUNTIME_API"))
        .map(|s| s.as_str())
        .map_err(|_| "AWS_LAMBDA_RUNTIME_API not set".into())
}

const EXTENSION_NAME_HEADER: &str = "Lambda-Extension-Name";
const EXTENSION_ID_HEADER: &str = "Lambda-Extension-Identifier";

#[derive(Deserialize, Debug)]
pub struct ExtensionRegistrationResponse {
    #[serde(rename = "functionName")]
    pub function_name: String,
    #[serde(rename = "functionVersion")]
    pub function_version: String,
    #[serde(rename = "accountId", default)]
    pub account_id: Option<String>,
}

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
    },
    #[serde(rename(deserialize = "SHUTDOWN"))]
    Shutdown {
        #[serde(rename(deserialize = "shutdownReason"))]
        shutdown_reason: ShutdownReason,
    },
}

pub async fn register_extension(
    client: &Client,
    extension_name: &str,
) -> Result<(ExtensionRegistrationResponse, String), Box<dyn std::error::Error + Send + Sync>> {
    let runtime_api = get_runtime_api()?;

    let url = format!("http://{}/2020-01-01/extension/register", runtime_api);
    
    let payload = serde_json::json!({
        "events": ["INVOKE", "SHUTDOWN"]
    });

    let response = client
        .post(&url)
        .header(EXTENSION_NAME_HEADER, extension_name)
        .header("Lambda-Extension-Accept-Feature", "accountId")
        .json(&payload)
        .timeout(Duration::from_secs(30))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_else(|_| "Failed to read response body".to_string());
        error!("Registration failed with status: {}, body: {}", status, body);
        return Err(format!("Registration failed with status: {}", status).into());
    }

    let extension_id = response
        .headers()
        .get(EXTENSION_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or("Missing extension ID in response headers")?
        .to_string();

    let registration: ExtensionRegistrationResponse = response.json().await?;

    Ok((registration, extension_id))
}

pub async fn subscribe_to_telemetry(
    client: &Client,
    ext_id: &str,
    port: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let runtime_api = get_runtime_api()?;

    let url = format!("http://{}/2022-07-01/telemetry", runtime_api);
    
    let payload = serde_json::json!({
        "schemaVersion": "2022-07-01",
        "types": ["platform", "function", "extension"],
        "buffering": {
            "maxBytes": 262144,
            "maxItems": 1000,
            "timeoutMs": 25  // Minimum value to ensure platform.report events are delivered in current invocation
        },
        "destination": {
            "protocol": "HTTP",
            "URI": format!("http://sandbox:{}/telemetry", port)
        }
    });

    let response = client
        .put(&url)
        .header(EXTENSION_ID_HEADER, ext_id)
        .json(&payload)
        .timeout(Duration::from_secs(30))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_else(|_| "Failed to read response body".to_string());
        error!("Telemetry subscription failed with status: {}, body: {}", status, body);
        return Err(format!("Telemetry subscription failed with status: {}", status).into());
    }

    Ok(())
}

pub async fn fetch_next_event(
    client: &Client,
    ext_id: &str,
) -> Result<LambdaRuntimeEvent, Box<dyn std::error::Error + Send + Sync>> {
    let runtime_api = get_runtime_api()?;

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
