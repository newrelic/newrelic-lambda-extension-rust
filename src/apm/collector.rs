// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! APM collector client for sending telemetry
//!
//! Based on collector.go CollectorRequest() and SendAPMTelemetry()

use crate::newrelic::tls_fallback::DualTlsClient;
use anyhow::Result;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::Value;
use std::io::Write;
use tracing::{debug, info, warn, error};

pub const CMD_METRICS: &str = "metric_data";
pub const CMD_SPAN_EVENTS: &str = "span_event_data";
pub const CMD_ERROR_EVENTS: &str = "error_event_data";
pub const CMD_ERROR_DATA: &str = "error_data";
pub const CMD_ANALYTIC_EVENTS: &str = "analytic_event_data";
pub const CMD_CUSTOM_EVENTS: &str = "custom_event_data";
pub const CMD_TRANSACTION_SAMPLES: &str = "transaction_sample_data";
pub const CMD_LOG_EVENTS: &str = "log_event_data";
pub const CMD_SLOW_SQLS: &str = "sql_trace_data";

const PROTOCOL_VERSION: u8 = 17;

fn get_user_agent() -> String {
    // Single source of truth (tracks Cargo.toml); shared with the handshake path.
    crate::version::user_agent()
}

/// APM collector error types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectorError {
    /// 410 Gone - Collector has disconnected this agent, must restart
    Disconnect,
    /// 401/409 - Restart exception, should reconnect
    RestartException,
}

impl std::fmt::Display for CollectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CollectorError::Disconnect => write!(f, "Collector disconnected (410)"),
            CollectorError::RestartException => write!(f, "Collector restart exception (401/409)"),
        }
    }
}

impl std::error::Error for CollectorError {}

/// Set when the collector returns 401/409 (restart) or 410 (disconnect): the
/// current `run_id` is no longer valid. The event loop consumes this once per
/// invoke to invalidate the cached `ApmApp` and force a fresh handshake.
static RECONNECT_NEEDED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Flag that a reconnect is required (collector restart/disconnect observed).
pub fn signal_reconnect_needed() {
    RECONNECT_NEEDED.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// Consume the reconnect-needed flag, returning whether it was set.
pub fn take_reconnect_needed() -> bool {
    RECONNECT_NEEDED.swap(false, std::sync::atomic::Ordering::Relaxed)
}

/// All telemetry types the customer may disable via `NEW_RELIC_APM_DISABLE_TELEMETRY`.
/// The first nine are agent-payload types; `platform_metrics` is the `apm.lambda.*`
/// pseudo-type derived from REPORT lines and sent to the Metric API.
pub const KNOWN_TELEMETRY_TYPES: &[&str] = &[
    "metric_data",
    "custom_event_data",
    "log_event_data",
    "analytic_event_data",
    "error_event_data",
    "error_data",
    "span_event_data",
    "sql_trace_data",
    "transaction_sample_data",
    "platform_metrics",
];

/// Process-wide set of telemetry types to drop, populated once at startup from
/// `NEW_RELIC_APM_DISABLE_TELEMETRY`. Lets code paths without `ExtensionConfig`
/// in scope (e.g. the telemetry listener) honor the customer's exclusions.
static DISABLED_TELEMETRY: once_cell::sync::Lazy<
    std::sync::RwLock<std::collections::HashSet<String>>,
> = once_cell::sync::Lazy::new(|| std::sync::RwLock::new(std::collections::HashSet::new()));

/// Record which telemetry types are disabled (called once at startup).
pub fn set_disabled_telemetry(types: std::collections::HashSet<String>) {
    if let Ok(mut guard) = DISABLED_TELEMETRY.write() {
        *guard = types;
    }
}

/// Whether the given telemetry type has been disabled by the customer.
pub fn is_telemetry_disabled(telemetry_type: &str) -> bool {
    DISABLED_TELEMETRY
        .read()
        .map(|g| g.contains(telemetry_type))
        .unwrap_or(false)
}

/// HTTP status codes worth retrying — transient server/throttle conditions.
/// 401/409 (restart) and 410 (disconnect) are handled separately and are NOT here.
pub fn is_retryable_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

/// Parse a `Retry-After: <seconds>` header into a Duration. The delta-seconds
/// form is the only one New Relic emits; HTTP-date form is ignored.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<std::time::Duration> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(std::time::Duration::from_secs)
}

/// Build the Metric API request body: an array with one map, `{"common": {"attributes": ...},
/// "metrics": [...]}`.
///
/// An attribute goes into `common` only if **every** data point in the request has it with the
/// same value, and it is then removed from each data point, so the receiver sees exactly the
/// same metrics, with the shared attributes written once. An attribute that differs between
/// data points (the request id of different invocations, say) is never moved: each data point
/// keeps its own. Timestamps, names, types and values are never touched. With fewer than two
/// data points nothing is shared, so there is no `common` block.
pub fn build_metric_payload(metrics: &[Value]) -> Value {
    let shared = shared_attributes(metrics);
    if shared.is_empty() {
        return serde_json::json!([{ "metrics": metrics }]);
    }
    let stripped: Vec<Value> = metrics.iter().map(|metric| without_attributes(metric, &shared)).collect();
    serde_json::json!([{ "common": { "attributes": shared }, "metrics": stripped }])
}

/// The attributes that every data point has, with the same value.
fn shared_attributes(metrics: &[Value]) -> serde_json::Map<String, Value> {
    if metrics.len() < 2 {
        return serde_json::Map::new();
    }
    let attributes_of = |metric: &Value| metric.get("attributes").and_then(Value::as_object).cloned();
    let Some(mut shared) = attributes_of(&metrics[0]) else {
        return serde_json::Map::new();
    };
    for metric in &metrics[1..] {
        let Some(attributes) = attributes_of(metric) else {
            return serde_json::Map::new();
        };
        shared.retain(|key, value| attributes.get(key) == Some(value));
        if shared.is_empty() {
            break;
        }
    }
    shared
}

/// A copy of the data point without the attributes that moved to `common`.
fn without_attributes(metric: &Value, shared: &serde_json::Map<String, Value>) -> Value {
    let mut metric = metric.clone();
    if let Some(attributes) = metric.get_mut("attributes").and_then(Value::as_object_mut) {
        attributes.retain(|key, _| !shared.contains_key(key));
        if attributes.is_empty() {
            if let Some(object) = metric.as_object_mut() {
                object.remove("attributes");
            }
        }
    }
    metric
}

/// gzip-compress a request body (the Metric API takes `Content-Encoding: gzip`).
pub(crate) fn gzip_bytes(data: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data)?;
    encoder.finish()
}

/// Test helper: the text of a gzip-compressed request body.
#[cfg(test)]
pub(crate) fn gunzip_text(body: &[u8]) -> String {
    use std::io::Read;
    let mut text = String::new();
    flate2::read::GzDecoder::new(body).read_to_string(&mut text).expect("a valid gzip body");
    text
}

/// Test helper: the JSON payload of a gzip-compressed Metric API request body.
#[cfg(test)]
pub(crate) fn decode_metric_request(body: &[u8]) -> Value {
    serde_json::from_str(&gunzip_text(body)).expect("the body is JSON")
}

/// Test helper: the data points a receiver ends up with, i.e. `common` attributes merged back
/// into each one (a data point's own value wins, as the API defines).
#[cfg(test)]
pub(crate) fn expand_common(payload: &Value) -> Vec<Value> {
    let mut all = Vec::new();
    for map in payload.as_array().expect("an array of maps") {
        let common = map["common"]["attributes"].as_object().cloned().unwrap_or_default();
        for metric in map["metrics"].as_array().expect("a metrics array") {
            let mut metric = metric.clone();
            if !common.is_empty() {
                let mut merged = common.clone();
                if let Some(own) = metric.get("attributes").and_then(Value::as_object) {
                    merged.extend(own.clone());
                }
                metric["attributes"] = Value::Object(merged);
            }
            all.push(metric);
        }
    }
    all
}

/// The Metric API rejects a POST over 1 MB (10^6 bytes). Requests are kept under this, with
/// margin for what the body adds around the data points.
pub const MAX_METRIC_PAYLOAD_BYTES: usize = 900_000;

/// Bytes around the data points in a request body: `[{"metrics":[` and `]}]`, with room to spare.
const METRIC_PAYLOAD_OVERHEAD_BYTES: usize = 16;

/// Split items (given by their serialized size) into consecutive groups that each fit one
/// request of at most `max` bytes. An item larger than `max` gets a group of its own: it is
/// sent as it is, and the API's answer decides what happens to it.
pub fn split_by_payload_size(sizes: &[usize], max: usize) -> Vec<std::ops::Range<usize>> {
    let mut groups = Vec::new();
    let mut start = 0;
    let mut total = METRIC_PAYLOAD_OVERHEAD_BYTES;
    for (index, size) in sizes.iter().enumerate() {
        let needed = size + 1; // the comma between data points
        if index > start && total + needed > max {
            groups.push(start..index);
            start = index;
            total = METRIC_PAYLOAD_OVERHEAD_BYTES;
        }
        total += needed;
    }
    if start < sizes.len() {
        groups.push(start..sizes.len());
    }
    groups
}

/// Outcome classification for a Metric API send failure.
#[derive(Debug)]
pub enum MetricApiError {
    /// Transient failure (5xx/429/408) — safe to retry.
    Retryable {
        status: u16,
        retry_after: Option<std::time::Duration>,
    },
    /// Permanent failure (4xx other than 429/408) — retrying will not help.
    Permanent { status: u16 },
    /// Transport/network/timeout error — safe to retry.
    Network(anyhow::Error),
}

impl MetricApiError {
    pub fn retry_after(&self) -> Option<std::time::Duration> {
        match self {
            MetricApiError::Retryable { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
    pub fn is_permanent(&self) -> bool {
        matches!(self, MetricApiError::Permanent { .. })
    }
}

impl std::fmt::Display for MetricApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricApiError::Retryable { status, .. } => {
                write!(f, "Metric API transient error (status {status})")
            }
            MetricApiError::Permanent { status } => {
                write!(f, "Metric API permanent error (status {status})")
            }
            MetricApiError::Network(e) => write!(f, "Metric API network error: {}", crate::newrelic::send_error::describe_anyhow(e)),
        }
    }
}

impl std::error::Error for MetricApiError {}

/// Send error event telemetry to APM collector
/// Error events have a special structure: [run_id, {events_seen, reservoir_size}, [events]]
pub async fn send_error_events(
    client: &DualTlsClient,
    license_key: &str,
    collector_host: &str,
    run_id: &str,
    error_events: &[Value],
    request_id: &str,
) -> Result<()> {
    if error_events.is_empty() {
        return Ok(());
    }

    let wrapped_data = serde_json::json!([
        run_id,
        {
            "events_seen": error_events.len(),
            "reservoir_size": 100
        },
        error_events
    ]);

    let url = format!(
        "https://{collector_host}/agent_listener/invoke_raw_method?marshal_format=json&protocol_version={PROTOCOL_VERSION}&method={CMD_ERROR_EVENTS}&license_key={license_key}&run_id={run_id}"
    );

    let payload_json = serde_json::to_string(&wrapped_data)?;
    let uncompressed_len = payload_json.len();

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(payload_json.as_bytes())?;
    let compressed = encoder.finish()?;

    debug!(
        "Sending {} error events for request {}: compressed={} bytes, uncompressed={} bytes",
        error_events.len(),
        request_id,
        compressed.len(),
        uncompressed_len
    );

    let start_time = std::time::Instant::now();
    let response = client
        .active()
        .post(&url)
        .header("NR-Session", run_id)
        .header("Accept-Encoding", "identity, deflate")
        .header("Content-Type", "application/octet-stream")
        .header("User-Agent", get_user_agent())
        .header("Content-Encoding", "gzip")
        .body(compressed)
        .send()
        .await
        // Strip the request URL (carries `license_key`) from any error before it
        // propagates to a log site.
        .map_err(|e| {
            crate::newrelic::tls_fallback::classify_and_maybe_flip(&e);
            e.without_url()
        })?;
    let duration = start_time.elapsed();

    let status = response.status();
    let status_code = status.as_u16();

    if status.is_success() {
        info!(
            "Successfully sent {} error events for request {} (status: {}, duration: {}ms)",
            error_events.len(), request_id, status, duration.as_millis()
        );
        Ok(())
    } else {
        let body = response.text().await.unwrap_or_default();
        debug!("Status Code for {} telemetry: {}", CMD_ERROR_EVENTS, status_code);

        if status_code == 410 {
            error!("APM collector disconnected (410) - agent should stop sending telemetry");
            signal_reconnect_needed();
            return Err(anyhow::Error::new(CollectorError::Disconnect)
                .context(format!("Collector returned 410 for {}", CMD_ERROR_EVENTS)));
        } else if status_code == 409 {
            info!("APM collector restart exception (409) - reconnection needed");
            signal_reconnect_needed();
            return Err(anyhow::Error::new(CollectorError::RestartException)
                .context(format!("Collector returned 409 for {}", CMD_ERROR_EVENTS)));
        } else if status_code == 401 {
            warn!("APM collector restart exception (401) - reconnection needed");
            signal_reconnect_needed();
            return Err(anyhow::Error::new(CollectorError::RestartException)
                .context(format!("Collector returned 401 for {}", CMD_ERROR_EVENTS)));
        }

        warn!(
            "Failed to send error events for request {} (status: {}, body: {})",
            request_id, status_code, body
        );

        Err(anyhow::anyhow!("APM collector returned status {}: {}", status_code, body))
    }
}

/// Send telemetry data to APM collector
pub async fn send_apm_telemetry(
    client: &DualTlsClient,
    license_key: &str,
    collector_host: &str,
    run_id: &str,
    command: &str,
    data: &[Value],
    request_id: &str,
) -> Result<()> {
    let mut processed_data = data.to_vec();
    if !processed_data.is_empty() {
        if processed_data[0].is_null() || processed_data[0].as_str() == Some("") {
            // null = Python/Node/.NET/Ruby placeholder; "" = Go placeholder — both replaced with actual run_id
            processed_data[0] = serde_json::json!(run_id);
        } else {
            // sql_trace_data: no placeholder at index 0 — prepend run_id
            processed_data.insert(0, serde_json::json!(run_id));
        }
    }

    let url = format!(
        "https://{collector_host}/agent_listener/invoke_raw_method?marshal_format=json&protocol_version={PROTOCOL_VERSION}&method={command}&license_key={license_key}&run_id={run_id}"
    );

    let payload_json = serde_json::to_string(&processed_data)?;
    let uncompressed_len = payload_json.len();

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(payload_json.as_bytes())?;
    let compressed = encoder.finish()?;

    debug!(
        "Sending {} for request {} to APM collector: {} bytes compressed (from {} bytes)",
        command,
        request_id,
        compressed.len(),
        uncompressed_len
    );

    let start_time = std::time::Instant::now();
    let response = client
        .active()
        .post(&url)
        .header("NR-Session", run_id)
        .header("Accept-Encoding", "identity, deflate")
        .header("Content-Type", "application/octet-stream")
        .header("User-Agent", get_user_agent())
        .header("Content-Encoding", "gzip")
        .body(compressed)
        .send()
        .await
        // Strip the request URL (carries `license_key`) from any error before it
        // propagates to a log site.
        .map_err(|e| {
            crate::newrelic::tls_fallback::classify_and_maybe_flip(&e);
            e.without_url()
        })?;
    let duration = start_time.elapsed();

    let status = response.status();
    let status_code = status.as_u16();

    if status.is_success() {
        info!(
            "Successfully sent {} for request {} (status: {}, duration: {}ms)",
            command, request_id, status, duration.as_millis()
        );
        Ok(())
    } else {
        let body = response.text().await.unwrap_or_default();

        debug!("Status Code for {} telemetry: {}", command, status_code);

        if status_code == 410 {
            error!("APM collector disconnected (410) - agent should stop sending telemetry");
            signal_reconnect_needed();
            return Err(anyhow::Error::new(CollectorError::Disconnect)
                .context(format!("Collector returned 410 for {}", command)));
        } else if status_code == 409 {
            info!("APM collector restart exception (409) - reconnection needed");
            signal_reconnect_needed();
            return Err(anyhow::Error::new(CollectorError::RestartException)
                .context(format!("Collector returned 409 for {}", command)));
        } else if status_code == 401 {
            warn!("APM collector restart exception (401) - reconnection needed");
            signal_reconnect_needed();
            return Err(anyhow::Error::new(CollectorError::RestartException)
                .context(format!("Collector returned 401 for {}", command)));
        }

        warn!(
            "Failed to send {} for request {} (status: {}, body: {})",
            command, request_id, status_code, body
        );

        Err(anyhow::anyhow!("APM collector returned status {}: {}", status_code, body))
    }
}

/// Send platform metrics to Metric API.
///
/// Returns a typed [`MetricApiError`] so the caller can distinguish transient
/// failures (buffer + retry) from permanent ones (drop). The caller retains
/// ownership of `metrics` so it can re-buffer them on a retryable failure.
pub async fn send_platform_metrics(
    client: &DualTlsClient,
    license_key: &str,
    metric_endpoint: &str,
    metrics: &[Value],
) -> std::result::Result<(), MetricApiError> {
    if metrics.is_empty() {
        debug!("No platform metrics to send");
        return Ok(());
    }

    let payload = build_metric_payload(metrics);

    let payload_json = match serde_json::to_vec(&payload) {
        Ok(bytes) => bytes,
        Err(e) => return Err(MetricApiError::Network(anyhow::Error::new(e))),
    };

    debug!("Platform metrics payload JSON: {}", String::from_utf8_lossy(&payload_json));

    let body = match gzip_bytes(&payload_json) {
        Ok(bytes) => bytes,
        Err(e) => return Err(MetricApiError::Network(anyhow::Error::new(e))),
    };

    debug!(
        "Sending {} platform metrics to Metric API endpoint: {} ({} bytes uncompressed, {} compressed)",
        metrics.len(),
        metric_endpoint,
        payload_json.len(),
        body.len()
    );

    let start_time = std::time::Instant::now();
    let response = match client
        .active()
        .post(metric_endpoint)
        .header("Api-Key", license_key)
        .header("Content-Type", "application/json")
        .header("Content-Encoding", "gzip")
        .body(body)
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            crate::newrelic::tls_fallback::classify_and_maybe_flip(&e);
            // license key travels in the `Api-Key` header here (not the URL), but
            // strip the URL anyway for defense-in-depth before logging.
            let e = e.without_url();
            warn!("Platform metrics network error: {} - will retry", crate::newrelic::send_error::describe_reqwest_error(&e));
            return Err(MetricApiError::Network(anyhow::Error::new(e)));
        }
    };
    let duration = start_time.elapsed();

    let status = response.status();
    let status_code = status.as_u16();

    if status.is_success() {
        info!(
            "Successfully sent {} platform metrics (status: {}, duration: {}ms)",
            metrics.len(), status, duration.as_millis()
        );
        Ok(())
    } else if is_retryable_status(status_code) {
        let retry_after = parse_retry_after(response.headers());
        let body = response.text().await.unwrap_or_default();
        warn!(
            "Platform metrics transient failure (status {}, retry_after {:?}) - will retry: {}",
            status_code, retry_after, body
        );
        Err(MetricApiError::Retryable {
            status: status_code,
            retry_after,
        })
    } else {
        let body = response.text().await.unwrap_or_default();
        warn!(
            "Platform metrics permanent failure (status {}) - dropping: {}",
            status_code, body
        );
        Err(MetricApiError::Permanent {
            status: status_code,
        })
    }
}
#[cfg(test)]
#[path = "collector_tests.rs"]
mod collector_tests;
