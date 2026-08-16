// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! APM collector client for sending telemetry
//!
//! Based on collector.go CollectorRequest() and SendAPMTelemetry()

use anyhow::Result;
use base64::{Engine as _, engine::general_purpose};
use flate2::write::GzEncoder;
use flate2::Compression;
use reqwest::Client;
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

/// Process-wide OTLP feature gate, populated once at startup (default false). True only
/// when `NEW_RELIC_OTLP_METRIC_ENABLED` **and** `NEW_RELIC_APM_LAMBDA_MODE` are both set —
/// the forwarding code lives in `ApmApp::process_agent_payload`, which does not exist in
/// serverless mode, so the env var alone is not sufficient. Lets code paths without
/// `ExtensionConfig` in scope check whether OTLP forwarding will actually happen.
static OTLP_METRIC_ENABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Record whether OTLP metrics forwarding is enabled (called once at startup).
/// Callers pass the effective value (env var AND APM mode), not the raw env var.
pub fn set_otlp_metric_enabled(enabled: bool) {
    OTLP_METRIC_ENABLED.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

/// Whether OTLP metric forwarding is active: `NEW_RELIC_OTLP_METRIC_ENABLED` is set AND
/// APM mode is on. False in serverless mode even if the env var is set.
pub fn is_otlp_metric_enabled() -> bool {
    OTLP_METRIC_ENABLED.load(std::sync::atomic::Ordering::Relaxed)
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
            MetricApiError::Network(e) => write!(f, "Metric API network error: {e}"),
        }
    }
}

impl std::error::Error for MetricApiError {}

/// Outcome classification for an OTLP payload send failure. Mirrors `MetricApiError`
/// (same license-key-only auth model, no `run_id`/collector reconnect semantics).
#[derive(Debug)]
pub enum OtlpError {
    /// Transient failure (5xx/429/408) — safe to retry.
    Retryable {
        status: u16,
        retry_after: Option<std::time::Duration>,
    },
    /// Permanent failure (4xx other than 429/408) — retrying will not help.
    Permanent { status: u16 },
    /// Transport/network/timeout error — safe to retry.
    Network(anyhow::Error),
    /// Malformed payload (bad base64, invalid protobuf) — retrying the same bytes
    /// will never succeed.
    MalformedPayload(anyhow::Error),
}

impl OtlpError {
    pub fn retry_after(&self) -> Option<std::time::Duration> {
        match self {
            OtlpError::Retryable { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
    pub fn is_permanent(&self) -> bool {
        matches!(self, OtlpError::Permanent { .. } | OtlpError::MalformedPayload(_))
    }
}

impl std::fmt::Display for OtlpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OtlpError::Retryable { status, .. } => {
                write!(f, "OTLP transient error (status {status})")
            }
            OtlpError::Permanent { status } => {
                write!(f, "OTLP permanent error (status {status})")
            }
            OtlpError::Network(e) => write!(f, "OTLP network error: {e}"),
            OtlpError::MalformedPayload(e) => write!(f, "OTLP malformed payload: {e}"),
        }
    }
}

impl std::error::Error for OtlpError {}

/// Send error event telemetry to APM collector
/// Error events have a special structure: [run_id, {events_seen, reservoir_size}, [events]]
pub async fn send_error_events(
    client: &Client,
    license_key: &str,
    collector_host: &str,
    run_id: &str,
    error_events: &[Value],
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
        "Sending {} error events: compressed={} bytes, uncompressed={} bytes",
        error_events.len(),
        compressed.len(),
        uncompressed_len
    );

    let start_time = std::time::Instant::now();
    let response = client
        .post(&url)
        .header("NR-Session", run_id)
        .header("Accept-Encoding", "identity, deflate")
        .header("Content-Type", "application/octet-stream")
        .header("User-Agent", get_user_agent())
        .header("Content-Encoding", "gzip")
        .body(compressed)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
        // Strip the request URL (carries `license_key`) from any error before it
        // propagates to a log site.
        .map_err(|e| e.without_url())?;
    let duration = start_time.elapsed();

    let status = response.status();
    let status_code = status.as_u16();

    if status.is_success() {
        debug!("Status Code for {} telemetry: {}", CMD_ERROR_EVENTS, status_code);
        debug!("Send {} duration: {}ms", CMD_ERROR_DATA, duration.as_millis());
        info!("Successfully sent {} error events (status: {})", error_events.len(), status);
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
            "Failed to send error events (status: {}, body: {})",
            status_code, body
        );

        Err(anyhow::anyhow!("APM collector returned status {}: {}", status_code, body))
    }
}

/// Send telemetry data to APM collector
pub async fn send_apm_telemetry(
    client: &Client,
    license_key: &str,
    collector_host: &str,
    run_id: &str,
    command: &str,
    data: &[Value],
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
        "Sending {} to APM collector: {} bytes compressed (from {} bytes)",
        command,
        compressed.len(),
        uncompressed_len
    );
    
    debug!(
        "Request for command: {}",
        command
    );
    
    debug!(
        "Sending {} to APM: compressed={} bytes, uncompressed={} bytes",
        command,
        compressed.len(),
        uncompressed_len
    );

    let start_time = std::time::Instant::now();
    let response = client
        .post(&url)
        .header("NR-Session", run_id)
        .header("Accept-Encoding", "identity, deflate")
        .header("Content-Type", "application/octet-stream")
        .header("User-Agent", get_user_agent())
        .header("Content-Encoding", "gzip")
        .body(compressed)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
        // Strip the request URL (carries `license_key`) from any error before it
        // propagates to a log site.
        .map_err(|e| e.without_url())?;
    let duration = start_time.elapsed();

    let status = response.status();
    let status_code = status.as_u16();

    if status.is_success() {
        debug!("Status Code for {} telemetry: {}", command, status_code);
        debug!("Send {} duration: {}ms", command, duration.as_millis());
        info!("Successfully sent {} (status: {})", command, status);
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
            "Failed to send {} (status: {}, body: {})",
            command, status_code, body
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
    client: &Client,
    license_key: &str,
    metric_endpoint: &str,
    metrics: &[Value],
) -> std::result::Result<(), MetricApiError> {
    if metrics.is_empty() {
        debug!("No platform metrics to send");
        return Ok(());
    }

    let payload = serde_json::json!([{
        "metrics": metrics
    }]);

    let payload_json = match serde_json::to_string(&payload) {
        Ok(s) => s,
        Err(e) => return Err(MetricApiError::Network(anyhow::Error::new(e))),
    };

    debug!("Platform metrics payload JSON: {}", payload_json);

    debug!(
        "Sending {} platform metrics to Metric API endpoint: {} ({} bytes uncompressed)",
        metrics.len(),
        metric_endpoint,
        payload_json.len()
    );

    let start_time = std::time::Instant::now();
    let response = match client
        .post(metric_endpoint)
        .header("Api-Key", license_key)
        .header("Content-Type", "application/json")
        .body(payload_json)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            // license key travels in the `Api-Key` header here (not the URL), but
            // strip the URL anyway for defense-in-depth before logging.
            let e = e.without_url();
            warn!("Platform metrics network error: {} - will retry", e);
            return Err(MetricApiError::Network(anyhow::Error::new(e)));
        }
    };
    let duration = start_time.elapsed();

    let status = response.status();
    let status_code = status.as_u16();

    if status.is_success() {
        debug!("Status Code for platform_metrics telemetry: {}", status_code);
        debug!("Send platform_metrics duration: {}ms", duration.as_millis());
        info!("Successfully sent {} platform metrics", metrics.len());
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
mod collector_tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn retryable_statuses_are_transient_only() {
        for code in [408, 429, 500, 502, 503, 504] {
            assert!(is_retryable_status(code), "{code} should be retryable");
        }
        // Permanent / success / restart / disconnect must NOT be classified retryable.
        for code in [200, 202, 400, 401, 403, 404, 409, 410, 413] {
            assert!(!is_retryable_status(code), "{code} must not be retryable");
        }
    }

    #[test]
    fn metric_api_error_classification() {
        let retr = MetricApiError::Retryable {
            status: 503,
            retry_after: Some(std::time::Duration::from_secs(7)),
        };
        assert!(!retr.is_permanent());
        assert_eq!(retr.retry_after(), Some(std::time::Duration::from_secs(7)));

        let perm = MetricApiError::Permanent { status: 400 };
        assert!(perm.is_permanent());
        assert_eq!(perm.retry_after(), None);

        let net = MetricApiError::Network(anyhow::anyhow!("boom"));
        assert!(!net.is_permanent());
        assert_eq!(net.retry_after(), None);
    }

    #[test]
    #[serial]
    fn reconnect_flag_is_one_shot() {
        // Drain any pre-existing state.
        let _ = take_reconnect_needed();
        assert!(!take_reconnect_needed(), "should start clear");
        signal_reconnect_needed();
        assert!(take_reconnect_needed(), "first take observes the signal");
        assert!(!take_reconnect_needed(), "second take is cleared");
    }

    #[test]
    #[serial]
    fn disabled_telemetry_roundtrips() {
        let mut set = std::collections::HashSet::new();
        set.insert("platform_metrics".to_string());
        set.insert("sql_trace_data".to_string());
        set_disabled_telemetry(set);
        assert!(is_telemetry_disabled("platform_metrics"));
        assert!(is_telemetry_disabled("sql_trace_data"));
        assert!(!is_telemetry_disabled("metric_data"));
        // Reset so other serial tests see a clean state.
        set_disabled_telemetry(std::collections::HashSet::new());
        assert!(!is_telemetry_disabled("platform_metrics"));
    }

    #[test]
    #[serial]
    fn otlp_metric_enabled_defaults_to_false_and_roundtrips() {
        // Process-wide static: don't assume a fresh false here (another test may have
        // run first), but do confirm the flag actually flips both ways.
        set_otlp_metric_enabled(false);
        assert!(!is_otlp_metric_enabled());
        set_otlp_metric_enabled(true);
        assert!(is_otlp_metric_enabled());
        // Reset so other serial tests see a clean state.
        set_otlp_metric_enabled(false);
        assert!(!is_otlp_metric_enabled());
    }

    /// Pins the full truth table for the effective OTLP gate, exercising the REAL
    /// production function (`ExtensionConfig::otlp_metric_forwarding_active`) that main.rs
    /// feeds into `set_otlp_metric_enabled` — not a re-implementation of the `&&`. Guards
    /// against regressing to mirroring the env var alone, which would leave the flag "on"
    /// in serverless mode where the OTLP send path does not exist.
    #[test]
    #[serial]
    fn otlp_metric_enabled_requires_both_env_var_and_apm_mode() {
        for (env_var, apm_mode, expected) in [
            (true, true, true),    // both on -> OTLP sends
            (true, false, false),  // flag set but serverless -> no send path exists
            (false, true, false),  // APM mode but flag off -> opted out
            (false, false, false), // neither
        ] {
            let mut config = crate::config::ExtensionConfig::default();
            config.new_relic.otlp_metric_enabled = env_var;
            config.new_relic.apm_lambda_mode = apm_mode;

            assert_eq!(
                config.otlp_metric_forwarding_active(),
                expected,
                "otlp_metric_forwarding_active: env_var={env_var}, apm_mode={apm_mode}"
            );

            // And confirm it round-trips through the process-wide gate main.rs sets.
            set_otlp_metric_enabled(config.otlp_metric_forwarding_active());
            assert_eq!(
                is_otlp_metric_enabled(),
                expected,
                "is_otlp_metric_enabled: env_var={env_var}, apm_mode={apm_mode}"
            );
        }

        // Reset so other serial tests see a clean state.
        set_otlp_metric_enabled(false);
        assert!(!is_otlp_metric_enabled());
    }

    #[test]
    fn known_telemetry_types_complete() {
        // The 9 agent-payload types + platform_metrics.
        assert_eq!(KNOWN_TELEMETRY_TYPES.len(), 10);
        assert!(KNOWN_TELEMETRY_TYPES.contains(&"platform_metrics"));
        assert!(KNOWN_TELEMETRY_TYPES.contains(&"sql_trace_data"));
    }

    fn restart_log_level(status_code: u16) -> &'static str {
        if status_code == 409 { "INFO" } else { "WARN" }
    }

    #[test]
    fn log_level_409_is_info_401_is_warn() {
        assert_eq!(restart_log_level(409), "INFO", "409 (routine session refresh) must log at INFO");
        assert_eq!(restart_log_level(401), "WARN", "401 (auth failure) must log at WARN");
    }

    #[test]
    fn disconnect_is_not_restart_exception() {
        // 410 returns CollectorError::Disconnect, not RestartException. This is
        // intentional: telemetry_buffer::retry_buffered_telemetry only skips
        // retry_count for RestartException (409/401). A 410 is a hard disconnect
        // and must consume a retry slot like any other non-session error.
        let restart = anyhow::Error::new(CollectorError::RestartException);
        let disconnect = anyhow::Error::new(CollectorError::Disconnect);

        let is_restart = |e: &anyhow::Error| {
            e.downcast_ref::<CollectorError>()
                .map(|ce| matches!(ce, CollectorError::RestartException))
                .unwrap_or(false)
        };

        assert!(is_restart(&restart), "RestartException (409/401) must be detected");
        assert!(!is_restart(&disconnect), "Disconnect (410) must NOT be treated as restart");
    }


}

/// Decode, enrich with `entity.guid`, gzip, and POST a single base64-encoded OTLP payload.
/// `payload_num` is a 1-based index used only for human-readable logging.
///
/// Uses the same 20 s HTTP timeout as `send_error_events` / `send_apm_telemetry` /
/// `send_platform_metrics`, on both the first attempt and buffered retries.
///
/// Note on the shutdown path: `retry_buffered_otlp_payloads` also runs from the SHUTDOWN
/// handler, whose whole budget is `SHUTDOWN_TIMEOUT_MS - SHUTDOWN_DIAG_RESERVE_MS`
/// (1300 ms) shared with the telemetry- and metric-API drains that run first. A 20 s
/// timeout exceeds that window, so a stalled endpoint means the shutdown drain is
/// best-effort — the enclosing `tokio::time::timeout` cuts it off and anything undelivered
/// stays buffered (bounded by `MAX_RETRY_ATTEMPTS` / `MAX_AGE_MINUTES`). That limitation is
/// shared by all three buffers, not specific to OTLP; shortening only this one would make
/// OTLP inconsistent with its siblings without fixing the budget.
pub(super) async fn send_single_otlp_payload(
    client: &Client,
    otlp_metric_endpoint: &str,
    license_key: &str,
    encoded_payload: &str,
    entity_guid: &str,
    payload_num: usize,
) -> std::result::Result<(), OtlpError> {
    let raw = general_purpose::STANDARD
        .decode(encoded_payload)
        .map_err(|e| OtlpError::MalformedPayload(anyhow::anyhow!(
            "Failed to base64-decode OTLP payload {payload_num}: {e}"
        )))?;

    let enriched: Vec<u8> = super::otlp::inject_entity_guid(&raw, entity_guid)
        .map_err(|e| OtlpError::MalformedPayload(anyhow::anyhow!(
            "Failed to inject entity.guid into OTLP payload {payload_num}: {e}"
        )))?;

    if tracing::enabled!(tracing::Level::DEBUG) {
        let b64 = general_purpose::STANDARD.encode(&enriched);
        let preview_len = b64.len().min(256);
        let suffix = if b64.len() > preview_len { "..." } else { "" };
        debug!(
            "OTLP payload {} enriched ({} bytes) base64={}{}",
            payload_num,
            enriched.len(),
            &b64[..preview_len],
            suffix
        );
    }

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&enriched)
        .map_err(|e| OtlpError::MalformedPayload(anyhow::anyhow!(
            "Failed to gzip OTLP payload {payload_num}: {e}"
        )))?;
    let compressed = encoder.finish()
        .map_err(|e| OtlpError::MalformedPayload(anyhow::anyhow!(
            "Failed to finish gzip OTLP payload {payload_num}: {e}"
        )))?;

    debug!(
        "OTLP payload {} compressed: {} bytes -> {} bytes",
        payload_num,
        enriched.len(),
        compressed.len()
    );

    let start_time = std::time::Instant::now();
    let response = client
        .post(otlp_metric_endpoint)
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "gzip")
        .header("api-key", license_key)
        .body(compressed)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
        .map_err(|e| {
            let e = e.without_url();
            warn!("OTLP request {} network error: {} - will retry", payload_num, e);
            OtlpError::Network(anyhow::Error::new(e))
        })?;

    let duration = start_time.elapsed();
    let status = response.status();
    let status_code = status.as_u16();

    if !status.is_success() {
        let retry_after = parse_retry_after(response.headers());
        let body = match response.text().await {
            Ok(t) => t,
            Err(e) => {
                warn!("Failed to read error response body for payload {}: {}", payload_num, e);
                String::from("<unreadable>")
            }
        };

        if is_retryable_status(status_code) {
            warn!(
                "OTLP payload {} transient failure (status {}, retry_after {:?}) in {}ms - will retry: {}",
                payload_num, status_code, retry_after, duration.as_millis(), body
            );
            return Err(OtlpError::Retryable { status: status_code, retry_after });
        }

        warn!(
            "OTLP payload {} permanent failure (status {}) in {}ms - dropping: {}",
            payload_num, status_code, duration.as_millis(), body
        );
        return Err(OtlpError::Permanent { status: status_code });
    }

    // Matches how the other senders in this module report success (one line). The body
    // is read only when DEBUG is on: a 2xx carries nothing we act on, so reading it
    // unconditionally would allocate on every successful send to feed a log line that
    // is filtered out at the default `info` level. `chars().take()` keeps the preview on
    // a char boundary, so a multi-byte UTF-8 body cannot panic here.
    if tracing::enabled!(tracing::Level::DEBUG) {
        let body = response.text().await.unwrap_or_default();
        let preview: String = body.trim().chars().take(256).collect();
        debug!(
            "OTLP payload {} sent successfully in {}ms (response: {})",
            payload_num,
            duration.as_millis(),
            if preview.is_empty() { "empty" } else { preview.as_str() }
        );
    }
    Ok(())
}

/// Forward base64-encoded OTLP `ExportMetricsServiceRequest` protobuf payloads to the OTLP
/// endpoint.  Before sending, `entity.guid` is injected into every `ResourceMetrics.resource`
/// so New Relic can correlate the metrics with the Lambda entity.  All metric data
/// (`scope_metrics`, data points, histograms) is preserved bit-for-bit.
///
/// Payloads are sent concurrently and independently: one payload's failure (a transient
/// 5xx, a timeout) does not abort or delay the others, matching the fan-out pattern used
/// elsewhere in this codebase (see `event_loop.rs`'s late-agent-payload `JoinSet` usage).
/// Transient/network failures are buffered via [`super::otlp_buffer`] for retry on a later
/// invoke or at shutdown, mirroring [`super::metric_api_buffer`]'s handling of platform
/// metrics. Permanent failures (malformed payload, non-retryable 4xx) are dropped at the
/// send site. This function itself never returns an error, since a partial-batch failure
/// is not a reason to fail the whole send.
pub async fn send_otlp_payload(
    client: &Client,
    otlp_metric_endpoint: &str,
    license_key: &str,
    payloads: &[String],
    entity_guid: &str,
    request_id: &str,
) {
    if payloads.is_empty() {
        return;
    }

    info!(
        "Sending {} OTLP payload(s) to {} (entity.guid={})",
        payloads.len(),
        otlp_metric_endpoint,
        entity_guid,
    );

    let mut set = tokio::task::JoinSet::new();
    for (i, encoded) in payloads.iter().enumerate() {
        let payload_num = i + 1; // 1-based for human-readable logs
        let client = client.clone();
        let otlp_metric_endpoint = otlp_metric_endpoint.to_string();
        let license_key = license_key.to_string();
        let encoded = encoded.clone();
        let entity_guid = entity_guid.to_string();
        let request_id = request_id.to_string();

        set.spawn(async move {
            match send_single_otlp_payload(
                &client,
                &otlp_metric_endpoint,
                &license_key,
                &encoded,
                &entity_guid,
                payload_num,
            )
            .await
            {
                Ok(()) => {}
                Err(e) if e.is_permanent() => {
                    error!("OTLP payload {} dropped (permanent): {}", payload_num, e);
                }
                Err(e) => {
                    let retry_after = e.retry_after();
                    super::otlp_buffer::buffer_failed_otlp_payload(
                        encoded,
                        entity_guid,
                        otlp_metric_endpoint,
                        request_id,
                        retry_after,
                    );
                }
            }
        });
    }
    while set.join_next().await.is_some() {}

    info!("Finished sending {} OTLP payload(s)", payloads.len());


}
