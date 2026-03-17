//! APM collector client for sending telemetry
//!
//! Based on collector.go CollectorRequest() and SendAPMTelemetry()

use anyhow::Result;
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

const PROTOCOL_VERSION: u8 = 17;
const USER_AGENT: &str = concat!("NewRelic-Rust-Lambda-Extension/", env!("CARGO_PKG_VERSION"));

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
        .header("User-Agent", USER_AGENT)
        .header("Content-Encoding", "gzip")
        .body(compressed)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await?;
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
            return Err(anyhow::Error::new(CollectorError::Disconnect)
                .context(format!("Collector returned 410 for {}", CMD_ERROR_EVENTS)));
        } else if status_code == 401 || status_code == 409 {
            warn!("APM collector restart exception ({}) - reconnection needed", status_code);
            return Err(anyhow::Error::new(CollectorError::RestartException)
                .context(format!("Collector returned {} for {}", status_code, CMD_ERROR_EVENTS)));
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
        processed_data[0] = serde_json::json!(run_id);
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
        .header("User-Agent", USER_AGENT)
        .header("Content-Encoding", "gzip")
        .body(compressed)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await?;
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
            return Err(anyhow::Error::new(CollectorError::Disconnect)
                .context(format!("Collector returned 410 for {}", command)));
        } else if status_code == 401 || status_code == 409 {
            warn!("APM collector restart exception ({}) - reconnection needed", status_code);
            return Err(anyhow::Error::new(CollectorError::RestartException)
                .context(format!("Collector returned {} for {}", status_code, command)));
        }
        
        warn!(
            "Failed to send {} (status: {}, body: {})",
            command, status_code, body
        );

        Err(anyhow::anyhow!("APM collector returned status {}: {}", status_code, body))
    }
}

/// Send platform metrics to Metric API
pub async fn send_platform_metrics(
    client: &Client,
    license_key: &str,
    metric_endpoint: &str,
    metrics: Vec<Value>,
) -> Result<()> {
    if metrics.is_empty() {
        debug!("No platform metrics to send");
        return Ok(());
    }

    let payload = serde_json::json!([{
        "metrics": metrics
    }]);

    let payload_json = serde_json::to_string(&payload)?;
    
    debug!("Platform metrics payload JSON: {}", payload_json);

    debug!(
        "Sending {} platform metrics to Metric API endpoint: {} ({} bytes uncompressed)",
        metrics.len(),
        metric_endpoint,
        payload_json.len()
    );

    let start_time = std::time::Instant::now();
    let response = client
        .post(metric_endpoint)
        .header("Api-Key", license_key)
        .header("Content-Type", "application/json")
        .body(payload_json)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await?;
    let duration = start_time.elapsed();

    let status = response.status();
    let status_code = status.as_u16();

    if status.is_success() {
        debug!("Status Code for platform_metrics telemetry: {}", status_code);
        debug!("Send platform_metrics duration: {}ms", duration.as_millis());
        info!("Successfully sent {} platform metrics", metrics.len());
        Ok(())
    } else {
        let body = response.text().await.unwrap_or_default();
        debug!("Status Code for platform_metrics telemetry: {}", status_code);
        warn!(
            "Failed to send platform metrics: {} - {}",
            status_code, body
        );
        Err(anyhow::anyhow!("Metric API returned status {}", status_code))
    }
}
