//! Buffer for failed telemetry in APM mode
//!
//! Stores individual telemetry types that fail to send to the APM collector
//! and retries them in subsequent invocations or during shutdown.

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tracing::{debug, error, warn};

/// Failed telemetry data that needs to be retried
#[derive(Debug, Clone)]
pub struct FailedTelemetry {
    pub telemetry_type: String,
    pub data: Vec<Value>,
    pub request_id: String,
    pub run_id: String,
    pub collector_host: String,
    pub failed_at: DateTime<Utc>,
    pub retry_count: usize,
}

/// Global buffer for failed telemetry (APM mode only)
pub static FAILED_TELEMETRY_BUFFER: Lazy<Arc<Mutex<Vec<FailedTelemetry>>>> =
    Lazy::new(|| Arc::new(Mutex::new(Vec::new())));

/// Add failed telemetry to buffer
pub fn buffer_failed_telemetry(
    telemetry_type: String,
    data: Vec<Value>,
    request_id: String,
    run_id: String,
    collector_host: String,
) {
    let failed_telemetry = FailedTelemetry {
        telemetry_type: telemetry_type.clone(),
        data,
        request_id: request_id.clone(),
        run_id,
        collector_host,
        failed_at: Utc::now(),
        retry_count: 0,
    };

    if let Ok(mut buffer) = FAILED_TELEMETRY_BUFFER.lock() {
        const MAX_BUFFERED_TELEMETRY: usize = 50;
        if buffer.len() >= MAX_BUFFERED_TELEMETRY {
            warn!("APM mode: Telemetry buffer at capacity ({}) - dropping oldest entry", MAX_BUFFERED_TELEMETRY);
            buffer.remove(0);
        }
        buffer.push(failed_telemetry);
        debug!(
            "APM mode: Buffered failed {} for request {} (total buffered: {})",
            telemetry_type,
            request_id,
            buffer.len()
        );
    } else {
        error!("Failed to lock telemetry buffer - data lost!");
    }
}

/// Retry all buffered telemetry
pub async fn retry_buffered_telemetry(
    client: &reqwest::Client,
    license_key: &str,
) {
    let failed_telemetry = {
        if let Ok(mut buffer) = FAILED_TELEMETRY_BUFFER.lock() {
            std::mem::take(&mut *buffer)
        } else {
            error!("Failed to lock telemetry buffer for retry");
            return;
        }
    };

    if failed_telemetry.is_empty() {
        debug!("No failed telemetry to retry");
        return;
    }

    debug!(
        "APM mode: Retrying {} buffered telemetry item(s)",
        failed_telemetry.len()
    );

    let mut retry_success_count = 0;
    let mut retry_failed_count = 0;

    for mut item in failed_telemetry {
        item.retry_count += 1;

        // Check age - drop if older than 1 hour (Lambda container lifecycle is short)
        let age = Utc::now().signed_duration_since(item.failed_at);
        if age.num_minutes() > 60 {
            warn!(
                "Dropping {} telemetry that's too old ({} minutes) for request {}",
                item.telemetry_type,
                age.num_minutes(),
                item.request_id
            );
            continue;
        }

        debug!(
            "Retrying {} for request {} (attempt {})",
            item.telemetry_type, item.request_id, item.retry_count
        );

        // Retry sending
        let result = if item.telemetry_type == "error_event_data" {
            super::collector::send_error_events(
                client,
                license_key,
                &item.collector_host,
                &item.run_id,
                &item.data,
            )
            .await
        } else {
            let command = match item.telemetry_type.as_str() {
                "metric_data" => super::collector::CMD_METRICS,
                "span_event_data" => super::collector::CMD_SPAN_EVENTS,
                "error_data" => super::collector::CMD_ERROR_DATA,
                "analytic_event_data" => super::collector::CMD_ANALYTIC_EVENTS,
                "custom_event_data" => super::collector::CMD_CUSTOM_EVENTS,
                "log_event_data" => super::collector::CMD_LOG_EVENTS,
                "transaction_sample_data" => super::collector::CMD_TRANSACTION_SAMPLES,
                _ => {
                    warn!("Unknown telemetry type: {}", item.telemetry_type);
                    continue;
                }
            };

            super::collector::send_apm_telemetry(
                client,
                license_key,
                &item.collector_host,
                &item.run_id,
                command,
                &item.data,
            )
            .await
        };

        match result {
            Ok(()) => {
                retry_success_count += 1;
                debug!(
                    "Successfully retried {} for request {}",
                    item.telemetry_type, item.request_id
                );
            }
            Err(e) => {
                retry_failed_count += 1;
                warn!(
                    "Failed to retry {} for request {}: {}",
                    item.telemetry_type, item.request_id, e
                );

                // Put back in buffer for next retry (unless too many attempts)
                if item.retry_count < 10 {
                    if let Ok(mut buffer) = FAILED_TELEMETRY_BUFFER.lock() {
                        buffer.push(item);
                    }
                } else {
                    error!(
                        "Dropping {} after {} retry attempts for request {}",
                        item.telemetry_type, item.retry_count, item.request_id
                    );
                }
            }
        }
    }

    if retry_success_count > 0 || retry_failed_count > 0 {
        debug!(
            "APM telemetry retry results: {} successful, {} still failed",
            retry_success_count, retry_failed_count
        );
    }
}

/// Get count of buffered telemetry for monitoring
pub fn get_buffer_count() -> usize {
    FAILED_TELEMETRY_BUFFER
        .lock()
        .map(|buffer| buffer.len())
        .unwrap_or(0)
}
