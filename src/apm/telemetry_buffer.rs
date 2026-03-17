//! Buffer for failed telemetry in APM mode
//!
//! Stores individual telemetry types that fail to send to the APM collector
//! and retries them in subsequent invocations or during shutdown.

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use serde_json::Value;
use std::collections::VecDeque;
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
pub static FAILED_TELEMETRY_BUFFER: Lazy<Arc<Mutex<VecDeque<FailedTelemetry>>>> =
    Lazy::new(|| Arc::new(Mutex::new(VecDeque::new())));

/// Add failed telemetry to buffer.
/// When the buffer is at capacity, the oldest entry is popped and sent to NR
/// in a background task rather than being silently discarded.
pub fn buffer_failed_telemetry(
    telemetry_type: String,
    data: Vec<Value>,
    request_id: String,
    run_id: String,
    collector_host: String,
    client: reqwest::Client,
    license_key: String,
) {
    if let Ok(mut buffer) = FAILED_TELEMETRY_BUFFER.lock() {
        const MAX_BUFFERED_TELEMETRY: usize = 50;
        if buffer.len() >= MAX_BUFFERED_TELEMETRY {
            if let Some(evicted) = buffer.pop_front() {
                warn!(
                    "APM mode: Telemetry buffer at capacity ({}) - sending oldest entry ({} for request {}) to NR in background",
                    MAX_BUFFERED_TELEMETRY, evicted.telemetry_type, evicted.request_id
                );
                let evicted_client = client.clone();
                let evicted_license_key = license_key.clone();
                tokio::spawn(async move {
                    send_evicted_telemetry(evicted, &evicted_client, &evicted_license_key).await;
                });
            }
        }
        debug!(
            "APM mode: Buffered failed {} for request {} (total buffered: {})",
            telemetry_type,
            request_id,
            buffer.len() + 1
        );
        buffer.push_back(FailedTelemetry {
            telemetry_type,
            data,
            request_id,
            run_id,
            collector_host,
            failed_at: Utc::now(),
            retry_count: 0,
        });
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
                        buffer.push_back(item);
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

/// Send an evicted telemetry item to NR as a last-chance attempt before it's lost
async fn send_evicted_telemetry(
    item: FailedTelemetry,
    client: &reqwest::Client,
    license_key: &str,
) {
    debug!(
        "Attempting last-chance send of evicted {} for request {}",
        item.telemetry_type, item.request_id
    );

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
                warn!("Unknown evicted telemetry type: {} - discarding", item.telemetry_type);
                return;
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
            debug!(
                "Successfully sent evicted {} for request {}",
                item.telemetry_type, item.request_id
            );
        }
        Err(e) => {
            error!(
                "Failed last-chance send of evicted {} for request {}: {} - data lost",
                item.telemetry_type, item.request_id, e
            );
        }
    }
}
