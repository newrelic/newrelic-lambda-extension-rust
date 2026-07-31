// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Buffer for failed telemetry in APM mode
//!
//! Stores individual telemetry types that fail to send to the APM collector
//! and retries them in subsequent invocations or during shutdown.

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tracing::{debug, error, warn};

/// Sentinel telemetry type for *synthesized* error events (timeout/fault errors).
/// These use the `send_error_events` wire format (`[run_id, {meta}, [events]]`),
/// which differs from agent-originated `error_event_data` that flows through
/// `send_apm_telemetry`. Routed specially in the retry loop.
pub const SYNTHESIZED_ERROR_EVENTS: &str = "__synthesized_error_event_data";

/// Hard cap on buffered telemetry items to bound memory during a sustained
/// collector failure on a high-traffic function. When full, the oldest is evicted.
const MAX_BUFFERED_ITEMS: usize = 500;

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
        // Bound memory: evict oldest if at capacity (prefer keeping fresher telemetry).
        if buffer.len() >= MAX_BUFFERED_ITEMS {
            buffer.remove(0);
            warn!("Telemetry buffer full ({}) - evicted oldest item", MAX_BUFFERED_ITEMS);
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

/// Retry all buffered telemetry.
///
/// `current_run_id`/`current_collector_host`, when supplied, OVERRIDE the values
/// captured when the item was buffered. This is essential after a reconnect: the
/// buffered `run_id` is stale (the collector expired it / issued a restart), so
/// retrying with it would fail forever. Passing the live session's identifiers
/// lets buffered items succeed against the fresh connection. When `None` (e.g.
/// not connected), the stored values are used as a best-effort fallback.
pub async fn retry_buffered_telemetry(
    client: &reqwest::Client,
    license_key: &str,
    current_run_id: Option<&str>,
    current_collector_host: Option<&str>,
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

        let run_id = current_run_id.unwrap_or(item.run_id.as_str());
        let collector_host = current_collector_host.unwrap_or(item.collector_host.as_str());

        // Synthesized error events use a different wire format and send function.
        let result = if item.telemetry_type == SYNTHESIZED_ERROR_EVENTS {
            super::collector::send_error_events(
                client,
                license_key,
                collector_host,
                run_id,
                &item.data,
            )
            .await
        } else {
            let command = match item.telemetry_type.as_str() {
                "metric_data" => super::collector::CMD_METRICS,
                "span_event_data" => super::collector::CMD_SPAN_EVENTS,
                "error_data" => super::collector::CMD_ERROR_DATA,
                "error_event_data" => super::collector::CMD_ERROR_EVENTS,
                "analytic_event_data" => super::collector::CMD_ANALYTIC_EVENTS,
                "custom_event_data" => super::collector::CMD_CUSTOM_EVENTS,
                "log_event_data" => super::collector::CMD_LOG_EVENTS,
                "transaction_sample_data" => super::collector::CMD_TRANSACTION_SAMPLES,
                "sql_trace_data" => super::collector::CMD_SLOW_SQLS,
                _ => {
                    warn!("Unknown telemetry type: {}", item.telemetry_type);
                    continue;
                }
            };
            super::collector::send_apm_telemetry(
                client,
                license_key,
                collector_host,
                run_id,
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

/// Distinct request_ids that still have un-sent buffered telemetry. Used by the
/// shutdown summary to report how many invocations' data was dropped.
pub fn buffered_request_ids() -> Vec<String> {
    let mut ids: Vec<String> = FAILED_TELEMETRY_BUFFER
        .lock()
        .map(|buffer| buffer.iter().map(|item| item.request_id.clone()).collect())
        .unwrap_or_default();
    ids.sort();
    ids.dedup();
    ids
}

#[cfg(test)]
#[path = "telemetry_buffer_tests.rs"]
mod telemetry_buffer_tests;
