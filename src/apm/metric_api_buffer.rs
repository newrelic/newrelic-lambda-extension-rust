// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Retry buffer for platform metrics sent to the Metric API (APM mode).
//!
//! Distinct from [`super::telemetry_buffer`], which retries APM-collector
//! telemetry keyed by `run_id`/`collector_host`. The Metric API authenticates
//! with the license key only (no `run_id`), so failed sends are buffered with
//! just the metric payload and endpoint and retried on later invocations and at
//! shutdown. Transient failures (5xx/429/408/network) are buffered; permanent
//! failures (other 4xx) are dropped at the send site and never reach here.

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, error, warn};

/// Maximum retry attempts before a metric batch is dropped.
const MAX_RETRY_ATTEMPTS: usize = 10;
/// Maximum age before a buffered batch is dropped (Lambda containers are short-lived).
const MAX_AGE_MINUTES: i64 = 60;
/// Hard cap on buffered batches to bound memory during a sustained Metric API
/// failure on a high-traffic function. When full, the oldest batch is evicted.
const MAX_BUFFERED_BATCHES: usize = 1000;

/// A platform-metric batch that failed to send and must be retried.
#[derive(Debug, Clone)]
pub struct FailedMetricApi {
    pub metrics: Vec<Value>,
    pub endpoint: String,
    pub failed_at: DateTime<Utc>,
    pub retry_count: usize,
    /// Earliest time to retry, honoring a `Retry-After` header when present.
    pub next_retry_at: Option<DateTime<Utc>>,
}

/// Global buffer for failed Metric API sends (APM mode only).
pub static FAILED_METRIC_API_BUFFER: Lazy<Arc<Mutex<Vec<FailedMetricApi>>>> =
    Lazy::new(|| Arc::new(Mutex::new(Vec::new())));

/// Buffer a failed platform-metric batch for retry.
pub fn buffer_failed_metric_api(metrics: Vec<Value>, endpoint: String, retry_after: Option<Duration>) {
    if metrics.is_empty() {
        return;
    }
    let next_retry_at = retry_after.and_then(|d| {
        chrono::Duration::from_std(d)
            .ok()
            .map(|cd| Utc::now() + cd)
    });
    let item = FailedMetricApi {
        metrics,
        endpoint,
        failed_at: Utc::now(),
        retry_count: 0,
        next_retry_at,
    };
    if let Ok(mut buffer) = FAILED_METRIC_API_BUFFER.lock() {
        // Bound memory: evict oldest if at capacity (prefer keeping fresher metrics).
        if buffer.len() >= MAX_BUFFERED_BATCHES {
            buffer.remove(0);
            warn!("Metric API buffer full ({}) - evicted oldest batch", MAX_BUFFERED_BATCHES);
        }
        buffer.push(item);
        debug!(
            "APM mode: Buffered failed platform metrics (total buffered: {})",
            buffer.len()
        );
    } else {
        error!("Failed to lock metric API buffer - platform metrics lost!");
    }
}

/// Retry all buffered platform-metric batches.
pub async fn retry_buffered_metric_api(client: &reqwest::Client, license_key: &str) {
    let buffered = {
        if let Ok(mut buffer) = FAILED_METRIC_API_BUFFER.lock() {
            std::mem::take(&mut *buffer)
        } else {
            error!("Failed to lock metric API buffer for retry");
            return;
        }
    };

    if buffered.is_empty() {
        return;
    }

    debug!(
        "APM mode: Retrying {} buffered platform-metric batch(es)",
        buffered.len()
    );

    let now = Utc::now();
    let mut success = 0;
    let mut still_failed = 0;

    for mut item in buffered {
        // Honor Retry-After: re-buffer untouched if the backoff window hasn't elapsed.
        if let Some(next) = item.next_retry_at {
            if now < next {
                re_buffer(item);
                continue;
            }
        }

        // Age-out.
        if now.signed_duration_since(item.failed_at).num_minutes() > MAX_AGE_MINUTES {
            warn!(
                "Dropping platform metrics too old ({} min) after {} attempt(s)",
                now.signed_duration_since(item.failed_at).num_minutes(),
                item.retry_count
            );
            continue;
        }

        item.retry_count += 1;

        match super::collector::send_platform_metrics(
            client,
            license_key,
            &item.endpoint,
            &item.metrics,
        )
        .await
        {
            Ok(()) => {
                success += 1;
                debug!("Successfully retried platform metrics (attempt {})", item.retry_count);
            }
            Err(e) if e.is_permanent() => {
                still_failed += 1;
                warn!("Dropping platform metrics after permanent error on retry: {}", e);
            }
            Err(e) => {
                still_failed += 1;
                if item.retry_count < MAX_RETRY_ATTEMPTS {
                    item.next_retry_at = e.retry_after().and_then(|d| {
                        chrono::Duration::from_std(d).ok().map(|cd| Utc::now() + cd)
                    });
                    re_buffer(item);
                } else {
                    error!(
                        "Dropping platform metrics after {} retry attempts: {}",
                        item.retry_count, e
                    );
                }
            }
        }
    }

    if success > 0 || still_failed > 0 {
        debug!(
            "APM metric API retry results: {} successful, {} still failed",
            success, still_failed
        );
    }
}

fn re_buffer(item: FailedMetricApi) {
    if let Ok(mut buffer) = FAILED_METRIC_API_BUFFER.lock() {
        buffer.push(item);
    }
}

/// Count of buffered metric batches (for monitoring/shutdown logging).
pub fn get_metric_api_buffer_count() -> usize {
    FAILED_METRIC_API_BUFFER
        .lock()
        .map(|b| b.len())
        .unwrap_or(0)
}

/// Distinct request_ids represented in the buffered platform metrics. Read from the
/// `aws.requestId` attribute the extension stamps on each metric. Used so the shutdown
/// drop summary's "invocations affected" count includes metric-only requests (which
/// otherwise show up in the item count but not the invocation count).
pub fn buffered_request_ids() -> Vec<String> {
    let mut ids: Vec<String> = FAILED_METRIC_API_BUFFER
        .lock()
        .map(|buf| {
            buf.iter()
                .flat_map(|item| item.metrics.iter())
                .filter_map(|m| {
                    m.get("attributes")?
                        .get("aws.requestId")?
                        .as_str()
                        .map(String::from)
                })
                .collect()
        })
        .unwrap_or_default();
    ids.sort();
    ids.dedup();
    ids
}

#[cfg(test)]
#[path = "metric_api_buffer_tests.rs"]
mod metric_api_buffer_tests;
