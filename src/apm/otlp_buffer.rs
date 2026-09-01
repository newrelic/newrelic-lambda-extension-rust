// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Retry buffer for OTLP metrics payloads sent to the OTLP endpoint (APM mode).
//!
//! Distinct from [`super::telemetry_buffer`], which retries APM-collector telemetry
//! keyed by `run_id`/`collector_host`. Like [`super::metric_api_buffer`], the OTLP
//! endpoint authenticates with the license key only (no `run_id`), so failed sends
//! are buffered with just the payload, endpoint, and `entity.guid` and retried on
//! later invocations and at shutdown. Transient failures (5xx/429/408/network) are
//! buffered; permanent failures (malformed payload, other 4xx) are dropped at the
//! send site and never reach here.
//!
//! Buffered per-payload (not per-batch): `send_otlp_payload` already decodes,
//! enriches, and sends each base64 entry independently, so a failure only affects
//! the one payload that failed, not its batch-mates.

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, error, warn};

/// Maximum retry attempts before an OTLP payload is dropped.
const MAX_RETRY_ATTEMPTS: usize = 10;
/// Maximum age before a buffered payload is dropped (Lambda containers are short-lived).
const MAX_AGE_MINUTES: i64 = 60;
/// Hard cap on buffered payloads to bound memory during a sustained OTLP endpoint
/// failure on a high-traffic function. When full, the oldest payload is evicted.
const MAX_BUFFERED_PAYLOADS: usize = 1000;

/// A single OTLP payload that failed to send and must be retried.
#[derive(Debug, Clone)]
pub struct FailedOtlpPayload {
    /// Base64-encoded OTLP `ExportMetricsServiceRequest` protobuf, not yet
    /// `entity.guid`-enriched (enrichment happens on every send attempt, including
    /// retries, since `inject_entity_guid` is idempotent).
    pub encoded_payload: String,
    pub entity_guid: String,
    pub otlp_metric_endpoint: String,
    pub request_id: String,
    pub failed_at: DateTime<Utc>,
    pub retry_count: usize,
    /// Earliest time to retry, honoring a `Retry-After` header when present.
    pub next_retry_at: Option<DateTime<Utc>>,
}

/// Global buffer for failed OTLP sends (APM mode only).
pub static FAILED_OTLP_BUFFER: Lazy<Arc<Mutex<Vec<FailedOtlpPayload>>>> =
    Lazy::new(|| Arc::new(Mutex::new(Vec::new())));

/// Buffer a failed OTLP payload for retry.
pub fn buffer_failed_otlp_payload(
    encoded_payload: String,
    entity_guid: String,
    otlp_metric_endpoint: String,
    request_id: String,
    retry_after: Option<Duration>,
) {
    let next_retry_at = retry_after.and_then(|d| {
        chrono::Duration::from_std(d)
            .ok()
            .map(|cd| Utc::now() + cd)
    });
    let item = FailedOtlpPayload {
        encoded_payload,
        entity_guid,
        otlp_metric_endpoint,
        request_id,
        failed_at: Utc::now(),
        retry_count: 0,
        next_retry_at,
    };
    if let Ok(mut buffer) = FAILED_OTLP_BUFFER.lock() {
        // Bound memory: evict oldest if at capacity (prefer keeping fresher payloads).
        if buffer.len() >= MAX_BUFFERED_PAYLOADS {
            buffer.remove(0);
            warn!("OTLP buffer full ({}) - evicted oldest payload", MAX_BUFFERED_PAYLOADS);
        }
        buffer.push(item);
        debug!(
            "APM mode: Buffered failed OTLP payload (total buffered: {})",
            buffer.len()
        );
    } else {
        error!("Failed to lock OTLP buffer - payload lost!");
    }
}

/// Retry all buffered OTLP payloads.
pub async fn retry_buffered_otlp_payloads(client: &reqwest::Client, license_key: &str) {
    let buffered = {
        if let Ok(mut buffer) = FAILED_OTLP_BUFFER.lock() {
            std::mem::take(&mut *buffer)
        } else {
            error!("Failed to lock OTLP buffer for retry");
            return;
        }
    };

    if buffered.is_empty() {
        return;
    }

    debug!(
        "APM mode: Retrying {} buffered OTLP payload(s)",
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
                "Dropping OTLP payload too old ({} min) after {} attempt(s) for request {}",
                now.signed_duration_since(item.failed_at).num_minutes(),
                item.retry_count,
                item.request_id
            );
            continue;
        }

        item.retry_count += 1;

        match super::collector::send_single_otlp_payload(
            client,
            &item.otlp_metric_endpoint,
            license_key,
            &item.encoded_payload,
            &item.entity_guid,
            item.retry_count,
        )
        .await
        {
            Ok(()) => {
                success += 1;
                debug!(
                    "Successfully retried OTLP payload for request {} (attempt {})",
                    item.request_id, item.retry_count
                );
            }
            Err(e) if e.is_permanent() => {
                still_failed += 1;
                error!(
                    "Dropping OTLP payload for request {} after permanent error on retry: {}",
                    item.request_id, e
                );
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
                        "Dropping OTLP payload for request {} after {} retry attempts: {}",
                        item.request_id, item.retry_count, e
                    );
                }
            }
        }
    }

    if success > 0 || still_failed > 0 {
        debug!(
            "OTLP retry results: {} successful, {} still failed",
            success, still_failed
        );
    }
}

fn re_buffer(item: FailedOtlpPayload) {
    if let Ok(mut buffer) = FAILED_OTLP_BUFFER.lock() {
        buffer.push(item);
    }
}

/// Count of buffered OTLP payloads (for monitoring/shutdown logging).
pub fn get_otlp_buffer_count() -> usize {
    FAILED_OTLP_BUFFER
        .lock()
        .map(|b| b.len())
        .unwrap_or(0)
}

/// Distinct `request_id`s represented in the buffered OTLP payloads. Used so the
/// shutdown drop summary's "invocations affected" count includes OTLP-only requests
/// (which otherwise show up in the item count but not the invocation count).
pub fn buffered_request_ids() -> Vec<String> {
    let mut ids: Vec<String> = FAILED_OTLP_BUFFER
        .lock()
        .map(|buf| buf.iter().map(|item| item.request_id.clone()).collect())
        .unwrap_or_default();
    ids.sort();
    ids.dedup();
    ids
}

#[cfg(test)]
#[path = "otlp_buffer_tests.rs"]
mod otlp_buffer_tests;
