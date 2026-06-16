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
/// outage on a high-traffic function. When full, the oldest batch is evicted.
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

#[cfg(test)]
mod metric_api_buffer_tests {
    use super::*;
    use serde_json::json;
    use serial_test::serial;

    fn clear() {
        if let Ok(mut b) = FAILED_METRIC_API_BUFFER.lock() {
            b.clear();
        }
    }

    /// Minimal one-shot-per-connection HTTP server that replies with the given
    /// status codes in order (one per incoming request). Returns its base URL.
    async fn mock_server(codes: Vec<u16>) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for code in codes {
                let Ok((mut stream, _)) = listener.accept().await else { break };
                let mut buf = [0u8; 8192];
                let _ = stream.read(&mut buf).await; // drain request (small payload)
                let reason = if code == 202 { "Accepted" } else { "Service Unavailable" };
                let body = "x";
                let resp = format!(
                    "HTTP/1.1 {code} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                let _ = stream.flush().await;
            }
        });
        format!("http://{addr}/metric/v1")
    }

    /// END-TO-END PROOF: a 503 is buffered, then a subsequent drain re-sends and
    /// succeeds (202), leaving the buffer empty. This exercises the real send →
    /// classify → buffer → retry → success path over a live socket.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn transient_503_is_buffered_then_resent_successfully() {
        clear();
        // First request → 503, second request → 202.
        let url = mock_server(vec![503, 202]).await;
        let client = reqwest::Client::new();

        // First send attempt: 503 → classified transient → buffered.
        let err = super::super::collector::send_platform_metrics(
            &client,
            "lk",
            &url,
            &[json!({"name": "apm.lambda.transaction.duration"})],
        )
        .await
        .expect_err("503 must be an error");
        assert!(!err.is_permanent(), "503 must be transient");
        buffer_failed_metric_api(
            vec![json!({"name": "apm.lambda.transaction.duration"})],
            url.clone(),
            err.retry_after(),
        );
        assert_eq!(get_metric_api_buffer_count(), 1, "503 should be buffered");

        // Drain → second request returns 202 → buffer empties. Retry happened.
        retry_buffered_metric_api(&client, "lk").await;
        assert_eq!(
            get_metric_api_buffer_count(),
            0,
            "after successful retry the buffer must be empty"
        );
        clear();
    }

    /// A permanent 4xx must NOT be retried/buffered (retrying can't help).
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn permanent_400_is_not_buffered() {
        clear();
        let url = mock_server(vec![400]).await;
        let client = reqwest::Client::new();
        let err = super::super::collector::send_platform_metrics(
            &client,
            "lk",
            &url,
            &[json!({"name": "m"})],
        )
        .await
        .expect_err("400 must be an error");
        assert!(err.is_permanent(), "400 must be permanent");
        // Caller drops permanent errors — nothing buffered.
        assert_eq!(get_metric_api_buffer_count(), 0);
        clear();
    }

    #[test]
    #[serial]
    fn buffers_nonempty_metrics_only() {
        clear();
        buffer_failed_metric_api(vec![], "https://m".into(), None);
        assert_eq!(get_metric_api_buffer_count(), 0, "empty batch must not buffer");

        buffer_failed_metric_api(vec![json!({"name": "x"})], "https://m".into(), None);
        assert_eq!(get_metric_api_buffer_count(), 1);
        clear();
    }

    #[test]
    #[serial]
    fn retry_after_gates_send_and_rebuffers() {
        clear();
        // A far-future Retry-After means the item is not yet eligible: retry should
        // re-buffer it untouched, without attempting any network send.
        buffer_failed_metric_api(
            vec![json!({"name": "x"})],
            "http://127.0.0.1:1/never".into(),
            Some(Duration::from_secs(3600)),
        );
        let client = reqwest::Client::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(retry_buffered_metric_api(&client, "lk"));
        assert_eq!(get_metric_api_buffer_count(), 1, "item should remain buffered until backoff elapses");
        clear();
    }

    #[test]
    #[serial]
    fn caps_buffer_size_by_evicting_oldest() {
        clear();
        for _ in 0..(MAX_BUFFERED_BATCHES + 50) {
            buffer_failed_metric_api(vec![json!({"name": "x"})], "https://m".into(), None);
        }
        assert_eq!(
            get_metric_api_buffer_count(),
            MAX_BUFFERED_BATCHES,
            "buffer must never exceed the cap"
        );
        clear();
    }

    #[test]
    #[serial]
    fn ages_out_old_items_without_sending() {
        clear();
        // Push an item older than the age cap; retry must drop it (and never send).
        if let Ok(mut b) = FAILED_METRIC_API_BUFFER.lock() {
            b.push(FailedMetricApi {
                metrics: vec![json!({"name": "x"})],
                endpoint: "http://127.0.0.1:1/never".into(),
                failed_at: Utc::now() - chrono::Duration::minutes(MAX_AGE_MINUTES + 5),
                retry_count: 0,
                next_retry_at: None,
            });
        }
        let client = reqwest::Client::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(retry_buffered_metric_api(&client, "lk"));
        assert_eq!(get_metric_api_buffer_count(), 0, "aged-out item should be dropped");
        clear();
    }
}
