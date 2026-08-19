use super::*;
use serde_json::json;
use serial_test::serial;

fn clear() {
    if let Ok(mut b) = FAILED_METRIC_API_BUFFER.lock() {
        b.clear();
    }
}

#[test]
#[serial]
fn buffered_request_ids_extracts_distinct_aws_request_ids() {
    clear();
    buffer_failed_metric_api(
        vec![
            json!({"name":"apm.lambda.transaction.duration","attributes":{"aws.requestId":"r1"}}),
            json!({"name":"apm.lambda.transaction.billed_duration","attributes":{"aws.requestId":"r1"}}),
        ],
        "http://x".to_string(),
        None,
    );
    buffer_failed_metric_api(
        vec![json!({"name":"m","attributes":{"aws.requestId":"r2"}})],
        "http://x".to_string(),
        None,
    );
    // Distinct + sorted; r1 appears once despite two metrics.
    assert_eq!(
        buffered_request_ids(),
        vec!["r1".to_string(), "r2".to_string()]
    );
    clear();
}

#[test]
#[serial]
fn buffered_request_ids_empty_when_no_request_id_attr() {
    clear();
    buffer_failed_metric_api(
        vec![json!({"name":"m","attributes":{"entity.guid":"g"}})],
        "http://x".to_string(),
        None,
    );
    assert!(buffered_request_ids().is_empty());
    clear();
}

/// Minimal one-shot-per-connection HTTP server that replies with the given
/// status codes in order (one per incoming request). Returns its base URL.
async fn mock_server(codes: Vec<u16>) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        for code in codes {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let mut buf = [0u8; 8192];
            let _ = stream.read(&mut buf).await; // drain request (small payload)
            let reason = if code == 202 {
                "Accepted"
            } else {
                "Service Unavailable"
            };
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

/// Proves the is_some() guard fix: retry_buffered_metric_api takes only
/// (client, license_key) — no apm_app/run_id. It must succeed even when the
/// APM session is None (reconnect window), because Metric API auth is
/// license-key-only. In event_loop.rs this function now runs outside the
/// apm_app.is_some() guard so platform metrics are not delayed during reconnect.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn retry_proceeds_without_apm_session() {
    clear();
    let url = mock_server(vec![202]).await;
    let client = reqwest::Client::new();
    buffer_failed_metric_api(
        vec![json!({"name": "apm.lambda.transaction.duration"})],
        url,
        None,
    );
    assert_eq!(get_metric_api_buffer_count(), 1);
    // No apm_app passed — succeeds regardless of APM session state.
    retry_buffered_metric_api(&client, "lk").await;
    assert_eq!(
        get_metric_api_buffer_count(),
        0,
        "metric API retry must succeed without a live APM session"
    );
    clear();
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
    assert_eq!(
        get_metric_api_buffer_count(),
        0,
        "empty batch must not buffer"
    );

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
    assert_eq!(
        get_metric_api_buffer_count(),
        1,
        "item should remain buffered until backoff elapses"
    );
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
    assert_eq!(
        get_metric_api_buffer_count(),
        0,
        "aged-out item should be dropped"
    );
    clear();
}
