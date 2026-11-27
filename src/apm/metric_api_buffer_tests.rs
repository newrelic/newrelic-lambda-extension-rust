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
/// A 429 response automatically includes a `Retry-After: 30` header so tests
/// can exercise the retry_after closure in retry_buffered_metric_api.
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
            let reason = match code {
                202 => "Accepted",
                400 => "Bad Request",
                429 => "Too Many Requests",
                _ => "Service Unavailable",
            };
            let extra = if code == 429 { "Retry-After: 30\r\n" } else { "" };
            let body = "x";
            let resp = format!(
                "HTTP/1.1 {code} {reason}\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
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
    let client = crate::newrelic::tls_fallback::test_client();
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
    let client = crate::newrelic::tls_fallback::test_client();

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
    let client = crate::newrelic::tls_fallback::test_client();
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
    let client = crate::newrelic::tls_fallback::test_client();
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
    let client = crate::newrelic::tls_fallback::test_client();
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

/// A 400 response on retry is a permanent error — the item must be dropped,
/// not re-buffered (retrying a permanent error is pointless).
/// Exercises the `Err(e) if e.is_permanent()` arm in retry_buffered_metric_api.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn permanent_error_on_retry_drops_item() {
    clear();
    let url = mock_server(vec![400]).await;
    buffer_failed_metric_api(vec![json!({"name": "m"})], url, None);
    assert_eq!(get_metric_api_buffer_count(), 1);

    let client = crate::newrelic::tls_fallback::test_client();
    retry_buffered_metric_api(&client, "lk").await;

    assert_eq!(
        get_metric_api_buffer_count(),
        0,
        "permanent error on retry must drop the item without re-buffering"
    );
    clear();
}

/// A transient 429 + Retry-After on a retry attempt causes the item to be
/// re-buffered with next_retry_at set to ~30 s from now.
/// Covers the `item.next_retry_at = e.retry_after().and_then(...)` closure
/// (lines 140-142 of metric_api_buffer.rs) which only runs when a retry
/// itself returns a retryable error that carries a Retry-After value.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn transient_429_on_retry_rebuffers_with_next_retry_at() {
    clear();
    let url = mock_server(vec![429]).await;
    buffer_failed_metric_api(vec![json!({"name": "m"})], url, None);
    assert_eq!(get_metric_api_buffer_count(), 1);

    let client = crate::newrelic::tls_fallback::test_client();
    retry_buffered_metric_api(&client, "lk").await;

    // Item must be re-buffered (retry_count=1 < MAX_RETRY_ATTEMPTS).
    assert_eq!(get_metric_api_buffer_count(), 1, "429 must re-buffer the item");
    if let Ok(buf) = FAILED_METRIC_API_BUFFER.lock() {
        let item = &buf[0];
        assert_eq!(item.retry_count, 1);
        assert!(item.next_retry_at.is_some(), "Retry-After: 30 must set next_retry_at");
    }
    clear();
}

/// An item that has already been retried MAX_RETRY_ATTEMPTS-1 times must be
/// dropped (not re-buffered) when the next attempt also fails.
/// Exercises the `else { error!("Dropping...") }` branch in retry_buffered_metric_api.
#[tokio::test]
#[serial]
async fn drop_after_max_retries_removes_item() {
    clear();
    if let Ok(mut b) = FAILED_METRIC_API_BUFFER.lock() {
        b.push(FailedMetricApi {
            metrics: vec![json!({"name": "m"})],
            endpoint: "http://127.0.0.1:1/metrics".into(),
            failed_at: Utc::now(),
            retry_count: 9,
            next_retry_at: None,
        });
    }
    assert_eq!(get_metric_api_buffer_count(), 1);

    let client = crate::newrelic::tls_fallback::test_client();
    retry_buffered_metric_api(&client, "lk").await;

    // retry_count bumps to 10; 10 < MAX_RETRY_ATTEMPTS (10) is false → dropped.
    assert_eq!(
        get_metric_api_buffer_count(),
        0,
        "item must be dropped after reaching the maximum retry limit"
    );
    clear();
}

// ── merged retries: several buffered batches share one request ────────────────────────────

use crate::apm::collector::{decode_metric_request, expand_common};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn metric_server(status: u16) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(status)).mount(&server).await;
    server
}

fn buffer_named(endpoint: &str, names: &[&str]) {
    let metrics = names.iter().map(|n| json!({"name": n, "type": "gauge", "value": 1})).collect();
    buffer_failed_metric_api(metrics, endpoint.to_string(), None);
}

/// Metric names in one request body, which must be the documented shape: a gzip-compressed
/// JSON array whose only map holds the `metrics` array.
fn metric_names(body: &[u8]) -> Vec<String> {
    let parsed = decode_metric_request(body);
    let maps = parsed.as_array().expect("the body is an array of maps");
    assert_eq!(maps.len(), 1, "one map per request");
    assert!(maps[0]["metrics"].is_array(), "each map has a metrics array");
    expand_common(&parsed)
        .iter()
        .map(|m| m["name"].as_str().unwrap_or_default().to_string())
        .collect()
}

/// Matches a request whose (decompressed) body contains the text.
struct BodyContains(&'static str);
impl wiremock::Match for BodyContains {
    fn matches(&self, request: &wiremock::Request) -> bool {
        crate::apm::collector::gunzip_text(&request.body).contains(self.0)
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn several_buffered_batches_go_out_in_one_request() {
    clear();
    let server = metric_server(202).await;
    let endpoint = format!("{}/metric/v1", server.uri());
    buffer_named(&endpoint, &["a1", "a2"]);
    buffer_named(&endpoint, &["b1"]);
    buffer_named(&endpoint, &["c1", "c2", "c3"]);
    let client = crate::newrelic::tls_fallback::test_client();

    retry_buffered_metric_api(&client, "lk").await;

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1, "three batches, one request");
    let mut names = metric_names(&requests[0].body);
    names.sort();
    assert_eq!(names, vec!["a1", "a2", "b1", "c1", "c2", "c3"], "every data point is in it");
    assert_eq!(get_metric_api_buffer_count(), 0);
    clear();
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn batches_for_different_endpoints_are_not_merged() {
    clear();
    let (eu, us) = (metric_server(202).await, metric_server(202).await);
    buffer_named(&format!("{}/metric/v1", eu.uri()), &["eu1"]);
    buffer_named(&format!("{}/metric/v1", us.uri()), &["us1"]);
    buffer_named(&format!("{}/metric/v1", eu.uri()), &["eu2"]);
    let client = crate::newrelic::tls_fallback::test_client();

    retry_buffered_metric_api(&client, "lk").await;

    let (eu_requests, us_requests) = (eu.received_requests().await.unwrap(), us.received_requests().await.unwrap());
    assert_eq!((eu_requests.len(), us_requests.len()), (1, 1), "one request per endpoint");
    let mut eu_names = metric_names(&eu_requests[0].body);
    eu_names.sort();
    assert_eq!(eu_names, vec!["eu1", "eu2"]);
    assert_eq!(metric_names(&us_requests[0].body), vec!["us1"]);
    clear();
}

/// The Metric API rejects a POST over 1 MB (10^6 bytes), so a big backlog is split.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn a_backlog_over_the_size_limit_is_split_into_requests_under_it() {
    clear();
    let server = metric_server(202).await;
    let endpoint = format!("{}/metric/v1", server.uri());
    let big = "x".repeat(300_000);
    for name in ["big1", "big2", "big3", "big4"] {
        buffer_failed_metric_api(
            vec![json!({"name": name, "type": "gauge", "value": 1, "attributes": {"blob": big}})],
            endpoint.clone(),
            None,
        );
    }
    let client = crate::newrelic::tls_fallback::test_client();

    retry_buffered_metric_api(&client, "lk").await;

    let requests = server.received_requests().await.unwrap();
    assert!(requests.len() >= 2, "1.2 MB cannot go in one request, got {}", requests.len());
    assert!(
        requests.iter().all(|r| crate::apm::collector::gunzip_text(&r.body).len() < 1_000_000),
        "every request stays under 1 MB even before compression"
    );
    let mut names: Vec<String> = requests.iter().flat_map(|r| metric_names(&r.body)).collect();
    names.sort();
    assert_eq!(names, vec!["big1", "big2", "big3", "big4"], "nothing lost in the split");
    assert_eq!(get_metric_api_buffer_count(), 0);
    clear();
}

/// One bad batch must not take the good ones down with it: when the merged request is
/// rejected for good, each batch is tried on its own, so only the bad one is dropped.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn a_rejected_merged_request_falls_back_to_one_request_per_batch() {
    clear();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(BodyContains("BAD"))
        .respond_with(ResponseTemplate::new(400))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(202)).with_priority(5).mount(&server).await;
    let endpoint = format!("{}/metric/v1", server.uri());
    buffer_named(&endpoint, &["good"]);
    buffer_named(&endpoint, &["BAD"]);
    let client = crate::newrelic::tls_fallback::test_client();

    retry_buffered_metric_api(&client, "lk").await;

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 3, "the merged request, then one per batch");
    let delivered: Vec<Vec<String>> = requests
        .iter()
        .map(|r| metric_names(&r.body))
        .filter(|names| !names.iter().any(|n| n == "BAD"))
        .collect();
    assert_eq!(delivered, vec![vec!["good".to_string()]], "the good batch got through on its own");
    assert_eq!(get_metric_api_buffer_count(), 0, "the bad one is dropped, not retried forever");
    clear();
}

/// A transient failure puts every batch back separately, each with its attempt counted.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn a_transient_failure_rebuffers_every_batch_separately() {
    clear();
    let server = metric_server(503).await;
    let endpoint = format!("{}/metric/v1", server.uri());
    buffer_named(&endpoint, &["a"]);
    buffer_named(&endpoint, &["b"]);
    buffer_named(&endpoint, &["c"]);
    let client = crate::newrelic::tls_fallback::test_client();

    retry_buffered_metric_api(&client, "lk").await;

    assert_eq!(server.received_requests().await.unwrap().len(), 1, "one merged attempt");
    assert_eq!(get_metric_api_buffer_count(), 3, "all three are back");
    let counts: Vec<usize> = FAILED_METRIC_API_BUFFER.lock().unwrap().iter().map(|i| i.retry_count).collect();
    assert_eq!(counts, vec![1, 1, 1], "the attempt counts for each");
    clear();
}

#[test]
fn push_capped_never_exceeds_cap_and_evicts_oldest_by_failed_at() {
    let at = |m: i64| FailedMetricApi {
        metrics: vec![json!({"id": m})],
        endpoint: "http://x".to_string(),
        failed_at: Utc::now() - chrono::Duration::minutes(m),
        retry_count: 0,
        next_retry_at: None,
    };
    let mut buf = vec![at(5), at(3)];
    // A re-buffered older batch lands last but must still be the one evicted.
    push_capped(&mut buf, at(10), 2);
    assert_eq!(buf.len(), 2);
    assert!(buf.iter().all(|i| i.metrics[0]["id"] != 10));
    push_capped(&mut buf, at(1), 2);
    assert_eq!(buf.len(), 2);
    assert!(buf.iter().all(|i| i.metrics[0]["id"] != 5), "oldest evicted first");
}
