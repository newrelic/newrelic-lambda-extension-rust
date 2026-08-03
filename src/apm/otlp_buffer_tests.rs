use super::*;
use serial_test::serial;

fn clear() {
    if let Ok(mut b) = FAILED_OTLP_BUFFER.lock() {
        b.clear();
    }
}

#[test]
#[serial]
fn buffered_request_ids_extracts_distinct_ids() {
    clear();
    buffer_failed_otlp_payload(
        "cGF5bG9hZA==".to_string(),
        "guid1".to_string(),
        "http://x".to_string(),
        "r1".to_string(),
        None,
    );
    buffer_failed_otlp_payload(
        "cGF5bG9hZA==".to_string(),
        "guid1".to_string(),
        "http://x".to_string(),
        "r1".to_string(),
        None,
    );
    buffer_failed_otlp_payload(
        "b3RoZXI=".to_string(),
        "guid2".to_string(),
        "http://x".to_string(),
        "r2".to_string(),
        None,
    );
    // Distinct + sorted; r1 appears once despite two buffered payloads.
    assert_eq!(
        buffered_request_ids(),
        vec!["r1".to_string(), "r2".to_string()]
    );
    clear();
}

#[test]
#[serial]
fn buffered_request_ids_empty_when_nothing_buffered() {
    clear();
    assert!(buffered_request_ids().is_empty());
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
    format!("http://{addr}/v1/metrics")
}

/// A minimal valid `ExportMetricsServiceRequest` (empty resource_metrics) that
/// `inject_entity_guid` can decode without error, base64-encoded as agents send it.
fn empty_otlp_payload_base64() -> String {
    use base64::{engine::general_purpose, Engine as _};
    general_purpose::STANDARD.encode(Vec::<u8>::new())
}

/// END-TO-END PROOF: a 503 is buffered, then a subsequent drain re-sends and
/// succeeds (202), leaving the buffer empty. Exercises the real send → classify
/// → buffer → retry → success path over a live socket.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn transient_503_is_buffered_then_resent_successfully() {
    clear();
    let url = mock_server(vec![503, 202]).await;
    let client = reqwest::Client::new();
    let payload = empty_otlp_payload_base64();

    let err = super::super::collector::send_single_otlp_payload(
        &client, &url, "lk", &payload, "guid1", 1,
    )
    .await
    .expect_err("503 must be an error");
    assert!(!err.is_permanent(), "503 must be transient");
    buffer_failed_otlp_payload(
        payload.clone(),
        "guid1".to_string(),
        url.clone(),
        "r1".to_string(),
        err.retry_after(),
    );
    assert_eq!(get_otlp_buffer_count(), 1, "503 should be buffered");

    retry_buffered_otlp_payloads(&client, "lk").await;
    assert_eq!(
        get_otlp_buffer_count(),
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
    let payload = empty_otlp_payload_base64();

    let err = super::super::collector::send_single_otlp_payload(
        &client, &url, "lk", &payload, "guid1", 1,
    )
    .await
    .expect_err("400 must be an error");
    assert!(err.is_permanent(), "400 must be permanent");
    assert_eq!(get_otlp_buffer_count(), 0);
    clear();
}

/// Malformed payloads (bad base64) must classify as permanent, never retried.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn malformed_base64_is_permanent() {
    let client = reqwest::Client::new();
    let err = super::super::collector::send_single_otlp_payload(
        &client, "http://127.0.0.1:1/never", "lk", "not-valid-base64!!!", "guid1", 1,
    )
    .await
    .expect_err("invalid base64 must be an error");
    assert!(err.is_permanent(), "malformed payload must be permanent");
}

#[test]
#[serial]
fn retry_after_gates_send_and_rebuffers() {
    clear();
    buffer_failed_otlp_payload(
        empty_otlp_payload_base64(),
        "guid1".to_string(),
        "http://127.0.0.1:1/never".to_string(),
        "r1".to_string(),
        Some(std::time::Duration::from_secs(3600)),
    );
    let client = reqwest::Client::new();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(retry_buffered_otlp_payloads(&client, "lk"));
    assert_eq!(
        get_otlp_buffer_count(),
        1,
        "item should remain buffered until backoff elapses"
    );
    clear();
}

#[test]
#[serial]
fn caps_buffer_size_by_evicting_oldest() {
    clear();
    for i in 0..(MAX_BUFFERED_PAYLOADS + 50) {
        buffer_failed_otlp_payload(
            empty_otlp_payload_base64(),
            "guid1".to_string(),
            "https://x".to_string(),
            format!("r{i}"),
            None,
        );
    }
    assert_eq!(
        get_otlp_buffer_count(),
        MAX_BUFFERED_PAYLOADS,
        "buffer must never exceed the cap"
    );
    clear();
}

#[test]
#[serial]
fn ages_out_old_items_without_sending() {
    clear();
    if let Ok(mut b) = FAILED_OTLP_BUFFER.lock() {
        b.push(FailedOtlpPayload {
            encoded_payload: empty_otlp_payload_base64(),
            entity_guid: "guid1".to_string(),
            otlp_endpoint: "http://127.0.0.1:1/never".to_string(),
            request_id: "r1".to_string(),
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
    rt.block_on(retry_buffered_otlp_payloads(&client, "lk"));
    assert_eq!(
        get_otlp_buffer_count(),
        0,
        "aged-out item should be dropped"
    );
    clear();
}
