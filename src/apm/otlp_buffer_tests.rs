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
            otlp_metric_endpoint: "http://127.0.0.1:1/never".to_string(),
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

/// Every element of an `otlp_payload` array is its own request: 5 in, 5 accepted.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn all_array_entries_are_sent_not_just_the_first() {
    clear();
    let url = mock_server(vec![202; 5]).await;
    let client = reqwest::Client::new();
    let payloads: Vec<String> = (0..5).map(|_| empty_otlp_payload_base64()).collect();

    let sent = super::super::collector::send_otlp_payload(
        &client, &url, "lk", &payloads, "guid1", "r1",
    )
    .await;

    assert_eq!(sent, 5);
    assert_eq!(get_otlp_buffer_count(), 0);
    clear();
}

/// One transient failure in a batch is buffered without affecting its siblings, and is
/// not counted as sent.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn one_failing_entry_does_not_drop_the_others() {
    clear();
    let url = mock_server(vec![202, 503, 202]).await;
    let client = reqwest::Client::new();
    let payloads: Vec<String> = (0..3).map(|_| empty_otlp_payload_base64()).collect();

    let sent = super::super::collector::send_otlp_payload(
        &client, &url, "lk", &payloads, "guid1", "r1",
    )
    .await;

    assert_eq!(sent, 2, "the two 202s count as sent; the 503 does not");
    assert_eq!(get_otlp_buffer_count(), 1, "only the 503 is buffered");
    clear();
}

// ---------------------------------------------------------------------------
// End to end: agent payload envelope -> parse -> enrich -> HTTP request on the wire
// ---------------------------------------------------------------------------

/// Real `otlp_payload` blobs captured from live agents.
const NODE_AGENT_OTLP: &str = "CuEKCgIQABLaCgoeChVub2RlLW90ZWwtbGFtYmRhLXRlc3QSBTEuMC4wEnoKD2FwcC5pbnZvY2F0aW9ucxIdTnVtYmVyIG9mIGhhbmRsZXIgaW52b2NhdGlvbnMaATE6RQo/EYAKqdonTtEYGQDFNdwnTtEYIQAAAAAAAPA/OgwKBG1vZGUSBAoCb2s6FAoHb3V0Y29tZRIJCgdzdWNjZXNzEAEYARKKAQoMYXBwLmluZmxpZ2h0EidIYW5kbGVyIGludm9jYXRpb25zIGN1cnJlbnRseSBpbiBmbGlnaHQaATE6TgpKEYD/Pb8nTtEYGQDFNdwnTtEYIQAAAAAAAAAAOhwKE2h0dHAucmVxdWVzdC5tZXRob2QSBQoDR0VUOg8KCHVybC5wYXRoEgMKAS8QARK+AwoUYXBwLmhhbmRsZXIuZHVyYXRpb24SFkhhbmRsZXIgZXhlY3V0aW9uIHRpbWUaAm1zSokDCoQDEcBMuNonTtEYGQDFNdwnTtEYIQEAAAAAAAAAKQAAAAAA0HxAMoABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA6eAAAAAAAAAAAAAAAAAAAFEAAAAAAAAAkQAAAAAAAADlAAAAAAAAASUAAAAAAAMBSQAAAAAAAAFlAAAAAAABAb0AAAAAAAEB/QAAAAAAAcIdAAAAAAABAj0AAAAAAAIijQAAAAAAAiLNAAAAAAABMvUAAAAAAAIjDQEocChNodHRwLnJlcXVlc3QubWV0aG9kEgUKA0dFVEoPCgh1cmwucGF0aBIDCgEvSiAKGWh0dHAucmVzcG9uc2Uuc3RhdHVzX2NvZGUSAxjIAVkAAAAAANB8QGEAAAAAANB8QBABEroDChVhcHAudXBzdHJlYW0uZHVyYXRpb24SGk91dGJvdW5kIEhUVFAgcmVxdWVzdCB0aW1lGgJtc0qAAwr7AhGACqnaJ07RGBkAxTXcJ07RGCEBAAAAAAAAACkAAAAAAIB7QDKAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAOngAAAAAAAAAAAAAAAAAABRAAAAAAAAAJEAAAAAAAAA5QAAAAAAAAElAAAAAAADAUkAAAAAAAABZQAAAAAAAQG9AAAAAAABAf0AAAAAAAHCHQAAAAAAAQI9AAAAAAACIo0AAAAAAAIizQAAAAAAATL1AAAAAAACIw0BKIAoZaHR0cC5yZXNwb25zZS5zdGF0dXNfY29kZRIDGMgBSiQKDnNlcnZlci5hZGRyZXNzEhIKEHBvc3RtYW4tZWNoby5jb21ZAAAAAACAe0BhAAAAAACAe0AQARJTChRhcHAubWVtb3J5LmhlYXBfdXNlZBIYVjggaGVhcCBjdXJyZW50bHkgaW4gdXNlGgJCeSodChsRAMU13CdO0RgZAMU13CdO0RghAAAAABrOf0ESXAoOYXBwLm1lbW9yeS5yc3MSJ1Jlc2lkZW50IHNldCBzaXplIG9mIHRoZSBMYW1iZGEgcHJvY2VzcxoCQnkqHQobEQDFNdwnTtEYGQDFNdwnTtEYIQAAAABAqptB";
const JAVA_AGENT_OTLP: &str = "Cq4FCuMBCg8KC2VudGl0eS5ndWlkEgAKPQoTc2VydmljZS5pbnN0YW5jZS5pZBImCiRmZGJiZTdlZi0xY2UyLTQxYWYtYjJjZC03YWQ0MTJhOWMwODgKJQoMc2VydmljZS5uYW1lEhUKE2xhbWJkYS1oeWJyaWQtYWdlbnQKIAoWdGVsZW1ldHJ5LnNkay5sYW5ndWFnZRIGCgRqYXZhCiUKEnRlbGVtZXRyeS5zZGsubmFtZRIPCg1vcGVudGVsZW1ldHJ5CiEKFXRlbGVtZXRyeS5zZGsudmVyc2lvbhIICgYxLjU5LjASxQMKIAoeb3BlbnRlbGVtZXRyeS1tZXRyaWNzLWFwaS1kZW1vEmsKLG9wZW50ZWxlbWV0cnktbWV0cmljcy1hcGktZGVtby51cGRvd25jb3VudGVyOjsKNxHgIyilLGrXGBk8gw27jGrXGDEVAAAAAAAAADoaChFMb25nVXBEb3duQ291bnRlchIFCgNmb28QAhKbAQowb3BlbnRlbGVtZXRyeS1tZXRyaWNzLWFwaS1kZW1vLm9ic2VydmFibGVjb3VudGVyEh5Ub3RhbCBudW1iZXIgb2Ygam9icyBwcm9jZXNzZWQaBGpvYnM6QQo7Ef1rJfJAatcYGTyDDbuMatcYMQAAAAAAAAAAOh4KFU9ic2VydmFibGVMb25nQ291bnRlchIFCgNmb28QARgBEpUBCi5vcGVudGVsZW1ldHJ5LW1ldHJpY3MtYXBpLWRlbW8ub2JzZXJ2YWJsZWdhdWdlEh5Ub3RhbCBudW1iZXIgb2Ygam9icyBwcm9jZXNzZWQaBGpvYnMqPQo7Ef1rJfJAatcYGTyDDbuMatcYIQAAAAAAAC5AOh4KFU9ic2VydmFibGVEb3VibGVHYXVnZRIFCgNmb28=";

/// A request as the mock collector received it.
struct CapturedRequest {
    headers: String,
    body: Vec<u8>,
}

/// Accepts one connection, replies 202, and hands back the request it received.
async fn capturing_server() -> (String, tokio::sync::oneshot::Receiver<CapturedRequest>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let header_end = loop {
            let n = stream.read(&mut chunk).await.unwrap();
            buf.extend_from_slice(&chunk[..n]);
            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break i + 4;
            }
        };
        let headers = String::from_utf8_lossy(&buf[..header_end]).to_lowercase();
        let len: usize = headers
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .and_then(|v| v.trim().parse().ok())
            .unwrap();
        while buf.len() < header_end + len {
            let n = stream.read(&mut chunk).await.unwrap();
            buf.extend_from_slice(&chunk[..n]);
        }
        let body = buf[header_end..header_end + len].to_vec();
        let _ = stream
            .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await;
        let _ = tx.send(CapturedRequest { headers, body });
    });
    (format!("http://{addr}/v1/metrics"), rx)
}

/// Wrap an agent's `otlp_payload` value in the serverless envelope the extension reads.
fn agent_envelope(otlp_payload_json: &str) -> Vec<u8> {
    use base64::{engine::general_purpose, Engine as _};
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;
    let data = format!(r#"{{"otlp_payload": {otlp_payload_json}, "metric_data": [[1,2,3]]}}"#);
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    gz.write_all(data.as_bytes()).unwrap();
    let encoded = general_purpose::STANDARD.encode(gz.finish().unwrap());
    format!(r#"["2", "NR_LAMBDA_MONITORING", "{encoded}"]"#).into_bytes()
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn agent_payload_reaches_the_wire_enriched_and_otherwise_unchanged() {
    use crate::apm::otlp::{ExportMetricsServiceRequest, KeyValue};
    use base64::{engine::general_purpose, Engine as _};
    use prost::Message;
    use std::io::Read;

    const GUID: &str = "MTAxOTYwODR8QVBNfEFQUExJQ0FUSU9OfDk5OQ";
    // (case, `otlp_payload` value as the agent writes it, the blob inside it)
    let cases = [
        ("array, no entity.guid (.NET, Node)", format!(r#"["{NODE_AGENT_OTLP}"]"#), NODE_AGENT_OTLP),
        ("array, empty entity.guid (Java)", format!(r#"["{JAVA_AGENT_OTLP}"]"#), JAVA_AGENT_OTLP),
    ];

    for (agent, otlp_payload_json, blob) in cases {
        clear();
        let (url, captured) = capturing_server().await;

        // The extension parses the agent's envelope.
        let (data_map, _) =
            crate::apm::payload_parser::parse_agent_payload(&agent_envelope(&otlp_payload_json)).unwrap();
        let entries: Vec<String> = data_map["otlp_payload"]
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        assert_eq!(entries.len(), 1, "{agent}: one blob extracted");

        // It enriches and sends.
        let client = reqwest::Client::new();
        let accepted =
            super::super::collector::send_otlp_payload(&client, &url, "license-key", &entries, GUID, "r1").await;
        assert_eq!(accepted, 1, "{agent}: accepted by the collector");
        assert_eq!(get_otlp_buffer_count(), 0, "{agent}: nothing buffered");

        // What arrived on the wire.
        let req = captured.await.unwrap();
        assert!(req.headers.contains("content-type: application/x-protobuf"), "{agent}");
        assert!(req.headers.contains("content-encoding: gzip"), "{agent}");
        assert!(req.headers.contains("api-key: license-key"), "{agent}");

        let mut wire = Vec::new();
        flate2::read::GzDecoder::new(&req.body[..]).read_to_end(&mut wire).unwrap();
        let sent = ExportMetricsServiceRequest::decode(&wire[..]).unwrap();
        let original =
            ExportMetricsServiceRequest::decode(&general_purpose::STANDARD.decode(blob).unwrap()[..]).unwrap();

        let attrs = sent.resource_metrics[0].resource.as_ref().unwrap().attributes();
        let guids: Vec<_> = attrs.iter().filter(|kv| kv.key == "entity.guid").collect();
        assert_eq!(guids.len(), 1, "{agent}: exactly one entity.guid");
        assert_eq!(guids[0].string_value(), Some(GUID), "{agent}");

        let (s, o) = (&sent.resource_metrics[0], &original.resource_metrics[0]);
        assert_eq!(s.scope_metrics_raw, o.scope_metrics_raw, "{agent}: scopes and metrics unchanged");
        assert_eq!(s.schema_url, o.schema_url, "{agent}");
        let not_guid = |raw: &&Vec<u8>| KeyValue::decode(raw.as_slice()).map_or(true, |kv| kv.key != "entity.guid");
        let sent_rest: Vec<_> = s.resource.as_ref().unwrap().attributes_raw.iter().filter(not_guid).collect();
        let orig_rest: Vec<_> = o.resource.as_ref().unwrap().attributes_raw.iter().filter(not_guid).collect();
        assert_eq!(sent_rest, orig_rest, "{agent}: other resource attributes unchanged");

        println!(
            "  {agent:<36} {} -> {} bytes on the wire (gzip {}), 1 entity.guid, rest unchanged",
            general_purpose::STANDARD.decode(blob).unwrap().len(),
            wire.len(),
            req.body.len()
        );
    }
    clear();
}
