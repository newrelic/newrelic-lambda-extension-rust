// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::json;
use std::io::Write;

fn create_test_payload(version: &str) -> Vec<u8> {
    let test_data = if version == "2" {
        r#"{"metric_data": [[1, 2, 3]], "span_event_data": [[4, 5, 6]]}"#
    } else {
        r#"{"data": {"metric_data": [[1, 2, 3]], "span_event_data": [[4, 5, 6]]}}"#
    };

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(test_data.as_bytes()).unwrap();
    let compressed = encoder.finish().unwrap();

    let encoded = general_purpose::STANDARD.encode(&compressed);

    let payload = format!(r#"["{}", "NR_LAMBDA_MONITORING", "{}"]"#, version, encoded);

    payload.into_bytes()
}

#[test]
fn test_parse_v2_payload() {
    let payload = create_test_payload("2");
    let (data_map, version) = parse_agent_payload(&payload).unwrap();

    assert_eq!(version, 2);
    assert!(data_map.contains_key("metric_data"));
    assert!(data_map.contains_key("span_event_data"));
}

#[test]
fn test_parse_v1_payload() {
    let payload = create_test_payload("1");
    let (data_map, version) = parse_agent_payload(&payload).unwrap();

    assert_eq!(version, 1);
    assert!(data_map.contains_key("metric_data"));
    assert!(data_map.contains_key("span_event_data"));
}

// ------------------------------------------------------------------
// NR-579361: extract_transaction_request_id / *_from_payload_bytes
// ------------------------------------------------------------------

/// Build a data_map whose `analytic_event_data` holds the given events array.
fn data_map_with_analytic(events: Value) -> HashMap<String, Vec<Value>> {
    let mut m = HashMap::new();
    m.insert(
        "analytic_event_data".to_string(),
        vec![Value::Null, json!({ "reservoir_size": 10, "events_seen": 1 }), events],
    );
    m
}

/// A v2 wire payload carrying a single Transaction event with `aws.requestId`,
/// plus an external span carrying a DIFFERENT id (the trap we must not read).
fn wire_payload_with_ids(txn_request_id: Option<&str>, span_request_id: &str) -> Vec<u8> {
    let data = match txn_request_id {
        Some(rid) => json!({
            "analytic_event_data": [null, { "reservoir_size": 10 },
                [ [ { "type": "Transaction" }, {}, { "aws.requestId": rid } ] ]],
            "span_event_data": [null, { "reservoir_size": 10 },
                [ [ { "type": "Span", "name": "External/secretsmanager" }, {},
                    { "aws.requestId": span_request_id } ] ]],
        }),
        // Id-less harvest: metrics only, no transaction.
        None => json!({ "metric_data": [null, [["Custom/x", [1, 2.0, 2.0, 2.0, 2.0, 4.0]]]] }),
    };
    let json_bytes = serde_json::to_vec(&data).unwrap();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&json_bytes).unwrap();
    let compressed = encoder.finish().unwrap();
    let encoded = general_purpose::STANDARD.encode(&compressed);
    format!(r#"["2", "NR_LAMBDA_MONITORING", "{}"]"#, encoded).into_bytes()
}

#[test]
fn extract_returns_transaction_request_id() {
    let m = data_map_with_analytic(json!([
        [ { "type": "Transaction" }, {}, { "aws.requestId": "req-A" } ]
    ]));
    assert_eq!(extract_transaction_request_id(&m), Some("req-A".to_string()));
}

#[test]
fn extract_returns_first_when_multiple_transactions() {
    let m = data_map_with_analytic(json!([
        [ { "type": "Transaction" }, {}, { "aws.requestId": "req-1" } ],
        [ { "type": "Transaction" }, {}, { "aws.requestId": "req-2" } ]
    ]));
    assert_eq!(extract_transaction_request_id(&m), Some("req-1".to_string()));
}

#[test]
fn extract_none_when_no_analytic_event_data() {
    // metrics-only harvest → no analytic_event_data key
    let mut m = HashMap::new();
    m.insert("metric_data".to_string(), vec![json!([1, 2, 3])]);
    assert_eq!(extract_transaction_request_id(&m), None);
}

#[test]
fn extract_ignores_empty_request_id() {
    let m = data_map_with_analytic(json!([
        [ { "type": "Transaction" }, {}, { "aws.requestId": "" } ]
    ]));
    assert_eq!(extract_transaction_request_id(&m), None);
}

#[test]
fn extract_no_panic_on_malformed_shapes() {
    // events element is not an array
    let m = data_map_with_analytic(json!(["not-an-array", 42]));
    assert_eq!(extract_transaction_request_id(&m), None);
    // analytic_event_data missing the events slot entirely
    let mut short = HashMap::new();
    short.insert("analytic_event_data".to_string(), vec![Value::Null]);
    assert_eq!(extract_transaction_request_id(&short), None);
}

#[test]
fn extract_from_bytes_reads_transaction_not_span() {
    // Transaction id is req-A; the external span carries 3e8a... — must pick req-A.
    let payload = wire_payload_with_ids(Some("req-A"), "3e8a166c-span-id");
    assert_eq!(
        extract_request_id_from_payload_bytes(&payload),
        Some("req-A".to_string())
    );
}

#[test]
fn extract_from_bytes_none_for_idless_harvest() {
    let payload = wire_payload_with_ids(None, "");
    assert_eq!(extract_request_id_from_payload_bytes(&payload), None);
}

#[test]
fn extract_from_bytes_none_for_garbage() {
    assert_eq!(extract_request_id_from_payload_bytes(b"not a payload"), None);
    assert_eq!(extract_request_id_from_payload_bytes(b""), None);
}
