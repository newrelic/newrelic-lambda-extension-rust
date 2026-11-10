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

// ── error-path coverage ───────────────────────────────────────────────────────

fn encode_gzip_b64(data: &[u8]) -> String {
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(data).unwrap();
    general_purpose::STANDARD.encode(enc.finish().unwrap())
}

/// Fewer than 2 comma-separated components after stripping brackets → Err.
#[test]
fn parse_agent_payload_single_component_returns_error() {
    let result = parse_agent_payload(b"[\"2\"]");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Insufficient"));
}

/// An encoded part that is not valid base64 → decode_uncompress returns Err.
#[test]
fn decode_uncompress_invalid_base64_returns_error() {
    let payload = b"[\"2\", \"NR_LAMBDA_MONITORING\", \"!!not_valid!!\"]";
    let result = parse_agent_payload(payload);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Base64"));
}

/// Valid base64 that decodes to non-gzip bytes → gzip decompression returns Err.
#[test]
fn decode_uncompress_invalid_gzip_returns_error() {
    let bad = general_purpose::STANDARD.encode(b"this is not gzip");
    let payload = format!("[\"2\", \"NR_LAMBDA_MONITORING\", \"{bad}\"]").into_bytes();
    let result = parse_agent_payload(&payload);
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("Gzip") || msg.contains("decompression"), "unexpected: {msg}");
}

/// Valid base64+gzip but content is a JSON array, not an object → v2 parse fails.
#[test]
fn parse_v2_array_content_returns_parse_error() {
    let encoded = encode_gzip_b64(b"[1, 2, 3]");
    let payload = format!("[\"2\", \"NR_LAMBDA_MONITORING\", \"{encoded}\"]").into_bytes();
    let result = parse_agent_payload(&payload);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("v2"));
}

/// Same for v1: JSON array instead of `{"data": {...}}` → v1 parse fails.
#[test]
fn parse_v1_array_content_returns_parse_error() {
    let encoded = encode_gzip_b64(b"[1, 2, 3]");
    let payload = format!("[\"1\", \"NR_LAMBDA_MONITORING\", \"{encoded}\"]").into_bytes();
    let result = parse_agent_payload(&payload);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("v1"));
}

// ── is_mergeable_event_list_type / merge_event_list_payloads ───────────────────────

#[test]
fn is_mergeable_event_list_type_is_true_for_the_four_documented_types() {
    for t in ["analytic_event_data", "span_event_data", "error_event_data", "custom_event_data"] {
        assert!(is_mergeable_event_list_type(t), "{t} must be mergeable");
    }
}

#[test]
fn is_mergeable_event_list_type_is_false_for_other_types() {
    for t in ["metric_data", "error_data", "transaction_sample_data", "sql_trace_data", "log_event_data", "unknown_type"] {
        assert!(!is_mergeable_event_list_type(t), "{t} must not be mergeable");
    }
}

#[test]
fn merge_event_list_payloads_concatenates_events_and_combines_reservoir_metadata() {
    let chunk_a = vec![
        Value::Null,
        json!({"reservoir_size": 100, "events_seen": 3}),
        json!([{"id": "a1"}, {"id": "a2"}, {"id": "a3"}]),
    ];
    let chunk_b = vec![
        Value::Null,
        json!({"reservoir_size": 1200, "events_seen": 5}),
        json!([{"id": "b1"}, {"id": "b2"}]),
    ];

    let merged = merge_event_list_payloads(vec![chunk_a, chunk_b]);

    assert_eq!(merged.len(), 3);
    assert_eq!(merged[1]["events_seen"], json!(8), "events_seen must sum across chunks");
    assert_eq!(merged[1]["reservoir_size"], json!(1200), "reservoir_size must be the max across chunks");
    let events = merged[2].as_array().expect("events must be an array");
    assert_eq!(events.len(), 5, "events arrays must be concatenated, not deduped or truncated");
    assert_eq!(events[0]["id"], json!("a1"));
    assert_eq!(events[4]["id"], json!("b2"));
}

#[test]
fn merge_event_list_payloads_skips_malformed_chunks_without_panicking() {
    let malformed = vec![Value::Null, json!({"reservoir_size": 10, "events_seen": 1})]; // len 2, missing events element
    let well_formed = vec![
        Value::Null,
        json!({"reservoir_size": 50, "events_seen": 2}),
        json!([{"id": "x"}, {"id": "y"}]),
    ];

    let merged = merge_event_list_payloads(vec![malformed, well_formed]);

    assert_eq!(merged[1]["events_seen"], json!(2), "malformed chunk's metadata must be skipped, not counted");
    assert_eq!(merged[1]["reservoir_size"], json!(50));
    let events = merged[2].as_array().unwrap();
    assert_eq!(events.len(), 2, "only the well-formed chunk's events must survive");
}

#[test]
fn merge_event_list_payloads_on_empty_input_returns_zeroed_shape() {
    let merged = merge_event_list_payloads(vec![]);
    assert_eq!(merged.len(), 3);
    assert_eq!(merged[1]["events_seen"], json!(0));
    assert_eq!(merged[1]["reservoir_size"], json!(0));
    assert_eq!(merged[2].as_array().unwrap().len(), 0);
}

#[test]
fn merge_event_list_payloads_tolerates_a_chunk_with_no_reservoir_metadata_fields() {
    // chunk[1] is a valid object but has neither events_seen nor reservoir_size —
    // both .get(...).and_then(...) calls must resolve to None without panicking.
    let chunk = vec![Value::Null, json!({}), json!([{"id": "x"}])];
    let merged = merge_event_list_payloads(vec![chunk]);
    assert_eq!(merged[1]["events_seen"], json!(0));
    assert_eq!(merged[1]["reservoir_size"], json!(0));
    assert_eq!(merged[2].as_array().unwrap().len(), 1);
}

#[test]
fn merge_event_list_payloads_tolerates_a_chunk_whose_third_element_is_not_an_array() {
    // chunk[2] present but not an array — .as_array() must resolve to None, skipping
    // this chunk's events rather than panicking.
    let chunk = vec![Value::Null, json!({"reservoir_size": 10, "events_seen": 1}), json!("not-an-array")];
    let merged = merge_event_list_payloads(vec![chunk]);
    assert_eq!(merged[1]["events_seen"], json!(1));
    assert_eq!(merged[1]["reservoir_size"], json!(10));
    assert_eq!(merged[2].as_array().unwrap().len(), 0);
}

#[test]
fn merge_event_list_payloads_tolerates_a_chunk_whose_second_element_is_not_an_object() {
    // chunk[1] present but not an object — .as_object() must resolve to None.
    let chunk = vec![Value::Null, json!("not-an-object"), json!([{"id": "x"}])];
    let merged = merge_event_list_payloads(vec![chunk]);
    assert_eq!(merged[1]["events_seen"], json!(0));
    assert_eq!(merged[1]["reservoir_size"], json!(0));
    assert_eq!(merged[2].as_array().unwrap().len(), 1);
}
