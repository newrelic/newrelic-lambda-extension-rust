// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;

// ---------------------------------------------------------------------------
// Protobuf helpers — manual encoding so tests don't depend on generated types
// ---------------------------------------------------------------------------

fn enc_varint(mut n: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 { out.push(byte); break; }
        out.push(byte | 0x80);
    }
    out
}

fn enc_len(field: u32, data: &[u8]) -> Vec<u8> {
    let mut out = enc_varint(((field as u64) << 3) | 2);
    out.extend(enc_varint(data.len() as u64));
    out.extend_from_slice(data);
    out
}

fn enc_str(field: u32, s: &str) -> Vec<u8> { enc_len(field, s.as_bytes()) }

fn enc_fixed64(field: u32, v: u64) -> Vec<u8> {
    let mut out = enc_varint(((field as u64) << 3) | 1);
    out.extend_from_slice(&v.to_le_bytes());
    out
}

fn enc_double(field: u32, v: f64) -> Vec<u8> { enc_fixed64(field, v.to_bits()) }

/// Build a `Metric` protobuf with a Gauge data point.
/// Gauge = field 5; NumberDataPoint = field 1 inside Gauge;
/// as_double = field 4, time_unix_nano = field 3 (fixed64).
fn gauge_metric(name: &str, value: f64, ts_ns: u64) -> Vec<u8> {
    let data_point = [enc_fixed64(3, ts_ns), enc_double(4, value)].concat();
    let gauge = enc_len(1, &data_point);
    [enc_str(1, name), enc_len(5, &gauge)].concat()
}

/// Build a complete `ExportMetricsServiceRequest` from raw attribute pairs
/// and pre-built scope_metrics blobs.
fn make_request_with_scopes(attrs: Vec<(&str, &str)>, scopes: &[Vec<u8>]) -> Vec<u8> {
    let kv_bytes: Vec<u8> = attrs.iter().flat_map(|(k, v)| {
        let value_msg = enc_len(1, v.as_bytes()); // AnyValue.string_value = field 1
        let kv = [enc_str(1, k), enc_len(2, &value_msg)].concat();
        enc_len(1, &kv) // Resource.attributes() repeated field 1
    }).collect();
    let resource = enc_len(1, &kv_bytes);
    let mut rm = resource;
    for s in scopes {
        rm.extend(enc_len(2, s));
    }
    enc_len(1, &rm) // ExportMetricsServiceRequest.resource_metrics field 1
}

fn make_request(attrs: Vec<(&str, &str)>) -> Vec<u8> {
    make_request_with_scopes(attrs, &[])
}

// ---------------------------------------------------------------------------
// entity.guid injection tests
// ---------------------------------------------------------------------------

#[test]
fn adds_entity_guid_when_absent() {
    let bytes = make_request(vec![("service.name", "my-fn")]);
    let out = inject_entity_guid(&bytes, "abc123").unwrap();
    let decoded = ExportMetricsServiceRequest::decode(out.as_slice()).unwrap();
    let attrs = &decoded.resource_metrics[0].resource.as_ref().unwrap().attributes();
    let guid = attrs.iter().find(|kv| kv.key == "entity.guid").and_then(KeyValue::string_value);
    assert_eq!(guid, Some("abc123"));
    assert!(attrs.iter().any(|kv| kv.key == "service.name"));
}

#[test]
fn skips_when_entity_guid_already_present() {
    let bytes = make_request(vec![("entity.guid", "existing")]);
    let out = inject_entity_guid(&bytes, "new-value").unwrap();
    let decoded = ExportMetricsServiceRequest::decode(out.as_slice()).unwrap();
    let attrs = &decoded.resource_metrics[0].resource.as_ref().unwrap().attributes();
    let guids: Vec<_> = attrs.iter().filter(|kv| kv.key == "entity.guid").collect();
    assert_eq!(guids.len(), 1);
    assert_eq!(
        guids[0].value.as_ref().unwrap().value,
        Some(AnyValueKind::StringValue("existing".to_string()))
    );
}

#[test]
fn replaces_empty_entity_guid() {
    let bytes = make_request(vec![("entity.guid", "")]);
    let out = inject_entity_guid(&bytes, "real-guid").unwrap();
    let decoded = ExportMetricsServiceRequest::decode(out.as_slice()).unwrap();
    let attrs = &decoded.resource_metrics[0].resource.as_ref().unwrap().attributes();
    let guid_val = attrs.iter()
        .find(|kv| kv.key == "entity.guid")
        .and_then(|kv| kv.value.as_ref())
        .and_then(|v| if let Some(AnyValueKind::StringValue(s)) = &v.value { Some(s.as_str()) } else { None });
    assert_eq!(guid_val, Some("real-guid"));
}

// ---------------------------------------------------------------------------
// Data-point preservation tests
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Realistic .NET Hybrid Agent-style payload
// Mirrors structure observed in production: two scopes, mixed metric types,
// description + unit fields alongside data points.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// metric_name_from_raw — malformed/truncated input must never panic
// ---------------------------------------------------------------------------

#[test]
fn metric_name_from_raw_handles_well_formed_input() {
    let metric = gauge_metric("cpu_usage", 12.5, 1_000);
    assert_eq!(metric_name_from_raw(&metric), "cpu_usage");
}

#[test]
fn metric_name_from_raw_rejects_length_prefix_exceeding_buffer() {
    // Field 1 (name), wire type 2 (length-delimited), claims a length far
    // longer than the bytes actually remaining. This is exactly the shape
    // that used to compute `end = pos + len` without checking `end` against
    // `raw.len()` before the *next* loop iteration's slicing — must return
    // "<truncated>" instead of panicking.
    let tag = enc_varint((1 << 3) | 2); // field 1, wire type 2
    let huge_len = enc_varint(1_000_000);
    let mut raw = tag;
    raw.extend(huge_len);
    raw.extend_from_slice(b"short"); // far fewer bytes than claimed

    assert_eq!(metric_name_from_raw(&raw), "<truncated>");
}

#[test]
fn metric_name_from_raw_handles_empty_input() {
    assert_eq!(metric_name_from_raw(&[]), "<unknown>");
}

// -----------------------------------------------------------------------
// Review finding 3: empty entity_guid must not be injected
// -----------------------------------------------------------------------

#[test]
fn empty_or_whitespace_entity_guid_is_rejected() {
    // Connect guarantees entity_guid is present, not non-empty. Injecting "" would
    // strip a placeholder and write back an identical one, so the payload would look
    // enriched while being unattributable.
    let payload = make_request(vec![("service.name", "svc")]);
    for guid in ["", "   ", "\t\n"] {
        let err = inject_entity_guid(&payload, guid)
            .expect_err("empty/whitespace guid must be rejected");
        assert!(
            matches!(err, InjectEntityGuidError::EmptyEntityGuid),
            "guid={guid:?} gave {err:?}"
        );
    }
}

// -----------------------------------------------------------------------
// Payload fidelity: everything except the entity.guid attribute round-trips
// byte-for-byte.
// -----------------------------------------------------------------------

/// One encoded `KeyValue` whose `AnyValue` is the given raw bytes.
fn kv_raw(key: &str, any_value: &[u8]) -> Vec<u8> {
    [enc_str(1, key), enc_len(2, any_value)].concat()
}

#[test]
fn every_attribute_value_type_survives_byte_for_byte() {
    let int_kv = kv_raw("process.pid", &[enc_varint(3 << 3), enc_varint(42)].concat());
    let bool_kv = kv_raw("cloud.cold_start", &[enc_varint(2 << 3), enc_varint(1)].concat());
    let dbl_kv = kv_raw("ratio", &[enc_varint((4 << 3) | 1), 1.5f64.to_bits().to_le_bytes().to_vec()].concat());
    let arr_kv = kv_raw("tags", &enc_len(5, &enc_len(1, &enc_len(1, b"a"))));
    let str_kv = kv_raw("service.name", &enc_len(1, b"svc"));
    let originals = [int_kv, bool_kv, dbl_kv, arr_kv, str_kv];

    let attrs: Vec<u8> = originals.iter().flat_map(|kv| enc_len(1, kv)).collect();
    let payload = enc_len(1, &enc_len(1, &attrs));
    let out = inject_entity_guid(&payload, "GUID").expect("must inject");

    let resource = ExportMetricsServiceRequest::decode(&out[..]).unwrap().resource_metrics[0]
        .resource
        .clone()
        .unwrap();
    assert_eq!(&resource.attributes_raw[..originals.len()], &originals[..]);
    assert_eq!(resource.attributes_raw.len(), originals.len() + 1, "only entity.guid added");
}

#[test]
fn scope_and_resource_metadata_survive_byte_for_byte() {
    let scope = [
        enc_str(1, "io.opentelemetry.demo"),
        enc_str(2, "1.59.0"),
        enc_len(3, &kv_raw("scope.attr", &enc_len(1, b"x"))),
    ]
    .concat();
    let scope_metrics = [
        enc_len(1, &scope),
        enc_len(2, &gauge_metric("m", 1.0, 1)),
        enc_str(3, "https://schema/scope"),
    ]
    .concat();
    let resource = [
        enc_len(1, &kv_raw("service.name", &enc_len(1, b"svc"))),
        enc_varint(2 << 3),
        enc_varint(7), // dropped_attributes_count
        enc_len(3, &enc_str(1, "service")), // entity_refs
    ]
    .concat();
    let rm = [
        enc_len(1, &resource),
        enc_len(2, &scope_metrics),
        enc_str(3, "https://schema/resource"),
    ]
    .concat();

    let out = inject_entity_guid(&enc_len(1, &rm), "GUID").expect("must inject");
    let rm_out = &ExportMetricsServiceRequest::decode(&out[..]).unwrap().resource_metrics[0];
    let res_out = rm_out.resource.as_ref().unwrap();

    assert_eq!(rm_out.scope_metrics_raw, vec![scope_metrics]);
    assert_eq!(rm_out.schema_url, "https://schema/resource");
    assert_eq!(res_out.dropped_attributes_count, 7);
    assert_eq!(res_out.entity_refs_raw, vec![enc_str(1, "service")]);
}

