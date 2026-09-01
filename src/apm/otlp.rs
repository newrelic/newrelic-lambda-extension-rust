//! Minimal OTLP protobuf types for `entity.guid` injection.
//!
//! Prost 0.13 has **no unknown-field preservation**: `derive(Message)` skips any tag a
//! struct does not declare, so undeclared fields are lost on re-encode. Anything that
//! must survive is therefore either declared, or held as raw bytes — a length-delimited
//! `bytes` field is wire-identical to an embedded message, so prost hands back the
//! sub-message's exact bytes and writes them out unchanged.
//!
//! That is why [`ScopeMetrics::metrics_raw`] is `Vec<Vec<u8>>`: every data point (gauge,
//! sum, histogram, exponential histogram, summary) round-trips bit-for-bit without this
//! module needing to model it. Only `Resource::attributes` is decoded, because the
//! injection has to read and edit it.

use prost::Message;
use tracing::debug;

/// Mirrors `opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest`.
#[derive(Clone, PartialEq, Message)]
pub struct ExportMetricsServiceRequest {
    #[prost(message, repeated, tag = "1")]
    pub resource_metrics: Vec<ResourceMetrics>,
}

/// Mirrors `opentelemetry.proto.metrics.v1.ResourceMetrics`.
/// `schema_url` (tag 3) is not declared here, so prost's decode silently drops it
/// (prost 0.13 has no unknown-field preservation — see `ScopeMetrics::metrics_raw`'s
/// comment below for why that matters and how it's worked around for metric data).
#[derive(Clone, PartialEq, Message)]
pub struct ResourceMetrics {
    #[prost(message, optional, tag = "1")]
    pub resource: Option<Resource>,
    #[prost(message, repeated, tag = "2")]
    pub scope_metrics: Vec<ScopeMetrics>,
}

/// Mirrors `opentelemetry.proto.metrics.v1.ScopeMetrics`.
/// `schema_url` (tag 3) is not declared here, so it is silently dropped on decode
/// (see the `metrics_raw` comment below — prost has no unknown-field preservation).
#[derive(Clone, PartialEq, Message)]
pub struct ScopeMetrics {
    #[prost(message, optional, tag = "1")]
    pub scope: Option<InstrumentationScope>,
    // Stored as raw bytes, NOT decoded into a typed struct.
    // Prost 0.13 calls skip_field() for any tag not declared in a struct — there is no
    // automatic unknown-field preservation. Decoding into MetricMeta (name only) would
    // silently drop every data-point field: gauge(5), sum(6), histogram(7),
    // exp_histogram(9), summary(11), description(2), unit(3). Keeping raw bytes
    // guarantees the measurement values reach New Relic bit-for-bit.
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub metrics_raw: Vec<Vec<u8>>,
}

/// Mirrors `opentelemetry.proto.common.v1.InstrumentationScope`.
/// Only `name` (tag 1) decoded; version and attributes are not needed for injection.
#[derive(Clone, PartialEq, Message)]
pub struct InstrumentationScope {
    #[prost(string, tag = "1")]
    pub name: String,
}

/// Mirrors `opentelemetry.proto.resource.v1.Resource`.
/// `dropped_attributes_count` (tag 2) is not declared here, so it is silently
/// dropped on decode (prost has no unknown-field preservation).
#[derive(Clone, PartialEq, Message)]
pub struct Resource {
    #[prost(message, repeated, tag = "1")]
    pub attributes: Vec<KeyValue>,
}

/// Mirrors `opentelemetry.proto.common.v1.KeyValue`.
#[derive(Clone, PartialEq, Message)]
pub struct KeyValue {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(message, optional, tag = "2")]
    pub value: Option<AnyValue>,
}

/// Mirrors `opentelemetry.proto.common.v1.AnyValue`.
///
/// Only the `string_value` oneof variant (tag 1) is declared. Non-string values
/// (bool=2, int64=3, double=4, array=5, kvlist=6, bytes=7) are NOT preserved: prost
/// 0.13 has no unknown-field preservation, so their bytes are skipped on decode.
///
/// Note the precise failure mode — the `AnyValue` message still *decodes* (its frame
/// is present), leaving `value: Some(AnyValue { value: None })`, which would re-encode
/// as `12 00`: a `KeyValue` carrying a valueless value. That is arguably malformed
/// OTLP, so `inject_entity_guid` drops such attributes instead (see its `retain`).
///
/// Consequence: a resource attribute with a non-string value is **lost**. Metric data
/// points (gauge/sum/histogram/summary) are unaffected — they round-trip bit-for-bit
/// via `ScopeMetrics::metrics_raw`. Holding attributes as raw bytes the same way would
/// close this gap; deferred as it needs a hand-written reader for the injection logic.
#[derive(Clone, PartialEq, Message)]
pub struct AnyValue {
    #[prost(oneof = "AnyValueKind", tags = "1")]
    pub value: Option<AnyValueKind>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub enum AnyValueKind {
    #[prost(string, tag = "1")]
    StringValue(String),
}

impl AnyValue {
    pub fn string(s: impl Into<String>) -> Self {
        Self {
            value: Some(AnyValueKind::StringValue(s.into())),
        }
    }
}

/// Just the `name` field of `opentelemetry.proto.metrics.v1.Metric`, for reading a
/// metric name out of the raw bytes held in [`ScopeMetrics::metrics_raw`].
///
/// Decode-only: undeclared fields are skipped, which is exactly what we want here
/// because this value is never re-encoded — the original bytes are what get sent.
#[derive(Clone, PartialEq, Message)]
struct MetricName {
    #[prost(string, tag = "1")]
    name: String,
}

/// Metric name for debug logging. Malformed or truncated input yields a placeholder
/// rather than panicking, since this runs inside a `debug!` in a process that must
/// not crash the Lambda function.
fn metric_name_from_raw(raw: &[u8]) -> String {
    match MetricName::decode(raw) {
        Ok(m) if !m.name.is_empty() => m.name,
        Ok(_) => "<unknown>".to_string(),
        Err(_) => "<truncated>".to_string(),
    }
}

/// Error injecting `entity.guid` into an OTLP metrics payload.
#[derive(Debug, thiserror::Error)]
pub enum InjectEntityGuidError {
    #[error("failed to decode OTLP payload: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("failed to re-encode OTLP payload: {0}")]
    Encode(#[from] prost::EncodeError),
    /// `entity_guid` was empty or whitespace. Connect guarantees the field is
    /// *present* (`.context("Missing entity_guid…")?`) but not that it is non-empty,
    /// and injecting an empty value would recreate exactly the empty placeholder the
    /// logic below strips — yielding metrics no entity can be resolved from.
    #[error("refusing to inject an empty entity.guid")]
    EmptyEntityGuid,
}

/// Decode `bytes` as an `ExportMetricsServiceRequest`, add `entity.guid`
/// to the resource attributes of every `ResourceMetrics` entry (skipping
/// entries that already carry the attribute), then re-encode to bytes.
///
/// All metric data (`scope_metrics`, data points, histograms, etc.) is
/// preserved bit-for-bit because `ScopeMetrics.metrics_raw` holds raw bytes.
pub fn inject_entity_guid(bytes: &[u8], entity_guid: &str) -> Result<Vec<u8>, InjectEntityGuidError> {
    // Guarded here rather than at one call site so every caller is covered: writing an
    // empty entity.guid back would strip a real placeholder and replace it with an
    // identical empty one, leaving the metrics unattributable while appearing to succeed.
    if entity_guid.trim().is_empty() {
        return Err(InjectEntityGuidError::EmptyEntityGuid);
    }

    let mut req = ExportMetricsServiceRequest::decode(bytes)?;

    for rm in &mut req.resource_metrics {
        let resource = rm.resource.get_or_insert_with(Resource::default);

        // Drop attributes whose value did not survive decode. `AnyValue` here declares
        // only `string_value`, so a bool/int/double/array/kvlist value decodes to a
        // *present but empty* AnyValue and would re-encode as `12 00` — a KeyValue with
        // a valueless value, i.e. malformed OTLP. Dropping the attribute is lossy but
        // well-formed; emitting `12 00` is neither. Note the check is on the inner
        // oneof, not `value.is_some()`: the AnyValue message itself IS present.
        resource
            .attributes
            .retain(|kv| kv.value.as_ref().is_some_and(|v| v.value.is_some()));

        if tracing::enabled!(tracing::Level::DEBUG) {
            let service_name = resource.attributes.iter()
                .find(|kv| kv.key == "service.name")
                .and_then(|kv| kv.value.as_ref())
                .and_then(|v| if let Some(AnyValueKind::StringValue(s)) = &v.value { Some(s.as_str()) } else { None })
                .unwrap_or("unknown");

            for sm in &rm.scope_metrics {
                let scope_name = sm.scope.as_ref().map_or("unknown", |s| s.name.as_str());
                // metrics_raw holds each Metric as opaque bytes; extract names for logging only.
                let metric_names: Vec<String> = sm.metrics_raw.iter()
                    .map(|raw| metric_name_from_raw(raw))
                    .collect();
                debug!(
                    "OTLP metrics [service={}] scope='{}' ({} metrics): {:?}",
                    service_name, scope_name, metric_names.len(), metric_names
                );
            }
        }

        // The .NET Hybrid Agent sets entity.guid in resource attributes via AddAttributes(),
        // but in Lambda mode the value may be empty/null (agent has no guid of its own).
        // Only skip injection if a real non-empty guid is already present; otherwise replace.
        let key_present = resource.attributes.iter().any(|kv| kv.key == "entity.guid");
        if key_present {
            let existing_val = resource.attributes.iter()
                .find(|kv| kv.key == "entity.guid")
                .and_then(|kv| kv.value.as_ref())
                .and_then(|v| if let Some(AnyValueKind::StringValue(s)) = &v.value { Some(s.as_str()) } else { None })
                .unwrap_or("");

            if !existing_val.is_empty() {
                debug!("OTLP resource already has entity.guid='{}', skipping injection", existing_val);
                continue;
            }

            // Empty or null entity.guid — remove the placeholder and replace with ours
            debug!("OTLP resource has empty entity.guid, replacing with '{}'", entity_guid);
            resource.attributes.retain(|kv| kv.key != "entity.guid");
        } else {
            debug!("Injecting entity.guid='{}' into OTLP resource attributes", entity_guid);
        }

        resource.attributes.push(KeyValue {
            key: "entity.guid".to_string(),
            value: Some(AnyValue::string(entity_guid)),
        });
    }

    let mut out = Vec::with_capacity(req.encoded_len());
    req.encode(&mut out)?;

    if tracing::enabled!(tracing::Level::DEBUG) {
        let preview_len = out.len().min(64);
        let hex: String = out[..preview_len]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<Vec<_>>()
            .join(" ");
        debug!(
            "OTLP re-encoded payload: {} bytes, first {} bytes (hex): {}{}",
            out.len(),
            preview_len,
            hex,
            if out.len() > preview_len { " ..." } else { "" }
        );
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
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

    /// Build a `Metric` protobuf with a Sum (monotonic counter) data point.
    /// Sum = field 6; data_points = field 1; as_int = field 6 (varint in oneof).
    fn sum_metric(name: &str, value: i64, ts_ns: u64) -> Vec<u8> {
        let mut data_point = enc_fixed64(3, ts_ns);
        // as_int is field 6, wire 0 (varint, zigzag encoded for sint64)
        data_point.extend(enc_varint((6 << 3) | 0));
        data_point.extend(enc_varint(value as u64));
        let sum = enc_len(1, &data_point);
        [enc_str(1, name), enc_len(6, &sum)].concat()
    }

    /// Build a `ScopeMetrics` message containing given raw metric blobs.
    fn scope_metrics(scope_name: &str, metrics: &[Vec<u8>]) -> Vec<u8> {
        let scope = enc_str(1, scope_name);
        let mut out = enc_len(1, &scope);
        for m in metrics {
            out.extend(enc_len(2, m));
        }
        out
    }

    /// Build a complete `ExportMetricsServiceRequest` from raw attribute pairs
    /// and pre-built scope_metrics blobs.
    fn make_request_with_scopes(attrs: Vec<(&str, &str)>, scopes: &[Vec<u8>]) -> Vec<u8> {
        let kv_bytes: Vec<u8> = attrs.iter().flat_map(|(k, v)| {
            let value_msg = enc_len(1, v.as_bytes()); // AnyValue.string_value = field 1
            let kv = [enc_str(1, k), enc_len(2, &value_msg)].concat();
            enc_len(1, &kv) // Resource.attributes repeated field 1
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
        let attrs = &decoded.resource_metrics[0].resource.as_ref().unwrap().attributes;
        assert!(attrs.iter().any(|kv| kv.key == "entity.guid"));
        assert!(attrs.iter().any(|kv| kv.key == "service.name"));
    }

    #[test]
    fn skips_when_entity_guid_already_present() {
        let bytes = make_request(vec![("entity.guid", "existing")]);
        let out = inject_entity_guid(&bytes, "new-value").unwrap();
        let decoded = ExportMetricsServiceRequest::decode(out.as_slice()).unwrap();
        let attrs = &decoded.resource_metrics[0].resource.as_ref().unwrap().attributes;
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
        let attrs = &decoded.resource_metrics[0].resource.as_ref().unwrap().attributes;
        let guid_val = attrs.iter()
            .find(|kv| kv.key == "entity.guid")
            .and_then(|kv| kv.value.as_ref())
            .and_then(|v| if let Some(AnyValueKind::StringValue(s)) = &v.value { Some(s.as_str()) } else { None });
        assert_eq!(guid_val, Some("real-guid"));
    }

    #[test]
    fn entity_guid_value_matches_input() {
        let bytes = make_request(vec![("service.name", "fn")]);
        let guid = "MTAxOTYwODR8QVBNfEFQUExJQ0FUSU9OfDMxMTgwNDM0OA";
        let out = inject_entity_guid(&bytes, guid).unwrap();
        let decoded = ExportMetricsServiceRequest::decode(out.as_slice()).unwrap();
        let attrs = &decoded.resource_metrics[0].resource.as_ref().unwrap().attributes;
        let found = attrs.iter()
            .find(|kv| kv.key == "entity.guid")
            .and_then(|kv| kv.value.as_ref())
            .and_then(|v| if let Some(AnyValueKind::StringValue(s)) = &v.value { Some(s.as_str()) } else { None });
        assert_eq!(found, Some(guid));
    }

    // ---------------------------------------------------------------------------
    // Data-point preservation tests
    // ---------------------------------------------------------------------------

    #[test]
    fn gauge_data_points_preserved() {
        let metric = gauge_metric("cpu_usage", 73.5, 1_700_000_000_000_000_000);
        let scope = scope_metrics("my_scope", &[metric.clone()]);
        let bytes = make_request_with_scopes(vec![("service.name", "fn")], &[scope]);

        let out = inject_entity_guid(&bytes, "guid-abc").unwrap();

        let decoded = ExportMetricsServiceRequest::decode(out.as_slice()).unwrap();
        let raw = &decoded.resource_metrics[0].scope_metrics[0].metrics_raw[0];
        // Raw metric bytes must be bit-for-bit identical after round-trip
        assert_eq!(raw, &metric, "gauge data points were stripped during injection");
    }

    #[test]
    fn sum_data_points_preserved() {
        let metric = sum_metric("requests_total", 42, 1_700_000_000_000_000_000);
        let scope = scope_metrics("my_scope", &[metric.clone()]);
        let bytes = make_request_with_scopes(vec![("service.name", "fn")], &[scope]);

        let out = inject_entity_guid(&bytes, "guid-abc").unwrap();

        let decoded = ExportMetricsServiceRequest::decode(out.as_slice()).unwrap();
        let raw = &decoded.resource_metrics[0].scope_metrics[0].metrics_raw[0];
        assert_eq!(raw, &metric, "sum data points were stripped during injection");
    }

    #[test]
    fn multiple_scopes_and_metrics_all_preserved() {
        let g1 = gauge_metric("mem_mb", 512.0, 1_000_000);
        let g2 = gauge_metric("cpu_pct", 12.5, 1_000_000);
        let s1 = sum_metric("errors_total", 7, 1_000_000);

        let scope_a = scope_metrics("app.metrics", &[g1.clone(), g2.clone()]);
        let scope_b = scope_metrics("System.Net.Http", &[s1.clone()]);

        let bytes = make_request_with_scopes(
            vec![("service.name", "multi-fn")],
            &[scope_a, scope_b],
        );

        let out = inject_entity_guid(&bytes, "test-guid").unwrap();
        let decoded = ExportMetricsServiceRequest::decode(out.as_slice()).unwrap();
        let scopes = &decoded.resource_metrics[0].scope_metrics;

        assert_eq!(scopes.len(), 2);
        assert_eq!(scopes[0].metrics_raw.len(), 2);
        assert_eq!(scopes[0].metrics_raw[0], g1);
        assert_eq!(scopes[0].metrics_raw[1], g2);
        assert_eq!(scopes[1].metrics_raw.len(), 1);
        assert_eq!(scopes[1].metrics_raw[0], s1);
    }

    #[test]
    fn output_larger_than_input_by_entity_guid_size() {
        let guid = "test-entity-guid";
        let bytes = make_request(vec![("service.name", "fn")]);
        let input_len = bytes.len();

        let out = inject_entity_guid(&bytes, guid).unwrap();

        // Output must be larger — entity.guid bytes were added, nothing removed
        assert!(
            out.len() > input_len,
            "output ({}) should be larger than input ({})",
            out.len(), input_len
        );
    }

    // ---------------------------------------------------------------------------
    // Realistic .NET Hybrid Agent-style payload
    // Mirrors structure observed in production: two scopes, mixed metric types,
    // description + unit fields alongside data points.
    // ---------------------------------------------------------------------------

    /// Build a metric with description (field 2), unit (field 3), and a gauge (field 5).
    fn full_gauge_metric(name: &str, desc: &str, unit: &str, value: f64, ts_ns: u64) -> Vec<u8> {
        let data_point = [enc_fixed64(3, ts_ns), enc_double(4, value)].concat();
        let gauge = enc_len(1, &data_point);
        [
            enc_str(1, name),
            enc_str(2, desc),
            enc_str(3, unit),
            enc_len(5, &gauge),
        ].concat()
    }

    /// Build a sum metric with description, unit, and one integer data point.
    fn full_sum_metric(name: &str, desc: &str, unit: &str, value: i64, ts_ns: u64) -> Vec<u8> {
        let mut data_point = enc_fixed64(3, ts_ns);
        data_point.extend(enc_varint((6 << 3) | 0)); // as_int = field 6, wire 0
        data_point.extend(enc_varint(value as u64));
        let sum = enc_len(1, &data_point);
        [
            enc_str(1, name),
            enc_str(2, desc),
            enc_str(3, unit),
            enc_len(6, &sum),
        ].concat()
    }

    #[test]
    fn dotnet_agent_style_payload_metric_bytes_unchanged() {
        let ts: u64 = 1_700_000_000_000_000_000;

        // OtelMeterApp.Metrics scope — gauges and counters
        let m1 = full_gauge_metric("cpu_usage_percent", "Current CPU usage percentage", "%", 12.3, ts);
        let m2 = full_gauge_metric("memory_usage_mb", "Current memory usage in MB", "MB", 512.0, ts);
        let m3 = full_sum_metric("google_calls_total", "Total number of calls to Google", "{call}", 5, ts);
        let scope_app = scope_metrics("OtelMeterApp.Metrics", &[m1.clone(), m2.clone(), m3.clone()]);

        // System.Net.Http scope — http client gauges
        let m4 = full_gauge_metric("http.client.active_requests", "Active outbound HTTP requests", "{request}", 2.0, ts);
        let m5 = full_sum_metric("total_requests_processed", "Requests processed since startup", "{request}", 100, ts);
        let scope_http = scope_metrics("System.Net.Http", &[m4.clone(), m5.clone()]);

        let attrs = vec![
            ("telemetry.sdk.name", "opentelemetry"),
            ("telemetry.sdk.language", "dotnet"),
            ("telemetry.sdk.version", "10.51.0.0"),
            ("service.name", "ashish-otel-hybrid-testing"),
            ("service.instance.id", "8d8ff97d-03a0-4931-8448-2c4b80589c1d"),
        ];
        let bytes = make_request_with_scopes(attrs, &[scope_app, scope_http]);

        // Decode original to get baseline metric bytes
        let original = ExportMetricsServiceRequest::decode(bytes.as_slice()).unwrap();
        let orig_metrics: Vec<Vec<u8>> = original.resource_metrics[0]
            .scope_metrics.iter()
            .flat_map(|s| s.metrics_raw.clone())
            .collect();

        // Inject entity.guid
        let guid = "MTAxOTYwODR8QVBNfEFQUExJQ0FUSU9OfDMxMTgwNDM0OA";
        let out = inject_entity_guid(&bytes, guid).unwrap();
        assert!(out.len() > bytes.len(), "payload shrank — entity.guid was not added");

        let enriched = ExportMetricsServiceRequest::decode(out.as_slice()).unwrap();
        let rm = &enriched.resource_metrics[0];

        // entity.guid injected correctly
        let guid_val = rm.resource.as_ref().unwrap().attributes.iter()
            .find(|kv| kv.key == "entity.guid")
            .and_then(|kv| kv.value.as_ref())
            .and_then(|v| if let Some(AnyValueKind::StringValue(s)) = &v.value { Some(s.as_str()) } else { None });
        assert_eq!(guid_val, Some(guid));

        // Both scopes intact
        assert_eq!(rm.scope_metrics.len(), 2);
        assert_eq!(rm.scope_metrics[0].scope.as_ref().unwrap().name, "OtelMeterApp.Metrics");
        assert_eq!(rm.scope_metrics[1].scope.as_ref().unwrap().name, "System.Net.Http");

        // All metric bytes bit-for-bit identical
        let enr_metrics: Vec<Vec<u8>> = rm.scope_metrics.iter()
            .flat_map(|s| s.metrics_raw.clone())
            .collect();
        assert_eq!(orig_metrics.len(), enr_metrics.len(), "metric count changed");
        for (i, (orig, enr)) in orig_metrics.iter().zip(enr_metrics.iter()).enumerate() {
            assert_eq!(orig, enr,
                "metric[{i}] bytes changed — description/unit/data-points were stripped");
        }
    }

    #[test]
    fn metric_with_description_and_unit_preserved() {
        // Regression: ensure fields beyond name (description=2, unit=3) survive injection
        let ts: u64 = 1_700_000_000_000_000_000;
        let metric = full_gauge_metric(
            "request_latency_ms",
            "P99 request latency in milliseconds",
            "ms",
            45.7,
            ts,
        );
        let scope = scope_metrics("app", &[metric.clone()]);
        let bytes = make_request_with_scopes(vec![("service.name", "svc")], &[scope]);

        let out = inject_entity_guid(&bytes, "guid-xyz").unwrap();
        let decoded = ExportMetricsServiceRequest::decode(out.as_slice()).unwrap();
        let raw = &decoded.resource_metrics[0].scope_metrics[0].metrics_raw[0];

        assert_eq!(raw, &metric,
            "description/unit/gauge fields were stripped — only name survived");
    }

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
    fn metric_name_from_raw_rejects_length_prefix_near_usize_max() {
        // A maliciously large varint that would overflow `pos + len` on 32-bit
        // targets (or any target once len approaches usize::MAX) must be caught
        // by the checked_add rather than wrapping into a bogus in-bounds index.
        let tag = enc_varint((1 << 3) | 2);
        let huge_len = enc_varint(u64::MAX);
        let mut raw = tag;
        raw.extend(huge_len);
        raw.extend_from_slice(b"trailing");

        assert_eq!(metric_name_from_raw(&raw), "<truncated>");
    }

    #[test]
    fn metric_name_from_raw_handles_truncated_tag_varint() {
        // A tag byte with the continuation bit set but nothing after it.
        let raw = vec![0x80];
        assert_eq!(metric_name_from_raw(&raw), "<truncated>");
    }

    #[test]
    fn metric_name_from_raw_handles_truncated_length_varint() {
        // Field 1, wire type 2, but the length varint itself is cut off mid-byte.
        let mut raw = enc_varint((1 << 3) | 2);
        raw.push(0x80); // continuation bit set, no following byte
        assert_eq!(metric_name_from_raw(&raw), "<truncated>");
    }

    #[test]
    fn metric_name_from_raw_skips_non_name_fields_without_panicking() {
        // description=2 (string) then name=1 (string) — exercises the "pos = end"
        // skip path for a non-target field before finding the real name.
        let mut raw = enc_str(2, "some description");
        raw.extend(enc_str(1, "the_metric_name"));
        assert_eq!(metric_name_from_raw(&raw), "the_metric_name");
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

    #[test]
    fn empty_guid_guard_fires_before_any_mutation() {
        // A payload already carrying a good guid must not be stripped by the rejected call.
        let payload = make_request(vec![("entity.guid", "REAL-GUID-123")]);
        assert!(inject_entity_guid(&payload, "").is_err());

        let out = inject_entity_guid(&payload, "OTHER").expect("valid guid");
        let decoded = ExportMetricsServiceRequest::decode(&out[..]).unwrap();
        let attrs = &decoded.resource_metrics[0].resource.as_ref().unwrap().attributes;
        let guid = attrs.iter().find(|kv| kv.key == "entity.guid")
            .and_then(|kv| kv.value.as_ref())
            .and_then(|v| match &v.value {
                Some(AnyValueKind::StringValue(s)) => Some(s.as_str()),
                _ => None,
            });
        assert_eq!(guid, Some("REAL-GUID-123"), "existing non-empty guid must win");
    }

    // -----------------------------------------------------------------------
    // Review finding 2 (minimal form): a value that didn't survive decode must be
    // dropped, not re-encoded as `12 00` (a KeyValue with a valueless value).
    // -----------------------------------------------------------------------

    #[test]
    fn non_string_attribute_is_dropped_not_emitted_as_empty_value() {
        // Hand-build a bool attribute (AnyValue.bool_value = field 2, varint) — the
        // mirrored AnyValue declares only string_value, so its inner oneof decodes to
        // None while the AnyValue frame itself is present.
        let mut bool_val = enc_varint(2 << 3); // field 2, wire 0
        bool_val.extend(enc_varint(1));        // true
        let bool_kv = [enc_str(1, "is_prod"), enc_len(2, &bool_val)].concat();
        let str_kv = [enc_str(1, "service.name"), enc_len(2, &enc_len(1, b"svc"))].concat();

        let attrs: Vec<u8> = [enc_len(1, &bool_kv), enc_len(1, &str_kv)].concat();
        let rm = enc_len(1, &attrs);
        let payload = enc_len(1, &rm);

        // Sanity: decoding leaves the value present-but-empty (the shape that would
        // otherwise re-encode as the malformed `12 00`).
        let pre = ExportMetricsServiceRequest::decode(&payload[..]).unwrap();
        let pre_attrs = &pre.resource_metrics[0].resource.as_ref().unwrap().attributes;
        let pre_bool = pre_attrs.iter().find(|kv| kv.key == "is_prod").unwrap();
        assert!(pre_bool.value.is_some(), "AnyValue frame decodes as present");
        assert!(
            pre_bool.value.as_ref().unwrap().value.is_none(),
            "but its inner oneof is None - this is what the retain() guard catches"
        );

        let out = inject_entity_guid(&payload, "GUID").expect("must inject");

        // `12 00` (field 2, length 0) must not appear anywhere in the output.
        assert!(
            !out.windows(2).any(|w| w == [0x12, 0x00]),
            "malformed empty value re-encoded; out={out:02x?}"
        );

        let decoded = ExportMetricsServiceRequest::decode(&out[..]).unwrap();
        let attrs = &decoded.resource_metrics[0].resource.as_ref().unwrap().attributes;
        assert!(
            !attrs.iter().any(|kv| kv.key == "is_prod"),
            "non-string attribute must be dropped, not emitted valueless"
        );
        // Well-formed neighbours and the injection are unaffected.
        assert!(attrs.iter().any(|kv| kv.key == "service.name"));
        assert!(attrs.iter().any(|kv| kv.key == "entity.guid"));
    }


}
