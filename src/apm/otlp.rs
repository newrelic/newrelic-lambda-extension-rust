//! Minimal OTLP protobuf types for `entity.guid` injection.
//!
//! Prost 0.13 has no unknown-field preservation: `derive(Message)` skips any tag a
//! struct does not declare, so an undeclared field is lost on re-encode. Every part of
//! the payload this module does not edit is therefore held as raw bytes. A
//! length-delimited `bytes` field is wire-identical to an embedded message, so prost
//! returns each sub-message's exact bytes and writes them back unchanged.
//!
//! The only edited part is `Resource.attributes`, and it is held as raw bytes too: an
//! attribute is decoded just to read its key and string value, and the encoded form of
//! the one `entity.guid` attribute this module builds is appended. A forwarded payload
//! is the agent's payload plus that attribute, minus any `entity.guid` placeholder it
//! replaces.
//!
//! The types marked *decode-only* are read views over raw bytes and are never
//! re-encoded, so their undeclared fields are irrelevant. The re-encoded types —
//! `ExportMetricsServiceRequest`, `ResourceMetrics` and `Resource` — declare every
//! field of the OTLP proto, so a field added to them in a later OTLP version must be
//! declared here to survive.

use prost::Message;
use tracing::debug;

/// Mirrors `opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest`.
#[derive(Clone, PartialEq, Message)]
pub struct ExportMetricsServiceRequest {
    #[prost(message, repeated, tag = "1")]
    pub resource_metrics: Vec<ResourceMetrics>,
}

/// Mirrors `opentelemetry.proto.metrics.v1.ResourceMetrics`.
#[derive(Clone, PartialEq, Message)]
pub struct ResourceMetrics {
    #[prost(message, optional, tag = "1")]
    pub resource: Option<Resource>,
    /// Each encoded `ScopeMetrics`: scope name, version and attributes, every metric and
    /// its data points, and `schema_url`.
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub scope_metrics_raw: Vec<Vec<u8>>,
    #[prost(string, tag = "3")]
    pub schema_url: String,
}

impl ResourceMetrics {
    /// Decode-only view of each scope, for logging. A scope whose bytes do not decode is
    /// absent here and still forwarded from `scope_metrics_raw`.
    pub(crate) fn scope_metrics(&self) -> Vec<ScopeMetrics> {
        self.scope_metrics_raw
            .iter()
            .filter_map(|raw| ScopeMetrics::decode(raw.as_slice()).ok())
            .collect()
    }
}

/// Mirrors `opentelemetry.proto.metrics.v1.ScopeMetrics`. Decode-only.
#[derive(Clone, PartialEq, Message)]
pub struct ScopeMetrics {
    #[prost(message, optional, tag = "1")]
    pub scope: Option<InstrumentationScope>,
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub metrics_raw: Vec<Vec<u8>>,
}

/// Mirrors `opentelemetry.proto.common.v1.InstrumentationScope`. Decode-only.
#[derive(Clone, PartialEq, Message)]
pub struct InstrumentationScope {
    #[prost(string, tag = "1")]
    pub name: String,
}

/// Mirrors `opentelemetry.proto.resource.v1.Resource`.
#[derive(Clone, PartialEq, Message)]
pub struct Resource {
    /// Each encoded `KeyValue`, of any value type.
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub attributes_raw: Vec<Vec<u8>>,
    #[prost(uint32, tag = "2")]
    pub dropped_attributes_count: u32,
    /// Each encoded `EntityRef`.
    #[prost(bytes = "vec", repeated, tag = "3")]
    pub entity_refs_raw: Vec<Vec<u8>>,
}

impl Resource {
    /// Decode-only view of each attribute. An attribute whose bytes do not decode is
    /// absent here and still forwarded from `attributes_raw`.
    pub(crate) fn attributes(&self) -> Vec<KeyValue> {
        self.attributes_raw
            .iter()
            .filter_map(|raw| KeyValue::decode(raw.as_slice()).ok())
            .collect()
    }
}

/// Mirrors `opentelemetry.proto.common.v1.KeyValue`. Encoded only for the injected
/// `entity.guid`; otherwise decode-only.
#[derive(Clone, PartialEq, Message)]
pub struct KeyValue {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(message, optional, tag = "2")]
    pub value: Option<AnyValue>,
}

impl KeyValue {
    /// The value when it is a `string_value`. `None` for every other value type, and for
    /// an `AnyValue` with no variant set.
    pub(crate) fn string_value(&self) -> Option<&str> {
        match self.value.as_ref()?.value.as_ref()? {
            AnyValueKind::StringValue(s) => Some(s.as_str()),
        }
    }
}

/// Mirrors `opentelemetry.proto.common.v1.AnyValue`, restricted to `string_value`:
/// the only value this module builds is the `entity.guid` string.
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

/// The `name` field of `opentelemetry.proto.metrics.v1.Metric`. Decode-only.
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
    /// `entity_guid` was empty or whitespace. Connect guarantees the field is present,
    /// not non-empty, and an empty value attributes the metrics to no entity.
    #[error("refusing to inject an empty entity.guid")]
    EmptyEntityGuid,
}

/// Add `entity.guid` to the resource attributes of every `ResourceMetrics` entry in an
/// encoded `ExportMetricsServiceRequest`, and return the re-encoded request.
///
/// An existing non-empty string `entity.guid` is kept. Any other existing `entity.guid`
/// — an empty string, or an `AnyValue` with no variant set, as the Java agent sends — is
/// a placeholder and is replaced.
pub fn inject_entity_guid(bytes: &[u8], entity_guid: &str) -> Result<Vec<u8>, InjectEntityGuidError> {
    if entity_guid.trim().is_empty() {
        return Err(InjectEntityGuidError::EmptyEntityGuid);
    }

    let mut req = ExportMetricsServiceRequest::decode(bytes)?;

    for rm in &mut req.resource_metrics {
        let attributes = rm.resource.as_ref().map(Resource::attributes).unwrap_or_default();

        if tracing::enabled!(tracing::Level::DEBUG) {
            let service_name = attributes
                .iter()
                .find(|kv| kv.key == "service.name")
                .and_then(KeyValue::string_value)
                .unwrap_or("unknown");
            for sm in rm.scope_metrics() {
                let scope_name = sm.scope.as_ref().map_or("unknown", |s| s.name.as_str());
                let metric_names: Vec<String> =
                    sm.metrics_raw.iter().map(|raw| metric_name_from_raw(raw)).collect();
                debug!(
                    "OTLP metrics [service={}] scope='{}' ({} metrics): {:?}",
                    service_name, scope_name, metric_names.len(), metric_names
                );
            }
        }

        // An agent in Lambda mode has no entity guid of its own, so it may send an
        // entity.guid placeholder for the extension to fill.
        let resource = rm.resource.get_or_insert_with(Resource::default);
        let existing = attributes.iter().find(|kv| kv.key == "entity.guid");
        match existing.and_then(KeyValue::string_value) {
            Some(guid) if !guid.is_empty() => {
                debug!("OTLP resource already has entity.guid='{}', skipping injection", guid);
                continue;
            }
            _ if existing.is_some() => {
                debug!("OTLP resource has an entity.guid placeholder, replacing with '{}'", entity_guid);
                resource.attributes_raw.retain(|raw| {
                    KeyValue::decode(raw.as_slice()).map_or(true, |kv| kv.key != "entity.guid")
                });
            }
            _ => debug!("Injecting entity.guid='{}' into OTLP resource attributes", entity_guid),
        }

        resource.attributes_raw.push(
            KeyValue {
                key: "entity.guid".to_string(),
                value: Some(AnyValue::string(entity_guid)),
            }
            .encode_to_vec(),
        );
    }

    let mut out = Vec::with_capacity(req.encoded_len());
    req.encode(&mut out)?;
    Ok(out)
}

#[cfg(test)]
#[path = "otlp_tests.rs"]
mod otlp_tests;
