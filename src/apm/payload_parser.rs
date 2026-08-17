// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Agent payload parsing for APM mode
//!
//! Parses New Relic agent telemetry payloads (protocol v1 and v2)
//! Based on apm_payload.go GetServerlessData()

use anyhow::{Result, anyhow};
use base64::{Engine as _, engine::general_purpose};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Read;
use tracing::{debug, trace};

/// Lambda telemetry data structure (protocol v2)
#[derive(Debug, Clone, Default)]
pub struct LambdaData {
    pub metric_data: Vec<Value>,
    pub custom_event_data: Vec<Value>,
    pub log_event_data: Vec<Value>,
    pub analytic_event_data: Vec<Value>,
    pub error_event_data: Vec<Value>,
    pub error_data: Vec<Value>,
    pub span_event_data: Vec<Value>,
    pub sql_trace_data: Vec<Value>,
    pub transaction_sample_data: Vec<Value>,
    /// Base64-encoded OTLP ExportMetricsServiceRequest protobuf entries
    pub otlp_payload: Vec<String>,
}

/// Protocol v1 wrapper with metadata and data fields
#[derive(Debug, Clone, serde::Deserialize)]
pub struct LambdaRawData {
    pub data: LambdaData,
}

/// Parse agent payload and return telemetry data by type
/// Returns (data_map, protocol_version)
pub fn parse_agent_payload(payload_bytes: &[u8]) -> Result<(HashMap<String, Vec<Value>>, u8)> {
    let payload_str = std::str::from_utf8(payload_bytes)?;
    
    if payload_str.is_empty() || !payload_str.trim_start().starts_with('[') {
        return Ok((HashMap::new(), 0));
    }

    let json_data = payload_str.trim().trim_start_matches('[').trim_end_matches(']');
    
    let components: Vec<&str> = json_data.split(',').collect();
    if components.len() < 2 {
        return Err(anyhow!("Insufficient data components"));
    }

    let protocol_version = components[0].trim().trim_matches('"');
    
    let encoded_part = components[components.len() - 1].trim().trim_matches('"');

    debug!("Parsing agent payload: protocol version = {}", protocol_version);

    let uncompressed_json = decode_uncompress(encoded_part)?;

    let (data_map, version) = match protocol_version {
        "2" => {
            // Escape newlines to prevent log corruption when captured by Lambda Telemetry API
            let sanitized_payload = String::from_utf8_lossy(&uncompressed_json)
                .replace('\n', "\\n")
                .replace('\r', "\\r");
            trace!("Parsing v2 payload: {}", sanitized_payload);
            let lambda_data: LambdaData = serde_json::from_slice(&uncompressed_json)
                .map_err(|e| {
                    let preview: String = String::from_utf8_lossy(&uncompressed_json)
                        .chars()
                        .take(500)
                        .collect::<String>()
                        .replace('\n', "\\n")
                        .replace('\r', "\\r");
                    anyhow!("Failed to parse v2 payload: {} - Payload preview: {}", e, preview)
                })?;
            (convert_lambda_data_to_map(lambda_data), 2)
        }
        "1" | _ => {
            let wrapper: LambdaRawData = serde_json::from_slice(&uncompressed_json)
                .map_err(|e| {
                    let preview: String = String::from_utf8_lossy(&uncompressed_json)
                        .chars()
                        .take(500)
                        .collect::<String>()
                        .replace('\n', "\\n")
                        .replace('\r', "\\r");
                    anyhow!("Failed to parse v1 payload: {} - Payload preview: {}", e, preview)
                })?;
            (convert_lambda_data_to_map(wrapper.data), 1)
        }
    };

    debug!("Parsed {} telemetry types from agent payload", data_map.len());
    Ok((data_map, version))
}

/// Decode base64 and decompress gzip data
fn decode_uncompress(input: &str) -> Result<Vec<u8>> {
    trace!("Decoding base64 string ({} chars)", input.len());

    let decoded = general_purpose::STANDARD.decode(input)
        .map_err(|e| anyhow!("Base64 decode error: {}", e))?;

    debug!("Base64 decoded to {} bytes", decoded.len());

    let mut decoder = GzDecoder::new(&decoded[..]);
    let mut uncompressed = Vec::new();
    decoder.read_to_end(&mut uncompressed)
        .map_err(|e| anyhow!("Gzip decompression error: {}", e))?;

    debug!("Decompressed to {} bytes", uncompressed.len());
    Ok(uncompressed)
}

/// Convert LambdaData to HashMap for easier processing
fn convert_lambda_data_to_map(data: LambdaData) -> HashMap<String, Vec<Value>> {
    let mut map = HashMap::new();

    if !data.metric_data.is_empty() {
        map.insert("metric_data".to_string(), data.metric_data);
    }
    if !data.custom_event_data.is_empty() {
        map.insert("custom_event_data".to_string(), data.custom_event_data);
    }
    if !data.log_event_data.is_empty() {
        map.insert("log_event_data".to_string(), data.log_event_data);
    }
    if !data.analytic_event_data.is_empty() {
        map.insert("analytic_event_data".to_string(), data.analytic_event_data);
    }
    if !data.error_event_data.is_empty() {
        map.insert("error_event_data".to_string(), data.error_event_data);
    }
    if !data.error_data.is_empty() {
        map.insert("error_data".to_string(), data.error_data);
    }
    if !data.span_event_data.is_empty() {
        map.insert("span_event_data".to_string(), data.span_event_data);
    }
    if !data.sql_trace_data.is_empty() {
        map.insert("sql_trace_data".to_string(), data.sql_trace_data);
    }
    if !data.transaction_sample_data.is_empty() {
        map.insert("transaction_sample_data".to_string(), data.transaction_sample_data);
    }
    if !data.otlp_payload.is_empty() {
        // Store as Vec<Value::String> so it fits the shared map type
        map.insert(
            "otlp_payload".to_string(),
            data.otlp_payload.into_iter().map(Value::String).collect(),
        );
    }

    map
}

/// Implement Deserialize for LambdaData manually to handle flexible field names
/// Supports both snake_case and camelCase field names for compatibility
impl<'de> serde::Deserialize<'de> for LambdaData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw_map: HashMap<String, Value> = HashMap::deserialize(deserializer)?;
        
        fn get_field(map: &HashMap<String, Value>, snake_case: &str, camel_case: &str) -> Vec<Value> {
            let value = map.get(snake_case).or_else(|| map.get(camel_case));
            match value {
                Some(Value::Array(arr)) => arr.clone(),
                _ => Vec::new(),
            }
        }

        fn get_string_array(map: &HashMap<String, Value>, snake_case: &str, camel_case: &str) -> Vec<String> {
            let value = map.get(snake_case).or_else(|| map.get(camel_case));
            match value {
                Some(Value::Array(arr)) => arr
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect(),
                _ => Vec::new(),
            }
        }

        Ok(LambdaData {
            metric_data: get_field(&raw_map, "metric_data", "metricData"),
            custom_event_data: get_field(&raw_map, "custom_event_data", "customEventData"),
            log_event_data: get_field(&raw_map, "log_event_data", "logEventData"),
            analytic_event_data: get_field(&raw_map, "analytic_event_data", "analyticEventData"),
            error_event_data: get_field(&raw_map, "error_event_data", "errorEventData"),
            error_data: get_field(&raw_map, "error_data", "errorData"),
            span_event_data: get_field(&raw_map, "span_event_data", "spanEventData"),
            sql_trace_data: get_field(&raw_map, "sql_trace_data", "sqlTraceData"),
            transaction_sample_data: get_field(&raw_map, "transaction_sample_data", "transactionSampleData"),
            otlp_payload: get_string_array(&raw_map, "otlp_payload", "otlpPayload"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
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

    fn create_test_payload_with_json(json_body: &str) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(json_body.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        let encoded = general_purpose::STANDARD.encode(&compressed);
        format!(r#"["2", "NR_LAMBDA_MONITORING", "{encoded}"]"#).into_bytes()
    }

    #[test]
    fn test_otlp_payload_snake_case_key() {
        let payload = create_test_payload_with_json(r#"{"otlp_payload": ["abc123"]}"#);
        let (data_map, _version) = parse_agent_payload(&payload).unwrap();

        let entries = data_map.get("otlp_payload").expect("otlp_payload key missing");
        assert_eq!(entries, &vec![Value::String("abc123".to_string())]);
    }

    #[test]
    fn test_otlp_payload_camel_case_key_fallback() {
        // .NET-style JSON serializers commonly default to camelCase — must not
        // silently drop otlp_payload if the agent emits "otlpPayload" instead.
        let payload = create_test_payload_with_json(r#"{"otlpPayload": ["def456"]}"#);
        let (data_map, _version) = parse_agent_payload(&payload).unwrap();

        let entries = data_map.get("otlp_payload").expect("otlp_payload key missing (camelCase fallback failed)");
        assert_eq!(entries, &vec![Value::String("def456".to_string())]);
    }

    #[test]
    fn test_otlp_payload_snake_case_takes_precedence_over_camel_case() {
        let payload = create_test_payload_with_json(
            r#"{"otlp_payload": ["snake"], "otlpPayload": ["camel"]}"#,
        );
        let (data_map, _version) = parse_agent_payload(&payload).unwrap();

        let entries = data_map.get("otlp_payload").unwrap();
        assert_eq!(entries, &vec![Value::String("snake".to_string())]);
    }

    #[test]
    fn test_otlp_payload_absent_key_yields_no_map_entry() {
        let payload = create_test_payload_with_json(r#"{"metric_data": [[1, 2, 3]]}"#);
        let (data_map, _version) = parse_agent_payload(&payload).unwrap();

        assert!(!data_map.contains_key("otlp_payload"));
    }

    #[test]
    fn test_otlp_payload_multi_element_array_keeps_every_entry_in_order() {
        // The agent may batch several OTLP requests into one array. Every element
        // must survive parsing (not just the first) and keep its original order,
        // since send_otlp_payload numbers them 1..N for log correlation.
        let payload = create_test_payload_with_json(
            r#"{"otlp_payload": ["first", "second", "third", "fourth"]}"#,
        );
        let (data_map, _version) = parse_agent_payload(&payload).unwrap();

        let entries = data_map
            .get("otlp_payload")
            .expect("otlp_payload key missing");
        assert_eq!(
            entries,
            &vec![
                Value::String("first".to_string()),
                Value::String("second".to_string()),
                Value::String("third".to_string()),
                Value::String("fourth".to_string()),
            ],
        );
    }

    #[test]
    fn test_otlp_payload_non_string_elements_are_skipped_not_fatal() {
        // get_string_array filters on as_str(), so a malformed element is dropped
        // rather than poisoning the whole batch. Assert that explicitly so the
        // lenient behaviour is intentional and not an accident of refactoring.
        let payload = create_test_payload_with_json(
            r#"{"otlp_payload": ["good1", 42, null, {"a":1}, ["nested"], "good2"]}"#,
        );
        let (data_map, _version) = parse_agent_payload(&payload).unwrap();

        let entries = data_map
            .get("otlp_payload")
            .expect("otlp_payload key missing");
        assert_eq!(
            entries,
            &vec![
                Value::String("good1".to_string()),
                Value::String("good2".to_string()),
            ],
        );
    }

    #[test]
    fn test_otlp_payload_empty_array_yields_no_map_entry() {
        // An empty array must behave like an absent key so app.rs takes its
        // "no otlp_payload found" branch rather than spawning a no-op send.
        let payload = create_test_payload_with_json(r#"{"otlp_payload": []}"#);
        let (data_map, _version) = parse_agent_payload(&payload).unwrap();

        assert!(!data_map.contains_key("otlp_payload"));
    }

    #[test]
    fn test_otlp_payload_scalar_string_not_wrapped_is_ignored() {
        // Defensive: a bare string (not an array) does not match Value::Array,
        // so it yields nothing. Documents the shape contract with the agent.
        let payload = create_test_payload_with_json(r#"{"otlp_payload": "bare"}"#);
        let (data_map, _version) = parse_agent_payload(&payload).unwrap();

        assert!(!data_map.contains_key("otlp_payload"));
    }
}
