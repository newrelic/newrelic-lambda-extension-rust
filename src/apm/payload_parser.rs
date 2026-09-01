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

/// Protocol v1 wrapper with metadata and data fields.
/// Note: metadata (agent_version, agent_language) is available in the payload
/// but arrives AFTER APM Connect has already been made — too late to use.
/// We only deserialize `data`; metadata is intentionally ignored.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct LambdaRawData {
    pub data: LambdaData,
}

/// Parse agent payload and return telemetry data by type.
/// Returns (data_map, protocol_version).
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
            // Protocol v2: no metadata wrapper — data is the root object
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

/// Extract the Lambda invocation request id (`aws.requestId`) the agent embedded in the
/// transaction analytic event of an agent payload.
///
/// Under LMI we route agent payloads by this **self-describing** id instead of a
/// process-global "current request", so concurrent invocations never cross-attribute
/// (NR-579361). We read **only** `analytic_event_data` (transaction analytic events);
/// we deliberately never look at `span_event_data`, whose external/AWS-SDK spans carry
/// their own unrelated `aws.requestId`.
///
/// `analytic_event_data = [ harvest_meta_or_null, { reservoir }, [ EVENT, … ] ]` where
/// `EVENT = [ intrinsics, userAttributes, agentAttributes ]`; `aws.requestId` lives in
/// `agentAttributes` (the 3rd tuple element). Every level is guarded — returns `None`
/// on any shape mismatch (Zero-Panic).
pub fn extract_transaction_request_id(data_map: &HashMap<String, Vec<Value>>) -> Option<String> {
    let events = data_map.get("analytic_event_data")?.get(2)?.as_array()?;
    events.iter().find_map(|event| {
        event
            .as_array()?
            .get(2)
            .and_then(|agent_attributes| agent_attributes.get("aws.requestId"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
    })
}

/// Parse a raw wire agent payload and extract its embedded transaction `aws.requestId`.
/// Returns `None` on any parse failure or when no transaction event carries the id
/// (e.g. metrics-only / log-only harvest). Reuses [`parse_agent_payload`].
pub fn extract_request_id_from_payload_bytes(payload_bytes: &[u8]) -> Option<String> {
    let (data_map, _version) = parse_agent_payload(payload_bytes).ok()?;
    extract_transaction_request_id(&data_map)
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
#[path = "payload_parser_tests.rs"]
mod tests;
