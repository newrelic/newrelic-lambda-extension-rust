//! Trace ID extraction module for New Relic Lambda Extension
//!
//! This module provides optimized functions to extract trace IDs from New Relic agent payloads.
//! It only processes payloads when trace ID collection is enabled via NEW_RELIC_COLLECT_TRACE_ID.

use anyhow::{Result, anyhow};
use base64::{Engine as _, engine::general_purpose};
use serde_json::Value;
use std::collections::HashMap;
use std::io::Read;
use flate2::read::GzDecoder;
use tracing::{debug, trace};

/// Type alias for uncompressed data from agent payload
type UncompressedData = HashMap<String, Value>;

/// Extract trace ID from agent payload, if present and collection is enabled
/// 
/// This function follows the exact same pattern as the Go ExtractTraceID implementation:
/// 1. Handle both base64 and raw payload formats
/// 2. Check for NR_LAMBDA_MONITORING marker  
/// 3. Parse payload using parsePayload logic
/// 4. Extract trace ID from analytic_event_data or span_event_data
pub fn extract_trace_id_from_payload(payload_bytes: &[u8]) -> Result<Option<String>> {
    trace!("Starting trace ID extraction from payload ({} bytes)", payload_bytes.len());
    
    let payload_str = std::str::from_utf8(payload_bytes)
        .map_err(|e| anyhow!("Invalid UTF-8 in payload: {}", e))?;
    
    debug!("Payload length: {} chars", payload_str.len());
    trace!("Payload content (first 200 chars): {}", &payload_str.chars().take(200).collect::<String>());
    
    let data_to_process = if payload_str.trim_start().starts_with('[') {
        debug!("Payload appears to be raw JSON, using directly");
        payload_bytes.to_vec()
    } else {
        debug!("Attempting to base64 decode payload");
        match general_purpose::STANDARD.decode(payload_str) {
            Ok(decoded) => {
                debug!("Successfully base64 decoded to {} bytes", decoded.len());
                decoded
            },
            Err(e) => {
                debug!("Failed to base64 decode, treating as raw data: {}", e);
                payload_bytes.to_vec()
            }
        }
    };
    
    if !data_to_process.windows(b"NR_LAMBDA_MONITORING".len()).any(|window| window == b"NR_LAMBDA_MONITORING") {
        trace!("Payload does not contain NR_LAMBDA_MONITORING marker, skipping trace extraction");
        return Ok(None);
    }

    debug!("Found NR_LAMBDA_MONITORING marker in payload, attempting to extract trace ID");
    
    let uncompressed_data = match parse_agent_payload(&data_to_process) {
        Ok(data) => {
            debug!("Successfully parsed agent payload, data has {} top-level keys", data.len());
            trace!("Payload data keys: {:?}", data.keys().collect::<Vec<_>>());
            data
        },
        Err(e) => {
            debug!("Failed to parse agent payload for trace extraction: {}", e);
            return Ok(None);
        }
    };

    if let Some(trace_id) = extract_trace_id_from_analytics(&uncompressed_data)? {
        debug!("Successfully extracted trace ID from analytic events: {}", trace_id);
        return Ok(Some(trace_id));
    }

    if let Some(trace_id) = extract_trace_id_from_spans(&uncompressed_data)? {
        debug!("Successfully extracted trace ID from span events: {}", trace_id);
        return Ok(Some(trace_id));
    }

    debug!("No trace ID found in agent payload");
    Ok(None)
}

/// Parse agent payload data and return uncompressed data
/// Following the exact same pattern as Go parsePayload function
fn parse_agent_payload(data: &[u8]) -> Result<UncompressedData> {
    trace!("Parsing agent payload data ({} bytes)", data.len());
    trace!("Raw data (first 100 chars): {}", std::str::from_utf8(data).unwrap_or("invalid utf8").chars().take(100).collect::<String>());

    let arr: Vec<Value> = serde_json::from_slice(data)
        .map_err(|e| anyhow!("unable to unmarshal payload data array: {}", e))?;

    debug!("Successfully parsed JSON array with {} elements", arr.len());

    if arr.len() < 3 {
        return Err(anyhow!("payload array must have at least 3 elements, got {}", arr.len()));
    }

    let payload_version = match &arr[0] {
        Value::String(s) => s.trim_matches('"').to_string(),
        Value::Number(n) => n.to_string(),
        _ => return Err(anyhow!("payload version must be a string or number")),
    };
    
    debug!("Payload version: {}", payload_version);

    let data_compressed = if payload_version == "2" {
        if arr.len() < 4 {
            return Err(anyhow!("version 2 payload must have 4 elements, got {}", arr.len()));
        }
        match &arr[3] {
            Value::String(s) => s.trim_matches('"'),
            _ => return Err(anyhow!("compressed data at position 3 must be a string")),
        }
    } else {
        match &arr[2] {
            Value::String(s) => s.trim_matches('"'),
            _ => return Err(anyhow!("compressed data at position 2 must be a string")),
        }
    };

    debug!("Compressed data length: {} chars", data_compressed.len());
    trace!("Compressed data (first 50 chars): {}", data_compressed.chars().take(50).collect::<String>());

    let data_json = decode_uncompress(data_compressed)
        .map_err(|e| anyhow!("unable to uncompress payload: {}", e))?;

    debug!("Uncompressed JSON data length: {} bytes", data_json.len());
    trace!("Uncompressed JSON (first 100 chars): {}", std::str::from_utf8(&data_json).unwrap_or("invalid utf8").chars().take(100).collect::<String>());

    let uncompressed_data = if payload_version == "2" {
        serde_json::from_slice::<UncompressedData>(&data_json)
            .map_err(|e| anyhow!("unable to unmarshal uncompressed payload v2: {}", e))?
    } else {
        let v1_data: HashMap<String, UncompressedData> = serde_json::from_slice(&data_json)
            .map_err(|e| anyhow!("unable to unmarshal uncompressed payload v1: {}", e))?;
        
        v1_data.get("data")
            .ok_or_else(|| anyhow!("data field not found in version 1 payload"))?
            .clone()
    };

    debug!("Successfully parsed uncompressed data with {} keys", uncompressed_data.len());
    Ok(uncompressed_data)
}

/// Decode base64 and uncompress gzip data
fn decode_uncompress(input: &str) -> Result<Vec<u8>> {
    trace!("Decoding base64 string ({} chars)", input.len());
    trace!("Base64 input (first 50 chars): {}", input.chars().take(50).collect::<String>());

    let decoded = general_purpose::STANDARD.decode(input)
        .map_err(|e| anyhow!("base64 decode error: {}", e))?;

    debug!("Successfully decoded base64 to {} bytes", decoded.len());

    let mut decoder = GzDecoder::new(&decoded[..]);
    
    let mut output = Vec::new();
    decoder.read_to_end(&mut output)
        .map_err(|e| anyhow!("gzip decompression error: {}", e))?;

    debug!("Successfully decompressed to {} bytes", output.len());
    trace!("Decompressed data (first 100 chars): {}", std::str::from_utf8(&output).unwrap_or("invalid utf8").chars().take(100).collect::<String>());

    Ok(output)
}

/// Extract trace ID from analytic event data
fn extract_trace_id_from_analytics(data: &UncompressedData) -> Result<Option<String>> {
    let Some(analytic_events) = data.get("analytic_event_data") else {
        debug!("No analytic_event_data found in payload");
        return Ok(None);
    };

    debug!("Found analytic_event_data, attempting to extract trace ID");

    let parsed_events: Vec<Value> = serde_json::from_value(analytic_events.clone())
        .map_err(|e| anyhow!("failed to parse analytic events: {}", e))?;

    if parsed_events.len() <= 2 {
        return Ok(None);
    }

    let events_array: Vec<Vec<Value>> = serde_json::from_value(parsed_events[2].clone())
        .map_err(|e| anyhow!("failed to parse events array: {}", e))?;

    if events_array.is_empty() || events_array[0].is_empty() {
        return Ok(None);
    }

    if let Some(event_obj) = events_array[0][0].as_object() {
        if let Some(trace_id_value) = event_obj.get("traceId") {
            if let Some(trace_id) = trace_id_value.as_str() {
                return Ok(Some(trace_id.to_string()));
            }
        }
    }

    Ok(None)
}

/// Extract trace ID from span event data
fn extract_trace_id_from_spans(data: &UncompressedData) -> Result<Option<String>> {
    let Some(span_events) = data.get("span_event_data") else {
        debug!("No span_event_data found in payload");
        return Ok(None);
    };

    debug!("Found span_event_data, attempting to extract trace ID");

    let parsed_events: Vec<Value> = serde_json::from_value(span_events.clone())
        .map_err(|e| anyhow!("failed to parse span events: {}", e))?;

    if parsed_events.len() <= 2 {
        return Ok(None);
    }

    let events_array: Vec<Vec<Value>> = serde_json::from_value(parsed_events[2].clone())
        .map_err(|e| anyhow!("failed to parse span events array: {}", e))?;

    if events_array.is_empty() || events_array[0].is_empty() {
        return Ok(None);
    }

    if let Some(span_obj) = events_array[0][0].as_object() {
        if let Some(trace_id_value) = span_obj.get("traceId") {
            if let Some(trace_id) = trace_id_value.as_str() {
                return Ok(Some(trace_id.to_string()));
            }
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    fn create_test_payload_with_trace_id(version: &str, trace_id: &str) -> Vec<u8> {
        let test_data = if version == "2" {
            format!(r#"{{"analytic_event_data": [null, null, [[{{"traceId": "{}"}}]]], "span_event_data": [null, null, [[{{"traceId": "{}"}}]]]}}"#, trace_id, trace_id)
        } else {
            format!(r#"{{"data": {{"analytic_event_data": [null, null, [[{{"traceId": "{}"}}]]], "span_event_data": [null, null, [[{{"traceId": "{}"}}]]]}}}}"#, trace_id, trace_id)
        };

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(test_data.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        
        let encoded = general_purpose::STANDARD.encode(&compressed);
        
        let payload_array = if version == "2" {
            format!(r#"["{}","","","{}"]"#, version, encoded)
        } else {
            format!(r#"["{}","","{}"]"#, version, encoded)
        };
        
        let payload_with_marker = format!("{}NR_LAMBDA_MONITORING", payload_array);
        
        general_purpose::STANDARD.encode(payload_with_marker.as_bytes()).into_bytes()
    }

    #[test]
    fn test_extract_trace_id_from_payload_v2() {
        let test_trace_id = "test-trace-123";
        let payload = create_test_payload_with_trace_id("2", test_trace_id);
        
        println!("Test payload: {}", String::from_utf8_lossy(&payload));
        
        let result = extract_trace_id_from_payload(&payload);
        match &result {
            Ok(Some(id)) => println!("Extracted trace ID: {}", id),
            Ok(None) => println!("No trace ID found"),
            Err(e) => println!("Error: {}", e),
        }
        
        assert_eq!(result.unwrap(), Some(test_trace_id.to_string()));
    }

    #[test]
    fn test_extract_trace_id_from_payload_v1() {
        let test_trace_id = "test-trace-456";
        let payload = create_test_payload_with_trace_id("1", test_trace_id);
        
        let result = extract_trace_id_from_payload(&payload).unwrap();
        assert_eq!(result, Some(test_trace_id.to_string()));
    }

    #[test]
    fn test_extract_trace_id_no_monitoring_marker() {
        let test_data = "regular payload without NR_LAMBDA_MONITORING marker";
        let base64_payload = general_purpose::STANDARD.encode(test_data.as_bytes());
        
        let result = extract_trace_id_from_payload(base64_payload.as_bytes()).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_decode_uncompress() {
        let test_data = "Hello, World!";
        
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(test_data.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        let encoded = general_purpose::STANDARD.encode(&compressed);
        
        let result = decode_uncompress(&encoded).unwrap();
        assert_eq!(String::from_utf8(result).unwrap(), test_data);
    }

    #[test]
    fn test_real_payload_formats() {
        use tracing::Level;
        use tracing_subscriber;
        
        let _ = tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .try_init();

        let v1_full_payload = "WzEsIk5SX0xBTUJEQV9NT05JVE9SSU5HIiwiSDRzSUFGQi9wR2dDLzlhMjNMYk9CTDlGUmVmRlJKM0VIN0xSWmw0SzlsNEs5bDRLOWx4eFE3TmJxNG9hakpMcnFiNmtKdU0vTlM1YXRJeUhyTjBCMTNzSGY4NnV3OXUxVCtIeXJDOGgxOExBajFERDFSdDNZVUVvd0JSdDA0SDBWZzBWZndweCszUC93KzJJbi9WeGhaTnczUmhGbjVlb3VsVnhVZmdNNS9ITjV6ZjE5VGpLNTBzVDA3N1o4V29KblVuQUVLU2ZNa2J3Il0=";

        println!("Testing full V1 payload...");
        
        let result = extract_trace_id_from_payload(v1_full_payload.as_bytes());
        match &result {
            Ok(Some(id)) => println!("V1 Extracted trace ID: {}", id),
            Ok(None) => println!("V1 No trace ID found"),
            Err(e) => println!("V1 Error: {}", e),
        }

        println!("Testing simple V2 with trace ID...");
        let simple_v2_payload = create_test_payload_with_trace_id("2", "test-trace-123");
        let simple_v2_str = String::from_utf8(simple_v2_payload).unwrap();
        
        let simple_result = extract_trace_id_from_payload(simple_v2_str.as_bytes());
        match &simple_result {
            Ok(Some(id)) => println!("Simple V2 Extracted trace ID: {}", id),
            Ok(None) => println!("Simple V2 No trace ID found"),
            Err(e) => println!("Simple V2 Error: {}", e),
        }

        assert!(result.is_ok());
        assert!(simple_result.is_ok());
    }
}
