// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tests for trace ID extraction
//!
//! Unit tests for extracting trace IDs from New Relic agent payloads

#[cfg(test)]
mod tests {
    use crate::trace::{extract_trace_id_from_payload, decode_uncompress};
    use base64::engine::general_purpose;
    use base64::Engine as _;
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
            format!(r#"[{},"NR_LAMBDA_MONITORING","","{}"]"#, version, encoded)
        } else {
            format!(r#"[{},"NR_LAMBDA_MONITORING","{}"]"#, version, encoded)
        };
        
        general_purpose::STANDARD.encode(payload_array.as_bytes()).into_bytes()
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
