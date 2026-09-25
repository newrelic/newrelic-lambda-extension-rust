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

#[cfg(test)]
mod coverage_tests {
    use super::super::{
        extract_trace_id_from_payload,
        extract_trace_id_from_analytics,
        extract_trace_id_from_spans,
        parse_agent_payload,
    };
    use base64::engine::general_purpose;
    use base64::Engine as _;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    fn gzip_encode(data: &[u8]) -> String {
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(data).unwrap();
        general_purpose::STANDARD.encode(enc.finish().unwrap())
    }

    /// v1 payload with ONLY span_event_data — forces analytics to return None
    /// so extract_trace_id_from_payload falls through to the spans path (lines 75-80).
    fn spans_only_payload(trace_id: &str) -> Vec<u8> {
        let inner = format!(
            r#"{{"data": {{"span_event_data": [null, null, [[{{"traceId": "{}"}}]]]}}}}"#,
            trace_id
        );
        let encoded = gzip_encode(inner.as_bytes());
        let array = format!(r#"[1,"NR_LAMBDA_MONITORING","{}"]"#, encoded);
        general_purpose::STANDARD.encode(array.as_bytes()).into_bytes()
    }

    /// v1 payload with NR_LAMBDA_MONITORING but no traceId anywhere (lines 82-83).
    fn no_trace_payload() -> Vec<u8> {
        let inner = r#"{"data": {"analytic_event_data": [null, null, [[{"otherAttr": "noTrace"}]]]}}"#;
        let encoded = gzip_encode(inner.as_bytes());
        let array = format!(r#"[1,"NR_LAMBDA_MONITORING","{}"]"#, encoded);
        general_purpose::STANDARD.encode(array.as_bytes()).into_bytes()
    }

    /// Like the v1 test payload but version is a JSON *string* "1", not the number 1.
    /// Exercises the Value::String arm in parse_agent_payload (line 102).
    fn string_version_payload(trace_id: &str) -> Vec<u8> {
        let inner = format!(
            r#"{{"data": {{"analytic_event_data": [null, null, [[{{"traceId": "{}"}}]]]}}}}"#,
            trace_id
        );
        let encoded = gzip_encode(inner.as_bytes());
        // quoted "1" → JSON string, not number
        let array = format!(r#"["1","NR_LAMBDA_MONITORING","{}"]"#, encoded);
        general_purpose::STANDARD.encode(array.as_bytes()).into_bytes()
    }

    // ── extract_trace_id_from_payload: spans fallback (lines 75, 77-80) ───────

    #[test]
    fn spans_fallback_when_no_analytics() {
        let payload = spans_only_payload("span-trace-xyz");
        let result = extract_trace_id_from_payload(&payload).unwrap();
        assert_eq!(result, Some("span-trace-xyz".to_string()));
    }

    // ── extract_trace_id_from_payload: no trace found (lines 82-83) ───────────

    #[test]
    fn returns_none_when_no_trace_anywhere() {
        let payload = no_trace_payload();
        let result = extract_trace_id_from_payload(&payload).unwrap();
        assert_eq!(result, None);
    }

    // ── parse_agent_payload: error branches ─────────────────────────────────

    #[test]
    fn parse_payload_fails_with_too_few_elements() {
        // < 3 elements → line 98
        let result = parse_agent_payload(br#"[1,"NR_LAMBDA_MONITORING"]"#);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("3 elements"));
    }

    #[test]
    fn parse_payload_string_version_is_accepted() {
        // version "1" as JSON string → line 102 (Value::String arm)
        let payload = string_version_payload("str-ver-trace");
        let result = extract_trace_id_from_payload(&payload).unwrap();
        assert_eq!(result, Some("str-ver-trace".to_string()));
    }

    #[test]
    fn parse_payload_null_version_fails() {
        // null version → line 104 (_ arm)
        let result = parse_agent_payload(br#"[null,"NR","data"]"#);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("string or number"));
    }

    #[test]
    fn parse_payload_v2_too_few_elements_fails() {
        // version 2 with only 3 elements → line 111
        let result = parse_agent_payload(br#"[2,"NR","data"]"#);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("4 elements"));
    }

    #[test]
    fn parse_payload_v2_non_string_at_pos3_fails() {
        // version 2, position 3 is a number → line 115
        let result = parse_agent_payload(br#"[2,"NR","something",42]"#);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("position 3"));
    }

    #[test]
    fn parse_payload_v1_non_string_at_pos2_fails() {
        // version 1, position 2 is a number → line 120
        let result = parse_agent_payload(br#"[1,"NR",42]"#);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("position 2"));
    }

    // ── extract_trace_id_from_analytics: guard paths ────────────────────────

    #[test]
    fn analytics_none_when_key_missing() {
        // no analytic_event_data key → lines 174-175
        let data = std::collections::HashMap::new();
        assert_eq!(extract_trace_id_from_analytics(&data).unwrap(), None);
    }

    #[test]
    fn analytics_none_when_events_too_short() {
        // analytic_event_data with <= 2 elements → line 184
        let data = std::collections::HashMap::from([(
            "analytic_event_data".to_string(),
            serde_json::json!([null, null]),
        )]);
        assert_eq!(extract_trace_id_from_analytics(&data).unwrap(), None);
    }

    #[test]
    fn analytics_none_when_events_array_empty() {
        // events_array is [] → line 191
        let data = std::collections::HashMap::from([(
            "analytic_event_data".to_string(),
            serde_json::json!([null, null, []]),
        )]);
        assert_eq!(extract_trace_id_from_analytics(&data).unwrap(), None);
    }

    #[test]
    fn analytics_none_when_no_trace_id_field() {
        // events present but no traceId key → lines 198-202 (if-let chain falls through → Ok(None))
        let data = std::collections::HashMap::from([(
            "analytic_event_data".to_string(),
            serde_json::json!([null, null, [[{"spanId": "no-trace-here"}]]]),
        )]);
        assert_eq!(extract_trace_id_from_analytics(&data).unwrap(), None);
    }

    // ── extract_trace_id_from_spans: all paths (lines 206-237) ──────────────
    // The existing tests never reach this function because analytics returns Some first.

    #[test]
    fn spans_none_when_key_missing() {
        // no span_event_data key → lines 207-209
        let data = std::collections::HashMap::new();
        assert_eq!(extract_trace_id_from_spans(&data).unwrap(), None);
    }

    #[test]
    fn spans_none_when_events_too_short() {
        // span_event_data with <= 2 elements → line 217 guard
        let data = std::collections::HashMap::from([(
            "span_event_data".to_string(),
            serde_json::json!([null, null]),
        )]);
        assert_eq!(extract_trace_id_from_spans(&data).unwrap(), None);
    }

    #[test]
    fn spans_none_when_events_array_empty() {
        // events_array is [] → line 224 guard
        let data = std::collections::HashMap::from([(
            "span_event_data".to_string(),
            serde_json::json!([null, null, []]),
        )]);
        assert_eq!(extract_trace_id_from_spans(&data).unwrap(), None);
    }

    #[test]
    fn spans_success_path_returns_trace_id() {
        // Full success path → lines 228-232
        let data = std::collections::HashMap::from([(
            "span_event_data".to_string(),
            serde_json::json!([null, null, [[{"traceId": "spans-direct-trace"}]]]),
        )]);
        assert_eq!(
            extract_trace_id_from_spans(&data).unwrap(),
            Some("spans-direct-trace".to_string())
        );
    }

    #[test]
    fn spans_none_when_no_trace_id_field() {
        // spans present but no traceId → Ok(None) fallthrough
        let data = std::collections::HashMap::from([(
            "span_event_data".to_string(),
            serde_json::json!([null, null, [[{"spanId": "no-trace"}]]]),
        )]);
        assert_eq!(extract_trace_id_from_spans(&data).unwrap(), None);
    }
}
