// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tests for trace ID extraction
//!
//! Unit tests for extracting trace IDs from New Relic agent payloads

#[cfg(test)]
mod tests {
    use crate::trace::extract_trace_id_from_payload;
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

    #[test]
    fn test_extract_trace_id_accepts_string_protocol_version() {
        // Protocol version as a JSON string ("1") rather than a bare number — the
        // shared parser (apm::payload_parser::parse_agent_payload) normalizes both
        // forms by trimming quotes from the raw text, so this must still succeed.
        let test_data = r#"{"data": {"analytic_event_data": [null, null, [[{"traceId": "str-ver-trace"}]]]}}"#;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(test_data.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        let encoded = general_purpose::STANDARD.encode(&compressed);
        let array = format!(r#"["1","NR_LAMBDA_MONITORING","{}"]"#, encoded);
        let payload = general_purpose::STANDARD.encode(array.as_bytes()).into_bytes();

        let result = extract_trace_id_from_payload(&payload).unwrap();
        assert_eq!(result, Some("str-ver-trace".to_string()));
    }
}

#[cfg(test)]
mod coverage_tests {
    use super::super::{
        extract_trace_id_from_payload,
        extract_trace_id_from_analytics,
        extract_trace_id_from_spans,
    };
    use base64::engine::general_purpose;
    use base64::Engine as _;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use serde_json::Value;
    use std::collections::HashMap;
    use std::io::Write;

    fn gzip_encode(data: &[u8]) -> String {
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(data).unwrap();
        general_purpose::STANDARD.encode(enc.finish().unwrap())
    }

    /// v1 payload with ONLY span_event_data — forces analytics to return None
    /// so extract_trace_id_from_payload falls through to the spans path.
    fn spans_only_payload(trace_id: &str) -> Vec<u8> {
        let inner = format!(
            r#"{{"data": {{"span_event_data": [null, null, [[{{"traceId": "{}"}}]]]}}}}"#,
            trace_id
        );
        let encoded = gzip_encode(inner.as_bytes());
        let array = format!(r#"[1,"NR_LAMBDA_MONITORING","{}"]"#, encoded);
        general_purpose::STANDARD.encode(array.as_bytes()).into_bytes()
    }

    /// v1 payload with NR_LAMBDA_MONITORING but no traceId anywhere.
    fn no_trace_payload() -> Vec<u8> {
        let inner = r#"{"data": {"analytic_event_data": [null, null, [[{"otherAttr": "noTrace"}]]]}}"#;
        let encoded = gzip_encode(inner.as_bytes());
        let array = format!(r#"[1,"NR_LAMBDA_MONITORING","{}"]"#, encoded);
        general_purpose::STANDARD.encode(array.as_bytes()).into_bytes()
    }

    // ── extract_trace_id_from_payload: spans fallback ─────────────────────────

    #[test]
    fn spans_fallback_when_no_analytics() {
        let payload = spans_only_payload("span-trace-xyz");
        let result = extract_trace_id_from_payload(&payload).unwrap();
        assert_eq!(result, Some("span-trace-xyz".to_string()));
    }

    // ── extract_trace_id_from_payload: no trace found ─────────────────────────

    #[test]
    fn returns_none_when_no_trace_anywhere() {
        let payload = no_trace_payload();
        let result = extract_trace_id_from_payload(&payload).unwrap();
        assert_eq!(result, None);
    }

    // ── extract_trace_id_from_analytics: guard paths ──────────────────────────
    // Each telemetry type's entry is a Vec<Value> ([run_id_placeholder, metadata,
    // events]) — the shape apm::payload_parser::parse_agent_payload already
    // unwraps to, not a wrapped serde_json::Value.

    #[test]
    fn analytics_none_when_key_missing() {
        let data: HashMap<String, Vec<Value>> = HashMap::new();
        assert_eq!(extract_trace_id_from_analytics(&data), None);
    }

    #[test]
    fn analytics_none_when_events_too_short() {
        let data = HashMap::from([(
            "analytic_event_data".to_string(),
            vec![Value::Null, Value::Null],
        )]);
        assert_eq!(extract_trace_id_from_analytics(&data), None);
    }

    #[test]
    fn analytics_none_when_events_array_empty() {
        let data = HashMap::from([(
            "analytic_event_data".to_string(),
            vec![Value::Null, Value::Null, serde_json::json!([])],
        )]);
        assert_eq!(extract_trace_id_from_analytics(&data), None);
    }

    #[test]
    fn analytics_none_when_no_trace_id_field() {
        let data = HashMap::from([(
            "analytic_event_data".to_string(),
            vec![Value::Null, Value::Null, serde_json::json!([[{"spanId": "no-trace-here"}]])],
        )]);
        assert_eq!(extract_trace_id_from_analytics(&data), None);
    }

    #[test]
    fn analytics_success_path_returns_trace_id() {
        let data = HashMap::from([(
            "analytic_event_data".to_string(),
            vec![Value::Null, Value::Null, serde_json::json!([[{"traceId": "analytics-direct-trace"}]])],
        )]);
        assert_eq!(
            extract_trace_id_from_analytics(&data),
            Some("analytics-direct-trace".to_string())
        );
    }

    // ── extract_trace_id_from_spans: all paths ────────────────────────────────
    // The existing tests never reach this function because analytics returns Some first.

    #[test]
    fn spans_none_when_key_missing() {
        let data: HashMap<String, Vec<Value>> = HashMap::new();
        assert_eq!(extract_trace_id_from_spans(&data), None);
    }

    #[test]
    fn spans_none_when_events_too_short() {
        let data = HashMap::from([(
            "span_event_data".to_string(),
            vec![Value::Null, Value::Null],
        )]);
        assert_eq!(extract_trace_id_from_spans(&data), None);
    }

    #[test]
    fn spans_none_when_events_array_empty() {
        let data = HashMap::from([(
            "span_event_data".to_string(),
            vec![Value::Null, Value::Null, serde_json::json!([])],
        )]);
        assert_eq!(extract_trace_id_from_spans(&data), None);
    }

    #[test]
    fn spans_success_path_returns_trace_id() {
        let data = HashMap::from([(
            "span_event_data".to_string(),
            vec![Value::Null, Value::Null, serde_json::json!([[{"traceId": "spans-direct-trace"}]])],
        )]);
        assert_eq!(
            extract_trace_id_from_spans(&data),
            Some("spans-direct-trace".to_string())
        );
    }

    #[test]
    fn spans_none_when_no_trace_id_field() {
        let data = HashMap::from([(
            "span_event_data".to_string(),
            vec![Value::Null, Value::Null, serde_json::json!([[{"spanId": "no-trace"}]])],
        )]);
        assert_eq!(extract_trace_id_from_spans(&data), None);
    }
}
