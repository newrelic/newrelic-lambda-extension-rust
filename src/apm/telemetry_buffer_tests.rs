#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::apm::telemetry_buffer::{
        buffer_failed_telemetry, get_buffer_count, FAILED_TELEMETRY_BUFFER,
    };

    /// Clear the global buffer before/after tests to avoid cross-test interference
    fn clear_buffer() {
        if let Ok(mut buf) = FAILED_TELEMETRY_BUFFER.lock() {
            buf.clear();
        }
    }

    #[test]
    fn test_buffer_and_count() {
        clear_buffer();

        assert_eq!(get_buffer_count(), 0);

        buffer_failed_telemetry(
            "metric_data".to_string(),
            vec![json!({"test": true})],
            "req-1".to_string(),
            "run-1".to_string(),
            "collector.example.com".to_string(),
        );

        assert_eq!(get_buffer_count(), 1);

        buffer_failed_telemetry(
            "span_event_data".to_string(),
            vec![json!({"span": true})],
            "req-2".to_string(),
            "run-1".to_string(),
            "collector.example.com".to_string(),
        );

        assert_eq!(get_buffer_count(), 2);

        clear_buffer();
        assert_eq!(get_buffer_count(), 0);
    }

    #[test]
    fn test_buffered_item_fields() {
        clear_buffer();

        buffer_failed_telemetry(
            "error_event_data".to_string(),
            vec![json!({"error": "something"})],
            "req-99".to_string(),
            "run-42".to_string(),
            "collector.newrelic.com".to_string(),
        );

        let buf = FAILED_TELEMETRY_BUFFER.lock().expect("lock");
        assert_eq!(buf.len(), 1);

        let item = &buf[0];
        assert_eq!(item.telemetry_type, "error_event_data");
        assert_eq!(item.request_id, "req-99");
        assert_eq!(item.run_id, "run-42");
        assert_eq!(item.collector_host, "collector.newrelic.com");
        assert_eq!(item.retry_count, 0);
        assert_eq!(item.data.len(), 1);

        drop(buf);
        clear_buffer();
    }

    #[test]
    fn test_get_buffer_count_empty() {
        clear_buffer();
        assert_eq!(get_buffer_count(), 0);
    }
}
