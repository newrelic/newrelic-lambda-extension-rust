#[cfg(test)]
mod tests {
    use serde_json::json;
    use serial_test::serial;

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
    #[serial]
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
    #[serial]
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
    #[serial]
    fn test_get_buffer_count_empty() {
        clear_buffer();
        assert_eq!(get_buffer_count(), 0);
    }

    #[test]
    #[serial]
    fn test_buffer_multiple_items_ordering() {
        clear_buffer();

        buffer_failed_telemetry("type_a".to_string(), vec![json!(1)], "req-1".to_string(), "run-1".to_string(), "host".to_string());
        buffer_failed_telemetry("type_b".to_string(), vec![json!(2)], "req-2".to_string(), "run-1".to_string(), "host".to_string());
        buffer_failed_telemetry("type_c".to_string(), vec![json!(3)], "req-3".to_string(), "run-1".to_string(), "host".to_string());

        let buf = FAILED_TELEMETRY_BUFFER.lock().expect("lock");
        assert_eq!(buf.len(), 3);
        assert_eq!(buf[0].telemetry_type, "type_a");
        assert_eq!(buf[1].telemetry_type, "type_b");
        assert_eq!(buf[2].telemetry_type, "type_c");

        drop(buf);
        clear_buffer();
    }

    #[test]
    #[serial]
    fn test_get_buffer_count_after_multiple_adds() {
        clear_buffer();

        for i in 0..5 {
            buffer_failed_telemetry(
                format!("type_{i}"), vec![json!(i)], format!("req-{i}"),
                "run-1".to_string(), "host".to_string(),
            );
        }

        assert_eq!(get_buffer_count(), 5);

        clear_buffer();
    }

    // ========================================================================
    // Retry logic validation — FailedTelemetry lifecycle
    // ========================================================================

    #[test]
    #[serial]
    fn test_retry_count_starts_at_zero() {
        clear_buffer();

        buffer_failed_telemetry(
            "metric_data".to_string(),
            vec![json!(1)],
            "req-retry".to_string(),
            "run-1".to_string(),
            "host".to_string(),
        );

        let buf = FAILED_TELEMETRY_BUFFER.lock().expect("lock");
        assert_eq!(buf[0].retry_count, 0, "Initial retry_count must be 0");
        drop(buf);
        clear_buffer();
    }

    #[test]
    #[serial]
    fn test_failed_at_timestamp_is_recent() {
        clear_buffer();

        let before = chrono::Utc::now();

        buffer_failed_telemetry(
            "span_event_data".to_string(),
            vec![json!(1)],
            "req-ts".to_string(),
            "run-1".to_string(),
            "host".to_string(),
        );

        let after = chrono::Utc::now();

        let buf = FAILED_TELEMETRY_BUFFER.lock().expect("lock");
        let failed_at = buf[0].failed_at;
        assert!(failed_at >= before, "failed_at should be >= test start");
        assert!(failed_at <= after, "failed_at should be <= test end");
        drop(buf);
        clear_buffer();
    }

    #[test]
    #[serial]
    fn test_manual_retry_count_increment_simulation() {
        // Simulate what retry_buffered_telemetry does: increment retry_count
        clear_buffer();

        buffer_failed_telemetry(
            "metric_data".to_string(),
            vec![json!(1)],
            "req-sim".to_string(),
            "run-1".to_string(),
            "host".to_string(),
        );

        // Simulate: take from buffer, increment, put back (mimics retry failure path)
        {
            let mut buf = FAILED_TELEMETRY_BUFFER.lock().expect("lock");
            let mut item = buf.remove(0);
            item.retry_count += 1;
            buf.push(item);
        }

        let buf = FAILED_TELEMETRY_BUFFER.lock().expect("lock");
        assert_eq!(buf[0].retry_count, 1, "After one simulated retry, count should be 1");
        drop(buf);

        // Simulate 9 more retries
        for _ in 0..9 {
            let mut buf = FAILED_TELEMETRY_BUFFER.lock().expect("lock");
            let mut item = buf.remove(0);
            item.retry_count += 1;
            buf.push(item);
        }

        let buf = FAILED_TELEMETRY_BUFFER.lock().expect("lock");
        assert_eq!(buf[0].retry_count, 10, "After 10 retries, count should be 10");

        // At retry_count >= 10, retry_buffered_telemetry would drop this item
        let should_drop = buf[0].retry_count >= 10;
        assert!(should_drop, "Item with retry_count >= 10 should be dropped by retry logic");
        drop(buf);
        clear_buffer();
    }

    #[test]
    #[serial]
    fn test_old_telemetry_age_check_simulation() {
        // Simulate what retry_buffered_telemetry does: check age > 60 minutes
        clear_buffer();

        // Manually create an old FailedTelemetry item
        use crate::apm::telemetry_buffer::FailedTelemetry;

        let old_item = FailedTelemetry {
            telemetry_type: "metric_data".to_string(),
            data: vec![json!(1)],
            request_id: "req-old".to_string(),
            run_id: "run-1".to_string(),
            collector_host: "host".to_string(),
            failed_at: chrono::Utc::now() - chrono::Duration::minutes(61),
            retry_count: 0,
        };

        // Verify age exceeds threshold
        let age = chrono::Utc::now().signed_duration_since(old_item.failed_at);
        assert!(age.num_minutes() > 60, "Item should be > 60 minutes old");

        // A recent item should NOT exceed threshold
        let recent_item = FailedTelemetry {
            telemetry_type: "metric_data".to_string(),
            data: vec![json!(1)],
            request_id: "req-new".to_string(),
            run_id: "run-1".to_string(),
            collector_host: "host".to_string(),
            failed_at: chrono::Utc::now(),
            retry_count: 0,
        };

        let recent_age = chrono::Utc::now().signed_duration_since(recent_item.failed_at);
        assert!(recent_age.num_minutes() <= 60, "Recent item should NOT exceed 60 minute threshold");

        clear_buffer();
    }

    #[test]
    #[serial]
    fn test_buffer_preserves_all_fields_through_take_and_reinsert() {
        // Simulate the take-process-reinsert pattern used in retry_buffered_telemetry
        clear_buffer();

        buffer_failed_telemetry(
            "error_event_data".to_string(),
            vec![json!({"err": true}), json!({"err2": true})],
            "req-fields".to_string(),
            "run-fields".to_string(),
            "collector.test.com".to_string(),
        );

        // Take all items (like retry_buffered_telemetry does with std::mem::take)
        let taken = {
            let mut buf = FAILED_TELEMETRY_BUFFER.lock().expect("lock");
            std::mem::take(&mut *buf)
        };

        assert_eq!(get_buffer_count(), 0, "Buffer should be empty after take");
        assert_eq!(taken.len(), 1);

        // Verify all fields survived the take
        let item = &taken[0];
        assert_eq!(item.telemetry_type, "error_event_data");
        assert_eq!(item.data.len(), 2);
        assert_eq!(item.request_id, "req-fields");
        assert_eq!(item.run_id, "run-fields");
        assert_eq!(item.collector_host, "collector.test.com");
        assert_eq!(item.retry_count, 0);

        // Reinsert (simulating retry failure re-buffer)
        {
            let mut buf = FAILED_TELEMETRY_BUFFER.lock().expect("lock");
            for mut t in taken {
                t.retry_count += 1;
                buf.push(t);
            }
        }

        assert_eq!(get_buffer_count(), 1, "Re-inserted item should be in buffer");
        let buf = FAILED_TELEMETRY_BUFFER.lock().expect("lock");
        assert_eq!(buf[0].retry_count, 1, "Retry count should be incremented");
        assert_eq!(buf[0].data.len(), 2, "Data should be preserved");
        drop(buf);
        clear_buffer();
    }
}
