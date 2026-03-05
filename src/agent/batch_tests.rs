//! Unit tests for agent payload batching logic
//!
//! Tests cover:
//! - add_to_batch: inserting, metadata tracking, no blocking on empty fields
//! - should_send_batch_by_threshold: threshold logic (3+ with reports)
//! - get_and_clear_batch: drain and reset
//! - get_batch_with_reports_only: filtering
//! - split_into_chunks: 1MB chunking behavior
//! - estimate_item_size / estimate_base_overhead: size calculations

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use std::sync::Arc;

    use crate::agent::batch::*;
    use crate::config::ExtensionConfig;

    /// Helper: clear all global batch state between tests
    fn clear_batch_state() {
        AGENT_BATCH_BUFFER.clear();
        if let Ok(mut meta) = BATCH_META.lock() {
            meta.agent_count = 0;
            meta.oldest_timestamp = None;
        }
    }

    // ========================================================================
    // add_to_batch tests
    // ========================================================================

    #[test]
    #[serial]
    fn test_add_to_batch_stores_payload() {
        clear_batch_state();

        add_to_batch(
            "req-1".to_string(),
            b"agent-data-1".to_vec(),
            Some("REPORT Duration: 100ms".to_string()),
            "arn:aws:lambda:us-east-1:123:function:my-fn".to_string(),
        );

        assert_eq!(AGENT_BATCH_BUFFER.len(), 1);
        {
            // Scope the DashMap Ref so the shard lock is released before clear_batch_state
            let item = AGENT_BATCH_BUFFER.get("req-1").expect("should exist");
            assert_eq!(item.request_id, "req-1");
            assert_eq!(*item.agent_payload_bytes, b"agent-data-1".to_vec());
            assert_eq!(item.report_line.as_deref(), Some("REPORT Duration: 100ms"));
            assert_eq!(
                item.invoked_function_arn,
                "arn:aws:lambda:us-east-1:123:function:my-fn"
            );
        }

        clear_batch_state();
    }

    #[test]
    #[serial]
    fn test_add_to_batch_updates_metadata() {
        clear_batch_state();

        add_to_batch("req-1".into(), vec![1], None, "arn".into());
        add_to_batch("req-2".into(), vec![2], None, "arn".into());

        let meta = BATCH_META.lock().expect("lock");
        assert_eq!(meta.agent_count, 2);
        assert!(meta.oldest_timestamp.is_some());

        drop(meta);
        clear_batch_state();
    }

    #[test]
    #[serial]
    fn test_add_to_batch_replaces_same_request_id() {
        clear_batch_state();

        add_to_batch("req-1".into(), vec![1], None, "arn-1".into());
        add_to_batch("req-1".into(), vec![2], Some("report".into()), "arn-2".into());

        // DashMap replaces on same key
        assert_eq!(AGENT_BATCH_BUFFER.len(), 1);
        {
            let item = AGENT_BATCH_BUFFER.get("req-1").expect("exists");
            assert_eq!(*item.agent_payload_bytes, vec![2]);
            assert_eq!(item.report_line.as_deref(), Some("report"));
        }

        clear_batch_state();
    }

    #[test]
    #[serial]
    fn test_add_to_batch_does_not_block_on_empty_request_id() {
        clear_batch_state();

        // Should not panic or return early - just stores with empty key
        add_to_batch("".into(), vec![1, 2, 3], None, "arn".into());
        assert_eq!(AGENT_BATCH_BUFFER.len(), 1);

        clear_batch_state();
    }

    #[test]
    #[serial]
    fn test_add_to_batch_does_not_block_on_empty_arn() {
        clear_batch_state();

        add_to_batch("req-1".into(), vec![1], None, "".into());
        assert_eq!(AGENT_BATCH_BUFFER.len(), 1);
        {
            let item = AGENT_BATCH_BUFFER.get("req-1").expect("exists");
            assert_eq!(item.invoked_function_arn, "");
        }

        clear_batch_state();
    }

    // ========================================================================
    // should_send_batch_by_threshold tests
    // ========================================================================

    #[test]
    #[serial]
    fn test_threshold_not_reached_with_zero_items() {
        clear_batch_state();
        assert!(!should_send_batch_by_threshold());
        clear_batch_state();
    }

    #[test]
    #[serial]
    fn test_threshold_not_reached_with_items_without_reports() {
        clear_batch_state();

        // 5 items but NONE have report lines
        for i in 0..5 {
            add_to_batch(format!("req-{i}"), vec![i as u8], None, "arn".into());
        }

        assert!(!should_send_batch_by_threshold());
        clear_batch_state();
    }

    #[test]
    #[serial]
    fn test_threshold_not_reached_with_two_reports() {
        clear_batch_state();

        add_to_batch("req-1".into(), vec![1], Some("report1".into()), "arn".into());
        add_to_batch("req-2".into(), vec![2], Some("report2".into()), "arn".into());

        assert!(!should_send_batch_by_threshold());
        clear_batch_state();
    }

    #[test]
    #[serial]
    fn test_threshold_reached_with_three_reports() {
        clear_batch_state();

        add_to_batch("req-1".into(), vec![1], Some("r1".into()), "arn".into());
        add_to_batch("req-2".into(), vec![2], Some("r2".into()), "arn".into());
        add_to_batch("req-3".into(), vec![3], Some("r3".into()), "arn".into());

        assert!(should_send_batch_by_threshold());
        clear_batch_state();
    }

    #[test]
    #[serial]
    fn test_threshold_counts_only_items_with_reports() {
        clear_batch_state();

        // 2 with reports, 5 without
        add_to_batch("req-1".into(), vec![1], Some("r1".into()), "arn".into());
        add_to_batch("req-2".into(), vec![2], None, "arn".into());
        add_to_batch("req-3".into(), vec![3], None, "arn".into());
        add_to_batch("req-4".into(), vec![4], Some("r4".into()), "arn".into());
        add_to_batch("req-5".into(), vec![5], None, "arn".into());

        assert!(!should_send_batch_by_threshold());
        clear_batch_state();
    }

    // ========================================================================
    // get_and_clear_batch tests
    // ========================================================================

    #[test]
    #[serial]
    fn test_get_and_clear_returns_all_items() {
        clear_batch_state();

        add_to_batch("req-1".into(), vec![1], None, "arn".into());
        add_to_batch("req-2".into(), vec![2], Some("r".into()), "arn".into());

        let items = get_and_clear_batch();
        assert_eq!(items.len(), 2);

        // Buffer should be empty after clearing
        assert_eq!(AGENT_BATCH_BUFFER.len(), 0);

        let meta = BATCH_META.lock().expect("lock");
        assert_eq!(meta.agent_count, 0);
        assert!(meta.oldest_timestamp.is_none());

        drop(meta);
        clear_batch_state();
    }

    #[test]
    #[serial]
    fn test_get_and_clear_empty_buffer() {
        clear_batch_state();

        let items = get_and_clear_batch();
        assert!(items.is_empty());

        clear_batch_state();
    }

    // ========================================================================
    // get_batch_with_reports_only tests
    // ========================================================================

    #[test]
    #[serial]
    fn test_get_batch_with_reports_returns_only_items_with_reports() {
        clear_batch_state();

        add_to_batch("req-1".into(), vec![1], Some("report-1".into()), "arn".into());
        add_to_batch("req-2".into(), vec![2], None, "arn".into());
        add_to_batch("req-3".into(), vec![3], Some("report-3".into()), "arn".into());

        let with_reports = get_batch_with_reports_only();
        assert_eq!(with_reports.len(), 2);

        // Items should NOT be removed from buffer
        assert_eq!(AGENT_BATCH_BUFFER.len(), 3);

        clear_batch_state();
    }

    #[test]
    #[serial]
    fn test_get_batch_with_reports_empty_when_no_reports() {
        clear_batch_state();

        add_to_batch("req-1".into(), vec![1], None, "arn".into());
        add_to_batch("req-2".into(), vec![2], None, "arn".into());

        let with_reports = get_batch_with_reports_only();
        assert!(with_reports.is_empty());

        clear_batch_state();
    }

    // ========================================================================
    // split_into_chunks tests
    // ========================================================================

    fn make_payload(id: &str, size: usize) -> BatchedAgentPayload {
        BatchedAgentPayload {
            request_id: id.to_string(),
            agent_payload_bytes: Arc::new(vec![0u8; size]),
            report_line: None,
            invoked_function_arn: "arn:test".to_string(),
            timestamp: chrono::Utc::now(),
        }
    }

    #[test]
    fn test_split_chunks_single_item() {
        let config = Arc::new(ExtensionConfig::default());
        let payloads = vec![make_payload("req-1", 100)];

        let chunks = split_into_chunks(payloads, 1_000_000, &config);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 1);
    }

    #[test]
    fn test_split_chunks_all_fit_in_one() {
        let config = Arc::new(ExtensionConfig::default());
        let payloads = vec![
            make_payload("req-1", 100),
            make_payload("req-2", 100),
            make_payload("req-3", 100),
        ];

        let chunks = split_into_chunks(payloads, 1_000_000, &config);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 3);
    }

    #[test]
    fn test_split_chunks_multiple_chunks() {
        let config = Arc::new(ExtensionConfig::default());
        // Each item ~500KB + overhead, max chunk 1MB → should split
        let payloads = vec![
            make_payload("req-1", 500_000),
            make_payload("req-2", 500_000),
            make_payload("req-3", 500_000),
        ];

        let chunks = split_into_chunks(payloads, 1_000_000, &config);
        assert!(chunks.len() >= 2, "Should split into multiple chunks");

        // All items should be accounted for
        let total: usize = chunks.iter().map(|c: &Vec<BatchedAgentPayload>| c.len()).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn test_split_chunks_empty_input() {
        let config = Arc::new(ExtensionConfig::default());
        let chunks = split_into_chunks(vec![], 1_000_000, &config);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_chunks_single_oversized_item() {
        let config = Arc::new(ExtensionConfig::default());
        // Single item larger than max_size → still gets its own chunk
        let payloads = vec![make_payload("req-1", 2_000_000)];

        let chunks = split_into_chunks(payloads, 1_000_000, &config);
        assert_eq!(chunks.len(), 1, "Oversized item gets its own chunk");
        assert_eq!(chunks[0].len(), 1);
    }

    // ========================================================================
    // estimate_item_size tests
    // ========================================================================

    #[test]
    fn test_estimate_item_size_without_report() {
        let item = BatchedAgentPayload {
            request_id: "req".to_string(),
            agent_payload_bytes: Arc::new(vec![0u8; 1000]),
            report_line: None,
            invoked_function_arn: "arn".to_string(),
            timestamp: chrono::Utc::now(),
        };

        let size = estimate_item_size(&item);
        // 1000 bytes payload + 150 overhead
        assert_eq!(size, 1150);
    }

    #[test]
    fn test_estimate_item_size_with_report() {
        let item = BatchedAgentPayload {
            request_id: "req".to_string(),
            agent_payload_bytes: Arc::new(vec![0u8; 500]),
            report_line: Some("REPORT Duration: 100ms".to_string()),
            invoked_function_arn: "arn".to_string(),
            timestamp: chrono::Utc::now(),
        };

        let size = estimate_item_size(&item);
        // 500 payload + 22 report + 150 + 150 overhead
        assert_eq!(size, 500 + 22 + 150 + 150);
    }

    // ========================================================================
    // estimate_base_overhead tests
    // ========================================================================

    #[test]
    fn test_estimate_base_overhead() {
        let config = Arc::new(ExtensionConfig::default());
        let overhead = estimate_base_overhead(&config);
        // Should be > 500 (base) and scale with function name length
        assert!(overhead >= 500);
    }

    #[test]
    fn test_estimate_base_overhead_scales_with_function_name() {
        let mut config1 = ExtensionConfig::default();
        config1.aws.function_name = "fn".to_string();
        let overhead1 = estimate_base_overhead(&Arc::new(config1));

        let mut config2 = ExtensionConfig::default();
        config2.aws.function_name = "a-very-long-function-name-for-testing".to_string();
        let overhead2 = estimate_base_overhead(&Arc::new(config2));

        assert!(overhead2 > overhead1);
    }

    // ========================================================================
    // send_batched_payloads_with_reports_only tests
    // (also covers clear_batch_with_reports indirectly)
    // ========================================================================

    #[tokio::test]
    #[serial]
    async fn test_send_batched_payloads_empty_returns_early() {
        clear_batch_state();

        let config = Arc::new(ExtensionConfig::default());
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));

        // No items with reports → should return early
        add_to_batch("req-1".into(), vec![1], None, "arn".into());

        send_batched_payloads_with_reports_only(newrelic_client, config).await;

        // Item without report should remain in buffer
        assert_eq!(AGENT_BATCH_BUFFER.len(), 1);

        clear_batch_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_send_batched_payloads_sends_and_clears_reports() {
        clear_batch_state();

        let config = Arc::new(ExtensionConfig::default());
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));

        // 2 with reports, 1 without
        add_to_batch("req-1".into(), vec![1], Some("report-1".into()), "arn:test".into());
        add_to_batch("req-2".into(), vec![2], None, "arn:test".into());
        add_to_batch("req-3".into(), vec![3], Some("report-3".into()), "arn:test".into());

        assert_eq!(AGENT_BATCH_BUFFER.len(), 3);

        // No license key → send_agent_payload returns Ok(()) → clear_batch_with_reports runs
        send_batched_payloads_with_reports_only(newrelic_client, config).await;

        // Items WITH reports should be removed, item WITHOUT report should remain
        assert_eq!(AGENT_BATCH_BUFFER.len(), 1);
        {
            let remaining = AGENT_BATCH_BUFFER.get("req-2").expect("req-2 should remain");
            assert!(remaining.report_line.is_none());
        }

        // Metadata should reflect remaining count
        {
            let meta = BATCH_META.lock().expect("lock");
            assert_eq!(meta.agent_count, 1);
        }

        clear_batch_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_send_batched_payloads_apm_mode_no_version_info() {
        clear_batch_state();

        let mut config = ExtensionConfig::default();
        config.new_relic.apm_lambda_mode = true;
        let config = Arc::new(config);
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));

        add_to_batch("req-1".into(), vec![1], Some("report".into()), "arn:test".into());

        send_batched_payloads_with_reports_only(newrelic_client, config).await;

        // Should be cleared after successful send
        assert_eq!(AGENT_BATCH_BUFFER.len(), 0);

        clear_batch_state();
    }

    // ========================================================================
    // send_all_pending_payloads_on_shutdown tests
    // ========================================================================

    #[tokio::test]
    #[serial]
    async fn test_shutdown_send_empty_returns_early() {
        clear_batch_state();
        crate::request::REQUEST_DATA.clear();

        let config = Arc::new(ExtensionConfig::default());
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));

        // Nothing in batch buffer or request buffers
        send_all_pending_payloads_on_shutdown(newrelic_client, config).await;

        // Should complete without error
        assert_eq!(AGENT_BATCH_BUFFER.len(), 0);

        clear_batch_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_shutdown_send_from_batch_buffer() {
        clear_batch_state();
        crate::request::REQUEST_DATA.clear();

        let config = Arc::new(ExtensionConfig::default());
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));

        add_to_batch("req-1".into(), vec![1, 2, 3], Some("report".into()), "arn:test".into());
        add_to_batch("req-2".into(), vec![4, 5, 6], None, "arn:test".into());

        send_all_pending_payloads_on_shutdown(newrelic_client, config).await;

        // Batch buffer should be cleared by get_and_clear_batch
        assert_eq!(AGENT_BATCH_BUFFER.len(), 0);

        clear_batch_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_shutdown_send_from_request_buffers() {
        clear_batch_state();
        crate::request::REQUEST_DATA.clear();

        let config = Arc::new(ExtensionConfig::default());
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));

        // Add payloads to REQUEST_DATA (simulating unbatched payloads)
        let buffer = Arc::new(std::sync::Mutex::new(vec![
            vec![10, 20, 30],
            vec![40, 50, 60],
        ]));
        let ctx = Arc::new(std::sync::Mutex::new(crate::context::InvocationContext {
            request_id: "req-buf-1".to_string(),
            invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:test-fn".to_string(),
            trace_id: None,
        }));
        crate::request::REQUEST_DATA.insert("req-buf-1".to_string(), crate::request::RequestData {
            context: ctx,
            agent_buffer: buffer,
            coordination_tx: None,
            pending_report: Some("REPORT Duration: 50ms".to_string()),
            creation_invocation: 0,
        });

        send_all_pending_payloads_on_shutdown(newrelic_client, config).await;

        // Batch buffer should be empty
        assert_eq!(AGENT_BATCH_BUFFER.len(), 0);

        crate::request::REQUEST_DATA.clear();
        clear_batch_state();
    }

    // ========================================================================
    // cleanup_old_batch_entries tests
    // ========================================================================

    #[tokio::test]
    #[serial]
    async fn test_cleanup_old_entries_none_old() {
        clear_batch_state();

        let config = Arc::new(ExtensionConfig::default());
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));

        // Add recent entries (not older than 5 minutes)
        add_to_batch("req-1".into(), vec![1], Some("r".into()), "arn".into());

        cleanup_old_batch_entries(newrelic_client, config).await;

        // Recent entries should remain
        assert_eq!(AGENT_BATCH_BUFFER.len(), 1);

        clear_batch_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_cleanup_old_entries_removes_old() {
        clear_batch_state();

        let config = Arc::new(ExtensionConfig::default());
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));

        // Insert old entry directly with timestamp > 5 min ago
        let old_timestamp = chrono::Utc::now() - chrono::Duration::minutes(10);
        AGENT_BATCH_BUFFER.insert(
            "old-req".to_string(),
            BatchedAgentPayload {
                request_id: "old-req".to_string(),
                agent_payload_bytes: Arc::new(vec![1, 2, 3]),
                report_line: Some("REPORT old".to_string()),
                invoked_function_arn: "arn:old".to_string(),
                timestamp: old_timestamp,
            },
        );

        // Also add a recent entry
        add_to_batch("new-req".into(), vec![4, 5], None, "arn:new".into());

        assert_eq!(AGENT_BATCH_BUFFER.len(), 2);

        cleanup_old_batch_entries(newrelic_client, config).await;

        // Old entry should be removed, new entry should remain
        assert_eq!(AGENT_BATCH_BUFFER.len(), 1);
        assert!(AGENT_BATCH_BUFFER.get("old-req").is_none());
        {
            let remaining = AGENT_BATCH_BUFFER.get("new-req").expect("new-req should remain");
            assert_eq!(remaining.request_id, "new-req");
        }

        // Metadata should be updated
        {
            let meta = BATCH_META.lock().expect("lock");
            assert_eq!(meta.agent_count, 1);
        }

        clear_batch_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_cleanup_old_entries_all_old() {
        clear_batch_state();

        let config = Arc::new(ExtensionConfig::default());
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));

        let old_timestamp = chrono::Utc::now() - chrono::Duration::minutes(10);
        AGENT_BATCH_BUFFER.insert(
            "old-1".to_string(),
            BatchedAgentPayload {
                request_id: "old-1".to_string(),
                agent_payload_bytes: Arc::new(vec![1]),
                report_line: None,
                invoked_function_arn: "arn".to_string(),
                timestamp: old_timestamp,
            },
        );
        AGENT_BATCH_BUFFER.insert(
            "old-2".to_string(),
            BatchedAgentPayload {
                request_id: "old-2".to_string(),
                agent_payload_bytes: Arc::new(vec![2]),
                report_line: Some("report".to_string()),
                invoked_function_arn: "arn".to_string(),
                timestamp: old_timestamp,
            },
        );

        cleanup_old_batch_entries(newrelic_client, config).await;

        // All old entries removed
        assert_eq!(AGENT_BATCH_BUFFER.len(), 0);

        {
            let meta = BATCH_META.lock().expect("lock");
            assert_eq!(meta.agent_count, 0);
            assert!(meta.oldest_timestamp.is_none());
        }

        clear_batch_state();
    }
}
