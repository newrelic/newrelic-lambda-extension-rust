//! Unit tests for request processing module
//!
//! Tests cover:
//! - CURRENT_ACTIVE_REQUEST_ID: singleton tracking
//! - TELEMETRY_CURRENT_REQUEST_ID: telemetry-based tracking
//! - ORPHANED_PAYLOADS: buffer + drain into first request
//! - route_payload_to_request_buffer: routing logic (active, late, orphan)
//! - create_request_processing_state: context creation, orphan draining
//! - cleanup_request_processing_state: proper cleanup

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use std::sync::{Arc, Mutex};

    use crate::request::*;
    use crate::context::InvocationContext;

    /// Helper: clear all global request state between tests
    fn clear_request_state() {
        REQUEST_PROCESSORS.clear();
        REQUEST_CONTEXTS.clear();
        REQUEST_AGENT_BUFFERS.clear();
        PAYLOAD_COORDINATION.clear();
        PENDING_REPORTS.clear();
        REQUEST_BUFFER_CREATION_INVOCATION.clear();

        // Reset invocation counter for test isolation
        reset_invocation_counter();

        if let Ok(mut active) = CURRENT_ACTIVE_REQUEST_ID.lock() {
            *active = None;
        }
        if let Ok(mut telemetry) = TELEMETRY_CURRENT_REQUEST_ID.lock() {
            *telemetry = None;
        }
        if let Ok(mut orphaned) = ORPHANED_PAYLOADS.lock() {
            orphaned.clear();
        }
    }

    // ========================================================================
    // CURRENT_ACTIVE_REQUEST_ID tests
    // ========================================================================

    #[test]
    #[serial]
    fn test_current_active_request_id_set_and_read() {
        clear_request_state();

        {
            let mut guard = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap();
            *guard = Some("req-abc-123".to_string());
        }

        let id = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap().clone();
        assert_eq!(id, Some("req-abc-123".to_string()));

        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_current_active_request_id_overwrite() {
        clear_request_state();

        {
            let mut guard = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap();
            *guard = Some("req-A".to_string());
        }
        {
            let mut guard = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap();
            *guard = Some("req-B".to_string());
        }

        let id = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap().clone();
        assert_eq!(id, Some("req-B".to_string()));

        clear_request_state();
    }

    // ========================================================================
    // TELEMETRY_CURRENT_REQUEST_ID tests
    // ========================================================================

    #[test]
    #[serial]
    fn test_telemetry_request_id_starts_none() {
        clear_request_state();

        let id = TELEMETRY_CURRENT_REQUEST_ID.lock().unwrap().clone();
        assert!(id.is_none());

        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_telemetry_request_id_set_from_platform_start() {
        clear_request_state();

        // Simulate what platform.start handler does
        let request_id_str = "telemetry-req-456";
        if let Ok(mut guard) = TELEMETRY_CURRENT_REQUEST_ID.lock() {
            *guard = Some(request_id_str.to_string());
        }

        let id = TELEMETRY_CURRENT_REQUEST_ID.lock().unwrap().clone();
        assert_eq!(id, Some("telemetry-req-456".to_string()));

        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_telemetry_and_active_are_independent() {
        clear_request_state();

        {
            let mut active = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap();
            *active = Some("active-req".to_string());
        }
        {
            let mut telemetry = TELEMETRY_CURRENT_REQUEST_ID.lock().unwrap();
            *telemetry = Some("telemetry-req".to_string());
        }

        let active = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap().clone();
        let telemetry = TELEMETRY_CURRENT_REQUEST_ID.lock().unwrap().clone();

        assert_eq!(active, Some("active-req".to_string()));
        assert_eq!(telemetry, Some("telemetry-req".to_string()));
        assert_ne!(active, telemetry);

        clear_request_state();
    }

    // ========================================================================
    // ORPHANED_PAYLOADS tests
    // ========================================================================

    #[test]
    #[serial]
    fn test_orphaned_payloads_starts_empty() {
        clear_request_state();

        {
            let orphaned = ORPHANED_PAYLOADS.lock().unwrap();
            assert!(orphaned.is_empty());
        }

        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_orphaned_payloads_store_and_drain() {
        clear_request_state();

        // Store orphaned payloads
        {
            let mut orphaned = ORPHANED_PAYLOADS.lock().unwrap();
            orphaned.push(vec![1, 2, 3]);
            orphaned.push(vec![4, 5, 6]);
        }

        assert_eq!(ORPHANED_PAYLOADS.lock().unwrap().len(), 2);

        // Drain them
        let drained: Vec<Vec<u8>> = {
            let mut orphaned = ORPHANED_PAYLOADS.lock().unwrap();
            orphaned.drain(..).collect()
        };

        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0], vec![1, 2, 3]);
        assert_eq!(drained[1], vec![4, 5, 6]);
        assert!(ORPHANED_PAYLOADS.lock().unwrap().is_empty());

        clear_request_state();
    }

    // ========================================================================
    // route_payload_to_request_buffer tests
    // ========================================================================

    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_route_payload_to_active_request() {
        clear_request_state();

        let buffer = Arc::new(Mutex::new(Vec::new()));
        REQUEST_AGENT_BUFFERS.insert("req-1".to_string(), buffer.clone());

        {
            let mut active = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap();
            *active = Some("req-1".to_string());
        }

        route_payload_to_request_buffer(vec![10, 20, 30]).await;

        {
            let stored = buffer.lock().unwrap();
            assert_eq!(stored.len(), 1);
            assert_eq!(stored[0], vec![10, 20, 30]);
        }

        clear_request_state();
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_route_payload_to_any_buffer_when_no_active() {
        clear_request_state();

        // No active request, but a buffer exists
        let buffer = Arc::new(Mutex::new(Vec::new()));
        REQUEST_AGENT_BUFFERS.insert("some-req".to_string(), buffer.clone());

        route_payload_to_request_buffer(vec![99]).await;

        {
            let stored = buffer.lock().unwrap();
            assert_eq!(stored.len(), 1);
            assert_eq!(stored[0], vec![99]);
        }

        clear_request_state();
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_route_payload_to_orphaned_when_no_buffers() {
        clear_request_state();

        // No active request, no buffers → orphaned
        route_payload_to_request_buffer(vec![42]).await;

        {
            let orphaned = ORPHANED_PAYLOADS.lock().unwrap();
            assert_eq!(orphaned.len(), 1);
            assert_eq!(orphaned[0], vec![42]);
        }

        clear_request_state();
    }



    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_route_signals_coordination_channel() {
        clear_request_state();

        let buffer = Arc::new(Mutex::new(Vec::new()));
        REQUEST_AGENT_BUFFERS.insert("req-1".to_string(), buffer);

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        PAYLOAD_COORDINATION.insert("req-1".to_string(), tx);

        {
            let mut active = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap();
            *active = Some("req-1".to_string());
        }

        route_payload_to_request_buffer(vec![1]).await;

        // Coordination channel should have been signaled
        let received = rx.try_recv();
        assert!(received.is_ok(), "Coordination signal should be sent");

        clear_request_state();
    }

    // ========================================================================
    // cleanup_request_processing_state tests
    // ========================================================================

    #[test]
    #[serial]
    fn test_cleanup_removes_all_state() {
        clear_request_state();

        let ctx = Arc::new(Mutex::new(InvocationContext {
            request_id: "req-1".to_string(),
            invoked_function_arn: "arn".to_string(),
            trace_id: None,
        }));
        REQUEST_CONTEXTS.insert("req-1".to_string(), ctx);
        REQUEST_AGENT_BUFFERS.insert("req-1".to_string(), Arc::new(Mutex::new(Vec::new())));
        REQUEST_BUFFER_CREATION_INVOCATION.insert("req-1".to_string(), 0);
        PENDING_REPORTS.insert("req-1".to_string(), "report".to_string());

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        PAYLOAD_COORDINATION.insert("req-1".to_string(), tx);

        cleanup_request_processing_state("req-1");

        assert!(REQUEST_CONTEXTS.get("req-1").is_none());
        assert!(REQUEST_AGENT_BUFFERS.get("req-1").is_none());
        assert!(REQUEST_BUFFER_CREATION_INVOCATION.get("req-1").is_none());
        assert!(PAYLOAD_COORDINATION.get("req-1").is_none());
        assert!(PENDING_REPORTS.get("req-1").is_none());

        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_cleanup_internal_skip_buffer_preserves_buffers() {
        clear_request_state();

        let ctx = Arc::new(Mutex::new(InvocationContext::default()));
        REQUEST_CONTEXTS.insert("req-1".to_string(), ctx);
        REQUEST_AGENT_BUFFERS.insert("req-1".to_string(), Arc::new(Mutex::new(Vec::new())));
        REQUEST_BUFFER_CREATION_INVOCATION.insert("req-1".to_string(), 0);
        PENDING_REPORTS.insert("req-1".to_string(), "r".to_string());

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        PAYLOAD_COORDINATION.insert("req-1".to_string(), tx);

        cleanup_request_processing_state_internal("req-1", true);

        // With skip_buffer_cleanup=true, these should be preserved
        assert!(REQUEST_CONTEXTS.get("req-1").is_some());
        assert!(REQUEST_AGENT_BUFFERS.get("req-1").is_some());
        assert!(REQUEST_BUFFER_CREATION_INVOCATION.get("req-1").is_some());

        // PAYLOAD_COORDINATION is always cleaned (receiver consumed by process_request_concurrently)
        assert!(PAYLOAD_COORDINATION.get("req-1").is_none());
        // PENDING_REPORTS is always cleaned
        assert!(PENDING_REPORTS.get("req-1").is_none());

        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_cleanup_nonexistent_request_is_safe() {
        clear_request_state();

        // Should not panic
        cleanup_request_processing_state("nonexistent-req");

        clear_request_state();
    }

    // ========================================================================
    // ProcessorFactory and create_request_processing_state tests
    // ========================================================================



    #[test]
    #[serial]
    fn test_create_request_processing_state_basic() {
        clear_request_state();

        let config = Arc::new(crate::config::ExtensionConfig::default());
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));
        let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
        let factory = Arc::new(ProcessorFactory::new(newrelic_client, config, apm_app));

        let state = create_request_processing_state(
            "req-create-1",
            "arn:aws:lambda:us-east-1:123:function:test-fn",
            &factory,
        );

        // Verify context was created correctly
        {
            let ctx = state.context.lock().expect("lock");
            assert_eq!(ctx.request_id, "req-create-1");
            assert_eq!(ctx.invoked_function_arn, "arn:aws:lambda:us-east-1:123:function:test-fn");
        }

        // Verify buffer is empty
        {
            let buf = state.agent_buffer.lock().expect("lock");
            assert!(buf.is_empty());
        }

        // Verify coordination channel exists
        assert!(state.coordination_rx.is_some());

        // Verify global maps were populated
        assert!(REQUEST_CONTEXTS.get("req-create-1").is_some());
        assert!(REQUEST_AGENT_BUFFERS.get("req-create-1").is_some());
        assert!(REQUEST_BUFFER_CREATION_INVOCATION.get("req-create-1").is_some());
        assert!(PAYLOAD_COORDINATION.get("req-create-1").is_some());

        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_create_request_processing_state_drains_orphans() {
        clear_request_state();

        // Pre-load orphaned payloads
        {
            let mut orphaned = ORPHANED_PAYLOADS.lock().expect("lock");
            orphaned.push(vec![10, 20, 30]);
            orphaned.push(vec![40, 50, 60]);
        }

        let config = Arc::new(crate::config::ExtensionConfig::default());
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));
        let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
        let factory = Arc::new(ProcessorFactory::new(newrelic_client, config, apm_app));

        let state = create_request_processing_state(
            "req-drain-1",
            "arn:test",
            &factory,
        );

        // Orphaned payloads should be drained into the request's buffer
        {
            let buf = state.agent_buffer.lock().expect("lock");
            assert_eq!(buf.len(), 2);
            assert_eq!(buf[0], vec![10, 20, 30]);
            assert_eq!(buf[1], vec![40, 50, 60]);
        }

        // Orphaned buffer should be empty now
        {
            let orphaned = ORPHANED_PAYLOADS.lock().expect("lock");
            assert!(orphaned.is_empty());
        }

        clear_request_state();
    }

    // ========================================================================
    // cleanup_old_request_buffers tests
    // ========================================================================

    /// Helper: create a noop newrelic client + config for cleanup tests
    fn make_test_client_and_config() -> (Arc<crate::newrelic::client::NewRelicClient>, Arc<crate::config::ExtensionConfig>) {
        let config = Arc::new(crate::config::ExtensionConfig::default());
        let client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        (client, config)
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_cleanup_old_request_buffers_none_old() {
        clear_request_state();

        // Buffer created at current invocation (0) — not stale
        REQUEST_BUFFER_CREATION_INVOCATION.insert("recent-req".to_string(), current_invocation_count());

        let (client, config) = make_test_client_and_config();
        cleanup_old_request_buffers(client, config).await;

        // Recent entry should remain
        assert!(REQUEST_BUFFER_CREATION_INVOCATION.get("recent-req").is_some());

        clear_request_state();
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_cleanup_old_request_buffers_removes_old() {
        clear_request_state();

        // Simulate a buffer created 10 invocations ago (stale: >= 5 threshold)
        REQUEST_BUFFER_CREATION_INVOCATION.insert("old-req".to_string(), 0);
        // Advance counter to invocation 10
        for _ in 0..10 {
            increment_invocation_counter();
        }

        // Add buffer with agent data for the old request
        let buffer = Arc::new(Mutex::new(vec![vec![1, 2, 3]]));
        REQUEST_AGENT_BUFFERS.insert("old-req".to_string(), buffer);

        // Add context
        let ctx = Arc::new(Mutex::new(InvocationContext {
            request_id: "old-req".to_string(),
            invoked_function_arn: "arn:test".to_string(),
            trace_id: None,
        }));
        REQUEST_CONTEXTS.insert("old-req".to_string(), ctx);

        // Add pending report
        PENDING_REPORTS.insert("old-req".to_string(), "REPORT old".to_string());

        let (client, config) = make_test_client_and_config();
        cleanup_old_request_buffers(client, config).await;

        // Old request should be cleaned up
        assert!(REQUEST_BUFFER_CREATION_INVOCATION.get("old-req").is_none());
        assert!(REQUEST_CONTEXTS.get("old-req").is_none());
        assert!(REQUEST_AGENT_BUFFERS.get("old-req").is_none());

        clear_request_state();
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_cleanup_old_request_buffers_empty_buffer() {
        clear_request_state();

        // Buffer created 10 invocations ago but empty
        REQUEST_BUFFER_CREATION_INVOCATION.insert("old-empty".to_string(), 0);
        for _ in 0..10 {
            increment_invocation_counter();
        }

        // Buffer exists but is empty
        let buffer = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        REQUEST_AGENT_BUFFERS.insert("old-empty".to_string(), buffer);

        let (client, config) = make_test_client_and_config();
        cleanup_old_request_buffers(client, config).await;

        // Should still be cleaned up even with empty buffer
        assert!(REQUEST_BUFFER_CREATION_INVOCATION.get("old-empty").is_none());

        clear_request_state();
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_cleanup_old_preserves_recent_removes_old() {
        clear_request_state();

        // Advance counter to 10
        for _ in 0..10 {
            increment_invocation_counter();
        }

        // "recent" created at invocation 10 (current) — not stale
        REQUEST_BUFFER_CREATION_INVOCATION.insert("recent".to_string(), current_invocation_count());
        // "old" created at invocation 0 — stale (10 invocations ago >= 5)
        REQUEST_BUFFER_CREATION_INVOCATION.insert("old".to_string(), 0);

        REQUEST_AGENT_BUFFERS.insert("recent".to_string(), Arc::new(Mutex::new(vec![vec![1]])));
        REQUEST_AGENT_BUFFERS.insert("old".to_string(), Arc::new(Mutex::new(vec![vec![2]])));

        let (client, config) = make_test_client_and_config();
        cleanup_old_request_buffers(client, config).await;

        // Recent should stay, old should go
        assert!(REQUEST_BUFFER_CREATION_INVOCATION.get("recent").is_some());
        assert!(REQUEST_AGENT_BUFFERS.get("recent").is_some());
        assert!(REQUEST_BUFFER_CREATION_INVOCATION.get("old").is_none());
        assert!(REQUEST_AGENT_BUFFERS.get("old").is_none());

        clear_request_state();
    }

    // ========================================================================
    // Race condition and concurrent access tests
    // ========================================================================

    /// Simulate rapid request_id overwriting (back-to-back invocations)
    #[test]
    #[serial]
    fn test_rapid_active_request_id_overwrite() {
        clear_request_state();

        // Simulate 100 rapid INVOKE events overwriting the active request
        for i in 0..100 {
            let id = format!("req-rapid-{}", i);
            let mut guard = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap();
            *guard = Some(id);
        }

        // Only the last one should be active
        let final_id = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap().clone();
        assert_eq!(final_id, Some("req-rapid-99".to_string()));

        clear_request_state();
    }

    /// Simulate late payload arriving when no active request exists
    /// (e.g., between invocations after cleanup)
    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_late_payload_after_active_request_cleared() {
        clear_request_state();

        // Set active request and a buffer
        {
            let mut guard = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap();
            *guard = Some("req-old".to_string());
        }
        REQUEST_AGENT_BUFFERS.insert("req-old".to_string(), Arc::new(Mutex::new(Vec::new())));

        // Now clear active (simulating cleanup between invocations)
        {
            let mut guard = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap();
            *guard = None;
        }

        // Late payload should fall back to any existing buffer
        route_payload_to_request_buffer(vec![88]).await;

        // Should be in req-old's buffer (fallback to any existing buffer)
        let stored = REQUEST_AGENT_BUFFERS
            .get("req-old")
            .map(|b| b.lock().unwrap().len())
            .unwrap_or(0);
        assert_eq!(stored, 1);

        clear_request_state();
    }

    /// Multiple orphaned payloads should all drain into first created request
    #[test]
    #[serial]
    fn test_multiple_orphans_drain_into_single_request() {
        clear_request_state();

        // Store 5 orphaned payloads
        {
            let mut orphaned = ORPHANED_PAYLOADS.lock().unwrap();
            for i in 0..5 {
                orphaned.push(vec![i as u8]);
            }
        }
        assert_eq!(ORPHANED_PAYLOADS.lock().unwrap().len(), 5);

        let config = Arc::new(crate::config::ExtensionConfig::default());
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));
        let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
        let factory = Arc::new(ProcessorFactory::new(newrelic_client, config, apm_app));

        let state = create_request_processing_state("req-drain-all", "arn:test", &factory);

        // All 5 should be drained
        {
            let buf = state.agent_buffer.lock().unwrap();
            assert_eq!(buf.len(), 5);
            assert_eq!(buf[0], vec![0]);
            assert_eq!(buf[4], vec![4]);
        }

        // Second request should get empty orphan buffer
        let state2 = create_request_processing_state("req-drain-none", "arn:test", &factory);
        {
            let buf = state2.agent_buffer.lock().unwrap();
            assert!(buf.is_empty());
        }

        clear_request_state();
    }

    /// Payload routing should signal coordination channel exactly once per payload
    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_coordination_channel_signaled_per_payload() {
        clear_request_state();

        let buffer = Arc::new(Mutex::new(Vec::new()));
        REQUEST_AGENT_BUFFERS.insert("req-coord".to_string(), buffer);

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        PAYLOAD_COORDINATION.insert("req-coord".to_string(), tx);

        {
            let mut active = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap();
            *active = Some("req-coord".to_string());
        }

        // Send 3 payloads
        route_payload_to_request_buffer(vec![1]).await;
        route_payload_to_request_buffer(vec![2]).await;
        route_payload_to_request_buffer(vec![3]).await;

        // Should receive exactly 3 signals
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err()); // No more

        clear_request_state();
    }

    /// Cleanup with skip_buffer should still clean PAYLOAD_COORDINATION
    /// since the receiver is already consumed by process_request_concurrently
    #[test]
    #[serial]
    fn test_cleanup_skip_buffer_still_cleans_coordination() {
        clear_request_state();

        let buffer = Arc::new(Mutex::new(Vec::new()));
        REQUEST_AGENT_BUFFERS.insert("req-sk".to_string(), buffer);
        REQUEST_BUFFER_CREATION_INVOCATION.insert("req-sk".to_string(), 0);

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        PAYLOAD_COORDINATION.insert("req-sk".to_string(), tx);

        cleanup_request_processing_state_internal("req-sk", true);

        // Buffer preserved
        assert!(REQUEST_AGENT_BUFFERS.get("req-sk").is_some());
        // But coordination sender should be cleaned (receiver already consumed)
        assert!(PAYLOAD_COORDINATION.get("req-sk").is_none());

        clear_request_state();
    }

    /// Sending to a dropped coordination channel should not panic
    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_send_to_dropped_coordination_channel_is_safe() {
        clear_request_state();

        let buffer = Arc::new(Mutex::new(Vec::new()));
        REQUEST_AGENT_BUFFERS.insert("req-drop".to_string(), buffer.clone());

        // Create channel and immediately drop receiver
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        PAYLOAD_COORDINATION.insert("req-drop".to_string(), tx);
        drop(rx);

        {
            let mut active = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap();
            *active = Some("req-drop".to_string());
        }

        // Should NOT panic even though receiver is dropped
        route_payload_to_request_buffer(vec![42]).await;

        // Payload should still be in buffer
        let stored = buffer.lock().unwrap().len();
        assert_eq!(stored, 1);

        clear_request_state();
    }

    /// TELEMETRY_CURRENT_REQUEST_ID and CURRENT_ACTIVE_REQUEST_ID
    /// can diverge temporarily — this is by design
    #[test]
    #[serial]
    fn test_telemetry_and_active_ids_can_diverge() {
        clear_request_state();

        // Event loop sets active to req-B (new invoke arrived)
        {
            let mut active = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap();
            *active = Some("req-B".to_string());
        }

        // But telemetry platform.start hasn't arrived yet, still on req-A
        {
            let mut telemetry = TELEMETRY_CURRENT_REQUEST_ID.lock().unwrap();
            *telemetry = Some("req-A".to_string());
        }

        // They should be different — this is the whole point of dual tracking
        let active = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap().clone();
        let telemetry = TELEMETRY_CURRENT_REQUEST_ID.lock().unwrap().clone();

        assert_eq!(active, Some("req-B".to_string()));
        assert_eq!(telemetry, Some("req-A".to_string()));

        // Agent payloads route via CURRENT_ACTIVE_REQUEST_ID → req-B
        // Function logs stamp via TELEMETRY_CURRENT_REQUEST_ID → req-A (correct!)

        clear_request_state();
    }

    /// Multiple requests creating state should get independent buffers
    #[test]
    #[serial]
    fn test_multiple_requests_get_independent_buffers() {
        clear_request_state();

        let config = Arc::new(crate::config::ExtensionConfig::default());
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));
        let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
        let factory = Arc::new(ProcessorFactory::new(newrelic_client, config, apm_app));

        let state_a = create_request_processing_state("req-A", "arn:a", &factory);
        let state_b = create_request_processing_state("req-B", "arn:b", &factory);

        // Add data to A's buffer
        state_a.agent_buffer.lock().unwrap().push(vec![1, 2, 3]);

        // B's buffer should be unaffected
        assert!(state_b.agent_buffer.lock().unwrap().is_empty());

        // Global maps should have both
        assert!(REQUEST_AGENT_BUFFERS.get("req-A").is_some());
        assert!(REQUEST_AGENT_BUFFERS.get("req-B").is_some());

        // A's global buffer should have the data
        let global_a_len = REQUEST_AGENT_BUFFERS
            .get("req-A")
            .map(|b| b.lock().unwrap().len())
            .unwrap_or(0);
        assert_eq!(global_a_len, 1);

        clear_request_state();
    }

    /// Cleanup of one request should not affect another
    #[test]
    #[serial]
    fn test_cleanup_one_request_does_not_affect_another() {
        clear_request_state();

        let ctx_a = Arc::new(Mutex::new(InvocationContext {
            request_id: "req-A".to_string(),
            invoked_function_arn: "arn:a".to_string(),
            trace_id: None,
        }));
        let ctx_b = Arc::new(Mutex::new(InvocationContext {
            request_id: "req-B".to_string(),
            invoked_function_arn: "arn:b".to_string(),
            trace_id: None,
        }));

        REQUEST_CONTEXTS.insert("req-A".to_string(), ctx_a);
        REQUEST_CONTEXTS.insert("req-B".to_string(), ctx_b);
        REQUEST_AGENT_BUFFERS.insert("req-A".to_string(), Arc::new(Mutex::new(vec![vec![1]])));
        REQUEST_AGENT_BUFFERS.insert("req-B".to_string(), Arc::new(Mutex::new(vec![vec![2]])));
        REQUEST_BUFFER_CREATION_INVOCATION.insert("req-A".to_string(), 0);
        REQUEST_BUFFER_CREATION_INVOCATION.insert("req-B".to_string(), 0);

        // Clean only A
        cleanup_request_processing_state("req-A");

        // A gone
        assert!(REQUEST_CONTEXTS.get("req-A").is_none());
        assert!(REQUEST_AGENT_BUFFERS.get("req-A").is_none());

        // B still intact
        assert!(REQUEST_CONTEXTS.get("req-B").is_some());
        assert!(REQUEST_AGENT_BUFFERS.get("req-B").is_some());
        let b_data = REQUEST_AGENT_BUFFERS.get("req-B").unwrap().lock().unwrap().clone();
        assert_eq!(b_data, vec![vec![2]]);

        clear_request_state();
    }











    /// PENDING_REPORTS should be isolated per request_id
    #[test]
    #[serial]
    fn test_pending_reports_per_request_id() {
        clear_request_state();

        PENDING_REPORTS.insert("req-X".to_string(), "REPORT for X".to_string());
        PENDING_REPORTS.insert("req-Y".to_string(), "REPORT for Y".to_string());

        // Each request has its own report
        assert_eq!(PENDING_REPORTS.get("req-X").unwrap().value(), "REPORT for X");
        assert_eq!(PENDING_REPORTS.get("req-Y").unwrap().value(), "REPORT for Y");

        // Removing X doesn't affect Y
        PENDING_REPORTS.remove("req-X");
        assert!(PENDING_REPORTS.get("req-X").is_none());
        assert_eq!(PENDING_REPORTS.get("req-Y").unwrap().value(), "REPORT for Y");

        clear_request_state();
    }



    /// Coordination channel: payload arriving BEFORE coordination_rx is polled
    /// should still be received (buffered in unbounded channel)
    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_coordination_payload_before_poll() {
        clear_request_state();

        let config = Arc::new(crate::config::ExtensionConfig::default());
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));
        let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
        let factory = Arc::new(ProcessorFactory::new(newrelic_client, config, apm_app));

        let mut state = create_request_processing_state("coord-req", "arn:test", &factory);
        {
            let mut active = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap();
            *active = Some("coord-req".to_string());
        }

        // Payload arrives BEFORE anyone polls coordination_rx
        route_payload_to_request_buffer(vec![11, 22]).await;

        // Now poll coordination_rx — should immediately receive the signal
        let rx = state.coordination_rx.as_mut().expect("has rx");
        let result = tokio::time::timeout(
            tokio::time::Duration::from_millis(10),
            rx.recv(),
        ).await;

        assert!(result.is_ok(), "Signal should be available immediately (buffered)");
        assert!(result.unwrap().is_some(), "Channel should not be closed");

        clear_request_state();
    }

    /// Coordination channel: timeout when no payload arrives
    /// (simulates the 100ms wait in process_request_concurrently)
    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_coordination_timeout_no_payload() {
        clear_request_state();

        let config = Arc::new(crate::config::ExtensionConfig::default());
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));
        let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));
        let factory = Arc::new(ProcessorFactory::new(newrelic_client, config, apm_app));

        let mut state = create_request_processing_state("timeout-req", "arn:test", &factory);

        // No payload sent — should timeout
        let rx = state.coordination_rx.as_mut().expect("has rx");
        let result = tokio::time::timeout(
            tokio::time::Duration::from_millis(50),
            rx.recv(),
        ).await;

        assert!(result.is_err(), "Should timeout since no payload arrived");

        clear_request_state();
    }





}
