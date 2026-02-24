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
        RUNTIME_DONE_CHANNELS.clear();
        PENDING_REPORTS.clear();
        REQUEST_BUFFER_TIMESTAMPS.clear();

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
    fn test_current_active_request_id_starts_none() {
        clear_request_state();

        let id = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap().clone();
        assert!(id.is_none());

        clear_request_state();
    }

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

    #[test]
    #[serial]
    fn test_route_payload_to_active_request() {
        clear_request_state();

        let buffer = Arc::new(Mutex::new(Vec::new()));
        REQUEST_AGENT_BUFFERS.insert("req-1".to_string(), buffer.clone());

        {
            let mut active = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap();
            *active = Some("req-1".to_string());
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(route_payload_to_request_buffer(vec![10, 20, 30]));

        {
            let stored = buffer.lock().unwrap();
            assert_eq!(stored.len(), 1);
            assert_eq!(stored[0], vec![10, 20, 30]);
        }

        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_route_payload_to_any_buffer_when_no_active() {
        clear_request_state();

        // No active request, but a buffer exists
        let buffer = Arc::new(Mutex::new(Vec::new()));
        REQUEST_AGENT_BUFFERS.insert("some-req".to_string(), buffer.clone());

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(route_payload_to_request_buffer(vec![99]));

        {
            let stored = buffer.lock().unwrap();
            assert_eq!(stored.len(), 1);
            assert_eq!(stored[0], vec![99]);
        }

        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_route_payload_to_orphaned_when_no_buffers() {
        clear_request_state();

        // No active request, no buffers → orphaned
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(route_payload_to_request_buffer(vec![42]));

        {
            let orphaned = ORPHANED_PAYLOADS.lock().unwrap();
            assert_eq!(orphaned.len(), 1);
            assert_eq!(orphaned[0], vec![42]);
        }

        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_route_multiple_payloads_to_orphaned() {
        clear_request_state();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(route_payload_to_request_buffer(vec![1]));
        rt.block_on(route_payload_to_request_buffer(vec![2]));
        rt.block_on(route_payload_to_request_buffer(vec![3]));

        {
            let orphaned = ORPHANED_PAYLOADS.lock().unwrap();
            assert_eq!(orphaned.len(), 3);
        }

        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_route_signals_coordination_channel() {
        clear_request_state();

        let buffer = Arc::new(Mutex::new(Vec::new()));
        REQUEST_AGENT_BUFFERS.insert("req-1".to_string(), buffer);

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        PAYLOAD_COORDINATION.insert("req-1".to_string(), tx);

        {
            let mut active = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap();
            *active = Some("req-1".to_string());
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(route_payload_to_request_buffer(vec![1]));

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
        REQUEST_BUFFER_TIMESTAMPS.insert("req-1".to_string(), chrono::Utc::now());
        PENDING_REPORTS.insert("req-1".to_string(), "report".to_string());

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        PAYLOAD_COORDINATION.insert("req-1".to_string(), tx);
        let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel();
        RUNTIME_DONE_CHANNELS.insert("req-1".to_string(), tx2);

        cleanup_request_processing_state("req-1");

        assert!(REQUEST_CONTEXTS.get("req-1").is_none());
        assert!(REQUEST_AGENT_BUFFERS.get("req-1").is_none());
        assert!(REQUEST_BUFFER_TIMESTAMPS.get("req-1").is_none());
        assert!(PAYLOAD_COORDINATION.get("req-1").is_none());
        assert!(RUNTIME_DONE_CHANNELS.get("req-1").is_none());
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
        REQUEST_BUFFER_TIMESTAMPS.insert("req-1".to_string(), chrono::Utc::now());
        PENDING_REPORTS.insert("req-1".to_string(), "r".to_string());

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        RUNTIME_DONE_CHANNELS.insert("req-1".to_string(), tx);

        cleanup_request_processing_state_internal("req-1", true);

        // With skip_buffer_cleanup=true, these should be preserved
        assert!(REQUEST_CONTEXTS.get("req-1").is_some());
        assert!(REQUEST_AGENT_BUFFERS.get("req-1").is_some());
        assert!(REQUEST_BUFFER_TIMESTAMPS.get("req-1").is_some());

        // These should still be cleaned
        assert!(RUNTIME_DONE_CHANNELS.get("req-1").is_none());
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
    fn test_processor_factory_new() {
        clear_request_state();

        let config = Arc::new(crate::config::ExtensionConfig::default());
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));
        let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

        let factory = ProcessorFactory::new(newrelic_client, config, apm_app);

        // Factory should be created without error
        assert!(!format!("{:?}", factory).is_empty());

        clear_request_state();
    }

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
            false,
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
        assert!(REQUEST_BUFFER_TIMESTAMPS.get("req-create-1").is_some());
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
            false,
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

    #[tokio::test]
    #[serial]
    async fn test_cleanup_old_request_buffers_none_old() {
        clear_request_state();

        let config = Arc::new(crate::config::ExtensionConfig::default());
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));

        // Add a recent timestamp
        REQUEST_BUFFER_TIMESTAMPS.insert("recent-req".to_string(), chrono::Utc::now());

        cleanup_old_request_buffers(newrelic_client, config).await;

        // Recent entry should remain
        assert!(REQUEST_BUFFER_TIMESTAMPS.get("recent-req").is_some());

        clear_request_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_cleanup_old_request_buffers_removes_old() {
        clear_request_state();

        let config = Arc::new(crate::config::ExtensionConfig::default());
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));

        let old_time = chrono::Utc::now() - chrono::Duration::minutes(10);
        REQUEST_BUFFER_TIMESTAMPS.insert("old-req".to_string(), old_time);

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

        cleanup_old_request_buffers(newrelic_client, config).await;

        // Old request should be cleaned up
        assert!(REQUEST_BUFFER_TIMESTAMPS.get("old-req").is_none());
        assert!(REQUEST_CONTEXTS.get("old-req").is_none());
        assert!(REQUEST_AGENT_BUFFERS.get("old-req").is_none());

        clear_request_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_cleanup_old_request_buffers_empty_buffer() {
        clear_request_state();

        let config = Arc::new(crate::config::ExtensionConfig::default());
        let newrelic_client = Arc::new(crate::newrelic::client::NewRelicClient::new(&config));

        let old_time = chrono::Utc::now() - chrono::Duration::minutes(10);
        REQUEST_BUFFER_TIMESTAMPS.insert("old-empty".to_string(), old_time);

        // Buffer exists but is empty
        let buffer = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        REQUEST_AGENT_BUFFERS.insert("old-empty".to_string(), buffer);

        cleanup_old_request_buffers(newrelic_client, config).await;

        // Should still be cleaned up even with empty buffer
        assert!(REQUEST_BUFFER_TIMESTAMPS.get("old-empty").is_none());

        clear_request_state();
    }
}
