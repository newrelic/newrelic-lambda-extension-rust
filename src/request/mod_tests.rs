// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

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
        REQUEST_DATA.clear();

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
        REQUEST_DATA.insert("req-1".to_string(), RequestData {
            context: Arc::new(Mutex::new(InvocationContext::default())),
            agent_buffer: buffer.clone(),
            pending_report: None,
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });

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
        REQUEST_DATA.insert("some-req".to_string(), RequestData {
            context: Arc::new(Mutex::new(InvocationContext::default())),
            agent_buffer: buffer.clone(),
            pending_report: None,
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });

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
        REQUEST_DATA.insert("req-1".to_string(), RequestData {
            context: ctx,
            agent_buffer: Arc::new(Mutex::new(Vec::new())),
            pending_report: Some("report".to_string()),
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });

        cleanup_request_processing_state("req-1");

        assert!(REQUEST_DATA.get("req-1").is_none());

        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_cleanup_internal_skip_buffer_preserves_buffers() {
        clear_request_state();

        let ctx = Arc::new(Mutex::new(InvocationContext::default()));
        REQUEST_DATA.insert("req-1".to_string(), RequestData {
            context: ctx,
            agent_buffer: Arc::new(Mutex::new(Vec::new())),
            pending_report: Some("r".to_string()),
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });

        cleanup_request_processing_state_internal("req-1", true);

        // With skip_buffer_cleanup=true, entry should still exist with preserved fields
        {
            let entry = REQUEST_DATA.get("req-1");
            assert!(entry.is_some());
            let entry = entry.unwrap();
            // Context and buffer are preserved
            assert!(entry.context.lock().is_ok());
            assert!(entry.agent_buffer.lock().is_ok());
            // pending_report is preserved too — NOT cleared on partial cleanup. It's
            // normally already None by this point (remove_pending_report() already took
            // it at the start of the caller's processing), but process_request_concurrently
            // can legitimately re-set it (restoring a report whose payload never showed up
            // within the blocking-agent-payload wait window) so the next invocation or
            // SHUTDOWN can still find it. Unconditionally clearing it here would silently
            // undo that restoration — this was a real bug, fixed alongside
            // NEW_RELIC_BLOCKING_AGENT_PAYLOAD.
            assert_eq!(entry.pending_report, Some("r".to_string()));
        }

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

        // Verify global REQUEST_DATA was populated
        assert!(REQUEST_DATA.get("req-create-1").is_some());

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
        REQUEST_DATA.insert("recent-req".to_string(), RequestData {
            context: Arc::new(Mutex::new(InvocationContext::default())),
            agent_buffer: Arc::new(Mutex::new(Vec::new())),
            pending_report: None,
            creation_invocation: current_invocation_count(),
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });

        let (client, config) = make_test_client_and_config();
        cleanup_old_request_buffers(client, config).await;

        // Recent entry should remain
        assert!(REQUEST_DATA.get("recent-req").is_some());

        clear_request_state();
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_cleanup_old_request_buffers_removes_old() {
        clear_request_state();

        // Simulate a buffer created 10 invocations ago (stale: >= 5 threshold)
        let ctx = Arc::new(Mutex::new(InvocationContext {
            request_id: "old-req".to_string(),
            invoked_function_arn: "arn:test".to_string(),
            trace_id: None,
        }));
        REQUEST_DATA.insert("old-req".to_string(), RequestData {
            context: ctx,
            agent_buffer: Arc::new(Mutex::new(vec![vec![1, 2, 3]])),
            pending_report: Some("REPORT old".to_string()),
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });
        // Advance counter to invocation 10
        for _ in 0..10 {
            increment_invocation_counter();
        }

        let (client, config) = make_test_client_and_config();
        cleanup_old_request_buffers(client, config).await;

        // Old request should be cleaned up
        assert!(REQUEST_DATA.get("old-req").is_none());

        clear_request_state();
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_cleanup_old_request_buffers_empty_buffer() {
        clear_request_state();

        // Buffer created 10 invocations ago but empty
        REQUEST_DATA.insert("old-empty".to_string(), RequestData {
            context: Arc::new(Mutex::new(InvocationContext::default())),
            agent_buffer: Arc::new(Mutex::new(Vec::<Vec<u8>>::new())),
            pending_report: None,
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });
        for _ in 0..10 {
            increment_invocation_counter();
        }

        let (client, config) = make_test_client_and_config();
        cleanup_old_request_buffers(client, config).await;

        // Should still be cleaned up even with empty buffer
        assert!(REQUEST_DATA.get("old-empty").is_none());

        clear_request_state();
    }

    // In APM mode the periodic cleanup must NOT use the serverless ingest
    // endpoint. A stranded stale payload must be re-queued to the APM retry
    // buffer (FAILED_AGENT_PAYLOADS) so retry_failed_agent_payloads resends it
    // to the APM collector — never leaked to serverless.
    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_cleanup_apm_mode_routes_stale_payload_to_apm_retry_buffer() {
        clear_request_state();
        if let Ok(mut b) = crate::event_loop::FAILED_AGENT_PAYLOADS.lock() {
            b.clear();
        }

        // Stale request (created 10 invocations ago) holding an un-drained payload.
        REQUEST_DATA.insert("apm-old-req".to_string(), RequestData {
            context: Arc::new(Mutex::new(InvocationContext {
                request_id: "apm-old-req".to_string(),
                invoked_function_arn: "arn:test".to_string(),
                trace_id: None,
            })),
            agent_buffer: Arc::new(Mutex::new(vec![vec![9, 9, 9]])),
            pending_report: None,
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
            invoked_function_arn: "arn:test".to_string(),
        });
        for _ in 0..10 {
            increment_invocation_counter();
        }

        // APM mode ON.
        let client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let mut cfg = crate::config::ExtensionConfig::default();
        cfg.new_relic.apm_lambda_mode = true;
        let config = Arc::new(cfg);

        cleanup_old_request_buffers(client, config).await;

        // Entry removed, and its payload routed to the APM retry buffer.
        assert!(REQUEST_DATA.get("apm-old-req").is_none());
        let routed_to_apm = crate::event_loop::FAILED_AGENT_PAYLOADS
            .lock()
            .map(|b| b.iter().any(|p| p.request_id == "apm-old-req"))
            .unwrap_or(false);
        assert!(
            routed_to_apm,
            "APM-mode stale payload must be re-queued to the APM retry buffer, not sent to serverless"
        );

        if let Ok(mut b) = crate::event_loop::FAILED_AGENT_PAYLOADS.lock() {
            b.clear();
        }
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
        REQUEST_DATA.insert("recent".to_string(), RequestData {
            context: Arc::new(Mutex::new(InvocationContext::default())),
            agent_buffer: Arc::new(Mutex::new(vec![vec![1]])),
            pending_report: None,
            creation_invocation: current_invocation_count(),
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });
        // "old" created at invocation 0 — stale (10 invocations ago >= 5)
        REQUEST_DATA.insert("old".to_string(), RequestData {
            context: Arc::new(Mutex::new(InvocationContext::default())),
            agent_buffer: Arc::new(Mutex::new(vec![vec![2]])),
            pending_report: None,
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });

        let (client, config) = make_test_client_and_config();
        cleanup_old_request_buffers(client, config).await;

        // Recent should stay, old should go
        assert!(REQUEST_DATA.get("recent").is_some());
        assert!(REQUEST_DATA.get("old").is_none());

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
        REQUEST_DATA.insert("req-old".to_string(), RequestData {
            context: Arc::new(Mutex::new(InvocationContext::default())),
            agent_buffer: Arc::new(Mutex::new(Vec::new())),
            pending_report: None,
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });

        // Now clear active (simulating cleanup between invocations)
        {
            let mut guard = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap();
            *guard = None;
        }

        // Late payload should fall back to any existing buffer
        route_payload_to_request_buffer(vec![88]).await;

        // Should be in req-old's buffer (fallback to any existing buffer)
        let stored = get_agent_buffer("req-old")
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
        assert!(REQUEST_DATA.get("req-A").is_some());
        assert!(REQUEST_DATA.get("req-B").is_some());

        // A's global buffer should have the data
        let global_a_len = get_agent_buffer("req-A")
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

        REQUEST_DATA.insert("req-A".to_string(), RequestData {
            context: ctx_a,
            agent_buffer: Arc::new(Mutex::new(vec![vec![1]])),
            pending_report: None,
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });
        REQUEST_DATA.insert("req-B".to_string(), RequestData {
            context: ctx_b,
            agent_buffer: Arc::new(Mutex::new(vec![vec![2]])),
            pending_report: None,
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });

        // Clean only A
        cleanup_request_processing_state("req-A");

        // A gone
        assert!(REQUEST_DATA.get("req-A").is_none());

        // B still intact
        assert!(REQUEST_DATA.get("req-B").is_some());
        let b_data = get_agent_buffer("req-B").unwrap().lock().unwrap().clone();
        assert_eq!(b_data, vec![vec![2]]);

        clear_request_state();
    }











    /// pending_report should be isolated per request_id
    #[test]
    #[serial]
    fn test_pending_reports_per_request_id() {
        clear_request_state();

        REQUEST_DATA.insert("req-X".to_string(), RequestData {
            context: Arc::new(Mutex::new(InvocationContext::default())),
            agent_buffer: Arc::new(Mutex::new(Vec::new())),
            pending_report: Some("REPORT for X".to_string()),
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });
        REQUEST_DATA.insert("req-Y".to_string(), RequestData {
            context: Arc::new(Mutex::new(InvocationContext::default())),
            agent_buffer: Arc::new(Mutex::new(Vec::new())),
            pending_report: Some("REPORT for Y".to_string()),
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });

        // Each request has its own report
        assert_eq!(get_pending_report("req-X").unwrap(), "REPORT for X");
        assert_eq!(get_pending_report("req-Y").unwrap(), "REPORT for Y");

        // Removing X doesn't affect Y
        remove_pending_report("req-X");
        assert!(get_pending_report("req-X").is_none());
        assert_eq!(get_pending_report("req-Y").unwrap(), "REPORT for Y");

        clear_request_state();
    }

    // ========================================================================
    // agent_payload_notify / report_notify tests (serverless-mode blocking-agent-payload)
    // ========================================================================

    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_agent_payload_notify_fires_on_active_route() {
        clear_request_state();

        let buffer = Arc::new(Mutex::new(Vec::new()));
        let notify = Arc::new(tokio::sync::Notify::new());
        REQUEST_DATA.insert("req-1".to_string(), RequestData {
            context: Arc::new(Mutex::new(InvocationContext::default())),
            agent_buffer: buffer.clone(),
            pending_report: None,
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: notify.clone(),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });

        {
            let mut active = CURRENT_ACTIVE_REQUEST_ID.lock().unwrap();
            *active = Some("req-1".to_string());
        }

        let waiter = tokio::spawn(async move {
            notify.notified().await;
        });

        route_payload_to_request_buffer(vec![1, 2, 3]).await;

        tokio::time::timeout(std::time::Duration::from_millis(500), waiter)
            .await
            .expect("notify should fire within timeout")
            .expect("waiter task should not panic");

        clear_request_state();
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_agent_payload_notify_fires_on_fallback_route() {
        clear_request_state();

        // No active request, but a buffer exists — exercises the fallback branch.
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let notify = Arc::new(tokio::sync::Notify::new());
        REQUEST_DATA.insert("some-req".to_string(), RequestData {
            context: Arc::new(Mutex::new(InvocationContext::default())),
            agent_buffer: buffer.clone(),
            pending_report: None,
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: notify.clone(),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });

        let waiter = tokio::spawn(async move {
            notify.notified().await;
        });

        route_payload_to_request_buffer(vec![9, 9, 9]).await;

        tokio::time::timeout(std::time::Duration::from_millis(500), waiter)
            .await
            .expect("notify should fire within timeout on the fallback route")
            .expect("waiter task should not panic");

        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_get_agent_payload_notify_returns_none_for_unknown_request() {
        clear_request_state();
        assert!(get_agent_payload_notify("does-not-exist").is_none());
        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_get_agent_payload_notify_returns_some_for_known_request() {
        clear_request_state();

        REQUEST_DATA.insert("req-1".to_string(), RequestData {
            context: Arc::new(Mutex::new(InvocationContext::default())),
            agent_buffer: Arc::new(Mutex::new(Vec::new())),
            pending_report: None,
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });

        assert!(get_agent_payload_notify("req-1").is_some());

        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_take_agent_buffer_if_nonempty_drains_and_empties() {
        clear_request_state();

        let buffer = Arc::new(Mutex::new(vec![vec![1u8, 2, 3], vec![4, 5]]));
        REQUEST_DATA.insert("req-1".to_string(), RequestData {
            context: Arc::new(Mutex::new(InvocationContext::default())),
            agent_buffer: buffer.clone(),
            pending_report: None,
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });

        let taken = take_agent_buffer_if_nonempty("req-1");
        assert_eq!(taken, Some(vec![vec![1, 2, 3], vec![4, 5]]));
        assert!(take_agent_buffer_if_nonempty("req-1").is_none());
        assert!(buffer.lock().unwrap().is_empty());

        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_take_agent_buffer_if_nonempty_returns_none_when_empty_or_missing() {
        clear_request_state();

        REQUEST_DATA.insert("req-1".to_string(), RequestData {
            context: Arc::new(Mutex::new(InvocationContext::default())),
            agent_buffer: Arc::new(Mutex::new(Vec::new())),
            pending_report: None,
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });

        assert!(take_agent_buffer_if_nonempty("req-1").is_none());
        assert!(take_agent_buffer_if_nonempty("does-not-exist").is_none());

        clear_request_state();
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial]
    async fn test_report_notify_fires_on_set_pending_report() {
        clear_request_state();

        let notify = Arc::new(tokio::sync::Notify::new());
        REQUEST_DATA.insert("req-1".to_string(), RequestData {
            context: Arc::new(Mutex::new(InvocationContext::default())),
            agent_buffer: Arc::new(Mutex::new(Vec::new())),
            pending_report: None,
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: notify.clone(),
                invoked_function_arn: String::new(),
        });

        let waiter = tokio::spawn(async move {
            notify.notified().await;
        });

        set_pending_report("req-1", "REPORT line".to_string());

        tokio::time::timeout(std::time::Duration::from_millis(500), waiter)
            .await
            .expect("report_notify must fire within timeout when set_pending_report is called")
            .expect("waiter task should not panic");

        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_get_report_notify_returns_none_for_unknown_request() {
        clear_request_state();
        assert!(get_report_notify("does-not-exist").is_none());
        clear_request_state();
    }

    #[test]
    #[serial]
    fn test_get_report_notify_returns_some_for_known_request() {
        clear_request_state();

        REQUEST_DATA.insert("req-1".to_string(), RequestData {
            context: Arc::new(Mutex::new(InvocationContext::default())),
            agent_buffer: Arc::new(Mutex::new(Vec::new())),
            pending_report: None,
            creation_invocation: 0,
            runtime_done_notify: Arc::new(tokio::sync::Notify::new()),
            agent_payload_notify: Arc::new(tokio::sync::Notify::new()),
            report_notify: Arc::new(tokio::sync::Notify::new()),
                invoked_function_arn: String::new(),
        });

        assert!(get_report_notify("req-1").is_some());

        clear_request_state();
    }






}
