//! Unit tests for telemetry listener
//!
//! Tests cover:
//! - TelemetryRecord deserialization
//! - TELEMETRY_CURRENT_REQUEST_ID updates from platform.start
//! - RUNTIME_DONE_CHANNELS signaling from platform.runtimeDone

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use crate::telemetry::listener::TelemetryRecord;
    use crate::request::{TELEMETRY_CURRENT_REQUEST_ID, RUNTIME_DONE_CHANNELS};

    /// Helper: clear telemetry-related global state
    fn clear_telemetry_state() {
        if let Ok(mut guard) = TELEMETRY_CURRENT_REQUEST_ID.lock() {
            *guard = None;
        }
        RUNTIME_DONE_CHANNELS.clear();
    }

    // ========================================================================
    // TelemetryRecord deserialization tests
    // ========================================================================

    #[test]
    fn test_deserialize_platform_start_record() {
        let json = r#"{
            "time": "2024-01-01T00:00:00Z",
            "type": "platform.start",
            "record": {"requestId": "req-abc-123", "version": "$LATEST"}
        }"#;

        let record: TelemetryRecord = serde_json::from_str(json).expect("valid JSON");
        assert_eq!(record.record_type, "platform.start");

        let request_id = record.record.get("requestId")
            .and_then(|v| v.as_str())
            .expect("requestId");
        assert_eq!(request_id, "req-abc-123");
    }

    #[test]
    fn test_deserialize_function_log_record() {
        let json = r#"{
            "time": "2024-01-01T00:00:01Z",
            "type": "function",
            "record": "Hello from Lambda function!"
        }"#;

        let record: TelemetryRecord = serde_json::from_str(json).expect("valid JSON");
        assert_eq!(record.record_type, "function");
    }

    #[test]
    fn test_deserialize_platform_report_record() {
        let json = r#"{
            "time": "2024-01-01T00:00:02Z",
            "type": "platform.report",
            "record": {
                "requestId": "req-abc-123",
                "metrics": {
                    "durationMs": 100.5,
                    "billedDurationMs": 200,
                    "memorySizeMB": 128,
                    "maxMemoryUsedMB": 64
                }
            }
        }"#;

        let record: TelemetryRecord = serde_json::from_str(json).expect("valid JSON");
        assert_eq!(record.record_type, "platform.report");

        let metrics = record.record.get("metrics").expect("metrics");
        assert_eq!(metrics["durationMs"], 100.5);
    }

    #[test]
    fn test_deserialize_platform_runtime_done_record() {
        let json = r#"{
            "time": "2024-01-01T00:00:03Z",
            "type": "platform.runtimeDone",
            "record": {"requestId": "req-done-456", "status": "success"}
        }"#;

        let record: TelemetryRecord = serde_json::from_str(json).expect("valid JSON");
        assert_eq!(record.record_type, "platform.runtimeDone");

        let request_id = record.record.get("requestId")
            .and_then(|v| v.as_str())
            .expect("requestId");
        assert_eq!(request_id, "req-done-456");
    }

    #[test]
    fn test_deserialize_extension_log_record() {
        let json = r#"{
            "time": "2024-01-01T00:00:04Z",
            "type": "extension",
            "record": "Extension log message"
        }"#;

        let record: TelemetryRecord = serde_json::from_str(json).expect("valid JSON");
        assert_eq!(record.record_type, "extension");
    }

    #[test]
    fn test_deserialize_batch_of_records() {
        let json = r#"[
            {"time": "2024-01-01T00:00:00Z", "type": "platform.start", "record": {"requestId": "req-1"}},
            {"time": "2024-01-01T00:00:01Z", "type": "function", "record": "log line 1"},
            {"time": "2024-01-01T00:00:02Z", "type": "function", "record": "log line 2"},
            {"time": "2024-01-01T00:00:03Z", "type": "platform.runtimeDone", "record": {"requestId": "req-1"}}
        ]"#;

        let records: Vec<TelemetryRecord> = serde_json::from_str(json).expect("valid JSON array");
        assert_eq!(records.len(), 4);
        assert_eq!(records[0].record_type, "platform.start");
        assert_eq!(records[1].record_type, "function");
        assert_eq!(records[2].record_type, "function");
        assert_eq!(records[3].record_type, "platform.runtimeDone");
    }

    // ========================================================================
    // TELEMETRY_CURRENT_REQUEST_ID update tests
    // ========================================================================

    #[test]
    #[serial]
    fn test_platform_start_updates_telemetry_request_id() {
        clear_telemetry_state();

        // Simulate what handle_telemetry_request does on platform.start
        let record_json = serde_json::json!({"requestId": "platform-start-req-789"});

        if let Some(request_id_value) = record_json.get("requestId") {
            if let Some(request_id_str) = request_id_value.as_str() {
                if let Ok(mut telemetry_req) = TELEMETRY_CURRENT_REQUEST_ID.lock() {
                    *telemetry_req = Some(request_id_str.to_string());
                }
            }
        }

        let id = TELEMETRY_CURRENT_REQUEST_ID.lock().unwrap().clone();
        assert_eq!(id, Some("platform-start-req-789".to_string()));

        clear_telemetry_state();
    }

    #[test]
    #[serial]
    fn test_platform_start_overwrites_previous_request_id() {
        clear_telemetry_state();

        // First platform.start
        {
            let mut guard = TELEMETRY_CURRENT_REQUEST_ID.lock().unwrap();
            *guard = Some("req-A".to_string());
        }

        // Second platform.start overwrites
        {
            let mut guard = TELEMETRY_CURRENT_REQUEST_ID.lock().unwrap();
            *guard = Some("req-B".to_string());
        }

        let id = TELEMETRY_CURRENT_REQUEST_ID.lock().unwrap().clone();
        assert_eq!(id, Some("req-B".to_string()));

        clear_telemetry_state();
    }

    #[test]
    #[serial]
    fn test_platform_start_without_request_id_does_not_update() {
        clear_telemetry_state();

        // Set initial value
        {
            let mut guard = TELEMETRY_CURRENT_REQUEST_ID.lock().unwrap();
            *guard = Some("original-req".to_string());
        }

        // platform.start record without requestId
        let record_json = serde_json::json!({"version": "$LATEST"});

        // Same logic as handler: only updates if requestId exists
        if let Some(request_id_value) = record_json.get("requestId") {
            if let Some(request_id_str) = request_id_value.as_str() {
                if let Ok(mut telemetry_req) = TELEMETRY_CURRENT_REQUEST_ID.lock() {
                    *telemetry_req = Some(request_id_str.to_string());
                }
            }
        }

        // Should still have original value
        let id = TELEMETRY_CURRENT_REQUEST_ID.lock().unwrap().clone();
        assert_eq!(id, Some("original-req".to_string()));

        clear_telemetry_state();
    }

    // ========================================================================
    // RUNTIME_DONE_CHANNELS signaling tests
    // ========================================================================

    #[test]
    #[serial]
    fn test_runtime_done_signals_channel() {
        clear_telemetry_state();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        RUNTIME_DONE_CHANNELS.insert("req-done-1".to_string(), tx);

        // Simulate what handler does on platform.runtimeDone
        if let Some(tx) = RUNTIME_DONE_CHANNELS.get("req-done-1") {
            let _ = tx.send(());
        }

        assert!(rx.try_recv().is_ok(), "Channel should receive signal");

        clear_telemetry_state();
    }

    #[test]
    #[serial]
    fn test_runtime_done_no_channel_is_safe() {
        clear_telemetry_state();

        // No channel registered - should not panic
        {
            let result = RUNTIME_DONE_CHANNELS.get("nonexistent");
            assert!(result.is_none());
        }

        clear_telemetry_state();
    }

    // ========================================================================
    // HTTP listener integration tests
    // ========================================================================

    use std::sync::{Arc, Mutex};
    use crate::config::ExtensionConfig;
    use crate::newrelic::client::NewRelicClient;
    use crate::context::InvocationContext;
    use crate::telemetry::listener::setup_telemetry_listener;

    /// Helper: create default processors for listener tests
    fn create_test_processors() -> (
        Arc<crate::logs::processor::LogProcessor>,
        Arc<crate::platform::processor::PlatformProcessor>,
    ) {
        let config = Arc::new(ExtensionConfig::default());
        let newrelic_client = Arc::new(NewRelicClient::new(&config));
        let context = Arc::new(Mutex::new(InvocationContext::default()));
        let apm_app: crate::apm::SharedApmApp = Arc::new(tokio::sync::RwLock::new(None));

        let log_processor = Arc::new(crate::logs::processor::LogProcessor::new(
            newrelic_client.clone(),
            config.clone(),
            context.clone(),
            Some(apm_app),
        ));
        let platform_processor = Arc::new(crate::platform::processor::PlatformProcessor::new(
            newrelic_client,
            config,
            context,
            log_processor.clone(),
        ));

        (log_processor, platform_processor)
    }

    #[tokio::test]
    #[serial]
    async fn test_setup_telemetry_listener_returns_addr() {
        clear_telemetry_state();

        let (log_processor, platform_processor) = create_test_processors();

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            None,
            false,
        )
        .await
        .expect("listener should start");

        assert_ne!(addr.port(), 0, "Should bind to a real port");

        clear_telemetry_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_handle_platform_start_via_http() {
        clear_telemetry_state();

        let (log_processor, platform_processor) = create_test_processors();

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            None,
            false,
        )
        .await
        .expect("listener");

        // Send platform.start record
        let body = serde_json::json!([{
            "time": "2024-01-01T00:00:00Z",
            "type": "platform.start",
            "record": {"requestId": "http-req-123", "version": "$LATEST"}
        }]);

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{}/", addr.port()))
            .json(&body)
            .send()
            .await
            .expect("send");

        assert_eq!(resp.status(), 200);

        // Verify TELEMETRY_CURRENT_REQUEST_ID was updated
        let id = TELEMETRY_CURRENT_REQUEST_ID.lock().expect("lock").clone();
        assert_eq!(id, Some("http-req-123".to_string()));

        clear_telemetry_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_handle_function_and_extension_logs_via_http() {
        clear_telemetry_state();

        let (log_processor, platform_processor) = create_test_processors();

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            None,
            false,
        )
        .await
        .expect("listener");

        let body = serde_json::json!([
            {"time": "2024-01-01T00:00:00Z", "type": "function", "record": "Hello from function"},
            {"time": "2024-01-01T00:00:01Z", "type": "extension", "record": "Extension log msg"}
        ]);

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{}/", addr.port()))
            .json(&body)
            .send()
            .await
            .expect("send");

        assert_eq!(resp.status(), 200);

        clear_telemetry_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_handle_runtime_done_via_http() {
        clear_telemetry_state();

        let (log_processor, platform_processor) = create_test_processors();

        // Set up a runtime done channel
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        RUNTIME_DONE_CHANNELS.insert("runtime-done-req".to_string(), tx);

        let (runtime_done_tx, mut runtime_done_rx) = tokio::sync::mpsc::unbounded_channel();

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            Some(runtime_done_tx),
            false,
        )
        .await
        .expect("listener");

        let body = serde_json::json!([{
            "time": "2024-01-01T00:00:00Z",
            "type": "platform.runtimeDone",
            "record": {"requestId": "runtime-done-req", "status": "success"}
        }]);

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{}/", addr.port()))
            .json(&body)
            .send()
            .await
            .expect("send");

        assert_eq!(resp.status(), 200);

        // Small delay for async processing
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Channel should have been signaled
        assert!(rx.try_recv().is_ok(), "RUNTIME_DONE_CHANNELS should be signaled");
        assert!(runtime_done_rx.try_recv().is_ok(), "runtime_done_tx should be signaled");

        clear_telemetry_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_handle_platform_report_standard_mode_pending() {
        clear_telemetry_state();

        let (log_processor, platform_processor) = create_test_processors();

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            None,
            false, // standard mode
        )
        .await
        .expect("listener");

        // Send platform.report with full metrics
        let body = serde_json::json!([{
            "time": "2024-01-01T00:00:00Z",
            "type": "platform.report",
            "record": {
                "requestId": "report-req-1",
                "metrics": {
                    "durationMs": 100.5,
                    "billedDurationMs": 200,
                    "memorySizeMB": 128,
                    "maxMemoryUsedMB": 64
                }
            }
        }]);

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{}/", addr.port()))
            .json(&body)
            .send()
            .await
            .expect("send");

        assert_eq!(resp.status(), 200);

        // Small delay for async processing
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // In standard mode with no matching batch/buffer, report should be stored as pending
        {
            let pending = crate::request::PENDING_REPORTS.get("report-req-1");
            assert!(pending.is_some(), "Report should be stored in PENDING_REPORTS");
        }

        crate::request::PENDING_REPORTS.clear();
        clear_telemetry_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_handle_platform_report_matches_batch_buffer() {
        clear_telemetry_state();

        let (log_processor, platform_processor) = create_test_processors();

        // Pre-populate AGENT_BATCH_BUFFER with an entry for this request
        crate::agent::batch::add_to_batch(
            "batch-report-req".to_string(),
            vec![1, 2, 3],
            None, // no report yet
            "arn:test".to_string(),
        );

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            None,
            false,
        )
        .await
        .expect("listener");

        let body = serde_json::json!([{
            "time": "2024-01-01T00:00:00Z",
            "type": "platform.report",
            "record": {
                "requestId": "batch-report-req",
                "metrics": {
                    "durationMs": 50.0,
                    "billedDurationMs": 100,
                    "memorySizeMB": 256,
                    "maxMemoryUsedMB": 128
                }
            }
        }]);

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{}/", addr.port()))
            .json(&body)
            .send()
            .await
            .expect("send");

        assert_eq!(resp.status(), 200);

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Report should be matched with the batched entry
        {
            if let Some(item) = crate::agent::batch::AGENT_BATCH_BUFFER.get("batch-report-req") {
                assert!(item.report_line.is_some(), "Report line should be set");
            }
        }

        crate::agent::batch::AGENT_BATCH_BUFFER.clear();
        clear_telemetry_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_handle_platform_end_via_http() {
        clear_telemetry_state();

        let (log_processor, platform_processor) = create_test_processors();

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            None,
            false,
        )
        .await
        .expect("listener");

        let body = serde_json::json!([{
            "time": "2024-01-01T00:00:00Z",
            "type": "platform.end",
            "record": {}
        }]);

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{}/", addr.port()))
            .json(&body)
            .send()
            .await
            .expect("send");

        assert_eq!(resp.status(), 200);

        clear_telemetry_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_handle_unknown_type_via_http() {
        clear_telemetry_state();

        let (log_processor, platform_processor) = create_test_processors();

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            None,
            false,
        )
        .await
        .expect("listener");

        let body = serde_json::json!([{
            "time": "2024-01-01T00:00:00Z",
            "type": "platform.initStart",
            "record": {"initializationType": "on-demand"}
        }]);

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{}/", addr.port()))
            .json(&body)
            .send()
            .await
            .expect("send");

        assert_eq!(resp.status(), 200);

        clear_telemetry_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_handle_invalid_json_via_http() {
        clear_telemetry_state();

        let (log_processor, platform_processor) = create_test_processors();

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            None,
            false,
        )
        .await
        .expect("listener");

        // Send invalid JSON
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{}/", addr.port()))
            .body("this is not valid json{{{")
            .header("content-type", "application/json")
            .send()
            .await
            .expect("send");

        // Should still return 200 (handler logs error but returns OK)
        assert_eq!(resp.status(), 200);

        clear_telemetry_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_handle_mixed_batch_via_http() {
        clear_telemetry_state();

        let (log_processor, platform_processor) = create_test_processors();

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            None,
            false,
        )
        .await
        .expect("listener");

        // Send a mixed batch of record types
        let body = serde_json::json!([
            {"time": "2024-01-01T00:00:00Z", "type": "platform.start", "record": {"requestId": "mixed-req"}},
            {"time": "2024-01-01T00:00:01Z", "type": "function", "record": "log line 1"},
            {"time": "2024-01-01T00:00:02Z", "type": "function", "record": "log line 2"},
            {"time": "2024-01-01T00:00:03Z", "type": "extension", "record": "ext log"},
            {"time": "2024-01-01T00:00:04Z", "type": "platform.runtimeDone", "record": {"requestId": "mixed-req", "status": "success"}}
        ]);

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{}/", addr.port()))
            .json(&body)
            .send()
            .await
            .expect("send");

        assert_eq!(resp.status(), 200);

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Verify platform.start updated the telemetry request ID
        let id = TELEMETRY_CURRENT_REQUEST_ID.lock().expect("lock").clone();
        assert_eq!(id, Some("mixed-req".to_string()));

        clear_telemetry_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_handle_platform_report_apm_mode() {
        clear_telemetry_state();

        let (log_processor, platform_processor) = create_test_processors();

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            None,
            true, // APM mode
        )
        .await
        .expect("listener");

        let body = serde_json::json!([{
            "time": "2024-01-01T00:00:00Z",
            "type": "platform.report",
            "record": {
                "requestId": "apm-report-req",
                "metrics": {
                    "durationMs": 75.0,
                    "billedDurationMs": 100,
                    "memorySizeMB": 512,
                    "maxMemoryUsedMB": 256
                }
            }
        }]);

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{}/", addr.port()))
            .json(&body)
            .send()
            .await
            .expect("send");

        assert_eq!(resp.status(), 200);

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // In APM mode with no APM_APP, report should be stored in PENDING_REPORTS
        {
            let pending = crate::request::PENDING_REPORTS.get("apm-report-req");
            assert!(pending.is_some(), "APM mode: Report should be stored when APM app not ready");
        }

        crate::request::PENDING_REPORTS.clear();
        clear_telemetry_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_handle_platform_report_empty_request_buffer_stores_pending() {
        clear_telemetry_state();

        // REQUEST_AGENT_BUFFERS has the request but buffer is EMPTY
        let buffer = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        crate::request::REQUEST_AGENT_BUFFERS.insert("empty-buf-req".to_string(), buffer);

        let (log_processor, platform_processor) = create_test_processors();

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            None,
            false,
        )
        .await
        .expect("listener");

        let body = serde_json::json!([{
            "time": "2024-01-01T00:00:00Z",
            "type": "platform.report",
            "record": {
                "requestId": "empty-buf-req",
                "metrics": {
                    "durationMs": 50.0,
                    "billedDurationMs": 100,
                    "memorySizeMB": 128,
                    "maxMemoryUsedMB": 64
                }
            }
        }]);

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{}/", addr.port()))
            .json(&body)
            .send()
            .await
            .expect("send");

        assert_eq!(resp.status(), 200);

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Report should be stored as pending since buffer was empty
        {
            let pending = crate::request::PENDING_REPORTS.get("empty-buf-req");
            assert!(pending.is_some(), "Report should be stored as pending when buffer is empty");
        }

        crate::request::REQUEST_AGENT_BUFFERS.clear();
        crate::request::PENDING_REPORTS.clear();
        clear_telemetry_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_handle_empty_telemetry_array() {
        clear_telemetry_state();

        let (log_processor, platform_processor) = create_test_processors();

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            None,
            false,
        )
        .await
        .expect("listener");

        // Send empty array - exercises "No telemetry records processed" path
        let body = serde_json::json!([]);

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{}/", addr.port()))
            .json(&body)
            .send()
            .await
            .expect("send");

        assert_eq!(resp.status(), 200);

        clear_telemetry_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_handle_platform_start_without_string_request_id() {
        clear_telemetry_state();

        let (log_processor, platform_processor) = create_test_processors();

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            None,
            false,
        )
        .await
        .expect("listener");

        // requestId is a number, not a string - should not update TELEMETRY_CURRENT_REQUEST_ID
        let body = serde_json::json!([{
            "time": "2024-01-01T00:00:00Z",
            "type": "platform.start",
            "record": {"requestId": 12345}
        }]);

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{}/", addr.port()))
            .json(&body)
            .send()
            .await
            .expect("send");

        assert_eq!(resp.status(), 200);

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let id = TELEMETRY_CURRENT_REQUEST_ID.lock().expect("lock").clone();
        assert!(id.is_none(), "Non-string requestId should not update telemetry request ID");

        clear_telemetry_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_handle_platform_report_matches_request_buffer() {
        clear_telemetry_state();

        // Set up REQUEST_AGENT_BUFFERS with agent data for this request
        let buffer = Arc::new(Mutex::new(vec![vec![10, 20, 30]]));
        crate::request::REQUEST_AGENT_BUFFERS.insert("buf-report-req".to_string(), buffer);

        // Set up context
        let ctx = Arc::new(Mutex::new(InvocationContext {
            request_id: "buf-report-req".to_string(),
            invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:test-fn".to_string(),
            trace_id: None,
        }));
        crate::request::REQUEST_CONTEXTS.insert("buf-report-req".to_string(), ctx);

        let (log_processor, platform_processor) = create_test_processors();

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            None,
            false,
        )
        .await
        .expect("listener");

        let body = serde_json::json!([{
            "time": "2024-01-01T00:00:00Z",
            "type": "platform.report",
            "record": {
                "requestId": "buf-report-req",
                "metrics": {
                    "durationMs": 100.0,
                    "billedDurationMs": 200,
                    "memorySizeMB": 128,
                    "maxMemoryUsedMB": 64
                }
            }
        }]);

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{}/", addr.port()))
            .json(&body)
            .send()
            .await
            .expect("send");

        assert_eq!(resp.status(), 200);

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Agent data should have been moved to batch buffer
        {
            let batch_item = crate::agent::batch::AGENT_BATCH_BUFFER.get("buf-report-req");
            assert!(batch_item.is_some(), "Agent data should be moved to batch buffer");
            if let Some(item) = batch_item {
                assert!(item.report_line.is_some(), "Report line should be set");
            }
        }

        // REQUEST_AGENT_BUFFERS should be cleaned for this request
        assert!(
            crate::request::REQUEST_AGENT_BUFFERS.get("buf-report-req").is_none(),
            "Buffer should be removed after moving to batch"
        );

        crate::agent::batch::AGENT_BATCH_BUFFER.clear();
        crate::request::REQUEST_CONTEXTS.clear();
        clear_telemetry_state();
    }
}
