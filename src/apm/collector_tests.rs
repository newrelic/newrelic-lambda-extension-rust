#[cfg(test)]
mod tests {
    use crate::apm::collector::{
        resolve_collector_command, CollectorError,
        CMD_METRICS, CMD_SPAN_EVENTS, CMD_ERROR_EVENTS, CMD_ERROR_DATA,
        CMD_ANALYTIC_EVENTS, CMD_CUSTOM_EVENTS, CMD_LOG_EVENTS, CMD_TRANSACTION_SAMPLES,
    };

    // ========================================================================
    // resolve_collector_command
    // ========================================================================

    #[test]
    fn test_resolve_all_known_commands() {
        let known = [
            CMD_METRICS,
            CMD_SPAN_EVENTS,
            CMD_ERROR_DATA,
            CMD_ANALYTIC_EVENTS,
            CMD_CUSTOM_EVENTS,
            CMD_LOG_EVENTS,
            CMD_TRANSACTION_SAMPLES,
        ];

        for cmd in known {
            let result = resolve_collector_command(cmd);
            assert_eq!(result, Some(cmd), "Expected Some for known command: {}", cmd);
        }
    }

    #[test]
    fn test_resolve_error_events_returns_none() {
        // error_event_data is handled separately via send_error_events
        assert_eq!(resolve_collector_command(CMD_ERROR_EVENTS), None);
    }

    #[test]
    fn test_resolve_unknown_type_returns_none() {
        assert_eq!(resolve_collector_command("completely_unknown"), None);
        assert_eq!(resolve_collector_command(""), None);
        assert_eq!(resolve_collector_command("metric_data_v2"), None);
    }

    // ========================================================================
    // CollectorError Display
    // ========================================================================

    #[test]
    fn test_collector_error_disconnect_display() {
        let err = CollectorError::Disconnect;
        assert_eq!(format!("{}", err), "Collector disconnected (410)");
    }

    #[test]
    fn test_collector_error_restart_display() {
        let err = CollectorError::RestartException;
        assert_eq!(format!("{}", err), "Collector restart exception (401/409)");
    }

    #[test]
    fn test_collector_error_equality() {
        assert_eq!(CollectorError::Disconnect, CollectorError::Disconnect);
        assert_eq!(CollectorError::RestartException, CollectorError::RestartException);
        assert_ne!(CollectorError::Disconnect, CollectorError::RestartException);
    }

    // ========================================================================
    // get_user_agent + constants
    // ========================================================================

    use crate::apm::collector::get_user_agent;

    #[test]
    fn test_get_user_agent_format() {
        let ua = get_user_agent();
        assert!(ua.starts_with("NewRelic-Rust-Lambda-Extension/"));
        assert!(ua.contains(env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn test_cmd_constants_have_expected_values() {
        assert_eq!(CMD_METRICS, "metric_data");
        assert_eq!(CMD_SPAN_EVENTS, "span_event_data");
        assert_eq!(CMD_ERROR_EVENTS, "error_event_data");
        assert_eq!(CMD_ERROR_DATA, "error_data");
        assert_eq!(CMD_ANALYTIC_EVENTS, "analytic_event_data");
        assert_eq!(CMD_CUSTOM_EVENTS, "custom_event_data");
        assert_eq!(CMD_TRANSACTION_SAMPLES, "transaction_sample_data");
        assert_eq!(CMD_LOG_EVENTS, "log_event_data");
    }

    #[test]
    fn test_collector_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CollectorError>();
    }

    // ========================================================================
    // send_error_events — early return + mock HTTP
    // ========================================================================

    use crate::apm::collector::{send_error_events, send_apm_telemetry, send_platform_metrics};

    #[tokio::test]
    async fn test_send_error_events_empty_returns_ok() {
        let client = reqwest::Client::new();
        let result = send_error_events(&client, "key", "host", "run-id", &[]).await;
        assert!(result.is_ok(), "Empty events should return Ok immediately");
    }

    #[tokio::test]
    async fn test_send_error_events_connection_refused() {
        let client = reqwest::Client::new();
        let events = vec![serde_json::json!({"error": "test"})];

        // Use unreachable host — should fail
        let result = send_error_events(&client, "key", "127.0.0.1:1", "run-1", &events).await;
        assert!(result.is_err(), "Unreachable host should return error");
    }

    #[tokio::test]
    async fn test_send_apm_telemetry_empty_data() {
        let client = reqwest::Client::new();
        // Empty data — still goes through (sets run_id at index 0)
        let result = send_apm_telemetry(&client, "key", "127.0.0.1:1", "run-1", "metric_data", &[]).await;
        // Will fail with connection error but should not panic
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_send_apm_telemetry_connection_refused() {
        let client = reqwest::Client::new();
        let data = vec![serde_json::json!("placeholder"), serde_json::json!([1, 2, 3])];

        let result = send_apm_telemetry(
            &client, "key", "127.0.0.1:1", "run-1", "span_event_data", &data,
        ).await;
        assert!(result.is_err(), "Unreachable host should return error");
    }

    // ========================================================================
    // send_platform_metrics — via mock HTTP server (takes full URL)
    // ========================================================================

    use std::convert::Infallible;
    use hyper::{Response, StatusCode};
    use hyper::body::Bytes;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use http_body_util::Full;
    use tokio::net::TcpListener;

    async fn start_mock_metric_api(status: u16) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://127.0.0.1:{}/metric/v1", listener.local_addr().expect("addr").port());
        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { break };
                let s = status;
                tokio::spawn(async move {
                    let svc = service_fn(move |_| {
                        let resp = Response::builder()
                            .status(StatusCode::from_u16(s).unwrap_or(StatusCode::OK))
                            .body(Full::new(Bytes::from("{}"))).expect("r");
                        async move { Ok::<_, Infallible>(resp) }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), svc).await;
                });
            }
        });
        (url, handle)
    }

    #[tokio::test]
    async fn test_send_platform_metrics_empty_returns_ok() {
        let client = reqwest::Client::new();
        let result = send_platform_metrics(&client, "key", "http://localhost", vec![]).await;
        assert!(result.is_ok(), "Empty metrics should return Ok");
    }

    #[tokio::test]
    async fn test_send_platform_metrics_200_success() {
        let (url, handle) = start_mock_metric_api(200).await;
        let client = reqwest::Client::new();

        let metrics = vec![serde_json::json!({
            "name": "test.metric",
            "type": "gauge",
            "value": 42.0,
            "timestamp": 1700000000000_i64
        })];

        let result = send_platform_metrics(&client, "test-key", &url, metrics).await;
        assert!(result.is_ok(), "200 should return Ok");
        handle.abort();
    }

    #[tokio::test]
    async fn test_send_platform_metrics_400_returns_error() {
        let (url, handle) = start_mock_metric_api(400).await;
        let client = reqwest::Client::new();

        let metrics = vec![serde_json::json!({"name": "test", "value": 1})];
        let result = send_platform_metrics(&client, "test-key", &url, metrics).await;
        assert!(result.is_err(), "400 should return Err");
        handle.abort();
    }

    #[tokio::test]
    async fn test_send_platform_metrics_500_returns_error() {
        let (url, handle) = start_mock_metric_api(500).await;
        let client = reqwest::Client::new();

        let metrics = vec![serde_json::json!({"name": "test", "value": 1})];
        let result = send_platform_metrics(&client, "test-key", &url, metrics).await;
        assert!(result.is_err(), "500 should return Err");
        handle.abort();
    }

    #[tokio::test]
    async fn test_send_platform_metrics_multiple_metrics() {
        let (url, handle) = start_mock_metric_api(200).await;
        let client = reqwest::Client::new();

        let metrics = vec![
            serde_json::json!({"name": "metric1", "value": 1.0}),
            serde_json::json!({"name": "metric2", "value": 2.0}),
            serde_json::json!({"name": "metric3", "value": 3.0}),
        ];

        let result = send_platform_metrics(&client, "key", &url, metrics).await;
        assert!(result.is_ok());
        handle.abort();
    }
}
