use crate::{config::ExtensionConfig, newrelic::payload, retry::get_backoff_delay, version::VersionInfo};
use reqwest::{header, Client, Error};
use serde::Serialize;
use tracing::{debug, warn};

const EXTENSION_NAME: &str = env!("CARGO_PKG_NAME");
const EXTENSION_VERSION: &str = env!("CARGO_PKG_VERSION");

fn get_extension_name_with_version() -> String {
    format!("{EXTENSION_NAME}:{EXTENSION_VERSION}")
}

#[derive(Debug)]
pub struct NewRelicClient {
    client: Client,
    cached_version_attrs: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>>,
}

impl NewRelicClient {
    /// Creates a new New Relic client with the provided configuration.
    pub fn new(config: &ExtensionConfig) -> Self {
        let license_key = config.new_relic.license_key.as_deref().unwrap_or_default();
        
        let mut headers = header::HeaderMap::new();
        headers.insert(
            "Api-Key",
            header::HeaderValue::from_str(license_key).unwrap(),
        );
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );
        headers.insert(
            "User-Agent",
            header::HeaderValue::from_str(&get_extension_name_with_version()).unwrap(),
        );

        let client = Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_millis(2400))  // 2.4s timeout - safe with separate Lambda Runtime client
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .pool_max_idle_per_host(10)
            .tcp_keepalive(std::time::Duration::from_secs(30))
            .build().unwrap();

        Self {
            client,
            cached_version_attrs: std::sync::OnceLock::new(),
        }
    }

    /// Creates a no-op New Relic client for disabled mode.
    pub fn new_noop() -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_millis(100))
            .build().unwrap();

        Self {
            client,
            cached_version_attrs: std::sync::OnceLock::new(),
        }
    }

    /// Sends a batch of logs to New Relic.
    pub async fn send_logs(
        &self,
        config: &ExtensionConfig,
        batch: Vec<payload::LogMessage>,
        function_arn: &str,
    ) -> Result<(), Error> {
        if batch.is_empty() {
            warn!("Attempted to send empty log batch");
            return Ok(());
        }

        if config.new_relic.license_key.is_none() {
            warn!("New Relic license key is not set, skipping log send");
            return Ok(());
        }

        debug!("Sending {} log messages to NR", batch.len());

        let mut common_attributes = serde_json::Map::new();
        common_attributes.insert("plugin".to_string(), serde_json::json!(get_extension_name_with_version()));
        common_attributes.insert("faas.arn".to_string(), serde_json::json!(function_arn));
        common_attributes.insert("faas.name".to_string(), serde_json::json!(&config.aws.function_name));

        if config.new_relic.add_version_detail_tags {
            let version_attrs = self.cached_version_attrs.get_or_init(|| {
                let version_info = VersionInfo::get_or_detect(config.new_relic.layer_version.clone());
                let version_tags = version_info.as_tags();
                let mut attrs = serde_json::Map::new();
                for (key, value) in version_tags {
                    attrs.insert(key, serde_json::json!(value));
                }
                attrs
            });
            common_attributes.extend(version_attrs.clone());
        }

        // Add NR_TAGS as common attributes
        for (key, value) in crate::config::parse_nr_tags() {
            debug!("Adding NR_TAGS to log payload: {}={}", key, value);
            common_attributes.insert(key, serde_json::json!(value));
        }

        let log_count = batch.len();
        let log_data = vec![payload::LogPayload {
            common: payload::Common { attributes: common_attributes },
            logs: batch,
        }];
        self.send_payload(&config.new_relic.log_endpoint, &log_data, Some(log_count)).await
    }

    /// Sends the wrapped agent payload to the New Relic collector.
    pub async fn send_agent_payload(
        &self,
        config: &ExtensionConfig,
        payload_json: &str,
    ) -> Result<(), Error> {
        if config.new_relic.license_key.is_none() {
            warn!("[agentsend] New Relic license key is not set, skipping agent payload send");
            return Ok(());
        }
        
        let start_time = std::time::Instant::now();
        let payload_size = payload_json.len();
        debug!("Sending agent payload to NR: {} bytes", payload_size);
        
        let mut retries = 0;
        // Allocate body once; reqwest takes ownership so we clone from this on retry
        let body = payload_json.to_string();

        loop {
            let res = self.client
                .post(&config.new_relic.telemetry_endpoint)
                .header("X-License-Key", config.new_relic.license_key.as_deref().unwrap_or_default())
                .header(header::CONTENT_TYPE, "application/json")
                .body(body.clone())
                .timeout(std::time::Duration::from_millis(2400))
                .send()
                .await;

            match res {
                Ok(response) => {
                    let status = response.status();

                    if status.is_success() {
                        let duration = start_time.elapsed();
                        debug!("Agent payload sent: {} bytes, duration: {:?}", payload_size, duration);
                        return Ok(());
                    }

                    // 4xx client errors: not retryable, log and return Ok
                    if status.is_client_error() {
                        let response_text = response.text().await.unwrap_or_else(|_| "Failed to read response".to_string());
                        if retries == 0 {
                            warn!("[agentsend] Client error {}. Response: {}", status, response_text);
                        }
                        warn!("[agentsend] Client error (4xx), not retrying");
                        return Ok(());
                    }

                    // 5xx server errors: retry with backoff, return Err if exhausted
                    if retries == 0 {
                        warn!("[agentsend] Server error ({}), will retry", status);
                    }
                    if retries < crate::retry::MAX_RETRIES {
                        retries += 1;
                        tokio::time::sleep(get_backoff_delay(retries)).await;
                    } else {
                        warn!("[agentsend] Max retries exceeded for server error {}", status);
                        return Err(response.error_for_status().expect_err("status was not success"));
                    }
                }
                Err(e) => {
                    if retries == 0 {
                        let error_msg = e.to_string();
                        if error_msg.contains("BrokenPipe") || error_msg.contains("ConnectionReset") {
                            warn!("[agentsend] Connection issue (will retry): {}", e);
                        } else if e.is_timeout() {
                            warn!("[agentsend] Request timeout after 2.4s (will retry): {}", e);
                        } else {
                            warn!("[agentsend] Network error: {}", e);
                        }
                    }

                    if retries < crate::retry::MAX_RETRIES {
                        retries += 1;
                        tokio::time::sleep(get_backoff_delay(retries)).await;
                    } else {
                        warn!("[agentsend] Max network retries exceeded");
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Sends a JSON payload to a specified endpoint
    async fn send_payload<T: Serialize>(&self, endpoint: &str, payload: &T, log_count: Option<usize>) -> Result<(), Error> {
        let start_time = std::time::Instant::now();
        let body = match serde_json::to_string(payload) {
            Ok(json) => json,
            Err(e) => {
                warn!("Failed to serialize payload to JSON: {}", e);
                return Ok(());
            }
        };

        let payload_size = body.len();
        debug!("Sending payload to NR endpoint: {} bytes", payload_size);

        let mut retries = 0;

        loop {
            let res = self.client
                .post(endpoint)
                .header("Content-Type", "application/json")
                .body(body.clone())
                .timeout(std::time::Duration::from_millis(2400))
                .send()
                .await;

            match res {
                Ok(response) => {
                    let status = response.status();

                    if status.is_success() {
                        let duration = start_time.elapsed();
                        if let Some(count) = log_count {
                            debug!("Logs sent: {} logs, {} bytes, duration: {:?}", count, payload_size, duration);
                        } else {
                            debug!("Payload sent: {} bytes, duration: {:?}", payload_size, duration);
                        }
                        return Ok(());
                    }

                    // 4xx client errors: not retryable
                    if status.is_client_error() {
                        let response_text = response.text().await.unwrap_or_else(|_| "Failed to read response".to_string());
                        if retries == 0 {
                            warn!("Client error {}. Response: {}", status, response_text);
                        }
                        warn!("Client error (4xx), not retrying");
                        return Ok(());
                    }

                    // 5xx server errors: retry with backoff
                    if retries == 0 {
                        warn!("Server error ({}), will retry", status);
                    }
                    if retries < crate::retry::MAX_RETRIES {
                        retries += 1;
                        tokio::time::sleep(get_backoff_delay(retries)).await;
                    } else {
                        warn!("Max retries exceeded for server error");
                        return Err(response.error_for_status().expect_err("status was not success"));
                    }
                }
                Err(e) => {
                    if retries == 0 {
                        let error_msg = e.to_string();
                        if error_msg.contains("BrokenPipe") || error_msg.contains("ConnectionReset") {
                            warn!("Connection issue (will retry): {}", e);
                        } else if e.is_timeout() {
                            warn!("Request timeout after 2.4s (will retry): {}", e);
                        } else {
                            warn!("Network error: {}", e);
                        }
                    }

                    if retries < crate::retry::MAX_RETRIES {
                        retries += 1;
                        tokio::time::sleep(get_backoff_delay(retries)).await;
                    } else {
                        warn!("Max network retries exceeded");
                        return Err(e);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ExtensionConfig;

    #[test]
    fn test_get_extension_name_with_version() {
        let name_version = get_extension_name_with_version();
        assert!(
            name_version.contains(':'),
            "Expected format 'name:version', got: {name_version}"
        );
        let parts: Vec<&str> = name_version.split(':').collect();
        assert_eq!(parts.len(), 2);
        assert!(!parts[0].is_empty(), "Extension name should not be empty");
        assert!(!parts[1].is_empty(), "Extension version should not be empty");
    }

    #[test]
    fn test_new_noop_client_construction() {
        let client = NewRelicClient::new_noop();
        // Should not panic — just verify it returns
        let _ = format!("{client:?}");
    }

    #[test]
    fn test_new_noop_client_debug() {
        let client = NewRelicClient::new_noop();
        let debug_str = format!("{client:?}");
        assert!(debug_str.contains("NewRelicClient"));
    }

    #[test]
    fn test_new_client_with_default_config() {
        let config = ExtensionConfig::default();
        let client = NewRelicClient::new(&config);
        let debug_str = format!("{client:?}");
        assert!(debug_str.contains("NewRelicClient"));
    }

    #[test]
    fn test_new_client_with_license_key() {
        let mut config = ExtensionConfig::default();
        config.new_relic.license_key = Some("test_license_key_1234567890".to_string());
        let client = NewRelicClient::new(&config);
        let _ = format!("{client:?}");
    }

    #[test]
    fn test_cached_version_attrs_initially_empty() {
        let client = NewRelicClient::new_noop();
        assert!(
            client.cached_version_attrs.get().is_none(),
            "cached_version_attrs should not be initialized yet"
        );
    }

    // ========================================================================
    // send_logs — early return paths
    // ========================================================================

    #[tokio::test]
    async fn test_send_logs_empty_batch_returns_ok() {
        let client = NewRelicClient::new_noop();
        let config = ExtensionConfig::default();
        let result = client.send_logs(&config, vec![], "arn:test").await;
        assert!(result.is_ok(), "Empty batch should return Ok immediately");
    }

    #[tokio::test]
    async fn test_send_logs_no_license_key_returns_ok() {
        let client = NewRelicClient::new_noop();
        let mut config = ExtensionConfig::default();
        config.new_relic.license_key = None;

        let batch = vec![payload::LogMessage {
            timestamp: 1000,
            message: "test".to_string(),
            attributes: serde_json::Map::new(),
        }];

        let result = client.send_logs(&config, batch, "arn:test").await;
        assert!(result.is_ok(), "No license key should return Ok (skip send)");
    }

    // ========================================================================
    // send_agent_payload — early return paths
    // ========================================================================

    #[tokio::test]
    async fn test_send_agent_payload_no_license_key_returns_ok() {
        let client = NewRelicClient::new_noop();
        let mut config = ExtensionConfig::default();
        config.new_relic.license_key = None;

        let result = client.send_agent_payload(&config, r#"{"test":"data"}"#).await;
        assert!(result.is_ok(), "No license key should return Ok (skip send)");
    }

    // ========================================================================
    // HTTP test server helper for retry logic tests
    // ========================================================================

    use std::sync::atomic::{AtomicU16, Ordering};
    use std::convert::Infallible;
    use hyper::{Response, StatusCode};
    use hyper::body::Bytes;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use http_body_util::Full;
    use tokio::net::TcpListener;

    /// Start a test HTTP server that returns a fixed status code
    async fn start_test_server(status_code: u16) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let url = format!("http://127.0.0.1:{}", addr.port());

        let handle = tokio::spawn(async move {
            // Accept connections until the test drops the handle
            loop {
                let Ok((stream, _)) = listener.accept().await else { break };
                let status = status_code;
                tokio::spawn(async move {
                    let service = service_fn(move |_req| {
                        let resp = Response::builder()
                            .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
                            .body(Full::new(Bytes::from("test response")))
                            .expect("response");
                        async move { Ok::<_, Infallible>(resp) }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });

        (url, handle)
    }

    /// Start a test server that returns different status codes based on request count
    async fn start_flaky_server(
        responses: Vec<u16>,
    ) -> (String, tokio::task::JoinHandle<()>, std::sync::Arc<AtomicU16>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let url = format!("http://127.0.0.1:{}", addr.port());
        let counter = std::sync::Arc::new(AtomicU16::new(0));
        let counter_clone = counter.clone();

        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { break };
                let responses = responses.clone();
                let counter = counter_clone.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |_req| {
                        let idx = counter.fetch_add(1, Ordering::SeqCst) as usize;
                        let status = if idx < responses.len() {
                            responses[idx]
                        } else {
                            200 // default to success after all responses consumed
                        };
                        let resp = Response::builder()
                            .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
                            .body(Full::new(Bytes::from("test response")))
                            .expect("response");
                        async move { Ok::<_, Infallible>(resp) }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });

        (url, handle, counter)
    }

    // ========================================================================
    // send_agent_payload — HTTP status code handling with real server
    // ========================================================================

    #[tokio::test]
    async fn test_send_agent_payload_200_success() {
        let (url, server_handle) = start_test_server(200).await;

        let client = NewRelicClient::new_noop();
        let mut config = ExtensionConfig::default();
        config.new_relic.license_key = Some("test-key".to_string());
        config.new_relic.telemetry_endpoint = url;

        let result = client.send_agent_payload(&config, r#"{"test":"data"}"#).await;
        assert!(result.is_ok(), "200 should return Ok");

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_send_agent_payload_400_client_error_no_retry() {
        let (url, server_handle, counter) = start_flaky_server(vec![400]).await;

        let client = NewRelicClient::new_noop();
        let mut config = ExtensionConfig::default();
        config.new_relic.license_key = Some("test-key".to_string());
        config.new_relic.telemetry_endpoint = url;

        let result = client.send_agent_payload(&config, r#"{"test":"data"}"#).await;
        // 4xx returns Ok (not retryable)
        assert!(result.is_ok(), "400 client error should return Ok (not retried)");
        // Should only make 1 request (no retries for 4xx)
        assert_eq!(counter.load(Ordering::SeqCst), 1, "Should not retry on 4xx");

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_send_agent_payload_403_forbidden_no_retry() {
        let (url, server_handle, counter) = start_flaky_server(vec![403]).await;

        let client = NewRelicClient::new_noop();
        let mut config = ExtensionConfig::default();
        config.new_relic.license_key = Some("test-key".to_string());
        config.new_relic.telemetry_endpoint = url;

        let result = client.send_agent_payload(&config, r#"{"test":"data"}"#).await;
        assert!(result.is_ok(), "403 should return Ok (client error, not retried)");
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_send_agent_payload_500_retries_then_fails() {
        // Return 500 for all requests (MAX_RETRIES + 1 = 4 attempts)
        let (url, server_handle, counter) =
            start_flaky_server(vec![500, 500, 500, 500]).await;

        let client = NewRelicClient::new_noop();
        let mut config = ExtensionConfig::default();
        config.new_relic.license_key = Some("test-key".to_string());
        config.new_relic.telemetry_endpoint = url;

        let result = client.send_agent_payload(&config, r#"{"test":"data"}"#).await;
        assert!(result.is_err(), "All 500s should eventually return Err");
        // Initial attempt + MAX_RETRIES retries = 4 total
        let total_requests = counter.load(Ordering::SeqCst);
        assert!(
            total_requests >= 2,
            "Should have retried at least once, got {total_requests} requests"
        );

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_send_agent_payload_500_then_200_recovers() {
        // First request returns 500, second returns 200
        let (url, server_handle, counter) = start_flaky_server(vec![500, 200]).await;

        let client = NewRelicClient::new_noop();
        let mut config = ExtensionConfig::default();
        config.new_relic.license_key = Some("test-key".to_string());
        config.new_relic.telemetry_endpoint = url;

        let result = client.send_agent_payload(&config, r#"{"test":"data"}"#).await;
        assert!(result.is_ok(), "Should recover after 500 → 200");
        assert_eq!(counter.load(Ordering::SeqCst), 2, "Should have made 2 requests");

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_send_agent_payload_503_then_503_then_200() {
        // Two 503s followed by success
        let (url, server_handle, counter) = start_flaky_server(vec![503, 503, 200]).await;

        let client = NewRelicClient::new_noop();
        let mut config = ExtensionConfig::default();
        config.new_relic.license_key = Some("test-key".to_string());
        config.new_relic.telemetry_endpoint = url;

        let result = client.send_agent_payload(&config, r#"{"test":"data"}"#).await;
        assert!(result.is_ok(), "Should recover after 503 → 503 → 200");
        assert_eq!(counter.load(Ordering::SeqCst), 3, "Should have made 3 requests");

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_send_agent_payload_connection_refused_retries() {
        // Point at a port nothing is listening on
        let client = NewRelicClient::new_noop();
        let mut config = ExtensionConfig::default();
        config.new_relic.license_key = Some("test-key".to_string());
        config.new_relic.telemetry_endpoint = "http://127.0.0.1:1".to_string(); // Port 1 is never open

        let result = client.send_agent_payload(&config, r#"{"test":"data"}"#).await;
        assert!(result.is_err(), "Connection refused should eventually return Err");
    }

    // ========================================================================
    // send_logs — HTTP tests
    // ========================================================================

    #[tokio::test]
    async fn test_send_logs_200_success() {
        let (url, server_handle) = start_test_server(200).await;

        let client = NewRelicClient::new_noop();
        let mut config = ExtensionConfig::default();
        config.new_relic.license_key = Some("test-key".to_string());
        config.new_relic.log_endpoint = url;

        let batch = vec![payload::LogMessage {
            timestamp: 1000,
            message: "test log".to_string(),
            attributes: serde_json::Map::new(),
        }];

        let result = client.send_logs(&config, batch, "arn:test").await;
        assert!(result.is_ok(), "200 should return Ok for logs");

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_send_logs_400_client_error_no_retry() {
        let (url, server_handle, counter) = start_flaky_server(vec![400]).await;

        let client = NewRelicClient::new_noop();
        let mut config = ExtensionConfig::default();
        config.new_relic.license_key = Some("test-key".to_string());
        config.new_relic.log_endpoint = url;

        let batch = vec![payload::LogMessage {
            timestamp: 1000,
            message: "test".to_string(),
            attributes: serde_json::Map::new(),
        }];

        let result = client.send_logs(&config, batch, "arn:test").await;
        assert!(result.is_ok(), "400 should return Ok for logs (not retried)");
        assert_eq!(counter.load(Ordering::SeqCst), 1, "No retry on 4xx");

        server_handle.abort();
    }
}
