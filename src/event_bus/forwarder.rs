//! Forwarder Module
//! 
//! This module contains forwarders for sending events to various New Relic endpoints.

use std::sync::Arc;
use hyper::{Request, Method, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use serde_json::Value;
use hyper_rustls::HttpsConnector;

use crate::config::ExtensionConfig;

/// New Relic forwarder for sending events to New Relic APIs
pub struct NewRelicForwarder {
    client: Client<HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>, Full<Bytes>>,
    config: Arc<ExtensionConfig>,
}impl NewRelicForwarder {
    /// Create a new New Relic forwarder
    pub fn new(config: Arc<ExtensionConfig>) -> Self {
        // Create HTTPS connector using rustls (pure Rust TLS implementation)
        // Use the default configuration which includes webpki root certificates
        let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();

        // Create HTTP client with HTTPS support
        let client = Client::builder(TokioExecutor::new())
            .build(https_connector);

        Self { client, config }
    }

    /// Send telemetry data to New Relic telemetry endpoint
    pub async fn send_telemetry(&self, telemetry_data: Value) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let endpoint = &self.config.new_relic.telemetry_endpoint;
        tracing::info!("📡 [NewRelicForwarder] Sending telemetry to: {}", endpoint);
        self.send_to_newrelic(endpoint, telemetry_data, "telemetry").await
    }

    /// Send log data to New Relic log endpoint  
    pub async fn send_log(&self, log_data: Value) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let endpoint = &self.config.new_relic.log_endpoint;
        tracing::info!("📝 [NewRelicForwarder] Sending log to: {}", endpoint);
        self.send_to_newrelic(endpoint, log_data, "log").await
    }

    /// Send metric data to New Relic metric endpoint
    pub async fn send_metric(&self, metric_data: Value) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let endpoint = &self.config.new_relic.metric_endpoint;
        tracing::info!("📊 [NewRelicForwarder] Sending metric to: {}", endpoint);
        self.send_to_newrelic(endpoint, metric_data, "metric").await
    }

    /// Generic method to send data to New Relic endpoints
    async fn send_to_newrelic(
        &self,
        endpoint: &str,
        data: Value,
        data_type: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Skip if extension is disabled
        if !self.config.new_relic.extension_enabled {
            tracing::debug!("🚫 [NewRelicForwarder] Extension disabled, skipping {} send", data_type);
            return Ok(());
        }

        // Skip if no license key
        let license_key = match &self.config.new_relic.license_key {
            Some(key) => key,
            None => {
                tracing::warn!("⚠️ [NewRelicForwarder] No license key configured, skipping {} send", data_type);
                return Ok(());
            }
        };

        tracing::debug!("📤 [NewRelicForwarder] Sending {} to endpoint: {}", data_type, endpoint);

        // Retry logic for network failures
        let max_retries = 3;
        let mut last_error = None;

        for attempt in 1..=max_retries {
            match self.try_send_to_newrelic(endpoint, &data, data_type, license_key).await {
                Ok(()) => {
                    if attempt > 1 {
                        tracing::info!("✅ [NewRelicForwarder] Successfully sent {} to New Relic on attempt {}", data_type, attempt);
                    } else {
                        tracing::debug!("✅ [NewRelicForwarder] Successfully sent {} to New Relic", data_type);
                    }
                    return Ok(());
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < max_retries {
                        let delay = std::time::Duration::from_millis(100 * attempt as u64); // Exponential backoff
                        tracing::warn!("⚠️ [NewRelicForwarder] Attempt {} failed for {}, retrying in {:?}: {}", 
                                     attempt, data_type, delay, last_error.as_ref().unwrap());
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        // All retries failed
        let final_error = last_error.unwrap();
        tracing::error!("❌ [NewRelicForwarder] Failed to send {} to New Relic after {} attempts: {}", 
                       data_type, max_retries, final_error);
        Err(final_error)
    }

    /// Single attempt to send data to New Relic
    async fn try_send_to_newrelic(
        &self,
        endpoint: &str,
        data: &Value,
        data_type: &str,
        license_key: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Prepare the request body
        let body_json = serde_json::to_string(data)?;
        let body = Full::new(Bytes::from(body_json));

        // Parse the endpoint URI
        let uri: Uri = endpoint.parse()?;

        // Build the request
        let mut request_builder = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("Content-Type", "application/json")
            .header("X-License-Key", license_key)
            .header("User-Agent", format!("newrelic-lambda-extension/{}", env!("CARGO_PKG_VERSION")))
            .header("Connection", "close"); // Ensure connections are properly closed

        // Add additional headers from config
        for (header_name, header_value) in self.config.new_relic_headers() {
            if header_name != "X-License-Key" { // Avoid duplicate license key header
                request_builder = request_builder.header(header_name, header_value);
            }
        }

        let request = request_builder.body(body)?;

        // Send the request with timeout
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(30), // 30 second timeout
            self.client.request(request)
        ).await
        .map_err(|_| "Request timeout after 30 seconds")?
        .map_err(|e| format!("Network error: {}", e))?;

        let status = response.status();
        if status.is_success() {
            tracing::debug!("✅ [NewRelicForwarder] Successfully sent {} to New Relic (status: {})", data_type, status);
            Ok(())
        } else {
            // Read response body for error details
            let body_bytes = response.into_body().collect().await?.to_bytes();
            let error_body = String::from_utf8_lossy(&body_bytes);
            
            let error_msg = format!("New Relic API request failed with status: {}, body: {}", status, error_body);
            tracing::error!("❌ [NewRelicForwarder] Failed to send {} to New Relic: {}", data_type, error_msg);
            
            Err(error_msg.into())
        }
    }

    /// Send batch of telemetry data
    pub async fn send_telemetry_batch(&self, batch: Vec<Value>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if batch.is_empty() {
            return Ok(());
        }

        let batch_data = serde_json::json!({
            "events": batch,
            "metadata": {
                "functionName": self.config.aws.function_name,
                "functionVersion": self.config.aws.function_version,
                "extensionVersion": env!("CARGO_PKG_VERSION"),
                "timestamp": chrono::Utc::now().timestamp_millis()
            }
        });

        tracing::info!("📦 [NewRelicForwarder] Sending batch of {} telemetry events", batch.len());
        self.send_telemetry(batch_data).await
    }

    /// Send batch of log data
    pub async fn send_log_batch(&self, batch: Vec<Value>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if batch.is_empty() {
            return Ok(());
        }

        let batch_data = serde_json::json!({
            "logs": batch,
            "metadata": {
                "functionName": self.config.aws.function_name,
                "functionVersion": self.config.aws.function_version,
                "extensionVersion": env!("CARGO_PKG_VERSION"),
                "timestamp": chrono::Utc::now().timestamp_millis()
            }
        });

        tracing::info!("📦 [NewRelicForwarder] Sending batch of {} log events", batch.len());
        self.send_log(batch_data).await
    }

    /// Send batch of metric data
    pub async fn send_metric_batch(&self, batch: Vec<Value>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if batch.is_empty() {
            return Ok(());
        }

        let batch_data = serde_json::json!({
            "metrics": batch,
            "metadata": {
                "functionName": self.config.aws.function_name,
                "functionVersion": self.config.aws.function_version,
                "extensionVersion": env!("CARGO_PKG_VERSION"),
                "timestamp": chrono::Utc::now().timestamp_millis()
            }
        });

        tracing::info!("📦 [NewRelicForwarder] Sending batch of {} metric events", batch.len());
        self.send_metric(batch_data).await
    }

    /// Health check the New Relic endpoints
    pub async fn health_check(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !self.config.new_relic.extension_enabled {
            return Ok(());
        }

        let endpoints = [
            ("telemetry", &self.config.new_relic.telemetry_endpoint),
            ("log", &self.config.new_relic.log_endpoint),
            ("metric", &self.config.new_relic.metric_endpoint),
        ];

        for (endpoint_type, endpoint_url) in &endpoints {
            // Send a minimal test payload
            let test_data = serde_json::json!({
                "healthCheck": true,
                "timestamp": chrono::Utc::now().timestamp_millis(),
                "extensionVersion": env!("CARGO_PKG_VERSION")
            });

            match self.send_to_newrelic(endpoint_url, test_data, endpoint_type).await {
                Ok(()) => {
                    tracing::info!("✅ [NewRelicForwarder] Health check passed for {} endpoint", endpoint_type);
                }
                Err(e) => {
                    tracing::warn!("⚠️ [NewRelicForwarder] Health check failed for {} endpoint: {}", endpoint_type, e);
                    // Don't fail the entire health check for individual endpoint failures
                }
            }
        }

        Ok(())
    }
}
