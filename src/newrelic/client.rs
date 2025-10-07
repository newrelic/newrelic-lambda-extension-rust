use crate::{config::ExtensionConfig, newrelic::payload};
use reqwest::{header, Client, Error};
use serde::Serialize;
use tracing::{info, warn};

// Extension name and version from Cargo.toml
const EXTENSION_NAME: &str = env!("CARGO_PKG_NAME");
const EXTENSION_VERSION: &str = env!("CARGO_PKG_VERSION");

// Helper function to get extension name with version
fn get_extension_name_with_version() -> String {
    format!("{}:{}", EXTENSION_NAME, EXTENSION_VERSION)
}

#[derive(Debug)]
pub struct NewRelicClient {
    client: Client,
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
            .timeout(std::time::Duration::from_millis(2400)) // 2.4s timeout for New Relic requests
            .pool_idle_timeout(std::time::Duration::from_secs(10)) // Reset stale connections faster
            .pool_max_idle_per_host(2) // Limit connection reuse
            .build().unwrap();

        Self { client }
    }

    /// Creates a no-op New Relic client for disabled mode.
    pub fn new_noop() -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_millis(100)) // Very short timeout for no-op
            .build().unwrap();

        Self { client }
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

        // Validate license key
        if config.new_relic.license_key.is_none() {
            warn!("New Relic license key is not set, skipping log send");
            return Ok(());
        }

        info!("Sending {} log messages to NR", batch.len());

        let mut common_attributes = serde_json::Map::new();
        common_attributes.insert("plugin".to_string(), serde_json::json!(get_extension_name_with_version()));
        common_attributes.insert("faas.arn".to_string(), serde_json::json!(function_arn));
        common_attributes.insert("faas.name".to_string(), serde_json::json!(&config.aws.function_name));
        
        let log_data = vec![payload::LogPayload {
            common: payload::Common { attributes: common_attributes },
            logs: batch,
        }];
        self.send_payload(&config.new_relic.log_endpoint, &log_data).await
    }

    /// Sends a batch of platform events to New Relic.
    pub async fn send_platform_events(
        &self,
        config: &ExtensionConfig,
        payload: serde_json::Value,
    ) -> Result<(), Error> {
        // Validate license key
        if config.new_relic.license_key.is_none() {
            warn!("New Relic license key is not set, skipping platform events send");
            return Ok(());
        }

        info!("Sending platform events to NR");
        self.send_payload(&config.new_relic.telemetry_endpoint, &payload).await
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
        info!("[agentsend] Sending agent payload to NR: {} bytes", payload_size);
        
        let mut retries = 0;
        const MAX_RETRIES: usize = 3;

        loop {
            
            let res = self.client
                .post(&config.new_relic.telemetry_endpoint)
                .header("X-License-Key", config.new_relic.license_key.as_deref().unwrap_or_default())
                .header(header::CONTENT_TYPE, "application/json")
                .body(payload_json.to_string())
                .timeout(std::time::Duration::from_millis(2400)) // 2.4s per-request timeout
                .send()
                .await;

            match res {
                Ok(response) => {
                    let status = response.status();
                    
                    if status.is_success() {
                        let duration = start_time.elapsed();
                        info!("[agentsend] Successfully sent agent payload - size: {} bytes, duration: {:?}", payload_size, duration);
                        return Ok(());
                    } else {
                        let response_text = response.text().await.unwrap_or_else(|_| "Failed to read response".to_string());
                        if retries == 0 {
                            warn!("[agentsend] Failed to send agent payload. Status: {}, Response: {}", status, response_text);
                        }
                        
                        if status.is_client_error() {
                            warn!("[agentsend] Client error (4xx), not retrying");
                            return Ok(());
                        }
                        
                        if retries < MAX_RETRIES {
                            retries += 1;
                            let delay = std::time::Duration::from_millis(200 * (2_u64.pow(retries as u32 - 1)));
                            tokio::time::sleep(delay).await;
                        } else {
                            warn!("[agentsend] Max retries exceeded");
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    if retries == 0 {
                        // Classify common network errors for better debugging
                        let error_msg = e.to_string();
                        if error_msg.contains("BrokenPipe") || error_msg.contains("ConnectionReset") {
                            warn!("[agentsend] Connection issue (will retry): {}", e);
                        } else if e.is_timeout() {
                            warn!("[agentsend] Request timeout after 2.4s (will retry): {}", e);
                        } else {
                            warn!("[agentsend] Network error: {}", e);
                        }
                    }

                    if retries < MAX_RETRIES {
                        retries += 1;
                        let delay = std::time::Duration::from_millis(200 * (2_u64.pow(retries as u32 - 1)));
                        tokio::time::sleep(delay).await;
                    } else {
                        warn!("[agentsend] Max network retries exceeded");
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Sends a JSON payload to a specified endpoint.
    async fn send_payload<T: Serialize>(&self, endpoint: &str, payload: &T) -> Result<(), Error> {
        let start_time = std::time::Instant::now();
        let body = match serde_json::to_string(payload) {
            Ok(json) => json,
            Err(e) => {
                warn!("Failed to serialize payload to JSON: {}", e);
                return Ok(());
            }
        };

        let payload_size = body.len();
        info!("Sending payload to NR endpoint: {} bytes", payload_size);

        // Retry logic with exponential backoff
        let mut retries = 0;
        const MAX_RETRIES: usize = 3;
        
        loop {
            
            let res = self.client
                .post(endpoint)
                .header("Content-Type", "application/json")
                .body(body.clone())
                .timeout(std::time::Duration::from_millis(2400)) // 2.4s per-request timeout
                .send()
                .await;

            match res {
                Ok(response) => {
                    let status = response.status();
                    
                    if status.is_success() {
                        let duration = start_time.elapsed();
                        info!("Successfully sent payload to NR - size: {} bytes, duration: {:?}", payload_size, duration);
                        return Ok(());
                    } else {
                        let response_text = response.text().await.unwrap_or_else(|_| "Failed to read response".to_string());
                        if retries == 0 {
                            warn!("Failed to send data. Status: {}, Response: {}", status, response_text);
                        }
                        
                        // Don't retry on client errors (4xx)
                        if status.is_client_error() {
                            warn!("Client error (4xx), not retrying");
                            return Ok(());
                        }
                        
                        // Retry on server errors (5xx) or other issues
                        if retries < MAX_RETRIES {
                            retries += 1;
                            let delay = std::time::Duration::from_millis(200 * (2_u64.pow(retries as u32 - 1)));
                            tokio::time::sleep(delay).await;
                            continue;
                        } else {
                            warn!("Max retries exceeded");
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    if retries == 0 {
                        // Classify common network errors for better debugging
                        let error_msg = e.to_string();
                        if error_msg.contains("BrokenPipe") || error_msg.contains("ConnectionReset") {
                            warn!("Connection issue (will retry): {}", e);
                        } else if e.is_timeout() {
                            warn!("Request timeout after 2.4s (will retry): {}", e);
                        } else {
                            warn!("Network error: {}", e);
                        }
                    }

                    if retries < MAX_RETRIES {
                        retries += 1;
                        let delay = std::time::Duration::from_millis(200 * (2_u64.pow(retries as u32 - 1)));
                        tokio::time::sleep(delay).await;
                        continue;
                    } else {
                        warn!("Max network retries exceeded");
                        return Err(e);
                    }
                }
            }
        }
    }
}

