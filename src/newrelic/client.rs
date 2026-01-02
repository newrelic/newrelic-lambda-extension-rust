use crate::{config::ExtensionConfig, newrelic::payload, version::VersionInfo};
use reqwest::{header, Client, Error};
use serde::Serialize;
use tracing::{debug, info, warn};

const EXTENSION_NAME: &str = env!("CARGO_PKG_NAME");
const EXTENSION_VERSION: &str = env!("CARGO_PKG_VERSION");

fn get_extension_name_with_version() -> String {
    format!("{}:{}", EXTENSION_NAME, EXTENSION_VERSION)
}

fn get_backoff_delay(retry_attempt: usize) -> std::time::Duration {
    match retry_attempt {
        1 => std::time::Duration::from_millis(200),
        2 => std::time::Duration::from_millis(400),
        _ => std::time::Duration::from_millis(900),
    }
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
                let version_info = VersionInfo::get_or_detect();
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
        const MAX_RETRIES: usize = 3;

        loop {
            
            let res = self.client
                .post(&config.new_relic.telemetry_endpoint)
                .header("X-License-Key", config.new_relic.license_key.as_deref().unwrap_or_default())
                .header(header::CONTENT_TYPE, "application/json")
                .body(payload_json.to_string())
                .timeout(std::time::Duration::from_millis(2400))  // 2.4s timeout
                .send()
                .await;

            match res {
                Ok(response) => {
                    let status = response.status();
                    
                    if status.is_success() {
                        let duration = start_time.elapsed();
                        info!("Agent payload sent: {} bytes, duration: {:?}", payload_size, duration);
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
                            let delay = get_backoff_delay(retries);
                            tokio::time::sleep(delay).await;
                        } else {
                            warn!("[agentsend] Max retries exceeded");
                            return Ok(());
                        }
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

                    if retries < MAX_RETRIES {
                        retries += 1;
                        let delay = get_backoff_delay(retries);
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
        const MAX_RETRIES: usize = 3;
        
        loop {
            
            let res = self.client
                .post(endpoint)
                .header("Content-Type", "application/json")
                .body(body.clone())
                .timeout(std::time::Duration::from_millis(2400))  // 2.4s timeout
                .send()
                .await;

            match res {
                Ok(response) => {
                    let status = response.status();
                    
                    if status.is_success() {
                        let duration = start_time.elapsed();
                        if let Some(count) = log_count {
                            info!("Logs sent: {} logs, {} bytes, duration: {:?}", count, payload_size, duration);
                        } else {
                            info!("Payload sent: {} bytes, duration: {:?}", payload_size, duration);
                        }
                        return Ok(());
                    } else {
                        let response_text = response.text().await.unwrap_or_else(|_| "Failed to read response".to_string());
                        if retries == 0 {
                            warn!("Failed to send data. Status: {}, Response: {}", status, response_text);
                        }
                        
                        if status.is_client_error() {
                            warn!("Client error (4xx), not retrying");
                            return Ok(());
                        }
                        
                        if retries < MAX_RETRIES {
                            retries += 1;
                            let delay = get_backoff_delay(retries);
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
                        let delay = get_backoff_delay(retries);
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

