use crate::{config, config::ExtensionConfig, newrelic::payload};
use reqwest::{header, Client, Error};
use serde::Serialize;
use tracing::{info, warn};

#[derive(Debug)]
pub struct NewRelicClient {
    client: Client,
}

impl NewRelicClient {
    /// Creates a new New Relic client.
    pub fn new() -> Self {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            "Api-Key",
            header::HeaderValue::from_str(
                config::get_config()
                    .new_relic
                    .license_key
                    .as_deref()
                    .unwrap_or_default(),
            )
            .unwrap(),
        );
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );

        let client = Client::builder().default_headers(headers).build().unwrap();

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

        info!("Sending {} log entries to New Relic", batch.len());
        
        // Validate license key
        if config.new_relic.license_key.is_none() {
            warn!("New Relic license key is not set, skipping log send");
            return Ok(());
        }

        let mut common_attributes = serde_json::Map::new();
        common_attributes.insert("plugin".to_string(), serde_json::json!({ "type": "newrelic-lambda-extension" }));
        common_attributes.insert("faas.arn".to_string(), serde_json::json!(function_arn));
        common_attributes.insert("faas.name".to_string(), serde_json::json!(&config.aws.function_name));
        
        let log_data = vec![payload::LogPayload {
            common: payload::Common { attributes: common_attributes },
            logs: batch,
        }];

        info!("Log payload created, sending to endpoint: {}", &config.new_relic.log_endpoint);
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

        info!("Sending platform events to New Relic");
        info!("Platform payload created, sending to endpoint: {}", &config.new_relic.telemetry_endpoint);
        self.send_payload(&config.new_relic.telemetry_endpoint, &payload).await
    }

    /// Sends a JSON payload to a specified endpoint.
    async fn send_payload<T: Serialize>(&self, endpoint: &str, payload: &T) -> Result<(), Error> {
        let body = match serde_json::to_string(payload) {
            Ok(json) => json,
            Err(e) => {
                warn!("Failed to serialize payload to JSON: {}", e);
                return Ok(());
            }
        };

        info!("🚀 Sending payload to endpoint: {}", endpoint);
        info!("📦 Payload size: {} bytes", body.len());
        
        // Log first 500 chars of payload for debugging (be careful with sensitive data)
        if body.len() > 500 {
            info!("📄 Payload preview: {}...", &body[..500]);
        } else {
            info!("📄 Full payload: {}", body);
        }

        // Retry logic with exponential backoff
        let mut retries = 0;
        const MAX_RETRIES: usize = 3;
        
        loop {
            info!("🔄 Attempt {} of {} to send data to New Relic", retries + 1, MAX_RETRIES + 1);
            
            let res = self.client
                .post(endpoint)
                .header("Content-Type", "application/json")
                .body(body.clone())
                .send()
                .await;

            match res {
                Ok(response) => {
                    let status = response.status();
                    info!("📡 Received response with status: {}", status);
                    
                    if status.is_success() {
                        info!("✅ Successfully sent data to New Relic! Status: {}", status);
                        return Ok(());
                    } else {
                        let response_text = response.text().await.unwrap_or_else(|_| "Failed to read response".to_string());
                        warn!("❌ Failed to send data to New Relic. Status: {}, Response: {}", status, response_text);
                        
                        // Don't retry on client errors (4xx)
                        if status.is_client_error() {
                            warn!("🚫 Client error (4xx), not retrying");
                            return Ok(());
                        }
                        
                        // Retry on server errors (5xx) or other issues
                        if retries < MAX_RETRIES {
                            retries += 1;
                            let delay = std::time::Duration::from_millis(1000 * retries as u64);
                            warn!("⏳ Retrying in {}ms...", delay.as_millis());
                            tokio::time::sleep(delay).await;
                            continue;
                        } else {
                            warn!("🔥 Max retries exceeded, giving up");
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    warn!("🌐 Network error sending data to New Relic: {}", e);
                    
                    if retries < MAX_RETRIES {
                        retries += 1;
                        let delay = std::time::Duration::from_millis(1000 * retries as u64);
                        warn!("⏳ Network error, retrying in {}ms...", delay.as_millis());
                        tokio::time::sleep(delay).await;
                        continue;
                    } else {
                        warn!("🔥 Max network retries exceeded, giving up");
                        return Err(e);
                    }
                }
            }
        }
    }
}

