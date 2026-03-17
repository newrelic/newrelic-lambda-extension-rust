use crate::{config::ExtensionConfig, newrelic::payload, version::VersionInfo};
use anyhow::anyhow;
use reqwest::{header, Client, NoProxy, Proxy};
use serde::Serialize;
use tracing::{debug, info, warn};

/// Error type for New Relic client operations.
/// Wraps both reqwest transport errors and HTTP status errors.
pub type SendError = anyhow::Error;

const EXTENSION_NAME_WITH_VERSION: &str = concat!(env!("CARGO_PKG_NAME"), ":", env!("CARGO_PKG_VERSION"));

fn get_backoff_delay(retry_attempt: usize) -> std::time::Duration {
    match retry_attempt {
        1 => std::time::Duration::from_millis(200),
        2 => std::time::Duration::from_millis(400),
        _ => std::time::Duration::from_millis(900),
    }
}

/// Mask credentials in a proxy URL for safe logging.
/// `http://user:pass@proxy:8080` -> `http://***:***@proxy:8080`
pub fn mask_proxy_url(url: &str) -> String {
    // Try to find the `@` that separates credentials from host
    // Pattern: scheme://user:pass@host...
    if let Some(at_pos) = url.find('@') {
        if let Some(scheme_end) = url.find("://") {
            let prefix = &url[..scheme_end + 3]; // "http://" or "https://"
            let suffix = &url[at_pos..];          // "@proxy:8080/..."
            return format!("{prefix}***:***{suffix}");
        }
    }
    url.to_string()
}

/// Parse a proxy URL string into a `reqwest::Proxy` for all traffic.
/// Excludes localhost/loopback from proxying (Lambda Extensions API, telemetry listener).
/// Returns `None` and logs a warning if the URL is invalid.
pub fn build_proxy(proxy_url: &str) -> Option<Proxy> {
    match Proxy::all(proxy_url) {
        Ok(proxy) => {
            let no_proxy = NoProxy::from_string("localhost, 127.0.0.1, [::1]");
            Some(proxy.no_proxy(no_proxy))
        }
        Err(e) => {
            warn!("Invalid proxy URL '{}', proceeding without proxy: {}", mask_proxy_url(proxy_url), e);
            None
        }
    }
}

/// Build a reqwest Client for outbound New Relic traffic.
///
/// - When `proxy_url` is `Some`, configures an explicit proxy and disables
///   reqwest's auto-detection of `HTTP_PROXY`/`HTTPS_PROXY` env vars
///   (calling `.proxy()` sets `auto_sys_proxy = false` internally).
/// - When `proxy_url` is `None`, auto-detection remains active as a fallback,
///   so `HTTPS_PROXY` still works for users who rely on it.
/// - Localhost/loopback is always excluded from proxying via `NoProxy`.
pub fn build_outbound_client(proxy_url: Option<&str>) -> Client {
    // Short pool_idle_timeout prevents stale connections after Lambda freeze/thaw:
    // Instant::now() uses CLOCK_MONOTONIC which advances during cgroup freeze,
    // so connections idle >2s are correctly evicted at checkout after thaw.
    // Within the same invocation, connections are reused (APM sends 6-8 requests).
    // reqwest/hyper does NOT auto-retry on stale connections (unlike Go's net/http),
    // so we rely on short idle timeout to discard dead connections before reuse.
    let mut builder = Client::builder()
        .timeout(std::time::Duration::from_millis(2400))
        .connect_timeout(std::time::Duration::from_secs(2))
        .pool_idle_timeout(std::time::Duration::from_secs(2))
        .pool_max_idle_per_host(10);

    if let Some(url) = proxy_url {
        if let Some(proxy) = build_proxy(url) {
            builder = builder.proxy(proxy);
        }
    }

    builder.build().expect("Failed to build outbound HTTP client")
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
            header::HeaderValue::from_static(EXTENSION_NAME_WITH_VERSION),
        );

        // Short pool_idle_timeout prevents stale connections after Lambda freeze/thaw:
        // Instant::now() uses CLOCK_MONOTONIC which advances during cgroup freeze,
        // so connections idle >2s are correctly evicted at checkout after thaw.
        // Within the same invocation, connections are reused (APM sends 6-8 requests).
        let mut builder = Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_millis(2400))
            .connect_timeout(std::time::Duration::from_secs(2))
            .pool_idle_timeout(std::time::Duration::from_secs(2))
            .pool_max_idle_per_host(10);

        if let Some(ref proxy_url) = config.new_relic.proxy_url {
            if let Some(proxy) = build_proxy(proxy_url) {
                info!("Proxy configured for New Relic client");
                builder = builder.proxy(proxy);
            }
        }

        let client = builder.build().unwrap();

        Self {
            client,
            cached_version_attrs: std::sync::OnceLock::new(),
        }
    }

    /// Returns a reference to the underlying reqwest Client for use in APM telemetry retry.
    /// This ensures retry calls use the same outbound client (with correct proxy/timeout settings)
    /// rather than the Lambda runtime API client.
    pub fn outbound_client(&self) -> &Client {
        &self.client
    }

    /// Creates a no-op New Relic client for disabled mode.
    pub fn new_noop() -> Self {
        let client = Client::builder()
            .no_proxy()
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
    ) -> Result<(), SendError> {
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
        common_attributes.insert("plugin".to_string(), serde_json::json!(EXTENSION_NAME_WITH_VERSION));
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

        // Add NR_TAGS as common attributes (cached at cold start)
        for (key, value) in crate::config::get_nr_tags() {
            debug!("Adding NR_TAGS to log payload: {}={}", key, value);
            common_attributes.insert(key.clone(), serde_json::json!(value));
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
    ) -> Result<(), SendError> {
        if config.new_relic.license_key.is_none() {
            warn!("[agentsend] New Relic license key is not set, skipping agent payload send");
            return Ok(());
        }
        
        let start_time = std::time::Instant::now();
        let payload_size = payload_json.len();
        debug!("Sending agent payload to NR: {} bytes", payload_size);
        
        let mut retries = 0;
        const MAX_RETRIES: usize = 3;
        // Pre-allocate body once outside the retry loop to avoid re-allocation per attempt
        let body: bytes::Bytes = payload_json.to_string().into();

        loop {
            let res = self.client
                .post(&config.new_relic.telemetry_endpoint)
                .header("X-License-Key", config.new_relic.license_key.as_deref().unwrap_or_default())
                .header(header::CONTENT_TYPE, "application/json")
                .body(body.clone()) // Bytes::clone is cheap (reference-counted)
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
                        continue;
                    }
                    warn!("[agentsend] Max retries exceeded for status {}", status);
                    return Err(anyhow!("[agentsend] HTTP {} after {} retries", status, MAX_RETRIES));
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
                        continue;
                    }
                    warn!("[agentsend] Max network retries exceeded");
                    return Err(e.into());
                }
            }
        }
    }

    /// Sends a JSON payload to a specified endpoint.
    async fn send_payload<T: Serialize>(&self, endpoint: &str, payload: &T, log_count: Option<usize>) -> Result<(), SendError> {
        let start_time = std::time::Instant::now();
        let body_str = match serde_json::to_string(payload) {
            Ok(json) => json,
            Err(e) => {
                warn!("Failed to serialize payload to JSON: {}", e);
                return Ok(());
            }
        };

        let payload_size = body_str.len();
        debug!("Sending payload to NR endpoint: {} bytes", payload_size);

        let mut retries = 0;
        const MAX_RETRIES: usize = 3;
        // Pre-allocate as Bytes once — Bytes::clone is cheap (reference-counted)
        let body: bytes::Bytes = body_str.into();

        loop {
            let res = self.client
                .post(endpoint)
                .header("Content-Type", "application/json")
                .body(body.clone())
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
                    }
                    warn!("Max retries exceeded for status {}", status);
                    return Err(anyhow!("HTTP {} after {} retries", status, MAX_RETRIES));
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
                    }
                    warn!("Max network retries exceeded");
                    return Err(e.into());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_proxy_url_with_credentials() {
        assert_eq!(
            mask_proxy_url("http://user:pass@proxy.internal:8080"),
            "http://***:***@proxy.internal:8080"
        );
    }

    #[test]
    fn test_mask_proxy_url_without_credentials() {
        assert_eq!(
            mask_proxy_url("http://proxy.internal:8080"),
            "http://proxy.internal:8080"
        );
    }

    #[test]
    fn test_mask_proxy_url_https_with_credentials() {
        assert_eq!(
            mask_proxy_url("https://admin:secret123@proxy:3128"),
            "https://***:***@proxy:3128"
        );
    }

    #[test]
    fn test_mask_proxy_url_with_path() {
        assert_eq!(
            mask_proxy_url("http://u:p@proxy:8080/path"),
            "http://***:***@proxy:8080/path"
        );
    }

    #[test]
    fn test_build_proxy_valid_url() {
        let proxy = build_proxy("http://proxy:8080");
        assert!(proxy.is_some());
    }

    #[test]
    fn test_build_proxy_empty_url() {
        // Empty string is the one case reqwest::Proxy::all() rejects
        let proxy = build_proxy("");
        assert!(proxy.is_none());
    }

    #[test]
    fn test_mask_proxy_url_never_leaks_credentials() {
        let test_cases = vec![
            ("http://myuser:mypassword@proxy:8080", "myuser", "mypassword"),
            ("https://admin:s3cret!@proxy.internal:3128", "admin", "s3cret!"),
            ("http://deploy-bot:token%40abc@corp-proxy:80/path", "deploy-bot", "token%40abc"),
            ("socks5://svc_account:P@$$w0rd@socks-proxy:1080", "svc_account", "P@$$w0rd"),
        ];

        for (url, username, password) in test_cases {
            let masked = mask_proxy_url(url);
            assert!(!masked.contains(username),
                "Credential leak: masked URL '{}' still contains the original username", masked);
            assert!(!masked.contains(password),
                "Credential leak: masked URL '{}' still contains the original password", masked);
            // Host must still be visible for debugging
            assert!(masked.contains("@"), "Masked URL should preserve @ separator: {}", masked);
            assert!(masked.contains("***:***"), "Masked URL should contain '***:***': {}", masked);
        }
    }
}
