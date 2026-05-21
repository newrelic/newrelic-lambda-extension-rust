// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::{config::ExtensionConfig, newrelic::payload, version::VersionInfo};
use reqwest::{header, Client, Error, NoProxy, Proxy};
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
    let mut builder = Client::builder()
        .timeout(std::time::Duration::from_millis(2400))
        .connect_timeout(std::time::Duration::from_secs(10))
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .pool_max_idle_per_host(10)
        .tcp_keepalive(std::time::Duration::from_secs(30));

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
    /// Static log common-attributes: plugin name, faas.name, NR_TAGS, and version tags.
    /// faas.arn is per-call and inserted separately. Cached on first log send.
    cached_static_log_attrs: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>>,
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

        let mut builder = Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_millis(2400))
            .connect_timeout(std::time::Duration::from_secs(10))
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .pool_max_idle_per_host(10)
            .tcp_keepalive(std::time::Duration::from_secs(30));

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
            cached_static_log_attrs: std::sync::OnceLock::new(),
        }
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
            cached_static_log_attrs: std::sync::OnceLock::new(),
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

        // Build static attributes once (plugin, faas.name, NR_TAGS). faas.arn is
        // per-call because it can change across invocations; insert it separately.
        let static_attrs = self.cached_static_log_attrs.get_or_init(|| {
            let mut attrs = serde_json::Map::new();
            attrs.insert("plugin".to_string(), serde_json::json!(get_extension_name_with_version()));
            attrs.insert("faas.name".to_string(), serde_json::json!(&config.aws.function_name));
            for (key, value) in crate::config::get_nr_tags() {
                debug!("Adding NR_TAGS to log payload: {}={}", key, value);
                attrs.insert(key.clone(), serde_json::json!(value));
            }
            attrs
        });

        let mut common_attributes = static_attrs.clone();
        common_attributes.insert("faas.arn".to_string(), serde_json::json!(function_arn));

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
        let uncompressed_size = payload_json.len();
        debug!("Sending agent payload to NR: {} bytes", uncompressed_size);
        
        let mut retries = 0;
        const MAX_RETRIES: usize = 3;

        loop {
            
            let res = self.client
                .post(&config.new_relic.telemetry_endpoint)
                .header("X-License-Key", config.new_relic.license_key.as_deref().unwrap_or_default())
                .header(header::CONTENT_TYPE, "application/json")
                .body(payload_json.to_string())
                .send()
                .await;

            match res {
                Ok(response) => {
                    let status = response.status();
                    
                    if status.is_success() {
                        let duration = start_time.elapsed();
                        debug!("Agent payload sent: {} bytes, duration: {:?}", uncompressed_size, duration);
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

        let uncompressed_size = body.len();

        let (send_bytes, use_gzip): (bytes::Bytes, bool) = {
            use flate2::write::GzEncoder;
            use flate2::Compression;
            use std::io::Write;
            let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
            match enc.write_all(body.as_bytes()).and_then(|_| enc.finish()) {
                Ok(compressed) => (bytes::Bytes::from(compressed), true),
                Err(e) => {
                    // Log warn once per process; subsequent failures downgrade to debug
                    // so a persistent gzip failure can't flood operator CloudWatch.
                    static WARN_ONCE: std::sync::atomic::AtomicBool =
                        std::sync::atomic::AtomicBool::new(false);
                    if !WARN_ONCE.swap(true, std::sync::atomic::Ordering::Relaxed) {
                        warn!("gzip compression failed ({}); sending uncompressed (further failures will be logged at debug)", e);
                    } else {
                        debug!("gzip compression failed ({}); sending uncompressed", e);
                    }
                    (bytes::Bytes::from(body.into_bytes()), false)
                }
            }
        };

        debug!("Sending payload to NR endpoint: {} bytes{}",
            send_bytes.len(),
            if use_gzip { format!(" (gzip, uncompressed: {})", uncompressed_size) } else { String::new() });

        let mut retries = 0;
        const MAX_RETRIES: usize = 3;

        loop {
            let mut request = self.client
                .post(endpoint)
                .header("Content-Type", "application/json");
            if use_gzip {
                request = request.header("Content-Encoding", "gzip");
            }
            // bytes::Bytes clone is a cheap Arc refcount bump, not a full Vec copy.
            let res = request
                .body(send_bytes.clone())
                .send()
                .await;

            match res {
                Ok(response) => {
                    let status = response.status();
                    
                    if status.is_success() {
                        let duration = start_time.elapsed();
                        if let Some(count) = log_count {
                            debug!("Logs sent: {} logs, {} bytes, duration: {:?}", count, uncompressed_size, duration);
                        } else {
                            debug!("Payload sent: {} bytes, duration: {:?}", uncompressed_size, duration);
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
                            warn!("Request timeout (will retry): {}", e);
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
