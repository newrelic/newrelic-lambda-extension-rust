// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::{config::ExtensionConfig, newrelic::payload, version::VersionInfo};
use reqwest::{header, Client, NoProxy, Proxy};
use tracing::{debug, info, warn};

pub(crate) const EXTENSION_NAME: &str = env!("CARGO_PKG_NAME");
pub(crate) const EXTENSION_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Error type for outbound telemetry sends.
/// Distinguishes network failures from server-side exhaustion so callers
/// can decide whether to buffer for cross-invocation retry.
#[derive(Debug)]
pub enum SendError {
    Network(reqwest::Error),
    ServerExhausted { status: u16 },
    ClientRejected { status: u16 },
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Network(e) => write!(f, "network error: {}", e),
            Self::ServerExhausted { status } => {
                write!(f, "server error {} after max retries", status)
            }
            Self::ClientRejected { status } => {
                write!(f, "client error {} (not retryable)", status)
            }
        }
    }
}

pub(crate) fn get_extension_name_with_version() -> String {
    format!("{}:{}", EXTENSION_NAME, EXTENSION_VERSION)
}

pub(crate) fn get_backoff_delay(retry_attempt: usize) -> std::time::Duration {
    match retry_attempt {
        1 => std::time::Duration::from_millis(200),
        2 => std::time::Duration::from_millis(400),
        _ => std::time::Duration::from_millis(900),
    }
}

/// Backoff schedule used only when `NEW_RELIC_DATA_COLLECTION_TIMEOUT` is set:
/// 200ms for attempts 1-3, doubling every 3 attempts, capped at 3s. Matches the
/// New Relic Go extension's schedule.
pub(crate) fn get_growing_backoff_delay(retry_attempt: usize) -> std::time::Duration {
    let stage = retry_attempt.saturating_sub(1) / 3;
    let ms = 200u64.saturating_mul(1u64 << stage.min(4));
    std::time::Duration::from_millis(ms.min(3000))
}

/// Pull a short, human-readable message out of an error response body.
/// HTML error pages (e.g. "503 Service Unavailable" from an upstream proxy) are
/// reduced to their `<title>` text instead of dumping the full page into logs.
pub(crate) fn summarize_response_body(body: &str) -> String {
    let trimmed = body.trim();
    if let (Some(start), Some(end)) = (trimmed.find("<title>"), trimmed.find("</title>")) {
        let start = start + "<title>".len();
        if start < end {
            return trimmed[start..end].trim().to_string();
        }
    }
    trimmed.chars().take(200).collect()
}

/// Whether another retry attempt is allowed.
/// `budget` is `config.new_relic.data_collection_timeout`:
/// - `None` (env var unset): preserves the existing fixed-count behavior — same
///   `max_retries` the caller already uses today.
/// - `Some(b)`: the env var is present, so retries continue until `b` has elapsed
///   since the first attempt, capped at 20 attempts as a safety net (matches the
///   Go extension) — `max_retries` is not used in this branch.
pub(crate) fn retry_allowed(
    retries: usize,
    elapsed: std::time::Duration,
    budget: Option<std::time::Duration>,
    max_retries: usize,
) -> bool {
    match budget {
        Some(b) => retries < 20 && elapsed < b,
        None => retries < max_retries,
    }
}

/// Mask credentials in a proxy URL for safe logging.
/// `http://user:pass@proxy:8080` -> `http://***:***@proxy:8080`
pub fn mask_proxy_url(url: &str) -> String {
    // Use rfind to find the LAST `@` — the credential/host separator.
    // Handles passwords containing `@` (e.g., `http://user:P@ss@proxy:8080`)
    if let Some(at_pos) = url.rfind('@') {
        if let Some(scheme_end) = url.find("://") {
            let prefix = &url[..scheme_end + 3]; // "http://" or "https://"
            let suffix = &url[at_pos..];          // "@proxy:8080/..."
            return format!("{prefix}***:***{suffix}");
        }
    }
    url.to_string()
}

/// Strip the query string and fragment from a URL for safe logging.
///
/// APM connect/preconnect and collector URLs carry the license key as a
/// `?...&license_key=<KEY>` query parameter, so this returns only the
/// `scheme://host/path` prefix — never query parameters or credentials.
/// `https://collector.newrelic.com/agent_listener/invoke_raw_method?license_key=KEY`
/// -> `https://collector.newrelic.com/agent_listener/invoke_raw_method`
pub fn redact_url(url: &str) -> String {
    let end = url.find(['?', '#']).unwrap_or(url.len());
    url[..end].to_string()
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
    /// Per-ARN cached common-attributes JSON string (includes faas.arn).
    /// Avoids re-serializing the common block on every send — only the logs array changes.
    cached_common_json_by_arn: dashmap::DashMap<String, String>,
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
            .timeout(config.new_relic.http_timeout.unwrap_or(std::time::Duration::from_millis(2400)))
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
            cached_common_json_by_arn: dashmap::DashMap::new(),
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
            cached_common_json_by_arn: dashmap::DashMap::new(),
        }
    }

    /// Sends a batch of logs to New Relic.
    /// Uses pre-serialized common block per-ARN and builds final JSON via string concat
    /// to avoid cloning Map + re-serializing common attributes on every call.
    pub async fn send_logs(
        &self,
        config: &ExtensionConfig,
        batch: &[payload::LogMessage],
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

        // Defensive: `function_arn` is written verbatim into the common `faas.arn`
        // attribute below. Every caller resolves a real/fallback ARN (and re-buffers
        // rather than send when none is available), so this is unreachable today — but
        // guard here so a future refactor can never silently emit `faas.arn:""`.
        if function_arn.is_empty() {
            warn!(
                "send_logs called with empty ARN — skipping {} log(s) to avoid emitting an empty faas.arn",
                batch.len()
            );
            return Ok(());
        }

        let log_count = batch.len();
        debug!("Sending {} log messages to NR", log_count);

        // Get or build the cached common JSON for this ARN
        let common_json = self.get_or_build_common_json(config, function_arn);

        // Serialize just the logs array
        let logs_json = match serde_json::to_string(&batch) {
            Ok(j) => j,
            Err(e) => {
                warn!("Failed to serialize log batch: {}", e);
                return Ok(());
            }
        };

        // Build final payload: [{"common": <cached>, "logs": <batch>}]
        let body = format!(r#"[{{"common":{{"attributes":{}}},"logs":{}}}]"#, common_json, logs_json);

        self.send_payload_raw(
            &config.new_relic.log_endpoint,
            body,
            Some(log_count),
            config.new_relic.data_collection_timeout,
        )
        .await
    }

    /// Build or retrieve the pre-serialized common attributes JSON for a given ARN.
    fn get_or_build_common_json(&self, config: &ExtensionConfig, function_arn: &str) -> String {
        if let Some(cached) = self.cached_common_json_by_arn.get(function_arn) {
            return cached.clone();
        }

        let static_attrs = self.cached_static_log_attrs.get_or_init(|| {
            let mut attrs = serde_json::Map::new();
            attrs.insert("plugin".to_string(), serde_json::json!(get_extension_name_with_version()));
            attrs.insert("faas.name".to_string(), serde_json::json!(&config.aws.function_name));
            for (key, value) in crate::config::get_nr_tags() {
                debug!("Adding NR_TAGS to log payload: {}={}", key, value);
                attrs.insert(key.clone(), serde_json::json!(value));
            }
            // NEW_RELIC_LABELS (agent-specs/Labels.md) - additive, alongside NR_TAGS
            // above. Per spec, forwarded-log attributes for labels are prefixed
            // `tags.<label_type>`; NR_TAGS attributes stay unprefixed, unchanged.
            for (key, value) in crate::config::get_new_relic_labels() {
                let attr_key = format!("tags.{key}");
                debug!("Adding NEW_RELIC_LABELS to log payload: {attr_key}={value}");
                attrs.insert(attr_key, serde_json::json!(value));
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

        let json = serde_json::to_string(&common_attributes).unwrap_or_else(|_| "{}".to_string());
        self.cached_common_json_by_arn.insert(function_arn.to_string(), json.clone());
        json
    }

    /// Sends the wrapped agent payload to the New Relic collector.
    pub async fn send_agent_payload(
        &self,
        config: &ExtensionConfig,
        payload_json: &str,
    ) -> Result<(), reqwest::Error> {
        if config.new_relic.license_key.is_none() {
            warn!("[agentsend] New Relic license key is not set, skipping agent payload send");
            return Ok(());
        }
        
        let start_time = std::time::Instant::now();
        let uncompressed_size = payload_json.len();
        debug!("Sending agent payload to NR: {} bytes", uncompressed_size);

        let mut retries = 0;
        const MAX_RETRIES: usize = 3;
        let budget = config.new_relic.data_collection_timeout;

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
                            warn!("[agentsend] Failed to send agent payload. Status: {}, Response: {}", status, summarize_response_body(&response_text));
                        }

                        if status.is_client_error() {
                            warn!("[agentsend] Client error (4xx), not retrying");
                            return Ok(());
                        }

                        if retry_allowed(retries, start_time.elapsed(), budget, MAX_RETRIES) {
                            retries += 1;
                            let delay = match budget {
                                Some(_) => get_growing_backoff_delay(retries),
                                None => get_backoff_delay(retries),
                            };
                            debug!("[agentsend] retry attempt {}, data_collection_timeout: {:?}, next delay: {:?}", retries, budget, delay);
                            tokio::time::sleep(delay).await;
                        } else {
                            warn!("[agentsend] Exhausted {} retries over {:?} - unable to send data to endpoint", retries, start_time.elapsed());
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
                            let effective_timeout = config.new_relic.http_timeout.unwrap_or(std::time::Duration::from_millis(2400));
                            warn!("[agentsend] Request timeout after {:?} (will retry): {}", effective_timeout, e);
                        } else {
                            warn!("[agentsend] Network error: {}", e);
                        }
                    }

                    if retry_allowed(retries, start_time.elapsed(), budget, MAX_RETRIES) {
                        retries += 1;
                        let delay = match budget {
                            Some(_) => get_growing_backoff_delay(retries),
                            None => get_backoff_delay(retries),
                        };
                        debug!("[agentsend] retry attempt {}, data_collection_timeout: {:?}, next delay: {:?}", retries, budget, delay);
                        tokio::time::sleep(delay).await;
                    } else {
                        warn!("[agentsend] Exhausted {} retries over {:?} - unable to send data to endpoint: {}", retries, start_time.elapsed(), e);
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Sends a pre-built JSON body string to an endpoint (compress + retry).
    /// Used by send_logs to avoid double-serialization when common block is pre-cached.
    async fn send_payload_raw(
        &self,
        endpoint: &str,
        body: String,
        log_count: Option<usize>,
        budget: Option<std::time::Duration>,
    ) -> Result<(), SendError> {
        let start_time = std::time::Instant::now();
        let uncompressed_size = body.len();

        const GZIP_MIN_BYTES: usize = 512;

        let (send_bytes, use_gzip): (bytes::Bytes, bool) = if uncompressed_size < GZIP_MIN_BYTES {
            (bytes::Bytes::from(body.into_bytes()), false)
        } else {
            let raw = bytes::Bytes::from(body.into_bytes());
            let raw_for_spawn = raw.clone();
            let compressed = tokio::task::spawn_blocking(move || -> Option<Vec<u8>> {
                use flate2::write::GzEncoder;
                use flate2::Compression;
                use std::io::Write;
                let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
                enc.write_all(&raw_for_spawn).and_then(|_| enc.finish()).ok()
            })
            .await
            .ok()
            .flatten();

            match compressed {
                Some(c) => (bytes::Bytes::from(c), true),
                None => {
                    debug!("gzip compression failed; sending uncompressed");
                    (raw, false)
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
                        }
                        return Ok(());
                    } else {
                        let response_text = response.text().await.unwrap_or_default();
                        if retries == 0 {
                            warn!("Failed to send data. Status: {}, Response: {}", status, response_text);
                        }
                        if status.is_client_error() {
                            return Err(SendError::ClientRejected { status: status.as_u16() });
                        }
                        if retry_allowed(retries, start_time.elapsed(), budget, MAX_RETRIES) {
                            retries += 1;
                            let delay = match budget {
                                Some(_) => get_growing_backoff_delay(retries),
                                None => get_backoff_delay(retries),
                            };
                            debug!("retry attempt {}, data_collection_timeout: {:?}, next delay: {:?}", retries, budget, delay);
                            tokio::time::sleep(delay).await;
                            continue;
                        } else {
                            return Err(SendError::ServerExhausted { status: status.as_u16() });
                        }
                    }
                }
                Err(e) => {
                    if retries == 0 {
                        warn!("Network error sending payload: {}", e);
                    }
                    if retry_allowed(retries, start_time.elapsed(), budget, MAX_RETRIES) {
                        retries += 1;
                        let delay = match budget {
                            Some(_) => get_growing_backoff_delay(retries),
                            None => get_backoff_delay(retries),
                        };
                        debug!("retry attempt {}, data_collection_timeout: {:?}, next delay: {:?}", retries, budget, delay);
                        tokio::time::sleep(delay).await;
                        continue;
                    } else {
                        return Err(SendError::Network(e));
                    }
                }
            }
        }
    }
}
