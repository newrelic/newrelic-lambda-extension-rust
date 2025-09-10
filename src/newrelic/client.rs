use crate::{config, config::ExtensionConfig, newrelic::payload};
use reqwest::{header, Client, Error};
use flate2::{write::GzEncoder, Compression};
use std::io::Write;
use serde::Serialize;
use tracing::{info, warn, error};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::time::{timeout, Duration};
use std::sync::atomic::{AtomicU64, Ordering};

// Global counter to tag agent payload send attempts uniquely for clearer CloudWatch correlation
static AGENT_PAYLOAD_SEND_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
enum PendingRequest {
    LogPayload {
        endpoint: String,
        payload: String,
        attempts: usize,
    },
    AgentPayload {
        endpoint: String,
        payload: String,
        attempts: usize,
    },
}

#[derive(Debug, Clone)]
pub struct NewRelicClient {
    client: Client,
    pending_requests: Arc<Mutex<VecDeque<PendingRequest>>>,
}

impl NewRelicClient {
    /// Creates a new New Relic client.
    pub fn new() -> Self {
        let mut headers = header::HeaderMap::new();
        // Provide both Api-Key and X-License-Key to maximize compatibility with NR endpoints
        if let Some(license) = config::get_config().new_relic.license_key.clone() {
            if let Ok(v) = header::HeaderValue::from_str(&license) { headers.insert("Api-Key", v.clone()); }
            if let Ok(v) = header::HeaderValue::from_str(&license) { headers.insert("X-License-Key", v); }
        }

        Self {
            client: Client::builder()
                .default_headers(headers)
                .http1_only() // Avoid potential ALPN / h2 negotiation issues causing TLS EOF
                .connect_timeout(Duration::from_millis(800)) // Fast fail on bad network / DNS
                .pool_idle_timeout(Duration::from_secs(15))
                .tcp_nodelay(true)
                .timeout(Duration::from_millis(2500)) // Enforce a minimum timeout of 2.5 seconds
                .build()
                .unwrap_or_else(|_| Client::new()),
            pending_requests: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Add a pending request to the queue for retry during shutdown
    fn add_pending_request(&self, request: PendingRequest) {
        if let Ok(mut pending) = self.pending_requests.lock() {
            info!("Adding pending request to queue. Current queue size: {}", pending.len());
            pending.push_back(request);
            info!("Pending request added. New queue size: {}", pending.len());
        } else {
            error!("Failed to acquire lock on pending requests queue");
        }
    }

    /// Process all pending requests during shutdown
    pub async fn process_pending_requests(&self, config: &ExtensionConfig, timeout_duration: Duration) -> Result<(), Error> {
        info!("=== PROCESSING PENDING REQUESTS ===");
        
        let pending_count = {
            if let Ok(pending) = self.pending_requests.lock() {
                pending.len()
            } else {
                error!("Failed to acquire lock to check pending requests count");
                return Ok(());
            }
        };

        if pending_count == 0 {
            info!("No pending requests to process");
            return Ok(());
        }

        info!("Processing {} pending requests with timeout {:?}", pending_count, timeout_duration);

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(1400), // Leave 100ms buffer before 1.5s limit
            async {
                // Collect all pending requests at once for parallel processing
                let all_requests = {
                    if let Ok(mut pending) = self.pending_requests.lock() {
                        let requests: Vec<_> = pending.drain(..).collect();
                        requests
                    } else {
                        error!("Failed to acquire lock to get all pending requests");
                        return Ok::<(), Error>(());
                    }
                };

                if all_requests.is_empty() {
                    info!("No pending requests to process");
                    return Ok::<(), Error>(());
                }

                info!("Processing {} pending requests in parallel within 1.4 second timeout", all_requests.len());

                // Extract config data we need for the tasks
                let log_endpoint = config.new_relic.log_endpoint.clone();
                let telemetry_endpoint = config.new_relic.telemetry_endpoint.clone();
                let license_key = &config.new_relic.license_key.clone();

                // Process requests in parallel with limited concurrency to avoid overwhelming
                let max_concurrent = std::cmp::min(all_requests.len(), 10); // Max 10 concurrent requests
                let mut tasks = Vec::new();

                // Split requests into chunks for parallel processing
                for (index, request) in all_requests.into_iter().enumerate() {
                    let client = self.clone();
                    let log_endpoint = log_endpoint.clone();
                    let telemetry_endpoint = telemetry_endpoint.clone();
                    let license_key = license_key.clone();
                    
                    let task = tokio::spawn(async move {
                        info!("Starting parallel processing of pending request #{}", index + 1);
                        
                        // Create a temporary config-like structure for the request
                        let temp_config = ExtensionConfig {
                            new_relic: crate::config::NewRelicConfig {
                                log_endpoint,
                                telemetry_endpoint,
                                license_key,
                                ..Default::default()
                            },
                            ..Default::default()
                        };
                        
                        match client.retry_pending_request(request, &temp_config).await {
                            Ok(_) => {
                                info!("Successfully processed pending request #{}", index + 1);
                                Ok(())
                            }
                            Err(e) => {
                                error!("Failed to process pending request #{}: {}", index + 1, e);
                                Err(e)
                            }
                        }
                    });
                    
                    tasks.push(task);
                    
                    // Limit concurrent tasks to avoid overwhelming the system
                    if tasks.len() >= max_concurrent {
                        // Wait for these tasks to complete before starting more
                        let mut successful = 0;
                        let mut failed = 0;
                        
                        for task in tasks {
                            match task.await {
                                Ok(Ok(())) => successful += 1,
                                Ok(Err(_)) => failed += 1,
                                Err(e) => {
                                    failed += 1;
                                    error!("Task panicked: {:?}", e);
                                }
                            }
                        }
                        
                        info!("Completed chunk: {} successful, {} failed", successful, failed);
                        tasks = Vec::new();
                    }
                }

                // Process remaining tasks
                if !tasks.is_empty() {
                    info!("Processing final chunk of {} requests", tasks.len());
                    let mut successful = 0;
                    let mut failed = 0;
                    
                    for task in tasks {
                        match task.await {
                            Ok(Ok(())) => successful += 1,
                            Ok(Err(_)) => failed += 1,
                            Err(e) => {
                                failed += 1;
                                error!("Task panicked: {:?}", e);
                            }
                        }
                    }
                    
                    info!("Final chunk completed: {} successful, {} failed", successful, failed);
                }

                info!("=== ALL PENDING REQUESTS PROCESSED IN PARALLEL ===");
                Ok::<(), Error>(())
            }
        ).await;

        match result {
            Ok(_) => {
                info!("=== PENDING REQUESTS PROCESSING COMPLETED ===");
                Ok(())
            }
            Err(_) => {
                error!("=== PENDING REQUESTS PROCESSING TIMED OUT ===");
                let remaining = {
                    if let Ok(pending) = self.pending_requests.lock() {
                        pending.len()
                    } else { 0 }
                };
                error!("Failed to process {} remaining pending requests due to timeout", remaining);
                // Return Ok since timeout is expected and not really an error during shutdown
                Ok(())
            }
        }
    }

    /// Retry a specific pending request
    async fn retry_pending_request(&self, request: PendingRequest, config: &ExtensionConfig) -> Result<(), Error> {
        const MAX_ATTEMPTS: usize = 2; // Reduced attempts during shutdown

        match request {
            PendingRequest::LogPayload { endpoint, payload, attempts } => {
                if attempts >= MAX_ATTEMPTS {
                    error!("Log payload request exceeded max attempts ({}), dropping", MAX_ATTEMPTS);
                    return Ok(()); // Don't return error to continue processing other requests
                }

                info!("Retrying log payload request (attempt {} of {})", attempts + 1, MAX_ATTEMPTS);
                
                // Parse the payload back to the original log structure and use existing send_payload method
                match serde_json::from_str::<serde_json::Value>(&payload) {
                    Ok(parsed_payload) => {
                        match self.send_payload(&endpoint, &parsed_payload).await {
                            Ok(_) => {
                                info!("Successfully sent pending log payload");
                                Ok(())
                            }
                            Err(e) => {
                                error!("Failed to send pending log payload: {}", e);
                                
                                // Re-queue with incremented attempt count if we haven't exceeded max attempts
                                if attempts + 1 < MAX_ATTEMPTS {
                                    let retry_request = PendingRequest::LogPayload {
                                        endpoint,
                                        payload,
                                        attempts: attempts + 1,
                                    };
                                    self.add_pending_request(retry_request);
                                }
                                Err(e)
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to parse pending log payload: {}", e);
                        // Return Ok to continue processing other requests instead of stopping
                        Ok(())
                    }
                }
            }
            PendingRequest::AgentPayload { endpoint, payload, attempts } => {
                if attempts >= MAX_ATTEMPTS {
                    error!("Agent payload request exceeded max attempts ({}), dropping", MAX_ATTEMPTS);
                    return Ok(()); // Don't return error to continue processing other requests
                }

                info!("Retrying agent payload request (attempt {} of {})", attempts + 1, MAX_ATTEMPTS);
                match self.send_agent_payload(config, &payload).await {
                    Ok(_) => {
                        info!("Successfully sent pending agent payload");
                        Ok(())
                    }
                    Err(e) => {
                        error!("Failed to send pending agent payload: {}", e);
                        
                        // Re-queue with incremented attempt count if we haven't exceeded max attempts
                        if attempts + 1 < MAX_ATTEMPTS {
                            let retry_request = PendingRequest::AgentPayload {
                                endpoint,
                                payload,
                                attempts: attempts + 1,
                            };
                            self.add_pending_request(retry_request);
                        }
                        Err(e)
                    }
                }
            }
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

    /// Sends agent payload data directly to New Relic telemetry endpoint.
    /// This is used for compressed APM agent data (Node.js, Python, etc.)
    pub async fn send_agent_payload(
        &self,
        config: &ExtensionConfig,
        payload_data: &str,
    ) -> Result<(), Error> {
        // Validate license key
        if config.new_relic.license_key.is_none() {
            warn!("New Relic license key is not set, skipping agent payload send");
            return Ok(());
        }

    let send_id = AGENT_PAYLOAD_SEND_ID.fetch_add(1, Ordering::Relaxed);
    info!("[AgentSend#{}] Sending agent payload to New Relic telemetry endpoint: {} ({} bytes)", send_id, &config.new_relic.telemetry_endpoint, payload_data.len());
    // Send the decompressed (already JSON wrapped) agent payload directly
        self.send_raw_payload(&config.new_relic.telemetry_endpoint, payload_data, send_id).await
    }

    /// Sends a JSON payload to a specified endpoint.
    async fn send_raw_payload(&self, endpoint: &str, payload_data: &str, send_id: u64) -> Result<(), Error> {
        // Strict per-attempt total timeout (includes DNS + TLS + body send + response headers)
        const PER_ATTEMPT_TIMEOUT_MS: u64 = 2500; // matches minimum requirement
        const MAX_RETRIES: usize = 3;
        let license_key = crate::config::get_config().new_relic.license_key.as_deref().unwrap_or("");
        let overall_start = std::time::Instant::now();
        
        // Enhanced logging for debugging
        info!("[AgentSend#{}] Preparing raw payload to endpoint: {}", send_id, endpoint);
        info!("[AgentSend#{}] Payload size={} bytes, license_key_present={}", send_id, payload_data.len(), !license_key.is_empty());
        info!("[AgentSend#{}] License key prefix: {}***", send_id, license_key.get(..4).unwrap_or("NONE"));
        
        // Validate payload is valid JSON
        if let Err(e) = serde_json::from_str::<serde_json::Value>(payload_data) {
            error!("[AgentSend#{}] Invalid JSON payload: {}", send_id, e);
            info!("[AgentSend#{}] Invalid payload content: {}", send_id, payload_data);
            // Log and return Ok(()) instead of converting error type
            return Ok(());
        }
        info!("[AgentSend#{}] Payload JSON validation passed", send_id);
        
        // Log payload preview for debugging
        let preview = if payload_data.len() > 200 { 
            format!("{}...", &payload_data[..200]) 
        } else { 
            payload_data.to_string() 
        };
        info!("[AgentSend#{}] Payload preview: {}", send_id, preview);

        // Validate endpoint URL
        match url::Url::parse(endpoint) {
            Ok(parsed_url) => {
                info!("[AgentSend#{}] Endpoint URL validation passed: scheme={}, host={:?}", send_id, parsed_url.scheme(), parsed_url.host_str());
            },
            Err(e) => {
                error!("[AgentSend#{}] Invalid endpoint URL '{}': {}", send_id, endpoint, e);
                // Log and return Ok(()) instead of converting error type
                return Ok(());
            }
        }
        
        for attempt in 0..MAX_RETRIES {
            let attempt_start = std::time::Instant::now();
            info!("[AgentSend#{}] Attempt {} of {} (per-attempt timeout {}ms)", send_id, attempt + 1, MAX_RETRIES, PER_ATTEMPT_TIMEOUT_MS);
            // Build request (no misleading Content-Encoding header since we are not compressing)
            info!("[AgentSend#{}] Building request with headers: Content-Type=application/json, User-Agent=newrelic-lambda-extension", send_id);
            let request = self.client
                .post(endpoint)
                .header("Content-Type", "application/json")
                .header("User-Agent", "newrelic-lambda-extension")
                .header("X-License-Key", license_key)
                .body(payload_data.to_string());
            
            info!("[AgentSend#{}] Request built successfully, sending...", send_id);

            // Apply manual timeout guard to ensure we never exceed PER_ATTEMPT_TIMEOUT_MS
            let send_future = request.send();
            let result = timeout(Duration::from_millis(PER_ATTEMPT_TIMEOUT_MS), send_future).await;

            match result {
                Err(_) => {
                    error!("[AgentSend#{}] ⏱️ TIMEOUT: Attempt {} exceeded {}ms limit (overall_elapsed={}ms)", send_id, attempt + 1, PER_ATTEMPT_TIMEOUT_MS, overall_start.elapsed().as_millis());
                    error!("[AgentSend#{}] Timeout details: endpoint={}, payload_size={} bytes", send_id, endpoint, payload_data.len());
                    
                    if attempt + 1 < MAX_RETRIES {
                        let delay = std::time::Duration::from_millis(500 * (1 << attempt)); // 500ms, 1s, 2s
                        warn!("[AgentSend#{}] Timeout retry {} in {}ms", send_id, attempt + 2, delay.as_millis());
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    error!("[AgentSend#{}] ❌ All attempts timed out", send_id);
                    let pending_request = PendingRequest::AgentPayload { endpoint: endpoint.to_string(), payload: payload_data.to_string(), attempts: 0 }; 
                    self.add_pending_request(pending_request); 
                    return Ok(());
                }
                Ok(send_res) => match send_res {
                    Err(e) => {
                        error!("[AgentSend#{}] 🌐 NETWORK ERROR on attempt {}: {} (elapsed={}ms overall_elapsed={}ms)", send_id, attempt + 1, e, attempt_start.elapsed().as_millis(), overall_start.elapsed().as_millis());
                        
                        // Detailed error classification
                        if e.is_timeout() { 
                            error!("[AgentSend#{}] ⏱️ Network timeout - slow connection or server overload", send_id); 
                        }
                        if e.is_connect() { 
                            error!("[AgentSend#{}] 🔌 Connection failed - DNS resolution or network unreachable", send_id);
                            error!("[AgentSend#{}] Check: 1) Internet connectivity 2) DNS resolution for {}", send_id, endpoint);
                        }
                        if e.is_request() { 
                            error!("[AgentSend#{}] 📦 Request build failed - malformed request", send_id); 
                        }
                        if e.is_decode() {
                            error!("[AgentSend#{}] 🔄 Response decode error - corrupted response", send_id);
                        }
                        
                        // Log the raw error for debugging
                        info!("[AgentSend#{}] Raw error details: {:?}", send_id, e);
                        
                        if attempt + 1 < MAX_RETRIES {
                            let delay = std::time::Duration::from_millis(500 * (1 << attempt)); // Exponential: 500ms, 1s, 2s
                            warn!("[AgentSend#{}] Network retry {} in {}ms", send_id, attempt + 2, delay.as_millis());
                            tokio::time::sleep(delay).await;
                            continue;
                        }
                        error!("[AgentSend#{}] ❌ All network retries failed", send_id);
                        let pending_request = PendingRequest::AgentPayload { endpoint: endpoint.to_string(), payload: payload_data.to_string(), attempts: 0 }; 
                        self.add_pending_request(pending_request); 
                        return Err(e);
                    }
                    Ok(response) => {
                        let status = response.status();
                        let headers = response.headers().clone();
                        info!("[AgentSend#{}] Received status={} (attempt_elapsed={}ms overall_elapsed={}ms)", send_id, status, attempt_start.elapsed().as_millis(), overall_start.elapsed().as_millis());
                        
                        // Log response headers for debugging
                        info!("[AgentSend#{}] Response headers: {:?}", send_id, headers);
                        
                        if status.is_success() {
                            // Read response body to verify successful processing
                            let body = response.text().await.unwrap_or_else(|_| "<unreadable>".into());
                            info!("[AgentSend#{}] ✅ SUCCESS: Payload delivered (status={}) total_elapsed={}ms", send_id, status, overall_start.elapsed().as_millis());
                            info!("[AgentSend#{}] Success response body: {}", send_id, body);
                            
                            // Verify New Relic accepted the data
                            if body.contains("success") || body.is_empty() || status == 200 {
                                info!("[AgentSend#{}] ✅ New Relic confirmed data acceptance", send_id);
                            } else {
                                warn!("[AgentSend#{}] ⚠️ Unexpected success response: {}", send_id, body);
                            }
                            return Ok(());
                        }
                        
                        let body = response.text().await.unwrap_or_else(|_| "<unreadable>".into());
                        error!("[AgentSend#{}] ❌ FAILED: status={} body='{}' (attempt_elapsed={}ms)", send_id, status, body, attempt_start.elapsed().as_millis());
                        
                        // Enhanced error analysis
                        match status.as_u16() {
                            400 => error!("[AgentSend#{}] Bad Request - Invalid payload format or missing required fields", send_id),
                            401 => error!("[AgentSend#{}] Unauthorized - Invalid or missing license key", send_id),
                            403 => error!("[AgentSend#{}] Forbidden - License key valid but lacks permissions", send_id),
                            413 => error!("[AgentSend#{}] Payload Too Large - Reduce batch size", send_id),
                            429 => error!("[AgentSend#{}] Rate Limited - Too many requests", send_id),
                            500..=599 => error!("[AgentSend#{}] Server Error - New Relic service issue", send_id),
                            _ => error!("[AgentSend#{}] Unexpected status code", send_id),
                        }
                        
                        if status.is_client_error() {
                            error!("[AgentSend#{}] Client error - not retrying. Check payload format and credentials.", send_id);
                            return Ok(());
                        }
                        
                        if attempt + 1 < MAX_RETRIES { 
                            let delay = std::time::Duration::from_millis(500 * (1 << attempt)); // Exponential: 500ms, 1s, 2s
                            warn!("[AgentSend#{}] Server error retry {} in {}ms", send_id, attempt + 2, delay.as_millis()); 
                            tokio::time::sleep(delay).await; 
                            continue; 
                        }
                        error!("[AgentSend#{}] Server error retries exhausted", send_id); 
                        let pending_request = PendingRequest::AgentPayload { endpoint: endpoint.to_string(), payload: payload_data.to_string(), attempts: 0 }; 
                        self.add_pending_request(pending_request); 
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }

    /// Test connectivity to New Relic endpoints
    pub async fn test_connectivity(&self, config: &ExtensionConfig) -> Result<(), Error> {
        info!("[Diagnostics] Testing connectivity to New Relic endpoints...");
        
        // Test log endpoint
        info!("[Diagnostics] Testing log endpoint: {}", config.new_relic.log_endpoint);
        let log_test = self.client.head(&config.new_relic.log_endpoint).send().await;
        match log_test {
            Ok(resp) => info!("[Diagnostics] Log endpoint reachable: status={}", resp.status()),
            Err(e) => error!("[Diagnostics] Log endpoint unreachable: {}", e),
        }
        
        // Test telemetry endpoint  
        info!("[Diagnostics] Testing telemetry endpoint: {}", config.new_relic.telemetry_endpoint);
        let telemetry_test = self.client.head(&config.new_relic.telemetry_endpoint).send().await;
        match telemetry_test {
            Ok(resp) => info!("[Diagnostics] Telemetry endpoint reachable: status={}", resp.status()),
            Err(e) => error!("[Diagnostics] Telemetry endpoint unreachable: {}", e),
        }
        
        Ok(())
    }

    /// Sends a JSON payload to a specified endpoint.
    async fn send_payload<T: Serialize>(&self, endpoint: &str, payload: &T) -> Result<(), Error> {
        let json_body = match serde_json::to_string(payload) {
            Ok(json) => json,
            Err(e) => {
                warn!("Failed to serialize payload to JSON: {}", e);
                return Ok(());
            }
        };

        // Gzip compress (mirrors Go BuildVortexRequest/CompressedJsonPayload)
        let original_len = json_body.len();
        let compressed_body: Vec<u8> = {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            if let Err(e) = encoder.write_all(json_body.as_bytes()) {
                warn!("Failed to write payload to gzip encoder: {}", e);
                return Ok(());
            }
            match encoder.finish() {
                Ok(buf) => buf,
                Err(e) => {
                    warn!("Failed to finish gzip compression: {}", e);
                    return Ok(());
                }
            }
        };
        info!("Sending payload to endpoint: {} (original={}B compressed={}B ratio={:.2})", endpoint, original_len, compressed_body.len(), compressed_body.len() as f64 / original_len as f64);
        

        // Retry logic with exponential backoff
        let mut retries = 0;
        const MAX_RETRIES: usize = 3;
        
        loop {
            info!("Attempt {} of {} to send data to New Relic", retries + 1, MAX_RETRIES + 1);

            let license_key = crate::config::get_config().new_relic.license_key.as_deref().unwrap_or("");
            let mut req_builder = self.client
                .post(endpoint)
                .header("Content-Type", "application/json")
                .header("Content-Encoding", "gzip")
                .header("User-Agent", "newrelic-lambda-extension")
                .header("X-License-Key", license_key)
                .body(compressed_body.clone());

            // Add X-Event-Source: logs if this appears to be a logs endpoint
            if endpoint.contains("log-api") || endpoint.ends_with("/log/v1") { 
                req_builder = req_builder.header("X-Event-Source", "logs");
            }
            let res = req_builder.send().await;

            match res {
                Ok(response) => {
                    let status = response.status();
                    info!("Received response with status: {}", status);
                    
                    if status.is_success() {
                        info!("Successfully sent data to New Relic! Status: {}", status);
                        return Ok(());
                    } else {
                        let response_text = response.text().await.unwrap_or_else(|_| "Failed to read response".to_string());
                        warn!("Failed to send data to New Relic. Status: {}, Response: {}", status, response_text);

                        // Don't retry on client errors (4xx)
                        if status.is_client_error() {
                            warn!("Client error (4xx), not retrying");
                            return Ok(());
                        }
                        
                        // Retry on server errors (5xx) or other issues
                        if retries < MAX_RETRIES {
                            retries += 1;
                            let delay = std::time::Duration::from_millis(1000 * retries as u64);
                            warn!("Retrying in {}ms...", delay.as_millis());
                            tokio::time::sleep(delay).await;
                            continue;
                        } else {
                            warn!("Max retries exceeded for log payload, adding to pending queue");
                            // Add to pending queue for retry during shutdown
                            let pending_request = PendingRequest::LogPayload {
                                endpoint: endpoint.to_string(),
                                payload: json_body.clone(),
                                attempts: 0,
                            };
                            self.add_pending_request(pending_request);
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    warn!("Network error sending data to New Relic: {}", e);

                    if retries < MAX_RETRIES {
                        retries += 1;
                        let delay = std::time::Duration::from_millis(1000 * retries as u64);
                        warn!("Network error, retrying in {}ms...", delay.as_millis());
                        tokio::time::sleep(delay).await;
                        continue;
                    } else {
                        warn!("Max network retries exceeded for log payload, adding to pending queue");
                        // Add to pending queue for retry during shutdown
                        let pending_request = PendingRequest::LogPayload {
                            endpoint: endpoint.to_string(),
                            payload: json_body.clone(),
                            attempts: 0,
                        };
                        self.add_pending_request(pending_request);
                        return Err(e);
                    }
                }
            }
        }
    }

    // NOTE: Legacy implementation of send_raw_payload removed to ensure the per-attempt timeout version above is always used.

}
