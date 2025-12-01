

use std::sync::{Arc, Mutex};
use once_cell::sync::Lazy;
use dashmap::DashMap;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::{
    context::InvocationContext,
    platform::processor::PlatformProcessor,
    config::ExtensionConfig,
    newrelic::client::NewRelicClient,
    newrelic::flush::Flush,
};

#[derive(Debug)]
pub struct RequestProcessingState {
    pub context: Arc<Mutex<InvocationContext>>,
    pub platform_processor: Arc<PlatformProcessor>,
    pub agent_buffer: Arc<Mutex<Vec<Vec<u8>>>>,
    pub coordination_rx: Option<mpsc::UnboundedReceiver<()>>,
}

#[derive(Debug, Clone)]
pub struct ProcessorFactory {
    pub newrelic_client: Arc<NewRelicClient>,
    pub config: Arc<ExtensionConfig>,
    pub apm_app: crate::apm::SharedApmApp,
}

impl ProcessorFactory {
    pub fn new(
        newrelic_client: Arc<NewRelicClient>,
        config: Arc<ExtensionConfig>,
        apm_app: crate::apm::SharedApmApp,
    ) -> Self {
        Self {
            newrelic_client,
            config,
            apm_app,
        }
    }

    pub fn create_log_processor(
        &self,
        request_context: Arc<Mutex<InvocationContext>>,
    ) -> Arc<crate::logs::processor::LogProcessor> {
        Arc::new(crate::logs::processor::LogProcessor::new(
            Arc::clone(&self.newrelic_client),
            Arc::clone(&self.config),
            request_context,
            Some(Arc::clone(&self.apm_app)),
        ))
    }

    pub fn create_platform_processor(
        &self,
        request_context: Arc<Mutex<InvocationContext>>,
    ) -> Arc<PlatformProcessor> {
        Arc::new(PlatformProcessor::new(
            Arc::clone(&self.newrelic_client),
            Arc::clone(&self.config),
            request_context,
        ))
    }
}

pub static REQUEST_PROCESSORS: Lazy<Arc<DashMap<String, RequestProcessingState>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

pub static REQUEST_CONTEXTS: Lazy<Arc<DashMap<String, Arc<Mutex<InvocationContext>>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

pub static REQUEST_AGENT_BUFFERS: Lazy<Arc<DashMap<String, Arc<Mutex<Vec<Vec<u8>>>>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

pub static PAYLOAD_COORDINATION: Lazy<Arc<DashMap<String, mpsc::UnboundedSender<()>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

pub static RUNTIME_DONE_CHANNELS: Lazy<Arc<DashMap<String, mpsc::UnboundedSender<()>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

pub static PENDING_REPORTS: Lazy<Arc<DashMap<String, String>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

/// Track creation timestamps for request buffers to enable periodic cleanup
pub static REQUEST_BUFFER_TIMESTAMPS: Lazy<Arc<DashMap<String, chrono::DateTime<chrono::Utc>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

/// Agent payloads lack request_id - route to currently active request
pub static CURRENT_ACTIVE_REQUEST_ID: Lazy<Arc<Mutex<Option<String>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

pub fn create_request_processing_state(
    request_id: &str,
    invoked_function_arn: &str,
    processor_factory: &Arc<ProcessorFactory>,
) -> RequestProcessingState {
    let context = Arc::new(Mutex::new(InvocationContext {
        request_id: request_id.to_string(),
        invoked_function_arn: invoked_function_arn.to_string(),
        trace_id: None,
    }));

    let platform_processor = processor_factory.create_platform_processor(context.clone());

    let agent_buffer = Arc::new(Mutex::new(Vec::new()));

    let (payload_tx, payload_rx) = mpsc::unbounded_channel();
    PAYLOAD_COORDINATION.insert(request_id.to_string(), payload_tx);

    let state = RequestProcessingState {
        context: context.clone(),
        platform_processor,
        agent_buffer: agent_buffer.clone(),
        coordination_rx: Some(payload_rx),
    };

    REQUEST_CONTEXTS.insert(request_id.to_string(), context);
    REQUEST_AGENT_BUFFERS.insert(request_id.to_string(), agent_buffer);
    REQUEST_BUFFER_TIMESTAMPS.insert(request_id.to_string(), chrono::Utc::now());

    debug!(
        "Created per-request processing state for {} (using global log processor)",
        request_id
    );
    state
}

pub fn cleanup_request_processing_state(request_id: &str) {
    cleanup_request_processing_state_internal(request_id, false);
}

pub fn cleanup_request_processing_state_internal(request_id: &str, skip_buffer_cleanup: bool) {
    if REQUEST_PROCESSORS.remove(request_id).is_some() {
        debug!("Cleaned up request processing state for {}", request_id);
    }

    if skip_buffer_cleanup {
        debug!(
            "Keeping buffer alive for request {} to catch late agent payloads (will be processed on next invocation)",
            request_id
        );
    } else {
        if REQUEST_CONTEXTS.remove(request_id).is_some() {
            debug!("Cleaned up context for request {}", request_id);
        }

        if REQUEST_AGENT_BUFFERS.remove(request_id).is_some() {
            debug!("Cleaned up agent buffer for request {}", request_id);
        }

        REQUEST_BUFFER_TIMESTAMPS.remove(request_id);

        cleanup_payload_coordination_channel(request_id);
    }

    if RUNTIME_DONE_CHANNELS.remove(request_id).is_some() {
        debug!("Cleaned up runtime.done channel for request {}", request_id);
    }

    if PENDING_REPORTS.remove(request_id).is_some() {
        debug!(
            "Cleaned up pending platform.report for request {}",
            request_id
        );
    }
}

fn cleanup_payload_coordination_channel(request_id: &str) {
    if PAYLOAD_COORDINATION.remove(request_id).is_some() {
        debug!("Cleaned up coordination channel for request {}", request_id);
    }
}

/// Route agent payload to active request (payloads lack request_id)
pub async fn route_payload_to_request_buffer(payload_bytes: Vec<u8>) {
    let current_request_id = CURRENT_ACTIVE_REQUEST_ID
        .lock()
        .ok()
        .and_then(|guard| guard.clone());

    if let Some(request_id) = current_request_id {
        if let Some(request_buffer) = REQUEST_AGENT_BUFFERS.get(&request_id) {
            match request_buffer.lock() {
                Ok(mut buffer) => {
                    buffer.push(payload_bytes);
                    debug!(
                        "Stored agent payload in request buffer for {} (buffer size now {})",
                        request_id,
                        buffer.len()
                    );

                    if let Some(tx) = PAYLOAD_COORDINATION.get(&request_id) {
                        let _ = tx.send(());
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to lock request buffer for {}: {} - payload lost!",
                        request_id, e
                    );
                }
            }
        } else {
            warn!("No buffer found for request: {} - payload lost!", request_id);
        }
    } else {
        let any_request_id = REQUEST_AGENT_BUFFERS.iter().next().map(|entry| entry.key().clone());

        if let Some(request_id) = any_request_id {
            warn!(
                "No active request - routing late agent payload to buffer: {}",
                request_id
            );
            if let Some(request_buffer) = REQUEST_AGENT_BUFFERS.get(&request_id) {
                if let Ok(mut buffer) = request_buffer.lock() {
                    buffer.push(payload_bytes);
                    debug!(
                        "Stored late agent payload in buffer for {} (buffer size now {})",
                        request_id,
                        buffer.len()
                    );
                }
            }
        } else {
            warn!("No active requests found - agent payload lost!");
        }
    }
}

pub async fn wait_for_all_requests_completion(
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<crate::logs::processor::LogProcessor>,
) {
    let pending_count = REQUEST_PROCESSORS.len();

    if pending_count == 0 {
        debug!("No pending requests at shutdown - proceeding immediately");
    } else {
        info!(
            "Waiting for {} request(s) to complete...",
            pending_count
        );

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let remaining_requests: Vec<String> = REQUEST_PROCESSORS
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for request_id in remaining_requests {
            warn!("Force cleaning up request: {}", request_id);
            cleanup_request_processing_state(&request_id);
        }

        info!("All requests completed");
    }

    // Send all pending telemetry and logs in parallel with 1MB chunking
    info!("Shutdown: Flushing all pending telemetry and logs...");

    use crate::agent::batch::send_all_pending_payloads_on_shutdown;

    let telemetry_flush = tokio::spawn({
        let client = newrelic_client.clone();
        let cfg = config.clone();
        async move {
            send_all_pending_payloads_on_shutdown(client, cfg).await;
        }
    });

    let logs_flush = tokio::spawn({
        let log_processor = global_log_processor.clone();
        async move {
            if let Err(e) = log_processor.flush().await {
                error!("Shutdown: Failed to flush logs: {}", e);
            } else {
                info!("Shutdown: Successfully flushed logs");
            }
        }
    });

    // Wait for both to complete
    let (telemetry_result, logs_result) = tokio::join!(telemetry_flush, logs_flush);

    if let Err(e) = telemetry_result {
        error!("Shutdown: Telemetry flush task failed: {}", e);
    }
    if let Err(e) = logs_result {
        error!("Shutdown: Logs flush task failed: {}", e);
    }

    info!("Shutdown: All pending data flushed");
}

/// Cleanup old request buffers by sending their payloads to New Relic first
/// Finds buffers older than 5 minutes, sends the payloads, then removes them
pub async fn cleanup_old_request_buffers(
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
) {
    use crate::agent::batch::BatchedAgentPayload;
    use crate::EXTENSION_VERSION;

    let now = chrono::Utc::now();
    let threshold = chrono::Duration::minutes(5);

    // Find old request IDs based on timestamps
    let old_request_ids: Vec<String> = REQUEST_BUFFER_TIMESTAMPS
        .iter()
        .filter(|entry| now.signed_duration_since(*entry.value()) >= threshold)
        .map(|entry| entry.key().clone())
        .collect();

    if old_request_ids.is_empty() {
        return;
    }

    info!("Periodic cleanup: Found {} old request buffers to send and remove", old_request_ids.len());

    let mut all_payloads: Vec<BatchedAgentPayload> = Vec::new();

    // Collect payloads from old request buffers
    for request_id in &old_request_ids {
        if let Some(buffer) = REQUEST_AGENT_BUFFERS.get(request_id) {
            let payloads = if let Ok(mut buf) = buffer.lock() {
                std::mem::take(&mut *buf)
            } else {
                Vec::new()
            };

            if !payloads.is_empty() {
                // Get report line if available
                let report_line = PENDING_REPORTS.get(request_id).map(|entry| entry.value().clone());

                // Get context
                let arn = REQUEST_CONTEXTS
                    .get(request_id)
                    .map(|ctx_entry| {
                        ctx_entry
                            .lock()
                            .ok()
                            .map(|ctx| ctx.invoked_function_arn.clone())
                            .unwrap_or_else(|| "unknown".to_string())
                    })
                    .unwrap_or_else(|| "unknown".to_string());

                for payload_bytes in payloads {
                    all_payloads.push(BatchedAgentPayload {
                        request_id: request_id.clone(),
                        agent_payload_bytes: Arc::new(payload_bytes),
                        report_line: report_line.clone(),
                        invoked_function_arn: arn.clone(),
                        timestamp: chrono::Utc::now(),
                    });
                }
            }
        }
    }

    // Send payloads to New Relic before cleanup
    if !all_payloads.is_empty() {
        info!("Periodic cleanup: Sending {} payloads from old request buffers", all_payloads.len());

        // Pre-allocate capacity: each item needs 1-2 log events (agent + optional report)
        let mut log_events = Vec::with_capacity(all_payloads.len() * 2);

        for item in &all_payloads {
            // Avoid unnecessary string clones - use Cow to only allocate on invalid UTF-8
            let agent_str = String::from_utf8_lossy(&item.agent_payload_bytes);
            log_events.push(serde_json::json!({
                "id": &item.request_id,
                "message": &*agent_str,
                "timestamp": item.timestamp.timestamp_millis(),
            }));

            if let Some(ref report) = item.report_line {
                log_events.push(serde_json::json!({
                    "id": item.request_id,
                    "message": report,
                    "timestamp": item.timestamp.timestamp_millis(),
                }));
            }
        }

        let most_recent = all_payloads.last().expect("all_payloads should not be empty");

        let entry = serde_json::json!({
            "logEvents": log_events,
            "logGroup": format!("/aws/lambda/{}", config.aws.function_name),
            "logStream": format!("newrelic-lambda-extension:{}", EXTENSION_VERSION),
            "messageType": "",
            "owner": "",
        });

        let payload = serde_json::json!({
            "context": {
                "function_name": config.aws.function_name,
                "invoked_function_arn": most_recent.invoked_function_arn,
                "log_group_name": format!("/aws/lambda/{}", config.aws.function_name),
                "log_stream_name": format!("newrelic-lambda-extension:{}", EXTENSION_VERSION),
            },
            "entry": entry.to_string(),
        });

        let payload_json = payload.to_string();

        if let Err(e) = newrelic_client.send_agent_payload(&config, &payload_json).await {
            error!("Periodic cleanup: Failed to send old request buffer payloads: {}", e);
        } else {
            info!("Periodic cleanup: Successfully sent payloads from old request buffers");
        }
    }

    // Now cleanup the old buffers
    for request_id in &old_request_ids {
        REQUEST_CONTEXTS.remove(request_id);
        REQUEST_AGENT_BUFFERS.remove(request_id);
        PAYLOAD_COORDINATION.remove(request_id);
        PENDING_REPORTS.remove(request_id);
        REQUEST_BUFFER_TIMESTAMPS.remove(request_id);
    }

    info!("Periodic cleanup: Removed {} old request buffers", old_request_ids.len());
}
