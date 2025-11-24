

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
