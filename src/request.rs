//! Per-request state management for concurrent request handling
//!
//! This module manages the lifecycle of per-request state including:
//! - Request contexts (`request_id`, `invoked_function_arn`, `trace_id`)
//! - Agent payload buffers (per-request buffers for incoming agent data)
//! - Coordination channels (for agent payload arrival notifications)
//! - `Runtime.done` channels (signaled by telemetry listener)
//! - Platform processors (per-request platform telemetry processing)
//!
//! Global state stores:
//! - `REQUEST_PROCESSORS`: Main per-request state
//! - `REQUEST_CONTEXTS`: Per-request invocation contexts
//! - `REQUEST_AGENT_BUFFERS`: Per-request agent payload buffers
//! - `PAYLOAD_COORDINATION`: Coordination channels for agent payload arrival
//! - `RUNTIME_DONE_CHANNELS`: `Runtime.done` signal channels
//! - `PENDING_REPORTS`: Pending `platform.report` lines (when report arrives before agent)
//! - `CURRENT_ACTIVE_REQUEST_ID`: Currently active request for agent payload routing

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
    agent::batch::{add_to_batch, should_send_batch, send_batched_payloads},
};

/// Per-request processing state
#[derive(Debug)]
pub struct RequestProcessingState {
    pub context: Arc<Mutex<InvocationContext>>,
    pub platform_processor: Arc<PlatformProcessor>,
    pub agent_buffer: Arc<Mutex<Vec<Vec<u8>>>>,
    pub coordination_rx: Option<mpsc::UnboundedReceiver<()>>,
    pub runtime_done_rx: Option<mpsc::UnboundedReceiver<()>>,
}

/// Processor factory for creating per-request processors
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

/// Global per-request processing state
pub static REQUEST_PROCESSORS: Lazy<Arc<DashMap<String, RequestProcessingState>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

/// Global per-request contexts
pub static REQUEST_CONTEXTS: Lazy<Arc<DashMap<String, Arc<Mutex<InvocationContext>>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

/// Global per-request agent buffers
pub static REQUEST_AGENT_BUFFERS: Lazy<Arc<DashMap<String, Arc<Mutex<Vec<Vec<u8>>>>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

/// Global coordination channels per request for agent payload processing
pub static PAYLOAD_COORDINATION: Lazy<Arc<DashMap<String, mpsc::UnboundedSender<()>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

/// Per-request runtime.done signal channels (signaled by telemetry listener on platform.runtimeDone)
pub static RUNTIME_DONE_CHANNELS: Lazy<Arc<DashMap<String, mpsc::UnboundedSender<()>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

/// Pending `platform.report` lines (stored when report arrives before agent is batched)
/// Key: `request_id`, Value: report log line
pub static PENDING_REPORTS: Lazy<Arc<DashMap<String, String>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

/// CRITICAL: Track currently active request for agent payload routing
/// Since agent payloads don't include `request_id`, we route to the most recent ACTIVE request
/// This works because Lambda typically processes requests sequentially (though concurrent is possible)
pub static CURRENT_ACTIVE_REQUEST_ID: Lazy<Arc<Mutex<Option<String>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

/// Create per-request processing state for concurrent request handling
pub fn create_request_processing_state(
    request_id: &str,
    invoked_function_arn: &str,
    processor_factory: &Arc<ProcessorFactory>,
) -> RequestProcessingState {
    // Create context
    let context = Arc::new(Mutex::new(InvocationContext {
        request_id: request_id.to_string(),
        invoked_function_arn: invoked_function_arn.to_string(),
        trace_id: None,
    }));

    // Create only platform processor - log processor will be global
    let platform_processor = processor_factory.create_platform_processor(context.clone());

    // Create agent buffer
    let agent_buffer = Arc::new(Mutex::new(Vec::new()));

    // Create coordination channel for agent payload arrival
    let (payload_tx, payload_rx) = mpsc::unbounded_channel();
    PAYLOAD_COORDINATION.insert(request_id.to_string(), payload_tx);

    // Create runtime.done channel (telemetry listener will signal this)
    let (runtime_done_tx, runtime_done_rx) = mpsc::unbounded_channel();
    RUNTIME_DONE_CHANNELS.insert(request_id.to_string(), runtime_done_tx);

    let state = RequestProcessingState {
        context: context.clone(),
        platform_processor,
        agent_buffer: agent_buffer.clone(),
        coordination_rx: Some(payload_rx),
        runtime_done_rx: Some(runtime_done_rx),
    };

    REQUEST_CONTEXTS.insert(request_id.to_string(), context);
    REQUEST_AGENT_BUFFERS.insert(request_id.to_string(), agent_buffer);

    debug!(
        "Created per-request processing state for {} (using global log processor)",
        request_id
    );
    state
}

/// Clean up per-request processing state after processing
pub fn cleanup_request_processing_state(request_id: &str) {
    cleanup_request_processing_state_internal(request_id, false);
}

/// Clean up per-request processing state - for both Standard and APM modes
/// Cold start: cleanup everything
/// Warm start: keep buffer alive for late agent payloads (both modes use this strategy)
pub fn cleanup_request_processing_state_conditional(request_id: &str, is_cold_start: bool) {
    let skip_buffer_cleanup = !is_cold_start;
    cleanup_request_processing_state_internal(request_id, skip_buffer_cleanup);
}

/// Clean up with option to skip buffer cleanup (for warm starts in both modes)
pub fn cleanup_request_processing_state_internal(request_id: &str, skip_buffer_cleanup: bool) {
    // Clean up request processing state
    if REQUEST_PROCESSORS.remove(request_id).is_some() {
        debug!("Cleaned up request processing state for {}", request_id);
    }

    if skip_buffer_cleanup {
        debug!(
            "Keeping buffer alive for request {} to catch late agent payloads (will be processed on next invocation)",
            request_id
        );
    } else {
        // Clean up context
        if REQUEST_CONTEXTS.remove(request_id).is_some() {
            debug!("Cleaned up context for request {}", request_id);
        }

        // Clean up agent buffer
        if REQUEST_AGENT_BUFFERS.remove(request_id).is_some() {
            debug!("Cleaned up agent buffer for request {}", request_id);
        }

        // Clean up payload coordination channel
        cleanup_payload_coordination_channel(request_id);
    }

    // Always clean up runtime.done channel
    if RUNTIME_DONE_CHANNELS.remove(request_id).is_some() {
        debug!("Cleaned up runtime.done channel for request {}", request_id);
    }

    // Clean up any pending report for this request
    if PENDING_REPORTS.remove(request_id).is_some() {
        debug!(
            "Cleaned up pending platform.report for request {}",
            request_id
        );
    }
}

/// Clean up payload coordination channel
fn cleanup_payload_coordination_channel(request_id: &str) {
    if PAYLOAD_COORDINATION.remove(request_id).is_some() {
        debug!("Cleaned up coordination channel for request {}", request_id);
    }
}

/// Route agent payload to the correct per-request buffer
///
/// CRITICAL: Agent payloads don't include `request_id`, so we route to the currently active request.
/// This works because:
/// 1. Lambda typically processes requests sequentially (though concurrent is possible)
/// 2. We track the active `request_id` in `CURRENT_ACTIVE_REQUEST_ID`
/// 3. Each request sets this before waiting for agent payload
/// 4. Each request clears this after processing
pub async fn route_payload_to_request_buffer(payload_bytes: Vec<u8>) {
    // Get the currently active request ID
    let current_request_id = CURRENT_ACTIVE_REQUEST_ID
        .lock()
        .ok()
        .and_then(|guard| guard.clone());

    if let Some(request_id) = current_request_id {
        // Store in request-specific buffer
        if let Some(request_buffer) = REQUEST_AGENT_BUFFERS.get(&request_id) {
            match request_buffer.lock() {
                Ok(mut buffer) => {
                    buffer.push(payload_bytes);
                    debug!(
                        "Stored agent payload in request buffer for {} (buffer size now {})",
                        request_id,
                        buffer.len()
                    );

                    // Notify the request's coordination channel if available
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
        // No active request - this could be a late payload from a previous request
        // Try to find ANY request buffer that's still alive (for APM mode warm starts)
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

/// Wait for all requests to complete and flush batched payloads
pub async fn wait_for_all_requests_completion(
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
) {
    // Check if there are any pending requests
    let pending_count = REQUEST_PROCESSORS.len();

    if pending_count == 0 {
        debug!("No pending requests at shutdown - proceeding immediately");
    } else {
        info!(
            "Waiting for {} request(s) to complete...",
            pending_count
        );

        // Wait a reasonable time for requests to complete
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Force cleanup of any remaining requests
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

    // Phase 9: Flush any batched agent payloads before shutdown
    let batch_count = crate::agent::batch::AGENT_BATCH_BUFFER.len();
    if batch_count > 0 {
        info!(
            "Flushing {} batched agent payload(s) before shutdown",
            batch_count
        );
        send_batched_payloads(newrelic_client, config).await;
    }
}

/// Process warm start old buffers - clean up old buffers and batch late agent payloads
#[allow(dead_code)]
pub async fn process_warm_start_old_buffers(
    current_request_id: &str,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
) {
    // Find old request buffers (from previous invocation)
    let old_requests: Vec<String> = REQUEST_AGENT_BUFFERS
        .iter()
        .map(|entry| entry.key().clone())
        .collect();

    for old_request_id in old_requests {
        if old_request_id == current_request_id {
            continue; // Skip current request
        }

        // Check if there's a late agent payload in the buffer
        if let Some(buffer) = REQUEST_AGENT_BUFFERS.get(&old_request_id) {
            if let Ok(buffer_guard) = buffer.lock() {
                if !buffer_guard.is_empty() {
                    info!(
                        "Found {} late agent payload(s) for previous request: {}",
                        buffer_guard.len(),
                        old_request_id
                    );

                    // Check if there's a matching platform.report in PENDING_REPORTS
                    let report_line = PENDING_REPORTS.remove(&old_request_id).map(|(_, report)| {
                        debug!(
                            "Found matching platform.report for late agent payload: {}",
                            old_request_id
                        );
                        report
                    });

                    // Add to batch for sending
                    for payload_bytes in buffer_guard.iter() {
                        let context = REQUEST_CONTEXTS
                            .get(&old_request_id)
                            .and_then(|ctx_entry| {
                                ctx_entry
                                    .lock()
                                    .ok()
                                    .map(|ctx| ctx.invoked_function_arn.clone())
                            })
                            .unwrap_or_else(|| "unknown".to_string());

                        add_to_batch(
                            old_request_id.clone(),
                            payload_bytes.clone(),
                            report_line.clone(),
                            context,
                        );
                    }
                }
            }
        }

        debug!(
            "Cleaning up old buffer from previous request: {}",
            old_request_id
        );
        cleanup_request_processing_state(&old_request_id);
    }

    // Check if batch should be sent now
    if should_send_batch() {
        tokio::spawn(async move {
            send_batched_payloads(newrelic_client, config).await;
        });
    }
}
