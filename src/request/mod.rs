//! Request processing module
//!
//! Handles per-request data collection and routing for Lambda executions.
//! Uses CURRENT_ACTIVE_REQUEST_ID (2.4.1 approach) for agent payload routing.
//! Each request gets isolated processors with their own InvocationContext.

use std::sync::{Arc, Mutex};
use once_cell::sync::Lazy;
use dashmap::DashMap;
use tokio::sync::mpsc;
use tracing::warn;

use crate::{
    context::InvocationContext,
    platform::processor::PlatformProcessor,
    config::ExtensionConfig,
    newrelic::client::NewRelicClient,
};

#[derive(Debug)]
pub struct RequestProcessingState {
    pub context: Arc<Mutex<InvocationContext>>,
    /// Per-request log processor - used internally by PlatformProcessor and kept alive for request lifetime
    #[allow(dead_code)]
    pub log_processor: Arc<crate::logs::processor::LogProcessor>,
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
        log_processor: Arc<crate::logs::processor::LogProcessor>,
    ) -> Arc<PlatformProcessor> {
        Arc::new(PlatformProcessor::new(
            Arc::clone(&self.newrelic_client),
            Arc::clone(&self.config),
            request_context,
            log_processor,
        ))
    }
}

// Legacy state tracking - kept for backward compatibility during migration
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

pub static REQUEST_BUFFER_TIMESTAMPS: Lazy<Arc<DashMap<String, chrono::DateTime<chrono::Utc>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

/// Agent payloads lack request_id - route to currently active request
/// This tracks which request is actively processing to route agent payloads correctly
pub static CURRENT_ACTIVE_REQUEST_ID: Lazy<Arc<std::sync::Mutex<Option<String>>>> =
    Lazy::new(|| Arc::new(std::sync::Mutex::new(None)));

/// Telemetry-based request_id tracking - updated from platform.start events
/// This is the SOURCE OF TRUTH for stamping function/extension logs with request_id.
///
/// WHY: The event loop updates CURRENT_ACTIVE_REQUEST_ID immediately when GET /next returns,
/// but the telemetry API delivers function logs asynchronously. Late logs from request_A
/// can arrive AFTER the event loop has moved to request_B. Using the event loop's context
/// would stamp them with B's request_id (WRONG).
///
/// platform.start always arrives BEFORE function logs for that request in the telemetry stream,
/// so tracking request_id from platform.start gives us the correct association.
pub static TELEMETRY_CURRENT_REQUEST_ID: Lazy<Arc<std::sync::Mutex<Option<String>>>> =
    Lazy::new(|| Arc::new(std::sync::Mutex::new(None)));

/// Buffer for orphaned agent payloads that arrive before any request is created.
/// Drained into the first request's buffer in create_request_processing_state().
/// In normal Lambda operation: at most 1 payload (cold start edge case only).
pub static ORPHANED_PAYLOADS: Lazy<Arc<std::sync::Mutex<Vec<Vec<u8>>>>> =
    Lazy::new(|| Arc::new(std::sync::Mutex::new(Vec::new())));

pub fn create_request_processing_state(
    request_id: &str,
    invoked_function_arn: &str,
    processor_factory: &Arc<ProcessorFactory>,
    is_apm_mode: bool,
) -> RequestProcessingState {
    let context = Arc::new(Mutex::new(InvocationContext {
        request_id: request_id.to_string(),
        invoked_function_arn: invoked_function_arn.to_string(),
        trace_id: None,
    }));

    // Create per-request processors - each request gets isolated processors with their own context
    // This prevents race conditions in concurrent executions
    let log_processor = processor_factory.create_log_processor(context.clone());
    let platform_processor = processor_factory.create_platform_processor(context.clone(), log_processor.clone());

    let agent_buffer = Arc::new(Mutex::new(Vec::new()));

    // Drain orphaned payloads into this request's buffer
    // Orphaned payloads arrive via named pipe before the first INVOKE event creates a request
    if let Ok(mut orphaned) = ORPHANED_PAYLOADS.lock() {
        if !orphaned.is_empty() {
            let drained: Vec<Vec<u8>> = orphaned.drain(..).collect();
            tracing::info!(
                "Draining {} orphaned agent payload(s) into request buffer for: {}",
                drained.len(),
                request_id
            );
            if let Ok(mut buf) = agent_buffer.lock() {
                buf.extend(drained);
            }
        }
    }

    let (payload_tx, payload_rx) = mpsc::unbounded_channel();
    PAYLOAD_COORDINATION.insert(request_id.to_string(), payload_tx);

    // Only create runtime.done channel for standard mode (not needed in APM mode)
    if !is_apm_mode {
        let (runtime_done_tx, _runtime_done_rx) = mpsc::unbounded_channel();
        RUNTIME_DONE_CHANNELS.insert(request_id.to_string(), runtime_done_tx);
    }

    let state = RequestProcessingState {
        context: context.clone(),
        log_processor: log_processor.clone(),
        platform_processor,
        agent_buffer: agent_buffer.clone(),
        coordination_rx: Some(payload_rx),
    };

    REQUEST_CONTEXTS.insert(request_id.to_string(), context);
    REQUEST_AGENT_BUFFERS.insert(request_id.to_string(), agent_buffer.clone());
    REQUEST_BUFFER_TIMESTAMPS.insert(request_id.to_string(), chrono::Utc::now());

    state
}

pub fn cleanup_request_processing_state(request_id: &str) {
    cleanup_request_processing_state_internal(request_id, false);
}

pub fn cleanup_request_processing_state_internal(request_id: &str, skip_buffer_cleanup: bool) {
    REQUEST_PROCESSORS.remove(request_id);
    
    if !skip_buffer_cleanup {
        REQUEST_CONTEXTS.remove(request_id);
        REQUEST_AGENT_BUFFERS.remove(request_id);
        REQUEST_BUFFER_TIMESTAMPS.remove(request_id);
        PAYLOAD_COORDINATION.remove(request_id);
    }
    
    RUNTIME_DONE_CHANNELS.remove(request_id);
    PENDING_REPORTS.remove(request_id);
}

/// Periodic cleanup of stale request buffers older than 5 minutes.
/// Stale buffers occur when agent payloads never get matched with platform.report.
/// The core sending path (telemetry listener + process_request_concurrently) handles
/// normal payload+report matching — this just prevents memory leaks from edge cases.
pub async fn cleanup_old_request_buffers() {
    let now = chrono::Utc::now();
    let threshold = chrono::Duration::minutes(5);

    let old_request_ids: Vec<String> = REQUEST_BUFFER_TIMESTAMPS
        .iter()
        .filter(|entry| now.signed_duration_since(*entry.value()) >= threshold)
        .map(|entry| entry.key().clone())
        .collect();

    if old_request_ids.is_empty() {
        return;
    }

    warn!(
        "Periodic cleanup: Removing {} stale request buffer(s) older than 5 minutes",
        old_request_ids.len()
    );

    for request_id in &old_request_ids {
        cleanup_request_processing_state(request_id);
    }
}

/// Route agent payload to the currently active request's buffer
/// Agent payloads come from named pipe without request_id, so we route to the active request
/// This is the same logic as 2.4.1 which worked correctly
pub async fn route_payload_to_request_buffer(payload_bytes: Vec<u8>) {
    use tracing::{debug, error, info, warn};

    let current_request_id = CURRENT_ACTIVE_REQUEST_ID
        .lock()
        .ok()
        .and_then(|guard| guard.clone());

    if let Some(request_id) = current_request_id {
        // Route to active request buffer
        if let Some(request_buffer) = REQUEST_AGENT_BUFFERS.get(&request_id) {
            match request_buffer.lock() {
                Ok(mut buffer) => {
                    buffer.push(payload_bytes);
                    debug!(
                        "Stored agent payload in request buffer for {} (buffer size: {})",
                        request_id, buffer.len()
                    );

                    // Signal coordination channel that payload arrived
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
        // No active request - try to route to any existing buffer (late payload scenario)
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
                        "Stored late agent payload in buffer for {} (buffer size: {})",
                        request_id, buffer.len()
                    );
                }
            }
        } else {
            // No requests exist yet - store in orphaned buffer
            // Will be drained into the first request's buffer in create_request_processing_state()
            if let Ok(mut orphaned) = ORPHANED_PAYLOADS.lock() {
                orphaned.push(payload_bytes);
                info!(
                    "No active requests - stored agent payload in orphaned buffer (size: {})",
                    orphaned.len()
                );
            } else {
                warn!("Failed to lock orphaned buffer - agent payload lost!");
            }
        }
    }
}

#[cfg(test)]
mod mod_tests;
