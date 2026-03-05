//! Request processing module
//!
//! Handles per-request data collection and routing for Lambda executions.
//! Uses CURRENT_ACTIVE_REQUEST_ID (2.4.1 approach) for agent payload routing.
//! Each request gets isolated processors with their own InvocationContext.

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use once_cell::sync::Lazy;
use dashmap::DashMap;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::{
    context::InvocationContext,
    platform::processor::PlatformProcessor,
    config::ExtensionConfig,
    newrelic::client::NewRelicClient,
    agent::payload::send_agent_payload_to_newrelic,
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

/// Per-request processing state stored in REQUEST_PROCESSORS (inserted by event_loop).
pub static REQUEST_PROCESSORS: Lazy<Arc<DashMap<String, RequestProcessingState>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

/// Consolidated per-request data — replaces 5 separate DashMaps
/// (context, agent_buffer, coordination_tx, pending_report, creation_invocation).
///
/// All fields are accessed via accessor functions below to maintain a clean API.
#[derive(Debug)]
pub struct RequestData {
    pub context: Arc<Mutex<InvocationContext>>,
    pub agent_buffer: Arc<Mutex<Vec<Vec<u8>>>>,
    pub coordination_tx: Option<mpsc::UnboundedSender<()>>,
    pub pending_report: Option<String>,
    pub creation_invocation: u64,
}

pub static REQUEST_DATA: Lazy<Arc<DashMap<String, RequestData>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

// ---------------------------------------------------------------------------
// Accessor functions — backward-compatible API for external modules
// ---------------------------------------------------------------------------

/// Get the invocation context for a request.
pub fn get_request_context(request_id: &str) -> Option<Arc<Mutex<InvocationContext>>> {
    REQUEST_DATA.get(request_id).map(|entry| entry.context.clone())
}

/// Get the agent payload buffer for a request.
pub fn get_agent_buffer(request_id: &str) -> Option<Arc<Mutex<Vec<Vec<u8>>>>> {
    REQUEST_DATA.get(request_id).map(|entry| entry.agent_buffer.clone())
}

/// Get the pending platform.report for a request.
pub fn get_pending_report(request_id: &str) -> Option<String> {
    REQUEST_DATA.get(request_id).and_then(|entry| entry.pending_report.clone())
}

/// Set the pending platform.report for a request.
pub fn set_pending_report(request_id: &str, report: String) {
    if let Some(mut entry) = REQUEST_DATA.get_mut(request_id) {
        entry.pending_report = Some(report);
    }
}

/// Remove and return the pending platform.report for a request.
pub fn remove_pending_report(request_id: &str) -> Option<String> {
    REQUEST_DATA.get_mut(request_id).and_then(|mut entry| entry.pending_report.take())
}

/// Get the number of entries in REQUEST_DATA (used for debug logging).
pub fn request_data_len() -> usize {
    REQUEST_DATA.len()
}

/// Global invocation counter — incremented on each Lambda INVOKE event.
/// Used to compute how many invocations a request buffer has survived without being processed.
static GLOBAL_INVOCATION_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Increment the global invocation counter and return the new value.
/// Called from the event loop on each INVOKE event.
pub fn increment_invocation_counter() -> u64 {
    GLOBAL_INVOCATION_COUNTER.fetch_add(1, Ordering::Relaxed) + 1
}

/// Get the current invocation counter value.
pub fn current_invocation_count() -> u64 {
    GLOBAL_INVOCATION_COUNTER.load(Ordering::Relaxed)
}

/// Reset the global invocation counter (for testing only).
#[cfg(test)]
pub fn reset_invocation_counter() {
    GLOBAL_INVOCATION_COUNTER.store(0, Ordering::Relaxed);
}

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

    // Insert consolidated request data (replaces 5 separate DashMap inserts)
    REQUEST_DATA.insert(request_id.to_string(), RequestData {
        context: context.clone(),
        agent_buffer: agent_buffer.clone(),
        coordination_tx: Some(payload_tx),
        pending_report: None,
        creation_invocation: current_invocation_count(),
    });

    let state = RequestProcessingState {
        context: context.clone(),
        log_processor: log_processor.clone(),
        platform_processor,
        agent_buffer: agent_buffer.clone(),
        coordination_rx: Some(payload_rx),
    };

    state
}

pub fn cleanup_request_processing_state(request_id: &str) {
    cleanup_request_processing_state_internal(request_id, false);
}

pub fn cleanup_request_processing_state_internal(request_id: &str, skip_buffer_cleanup: bool) {
    REQUEST_PROCESSORS.remove(request_id);

    if !skip_buffer_cleanup {
        // Full cleanup: remove the entire consolidated entry
        REQUEST_DATA.remove(request_id);
    } else {
        // Partial cleanup: keep context/buffer/creation_invocation, clear coordination + pending_report
        if let Some(mut entry) = REQUEST_DATA.get_mut(request_id) {
            entry.coordination_tx = None;
            entry.pending_report = None;
        }
    }
}

/// Periodic cleanup of stale request buffers that have survived more than 5 invocations
/// without being processed through the normal send path.
///
/// Stale buffers occur when agent payloads never get matched with platform.report
/// (e.g., missed events, Lambda execution anomalies).
///
/// Unlike the old time-based approach (5 minutes), invocation-count-based cleanup is safe
/// for long-running Lambda functions (up to 15 minutes) — a buffer won't be cleaned up
/// while its invocation is still active.
///
/// **Sends agent payloads to New Relic before removing** to prevent data loss.
pub async fn cleanup_old_request_buffers(
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
) {
    let current = current_invocation_count();
    let threshold: u64 = 5;

    let stale_request_ids: Vec<String> = REQUEST_DATA
        .iter()
        .filter(|entry| current.saturating_sub(entry.creation_invocation) >= threshold)
        .map(|entry| entry.key().clone())
        .collect();

    if stale_request_ids.is_empty() {
        return;
    }

    warn!(
        "Periodic cleanup: Found {} stale request buffer(s) (older than {} invocations) — sending before cleanup",
        stale_request_ids.len(),
        threshold
    );

    for request_id in &stale_request_ids {
        // Extract and send any unsent agent payloads before removing
        let (payloads, arn) = if let Some(entry) = REQUEST_DATA.get(request_id) {
            let payloads: Vec<Vec<u8>> = match entry.agent_buffer.lock() {
                Ok(mut buf) => buf.drain(..).collect(),
                Err(_) => Vec::new(),
            };
            let arn = entry.context.lock()
                .ok()
                .map(|c| c.invoked_function_arn.clone())
                .unwrap_or_else(|| format!("arn:aws:lambda:unknown:unknown:function:{}", config.aws.function_name));
            (payloads, arn)
        } else {
            (Vec::new(), format!("arn:aws:lambda:unknown:unknown:function:{}", config.aws.function_name))
        };

        if !payloads.is_empty() {
            info!(
                "Periodic cleanup: Sending {} stale agent payload(s) for request {} before removal",
                payloads.len(),
                request_id
            );

            for payload_bytes in &payloads {
                if let Err(e) = send_agent_payload_to_newrelic(
                    payload_bytes,
                    request_id,
                    &arn,
                    &newrelic_client,
                    &config,
                    None,
                )
                .await
                {
                    error!(
                        "Periodic cleanup: Failed to send stale agent payload for {}: {}",
                        request_id, e
                    );
                }
            }
        }

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
        if let Some(entry) = REQUEST_DATA.get(&request_id) {
            match entry.agent_buffer.lock() {
                Ok(mut buffer) => {
                    buffer.push(payload_bytes);
                    debug!(
                        "Stored agent payload in request buffer for {} (buffer size: {})",
                        request_id, buffer.len()
                    );

                    // Signal coordination channel that payload arrived
                    if let Some(ref tx) = entry.coordination_tx {
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
        let any_request_id = REQUEST_DATA.iter().next().map(|entry| entry.key().clone());

        if let Some(request_id) = any_request_id {
            warn!(
                "No active request - routing late agent payload to buffer: {}",
                request_id
            );
            if let Some(entry) = REQUEST_DATA.get(&request_id) {
                if let Ok(mut buffer) = entry.agent_buffer.lock() {
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
