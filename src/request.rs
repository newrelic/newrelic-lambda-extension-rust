

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
    #[allow(dead_code)]
    pub runtime_done_rx: Option<mpsc::UnboundedReceiver<()>>,
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

/// Buffer for orphaned agent payloads that arrive before any request is created
pub static ORPHANED_PAYLOADS: Lazy<Arc<Mutex<Vec<Vec<u8>>>>> =
    Lazy::new(|| Arc::new(Mutex::new(Vec::new())));

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

    let log_processor = processor_factory.create_log_processor(context.clone());
    let platform_processor = processor_factory.create_platform_processor(context.clone(), log_processor.clone());

    let agent_buffer = Arc::new(Mutex::new(Vec::new()));

    let (payload_tx, payload_rx) = mpsc::unbounded_channel();
    PAYLOAD_COORDINATION.insert(request_id.to_string(), payload_tx);

    // Only create runtime.done channel for standard mode (not needed in APM mode)
    let runtime_done_rx = if !is_apm_mode {
        let (runtime_done_tx, runtime_done_rx) = mpsc::unbounded_channel();
        RUNTIME_DONE_CHANNELS.insert(request_id.to_string(), runtime_done_tx);
        Some(runtime_done_rx)
    } else {
        None
    };

    let state = RequestProcessingState {
        context: context.clone(),
        platform_processor,
        agent_buffer: agent_buffer.clone(),
        coordination_rx: Some(payload_rx),
        runtime_done_rx,
    };

    REQUEST_CONTEXTS.insert(request_id.to_string(), context);
    REQUEST_AGENT_BUFFERS.insert(request_id.to_string(), agent_buffer.clone());
    REQUEST_BUFFER_TIMESTAMPS.insert(request_id.to_string(), chrono::Utc::now());

    // Move any orphaned payloads to this request buffer
    if let Ok(mut orphaned) = ORPHANED_PAYLOADS.lock() {
        if !orphaned.is_empty() {
            if let Ok(mut buffer) = agent_buffer.lock() {
                let count = orphaned.len();
                buffer.extend(orphaned.drain(..));
                debug!(
                    "Moved {} orphaned agent payload(s) to request buffer for {}",
                    count, request_id
                );
            }
        }
    }

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
            // No requests exist yet - store in orphaned buffer
            if let Ok(mut orphaned) = ORPHANED_PAYLOADS.lock() {
                orphaned.push(payload_bytes);
                debug!(
                    "No active requests - stored agent payload in orphaned buffer (total orphaned: {})",
                    orphaned.len()
                );
            } else {
                warn!("Failed to lock orphaned buffer - agent payload lost!");
            }
        }
    }
}

pub async fn wait_for_all_requests_completion(
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<crate::logs::processor::LogProcessor>,
    shutdown_start_time: std::time::Instant,
) {
    let pending_count = REQUEST_PROCESSORS.len();

    if pending_count == 0 {
        debug!("No pending requests at shutdown - proceeding immediately");
    } else {
        debug!(
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

        debug!("All requests completed");
    }

    // Send all pending telemetry and logs in parallel with 1MB chunking
    debug!("Shutdown: Flushing all pending telemetry and logs...");

    use crate::agent::batch::DEFAULT_BATCH_BUFFER;

    let telemetry_flush = tokio::spawn({
        let client = newrelic_client.clone();
        let cfg = config.clone();
        async move {
            DEFAULT_BATCH_BUFFER.send_all_pending_payloads_on_shutdown(client, cfg).await;
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

    let shutdown_duration = shutdown_start_time.elapsed();
    debug!("Shutdown: All pending data flushed");
    debug!("[NR_EXT] Shutdown completed - Duration: {}ms", shutdown_duration.as_millis());
}

/// Cleanup old request buffers by sending their payloads to New Relic first
/// Finds buffers older than 5 minutes, sends the payloads, then removes them
pub async fn cleanup_old_request_buffers(
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
) {
    use crate::agent::batch::BatchedAgentPayload;

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

    debug!("Periodic cleanup: Found {} old request buffers to send and remove", old_request_ids.len());

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
                        agent_payload_bytes: Arc::from(payload_bytes),
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
        debug!("Periodic cleanup: Sending {} payloads from old request buffers", all_payloads.len());

        let payload_json = crate::agent::batch::build_newrelic_payload(&all_payloads, &config, None);

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

    debug!("Periodic cleanup: Removed {} old request buffers", old_request_ids.len());
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn clear_all_global_state() {
        REQUEST_PROCESSORS.clear();
        REQUEST_CONTEXTS.clear();
        REQUEST_AGENT_BUFFERS.clear();
        PAYLOAD_COORDINATION.clear();
        RUNTIME_DONE_CHANNELS.clear();
        PENDING_REPORTS.clear();
        REQUEST_BUFFER_TIMESTAMPS.clear();
        if let Ok(mut active) = CURRENT_ACTIVE_REQUEST_ID.lock() {
            *active = None;
        }
        if let Ok(mut orphaned) = ORPHANED_PAYLOADS.lock() {
            orphaned.clear();
        }
    }

    // ========================================================================
    // cleanup_request_processing_state
    // ========================================================================

    #[test]
    #[serial]
    fn test_cleanup_removes_all_maps() {
        clear_all_global_state();
        let req_id = "req-cleanup-all";

        // Manually populate all maps
        REQUEST_CONTEXTS.insert(req_id.to_string(), Arc::new(Mutex::new(InvocationContext::default())));
        REQUEST_AGENT_BUFFERS.insert(req_id.to_string(), Arc::new(Mutex::new(Vec::new())));
        REQUEST_BUFFER_TIMESTAMPS.insert(req_id.to_string(), chrono::Utc::now());
        let (tx, _rx) = mpsc::unbounded_channel();
        PAYLOAD_COORDINATION.insert(req_id.to_string(), tx);
        let (rt_tx, _rt_rx) = mpsc::unbounded_channel();
        RUNTIME_DONE_CHANNELS.insert(req_id.to_string(), rt_tx);
        PENDING_REPORTS.insert(req_id.to_string(), "REPORT line".to_string());

        cleanup_request_processing_state(req_id);

        assert!(!REQUEST_CONTEXTS.contains_key(req_id));
        assert!(!REQUEST_AGENT_BUFFERS.contains_key(req_id));
        assert!(!REQUEST_BUFFER_TIMESTAMPS.contains_key(req_id));
        assert!(!PAYLOAD_COORDINATION.contains_key(req_id));
        assert!(!RUNTIME_DONE_CHANNELS.contains_key(req_id));
        assert!(!PENDING_REPORTS.contains_key(req_id));
        clear_all_global_state();
    }

    #[test]
    #[serial]
    fn test_cleanup_skip_buffer_preserves_buffers() {
        clear_all_global_state();
        let req_id = "req-skip-buffer";

        REQUEST_CONTEXTS.insert(req_id.to_string(), Arc::new(Mutex::new(InvocationContext::default())));
        REQUEST_AGENT_BUFFERS.insert(req_id.to_string(), Arc::new(Mutex::new(Vec::new())));
        REQUEST_BUFFER_TIMESTAMPS.insert(req_id.to_string(), chrono::Utc::now());
        let (tx, _rx) = mpsc::unbounded_channel();
        PAYLOAD_COORDINATION.insert(req_id.to_string(), tx);
        let (rt_tx, _rt_rx) = mpsc::unbounded_channel();
        RUNTIME_DONE_CHANNELS.insert(req_id.to_string(), rt_tx);
        PENDING_REPORTS.insert(req_id.to_string(), "report".to_string());

        cleanup_request_processing_state_internal(req_id, true);

        // Buffers and contexts should be preserved
        assert!(REQUEST_CONTEXTS.contains_key(req_id));
        assert!(REQUEST_AGENT_BUFFERS.contains_key(req_id));
        assert!(REQUEST_BUFFER_TIMESTAMPS.contains_key(req_id));
        assert!(PAYLOAD_COORDINATION.contains_key(req_id));

        // Runtime done and pending reports always cleaned
        assert!(!RUNTIME_DONE_CHANNELS.contains_key(req_id));
        assert!(!PENDING_REPORTS.contains_key(req_id));
        clear_all_global_state();
    }

    #[test]
    #[serial]
    fn test_cleanup_no_skip_removes_everything() {
        clear_all_global_state();
        let req_id = "req-no-skip";

        REQUEST_CONTEXTS.insert(req_id.to_string(), Arc::new(Mutex::new(InvocationContext::default())));
        REQUEST_AGENT_BUFFERS.insert(req_id.to_string(), Arc::new(Mutex::new(Vec::new())));
        let (rt_tx, _rt_rx) = mpsc::unbounded_channel();
        RUNTIME_DONE_CHANNELS.insert(req_id.to_string(), rt_tx);

        cleanup_request_processing_state_internal(req_id, false);

        assert!(!REQUEST_CONTEXTS.contains_key(req_id));
        assert!(!REQUEST_AGENT_BUFFERS.contains_key(req_id));
        assert!(!RUNTIME_DONE_CHANNELS.contains_key(req_id));
        clear_all_global_state();
    }

    #[test]
    #[serial]
    fn test_cleanup_nonexistent_request_noop() {
        clear_all_global_state();
        // Should not panic
        cleanup_request_processing_state("nonexistent-request-id");
        cleanup_request_processing_state_internal("another-nonexistent", true);
        cleanup_request_processing_state_internal("yet-another", false);
        clear_all_global_state();
    }

    // ========================================================================
    // route_payload_to_request_buffer
    // ========================================================================

    #[tokio::test]
    #[serial]
    async fn test_route_to_active_request_buffer() {
        clear_all_global_state();
        let req_id = "req-active";

        let buffer = Arc::new(Mutex::new(Vec::new()));
        REQUEST_AGENT_BUFFERS.insert(req_id.to_string(), buffer.clone());
        let (tx, _rx) = mpsc::unbounded_channel();
        PAYLOAD_COORDINATION.insert(req_id.to_string(), tx);
        if let Ok(mut active) = CURRENT_ACTIVE_REQUEST_ID.lock() {
            *active = Some(req_id.to_string());
        }

        route_payload_to_request_buffer(vec![1, 2, 3]).await;

        let guard = buffer.lock().expect("should lock");
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0], vec![1, 2, 3]);
        drop(guard);
        clear_all_global_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_route_no_active_routes_to_any_buffer() {
        clear_all_global_state();
        let req_id = "req-fallback";

        let buffer = Arc::new(Mutex::new(Vec::new()));
        REQUEST_AGENT_BUFFERS.insert(req_id.to_string(), buffer.clone());

        // No CURRENT_ACTIVE_REQUEST_ID set
        route_payload_to_request_buffer(vec![4, 5, 6]).await;

        let guard = buffer.lock().expect("should lock");
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0], vec![4, 5, 6]);
        drop(guard);
        clear_all_global_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_route_no_buffers_routes_to_orphaned() {
        clear_all_global_state();
        // No active request, no buffers
        route_payload_to_request_buffer(vec![7, 8, 9]).await;

        let guard = ORPHANED_PAYLOADS.lock().expect("should lock");
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0], vec![7, 8, 9]);
        drop(guard);
        clear_all_global_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_route_active_no_matching_buffer() {
        clear_all_global_state();
        // Set active request but NO matching buffer
        if let Ok(mut active) = CURRENT_ACTIVE_REQUEST_ID.lock() {
            *active = Some("ghost-request".to_string());
        }

        // Should warn but not panic (payload is lost)
        route_payload_to_request_buffer(vec![10, 11]).await;
        clear_all_global_state();
    }

    #[tokio::test]
    #[serial]
    async fn test_route_coordination_signal_sent() {
        clear_all_global_state();
        let req_id = "req-signal";

        let buffer = Arc::new(Mutex::new(Vec::new()));
        REQUEST_AGENT_BUFFERS.insert(req_id.to_string(), buffer);
        let (tx, mut rx) = mpsc::unbounded_channel();
        PAYLOAD_COORDINATION.insert(req_id.to_string(), tx);
        if let Ok(mut active) = CURRENT_ACTIVE_REQUEST_ID.lock() {
            *active = Some(req_id.to_string());
        }

        route_payload_to_request_buffer(vec![1]).await;

        // Coordination signal should have been sent
        let signal = rx.try_recv();
        assert!(signal.is_ok(), "Should have received coordination signal");
        clear_all_global_state();
    }

    // ========================================================================
    // Orphaned payloads
    // ========================================================================

    #[test]
    #[serial]
    fn test_orphaned_payloads_moved_on_create() {
        clear_all_global_state();

        // Pre-populate orphaned payloads
        if let Ok(mut orphaned) = ORPHANED_PAYLOADS.lock() {
            orphaned.push(vec![10, 20]);
            orphaned.push(vec![30, 40]);
        }

        // Simulate the orphan-moving logic from create_request_processing_state
        let req_id = "req-new";
        let agent_buffer = Arc::new(Mutex::new(Vec::new()));
        REQUEST_AGENT_BUFFERS.insert(req_id.to_string(), agent_buffer.clone());

        if let Ok(mut orphaned) = ORPHANED_PAYLOADS.lock() {
            if !orphaned.is_empty() {
                if let Ok(mut buffer) = agent_buffer.lock() {
                    buffer.extend(orphaned.drain(..));
                }
            }
        }

        // Verify orphans moved to buffer
        let guard = agent_buffer.lock().expect("should lock");
        assert_eq!(guard.len(), 2);
        assert_eq!(guard[0], vec![10, 20]);
        assert_eq!(guard[1], vec![30, 40]);
        drop(guard);

        // Verify orphan buffer is now empty
        let orphaned = ORPHANED_PAYLOADS.lock().expect("should lock");
        assert!(orphaned.is_empty());
        drop(orphaned);
        clear_all_global_state();
    }

    // ========================================================================
    // Multiple sequential cleanups
    // ========================================================================

    #[test]
    #[serial]
    fn test_multiple_sequential_cleanups() {
        clear_all_global_state();

        for i in 0..3 {
            let req_id = format!("req-seq-{i}");
            REQUEST_CONTEXTS.insert(req_id.clone(), Arc::new(Mutex::new(InvocationContext::default())));
            REQUEST_AGENT_BUFFERS.insert(req_id.clone(), Arc::new(Mutex::new(Vec::new())));
        }

        assert_eq!(REQUEST_CONTEXTS.len(), 3);
        assert_eq!(REQUEST_AGENT_BUFFERS.len(), 3);

        for i in 0..3 {
            cleanup_request_processing_state(&format!("req-seq-{i}"));
        }

        assert_eq!(REQUEST_CONTEXTS.len(), 0);
        assert_eq!(REQUEST_AGENT_BUFFERS.len(), 0);
        clear_all_global_state();
    }

    // ========================================================================
    // Concurrent routing (no panic / deadlock)
    // ========================================================================

    #[tokio::test]
    #[serial]
    async fn test_concurrent_route_no_panic() {
        clear_all_global_state();
        let req_id = "req-concurrent";

        let buffer = Arc::new(Mutex::new(Vec::new()));
        REQUEST_AGENT_BUFFERS.insert(req_id.to_string(), buffer.clone());
        let (tx, _rx) = mpsc::unbounded_channel();
        PAYLOAD_COORDINATION.insert(req_id.to_string(), tx);
        if let Ok(mut active) = CURRENT_ACTIVE_REQUEST_ID.lock() {
            *active = Some(req_id.to_string());
        }

        let mut handles = Vec::new();
        for i in 0..10u8 {
            handles.push(tokio::spawn(async move {
                route_payload_to_request_buffer(vec![i]).await;
            }));
        }

        for handle in handles {
            handle.await.expect("task should not panic");
        }

        let guard = buffer.lock().expect("should lock");
        assert_eq!(guard.len(), 10, "All 10 payloads should be routed");
        drop(guard);
        clear_all_global_state();
    }

    // ========================================================================
    // Buffer timestamp filtering
    // ========================================================================

    #[test]
    #[serial]
    fn test_old_buffer_timestamp_filtering() {
        clear_all_global_state();
        let old_req = "req-old";
        REQUEST_BUFFER_TIMESTAMPS.insert(
            old_req.to_string(),
            chrono::Utc::now() - chrono::Duration::minutes(10),
        );

        let now = chrono::Utc::now();
        let threshold = chrono::Duration::minutes(5);
        let old_ids: Vec<String> = REQUEST_BUFFER_TIMESTAMPS
            .iter()
            .filter(|entry| now.signed_duration_since(*entry.value()) >= threshold)
            .map(|entry| entry.key().clone())
            .collect();

        assert_eq!(old_ids.len(), 1);
        assert_eq!(old_ids[0], old_req);
        clear_all_global_state();
    }

    #[test]
    #[serial]
    fn test_recent_buffer_survives_cleanup() {
        clear_all_global_state();
        let recent_req = "req-recent";
        REQUEST_BUFFER_TIMESTAMPS.insert(
            recent_req.to_string(),
            chrono::Utc::now() - chrono::Duration::minutes(2),
        );

        let now = chrono::Utc::now();
        let threshold = chrono::Duration::minutes(5);
        let old_ids: Vec<String> = REQUEST_BUFFER_TIMESTAMPS
            .iter()
            .filter(|entry| now.signed_duration_since(*entry.value()) >= threshold)
            .map(|entry| entry.key().clone())
            .collect();

        assert!(old_ids.is_empty(), "Recent buffer should not be flagged as old");
        clear_all_global_state();
    }

    // ========================================================================
    // ProcessorFactory
    // ========================================================================

    #[test]
    fn test_processor_factory_new() {
        let config = Arc::new(crate::config::ExtensionConfig::default());
        let client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));

        let factory = ProcessorFactory::new(client.clone(), config.clone(), apm_app.clone());

        // Verify fields are stored (via Debug output)
        let debug_str = format!("{factory:?}");
        assert!(debug_str.contains("ProcessorFactory"));
    }

    #[test]
    fn test_request_processing_state_debug() {
        let state = RequestProcessingState {
            context: Arc::new(Mutex::new(InvocationContext::default())),
            platform_processor: {
                let config = Arc::new(crate::config::ExtensionConfig::default());
                let client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
                let apm_app = Arc::new(tokio::sync::RwLock::new(None));
                let factory = ProcessorFactory::new(client, config, apm_app);
                let ctx = Arc::new(Mutex::new(InvocationContext::default()));
                let log_proc = factory.create_log_processor(ctx.clone());
                factory.create_platform_processor(ctx, log_proc)
            },
            agent_buffer: Arc::new(Mutex::new(Vec::new())),
            coordination_rx: None,
            runtime_done_rx: None,
        };
        let debug_str = format!("{state:?}");
        assert!(debug_str.contains("RequestProcessingState"));
    }

    // ========================================================================
    // create_request_processing_state — full flow
    // ========================================================================

    fn make_factory() -> Arc<ProcessorFactory> {
        let config = Arc::new(crate::config::ExtensionConfig::default());
        let client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));
        Arc::new(ProcessorFactory::new(client, config, apm_app))
    }

    #[test]
    #[serial]
    fn test_create_request_processing_state_standard_mode() {
        clear_all_global_state();
        let factory = make_factory();

        let state = create_request_processing_state(
            "req-std-001",
            "arn:aws:lambda:us-east-1:123:function:fn",
            &factory,
            false, // standard mode
        );

        // Verify context set
        let ctx = state.context.lock().expect("lock");
        assert_eq!(ctx.request_id, "req-std-001");
        assert_eq!(ctx.invoked_function_arn, "arn:aws:lambda:us-east-1:123:function:fn");
        drop(ctx);

        // Verify buffer created in global maps
        assert!(REQUEST_CONTEXTS.contains_key("req-std-001"));
        assert!(REQUEST_AGENT_BUFFERS.contains_key("req-std-001"));
        assert!(REQUEST_BUFFER_TIMESTAMPS.contains_key("req-std-001"));
        assert!(PAYLOAD_COORDINATION.contains_key("req-std-001"));

        // Standard mode should create runtime_done channel
        assert!(state.runtime_done_rx.is_some(), "Standard mode should have runtime_done channel");
        assert!(RUNTIME_DONE_CHANNELS.contains_key("req-std-001"));

        // Verify coordination channel exists
        assert!(state.coordination_rx.is_some());

        clear_all_global_state();
    }

    #[test]
    #[serial]
    fn test_create_request_processing_state_apm_mode() {
        clear_all_global_state();
        let factory = make_factory();

        let state = create_request_processing_state(
            "req-apm-001",
            "arn:aws:lambda:us-east-1:123:function:fn",
            &factory,
            true, // APM mode
        );

        // APM mode should NOT create runtime_done channel
        assert!(state.runtime_done_rx.is_none(), "APM mode should NOT have runtime_done channel");
        assert!(!RUNTIME_DONE_CHANNELS.contains_key("req-apm-001"));

        // But should still have coordination channel
        assert!(state.coordination_rx.is_some());
        assert!(PAYLOAD_COORDINATION.contains_key("req-apm-001"));

        clear_all_global_state();
    }

    #[test]
    #[serial]
    fn test_create_request_processing_state_moves_orphaned_payloads() {
        clear_all_global_state();
        let factory = make_factory();

        // Pre-populate orphaned payloads
        if let Ok(mut orphaned) = ORPHANED_PAYLOADS.lock() {
            orphaned.push(vec![1, 2, 3]);
            orphaned.push(vec![4, 5, 6]);
        }

        let state = create_request_processing_state(
            "req-orphan-test",
            "arn:test",
            &factory,
            false,
        );

        // Orphans should be moved to the new request's buffer
        let buf = state.agent_buffer.lock().expect("lock");
        assert_eq!(buf.len(), 2, "Should have 2 orphaned payloads");
        assert_eq!(buf[0], vec![1, 2, 3]);
        assert_eq!(buf[1], vec![4, 5, 6]);
        drop(buf);

        // Orphan buffer should be empty
        let orphaned = ORPHANED_PAYLOADS.lock().expect("lock");
        assert!(orphaned.is_empty());
        drop(orphaned);

        clear_all_global_state();
    }

    #[test]
    #[serial]
    fn test_create_request_processing_state_empty_orphans_noop() {
        clear_all_global_state();
        let factory = make_factory();

        let state = create_request_processing_state("req-no-orphans", "arn:test", &factory, false);

        let buf = state.agent_buffer.lock().expect("lock");
        assert!(buf.is_empty(), "No orphans should mean empty buffer");
        drop(buf);

        clear_all_global_state();
    }
}
