// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0


use tracing::{debug, error, info, trace, warn};
use crate::{
    config::ExtensionConfig,
    context::InvocationContext,
    newrelic::{client::NewRelicClient, flush::Flush, payload},
    telemetry::listener::TelemetryRecord,
};
use async_trait::async_trait;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

/// Recursively estimate the JSON byte size of a serde_json Value without allocating.
fn estimate_json_value_size(v: &serde_json::Value) -> usize {
    match v {
        serde_json::Value::String(s) => s.len() + 2, // surrounding quotes
        serde_json::Value::Number(n) => n.to_string().len(),
        serde_json::Value::Bool(b) => if *b { 4 } else { 5 },
        serde_json::Value::Null => 4,
        serde_json::Value::Array(arr) => {
            let inner: usize = arr.iter().map(estimate_json_value_size).sum();
            2 + inner + arr.len().saturating_sub(1) // [] + commas
        }
        serde_json::Value::Object(obj) => {
            // Each key-value pair: "key": value
            let inner: usize = obj.iter()
                .map(|(k, v)| k.len() + 4 + estimate_json_value_size(v)) // 4 = 2 quotes + colon + space
                .sum();
            2 + inner + obj.len().saturating_sub(1) // {} + commas
        }
    }
}

/// Estimate a log message's serialized JSON size in bytes without allocating.
/// Structural traversal replaces serde_json::to_string so no heap allocation occurs.
fn estimate_log_size(log: &payload::LogMessage) -> usize {
    const PER_LOG_OVERHEAD: usize = 8;
    let attrs_size: usize = log.attributes.iter()
        .map(|(k, v)| k.len() + 4 + estimate_json_value_size(v))
        .sum::<usize>()
        + log.attributes.len().saturating_sub(1) // commas between pairs
        + 2; // surrounding {}
    PER_LOG_OVERHEAD + log.message.len() + attrs_size
}

/// Counts how often `start_invocation_retry` was called while a prior retry task
/// was still in flight (i.e. `flush()` was not awaited between invocations). The
/// new call lets the previous task finish in the background instead of aborting
/// it — this counter makes the invariant violation observable without losing logs.
/// Tests in the child `processor_tests` module read/reset it directly.
static RETRY_INVARIANT_VIOLATIONS: AtomicU64 = AtomicU64::new(0);

use crate::apm::app::ApmApp;

/// Safe mutex operations that won't panic and allow graceful degradation.
///
/// Convention for this file:
///   - Use `safe_lock()` on mutexes that guard *external-facing state* (invocation context,
///     invocation start time) where a poisoned lock should degrade gracefully rather than crash.
///   - Use `.unwrap()` on mutexes that guard *internal pipeline state* (log_batch,
///     failed_logs_buffer, pending_flush_handles, etc.) where poisoning indicates a bug
///     that has already corrupted the pipeline — panicking is the safer choice.
trait SafeMutexOps<T> {
    fn safe_lock(&self) -> Option<std::sync::MutexGuard<'_, T>>;
}

impl<T> SafeMutexOps<T> for Mutex<T> {
    fn safe_lock(&self) -> Option<std::sync::MutexGuard<'_, T>> {
        match self.lock() {
            Ok(guard) => Some(guard),
            Err(e) => {
                error!("Mutex poisoned (extension will continue in degraded mode): {}", e);
                None
            }
        }
    }
}

/// State of trace ID extraction for the current invocation
#[derive(Debug, Clone, PartialEq)]
enum TraceIdExtractionState {

    Waiting,

    Extracted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogType {
    Function,
    Platform,
    Extension,
}

impl LogType {
    fn from_record_type(s: &str) -> Self {
        match s {
            "platform"  => Self::Platform,
            "extension" => Self::Extension,
            _           => Self::Function,
        }
    }
}

/// The LogProcessor is responsible for handling and transforming function and extension logs.
#[derive(Debug, Clone)]
pub struct LogProcessor {
    log_batch: Arc<Mutex<Vec<payload::LogMessage>>>,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
    invocation_context: Arc<Mutex<InvocationContext>>,

    buffered_logs: Option<Arc<Mutex<Vec<payload::LogMessage>>>>,

    trace_extraction_state: Option<Arc<Mutex<TraceIdExtractionState>>>,

    request_id_buffer: Arc<Mutex<Vec<payload::LogMessage>>>,

    invocation_start_time: Arc<Mutex<chrono::DateTime<chrono::Utc>>>,

    apm_app: Option<Arc<tokio::sync::RwLock<Option<ApmApp>>>>,
    failed_logs_buffer: Arc<Mutex<FailedBuffer>>,

    /// Track pending auto-flush tasks to ensure they complete before function ends
    pending_flush_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,

    /// Buffer for logs received during INIT phase before first INVOKE event
    pre_invoke_buffer: Arc<Mutex<Vec<payload::LogMessage>>>,

    /// Fallback ARN constructed from registration response (function_name + account_id + AWS_REGION)
    fallback_function_arn: Arc<Mutex<Option<String>>>,

    is_auto_flushing: Arc<Mutex<bool>>,

    /// Handle for the start-of-invocation retry task; awaited in flush() before GET /next
    invocation_retry_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,

    /// Fired when the processor transitions to fully drained (batch empty, no pending handles).
    /// Replaces the 2ms poll loop in wait_for_runtime_done_with_grace with a zero-overhead wait.
    drain_notify: Arc<tokio::sync::Notify>,
}

#[derive(Debug, Clone)]
struct FailedLogEntry {
    log_message: payload::LogMessage,
    original_request_id: String,
    retry_count: usize,
    log_type: LogType,
}

/// Configuration constants for batching and retry logic
const MAX_RETRIES: usize = 3;
/// Per-LogType capacity. Each of Function/Platform/Extension has its own queue,
/// so the overall cap equals `MAX_FAILED_BUFFER_PER_TYPE * 3 = 300` — preserving
/// the original total. With per-type queues, eviction is O(1) (pop_front on the
/// type's own VecDeque) instead of O(n) scan across a shared buffer.
const MAX_FAILED_BUFFER_PER_TYPE: usize = 100;
const MAX_TRACE_BUFFER: usize = 200;

/// Per-LogType failed-send retry queues. Splitting by type gives O(1) eviction
/// on overflow (FIFO per queue) and guarantees that a flood of one type's
/// failures can't crowd out the others — Function failures never evict
/// Platform or Extension failures, and vice versa.
#[derive(Debug)]
struct FailedBuffer {
    function: VecDeque<FailedLogEntry>,
    platform: VecDeque<FailedLogEntry>,
    extension: VecDeque<FailedLogEntry>,
}

impl FailedBuffer {
    fn new() -> Self {
        Self {
            function: VecDeque::new(),
            platform: VecDeque::new(),
            extension: VecDeque::new(),
        }
    }

    fn len(&self) -> usize {
        self.function.len() + self.platform.len() + self.extension.len()
    }

    fn is_empty(&self) -> bool {
        self.function.is_empty() && self.platform.is_empty() && self.extension.is_empty()
    }

    /// Push a new entry. On overflow, FIFO-evict the oldest entry of the same
    /// type (O(1)). Operators see a warn once per eviction so runaway failures
    /// remain visible.
    fn push(&mut self, entry: FailedLogEntry) {
        let log_type = entry.log_type;
        let queue = match log_type {
            LogType::Function => &mut self.function,
            LogType::Platform => &mut self.platform,
            LogType::Extension => &mut self.extension,
        };
        if queue.len() >= MAX_FAILED_BUFFER_PER_TYPE {
            queue.pop_front();
            warn!(
                "failed_buffer[{:?}] at capacity ({}) — evicting oldest (FIFO)",
                log_type, MAX_FAILED_BUFFER_PER_TYPE
            );
        }
        queue.push_back(entry);
    }

    /// Drain all entries, returning `(high_priority, low_priority)` matching the
    /// existing `start_invocation_retry` partition: Function + Platform first,
    /// then Extension.
    fn drain_partitioned(&mut self) -> (Vec<FailedLogEntry>, Vec<FailedLogEntry>) {
        let mut high: Vec<FailedLogEntry> =
            Vec::with_capacity(self.function.len() + self.platform.len());
        high.extend(self.function.drain(..));
        high.extend(self.platform.drain(..));
        let low: Vec<FailedLogEntry> = self.extension.drain(..).collect();
        (high, low)
    }

    /// Iterate all entries in priority order (Function, Platform, Extension).
    #[cfg(test)]
    fn iter_all(&self) -> impl Iterator<Item = &FailedLogEntry> {
        self.function
            .iter()
            .chain(self.platform.iter())
            .chain(self.extension.iter())
    }

    /// First-queued entry of a specific type (for FIFO eviction tests).
    #[cfg(test)]
    fn front_of(&self, log_type: LogType) -> Option<&FailedLogEntry> {
        match log_type {
            LogType::Function => self.function.front(),
            LogType::Platform => self.platform.front(),
            LogType::Extension => self.extension.front(),
        }
    }

    /// Count entries of a specific type.
    #[cfg(test)]
    fn len_of(&self, log_type: LogType) -> usize {
        match log_type {
            LogType::Function => self.function.len(),
            LogType::Platform => self.platform.len(),
            LogType::Extension => self.extension.len(),
        }
    }
}

fn get_backoff_delay(retry_attempt: usize) -> Duration {
    match retry_attempt {
        1 => Duration::from_millis(200),
        2 => Duration::from_millis(400),
        _ => Duration::from_millis(900),
    }
}

/// Extract structured log level from JSON record
/// Returns the uppercase level string if found in common level field names
fn get_structured_log_level(record: &serde_json::Value) -> Option<String> {
    if let serde_json::Value::Object(obj) = record {
        // Check common level field names used by various logging frameworks
        for level_key in &["level", "Level", "LogLevel", "log_level", "severity", "Severity"] {
            if let Some(level_value) = obj.get(*level_key) {
                if let Some(level_str) = level_value.as_str() {
                    return Some(level_str.to_uppercase());
                }
            }
        }
    }
    None
}

impl LogProcessor {
    
   
    pub fn new(
        newrelic_client: Arc<NewRelicClient>,
        config: Arc<ExtensionConfig>,
        invocation_context: Arc<Mutex<InvocationContext>>,
        apm_app: Option<Arc<tokio::sync::RwLock<Option<ApmApp>>>>,
    ) -> Self {
        let (buffered_logs, trace_extraction_state) = if config.new_relic.collect_trace_id {
            (
                Some(Arc::new(Mutex::new(Vec::new()))),
                Some(Arc::new(Mutex::new(TraceIdExtractionState::Waiting))),
            )
        } else {
            (None, None)
        };

        Self {
            log_batch: Arc::new(Mutex::new(Vec::new())),
            newrelic_client,
            config,
            invocation_context,
            buffered_logs,
            trace_extraction_state,
            request_id_buffer: Arc::new(Mutex::new(Vec::new())),
            invocation_start_time: Arc::new(Mutex::new(chrono::Utc::now())),
            failed_logs_buffer: Arc::new(Mutex::new(FailedBuffer::new())),
            apm_app,
            pending_flush_handles: Arc::new(Mutex::new(Vec::new())),
            pre_invoke_buffer: Arc::new(Mutex::new(Vec::new())),
            fallback_function_arn: Arc::new(Mutex::new(None)),
            is_auto_flushing: Arc::new(Mutex::new(false)),
            invocation_retry_handle: Arc::new(Mutex::new(None)),
            drain_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Returns the drain notifier so the event loop can await it instead of polling.
    pub fn drain_notify(&self) -> Arc<tokio::sync::Notify> {
        self.drain_notify.clone()
    }

    /// Fires the drain notifier if the processor is fully drained.
    fn notify_if_drained(&self) {
        if self.is_drained() {
            self.drain_notify.notify_one();
        }
    }

   
    /// Add a log message directly to the batch (used by platform processor)
    pub fn add_log_to_batch(&self, log_message: payload::LogMessage) {
        if let Ok(mut batch) = self.log_batch.lock() {
            batch.push(log_message);
        }
    }

    /// Remove finished handles from `pending_flush_handles` so the vec doesn't grow
    /// unbounded over a long-lived warm container. Called from the auto-flush spawn
    /// path (before registering a new handle) and from `is_drained()`.
    fn reap_finished_flush_handles(&self) {
        if let Ok(mut handles) = self.pending_flush_handles.lock() {
            handles.retain(|handle| !handle.is_finished());
        }
    }

    /// If `log_batch` has reached the auto-flush threshold, drain it and spawn a
    /// background task that ships the drained logs in 1 MB chunks with per-chunk
    /// retry. Shared by `process_record` (per-log push) and by bulk-insert paths
    /// (`on_trace_id_extracted`, `reset_trace_id_state`) so trace-buffer drains
    /// don't silently let the batch grow past the threshold.
    ///
    /// No-op when another auto-flush is already in flight (`is_auto_flushing`
    /// mutex). Returns with the batch untouched when no ARN is available.
    fn try_spawn_auto_flush(&self) {
        /// Auto-flush threshold: 10 logs flushes more batches during function execution,
        /// leaving a smaller tail for the post-runtime-done flush (billed time).
        const FLUSH_THRESHOLD: usize = 25;

        let logs_to_send = {
            let mut batch = self.log_batch.lock().unwrap();
            if batch.len() < FLUSH_THRESHOLD {
                return;
            }
            let mut is_flushing = self.is_auto_flushing.lock().unwrap();
            if *is_flushing {
                debug!("Auto-flush already in progress - skipping to prevent infinite loop");
                return;
            }
            *is_flushing = true;
            std::mem::take(&mut *batch)
        };

        debug!(
            "Auto-flushing batch of {} logs (threshold={})",
            logs_to_send.len(),
            FLUSH_THRESHOLD
        );

        let client = Arc::clone(&self.newrelic_client);
        let config = Arc::clone(&self.config);
        let context = self.invocation_context.lock().unwrap().clone();
        let processor_clone = self.clone();

        let auto_flush_arn = if !context.invoked_function_arn.is_empty() {
            context.invoked_function_arn.clone()
        } else {
            let fallback = self.get_best_available_arn();
            if fallback.is_empty() {
                error!(
                    "BLOCKED: Auto-flush skipped - no ARN available (request_id: '{}', {} logs returned to batch)",
                    context.request_id,
                    logs_to_send.len()
                );
                if let Ok(mut batch) = self.log_batch.lock() {
                    batch.extend(logs_to_send);
                }
                if let Ok(mut is_flushing) = self.is_auto_flushing.lock() {
                    *is_flushing = false;
                }
                return;
            }
            fallback
        };

        let handle = tokio::spawn(async move {
            const MAX_PAYLOAD_SIZE: usize = 1_000_000;
            let mut chunks: Vec<Vec<payload::LogMessage>> = Vec::new();
            let mut current_chunk = Vec::new();
            let mut current_size = 0;

            for log in logs_to_send {
                let log_size = estimate_log_size(&log);
                if current_size + log_size > MAX_PAYLOAD_SIZE && !current_chunk.is_empty() {
                    chunks.push(std::mem::take(&mut current_chunk));
                    current_size = 0;
                }
                current_chunk.push(log);
                current_size += log_size;
            }
            if !current_chunk.is_empty() {
                chunks.push(current_chunk);
            }

            let mut successful = 0;
            for chunk in chunks {
                let mut retries = 0;
                loop {
                    match client.send_logs(&config, chunk.clone(), &auto_flush_arn).await {
                        Ok(()) => {
                            successful += 1;
                            break;
                        }
                        Err(_e) => {
                            if retries < MAX_RETRIES {
                                retries += 1;
                                tokio::time::sleep(get_backoff_delay(retries)).await;
                                continue;
                            } else {
                                warn!(
                                    "Auto-flush failed after {} retries - buffering {} logs",
                                    MAX_RETRIES,
                                    chunk.len()
                                );
                                for log in chunk {
                                    let lt = LogProcessor::log_type_from_message(&log);
                                    processor_clone.push_to_failed_buffer(FailedLogEntry {
                                        log_message: log,
                                        original_request_id: context.request_id.clone(),
                                        retry_count: 0,
                                        log_type: lt,
                                    });
                                }
                                break;
                            }
                        }
                    }
                }
            }
            if successful > 0 {
                debug!("Auto-flush sent {} chunk(s) successfully", successful);
            }
            // Signal any grace-period waiter that this auto-flush task finished.
            processor_clone.notify_if_drained();
        });

        self.reap_finished_flush_handles();
        if let Ok(mut handles) = self.pending_flush_handles.lock() {
            handles.push(handle);
        }
        if let Ok(mut is_flushing) = self.is_auto_flushing.lock() {
            *is_flushing = false;
        }
    }

    /// Current count of entries waiting in `failed_logs_buffer`. Used by the shutdown
    /// drain loop to decide whether another retry pass is worth running.
    pub fn failed_logs_buffer_len(&self) -> usize {
        self.failed_logs_buffer.lock().map(|b| b.len()).unwrap_or(0)
    }

    /// Shutdown-only flush that handles the "flush fails → entry re-queued → never
    /// retried" gap. Normal `flush()` awaits the retry handle set up by the last
    /// `start_invocation_retry()` call, but any chunk failures during that flush push
    /// entries back into `failed_logs_buffer`. On a normal invocation the next INVOKE
    /// picks them up; during SHUTDOWN there IS no next INVOKE, so those entries would
    /// be stranded.
    ///
    /// Loops `start_invocation_retry` + `flush` up to `MAX_RETRIES` extra times. The
    /// per-entry `retry_count` filter inside `start_invocation_retry` guarantees this
    /// terminates — entries that have already exceeded `MAX_RETRIES` are dropped with
    /// a warn from `push_to_failed_buffer` rather than retried forever.
    pub async fn flush_on_shutdown(&self) -> std::io::Result<()> {
        let first_result = self.flush().await;

        for attempt in 1..=MAX_RETRIES {
            let remaining = self.failed_logs_buffer_len();
            if remaining == 0 {
                return first_result;
            }
            debug!(
                "Shutdown drain: pass {} with {} log(s) still in failed_logs_buffer",
                attempt, remaining
            );
            self.start_invocation_retry();
            if let Err(e) = self.flush().await {
                warn!("Shutdown drain pass {} failed: {}", attempt, e);
                return Err(e);
            }
        }

        let final_remaining = self.failed_logs_buffer_len();
        if final_remaining > 0 {
            error!(
                "Shutdown: {} log(s) remained in failed_logs_buffer after {} retry passes — dropped",
                final_remaining, MAX_RETRIES
            );
        }
        first_result
    }

    /// Returns true when there is nothing pending to flush: no auto-flush is mid-spawn,
    /// log_batch is empty, and no auto-flush tasks are still in-flight.
    ///
    /// Gating on `is_auto_flushing` closes a TOCTOU window in the auto-flush spawn path:
    /// between `mem::take(log_batch)` and `pending_flush_handles.push(handle)` the batch
    /// is empty AND the handle isn't registered yet. Without this check the event loop
    /// would falsely conclude the batch is drained and skip the post-runtime.done grace
    /// period, dropping trailing logs that Lambda is still POSTing.
    ///
    /// Returns false on lock poisoning (conservative — prefer waiting the grace period
    /// over skipping it on an inconsistent state).
    pub fn is_drained(&self) -> bool {
        // Prune finished handles so the vec can't grow unbounded while we're checking.
        self.reap_finished_flush_handles();

        let not_flushing = self
            .is_auto_flushing
            .lock()
            .map(|f| !*f)
            .unwrap_or(false);
        let batch_empty = self.log_batch.lock().map(|b| b.is_empty()).unwrap_or(false);
        let no_pending = self
            .pending_flush_handles
            .lock()
            .map(|h| h.is_empty())
            .unwrap_or(false);
        not_flushing && batch_empty && no_pending
    }

   
    pub fn update_invocation_context(&self, new_context: Arc<Mutex<InvocationContext>>) {
        if let (Some(mut current), Some(new)) = (self.invocation_context.safe_lock(), new_context.safe_lock()) {
            current.request_id = new.request_id.clone();
            current.invoked_function_arn = new.invoked_function_arn.clone();
            current.trace_id = new.trace_id.clone();
        } else {
            warn!("Failed to update invocation context - mutex poisoned, extension continuing in degraded mode");
        }
    }
   
    pub fn set_invocation_start_time(&self, start_time: chrono::DateTime<chrono::Utc>) {
        if let Some(mut guard) = self.invocation_start_time.safe_lock() {
            *guard = start_time;
        } else {
            warn!("Failed to update invocation start time - mutex poisoned, extension continuing in degraded mode");
        }
    }

   
    pub fn apply_current_invocation_metadata(&self, mut log_message: payload::LogMessage) -> payload::LogMessage {
        if let Some(context) = self.invocation_context.safe_lock() {
            // REQUEST_ID: Prefer TELEMETRY_CURRENT_REQUEST_ID over invocation context.
            //
            // WHY: The event loop updates invocation context immediately when GET /next returns
            // with the NEW invoke, but telemetry API delivers function logs asynchronously.
            // Late logs from request_A can arrive AFTER the context has been updated to request_B.
            //
            // platform.start always arrives BEFORE function logs for that request in the telemetry
            // stream, so TELEMETRY_CURRENT_REQUEST_ID gives us the correct request_id association.
            // Falls back to invocation context if telemetry tracking hasn't started yet.
            let effective_request_id = crate::request::TELEMETRY_CURRENT_REQUEST_ID
                .lock()
                .ok()
                .and_then(|guard| guard.clone())
                .filter(|id| !id.is_empty())
                .unwrap_or_else(|| context.request_id.clone());

            if !effective_request_id.is_empty() && effective_request_id != "unknown" {
                let mut aws_attrs = serde_json::Map::new();
                aws_attrs.insert("lambda_request_id".to_string(),
                    serde_json::Value::String(effective_request_id.clone()));
                log_message.attributes.insert("aws".to_string(),
                    serde_json::Value::Object(aws_attrs));
                log_message.attributes.insert("faas.execution".to_string(),
                    serde_json::Value::String(effective_request_id));
            }

            // Always use best available ARN (prefer invoked_function_arn, fallback to global context ARN)
            let arn = if !context.invoked_function_arn.is_empty() {
                context.invoked_function_arn.clone()
            } else {
                // Use fallback ARN from LogProcessor or global context
                self.get_best_available_arn()
            };

            if !arn.is_empty() {
                log_message.attributes.insert("faas.arn".to_string(),
                    serde_json::Value::String(arn));
            }

            if let Some(ref trace_id) = context.trace_id {
                log_message.attributes.insert("trace.id".to_string(),
                    serde_json::Value::String(trace_id.clone()));
            }
        } else {
            warn!("Cannot apply invocation metadata - context mutex poisoned, log will be sent without metadata");
        }
        
        if let Some(ref apm_app_arc) = self.apm_app {
            if let Ok(apm_guard) = apm_app_arc.try_read() {
                if let Some(ref app) = *apm_guard {
                    let entity_guid = app.get_entity_guid();
                    if !entity_guid.is_empty() {
                        log_message.attributes.insert("entity.guid".to_string(),
                            serde_json::Value::String(entity_guid.to_string()));
                    }
                }
            }
        }

        log_message
    }

   
    pub async fn process_record(&self, record: TelemetryRecord) {
        match record.record_type.as_str() {
            "function" => {
                if !self.config.extension.send_function_logs {
                    return;
                }
            }
            "extension" => {
                if !self.config.extension.send_extension_logs {
                    return;
                }
            }
            "platform" => {
                if !self.config.extension.send_platform_logs {
                    return;
                }
            }
            _ => {
                trace!("Processing unknown log type: {}", record.record_type);
            }
        }
        
        let message_str = match &record.record {
            serde_json::Value::String(s) => s.as_str(),
            serde_json::Value::Object(obj) => {
                if let Some(message_value) = obj.get("message") {
                    message_value.as_str().unwrap_or("")
                } else {
                    &serde_json::to_string(&record.record).unwrap_or_default()
                }
            }
            _ => {
                &serde_json::to_string(&record.record).unwrap_or_default()
            }
        };
    
        if let Some(log_message) = self.to_log_message(record.clone()) {
            // Route to pre_invoke_buffer if ARN is empty (INIT phase before first INVOKE)
            let has_arn = {
                let context = self.invocation_context.lock().unwrap();
                !context.invoked_function_arn.is_empty()
            };
    
            if !has_arn {
                let mut pre_invoke_buf = self.pre_invoke_buffer.lock().unwrap();
                pre_invoke_buf.push(log_message);
                return;
            }

            let has_valid_request_id = {
                let context = self.invocation_context.lock().unwrap();
                !context.request_id.is_empty() && context.request_id != "unknown"
            };
    
            if !has_valid_request_id {
                let mut request_buffer = self.request_id_buffer.lock().unwrap();
                request_buffer.push(log_message);
                return;
            }
            
            // Check if we're actually in APM mode (both outer and inner Option must be Some)
            let is_apm_mode = self.apm_app.as_ref().and_then(|apm_arc| {
                apm_arc.try_read().ok().and_then(|guard| {
                    if guard.is_some() { Some(()) } else { None }
                })
            }).is_some();

            // Determine if this log should be treated as an error
            // Uses extract_log_level which handles both structured JSON levels and
            // unstructured keyword matching with position-priority and word boundaries
            let should_treat_as_error = if record.record_type == "function" && !message_str.is_empty() {
                message_str.contains("Task timed out")
                    || self.extract_log_level(&record.record, message_str) == "ERROR"
            } else {
                false
            };

            if should_treat_as_error {
                // Escape newlines to prevent log corruption when captured by Lambda Telemetry API
                let sanitized_msg: String = message_str.chars().take(100).collect::<String>()
                    .replace('\n', "\\n").replace('\r', "\\r");
                
                let (request_id, function_arn) = {
                    let context = self.invocation_context.lock().unwrap();
                    (context.request_id.clone(), context.invoked_function_arn.clone())
                };

                // Store error details for potential platform fault correlation
                let error_type = if message_str.contains("Task timed out") {
                    "Timeout"
                } else if message_str.contains("Exception") || message_str.contains("exception") {
                    "Exception"
                } else if message_str.contains("Fatal") || message_str.contains("fatal") {
                    "Fatal"
                } else {
                    "Error"
                };

                if let Ok(mut last_error) = crate::error_synthesis::LAST_DETECTED_ERROR.lock() {
                    *last_error = Some(crate::error_synthesis::LastDetectedError {
                        request_id: request_id.clone(),
                        error_type: error_type.to_string(),
                    });
                }

                if is_apm_mode {
                    debug!("APM mode: Error detected in function log: {}", sanitized_msg);
                    debug!("APM mode: Sending error event for request_id: {}", request_id);

                    if let Some(ref apm_app_arc) = self.apm_app {
                        let apm_clone = Arc::clone(apm_app_arc);
                        let msg_clone = message_str.to_string();

                        // Send error asynchronously during the current invoke
                        let apm_guard = apm_clone.read().await;
                        if let Some(ref app) = *apm_guard {
                            if let Err(e) = app.send_error_event_from_fault(&msg_clone, &request_id, &function_arn).await {
                                debug!("Failed to send error event from function log fault: {}", e);
                            }
                        }
                    }
                } else {
                    // Standard (non-APM) mode: Send errors to telemetry endpoint
                    debug!("Serverless mode: Error detected in function log: {}", sanitized_msg);
                    debug!("Serverless mode: Sending error for request_id: {}", request_id);

                    // Determine error type - use consistent LambdaError for all function errors
                    // (except timeout which should match platform timeout)
                    let error_type = if message_str.contains("Task timed out") {
                        "LambdaTimeout"  // Match platform timeout error class
                    } else {
                        "LambdaError"    // Use single consistent error class
                    };

                    let client = Arc::clone(&self.newrelic_client);
                    let config = Arc::clone(&self.config);
                    let msg_clone = message_str.to_string();
                    let error_type_clone = error_type.to_string();

                    // Format error message like timeout/platform errors for Error Inbox recognition
                    // Format: "{ISO_TIMESTAMP} {REQUEST_ID} {ERROR_CLASS} {original_error_message}"
                    // Keep the original error message with [ERROR] prefix intact
                    let formatted_error_msg = format!(
                        "{} {} {} {}",
                        chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
                        request_id,
                        error_type,  // LambdaError or LambdaTimeout
                        msg_clone    // Keep original message with [ERROR] prefix
                    );

                    // Store error details for potential platform fault correlation
                    if let Ok(mut last_error) = crate::error_synthesis::LAST_DETECTED_ERROR.lock() {
                        *last_error = Some(crate::error_synthesis::LastDetectedError {
                            request_id: request_id.clone(),
                            error_type: error_type_clone.clone(),
                        });
                    }

                    // Send error asynchronously during the current invoke
                    crate::error_synthesis::send_lambda_error(
                        &formatted_error_msg,  // Send formatted message, not raw log
                        &request_id,
                        &function_arn,
                        &error_type_clone,
                        &client,
                        &config,
                    ).await;
                }
            }
    
            let log_message = self.apply_current_invocation_metadata(log_message);
    
            if let (Some(ref extraction_state), Some(ref buffered_logs)) = 
                (&self.trace_extraction_state, &self.buffered_logs) {
                
                let state = extraction_state.lock().unwrap();
                let has_trace_id = {
                    let context = self.invocation_context.lock().unwrap();
                    context.trace_id.is_some()
                };
                
                if *state == TraceIdExtractionState::Waiting && !has_trace_id {
                    drop(state);
                    let mut buffered = buffered_logs.lock().unwrap();
                    if buffered.len() < MAX_TRACE_BUFFER {
                        buffered.push(log_message);
                        return;
                    }
                    drop(buffered);
                    debug!("buffered_logs overflow ({} cap) — routing log to log_batch without trace.id", MAX_TRACE_BUFFER);
                    // fall through to log_batch push + auto-flush below
                }
            }
            
            {
                let mut batch = self.log_batch.lock().unwrap();
                batch.push(log_message);
            }
            self.try_spawn_auto_flush();
        } else {
            warn!("Failed to convert telemetry record to log message");
        }
    }

   
    fn to_log_message(&self, record: TelemetryRecord) -> Option<payload::LogMessage> {
        let timestamp = record.time.timestamp_millis();
        
        let message = if let Some(message_value) = record.record.get("message") {
            match message_value {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string()
            }
        } else {
            match &record.record {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string()
            }
        };
        
        let mut attributes = serde_json::Map::new();
        
        let log_level = self.extract_log_level(&record.record, &message);
        attributes.insert("level".to_string(), log_level.into());
        
        attributes.insert("newrelic.logPattern".to_string(), "nr.DID_NOT_MATCH".into());
        
        attributes.insert("newrelic.source".to_string(), "api.logs".into());

        attributes.insert(
            "_nr.logType".to_string(),
            serde_json::Value::String(record.record_type.clone()),
        );

        Some(payload::LogMessage {
            timestamp,
            message,
            attributes,
        })
    }

    /// Derive LogType from the _nr.logType attribute stamped by to_log_message.
    /// Safe default is Function (never drop).
    fn log_type_from_message(msg: &payload::LogMessage) -> LogType {
        msg.attributes
            .get("_nr.logType")
            .and_then(|v| v.as_str())
            .map(LogType::from_record_type)
            .unwrap_or(LogType::Function)
    }

    /// Extract log level from structured JSON or unstructured message string
    /// Priority: 1) Structured JSON fields, 2) Keyword patterns with word boundaries
    fn extract_log_level(&self, record: &serde_json::Value, message: &str) -> &'static str {
        // FIRST: Check for structured level field in JSON record
        if let Some(structured_level) = get_structured_log_level(record) {
            return match structured_level.as_str() {
                "TRACE" | "VERBOSE" => "TRACE",
                "DEBUG" => "DEBUG",
                "INFO" | "INFORMATION" => "INFO",
                "WARN" | "WARNING" => "WARN",
                "ERROR" | "FATAL" | "CRITICAL" => "ERROR",
                _ => "INFO",
            };
        }

        // FALLBACK: Position-priority parsing for unstructured logs
        // The earliest keyword match wins, because log level prefixes (e.g. [INFO], ERROR:)
        // appear at the start of the line, before any message body that might contain
        // level-like words (e.g. "No error detected").
        let search_limit = (0..=message.len().min(150))
            .rev()
            .find(|&i| message.is_char_boundary(i))
            .unwrap_or(0);
        let search_area = &message[..search_limit];
        let lower = search_area.to_lowercase();

        // Level keywords mapped to their output levels
        const LEVELS: &[(&str, &str)] = &[
            ("fatal", "ERROR"),
            ("critical", "ERROR"),
            ("error", "ERROR"),
            ("warning", "WARN"),
            ("warn", "WARN"),
            ("debug", "DEBUG"),
            ("trace", "TRACE"),
            ("info", "INFO"),
        ];

        // Find all keyword matches with valid word boundaries, pick earliest position
        let mut best_match: Option<(usize, &str)> = None;

        for &(keyword, level) in LEVELS {
            if let Some(pos) = lower.find(keyword) {
                // Check word boundaries to prevent false matches
                // Before: must be start of string or non-alphanumeric
                let before_ok = pos == 0 || {
                    let prev_byte = lower.as_bytes()[pos - 1];
                    !prev_byte.is_ascii_alphanumeric()
                };

                // After: must be end of string or non-alphanumeric
                let after_pos = pos + keyword.len();
                let after_ok = after_pos >= lower.len() || {
                    let next_byte = lower.as_bytes()[after_pos];
                    !next_byte.is_ascii_alphanumeric()
                };

                if before_ok && after_ok {
                    match best_match {
                        Some((best_pos, _)) if pos >= best_pos => {}
                        _ => best_match = Some((pos, level)),
                    }
                }
            }
        }

        match best_match {
            Some((_, level)) => level,
            None => "INFO",
        }
    }

    /// Construct ARN from registration response Format: arn:aws:lambda:{region}:{account_id}:function:{function_name}
    /// Store fallback ARN from registration response for emergency shutdown scenarios
    /// ARN is pre-constructed from registration data to ensure consistency
    pub fn set_fallback_arn(&self, registration_fallback_arn: &str) {
        if let Ok(mut arn_guard) = self.fallback_function_arn.lock() {
            *arn_guard = Some(registration_fallback_arn.to_string());
            debug!("Set registration fallback ARN: {}", registration_fallback_arn);
        }
    }

    /// Get best available ARN: check fallback_function_arn, then global context
    fn get_best_available_arn(&self) -> String {
        // First try LogProcessor's fallback ARN
        if let Ok(arn_guard) = self.fallback_function_arn.lock() {
            if let Some(ref arn) = *arn_guard {
                return arn.clone();
            }
        }
        
        // Fallback to global registration ARN
        crate::get_global_fallback_arn()
    }

    /// Add a failed log entry to its per-type queue (O(1) eviction via FailedBuffer).
    /// Function/Platform/Extension each have their own `MAX_FAILED_BUFFER_PER_TYPE`
    /// cap, so one type's flood can no longer crowd out the others.
    fn push_to_failed_buffer(&self, entry: FailedLogEntry) {
        if let Ok(mut buf) = self.failed_logs_buffer.lock() {
            buf.push(entry);
        }
    }

    /// Drain the failed-log buffer and spawn a tracked retry task.
    /// The task runs concurrently with the Lambda function; flush() awaits it before GET /next.
    ///
    /// Must be called exactly once per invocation, before flush().
    /// Calling twice without an intervening flush() aborts the prior retry task and may lose in-flight logs.
    pub fn start_invocation_retry(&self) {
        // Drain atomically per-type — release lock before any async work.
        // drain_partitioned returns (Function+Platform, Extension) already split.
        let (high_pri, low_pri): (Vec<_>, Vec<_>) = {
            let mut buf = self.failed_logs_buffer.lock().unwrap();
            if buf.is_empty() {
                return;
            }
            buf.drain_partitioned()
        };

        let prepare = |group: Vec<FailedLogEntry>| -> Vec<FailedLogEntry> {
            group.into_iter()
                .filter(|e| e.retry_count < MAX_RETRIES)
                .map(|mut e| { e.retry_count += 1; e })
                .collect()
        };
        let high_pri = prepare(high_pri);
        let low_pri  = prepare(low_pri);

        if high_pri.is_empty() && low_pri.is_empty() {
            debug!("start_invocation_retry: all entries exceeded MAX_RETRIES, nothing to send");
            return;
        }

        let client     = Arc::clone(&self.newrelic_client);
        let config     = Arc::clone(&self.config);
        let proc_clone = self.clone();
        let arn        = self.get_best_available_arn();

        let handle = tokio::spawn(async move {
            async fn send_with_rebuffer(
                entries: Vec<FailedLogEntry>,
                client: &NewRelicClient,
                config: &ExtensionConfig,
                arn: &str,
                proc: &LogProcessor,
            ) {
                if entries.is_empty() { return; }

                const MAX_PAYLOAD_SIZE: usize = 1_000_000;
                let mut chunks: Vec<Vec<FailedLogEntry>> = Vec::new();
                let mut current: Vec<FailedLogEntry> = Vec::new();
                let mut current_size = 0usize;

                for entry in entries {
                    let sz = estimate_log_size(&entry.log_message);
                    if current_size + sz > MAX_PAYLOAD_SIZE && !current.is_empty() {
                        chunks.push(std::mem::take(&mut current));
                        current_size = 0;
                    }
                    current_size += sz;
                    current.push(entry);
                }
                if !current.is_empty() { chunks.push(current); }

                for chunk in chunks {
                    let batch: Vec<_> = chunk.iter().map(|e| e.log_message.clone()).collect();
                    let origin_req = chunk.first()
                        .map(|e| e.original_request_id.as_str())
                        .unwrap_or("?");
                    // Use send_chunk_with_retry_internal so a single transient network
                    // blip doesn't consume an entire invocation-retry slot. That helper
                    // applies MAX_RETRIES retries with exponential backoff per chunk.
                    match proc.send_chunk_with_retry_internal(client, config, batch, arn).await {
                        Ok(()) => {
                            info!("Invocation retry: sent {} logs successfully (origin req: {})", chunk.len(), origin_req);
                        }
                        Err(e) => {
                            warn!("Invocation retry send failed after in-task retries: {} — re-buffering {} logs (origin req: {})", e, chunk.len(), origin_req);
                            for entry in chunk {
                                proc.push_to_failed_buffer(entry);
                            }
                        }
                    }
                }
            }

            send_with_rebuffer(high_pri, &client, &config, &arn, &proc_clone).await;
            send_with_rebuffer(low_pri,  &client, &config, &arn, &proc_clone).await;
        });

        let mut slot = self.invocation_retry_handle.lock().unwrap();
        if let Some(prev) = slot.take() {
            if !prev.is_finished() {
                // Invariant: flush() should have been awaited between two
                // start_invocation_retry calls. When it wasn't, abort()ing the prior
                // task would drop whatever logs it was sending. Instead spawn a
                // background await so the old task completes in parallel with the new
                // one; they operate on disjoint drains of failed_logs_buffer (the old
                // task already captured its entries at spawn time).
                let n = RETRY_INVARIANT_VIOLATIONS.fetch_add(1, Ordering::Relaxed) + 1;
                warn!(
                    "start_invocation_retry called with prior task still running \
                     (total invariant violations: {}); awaiting prior in background",
                    n
                );
                tokio::spawn(async move {
                    if let Err(e) = prev.await {
                        warn!("Prior invocation retry task ended with error: {}", e);
                    }
                });
            } else {
                // Prior task already finished; just drop its handle.
                drop(prev);
            }
        }
        *slot = Some(handle);
    }

    /// Transfer logs from pre_invoke_buffer to log_batch with ARN/request_id added
    /// Only processes logs if invocation context is valid. Invalid context leaves logs in buffer.
    pub fn process_pre_invoke_logs(&self) {
        let context_valid = {
            if let Some(context) = self.invocation_context.safe_lock() {
                !context.invoked_function_arn.is_empty() 
                    && !context.request_id.is_empty() 
                    && context.request_id != "unknown"
            } else {
                false
            }
        };
        
        if !context_valid {
            let buf_size = self.pre_invoke_buffer.lock().unwrap().len();
            if buf_size > 0 {
                debug!("Skipping pre-invoke log processing - context not ready yet ({} logs waiting)", buf_size);
            }
            return;
        }
        
        let mut pre_invoke_logs = {
            let mut buf = self.pre_invoke_buffer.lock().unwrap();
            std::mem::take(&mut *buf)
        };
        
        if pre_invoke_logs.is_empty() {
            return;
        }
        
        debug!("Processing {} pre-invoke logs with invocation metadata", pre_invoke_logs.len());
        
        // At this point, context is guaranteed valid - stamp all logs
        for log in &mut pre_invoke_logs {
            if let Some(context) = self.invocation_context.safe_lock() {
                log.attributes.insert("faas.arn".to_string(),
                    serde_json::Value::String(context.invoked_function_arn.clone()));
                
                // Stamp request_id in New Relic expected format
                let mut aws_attrs = serde_json::Map::new();
                aws_attrs.insert("lambda_request_id".to_string(),
                    serde_json::Value::String(context.request_id.clone()));
                log.attributes.insert("aws".to_string(),
                    serde_json::Value::Object(aws_attrs));
                log.attributes.insert("faas.execution".to_string(),
                    serde_json::Value::String(context.request_id.clone()));
                
                // Stamp trace_id if available
                if let Some(ref trace_id) = context.trace_id {
                    log.attributes.insert("trace.id".to_string(),
                        serde_json::Value::String(trace_id.clone()));
                }
            }
            
            // Stamp entity.guid if APM app available
            if let Some(ref apm_app_arc) = self.apm_app {
                if let Ok(apm_guard) = apm_app_arc.try_read() {
                    if let Some(ref app) = *apm_guard {
                        let entity_guid = app.get_entity_guid();
                        if !entity_guid.is_empty() {
                            log.attributes.insert("entity.guid".to_string(),
                                serde_json::Value::String(entity_guid.to_string()));
                        }
                    }
                }
            }
        }
        
        // All logs are now complete - move to batch for sending
        if let Ok(mut batch) = self.log_batch.lock() {
            batch.extend(pre_invoke_logs);
        }
    }

    /// Send pre-invoke logs on shutdown with last request ID (or force flush with marker in error cases)
    /// Normal case: Use last request ID from previous invocation
    /// Error case (crash before first invoke): Send with nr.forceFlushed=true marker
    pub async fn flush_pre_invoke_buffer_on_shutdown(&self) -> std::io::Result<()> {
        let mut pre_invoke_logs = {
            let mut buf = self.pre_invoke_buffer.lock().unwrap();
            std::mem::take(&mut *buf)
        };
        
        if pre_invoke_logs.is_empty() {
            debug!("No pre-invoke logs to flush on shutdown");
            return Ok(());
        }
        
        // Try to get last request ID from previous invocations
        let last_context = if let Ok(guard) = crate::event_loop::LAST_REQUEST_CONTEXT.lock() {
            guard.as_ref().cloned()
        } else {
            None
        };
        
        let function_arn = if let Some(context) = self.invocation_context.safe_lock() {
            if !context.invoked_function_arn.is_empty() {
                context.invoked_function_arn.clone()
            } else {
                self.fallback_function_arn.lock()
                    .ok()
                    .and_then(|guard| guard.as_ref().cloned())
                    .unwrap_or_else(String::new)
            }
        } else {
            String::new()
        };
        
        match last_context {
            Some((request_id, arn)) => {
                info!("Shutdown: Sending {} pre-invoke logs with last request ID: {}", pre_invoke_logs.len(), request_id);
                
                let use_arn = if !arn.is_empty() { arn } else { function_arn };
                
                for log in &mut pre_invoke_logs {
                    if !use_arn.is_empty() {
                        log.attributes.insert("faas.arn".to_string(),
                            serde_json::Value::String(use_arn.clone()));
                    }
                    // Create nested AWS structure: {"aws": {"lambda_request_id": "..."}}
                    let mut aws_attrs = serde_json::Map::new();
                    aws_attrs.insert("lambda_request_id".to_string(),
                        serde_json::Value::String(request_id.clone()));
                    log.attributes.insert("aws".to_string(),
                        serde_json::Value::Object(aws_attrs));
                    log.attributes.insert("faas.execution".to_string(),
                        serde_json::Value::String(request_id.clone()));
                    
                    // Add entity.guid if in APM mode
                    if let Some(ref apm_app_arc) = self.apm_app {
                        if let Ok(apm_guard) = apm_app_arc.try_read() {
                            if let Some(ref app) = *apm_guard {
                                let entity_guid = app.get_entity_guid();
                                if !entity_guid.is_empty() {
                                    log.attributes.insert("entity.guid".to_string(),
                                        serde_json::Value::String(entity_guid.to_string()));
                                }
                            }
                        }
                    }
                }
                
                let client = Arc::clone(&self.newrelic_client);
                let config = Arc::clone(&self.config);
                
                Self::send_logs_with_chunking(&client, &config, pre_invoke_logs, &use_arn).await;
            }
            None => {
                // Error case: Crash/shutdown before first invoke - force flush with marker
                warn!("Shutdown before first invoke (error/crash) - force flushing {} pre-invoke logs with nr.forceFlushed marker", pre_invoke_logs.len());
                
                if function_arn.is_empty() {
                    error!("Cannot flush pre-invoke logs: no ARN available (neither from INVOKE nor registration)");
                    return Ok(());
                }
                
                for log in &mut pre_invoke_logs {
                    log.attributes.insert("faas.arn".to_string(),
                        serde_json::Value::String(function_arn.clone()));
                    // Create nested AWS structure: {"aws": {"lambda_request_id": "..."}}
                    let mut aws_attrs = serde_json::Map::new();
                    aws_attrs.insert("lambda_request_id".to_string(),
                        serde_json::Value::String("INIT_PHASE_LOGS".to_string()));
                    log.attributes.insert("aws".to_string(),
                        serde_json::Value::Object(aws_attrs));
                    log.attributes.insert("nr.forceFlushed".to_string(),
                        serde_json::Value::Bool(true));
                    
                    // Add entity.guid if in APM mode
                    if let Some(ref apm_app_arc) = self.apm_app {
                        if let Ok(apm_guard) = apm_app_arc.try_read() {
                            if let Some(ref app) = *apm_guard {
                                let entity_guid = app.get_entity_guid();
                                if !entity_guid.is_empty() {
                                    log.attributes.insert("entity.guid".to_string(),
                                        serde_json::Value::String(entity_guid.to_string()));
                                }
                            }
                        }
                    }
                }
                
                let client = Arc::clone(&self.newrelic_client);
                let config = Arc::clone(&self.config);
                
                Self::send_logs_with_chunking(&client, &config, pre_invoke_logs, &function_arn).await;
            }
        }
        
        Ok(())
    }

   
    pub async fn on_trace_id_extracted(&self, trace_id: &str) -> std::io::Result<()> {
        let (Some(ref extraction_state), Some(ref buffered_logs_arc)) = 
            (&self.trace_extraction_state, &self.buffered_logs) else {
            return Ok(());
        };

        *extraction_state.lock().unwrap() = TraceIdExtractionState::Extracted;
        
        let mut buffered_logs = {
            let mut buffered = buffered_logs_arc.lock().unwrap();
            std::mem::take(&mut *buffered)
        };
        
        if buffered_logs.is_empty() {
            return Ok(());
        }

        debug!("Applied trace ID to {} buffered logs; routing to log_batch", buffered_logs.len());

        let entity_guid_opt: Option<String> = self.apm_app.as_ref().and_then(|arc| {
            match arc.try_read() {
                Ok(guard) => guard.as_ref().map(|app| app.get_entity_guid().to_string()).filter(|g| !g.is_empty()),
                Err(_) => {
                    debug!("on_trace_id_extracted: entity.guid unavailable (apm_app write lock held); logs routed without it");
                    None
                }
            }
        });

        for log in &mut buffered_logs {
            log.attributes.insert("trace.id".to_string(), trace_id.into());
            if let Some(ref guid) = entity_guid_opt {
                log.attributes.insert("entity.guid".to_string(),
                    serde_json::Value::String(guid.clone()));
            }
        }

        // Route through log_batch so ARN fallback chain and deduplication apply
        self.log_batch.lock().unwrap().extend(buffered_logs);
        // Bulk push can push the batch past FLUSH_THRESHOLD without triggering the
        // per-record auto-flush path; explicitly check so the trace-buffer drain
        // doesn't silently inflate batch-in-memory peak.
        self.try_spawn_auto_flush();
        Ok(())
    }

   
    pub fn reset_trace_id_state(&self) {
        if let (Some(ref extraction_state), Some(ref buffered_logs)) =
            (&self.trace_extraction_state, &self.buffered_logs)
        {
            *extraction_state.lock().unwrap() = TraceIdExtractionState::Waiting;
            let mut rescued: Vec<_> = {
                let mut buf = buffered_logs.lock().unwrap();
                std::mem::take(&mut *buf)
            };
            if !rescued.is_empty() {
                debug!("reset_trace_id_state: rescuing {} trace-waiting logs into log_batch", rescued.len());
                let entity_guid_opt: Option<String> = self.apm_app.as_ref().and_then(|arc| {
                    match arc.try_read() {
                        Ok(guard) => guard.as_ref().map(|app| app.get_entity_guid().to_string()).filter(|g| !g.is_empty()),
                        Err(_) => {
                            debug!("reset_trace_id_state: entity.guid unavailable (apm_app write lock held); logs rescued without it");
                            None
                        }
                    }
                });
                if let Some(ref guid) = entity_guid_opt {
                    for log in &mut rescued {
                        log.attributes.insert("entity.guid".to_string(),
                            serde_json::Value::String(guid.clone()));
                    }
                }
                self.log_batch.lock().unwrap().extend(rescued);
                // Same rationale as on_trace_id_extracted: bulk push can exceed the
                // auto-flush threshold, so probe after extending.
                self.try_spawn_auto_flush();
            }
        }
    }

   
   
    pub fn process_buffered_logs_with_request_id(&self, request_id: &str) {
        // Failed-log retry is now handled by start_invocation_retry() which spawns a tracked
        // task that is awaited in flush() before GET /next. Nothing to do here.

        let buffered_logs = {
            let mut buffer = self.request_id_buffer.lock().unwrap();
            std::mem::take(&mut *buffer)
        };
        
        if !buffered_logs.is_empty() {
            debug!("Processing {} buffered logs with new request_id: {}", buffered_logs.len(), request_id);
            
            // Capture ARN once outside the loop — context is stable at this point.
            let buffered_arn = {
                let ctx = self.invocation_context.lock().unwrap();
                if !ctx.invoked_function_arn.is_empty() {
                    ctx.invoked_function_arn.clone()
                } else {
                    drop(ctx);
                    self.get_best_available_arn()
                }
            };

            for mut log_message in buffered_logs {
                // New Relic expects nested structure: {"aws": {"lambda_request_id": "..."}}
                let mut aws_attrs = serde_json::Map::new();
                aws_attrs.insert("lambda_request_id".to_string(),
                    serde_json::Value::String(request_id.to_string()));
                log_message.attributes.insert("aws".to_string(),
                    serde_json::Value::Object(aws_attrs));
                log_message.attributes.insert("faas.execution".to_string(),
                    serde_json::Value::String(request_id.to_string()));
                if !buffered_arn.is_empty() {
                    log_message.attributes.insert("faas.arn".to_string(),
                        serde_json::Value::String(buffered_arn.clone()));
                }
                
                if let (Some(ref extraction_state), Some(ref buffered_logs_arc)) =
                    (&self.trace_extraction_state, &self.buffered_logs) {

                    let state = extraction_state.lock().unwrap();
                    let trace_id_opt = {
                        let context = self.invocation_context.lock().unwrap();
                        context.trace_id.clone()
                    };

                    if *state == TraceIdExtractionState::Waiting && trace_id_opt.is_none() {
                        drop(state);
                        let mut buffered = buffered_logs_arc.lock().unwrap();
                        buffered.push(log_message);
                        continue;
                    }
                    drop(state);

                    // We're going to log_batch directly — stamp trace.id if already known.
                    if let Some(ref trace_id) = trace_id_opt {
                        log_message.attributes.insert("trace.id".to_string(),
                            serde_json::Value::String(trace_id.clone()));
                    }
                }

                // Stamp entity.guid for APM mode (same pattern as pre-invoke and shutdown paths).
                if let Some(ref apm_app_arc) = self.apm_app {
                    match apm_app_arc.try_read() {
                        Ok(apm_guard) => {
                            if let Some(ref app) = *apm_guard {
                                let entity_guid = app.get_entity_guid();
                                if !entity_guid.is_empty() {
                                    log_message.attributes.insert("entity.guid".to_string(),
                                        serde_json::Value::String(entity_guid.to_string()));
                                }
                            }
                        }
                        Err(_) => {
                            debug!("process_buffered_logs: entity.guid unavailable (apm_app write lock held); log routed without it");
                        }
                    }
                }

                let mut batch = self.log_batch.lock().unwrap();
                batch.push(log_message);
            }
        }
    }

    pub async fn send_and_clear_batch_simple(&self) -> std::io::Result<()> {
        // FIRST: Try to stamp any remaining pre-invoke logs before final flush
        // This catches logs that arrived early (before context was ready)
        debug!("Final flush: Attempting to process pre-invoke logs one last time");
        self.process_pre_invoke_logs();
        
        // Master check: if all log types are disabled, don't send anything
        if !self.config.extension.send_function_logs
            && !self.config.extension.send_extension_logs
            && !self.config.extension.send_platform_logs {
            debug!("All log types disabled - clearing batch without sending");
            if let Some(mut batch_guard) = self.log_batch.safe_lock() {
                batch_guard.clear();
            }
            return Ok(());
        }

        // Drain all pending auto-flush tasks. Loop in case awaiting a handle causes
        // a new one to be pushed (e.g., overflow chunk triggering another flush).
        // Cap at 10 rounds to surface any future infinite-spawn regression.
        const MAX_DRAIN_ROUNDS: usize = 10;
        let mut round = 0usize;
        loop {
            if round >= MAX_DRAIN_ROUNDS {
                error!("pending_flush_handles drain exceeded {} rounds — possible infinite spawn loop; aborting drain", MAX_DRAIN_ROUNDS);
                break;
            }
            let handles = {
                let mut guard = self.pending_flush_handles.lock().unwrap();
                std::mem::take(&mut *guard)
            };
            if handles.is_empty() {
                break;
            }
            debug!("Waiting for {} pending auto-flush tasks (round {})", handles.len(), round + 1);
            for handle in handles {
                let _ = handle.await;
            }
            round += 1;
        }

        let batch = {
            let mut batch_guard = self.log_batch.lock().unwrap();
            std::mem::take(&mut *batch_guard)
        };
        
        if batch.is_empty() {
            debug!("No logs in batch to send");
            return Ok(());
        }

        let deduplicated_batch = {
            use std::collections::HashMap;
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            // DefaultHasher is acceptable here: the `seen` map is local to this single flush call
            // (not persisted, not cross-version), so hash instability across Rust releases is
            // irrelevant. Collision probability for the 4-field tuple
            // (message, timestamp, _nr.logType, lambda_request_id) over a typical batch of
            // ≤1000 logs is negligible (~batch²/2⁶⁴ ≈ 5×10⁻¹⁴).
            let mut seen = HashMap::new();
            let mut unique_logs = Vec::new();
            let mut duplicate_count = 0;
            
            for log in batch {
                let mut hasher = DefaultHasher::new();
                log.message.hash(&mut hasher);
                log.timestamp.hash(&mut hasher);

                // Include log source type so function/platform/extension logs with the
                // same text and timestamp are never collapsed into one.
                if let Some(log_type) = log.attributes.get("_nr.logType").and_then(|v| v.as_str()) {
                    log_type.hash(&mut hasher);
                }

                // Check nested AWS structure for request_id
                if let Some(aws_value) = log.attributes.get("aws") {
                    if let Some(aws_obj) = aws_value.as_object() {
                        if let Some(request_id_value) = aws_obj.get("lambda_request_id") {
                            if let Some(request_id_str) = request_id_value.as_str() {
                                request_id_str.hash(&mut hasher);
                            }
                        }
                    }
                }

                let log_hash = hasher.finish();
                
                if seen.insert(log_hash, log.timestamp).is_none() {
                    unique_logs.push(log);
                } else {
                    duplicate_count += 1;
                }
            }
            
            if duplicate_count > 0 {
                debug!("Deduplicated {} duplicate log(s) before sending", duplicate_count);
            }
            
            unique_logs
        };

        if deduplicated_batch.is_empty() {
            debug!("All logs were duplicates, nothing to send");
            return Ok(());
        }

        debug!("Final flush: sending {} logs to New Relic", deduplicated_batch.len());

        let client = Arc::clone(&self.newrelic_client);
        let config = Arc::clone(&self.config);
        let context = self.invocation_context.lock().unwrap().clone();

        // GUARD: Never send logs without ARN - use fallback chain if context ARN is empty
        let effective_arn = if !context.invoked_function_arn.is_empty() {
            context.invoked_function_arn.clone()
        } else {
            let fallback = self.get_best_available_arn();
            if fallback.is_empty() {
                error!(
                    "BLOCKED: Refusing to flush {} logs without ARN (request_id: '{}'). \
                     Neither invocation context nor fallback ARN available.",
                    deduplicated_batch.len(),
                    context.request_id
                );
                // Put logs back in batch so they can be sent later when ARN is available
                if let Ok(mut batch) = self.log_batch.lock() {
                    batch.extend(deduplicated_batch);
                }
                return Ok(());
            }
            warn!(
                "Log flush: Using fallback ARN '{}' (invocation context ARN was empty, request_id: '{}')",
                fallback, context.request_id
            );
            fallback
        };
        
        const MAX_PAYLOAD_SIZE: usize = 1_000_000; // 1MB
        let mut chunks: Vec<Vec<payload::LogMessage>> = Vec::new();
        let mut current_chunk = Vec::new();
        let mut current_size = 0;
        
        for log in deduplicated_batch {
            let log_size = estimate_log_size(&log);

            if current_size + log_size > MAX_PAYLOAD_SIZE && !current_chunk.is_empty() {
                chunks.push(std::mem::take(&mut current_chunk));
                current_size = 0;
            }
            
            current_chunk.push(log);
            current_size += log_size;
        }
        
        if !current_chunk.is_empty() {
            chunks.push(current_chunk);
        }
        
        if chunks.len() > 1 {
            debug!("Chunking {} logs into {} size-based batches (max 1MB each)", 
                  chunks.iter().map(|c| c.len()).sum::<usize>(), chunks.len());
        }
        
        let mut failed_logs = Vec::new();
        let mut successful_chunks = 0;
        
        for chunk in chunks {
            match self.send_chunk_with_retry_internal(&client, &config, chunk.clone(), &effective_arn).await {
                Ok(()) => {
                    successful_chunks += 1;
                },
                Err(e) => {
                    error!("Log batch send failed: {}", e);
                    failed_logs.extend(chunk);
                }
            }
        }
        
        if successful_chunks > 0 {
            info!("Successfully sent {} log chunks", successful_chunks);
        }
        if !failed_logs.is_empty() {
            warn!("Buffering {} failed logs for retry on next invocation", failed_logs.len());
            for log in failed_logs {
                let lt = Self::log_type_from_message(&log);
                self.push_to_failed_buffer(FailedLogEntry {
                    log_message: log,
                    original_request_id: context.request_id.clone(),
                    retry_count: 0,
                    log_type: lt,
                });
            }
        }

        // Signal any waiter in wait_for_runtime_done_with_grace that the batch drained.
        self.notify_if_drained();

        Ok(())
    }


    async fn send_chunk_with_retry_internal(
        &self,
        client: &NewRelicClient,
        config: &ExtensionConfig,
        chunk: Vec<payload::LogMessage>,
        function_arn: &str,
    ) -> std::io::Result<()> {
        let mut retries = 0;

        loop {
            match client.send_logs(config, chunk.clone(), function_arn).await {
                Ok(()) => {
                    return Ok(());
                },
                Err(e) => {
                    if retries == 0 {
                        warn!("Log send failed: {}", e);
                    }

                    if e.to_string().contains("413") || e.to_string().contains("Payload Too Large") {
                        error!("Payload too large even after chunking - dropping {} logs", chunk.len());
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Payload too large even after chunking"
                        ));
                    }

                    if retries < MAX_RETRIES {
                        retries += 1;
                        let delay = get_backoff_delay(retries);
                        tokio::time::sleep(delay).await;
                        continue;
                    } else {
                        // Callers decide whether to re-buffer; this function only signals failure.
                        return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
                    }
                }
            }
        }
    }

    /// Helper method to send logs with proper 1MB chunking
    /// Used by both auto-flush (25 logs) and end-of-request flush
    async fn send_logs_with_chunking(
        client: &Arc<NewRelicClient>,
        config: &Arc<ExtensionConfig>,
        logs: Vec<payload::LogMessage>,
        function_arn: &str,
    ) {
        if logs.is_empty() {
            return;
        }

        const MAX_PAYLOAD_SIZE: usize = 1_000_000; // 1MB
        let mut chunks: Vec<Vec<payload::LogMessage>> = Vec::new();
        let mut current_chunk = Vec::new();
        let mut current_size = 0;

        for log in logs {
            let log_size = estimate_log_size(&log);

            if current_size + log_size > MAX_PAYLOAD_SIZE && !current_chunk.is_empty() {
                chunks.push(std::mem::take(&mut current_chunk));
                current_size = 0;
            }

            current_chunk.push(log);
            current_size += log_size;
        }

        if !current_chunk.is_empty() {
            chunks.push(current_chunk);
        }

        if chunks.len() > 1 {
            debug!("Chunking logs into {} batches (max 1MB each)", chunks.len());
        }

        for chunk in chunks {
            let dropped = chunk.len();
            if let Err(e) = client.send_logs(config, chunk, function_arn).await {
                error!("Failed to send log chunk on shutdown — {} logs permanently dropped: {}", dropped, e);
            }
        }
    }

}

#[async_trait]
impl Flush for LogProcessor {
    async fn flush(&self) -> std::io::Result<()> {
        // Fast-path: nothing to do — skip all lock acquisitions and allocations.
        {
            let no_retry = self.invocation_retry_handle.lock().unwrap().is_none();
            let batch_empty = self.log_batch.lock().unwrap().is_empty();
            let not_flushing = !*self.is_auto_flushing.lock().unwrap();
            let no_handles = self.pending_flush_handles.lock().unwrap().is_empty();
            if no_retry && batch_empty && not_flushing && no_handles {
                return Ok(());
            }
        }

        // Take the handle before the join to avoid holding the Mutex across an await
        let retry_handle = self.invocation_retry_handle.lock().unwrap().take();

        // Invariant: retry task and main batch send operate on disjoint sets of logs.
        // Retry drains failed_logs_buffer at start_invocation_retry() time (before function runs).
        // Main send drains log_batch at flush() time. No log appears in both.
        let (send_result, _) = tokio::join!(
            self.send_and_clear_batch_simple(),
            async move {
                if let Some(h) = retry_handle {
                    if let Err(e) = h.await {
                        warn!("Invocation retry task panicked or was cancelled: {}", e);
                    }
                }
            }
        );
        send_result
    }
}

#[cfg(test)]
#[path = "processor_tests.rs"]
mod processor_tests;