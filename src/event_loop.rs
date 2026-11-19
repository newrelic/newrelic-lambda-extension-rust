// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0



use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use std::time::Duration;
use reqwest::Client;
use tracing::{debug, error, info, trace, warn};
use crate::send::concurrent::{join_all_concurrently, BoxedFuture};

use crate::{
    runtime,
    config::{self, ExtensionConfig, deployment::DeploymentContext},
    newrelic::client::NewRelicClient,
    newrelic::flush::Flush,
    logs::processor::LogProcessor,
    request::{
        self,
        ProcessorFactory,
        create_request_processing_state,
        cleanup_request_processing_state_internal,
        REQUEST_PROCESSORS, REQUEST_DATA,
        get_agent_buffer, get_request_context, get_pending_report,
        remove_pending_report, request_data_len,
    },
    agent::batch::{
        add_to_batch, should_send_batch_by_threshold, 
        send_batched_payloads_with_reports_only,
        send_all_pending_payloads_on_shutdown,
    },
    error_synthesis,
    trace,
    version,
    IS_WARM_START,
};

const SHUTDOWN_TIMEOUT_MS: u64 = 1800;

/// Safety margin (ms) reserved before a function's own remaining deadline when
/// bound-waiting for something within an invocation (an APM handshake, a late agent
/// payload, or a late `platform.report`) — leaves headroom for the downstream
/// flush/cleanup work that still has to run before the extension returns to `/next`.
/// Shared by every bounded-wait helper so they can't drift apart on this value.
const INVOKE_DEADLINE_SAFETY_MARGIN_MS: u64 = 500;

/// Budget reserved (out of SHUTDOWN_TIMEOUT_MS) to POST the APM "telemetry
/// dropped" diagnostic directly to New Relic Logs. The main shutdown work runs
/// in `SHUTDOWN_TIMEOUT_MS - SHUTDOWN_DIAG_RESERVE_MS`, then the diagnostic gets
/// this protected window — so a slow flush/reconnect can't starve the one log
/// the customer most needs to see. Sum stays < Lambda's 2s SHUTDOWN deadline.
const SHUTDOWN_DIAG_RESERVE_MS: u64 = 500;

/// Drop guard that clears `reconnect_in_flight` on any exit path (success, error, or panic).
struct ReconnectGuard(Arc<watch::Sender<bool>>);

impl Drop for ReconnectGuard {
    fn drop(&mut self) {
        self.0.send_replace(false);
    }
}

#[derive(Debug)]
pub struct ExtensionComponents {
    pub client: Arc<Client>,
    pub extension_id: String,
    pub processor_factory: Arc<ProcessorFactory>,
    pub newrelic_client: Arc<NewRelicClient>,
    pub config: Arc<ExtensionConfig>,
    pub global_log_processor: Arc<LogProcessor>,
    pub apm_app: crate::apm::SharedApmApp,
    pub apm_mode_enabled: bool, // Actual mode after runtime detection (may differ from config for Java)
    pub apm_client: crate::newrelic::tls_fallback::DualTlsClient,
    pub reconnect_in_flight: Arc<watch::Sender<bool>>,
    /// Deployment context (Normal Lambda vs LMI). Drives top-level loop dispatch
    /// in `execute_main_telemetry_processing_loop` — Normal Lambda routes to the
    /// existing APM/Serverless loops; LMI routes to `event_loop_lmi`.
    pub deployment: DeploymentContext,
}

#[derive(Debug, Clone)]
pub struct FailedAgentPayload {
    pub payload_bytes: Vec<u8>,
    pub request_id: String,
    pub invoked_function_arn: String,
    pub retry_count: usize,
    pub failed_at: chrono::DateTime<chrono::Utc>,
}

pub static FAILED_AGENT_PAYLOADS: once_cell::sync::Lazy<Arc<Mutex<Vec<FailedAgentPayload>>>> =
    once_cell::sync::Lazy::new(|| Arc::new(Mutex::new(Vec::new())));

/// Max buffered agent payloads = the circuit breaker's **3N** buffer cap, derived
/// once from `NEW_RELIC_EXTENSION_SEND_TIER`: high tier `N=5 → 3N=15`, default tier
/// `N=10 → 3N=30`. When full, the oldest is evicted (and counted) — FIFO
/// drop-oldest — so a long disconnected period on a busy function can't grow
/// unbounded. (Was a flat 500 before the invocation-count breaker model; the
/// per-tier 3N cap replaces it so the buffer matches the breaker's
/// fill→retry→slide lifecycle exactly.)
static MAX_FAILED_AGENT_PAYLOADS: once_cell::sync::Lazy<usize> =
    once_cell::sync::Lazy::new(|| match crate::send::SendTier::from_env() {
        crate::send::SendTier::High => 15,
        crate::send::SendTier::Default => 30,
    });

/// Count of buffered agent payloads dropped *before* shutdown (evicted when the
/// buffer is full). Lets the shutdown summary report the true loss, since evicted
/// payloads are no longer in the buffer to be counted.
static DROPPED_AGENT_PAYLOADS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Number of agent payloads dropped before shutdown due to buffer-full eviction.
pub fn dropped_agent_payload_count() -> u64 {
    DROPPED_AGENT_PAYLOADS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Distinct request_ids whose agent payloads are still buffered (un-sent). Used
/// by the shutdown summary, since this buffer is the main bucket of un-delivered
/// data when APM never connected.
pub fn buffered_agent_payload_request_ids() -> Vec<String> {
    let mut ids: Vec<String> = FAILED_AGENT_PAYLOADS
        .lock()
        .map(|b| b.iter().map(|p| p.request_id.clone()).collect())
        .unwrap_or_default();
    ids.sort();
    ids.dedup();
    ids
}

/// Push a failed agent payload, evicting (and counting) the oldest if the buffer
/// is at capacity. Single choke point so both first-failure and retry re-buffer
/// paths enforce the same cap.
fn push_failed_payload_capped(buf: &mut Vec<FailedAgentPayload>, payload: FailedAgentPayload) {
    if buf.len() >= *MAX_FAILED_AGENT_PAYLOADS {
        buf.remove(0);
        DROPPED_AGENT_PAYLOADS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        warn!(
            "Failed agent payload buffer full ({}) - evicted oldest invocation's data",
            *MAX_FAILED_AGENT_PAYLOADS
        );
    }
    buf.push(payload);
}

/// A platform.report line whose metrics send was aborted (shared runtimeDone
/// budget elapsed, `SendTier::High` only) before it could complete — buffered
/// here, with `request_id` attribution, for a background retry on a later
/// invocation. Mirrors `FailedAgentPayload`'s role, but for the raw report
/// line rather than an already-parsed metric batch — this is deliberately
/// separate from `apm::metric_api_buffer::FAILED_METRIC_API_BUFFER` (which
/// buffers a different, later failure mode: a real HTTP failure on an
/// already-parsed batch, with no `request_id` to attribute to) rather than
/// overloading that struct with a shape it wasn't designed for.
#[derive(Debug, Clone)]
struct FailedPlatformReport {
    report_line: String,
    request_id: String,
    invoked_function_arn: String,
    retry_count: usize,
    failed_at: chrono::DateTime<chrono::Utc>,
}

static FAILED_PLATFORM_REPORTS: once_cell::sync::Lazy<Mutex<Vec<FailedPlatformReport>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(Vec::new()));

/// Memory guard, not a retry-count policy (same reasoning as
/// `MAX_FAILED_AGENT_PAYLOADS`): a sandbox lives at most ~30 min, so this
/// bounds worst-case buffered size during a sustained outage.
const MAX_FAILED_PLATFORM_REPORTS: usize = 100;
/// Drop (not re-buffer) a report after this many failed retry attempts —
/// unlike agent payload, a platform.report line is a periodic metrics
/// snapshot; an arbitrarily stale one retried forever has little value.
const MAX_PLATFORM_REPORT_RETRIES: usize = 3;

/// Buffer a platform.report whose send was aborted, for retry on a later
/// invocation (see `retry_failed_platform_reports`).
fn buffer_failed_platform_report(report_line: String, request_id: &str, invoked_function_arn: &str) {
    let entry = FailedPlatformReport {
        report_line,
        request_id: request_id.to_string(),
        invoked_function_arn: invoked_function_arn.to_string(),
        retry_count: 0,
        failed_at: chrono::Utc::now(),
    };
    match FAILED_PLATFORM_REPORTS.lock() {
        Ok(mut buf) => {
            if buf.len() >= MAX_FAILED_PLATFORM_REPORTS {
                buf.remove(0);
                warn!(
                    "Failed platform-report buffer full ({}) - evicted oldest",
                    MAX_FAILED_PLATFORM_REPORTS
                );
            }
            debug!(
                "Buffered aborted platform-report for request {} (total buffered: {})",
                request_id,
                buf.len() + 1
            );
            buf.push(entry);
        }
        Err(e) => error!(
            "FAILED_PLATFORM_REPORTS lock poisoned, report for {} lost: {}",
            request_id, e
        ),
    }
}

/// The high tier's INVOKE-time retries of earlier failures, every one spawned and none
/// awaited: failed agent payloads, failed platform reports, buffered per-type telemetry
/// (needs a live APM session) and buffered Metric API sends. Same conditions as the
/// default tier's inline retries; they only run later, once the invoke is not short.
async fn run_invoke_retries(
    apm_app: crate::apm::SharedApmApp,
    apm_client: crate::newrelic::tls_fallback::DualTlsClient,
    license_key: String,
    apm_mode_enabled: bool,
) {
    tokio::spawn(retry_failed_agent_payloads(apm_app.clone()));
    tokio::spawn(retry_failed_platform_reports(apm_app.clone()));

    if apm_mode_enabled && crate::apm::telemetry_buffer::get_buffer_count() > 0 {
        let session = apm_app
            .read()
            .await
            .as_ref()
            .map(|app| (app.run_id.clone(), app.collector_host.clone()));
        if let Some((run_id, collector_host)) = session {
            let (client, key) = (apm_client.clone(), license_key.clone());
            tokio::spawn(async move {
                crate::apm::telemetry_buffer::retry_buffered_telemetry(
                    &client,
                    &key,
                    Some(run_id.as_str()),
                    Some(collector_host.as_str()),
                )
                .await;
            });
        }
    }
    if apm_mode_enabled && crate::apm::metric_api_buffer::get_metric_api_buffer_count() > 0 {
        tokio::spawn(async move {
            crate::apm::metric_api_buffer::retry_buffered_metric_api(&apm_client, &license_key).await;
        });
    }
}

/// Retry buffered platform-report sends (APM mode, `SendTier::High` only —
/// `FAILED_PLATFORM_REPORTS` is only ever populated by the shared-budget
/// abort path). Spawned detached, alongside `retry_failed_agent_payloads`,
/// at the top of every invocation — never awaited inline, so a backlog here
/// never blocks `/next`. If `apm_app` isn't ready yet, puts everything back
/// untouched rather than burning a retry attempt on a guaranteed failure.
async fn retry_failed_platform_reports(apm_app: crate::apm::SharedApmApp) {
    let pending = match FAILED_PLATFORM_REPORTS.lock() {
        Ok(mut buf) => std::mem::take(&mut *buf),
        Err(e) => {
            error!("Failed to lock FAILED_PLATFORM_REPORTS for retry: {}", e);
            return;
        }
    };
    if pending.is_empty() {
        return;
    }

    debug!("Retrying {} buffered platform-report(s)", pending.len());

    let apm_guard = apm_app.read().await;
    let Some(ref app) = *apm_guard else {
        drop(apm_guard);
        if let Ok(mut buf) = FAILED_PLATFORM_REPORTS.lock() {
            buf.extend(pending);
        }
        return;
    };

    for mut entry in pending {
        // A sandbox lives at most ~30 min -- this guard against absurdly
        // stale data essentially never fires in practice, same reasoning as
        // retry_failed_agent_payloads's 24h check.
        let age = chrono::Utc::now().signed_duration_since(entry.failed_at);
        if age.num_hours() > 24 {
            warn!(
                "Dropping platform-report for request {} that's too old ({} hours)",
                entry.request_id, age.num_hours()
            );
            continue;
        }

        match app
            .send_platform_report_metrics(&entry.report_line, &entry.invoked_function_arn)
            .await
        {
            Ok(()) => {
                info!(
                    "Successfully retried buffered platform-report for request {}",
                    entry.request_id
                );
            }
            Err(e) => {
                entry.retry_count += 1;
                if entry.retry_count >= MAX_PLATFORM_REPORT_RETRIES {
                    warn!(
                        "Dropping platform-report for request {} after {} failed retries: {}",
                        entry.request_id, entry.retry_count, e
                    );
                    continue;
                }
                warn!(
                    "Retry failed for buffered platform-report (request {}, attempt {}): {}",
                    entry.request_id, entry.retry_count, e
                );
                if let Ok(mut buf) = FAILED_PLATFORM_REPORTS.lock() {
                    if buf.len() >= MAX_FAILED_PLATFORM_REPORTS {
                        buf.remove(0);
                    }
                    buf.push(entry);
                }
            }
        }
    }
}

/// Track last processed request for error synthesis on shutdown
pub static LAST_REQUEST_CONTEXT: once_cell::sync::Lazy<Arc<Mutex<Option<(String, String)>>>> =
    once_cell::sync::Lazy::new(|| Arc::new(Mutex::new(None)));


/// Default tier: `platform.report` sends that the telemetry listener started in the
/// background. The listener answers Lambda at once instead of waiting for the Metric API,
/// so `runtimeDone` is not held back; the invocation then waits here, in parallel with its
/// own sends, before calling `/next`, so a send is never left to be frozen mid-flight.
static INFLIGHT_REPORT_SENDS: once_cell::sync::Lazy<Mutex<Vec<tokio::task::JoinHandle<()>>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(Vec::new()));

/// Remember a background report send so `wait_for_report_sends` can wait for it.
pub(crate) fn track_report_send(handle: tokio::task::JoinHandle<()>) {
    if let Ok(mut sends) = INFLIGHT_REPORT_SENDS.lock() {
        sends.retain(|h| !h.is_finished());
        sends.push(handle);
    }
}

/// Wait until every tracked report send has finished, but no longer than `budget`. Sends
/// still running at the budget are left running (a send that has already reached the
/// Metric API must not be cut off and repeated); each failed send keeps its own report
/// for retry, so nothing is lost either way.
async fn wait_for_report_sends(budget: Duration) {
    let handles: Vec<_> = INFLIGHT_REPORT_SENDS
        .lock()
        .map(|mut sends| std::mem::take(&mut *sends))
        .unwrap_or_default();
    if handles.is_empty() {
        return;
    }
    let waited = tokio::time::timeout(budget, async {
        for handle in handles {
            let _ = handle.await;
        }
    })
    .await;
    if waited.is_err() {
        warn!("Platform report send still running after {}ms - not waiting longer", budget.as_millis());
    }
}

/// Most SHUTDOWN waits for the last invocation's report send.
const SHUTDOWN_REPORT_WAIT_MS: u64 = 1_000;

/// Tracks the two in-flight `high`-tier sends: the INVOKE-time batch and the
/// log flush. `default` never touches it. The batch is never aborted (it may
/// already be at the collector); only the log flush is, at the cap.
static SEND_SCHEDULER: once_cell::sync::Lazy<crate::send::SendScheduler> =
    once_cell::sync::Lazy::new(|| crate::send::SendScheduler::new(
        crate::send::SendMode::Deferred,
        // Real invocation-count breaker, high-tier defaults (N=5 → open 2N=10
        // → buffer cap 3N=15). SEND_SCHEDULER is only ever used by the
        // high-tier path (process_apm_request_high), so this does not touch the
        // default tier's pipeline. `from_env` lets NEW_RELIC_EXTENSION_CIRCUIT_BREAKER_THRESHOLD
        // override N; everything else derives from it.
        std::sync::Arc::new(crate::send::ConsecutiveFailureBreaker::from_env(
            crate::send::DEFAULT_THRESHOLD_HIGH,
        )),
        std::sync::Arc::new(crate::send::FixedTimeoutProvider(std::time::Duration::from_millis(crate::send::tier::DEFAULT_SEND_TIMEOUT_MS))),
    ));

/// One earlier request's un-sent data, taken out of `REQUEST_DATA` once the invoke is known
/// to be long.
struct PendingGroup {
    request_id: String,
    invoked_function_arn: String,
    payloads: Vec<Vec<u8>>,
    report: Option<String>,
    /// Set once the payload POSTs were handed to their per-type tasks. From then on a
    /// cancel cleans that group up per type (`inflight`), not as a whole.
    started: std::sync::atomic::AtomicBool,
}

/// The batch currently in flight. The id ties the log lines of one batch together
/// (start, failures, outcome); the start time lets a batch that outlives
/// `batch_max_age_ms` stop blocking new ones.
struct InFlightBatch {
    id: String,
    started_at: std::time::Instant,
}

static IN_FLIGHT_BATCH: once_cell::sync::Lazy<Mutex<Option<InFlightBatch>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(None));

static NEXT_BATCH_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// What the in-flight batch is working on, kept only so a batch the function's deadline cuts
/// off can be put back:
/// the groups, and the owner id its POSTs are registered under in `inflight`. How far
/// it got is the scheduler's cursor.
struct BatchMeta {
    owner: u64,
    groups: Arc<Vec<PendingGroup>>,
}

static BATCH_META: once_cell::sync::Lazy<Mutex<Option<BatchMeta>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(None));

/// Set once the agent has delivered a payload in this sandbox. An
/// uninstrumented function never sends one, so waiting for it would be wasted.
static AGENT_PAYLOAD_SEEN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Write-ahead copy of the log batch about to be flushed. `LogProcessor::flush()`
/// drains its batch before sending, so an abort would otherwise lose it; this
/// snapshot, taken before the spawn, is what `handle_logs_outcome` requeues on
/// `TimedOut`. Cleared once the flush is resolved either way.
struct LogBatchWriteAhead {
    batch: Vec<crate::newrelic::payload::LogMessage>,
    request_id: String,
}

static LOG_BATCH_WRITE_AHEAD: once_cell::sync::Lazy<Mutex<Option<LogBatchWriteAhead>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(None));

fn buffer_group(group: &PendingGroup) {
    for payload in &group.payloads {
        buffer_failed_agent_payload(payload, &group.request_id, &group.invoked_function_arn);
    }
    if let Some(report) = &group.report {
        buffer_failed_platform_report(report.clone(), &group.request_id, &group.invoked_function_arn);
    }
}

/// Take every other request's payloads and report. The current request is
/// excluded: its handler is still producing its own payload.
fn take_pending_groups(current_request_id: &str) -> Vec<PendingGroup> {
    let ids: Vec<String> = REQUEST_DATA
        .iter()
        .filter(|entry| entry.key() != current_request_id)
        .map(|entry| entry.key().clone())
        .collect();

    let mut groups = Vec::new();
    for request_id in ids {
        let payloads = get_agent_buffer(&request_id)
            .and_then(|buf| buf.lock().ok().map(|mut b| std::mem::take(&mut *b)))
            .unwrap_or_default();
        let report = remove_pending_report(&request_id);
        let invoked_function_arn = get_request_context(&request_id)
            .and_then(|ctx| ctx.lock().ok().map(|c| c.invoked_function_arn.clone()))
            .filter(|arn| !arn.is_empty())
            .unwrap_or_else(crate::get_global_fallback_arn);
        cleanup_request_processing_state_internal(&request_id, false);

        // A short invoke without an error is dropped, whenever its data got here.
        if crate::send::short_invoke::get(&request_id) == Some(crate::send::short_invoke::ShortOutcome::Dropped) {
            continue;
        }
        if !payloads.is_empty() || report.is_some() {
            groups.push(PendingGroup {
                request_id,
                invoked_function_arn,
                payloads,
                report,
                started: std::sync::atomic::AtomicBool::new(false),
            });
        }
    }
    // Reports the listener kept: they ride with their request's payload when it is still
    // here, alone when the payload already left.
    for stashed in take_stashed_reports(current_request_id) {
        if crate::send::short_invoke::get(&stashed.request_id) == Some(crate::send::short_invoke::ShortOutcome::Dropped) {
            continue;
        }
        match groups.iter_mut().find(|g| g.request_id == stashed.request_id && g.report.is_none()) {
            Some(group) => group.report = Some(stashed.report),
            None => groups.push(PendingGroup {
                request_id: stashed.request_id,
                invoked_function_arn: stashed.invoked_function_arn,
                payloads: Vec::new(),
                report: Some(stashed.report),
                started: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }
    groups
}

/// A `platform.report` the telemetry listener received and did not send. In the high tier the
/// listener never sends it: it answers Lambda's delivery request only after the send, and a
/// slow Metric API POST there held back the next batch of events, `runtimeDone` with it. The
/// next INVOKE's batch sends it instead, after that request's payloads.
pub(crate) struct StashedReport {
    pub(crate) request_id: String,
    pub(crate) report: String,
    pub(crate) invoked_function_arn: String,
}

static STASHED_REPORTS: once_cell::sync::Lazy<std::sync::Mutex<Vec<StashedReport>>> =
    once_cell::sync::Lazy::new(|| std::sync::Mutex::new(Vec::new()));

/// Same bound as the failed-report buffer: past it the oldest report is dropped.
const MAX_STASHED_REPORTS: usize = 100;

/// Whether the listener keeps reports for the batch (high tier, APM mode, not LMI).
static REPORTS_RIDE_THE_BATCH: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub fn set_reports_ride_the_batch(on: bool) {
    REPORTS_RIDE_THE_BATCH.store(on, std::sync::atomic::Ordering::Relaxed);
}

pub fn reports_ride_the_batch() -> bool {
    REPORTS_RIDE_THE_BATCH.load(std::sync::atomic::Ordering::Relaxed)
}

fn stash_lock() -> std::sync::MutexGuard<'static, Vec<StashedReport>> {
    STASHED_REPORTS.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Keep a report for the next batch. Called by the listener instead of sending it.
pub fn stash_platform_report(request_id: &str, report: String) {
    let invoked_function_arn = get_request_context(request_id)
        .and_then(|ctx| ctx.lock().ok().map(|c| c.invoked_function_arn.clone()))
        .filter(|arn| !arn.is_empty())
        .unwrap_or_else(crate::get_global_fallback_arn);
    let mut stash = stash_lock();
    if stash.len() >= MAX_STASHED_REPORTS {
        stash.remove(0);
        warn!("Stashed platform-report buffer full ({}) - dropped the oldest", MAX_STASHED_REPORTS);
    }
    stash.push(StashedReport { request_id: request_id.to_string(), report, invoked_function_arn });
}

/// Take every kept report except the running request's: its own batch is still to come.
pub(crate) fn take_stashed_reports(current_request_id: &str) -> Vec<StashedReport> {
    let (keep, take): (Vec<_>, Vec<_>) = std::mem::take(&mut *stash_lock())
        .into_iter()
        .partition(|s| s.request_id == current_request_id);
    *stash_lock() = keep;
    take
}

/// Discard the report kept for `request_id` (a short invoke that is dropped).
fn drop_stashed_report(request_id: &str) {
    stash_lock().retain(|stashed| stashed.request_id != request_id);
}

/// SHUTDOWN has no next batch: hand what is kept to the failed-report buffer, which the
/// shutdown retry sends.
fn drain_stashed_reports_into_retry_buffer() {
    for stashed in take_stashed_reports("") {
        if crate::send::short_invoke::get(&stashed.request_id) == Some(crate::send::short_invoke::ShortOutcome::Dropped) {
            continue;
        }
        buffer_failed_platform_report(stashed.report, &stashed.request_id, &stashed.invoked_function_arn);
    }
}

/// Sends one group at a time and buffers its own failures, so the task never
/// loses data by itself and needs no one to clean up after it. Nothing aborts it.
async fn send_pending_groups(
    batch_id: String,
    owner: u64,
    groups: Arc<Vec<PendingGroup>>,
    cursor: Arc<std::sync::atomic::AtomicUsize>,
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
    apm_app: crate::apm::SharedApmApp,
) -> crate::send::SendOutcome {
    // Every POST this batch starts is registered under `owner`, so it can be cancelled.
    crate::apm::inflight::scope(owner, async move {
        send_groups(batch_id, groups, cursor, config, global_log_processor, apm_app).await
    })
    .await
}

async fn send_groups(
    batch_id: String,
    groups: Arc<Vec<PendingGroup>>,
    cursor: Arc<std::sync::atomic::AtomicUsize>,
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
    apm_app: crate::apm::SharedApmApp,
) -> crate::send::SendOutcome {
    let mut all_sent = true;
    for group in groups.iter() {
        for payload in &group.payloads {
            extract_and_coordinate_trace_id(payload, &group.request_id, &config, &global_log_processor).await;
        }

        // Clone out: a read lock held across the POSTs would block a reconnect.
        let app = apm_app.read().await.clone();
        let sent = if let Some(app) = app {
            send_group(&app, group).await
        } else {
            buffer_group(group);
            false
        };
        all_sent &= sent;
        cursor.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    if !all_sent {
        warn!("APM mode (high): batch {} had failures; the failed data is buffered for retry", batch_id);
    }
    if all_sent {
        crate::send::SendOutcome::Success
    } else {
        crate::send::SendOutcome::Failure
    }
}

/// Failed types the per-type tasks buffered for `request_id` since `since`. A type
/// that got 202 leaves no entry, so zero means every type landed. Entries from
/// earlier retries keep their old `failed_at` and are not counted. Approximate if a
/// background retry drains the buffer at the same moment, which only hides a failure
/// that is already being retried.
fn buffered_failures_since(request_id: &str, since: chrono::DateTime<chrono::Utc>) -> usize {
    crate::apm::telemetry_buffer::FAILED_TELEMETRY_BUFFER
        .lock()
        .map_or(0, |buffer| {
            buffer
                .iter()
                .filter(|item| item.request_id == request_id && item.failed_at >= since)
                .count()
        })
}

/// `false` if anything failed. The failed data is already buffered: whole payloads
/// when nothing was sent, individual types when only some failed.
///
/// The payload POSTs go to the APM collector and the report POST to the Metric API: different
/// hosts, nothing needs one before the other, so they go out at the same time and the group
/// takes one round trip, not two.
async fn send_group(app: &crate::apm::ApmApp, group: &PendingGroup) -> bool {
    send_group_in_parallel(send_group_payloads(app, group), send_group_report(app, group)).await
}

/// Runs a group's two sends together; the group is sent only if both were.
async fn send_group_in_parallel(
    payloads: impl std::future::Future<Output = bool>,
    report: impl std::future::Future<Output = bool>,
) -> bool {
    let (payloads_sent, report_sent) = tokio::join!(payloads, report);
    payloads_sent && report_sent
}

/// The group's agent payloads, one POST per telemetry type, all at once.
async fn send_group_payloads(app: &crate::apm::ApmApp, group: &PendingGroup) -> bool {
    if group.payloads.is_empty() {
        return true;
    }
    let started = chrono::Utc::now();
    // The merged send hands every type to its own task before its first await, so
    // this flag and that hand-off cannot be split by a cancel.
    group.started.store(true, std::sync::atomic::Ordering::SeqCst);
    let result = app
        .process_merged_agent_payloads(group.payloads.clone(), &group.request_id, &group.invoked_function_arn)
        .await;
    if let Err(e) = result {
        warn!("Agent payload send failed for {}: {} - buffering", group.request_id, e);
        for payload in &group.payloads {
            buffer_failed_agent_payload(payload, &group.request_id, &group.invoked_function_arn);
        }
        return false;
    }
    // The merged send returns Ok even when some types failed, so ask the buffer.
    let failed = buffered_failures_since(&group.request_id, started);
    if failed > 0 {
        warn!("{} type(s) of {} failed and were buffered for retry", failed, group.request_id);
        return false;
    }
    true
}

/// The group's platform-report metrics (Metric API). They ride *inside* the `ApmCollector`
/// batch, so the same circuit breaker gates them as the agent payloads (this runs inside the
/// `SEND_SCHEDULER.spawn(ApmCollector)` task), and on open `buffer_group` buffers payloads and
/// report together.
async fn send_group_report(app: &crate::apm::ApmApp, group: &PendingGroup) -> bool {
    let Some(report) = &group.report else { return true };
    match app.send_platform_report_metrics(report, &group.invoked_function_arn).await {
        Ok(()) => true,
        Err(e) => {
            warn!("Report metrics send failed for {}: {} - buffering", group.request_id, e);
            buffer_failed_platform_report(report.clone(), &group.request_id, &group.invoked_function_arn);
            false
        }
    }
}

/// `high` tier, once the invoke is known to be long (not before: a short invoke starts
/// nothing): start sending what earlier invocations left behind, so the round-trips
/// overlap this invocation's handler instead of following it.
async fn spawn_pending_batch(
    current_request_id: &str,
    config: &Arc<ExtensionConfig>,
    global_log_processor: &Arc<LogProcessor>,
    apm_app: &crate::apm::SharedApmApp,
) {
    // One batch at a time: the previous one may be left running across a freeze.
    // If it is still going, skip this round; the data waits for the next INVOKE.
    if let Some(outcome) = SEND_SCHEDULER
        .resolve_pending_no_abort(crate::send::SendKind::ApmCollector)
        .await
    {
        if outcome.result == crate::send::DispatchResult::StillRunning {
            let max_age = Duration::from_millis(config.extension.batch_max_age_ms);
            if !batch_exceeded_max_age(max_age) {
                return;
            }
            warn!("APM mode (high): previous batch still running after {}ms - detaching it", max_age.as_millis());
            SEND_SCHEDULER.detach(crate::send::SendKind::ApmCollector).await;
        } else {
            log_batch_outcome(&outcome);
        }
    }

    let groups = take_pending_groups(current_request_id);
    if groups.is_empty() {
        return;
    }
    if groups.iter().any(|g| !g.payloads.is_empty()) {
        AGENT_PAYLOAD_SEEN.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    // Shared with the send task so the data can still be buffered if the circuit
    // breaker refuses to spawn it.
    let groups = Arc::new(groups);
    let owner = NEXT_BATCH_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let batch_id = format!("b{owner}");
    let cursor = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    info!(
        "APM mode (high): batch {} started: {} request(s), {} payload chunk(s), {} report(s)",
        batch_id,
        groups.len(),
        groups.iter().map(|g| g.payloads.len()).sum::<usize>(),
        groups.iter().filter(|g| g.report.is_some()).count()
    );
    let send: crate::send::BoxedSend = Box::pin(send_pending_groups(
        batch_id.clone(),
        owner,
        groups.clone(),
        cursor.clone(),
        config.clone(),
        global_log_processor.clone(),
        apm_app.clone(),
    ));
    let spawned = SEND_SCHEDULER
        .spawn(crate::send::SendKind::ApmCollector, send, Some(cursor.clone()))
        .await;
    if spawned {
        if let Ok(mut in_flight) = IN_FLIGHT_BATCH.lock() {
            *in_flight = Some(InFlightBatch { id: batch_id, started_at: std::time::Instant::now() });
        }
        if let Ok(mut meta) = BATCH_META.lock() {
            *meta = Some(BatchMeta { owner, groups });
        }
    } else {
        // Circuit breaker open for the APM collector (decided inside
        // `SendScheduler::spawn`): nothing was spawned. Buffer the groups, payloads
        // and report together, for the retry loops to pick up later.
        debug!(
            "APM mode (high): circuit breaker open for APM collector - buffering batch {} ({} group(s))",
            batch_id,
            groups.len()
        );
        groups.iter().for_each(buffer_group);
    }
}

/// What a cancelled batch had not delivered goes back to the normal retry buffers,
/// with retry count 0: deferring is not a failure. Groups before `resolved_cursor` are
/// done. A group whose POSTs were already handed out is not re-buffered whole (that would
/// repeat the types that landed): `inflight` buffers just its unfinished types, and only
/// its report, sent at the same time but not tracked there, is still ours.
fn buffer_unresolved_groups(groups: &[PendingGroup], resolved_cursor: usize) {
    for (index, group) in groups.iter().enumerate().skip(resolved_cursor) {
        if index == resolved_cursor && group.started.load(std::sync::atomic::Ordering::SeqCst) {
            if let Some(report) = &group.report {
                buffer_failed_platform_report(report.clone(), &group.request_id, &group.invoked_function_arn);
            }
        } else {
            buffer_group(group);
        }
    }
}

/// The batch's outcome after the wait that ends at the function's deadline. A batch that
/// finished needs nothing but its outcome logged. One the deadline cut off is stopped, and
/// whatever it had not delivered goes back to the normal retry buffers: groups before the
/// cursor are done, types that already got an answer are never resent. Returns whether
/// the batch was cut off.
fn finish_batch_outcome(outcome: &crate::send::DispatchOutcome) -> bool {
    let meta = BATCH_META.lock().ok().and_then(|mut m| m.take());
    if outcome.result != crate::send::DispatchResult::TimedOut {
        log_batch_outcome(outcome);
        return false;
    }
    if let Ok(mut in_flight) = IN_FLIGHT_BATCH.lock() {
        *in_flight = None;
    }
    let Some(meta) = meta else { return true };
    let stopped = crate::apm::inflight::abort_owner(meta.owner);
    buffer_unresolved_groups(&meta.groups, outcome.cursor.unwrap_or(0));
    warn!(
        "APM mode (high): batch reached the function deadline - {} POST(s) stopped, unsent data kept for the next invoke",
        stopped
    );
    true
}

/// Whether an invocation is short enough that nothing should be sent for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InvokeLength {
    Short,
    NotShort,
}

/// Runs the INVOKE-time retries, but only once the invoke is known not to be short.
type InvokeRetries = Box<dyn FnOnce() + Send>;

/// How long the agent says the invocation's transaction took: the `duration` (seconds) of
/// the first transaction event in its payload.
fn transaction_duration(telemetry: &std::collections::HashMap<String, Vec<serde_json::Value>>) -> Option<Duration> {
    let events = telemetry.get("analytic_event_data")?.get(2)?.as_array()?;
    let seconds = events.first()?.get(0)?.get("duration")?.as_f64()?;
    (seconds >= 0.0).then(|| Duration::from_secs_f64(seconds))
}

/// What an agent payload says about the current invocation.
enum PayloadSignal {
    /// It belongs to another request (a late payload routed here): not a signal.
    OtherRequest,
    /// How long the agent measured the transaction.
    Took(Duration),
    /// It is this invocation's payload but carries no duration.
    NoDuration,
}

fn read_payload_signal(payload_bytes: &[u8], request_id: &str) -> PayloadSignal {
    if let Some(owner) = crate::apm::payload_parser::extract_request_id_from_payload_bytes(payload_bytes) {
        if owner != request_id {
            return PayloadSignal::OtherRequest;
        }
    }
    match crate::apm::payload_parser::parse_agent_payload_cached(payload_bytes) {
        Ok(parsed) => transaction_duration(&parsed.0).map_or(PayloadSignal::NoDuration, PayloadSignal::Took),
        Err(_) => PayloadSignal::NoDuration,
    }
}

/// Decide whether this invocation is short, as soon as a signal allows and no later than
/// `threshold` plus the Telemetry API's worst delivery lag (an event can arrive that much
/// after it happened, so waiting less could mistake a short invoke for a long one).
///
/// Whichever signal comes first decides, and it supplies the duration:
/// - the agent's payload for this request: written as the invocation ends with no
///   buffering, so it arrives well before `runtimeDone`, and it carries the transaction's
///   own duration (without the agent's post-code work);
/// - `platform.runtimeDone`: Lambda's own `durationMs` when the event has one, else how
///   long it took to reach us.
///
/// Neither signal consumes the `runtimeDone` notify permit, so the normal wait afterwards
/// still works.
async fn detect_short_invoke(
    request_id: &str,
    state: &request::RequestProcessingState,
    threshold: Duration,
    invoke_start: tokio::time::Instant,
) -> InvokeLength {
    if threshold.is_zero() {
        return InvokeLength::NotShort;
    }
    let verdict = |took: Duration| if took < threshold { InvokeLength::Short } else { InvokeLength::NotShort };
    let window = threshold + Duration::from_millis(crate::runtime::TELEMETRY_BUFFER_TIMEOUT_MS);
    let mut payload_judged = false;
    loop {
        let elapsed = invoke_start.elapsed();
        if !payload_judged {
            let first = state.agent_buffer.lock().ok().and_then(|b| b.first().cloned());
            if let Some(bytes) = first {
                payload_judged = true; // judge each arrival once; parsing is not free
                match read_payload_signal(&bytes, request_id) {
                    PayloadSignal::Took(took) => return verdict(took),
                    PayloadSignal::NoDuration => return verdict(elapsed),
                    PayloadSignal::OtherRequest => {}
                }
            }
        }
        if request::get_runtime_done_elapsed_ms(request_id).is_some() {
            let ran = request::get_runtime_done_duration_ms(request_id)
                .map_or(elapsed, |ms| Duration::from_secs_f64(ms.max(0.0) / 1000.0));
            return verdict(ran);
        }
        if elapsed >= window {
            return InvokeLength::NotShort;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn batch_exceeded_max_age(max_age: Duration) -> bool {
    IN_FLIGHT_BATCH
        .lock()
        .ok()
        .and_then(|batch| batch.as_ref().map(|b| b.started_at))
        .is_some_and(|started| started.elapsed() >= max_age)
}

/// SHUTDOWN: there is no next invocation to defer to, so wait (bounded by
/// Lambda's 2s limit) for the last batch, and leave it running if it is slow.
async fn resolve_pending_agent_batch(wait_ms: u64) {
    wait_for_pending_agent_batch(Duration::from_millis(wait_ms)).await;
}

async fn wait_for_pending_agent_batch(wait: Duration) {
    let done = SEND_SCHEDULER
        .wait_finished(&[crate::send::SendKind::ApmCollector], wait)
        .await;
    if let Some(outcome) = done.get(&crate::send::SendKind::ApmCollector) {
        log_batch_outcome(outcome);
    }
}

/// A finished batch has nothing left to clean up: each failure was buffered by
/// the task that hit it. This only records which batch finished and how.
fn log_batch_outcome(outcome: &crate::send::DispatchOutcome) {
    let finished = IN_FLIGHT_BATCH.lock().ok().and_then(|mut b| b.take());
    let (id, took_ms) = finished.map_or(("?".to_string(), 0), |b| (b.id, b.started_at.elapsed().as_millis()));
    match outcome.result {
        crate::send::DispatchResult::Sent => {
            info!("APM mode (high): batch {} finished, all data delivered ({}ms since start)", id, took_ms);
        }
        crate::send::DispatchResult::Failed => {
            warn!("APM mode (high): batch {} finished with failures, buffered for retry ({}ms since start)", id, took_ms);
        }
        other => debug!("APM mode (high): batch {} ended as {:?}", id, other),
    }
}

/// Spawn this invocation's log flush into `SEND_SCHEDULER`, unconditionally —
/// logs are never tier-gated (unlike agent payload): they were already
/// unconditionally detached before `SendTier` existed, and stay that way
/// regardless of `default`/`high`. What changes is *tracking*: instead of
/// pure fire-and-forget (never checked again), the flush is now resolved —
/// without ever being aborted — at the start of the next invocation (see
/// `resolve_pending_logs`) or at `SHUTDOWN`.
///
/// Guards against ever running two log flushes concurrently: if
/// `has_pending` says one is still running (`StillRunning` from a previous
/// `resolve_pending_logs` call), this skips spawning a new one entirely —
/// new logs simply stay in `LogProcessor`'s own batch to go out on the
/// *next* flush, same as if the threshold-triggered auto-flush path were
/// still in progress.
async fn spawn_log_flush(log_processor: &Arc<LogProcessor>, request_id: &str) {
    if SEND_SCHEDULER.has_pending(crate::send::SendKind::Logs).await {
        debug!(
            "APM mode: previous log flush still running — new logs for {} stay batched for the next flush",
            request_id
        );
        return;
    }

    // Write-ahead snapshot, taken *before* the send is spawned — see
    // `LogBatchWriteAhead`'s doc for why this (not `flush()`'s own internal
    // drain) is what survives an abort.
    let write_ahead_batch = log_processor.snapshot_batch_for_write_ahead();
    match LOG_BATCH_WRITE_AHEAD.lock() {
        Ok(mut guard) => {
            if guard.is_some() {
                warn!(
                    "APM mode (high): overwriting unresolved LOG_BATCH_WRITE_AHEAD for {} — \
                     the shared-budget resolve should have cleared it first",
                    request_id
                );
            }
            *guard = Some(LogBatchWriteAhead {
                batch: write_ahead_batch,
                request_id: request_id.to_string(),
            });
        }
        Err(e) => error!("LOG_BATCH_WRITE_AHEAD lock poisoned, dropping write-ahead snapshot: {}", e),
    }

    let log_processor_clone = log_processor.clone();
    let request_id_clone = request_id.to_string();
    let send: crate::send::BoxedSend = Box::pin(async move {
        match log_processor_clone.flush().await {
            Ok(()) => crate::send::SendOutcome::Success,
            Err(e) => {
                error!("Failed to flush logs for request {}: {}", request_id_clone, e);
                crate::send::SendOutcome::Failure
            }
        }
    });
    let spawned = SEND_SCHEDULER.spawn(crate::send::SendKind::Logs, send, None).await;
    if !spawned {
        // Circuit breaker Open for logs (decided inside SendScheduler::spawn):
        // nothing was spawned. Clear the write-ahead we staged; the logs stay
        // in LogProcessor's own batch and go out on the next flush — same as
        // the has_pending skip above, nothing is lost.
        if let Ok(mut guard) = LOG_BATCH_WRITE_AHEAD.lock() {
            *guard = None;
        }
        debug!(
            "APM mode (high): circuit breaker open for logs — flush skipped, logs for {} stay batched",
            request_id
        );
    }
}

/// Resolve whatever log flush is pending from a previous invocation, if any —
/// called unconditionally at the start of every invocation and at `SHUTDOWN`,
/// regardless of `send_tier` (logs are never tier-gated — see
/// `spawn_log_flush`). Never aborts: a still-running flush is left exactly
/// alone (see `SendScheduler::resolve_pending_no_abort`'s doc for why —
/// `LogProcessor::flush`'s chunk list lives in a purely local variable with
/// no externally visible progress, unlike the agent-payload batch, so
/// killing it mid-flight would lose data with nothing to buffer).
///
/// No buffering happens here even on `Failed`: `LogProcessor::flush` already
/// buffers its own per-chunk send failures into its internal
/// `failed_logs_buffer` (see `send_and_clear_batch_simple`) — this function
/// only needs to observe and log the outcome, not act on it.
async fn resolve_pending_logs(log_processor: &Arc<LogProcessor>, request_id: &str) {
    let Some(outcome) = SEND_SCHEDULER
        .resolve_pending_no_abort(crate::send::SendKind::Logs)
        .await
    else {
        return;
    };
    handle_logs_outcome(&outcome, log_processor, request_id);
}

/// Shared by `resolve_pending_logs` (never aborts) and the abort-capable
/// `runtimeDone` resolve, so `TimedOut` is only reachable from the latter.
/// Always clears `LOG_BATCH_WRITE_AHEAD` before returning, resolved either
/// way (sent, failed-internally, or requeued on timeout) — it must never
/// outlive the outcome it was recorded for.
fn handle_logs_outcome(
    outcome: &crate::send::DispatchOutcome,
    log_processor: &Arc<LogProcessor>,
    request_id: &str,
) {
    // Still running: the write-ahead must survive until the flush really resolves.
    if outcome.result == crate::send::DispatchResult::StillRunning {
        return;
    }
    let write_ahead = match LOG_BATCH_WRITE_AHEAD.lock() {
        Ok(mut guard) => guard.take(),
        Err(e) => {
            error!("LOG_BATCH_WRITE_AHEAD lock poisoned: {}", e);
            None
        }
    };

    match outcome.result {
        crate::send::DispatchResult::Sent => {
            debug!(
                "APM mode: log flush for {} completed successfully",
                request_id
            );
        }
        crate::send::DispatchResult::Failed => {
            debug!(
                "APM mode: log flush for {} had failures, already buffered internally",
                request_id
            );
        }
        crate::send::DispatchResult::TimedOut => {
            // Recover via the write-ahead snapshot taken before this send
            // was spawned — see `LogBatchWriteAhead`'s doc. Uses the
            // snapshot's own request_id (the invocation that spawned the
            // send), not this call's `request_id` (which, at SHUTDOWN, is
            // literally the string "shutdown") — attribution must follow
            // the data, not the caller.
            if let Some(write_ahead) = &write_ahead {
                log_processor.requeue_aborted_batch(write_ahead.batch.clone(), &write_ahead.request_id);
            } else {
                warn!(
                    "APM mode: log flush for {} timed out but LOG_BATCH_WRITE_AHEAD was \
                     empty — nothing to re-queue, logs in that batch are lost",
                    request_id
                );
            }
        }
        other => {
            // Dropped/Deferred: unreachable in practice — neither
            // resolve_pending_no_abort nor resolve_pending_shared_budget's
            // caller for Logs checks the breaker or produces Deferred.
            warn!(
                "APM mode: unexpected {:?} resolving log flush for {}",
                other, request_id
            );
        }
    }
}

/// Event loop: handles cold start (first invoke) and warm starts (subsequent invokes)
pub async fn run_infinite_event_loop(
    mut extension_components: ExtensionComponents,
) -> u32 {
    if !extension_components.config.new_relic.extension_enabled
        || extension_components.config.new_relic.license_key.is_none()
    {
        info!("Running in no-op mode");
        execute_noop_event_loop(&extension_components.client, &extension_components.extension_id)
            .await;
        return 0;
    }

    execute_main_telemetry_processing_loop(&mut extension_components).await
}

/// Lambda extension pattern: GET /next (block) → process INVOKE → repeat until SHUTDOWN
///
/// Top-level dispatch on `DeploymentContext` first — LMI runs a dedicated
/// telemetry-driven loop because AWS rejects `INVOKE` registration for LMI.
/// On Normal Lambda, the existing APM-vs-Serverless decision is preserved
/// (Java-runtime override is encoded in `apm_mode_enabled`).
///
/// The match has no `_` arm: adding a new `DeploymentContext` variant in the
/// future is a compile error here, forcing an explicit decision (see
/// `LMI_SUPPORT.md` §3).
async fn execute_main_telemetry_processing_loop(components: &mut ExtensionComponents) -> u32 {
    match components.deployment {
        DeploymentContext::Lmi => {
            info!("Starting LMI event loop (telemetry-driven, no INVOKE polling)");
            crate::event_loop_lmi::execute_lmi_mode_event_loop(components).await
        }
        DeploymentContext::Normal { .. } => {
            if components.apm_mode_enabled {
                info!("Starting APM mode event loop (connection may still be in progress)");
                execute_apm_mode_event_loop(components).await
            } else {
                debug!("Starting serverless mode event loop");
                execute_standard_mode_event_loop(components).await
            }
        }
    }
}

/// APM mode: immediate sending to collector, no batching, keeps buffers alive for late payloads
pub async fn execute_apm_mode_event_loop(components: &mut ExtensionComponents) -> u32 {
    let mut event_counter = 0;
    let mut cleanup_counter = 0; // Track when to run periodic cleanup
    let mut pending_flush_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    loop {
        debug!("APM mode: waiting for next lambda invocation event...");

        let runtime_event = match runtime::fetch_next_event(&components.client, &components.extension_id).await
        {
            Ok(event) => event,
            Err(e) => {
                let error_msg = e.to_string();
                // Check if this is a fatal 403 state transition error (AWS shutting down without sending SHUTDOWN event)
                if error_msg.contains("403") || error_msg.contains("State transition") {
                    error!("Fatal extension state error (403 - Lambda shutting down): {:?}", e);
                    debug!("Performing emergency shutdown cleanup...");

                    let _ = tokio::time::timeout(Duration::from_millis(SHUTDOWN_TIMEOUT_MS), async {
                        if components.config.extension.pipeline_flush {
                            for handle in pending_flush_handles.drain(..) {
                                if let Err(e) = handle.await {
                                    error!("Pipeline flush task panicked during emergency shutdown: {}", e);
                                }
                            }
                        }

                        process_pending_agent_payloads(
                            &components.config,
                            &components.global_log_processor,
                            &components.apm_app,
                            "",
                        )
                        .await;
                    }).await;

                    info!("Emergency shutdown cleanup completed. Extension exiting.");
                    break;
                }
                error!("Error receiving next event: {:?}. Continuing.", e);
                continue;
            }
        };

        event_counter += 1;
        let is_cold_start = event_counter == 1;

        match runtime_event {
            runtime::LambdaRuntimeEvent::Invoke {
                request_id,
                invoked_function_arn,
                deadline_ms,
            } => {
                let event_start = std::time::Instant::now();

                // If a prior send saw a collector restart (401/409) or disconnect (410),
                // the cached run_id is stale. Invalidate the connection so the block below
                // re-establishes a fresh handshake; buffered telemetry then retries with the
                // new run_id (see retry_buffered_telemetry's current_run_id override).
                if components.apm_mode_enabled
                    && crate::apm::collector::take_reconnect_needed()
                    && components.apm_app.read().await.is_some()
                {
                    *components.apm_app.write().await = None;
                    warn!("APM run_id invalidated (collector restart/disconnect) - will reconnect");
                }

                // Retry buffered telemetry on each invoke. APM telemetry needs a live session
                // (run_id), so it only fires when apm_app is Some. Metric API is license-key-only
                // and retries unconditionally.
                // The high tier runs these same retries later, once the invoke is known not
                // to be short (see `run_invoke_retries`); the default tier runs them here.
                let is_high = components.config.extension.send_tier == crate::send::SendTier::High;
                if !is_high
                    && components.apm_mode_enabled
                    && crate::apm::telemetry_buffer::get_buffer_count() > 0
                    && components.apm_app.read().await.is_some()
                {
                    let (cur_run_id, cur_collector_host) = {
                        let guard = components.apm_app.read().await;
                        match guard.as_ref() {
                            Some(app) => (Some(app.run_id.clone()), Some(app.collector_host.clone())),
                            None => (None, None),
                        }
                    };
                    let apm_client = components.apm_client.clone();
                    let license_key = components.config.new_relic.license_key.clone().unwrap_or_default();
                    tokio::spawn(async move {
                        crate::apm::telemetry_buffer::retry_buffered_telemetry(
                            &apm_client,
                            &license_key,
                            cur_run_id.as_deref(),
                            cur_collector_host.as_deref(),
                        )
                        .await;
                    });
                }

                if !is_high
                    && components.apm_mode_enabled
                    && crate::apm::metric_api_buffer::get_metric_api_buffer_count() > 0
                {
                    let apm_client = components.apm_client.clone();
                    let license_key = components.config.new_relic.license_key.clone().unwrap_or_default();
                    tokio::spawn(async move {
                        crate::apm::metric_api_buffer::retry_buffered_metric_api(
                            &apm_client,
                            &license_key,
                        )
                        .await;
                    });
                }

                // If APM handshake hasn't completed yet, spawn a fresh reconnect attempt.
                // The spawn is non-blocking — the invoke proceeds immediately. If
                // NEW_RELIC_APM_BLOCKING_HANDSHAKE=true the post-invoke wait may still capture
                // this invoke's data; otherwise APM data arrives on a later invoke.
                // watch::Sender guard prevents multiple concurrent reconnects.
                if components.apm_mode_enabled
                    && components.apm_app.read().await.is_none()
                    && !crate::apm::connection::is_handshake_fatal()
                {
                    if *components.reconnect_in_flight.borrow() {
                        debug!("APM handshake already in progress — skipping duplicate spawn (BLOCKING_HANDSHAKE will wait if enabled)");
                    } else {
                        components.reconnect_in_flight.send_replace(true);
                        let apm_app = components.apm_app.clone();
                        let reconnect_flag = components.reconnect_in_flight.clone();
                        let license_key = components.config.new_relic.license_key
                            .clone()
                            .unwrap_or_default();
                        let apm_host = components.config.new_relic.apm_host.clone();
                        let metric_endpoint = components.config.new_relic.metric_endpoint.clone();
                        let apm_client = components.apm_client.clone();
                        let lambda_function_name = components.config.aws.function_name.clone();
                        let function_name = std::env::var("NEW_RELIC_APP_NAME")
                            .ok()
                            .filter(|s| !s.is_empty())
                            .unwrap_or_else(|| lambda_function_name.clone());
                        let function_version = components.config.aws.function_version
                            .clone()
                            .unwrap_or_else(|| "$LATEST".to_string());
                        let account_id = components.config.aws.account_id.clone();
                        let region = components.config.aws.region.clone();
                        let timeout_secs = components.config.new_relic.apm_handshake_timeout_secs;
                        // Copy out before the move closure (don't borrow `components`).
                        let deployment = components.config.deployment;

                        tokio::spawn(async move {
                            let _guard = ReconnectGuard(reconnect_flag);
                            debug!("APM reconnect attempt started (no delays — fresh invoke)");
                            match crate::apm::ApmApp::new(
                                license_key,
                                apm_host,
                                metric_endpoint,
                                apm_client,
                                function_name,
                                lambda_function_name,
                                function_version,
                                account_id,
                                region,
                                timeout_secs,
                                deployment,
                            )
                            .await
                            {
                                Ok(app) => {
                                    info!(
                                        "APM reconnect succeeded - Entity GUID: {}",
                                        app.get_entity_guid()
                                    );
                                    let mut w = apm_app.write().await;
                                    *w = Some(app);
                                }
                                Err(e) => {
                                    // A permanent auth failure already logged an error and
                                    // latched APM off in ApmApp::new — don't claim we'll retry.
                                    if !crate::apm::connection::is_handshake_fatal() {
                                        warn!("APM reconnect attempt failed: {} - will retry next invoke", e);
                                    }
                                }
                            }
                        });
                    }
                }

                if is_cold_start {
                    let mut updated_config = (*components.config).clone();
                    updated_config.aws.extract_and_update_account_id_from_arn(&invoked_function_arn);
                    components.config = Arc::new(updated_config);

                    match std::fs::read_to_string("/opt/newrelic/java-agent-version.txt") {
                        Ok(version) => info!("Java agent version: {}", version.trim()),
                        Err(e) => info!("Java agent version file not found at /opt/newrelic/java-agent-version.txt: {}", e),
                    }
                }

                if let Ok(mut guard) = LAST_REQUEST_CONTEXT.lock() {
                    *guard = Some((request_id.clone(), invoked_function_arn.clone()));
                }

                error_synthesis::clear_sent_errors_for_request(&request_id);

                error_synthesis::retry_failed_errors(&components.newrelic_client, &components.config).await;

                if is_cold_start && components.config.new_relic.add_version_detail_tags {
                    tag_lambda_function_once(invoked_function_arn.clone(), &components.config);
                }

                update_global_invocation_context(&request_id, &invoked_function_arn);

                // Set this as the currently active request for agent payload routing
                if let Ok(mut active_request) = request::CURRENT_ACTIVE_REQUEST_ID.lock() {
                    *active_request = Some(request_id.clone());
                }

                // Retry any previously-failed agent payloads in the background so the
                // HTTP round-trips happen during function execution, not post-runtime-done.
                if !is_high {
                    let apm = components.apm_app.clone();
                    tokio::spawn(retry_failed_agent_payloads(apm));
                }

                // Create per-request state (platform_processor, agent_buffer, context)
                let request_state = create_request_processing_state(
                    &request_id,
                    &invoked_function_arn,
                    &components.processor_factory,
                );

                // Update global log processor's context to this request BEFORE processing logs
                components
                    .global_log_processor
                    .update_invocation_context(request_state.context.clone());

                // Pipeline flush: prior flushes run in parallel — don't block.
                // Drain finished handles (0ms each); leave in-flight ones running.
                if components.config.extension.pipeline_flush {
                    pending_flush_handles.retain(|h| !h.is_finished());
                    // Cap at 8 to bound memory under sustained fast invocations
                    while pending_flush_handles.len() >= 8 {
                        if let Some(oldest) = pending_flush_handles.drain(..1).next() {
                            if let Err(e) = oldest.await {
                                error!("Oldest pipeline flush task panicked: {}", e);
                            }
                        }
                    }
                }

                components
                    .global_log_processor
                    .start_invocation_retry();

                // Process pre-invoke logs FIRST (add metadata and move to batch)
                components
                    .global_log_processor
                    .process_pre_invoke_logs();

                // THEN process buffered logs (so they don't trigger auto-flush of incomplete logs)
                components
                    .global_log_processor
                    .process_buffered_logs_with_request_id(&request_id);

                REQUEST_PROCESSORS.insert(request_id.clone(), request_state);

                let buffer_count = request_data_len();
                if buffer_count > 0 {
                    debug!(
                        "APM mode: Found {} request buffer(s) before processing (current: {})",
                        buffer_count, request_id
                    );
                }

                let pending_task = tokio::spawn({
                    let config = components.config.clone();
                    let global_log_processor = components.global_log_processor.clone();
                    let apm_app = components.apm_app.clone();
                    let current_request_id = request_id.clone();

                    let is_high = config.extension.send_tier == crate::send::SendTier::High;

                    async move {
                        // high delivers earlier requests' data via spawn_pending_batch.
                        if is_high {
                            return;
                        }
                        process_pending_agent_payloads(
                            &config,
                            &global_log_processor,
                            &apm_app,
                            &current_request_id,
                        )
                        .await;
                    }
                });

                let request_id_clone = request_id.clone();
                let invoked_function_arn_clone = invoked_function_arn.clone();
                let config_clone = components.config.clone();
                let global_log_processor_clone = components.global_log_processor.clone();
                let apm_app_clone = components.apm_app.clone();

                // High tier: the INVOKE-time retries wait until the invoke is known not to be short.
                let invoke_retries: Option<InvokeRetries> = if is_high {
                    let apm_app = components.apm_app.clone();
                    let apm_client = components.apm_client.clone();
                    let license_key = components.config.new_relic.license_key.clone().unwrap_or_default();
                    let apm_mode_enabled = components.apm_mode_enabled;
                    Some(Box::new(move || {
                        tokio::spawn(run_invoke_retries(apm_app, apm_client, license_key, apm_mode_enabled));
                    }))
                } else {
                    None
                };

                let current_task = tokio::spawn(async move {
                    process_apm_request_with(
                        request_id_clone,
                        invoked_function_arn_clone,
                        config_clone,
                        global_log_processor_clone,
                        apm_app_clone,
                        deadline_ms,
                        invoke_retries,
                    )
                    .await;
                });

                if components.config.extension.send_tier == crate::send::SendTier::High {
                    // pending_task is a no-op for high (see above); only the
                    // current request is awaited, bounded by its own send budget.
                    if components.config.extension.pipeline_flush {
                        pending_flush_handles.push(current_task);
                    } else if let Err(e) = current_task.await {
                        error!("Error in APM request processing: {}", e);
                    }
                } else if components.config.extension.pipeline_flush {
                    let combined = tokio::spawn(async move {
                        let (r1, r2) = tokio::join!(current_task, pending_task);
                        if let Err(e) = r1 { error!("Error in APM request processing: {}", e); }
                        if let Err(e) = r2 { error!("Error in pending payload processing: {}", e); }
                    });
                    pending_flush_handles.push(combined);
                } else {
                    let (current_result, pending_result) = tokio::join!(current_task, pending_task);
                    if let Err(e) = current_result {
                        error!("Error in APM request processing: {}", e);
                    }
                    if let Err(e) = pending_result {
                        error!("Error in pending payload processing: {}", e);
                    }
                }

                // Post-invoke wait: only when NEW_RELIC_APM_BLOCKING_HANDSHAKE=true.
                // Independent of pipeline_flush — handshake establishes APM connection (one-time
                // cost on cold start), pipeline_flush defers data send (every-invoke savings).
                // Once connected, this is a no-op (reconnect_in_flight=false → instant return).
                if components.apm_mode_enabled && components.config.new_relic.apm_blocking_handshake {
                    wait_for_apm_handshake_within_budget(
                        &components.reconnect_in_flight,
                        deadline_ms,
                    )
                    .await;
                }

                if !components.config.extension.pipeline_flush {
                    let event_time = event_start.elapsed();
                    if is_cold_start {
                        debug!(
                            "COLD START: First invocation processed in {:?} (request_id: {})",
                            event_time, request_id
                        );
                        IS_WARM_START.store(true, std::sync::atomic::Ordering::Relaxed);
                    } else {
                        debug!(
                            "WARM START: Event {} processed in {:?} (request_id: {})",
                            event_counter, event_time, request_id
                        );
                    }
                } else if is_cold_start {
                    IS_WARM_START.store(true, std::sync::atomic::Ordering::Relaxed);
                }

                // Periodic cleanup: Run every 10 invocations to prevent memory leaks
                cleanup_counter += 1;
                if cleanup_counter >= 10 {
                    cleanup_counter = 0;
                    let newrelic_client = components.newrelic_client.clone();
                    let config = components.config.clone();
                    tokio::spawn(async move {
                        use crate::agent::batch::cleanup_old_batch_entries;
                        use crate::request::cleanup_old_request_buffers;
                        cleanup_old_batch_entries(newrelic_client.clone(), config.clone()).await;
                        cleanup_old_request_buffers(newrelic_client, config).await;
                        cleanup_old_failed_payloads();
                    });
                }
            }
            runtime::LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
                // Stop forwarding the extension's own shutdown-sequence logs to NR
                // (set before any shutdown log is emitted). The structured drop
                // diagnostic is sent directly below as the single NR record.
                crate::IS_SHUTTING_DOWN.store(true, std::sync::atomic::Ordering::Relaxed);
                let shutdown_start_time = std::time::Instant::now();
                info!("APM mode: Extension shutting down with reason: {} (started at {:?})", shutdown_reason, std::time::SystemTime::now());

                // Computed inside the main block, sent AFTER it with reserved budget
                // so the critical "telemetry dropped" line always reaches New Relic.
                let mut shutdown_diagnostic: Option<ShutdownDropDiagnostic> = None;

                let shutdown_result = tokio::time::timeout(Duration::from_millis(SHUTDOWN_TIMEOUT_MS - SHUTDOWN_DIAG_RESERVE_MS), async {
                // SHUTDOWN has about two seconds in total, so everything that does not depend on
                // something else runs at the same time.

                // 1. The only prerequisite: a session, if we have none (the sends below need it).
                shutdown_reconnect_if_needed(components).await;

                // Let the IPC pipe collector route last in-flight payloads before we take them;
                // it runs in its own task, so two yields let items already in the channel through.
                tokio::task::yield_now().await;
                tokio::task::yield_now().await;

                // 2. No network: take the buffered payloads, extract their trace ids so logs held
                // for them are stamped, and release logs whose payload never arrived (untagged,
                // the last chance before the sandbox dies).
                let payloads = shutdown_take_payloads();
                for payload in &payloads {
                    extract_and_coordinate_trace_id(
                        &payload.bytes,
                        &payload.request_id,
                        &components.config,
                        &components.global_log_processor,
                    )
                    .await;
                }
                components.global_log_processor.flush_pending_logs_unstamped();

                // 3. All the sending, at once: the error event, every payload, every pending
                // report, the last invocation's report (default tier), the high tier's last
                // batch, the log flushes, and any pipeline flushes still running.
                tokio::join!(
                    shutdown_await_pipeline_flushes(&mut pending_flush_handles, components.config.extension.pipeline_flush),
                    shutdown_send_error_event(components, shutdown_reason.clone()),
                    shutdown_send_payloads(components, &payloads),
                    shutdown_send_reports(components),
                    wait_for_report_sends(Duration::from_millis(SHUTDOWN_REPORT_WAIT_MS)),
                    shutdown_drain_high_tier(components),
                    shutdown_flush_logs(components),
                );

                // Distinct request_ids (invocations) whose telemetry we could not
                // deliver — used to build the single shutdown summary below.
                let mut dropped_request_ids: std::collections::BTreeSet<String> =
                    std::collections::BTreeSet::new();

                // 4. One last retry of whatever failed above (the three buffers are independent,
                // so they are retried together), so the drop count below reflects only what
                // genuinely could not be delivered.
                debug!("APM mode shutdown: Retrying all buffered telemetry");
                let license_key = components.config.new_relic.license_key.clone().unwrap_or_default();
                let (cur_run_id, cur_collector_host) = {
                    let guard = components.apm_app.read().await;
                    match guard.as_ref() {
                        Some(app) => (Some(app.run_id.clone()), Some(app.collector_host.clone())),
                        None => (None, None),
                    }
                };
                tokio::join!(
                    crate::apm::telemetry_buffer::retry_buffered_telemetry(
                        &components.apm_client,
                        &license_key,
                        cur_run_id.as_deref(),
                        cur_collector_host.as_deref(),
                    ),
                    crate::apm::metric_api_buffer::retry_buffered_metric_api(&components.apm_client, &license_key),
                    retry_failed_agent_payloads(components.apm_app.clone()),
                );

                // Gather every request_id whose data is still un-delivered:
                //  - agent payloads buffered while disconnected (the main bucket),
                //  - parsed telemetry that failed to send.
                for id in buffered_agent_payload_request_ids() {
                    dropped_request_ids.insert(id);
                }
                for id in crate::apm::telemetry_buffer::buffered_request_ids() {
                    dropped_request_ids.insert(id);
                }
                // Metric-API items count toward remaining_count below, so their requests
                // must count toward `affected` too — otherwise a metric-only request
                // inflates item count without bumping the invocation count.
                for id in crate::apm::metric_api_buffer::buffered_request_ids() {
                    dropped_request_ids.insert(id);
                }

                let remaining_count = FAILED_AGENT_PAYLOADS.lock().map(|b| b.len()).unwrap_or(0)
                    + crate::apm::telemetry_buffer::get_buffer_count()
                    + crate::apm::metric_api_buffer::get_metric_api_buffer_count();
                // Payloads already evicted/aged-out earlier (no longer in the buffer
                // to count) — add them so the total loss is honest.
                let dropped_earlier = dropped_agent_payload_count();

                if remaining_count > 0 || !dropped_request_ids.is_empty() || dropped_earlier > 0 {
                    let apm_connected = cur_run_id.is_some();
                    let affected = dropped_request_ids.len();
                    let ids: Vec<String> = {
                        let mut v: Vec<String> = dropped_request_ids.iter().cloned().collect();
                        v.sort();
                        v
                    };

                    let reason = if apm_connected {
                        String::new()
                    } else {
                        crate::apm::connection::last_failure_reason()
                            .unwrap_or_else(|| "unknown".to_string())
                    };
                    let summary = build_shutdown_drop_summary(
                        apm_connected,
                        affected,
                        remaining_count,
                        dropped_earlier,
                        &reason,
                        crate::apm::connection::connect_cycles(),
                        crate::apm::connection::connect_attempts_total(),
                    );

                    // The drop diagnostic is sent ONLY to New Relic (directly, after this
                    // block, in its reserved budget) — as the log message plus queryable
                    // attributes (dropped.request_ids / dropped.request_id_count /
                    // dropped.item_count). We deliberately do NOT also print it to stdout:
                    // that CloudWatch copy was redundant with the NR record (and was the
                    // source of the duplicate the operator saw). Other shutdown errors
                    // (e.g. the reconnect failure) are still logged to CloudWatch.
                    let last_ctx = LAST_REQUEST_CONTEXT.lock().ok().and_then(|g| g.clone());
                    let last_request_id =
                        last_ctx.as_ref().map(|(rid, _)| rid.clone()).unwrap_or_default();
                    let arn = last_ctx
                        .map(|(_, arn)| arn)
                        .filter(|a| !a.is_empty())
                        .unwrap_or_else(crate::get_global_fallback_arn);
                    shutdown_diagnostic = Some(ShutdownDropDiagnostic {
                        message: summary,
                        arn,
                        request_id: last_request_id,
                        request_ids: ids,
                        request_id_count: affected,
                        item_count: remaining_count,
                    });
                }
                }).await;

                // Reserved-budget delivery of the "telemetry dropped" diagnostic to
                // New Relic Logs. Runs OUTSIDE the main block (even if that timed out)
                // with its own protected window, so the one log the customer most
                // needs is not starved by a slow flush/reconnect. Direct POST to the
                // log ingest (license-key header) — does not need an APM handshake.
                if let Some(diag_data) = shutdown_diagnostic {
                    let diag = build_shutdown_drop_log(&diag_data);
                    match tokio::time::timeout(
                        Duration::from_millis(SHUTDOWN_DIAG_RESERVE_MS),
                        components.newrelic_client.send_logs(
                            components.config.as_ref(),
                            std::slice::from_ref(&diag),
                            &diag_data.arn,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(())) => info!("Forwarded shutdown drop diagnostic to New Relic Logs"),
                        Ok(Err(e)) => warn!("Could not forward shutdown diagnostic to New Relic: {}", e),
                        Err(_) => warn!("Timed out forwarding shutdown diagnostic to New Relic"),
                    }
                }

                if shutdown_result.is_err() {
                    warn!("APM shutdown timed out after {}ms — Lambda will terminate remaining work", shutdown_start_time.elapsed().as_millis());
                } else {
                    info!("APM mode shutdown: All data processed and sent in {}ms", shutdown_start_time.elapsed().as_millis());
                }
                break;
            }
        }
    }

    event_counter
}

/// Serverless mode: batches payloads with platform.report, sends to serverless API
pub async fn execute_standard_mode_event_loop(components: &mut ExtensionComponents) -> u32 {
    let mut event_counter = 0;
    let mut cleanup_counter = 0; // Track when to run periodic cleanup
    let mut pending_flush_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    loop {
        debug!("Serverless mode: waiting for next lambda invocation event...");

        let runtime_event =
            match runtime::fetch_next_event(&components.client, &components.extension_id).await {
                Ok(event) => event,
                Err(e) => {
                    let error_msg = e.to_string();
                    if error_msg.contains("403") || error_msg.contains("State transition") {
                        error!("Fatal extension state error (403 - Lambda shutting down): {:?}", e);
                        info!("Performing emergency shutdown cleanup...");

                        let _ = tokio::time::timeout(Duration::from_millis(SHUTDOWN_TIMEOUT_MS), async {
                            if components.config.extension.pipeline_flush {
                                for handle in pending_flush_handles.drain(..) {
                                    if let Err(e) = handle.await {
                                        error!("Pipeline flush task panicked during emergency shutdown: {}", e);
                                    }
                                }
                            }

                            send_batched_payloads_with_reports_only(
                                components.newrelic_client.clone(),
                                components.config.clone(),
                            )
                            .await;

                            let _ = components.global_log_processor.flush().await;
                        }).await;

                        info!("Emergency shutdown cleanup completed. Extension exiting.");
                        break;
                    }
                    error!("Error receiving next event: {:?}. Continuing.", e);
                    continue;
                }
            };

        event_counter += 1;
        crate::request::increment_invocation_counter();
        let is_cold_start = event_counter == 1;

        match runtime_event {
            runtime::LambdaRuntimeEvent::Invoke {
                request_id,
                invoked_function_arn,
                deadline_ms,
            } => {
                let event_start = std::time::Instant::now();

                // Extract real account ID from ARN on first invocation (if not already set)
                if is_cold_start {
                    let mut updated_config = (*components.config).clone();
                    updated_config.aws.extract_and_update_account_id_from_arn(&invoked_function_arn);
                    components.config = Arc::new(updated_config);
                }

                // Track this request for potential error synthesis on shutdown
                if let Ok(mut guard) = LAST_REQUEST_CONTEXT.lock() {
                    *guard = Some((request_id.clone(), invoked_function_arn.clone()));
                }

                error_synthesis::clear_sent_errors_for_request(&request_id);

                // Spawn error and telemetry retries into the background so they run during
                // function execution, not pre-invoke. Lambda freeze suspends the tokio runtime,
                // so sleeping/retrying tasks are paused and resume cleanly on unfreeze —
                // retry counts are only incremented on actual HTTP failures, not on suspensions.
                {
                    let nr = components.newrelic_client.clone();
                    let cfg = components.config.clone();
                    tokio::spawn(async move {
                        error_synthesis::retry_failed_errors(&nr, &cfg).await;
                    });
                }
                {
                    let apm_client = components.apm_client.clone();
                    let license_key = components.config.new_relic.license_key.clone();
                    tokio::spawn(async move {
                        crate::apm::telemetry_buffer::retry_buffered_telemetry(
                            &apm_client,
                            license_key.as_deref().unwrap_or(""),
                            None,
                            None,
                        )
                        .await;
                    });
                }

                if is_cold_start && components.config.new_relic.add_version_detail_tags {
                    tag_lambda_function_once(invoked_function_arn.clone(), &components.config);
                }

                update_global_invocation_context(&request_id, &invoked_function_arn);

                // Set this as the currently active request for agent payload routing
                if let Ok(mut active_request) = request::CURRENT_ACTIVE_REQUEST_ID.lock() {
                    *active_request = Some(request_id.clone());
                }

                // On warm starts, pair any previous-request agent payloads that arrived after
                // their invocation ended but whose platform.report has since been stored.
                // Must run after updating CURRENT_ACTIVE_REQUEST_ID so new pipe payloads
                // route to the current request, not old buffers.
                if !is_cold_start {
                    let paired = drain_late_paired_payloads_serverless(
                        &request_id,
                        &components.config,
                        &components.global_log_processor,
                    )
                    .await;
                    if paired > 0 {
                        debug!(
                            "Serverless mode: Paired {} late agent payload(s) into batch at start of invocation: {}",
                            paired, request_id
                        );
                    }
                }

                // Create per-request state (platform_processor, agent_buffer, context)
                let request_state = create_request_processing_state(
                    &request_id,
                    &invoked_function_arn,
                    &components.processor_factory,
                );

                // Update global log processor's context to this request BEFORE processing logs
                components
                    .global_log_processor
                    .update_invocation_context(request_state.context.clone());

                // Pipeline flush: prior flushes run in parallel — don't block.
                // Drain finished handles (0ms each); leave in-flight ones running.
                if components.config.extension.pipeline_flush {
                    pending_flush_handles.retain(|h| !h.is_finished());
                    // Cap at 8 to bound memory under sustained fast invocations
                    while pending_flush_handles.len() >= 8 {
                        if let Some(oldest) = pending_flush_handles.drain(..1).next() {
                            if let Err(e) = oldest.await {
                                error!("Oldest pipeline flush task panicked: {}", e);
                            }
                        }
                    }
                }

                components
                    .global_log_processor
                    .start_invocation_retry();

                // Process buffered logs and pre-invoke logs using global log processor
                components
                    .global_log_processor
                    .process_buffered_logs_with_request_id(&request_id);
                components
                    .global_log_processor
                    .process_pre_invoke_logs();

                // Send batch if threshold is reached after processing late payloads (only with report lines)
                // CRITICAL: Must await to prevent Lambda from freezing network mid-request
                if should_send_batch_by_threshold() {
                    debug!("Batch threshold reached - sending payloads with report lines only");
                    send_batched_payloads_with_reports_only(
                        components.newrelic_client.clone(),
                        components.config.clone()
                    ).await;
                }

                REQUEST_PROCESSORS.insert(request_id.clone(), request_state);

                let request_id_clone = request_id.clone();
                let invoked_function_arn_clone = invoked_function_arn.clone();
                let newrelic_client_clone = components.newrelic_client.clone();
                let config_clone = components.config.clone();
                let global_log_processor_clone = components.global_log_processor.clone();

                let processing_handle = tokio::spawn(async move {
                    process_request_concurrently(
                        request_id_clone,
                        invoked_function_arn_clone,
                        newrelic_client_clone,
                        config_clone,
                        global_log_processor_clone,
                        deadline_ms,
                    )
                    .await;
                });

                // synchronous_flush wins over pipeline_flush: process_request_concurrently
                // (processing_handle) awaits any immediate-send tasks kicked off for this
                // request by route_payload_to_request_buffer, plus it may itself send
                // immediately on a successful pairing (maybe_spawn_batch_send) instead of
                // waiting for the batch threshold. If pipeline_flush deferred
                // processing_handle into the background here, those sends could be frozen
                // mid-flight by the sandbox freeze — silently defeating the delivery
                // guarantee the customer explicitly opted into.
                let defer_via_pipeline_flush = should_defer_via_pipeline_flush(
                    components.config.extension.pipeline_flush,
                    components.config.new_relic.synchronous_flush,
                );

                if defer_via_pipeline_flush {
                    pending_flush_handles.push(processing_handle);
                } else {
                    if let Err(e) = processing_handle.await {
                        error!("Error in standard mode request processing: {}", e);
                    }
                }

                if !components.config.extension.pipeline_flush {
                    let event_time = event_start.elapsed();
                    if is_cold_start {
                        debug!(
                            "COLD START: First invocation processed in {:?} (request_id: {})",
                            event_time, request_id
                        );
                        IS_WARM_START.store(true, std::sync::atomic::Ordering::Relaxed);
                    } else {
                        debug!(
                            "WARM START: Event {} processed in {:?} (request_id: {})",
                            event_counter, event_time, request_id
                        );
                    }
                } else if is_cold_start {
                    IS_WARM_START.store(true, std::sync::atomic::Ordering::Relaxed);
                }

                // Periodic cleanup: Run every 10 invocations to prevent memory leaks
                cleanup_counter += 1;
                if cleanup_counter >= 10 {
                    cleanup_counter = 0;
                    let newrelic_client = components.newrelic_client.clone();
                    let config = components.config.clone();
                    tokio::spawn(async move {
                        use crate::agent::batch::cleanup_old_batch_entries;
                        use crate::request::cleanup_old_request_buffers;
                        cleanup_old_batch_entries(newrelic_client.clone(), config.clone()).await;
                        cleanup_old_request_buffers(newrelic_client, config).await;
                    });
                }
            }
            runtime::LambdaRuntimeEvent::Shutdown { shutdown_reason } => {
                // Stop forwarding the extension's own shutdown-sequence logs to NR
                // (set before any shutdown log is emitted).
                crate::IS_SHUTTING_DOWN.store(true, std::sync::atomic::Ordering::Relaxed);
                let shutdown_start_time = std::time::Instant::now();
                info!("Serverless mode: Extension shutting down with reason: {} (started at {:?})", shutdown_reason, std::time::SystemTime::now());

                let shutdown_timeout_result = tokio::time::timeout(Duration::from_millis(SHUTDOWN_TIMEOUT_MS), async {
                if components.config.extension.pipeline_flush {
                    for handle in pending_flush_handles.drain(..) {
                        debug!("Shutdown: awaiting in-flight pipeline flush");
                        if let Err(e) = handle.await {
                            error!("Pipeline flush task panicked during shutdown: {}", e);
                        }
                    }
                }

                // Synthesize and send error based on shutdown reason
                if let Some((last_request_id, last_arn)) = LAST_REQUEST_CONTEXT.lock().ok().and_then(|guard| guard.clone()) {
                    match shutdown_reason {
                        runtime::ShutdownReason::Timeout => {
                            // Lambda timeout - send timeout error with reason
                            debug!("Shutdown due to timeout - synthesizing timeout error for request: {}", last_request_id);
                            error_synthesis::send_timeout_error(
                                &last_request_id,
                                &last_arn,
                                None,
                                &components.newrelic_client,
                                &components.config,
                            )
                            .await;
                        }
                        runtime::ShutdownReason::Failure => {
                            // Lambda failure/fault - send platform fault error
                            debug!("Shutdown due to failure - synthesizing fault error for request: {}", last_request_id);
                            error_synthesis::send_platform_fault_error(
                                &last_request_id,
                                &last_arn,
                                &components.newrelic_client,
                                &components.config,
                            )
                            .await;
                        }
                        runtime::ShutdownReason::Spindown => {
                            // Normal shutdown - no error needed
                            debug!("Normal spindown shutdown - no error synthesis needed");
                        }
                        runtime::ShutdownReason::Unknown => {
                            // Unknown/unexpected shutdown reason - send generic error
                            warn!("Unknown shutdown reason - synthesizing generic error for request: {}", last_request_id);
                            error_synthesis::send_lambda_error(
                                &format!("Lambda shutdown with unknown reason"),
                                &last_request_id,
                                &last_arn,
                                "LambdaShutdown",
                                &components.newrelic_client,
                                &components.config,
                            )
                            .await;
                        }
                    }
                }

                // Yield to let the IPC pipe collector task route any last in-flight payloads
                // into agent_buffer before we collect from REQUEST_DATA below.
                // The collector runs in a separate tokio task; two yields ensure items
                // already in the mpsc channel are routed before the shutdown collect.
                tokio::task::yield_now().await;
                tokio::task::yield_now().await;

                // Under NEW_RELIC_EXTENSION_SYNCHRONOUS_FLUSH, a payload that arrived after
                // its own invocation's process_request_concurrently already returned (racing
                // the final flush) has its immediate-send task's handle sitting unawaited in
                // some request's pending_send_handles — nothing else will ever come back for
                // it. Sweep every request now, inside this same SHUTDOWN_TIMEOUT_MS-bounded
                // timeout, so an in-flight send is waited for rather than silently abandoned
                // when the sandbox is torn down.
                request::drain_and_await_all_pending_send_handles().await;

                // CRITICAL: Send ALL remaining payloads at shutdown (with or without reports)
                debug!("Standard mode shutdown: Sending ALL remaining payloads (including those without reports)");
                send_all_pending_payloads_on_shutdown(
                    components.newrelic_client.clone(),
                    components.config.clone(),
                    Some(&components.global_log_processor),
                )
                .await;

                // Flush any logs still held for a request whose payload never arrived,
                // untagged — last chance before the sandbox dies.
                components.global_log_processor.flush_pending_logs_unstamped();

                // Flush pre-invoke buffer and main log buffer concurrently.
                // Both paths write to independent payloads (pre-invoke vs main batch),
                // so parallel execution is safe and cuts shutdown latency in half
                // when both are non-empty. Each path already chunks at 1MB.
                let (pre_invoke_result, flush_result) = tokio::join!(
                    components.global_log_processor.flush_pre_invoke_buffer_on_shutdown(),
                    components.global_log_processor.flush_on_shutdown(),
                );
                if let Err(e) = pre_invoke_result {
                    error!("Standard mode shutdown: Failed to flush pre-invoke buffer: {}", e);
                }
                if let Err(e) = flush_result {
                    error!("Standard mode shutdown: Failed to flush logs: {}", e);
                }
                }).await;

                if shutdown_timeout_result.is_err() {
                    warn!("Serverless shutdown timed out after {}ms — Lambda will terminate remaining work", shutdown_start_time.elapsed().as_millis());
                } else {
                    info!("Standard mode shutdown: All data processed and sent in {}ms", shutdown_start_time.elapsed().as_millis());
                }
                break;
            }
        }
    }

    event_counter
}

pub async fn execute_noop_event_loop(client: &Arc<Client>, extension_id: &str) {
    info!("Starting no-op mode, no telemetry will be sent");

    loop {
        let loop_start = std::time::Instant::now();
        match runtime::fetch_next_event(client, extension_id).await {
            Ok(runtime::LambdaRuntimeEvent::Shutdown { shutdown_reason: _ }) => {
                debug!("Extension shutting down");
                break;
            }
            Ok(runtime::LambdaRuntimeEvent::Invoke {
                request_id,
                invoked_function_arn: _,
                deadline_ms: _,
            }) => {
                trace!(
                    "No-op mode invocation processed in {:?} (request_id: {})",
                    loop_start.elapsed(),
                    request_id
                );
            }
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("403") || error_msg.contains("State transition") {
                    error!("Fatal extension state error (403 - Lambda shutting down): {:?}", e);
                    debug!("No-op mode exiting due to Lambda shutdown");
                    break;
                }
                error!("Error in no-op event loop: {:?}. Continuing.", e);
            }
        }
    }
}

/// Render an elapsed-ms-since-runtime.done value for a log line. `None` means
/// `platform.runtimeDone` was never observed for that request (e.g. it fired before
/// `RequestData` existed and the pre-fire timestamp was somehow lost, or this is being
/// called defensively on a request with no recorded runtime.done at all) — render that
/// as "unknown" rather than a misleading "0ms".
fn format_elapsed_for_log(elapsed_ms: Option<i64>) -> String {
    match elapsed_ms {
        Some(ms) => format!("{}ms", ms),
        None => "an unknown time".to_string(),
    }
}

#[cfg(test)]
pub async fn process_apm_request(
    request_id: String,
    invoked_function_arn: String,
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
    apm_app: crate::apm::SharedApmApp,
    deadline_ms: i64,
) {
    process_apm_request_with(request_id, invoked_function_arn, config, global_log_processor, apm_app, deadline_ms, None).await;
}

/// `invoke_retries` is the high tier's INVOKE-time retries of earlier failures: held back
/// until the invoke is known not to be short. The default tier passes `None` and runs
/// its retries inline, as before.
async fn process_apm_request_with(
    request_id: String,
    invoked_function_arn: String,
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
    apm_app: crate::apm::SharedApmApp,
    deadline_ms: i64,
    invoke_retries: Option<InvokeRetries>,
) {
    debug!("APM mode: Starting processing for request: {}", request_id);

    // Draining other (previous-invocation) requests' leftover buffers/pending
    // reports is NOT this function's job — it's handled by
    // process_pending_agent_payloads, spawned concurrently alongside this one
    // (see execute_apm_mode_event_loop's `pending_task`). The two used to
    // independently duplicate that responsibility, racing each other on the same
    // REQUEST_DATA entries. This function owns exactly one thing: this request's
    // own Flow 1/2/3 decision below.

    let state = REQUEST_PROCESSORS.remove(&request_id).map(|(_k, v)| v);

    let Some(state) = state else {
        error!("No processing state found for request: {}", request_id);
        return;
    };

    let invocation_start_time = chrono::Utc::now();
    global_log_processor.set_invocation_start_time(invocation_start_time);
    // trace.id buffering is per-request (keyed by request_id) inside LogProcessor, so
    // there's no per-invocation state to reset here: the previous request's logs are
    // stamped + flushed when its deferred agent payload is processed
    // (process_pending_agent_payloads -> on_trace_id_extracted), independent of this
    // invocation.
    state
        .platform_processor
        .process_invoke_event(&request_id, &invoked_function_arn);

    match config.extension.send_tier {
        crate::send::SendTier::Default => {
            process_apm_request_default(
                &request_id,
                &invoked_function_arn,
                &config,
                &global_log_processor,
                &apm_app,
                deadline_ms,
                &state,
            ).await;
        }
        crate::send::SendTier::High => {
            process_apm_request_high(
                &request_id,
                &config,
                &global_log_processor,
                &apm_app,
                deadline_ms,
                &state,
                invoke_retries,
            ).await;
        }
    }
    // Agent payloads arrive via the named pipe independently and are already sent above.
    // Late agent payloads will be caught by the warm-start pending-payload logic.

    cleanup_request_processing_state_internal(&request_id, true);


    debug!(
        "APM mode: Completed processing for request: {}",
        request_id
    );
}

/// `SendTier::Default` — the exact pre-existing `main` code (pipeline_flush-based,
/// pre-`SendScheduler`), moved here verbatim, not edited, per `DUAL_PATH_PLAN.md`.
/// Zero behavior change versus `main` -- this is the whole point of the dual-path
/// design: `default` tier's testing surface/regression risk stays exactly what it
/// already was.
///
/// `#[allow(clippy::too_many_lines)]`: a function-boundary artifact of
/// extracting this logic out of the original combined `process_apm_request`
/// (which didn't trip this lint at its old boundary) -- the code itself is
/// unchanged, not newly complex.
#[allow(clippy::too_many_lines)]
async fn process_apm_request_default(
    request_id: &String,
    invoked_function_arn: &String,
    config: &Arc<ExtensionConfig>,
    global_log_processor: &Arc<LogProcessor>,
    apm_app: &crate::apm::SharedApmApp,
    deadline_ms: i64,
    state: &request::RequestProcessingState,
) {
    // Wait for the handler to actually finish (platform.runtimeDone) BEFORE deciding
    // whether the agent payload / platform.report have arrived. Bounded by the
    // invocation's own remaining deadline. Always runs regardless of
    // NEW_RELIC_EXTENSION_SEND_FUNCTION_LOGS.
    let runtime_done_signaled = wait_for_runtime_done(request_id, deadline_ms).await;

    // APM mode: Check if run_id is available
    let has_run_id = {
        let apm_app_guard = apm_app.read().await;
        apm_app_guard.is_some()
    };

    let agent_payloads = {
        if let Ok(mut buffer) = state.agent_buffer.lock() {
            std::mem::take(&mut *buffer)
        } else {
            Vec::new()
        }
    };

    let got_payload = !agent_payloads.is_empty();

    // Flow 1: run_id and agent payload both available — send now.
    // Flow 2: run_id not available yet — re-buffer for the next invocation.
    // Flow 3: nothing buffered yet.
    let send_agent_task = if has_run_id && got_payload {
        debug!(
            "APM mode: run_id available + agent payload arrived ({} payload(s)) - sending immediately",
            agent_payloads.len()
        );
        let request_id_clone = request_id.clone();
        let config_clone = config.clone();
        let global_log_processor_clone = global_log_processor.clone();
        let apm_app_clone = apm_app.clone();
        let agent_payloads_clone = agent_payloads.clone();
        let invoked_function_arn_clone = invoked_function_arn.clone();

        Some(tokio::spawn(async move {
            // Trace-id extraction is per-chunk (independent of telemetry-map
            // merging) — same as process_pending_agent_payloads's identical step.
            for payload_bytes in &agent_payloads_clone {
                extract_and_coordinate_trace_id(
                    payload_bytes,
                    &request_id_clone,
                    &config_clone,
                    &global_log_processor_clone,
                )
                .await;
            }

            // Merge mergeable event-list types across all of this request's
            // buffered chunks into one collector POST per type, rather than one
            // POST per chunk per type — mirrors process_pending_agent_payloads's
            // identical call for the previous-invocation drain path. Before this,
            // only that drain path got the batching win; this same-invoke path
            // (the common case) still sent one POST per chunk per type.
            // Clone out of the lock and drop it right away — don't hold a read
            // lock across the send below, or it blocks a reconnect from writing.
            let app_owned = apm_app_clone.read().await.clone();
            let all_sent = if let Some(app) = app_owned {
                let send_result = app.process_merged_agent_payloads(agent_payloads_clone.clone(), &request_id_clone, &invoked_function_arn_clone).await;
                send_result.is_ok()
            } else {
                false
            };

            if !all_sent {
                // On failure the payloads are buffered into FAILED_AGENT_PAYLOADS
                // (never silently dropped); the global retry loop resends them on a
                // later invoke / at shutdown. Mirrors process_pending_agent_payloads's
                // identical fallback when apm_app isn't ready or the merged send errors.
                for payload_bytes in &agent_payloads_clone {
                    buffer_failed_agent_payload(payload_bytes, &request_id_clone, &invoked_function_arn_clone);
                }
            }
            (all_sent, agent_payloads_clone)
        }))
    } else {
        // Flow 2 or 3: Either no run_id yet, or no payload yet
        if !has_run_id && got_payload {
            info!(
                "APM mode: No run_id yet but agent payload arrived ({} payload(s)) for request {} - buffering until run_id becomes available",
                agent_payloads.len(), request_id
            );
            // Put payloads back in buffer to send when run_id arrives
            if let Some(buffer_ref) = get_agent_buffer(request_id) {
                if let Ok(mut buffer) = buffer_ref.lock() {
                    buffer.extend(agent_payloads);
                }
            }
        } else if has_run_id && !got_payload {
            debug!(
                "APM mode: run_id available but no agent payload for request: {} even after waiting for runtime.done (signaled: {}) - will catch in next invocation if it arrives late",
                request_id, runtime_done_signaled
            );
        } else {
            debug!(
                "APM mode: No run_id and no agent payload for request: {} - normal flow",
                request_id
            );
        }
        None
    };

    // The Flow-1 agent-payload send, the platform.report send, and the log/platform
    // flush are mutually independent — none needs another's result — but used to
    // run strictly sequentially here, adding up their individual durations into
    // billed duration on every invocation that has more than one of them. Run all
    // three concurrently instead: total tail latency becomes roughly the slowest of
    // them, not their sum.
    let agent_send_fut = async {
        // Awaited inline (rather than fire-and-forget) so a same-invoke send
        // genuinely completes — with its own result observed — before this
        // invocation returns to /next, instead of racing the sandbox freeze in
        // the background.
        if let Some(handle) = send_agent_task {
            match handle.await {
                Ok((success, payloads)) => {
                    if !success && !payloads.is_empty() {
                        debug!("APM mode: agent send had failures — unsent payloads buffered for retry (next invoke / shutdown)");
                    }
                }
                Err(e) => {
                    error!("Agent send task failed: {}", e);
                }
            }
        }
    };

    let report_send_fut = async {
        // Check for pending platform.report and send as metrics to Metric API (APM mode)
        if let Some(report_line) = get_pending_report(&request_id) {
            debug!("APM mode: Found platform.report for request {} - converting to metrics", request_id);

            // Clone out and drop the guard before the send — same reasoning as
            // process_apm_request's agent-payload send fix: don't hold a read lock
            // across an HTTP POST and risk blocking a reconnect's write lock.
            let app_owned = apm_app.read().await.clone();
            if let Some(app) = app_owned {
                // send_platform_report_metrics always returns Ok (permanent-drop and
                // buffered-for-retry are Ok too, by design) and already logs its own
                // accurate outcome internally — don't re-log here.
                let _ = app.send_platform_report_metrics(&report_line, &invoked_function_arn).await;
            } else {
                warn!("APM mode: APM app not ready - cannot send platform metrics for {}", request_id);
            }

            // Remove pending report after sending
            remove_pending_report(&request_id);
        } else {
            debug!(
                "APM mode: No platform.report found for request {} even after waiting for runtime.done (signaled: {}) (may arrive in next invocation)",
                request_id, runtime_done_signaled
            );
        }
    };

    let log_flush_fut = async {
        wait_for_runtime_done_with_grace(
            &request_id,
            runtime_done_signaled,
            &config,
            &global_log_processor,
        )
        .await;

        // Wait for logs and platform to complete before returning
        let log_flushing = global_log_processor.flush();
        let platform_flushing = state.platform_processor.flush();

        let (log_result, platform_result) = tokio::join!(
            log_flushing,
            platform_flushing,
        );

        if let Err(e) = log_result {
            error!("Failed to flush logs for request {}: {}", request_id, e);
        }
        if let Err(e) = platform_result {
            error!("Failed to flush platform for request {}: {}", request_id, e);
        }
    };

    // Report sends the listener started in the background: /next waits for them too.
    let reports_in_flight_fut = wait_for_report_sends(Duration::from_millis(bounded_wait_budget_ms(
        deadline_ms,
        MAX_SEND_WAIT_MS,
    )));

    tokio::join!(agent_send_fut, report_send_fut, log_flush_fut, reports_in_flight_fut);
}

/// What one invocation cost, logged once per invoke so the placeholder timings can
/// be set from real numbers (billed time is what `waited_ms` adds).
struct InvokeTiming {
    handler_ms: u128,
    waited_ms: u128,
    short_invoke: bool,
    /// The function's deadline cut a send off before it finished.
    deadline_cut: bool,
    /// Lambda's own `metrics.durationMs` from `runtimeDone`, when it had arrived.
    runtime_done_ms: Option<f64>,
}

fn log_invoke_timing(request_id: &str, t: &InvokeTiming) {
    info!(
        "APM high timing: request={} handler_ms={} runtime_done_ms={} short_invoke={} waited_ms={} deadline_cut={}",
        request_id,
        t.handler_ms,
        t.runtime_done_ms.map_or_else(|| "n/a".to_string(), |ms| format!("{ms:.1}")),
        t.short_invoke,
        t.waited_ms,
        t.deadline_cut
    );
}

/// Whether a short invoke recorded an error: the agent reported error events, Lambda's
/// `runtimeDone` status is not success, or the invoke logged at ERROR level.
fn short_invoke_had_error(
    request_id: &str,
    state: &request::RequestProcessingState,
    log_processor: &Arc<LogProcessor>,
) -> bool {
    let payloads = state.agent_buffer.lock().map(|b| b.clone()).unwrap_or_default();
    request::runtime_done_failed(request_id)
        || log_processor.has_error_log_for(request_id)
        || payloads.iter().any(|bytes| payload_has_errors(bytes))
}

/// Whether an agent payload carries error events (`error_event_data`) or traced errors
/// (`error_data`).
fn payload_has_errors(payload_bytes: &[u8]) -> bool {
    let Ok(parsed) = crate::apm::payload_parser::parse_agent_payload_cached(payload_bytes) else {
        return false;
    };
    telemetry_has_errors(&parsed.0)
}

fn telemetry_has_errors(telemetry: &std::collections::HashMap<String, Vec<serde_json::Value>>) -> bool {
    let non_empty = |key: &str, index: usize| {
        telemetry
            .get(key)
            .and_then(|data| data.get(index))
            .and_then(serde_json::Value::as_array)
            .is_some_and(|items| !items.is_empty())
    };
    non_empty("error_event_data", 2) || non_empty("error_data", 1)
}

/// A short invoke is decided: nothing is sent or held for it. Without an error its own
/// agent payload, report and logs are dropped; with one they are kept for the next long
/// invoke's batch (logs: ERROR level only). Either way the verdict is remembered, so data
/// of this request that arrives later is treated the same. Earlier invokes' data is not
/// touched. Returns whether an error was recorded.
fn handle_short_invoke(
    request_id: &str,
    state: &request::RequestProcessingState,
    log_processor: &Arc<LogProcessor>,
) -> bool {
    use crate::send::short_invoke::{mark, ShortOutcome};

    let had_error = short_invoke_had_error(request_id, state, log_processor);
    if had_error {
        mark(request_id, ShortOutcome::ErrorKept);
        log_processor.apply_short_invoke(request_id, true);
        info!("APM mode (high): short invoke {} recorded an error - its data goes with the next long invoke", request_id);
    } else {
        mark(request_id, ShortOutcome::Dropped);
        if let Ok(mut buffer) = state.agent_buffer.lock() {
            buffer.clear();
        }
        remove_pending_report(request_id);
        drop_stashed_report(request_id);
        log_processor.apply_short_invoke(request_id, false);
        debug!("APM mode (high): short invoke {} had no error - its data is dropped", request_id);
    }
    had_error
}

/// The longest any wait in the high tier may last; the function's own deadline cuts it
/// shorter in practice.
const MAX_SEND_WAIT_MS: u64 = 15 * 60 * 1_000;

/// `SendTier::High`. Nothing is sent until the invoke is known to be long (the agent's
/// own duration, or `runtimeDone`, past `short_invoke_threshold_ms`). A short invoke
/// sends nothing and is not held: its own data is dropped unless it recorded an error.
/// A long invoke then sends everything earlier invokes left (agent payloads, platform
/// reports, logs) in parallel with the handler, and `/next` waits until all of it is
/// done, bounded only by the function's own deadline.
async fn process_apm_request_high(
    request_id: &str,
    config: &Arc<ExtensionConfig>,
    global_log_processor: &Arc<LogProcessor>,
    apm_app: &crate::apm::SharedApmApp,
    deadline_ms: i64,
    state: &request::RequestProcessingState,
    invoke_retries: Option<InvokeRetries>,
) {
    let invoke_start = tokio::time::Instant::now();

    // Collect last invoke's log flush if it finished; never abort it here.
    resolve_pending_logs(global_log_processor, request_id).await;

    let threshold = Duration::from_millis(config.extension.short_invoke_threshold_ms);
    if detect_short_invoke(request_id, state, threshold, invoke_start).await == InvokeLength::Short {
        handle_short_invoke(request_id, state, global_log_processor);
        log_invoke_timing(request_id, &InvokeTiming {
            handler_ms: invoke_start.elapsed().as_millis(),
            waited_ms: 0,
            short_invoke: true,
            deadline_cut: false,
            runtime_done_ms: request::get_runtime_done_duration_ms(request_id),
        });
        return;
    }

    // Long invoke: everything from earlier invokes starts now, all at once. Earlier
    // failures may be retried too.
    spawn_pending_batch(request_id, config, global_log_processor, apm_app).await;
    if let Some(retries) = invoke_retries {
        retries();
    }
    spawn_log_flush(global_log_processor, request_id).await;

    let wait_started = tokio::time::Instant::now();
    let _ = wait_for_runtime_done(request_id, deadline_ms).await;
    let handler = invoke_start.elapsed();
    debug!(
        "APM mode (high): runtimeDone for {} reached the extension {}ms after INVOKE (waited {}ms for it, Lambda's own handler time: {})",
        request_id,
        handler.as_millis(),
        wait_started.elapsed().as_millis(),
        request::get_runtime_done_duration_ms(request_id).map_or_else(|| "n/a".to_string(), |ms| format!("{ms:.1}ms")),
    );

    // The sends may take as long as they need, but never past the function's own deadline.
    let hold_started = tokio::time::Instant::now();
    let hard_deadline = hold_started + Duration::from_millis(bounded_wait_budget_ms(deadline_ms, MAX_SEND_WAIT_MS));
    let window_until = hard_deadline.min(
        hold_started + Duration::from_millis(config.extension.payload_window_ms),
    );
    wait_for_own_payload(request_id, state, config, global_log_processor, apm_app, window_until).await;

    let mut outcomes = SEND_SCHEDULER
        .resolve_pending_shared_budget(
            &[crate::send::SendKind::ApmCollector, crate::send::SendKind::Logs],
            hard_deadline.saturating_duration_since(tokio::time::Instant::now()),
        )
        .await;
    let batch_cut = outcomes
        .remove(&crate::send::SendKind::ApmCollector)
        .is_some_and(|outcome| finish_batch_outcome(&outcome));
    let logs_cut = match outcomes.remove(&crate::send::SendKind::Logs) {
        Some(outcome) => {
            handle_logs_outcome(&outcome, global_log_processor, request_id);
            outcome.result == crate::send::DispatchResult::TimedOut
        }
        None => false,
    };
    log_invoke_timing(request_id, &InvokeTiming {
        handler_ms: handler.as_millis(),
        waited_ms: hold_started.elapsed().as_millis(),
        short_invoke: false,
        deadline_cut: batch_cut || logs_cut,
        runtime_done_ms: request::get_runtime_done_duration_ms(request_id),
    });
}

/// Waiting for this request's own payload only matters for log correlation:
/// the payload ships with the next batch either way, but it carries the
/// trace.id our held logs are waiting for. So wait only when logs are actually
/// held and an agent is known to exist; otherwise the answer is "don't".
async fn wait_for_own_payload(
    request_id: &str,
    state: &request::RequestProcessingState,
    config: &Arc<ExtensionConfig>,
    global_log_processor: &Arc<LogProcessor>,
    apm_app: &crate::apm::SharedApmApp,
    until: tokio::time::Instant,
) {
    let buffered = || state.agent_buffer.lock().map(|b| !b.is_empty()).unwrap_or(false);

    let expected = config.new_relic.collect_trace_id
        && config.extension.send_function_logs
        && AGENT_PAYLOAD_SEEN.load(std::sync::atomic::Ordering::Relaxed)
        && global_log_processor.has_held_logs_for(request_id)
        && apm_app.read().await.is_some();
    if !expected {
        return;
    }

    // No arrival signal exists, so poll; the window is only a few ms.
    while !buffered() {
        if tokio::time::Instant::now() >= until {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    // Peek, don't take: the payload still ships in the next batch.
    let payloads = state.agent_buffer.lock().map(|b| b.clone()).unwrap_or_default();
    for payload in &payloads {
        extract_and_coordinate_trace_id(payload, request_id, config, global_log_processor).await;
    }
}

/// Core wait, shared by every caller that needs to know when the handler has actually
/// finished: blocks on `runtime_done_notify` (pre-armable — if `platform.runtimeDone`
/// already fired, `notified()` returns immediately), bounded by the invocation's own
/// remaining deadline so it can never outlive the function. Returns `true` if the
/// signal arrived, `false` if the deadline was hit first.
///
/// Deliberately has no config gate: callers that only care about this for log-flushing
/// purposes (`wait_for_runtime_done_with_grace`) may skip calling it entirely when log
/// forwarding is off, but callers that need it for correctness (e.g. the agent-payload
/// Flow-1 check in `process_apm_request`) must always call it regardless of any
/// unrelated logging config — `NEW_RELIC_EXTENSION_SEND_FUNCTION_LOGS=false` must not
/// silently disable the agent-payload same-invoke-send fix.
async fn wait_for_runtime_done(request_id: &str, deadline_ms: i64) -> bool {
    let Some(notify) = request::get_runtime_done_notify(request_id) else {
        return false;
    };

    const MAX_RUNTIME_DONE_WAIT_MS: u64 = 15 * 60 * 1_000;
    const FALLBACK_RUNTIME_DONE_WAIT_MS: u64 = 5_000;

    let now_ms = chrono::Utc::now().timestamp_millis();
    // Leave the safety margin so we don't wait all the way to the real deadline.
    let remaining_ms: u64 = if deadline_ms > now_ms {
        bounded_wait_budget_ms(deadline_ms, MAX_RUNTIME_DONE_WAIT_MS)
    } else {
        warn!(
            "INVOKE for request {} missing/stale deadlineMs ({}); using {}ms fallback for runtime.done wait",
            request_id, deadline_ms, FALLBACK_RUNTIME_DONE_WAIT_MS
        );
        FALLBACK_RUNTIME_DONE_WAIT_MS
    };

    let signal = tokio::select! {
        _ = notify.notified() => true,
        _ = tokio::time::sleep(Duration::from_millis(remaining_ms)) => false,
    };

    if signal {
        debug!(
            "runtime.done signal received for request: {} (after {}ms)",
            request_id,
            chrono::Utc::now().timestamp_millis().saturating_sub(now_ms)
        );
    } else {
        debug!(
            "runtime.done wait reached function deadline ({}ms) for request: {} - proceeding anyway",
            remaining_ms, request_id
        );
    }

    signal
}

/// Log-flush-specific grace period, layered on top of an *already-observed*
/// `runtime.done` signal. Takes `signaled` rather than calling `wait_for_runtime_done`
/// itself: `Notify::notify_one` hands out a single wake-up permit, so calling the core
/// wait twice for the same request would consume it once and then block the second
/// caller out to the full deadline/fallback timeout every time — callers must wait
/// exactly once (see `process_apm_request`) and share the resulting bool.
async fn wait_for_runtime_done_with_grace(
    request_id: &str,
    signaled: bool,
    config: &ExtensionConfig,
    log_processor: &Arc<LogProcessor>,
) {
    if !config.extension.send_function_logs {
        return;
    }

    if signaled {
        if !log_processor.is_drained() {
            let grace_ms = config.extension.runtime_done_grace_ms;
            if grace_ms > 0 {
                // Subscribe to drain notification BEFORE re-checking is_drained()
                // to avoid a TOCTOU window where notify_one() fires between the
                // is_drained() check above and the notified().await below.
                let notify = log_processor.drain_notify();
                let notified = notify.notified();
                tokio::pin!(notified);
                // enable() registers the subscription so any concurrent notify_one()
                // fired between here and the await is captured.
                notified.as_mut().enable();

                if !log_processor.is_drained() {
                    // "Drained" just means the batch was handed off to a background
                    // send task, not that the send finished. The real wait for the
                    // send to finish happens later, unconditionally.
                    debug!(
                        "runtime.done: log batch not yet handed off for sending - waiting up to {}ms for the hand-off (not the send itself; request: {})",
                        grace_ms, request_id
                    );
                    let grace_start = tokio::time::Instant::now();
                    let _ = tokio::time::timeout(
                        Duration::from_millis(grace_ms),
                        notified,
                    )
                    .await;
                    debug!(
                        "runtime.done: hand-off wait ended after {}ms / {}ms max (request: {}) - actual send completion is awaited separately below",
                        grace_start.elapsed().as_millis(),
                        grace_ms,
                        request_id
                    );
                }
            }
        } else {
            debug!(
                "runtime.done: batch already drained for request: {} - skipping grace",
                request_id
            );
        }
    }
    // else: wait_for_runtime_done already logged the deadline-reached case.
}

/// One agent payload still buffered when SHUTDOWN arrives.
struct ShutdownPayload {
    request_id: String,
    arn: String,
    bytes: Vec<u8>,
}

/// SHUTDOWN: take every agent payload still buffered. A short invoke that was decided to be
/// dropped is skipped, even if its payload came late.
fn shutdown_take_payloads() -> Vec<ShutdownPayload> {
    let request_ids: Vec<String> = REQUEST_DATA.iter().map(|entry| entry.key().clone()).collect();
    let mut taken = Vec::new();
    for request_id in request_ids {
        if crate::send::short_invoke::get(&request_id) == Some(crate::send::short_invoke::ShortOutcome::Dropped) {
            continue;
        }
        let Some(buffer) = get_agent_buffer(&request_id) else { continue };
        let payloads = buffer.lock().map(|mut b| std::mem::take(&mut *b)).unwrap_or_default();
        if payloads.is_empty() {
            continue;
        }
        info!("APM mode shutdown: Sending {} unsent payload(s) for request: {}", payloads.len(), request_id);
        let arn = get_request_context(&request_id)
            .and_then(|ctx| ctx.lock().ok().map(|c| c.invoked_function_arn.clone()).filter(|arn| !arn.is_empty()))
            .unwrap_or_else(crate::get_global_fallback_arn);
        taken.extend(payloads.into_iter().map(|bytes| ShutdownPayload {
            request_id: request_id.clone(),
            arn: arn.clone(),
            bytes,
        }));
    }
    taken
}

/// SHUTDOWN, only when APM is not connected: one last attempt to connect, because the sends
/// below need a session. Nothing else can start until this is done.
async fn shutdown_reconnect_if_needed(components: &ExtensionComponents) {
    if components.apm_app.read().await.is_some() {
        return;
    }
    if crate::apm::connection::is_handshake_fatal() {
        // A reconnect cannot succeed after a permanent auth rejection - don't spend the budget.
        error!("APM permanently disabled (auth rejected) — shutdown error event DROPPED");
        return;
    }
    // The sandbox stays active for the whole SHUTDOWN handler, so there is no freeze risk.
    debug!("APM not connected at shutdown — attempting final sync reconnect");
    let lambda_function_name = components.config.aws.function_name.clone();
    let function_name = std::env::var("NEW_RELIC_APP_NAME")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| lambda_function_name.clone());
    let shutdown_app = crate::apm::ApmApp::new(
        components.config.new_relic.license_key.clone().unwrap_or_default(),
        components.config.new_relic.apm_host.clone(),
        components.config.new_relic.metric_endpoint.clone(),
        components.apm_client.clone(),
        function_name,
        lambda_function_name,
        components.config.aws.function_version.clone().unwrap_or_else(|| "$LATEST".to_string()),
        components.config.aws.account_id.clone(),
        components.config.aws.region.clone(),
        // Lambda gives ~2s for SHUTDOWN; cap to avoid being killed mid-flight.
        components.config.new_relic.apm_handshake_timeout_secs.min(2),
        components.config.deployment,
    )
    .await;
    match shutdown_app {
        Ok(app) => {
            info!("APM reconnect succeeded during shutdown - Entity GUID: {}", app.get_entity_guid());
            *components.apm_app.write().await = Some(app);
        }
        Err(e) => {
            // Shutdown is the last chance to flush; a failure here means the shutdown error
            // event is permanently lost - log as error.
            error!("APM not connected at shutdown and final reconnect failed: {} - shutdown error event DROPPED", e);
        }
    }
}

/// SHUTDOWN: send the error event that matches the shutdown reason, if there is a session.
async fn shutdown_send_error_event(components: &ExtensionComponents, shutdown_reason: runtime::ShutdownReason) {
    let Some((last_request_id, last_arn)) = LAST_REQUEST_CONTEXT.lock().ok().and_then(|guard| guard.clone()) else {
        return;
    };
    // Clone out: a read lock held across the POST would block a reconnect.
    let app = components.apm_app.read().await.clone();
    if let Some(app) = app {
        send_error_for_shutdown_reason(&app, shutdown_reason, &last_request_id, &last_arn, &components.config).await;
    }
}

/// SHUTDOWN: send every buffered agent payload, all at once.
async fn shutdown_send_payloads(components: &ExtensionComponents, payloads: &[ShutdownPayload]) {
    // process_and_send_agent_payload always returns Ok and logs its own outcome. The drop
    // accounting is done once, afterwards, from the real buffer contents.
    let sends: Vec<BoxedFuture<'_, ()>> = payloads
        .iter()
        .map(|p| {
            Box::pin(async move {
                let _ = process_and_send_agent_payload(
                    &p.bytes,
                    &p.request_id,
                    &p.arn,
                    &components.global_log_processor,
                    &components.config,
                    &components.apm_app,
                )
                .await;
            }) as BoxedFuture<'_, ()>
        })
        .collect();
    join_all_concurrently(sends).await;
}

/// Every `platform.report` that is still waiting to be sent (request id, report line).
fn shutdown_pending_reports() -> Vec<(String, String)> {
    REQUEST_DATA
        .iter()
        .filter_map(|entry| entry.pending_report.as_ref().map(|report| (entry.key().clone(), report.clone())))
        .collect()
}

/// SHUTDOWN: send every pending `platform.report` as metrics. They go out together in as few
/// Metric API requests as the size limit allows (normally one), not one request per report.
async fn shutdown_send_reports(components: &ExtensionComponents) {
    let reports = shutdown_pending_reports();
    if reports.is_empty() {
        debug!("APM mode shutdown: No pending platform.report lines to process");
        return;
    }
    debug!("APM mode shutdown: Found {} pending platform.report(s) to send as metrics", reports.len());
    let Some(app) = components.apm_app.read().await.clone() else {
        warn!("APM mode shutdown: APM app not initialized - cannot send platform metrics");
        return;
    };
    let arn = LAST_REQUEST_CONTEXT
        .lock()
        .ok()
        .and_then(|g| g.clone().map(|(_, arn)| arn))
        .unwrap_or_default();
    let lines: Vec<(String, String)> = reports.iter().map(|(_, line)| (line.clone(), arn.clone())).collect();
    // Always handled: a failed send is buffered for retry inside, so each report is done with.
    app.send_platform_reports_batched(&lines).await;
    for (request_id, _) in &reports {
        remove_pending_report(request_id);
    }
}

/// SHUTDOWN, high tier only: there is no next invocation to defer to, so settle the last batch
/// and retry the platform reports it left behind.
async fn shutdown_drain_high_tier(components: &ExtensionComponents) {
    if components.config.extension.send_tier != crate::send::SendTier::High {
        return;
    }
    resolve_pending_agent_batch(components.config.extension.shutdown_batch_wait_ms).await;
    drain_stashed_reports_into_retry_buffer();
    retry_failed_platform_reports(components.apm_app.clone()).await;
}

/// SHUTDOWN: flush every log. The steps share the log processor's batch, so they stay in order.
async fn shutdown_flush_logs(components: &ExtensionComponents) {
    if components.config.extension.send_tier == crate::send::SendTier::High {
        resolve_pending_logs(&components.global_log_processor, "shutdown").await;
    }
    // Logs from INIT, if shutdown came before the first INVOKE.
    if let Err(e) = components.global_log_processor.flush_pre_invoke_buffer_on_shutdown().await {
        error!("APM mode shutdown: Failed to flush pre-invoke buffer: {}", e);
    }
    // Flush, and re-flush any entries a send failure pushed back; bounded by MAX_RETRIES.
    if let Err(e) = components.global_log_processor.flush_on_shutdown().await {
        error!("APM mode shutdown: Failed to flush logs: {}", e);
    }
}

/// SHUTDOWN: wait for pipeline flushes that were started during earlier invocations.
async fn shutdown_await_pipeline_flushes(handles: &mut Vec<tokio::task::JoinHandle<()>>, enabled: bool) {
    if !enabled {
        return;
    }
    for handle in handles.drain(..) {
        debug!("APM shutdown: awaiting in-flight pipeline flush");
        if let Err(e) = handle.await {
            error!("Pipeline flush task panicked during APM shutdown: {}", e);
        }
    }
}

/// Sends the appropriate APM error event for a given shutdown reason.
/// Called from the SHUTDOWN handler whether APM was already connected or just reconnected.
///
/// Consults `config.extension.ignore_errors`/`expected_errors`
/// (`NEW_RELIC_EXTENSION_IGNORE_ERRORS`/`NEW_RELIC_EXTENSION_EXPECTED_ERRORS`) so a
/// matching class is either dropped entirely or sent with `error.expected: true`.
/// Ignore takes precedence if a class is listed in both.
pub(crate) async fn send_error_for_shutdown_reason(
    app: &crate::apm::ApmApp,
    reason: runtime::ShutdownReason,
    request_id: &str,
    arn: &str,
    config: &crate::config::ExtensionConfig,
) {
    async fn send(
        app: &crate::apm::ApmApp,
        config: &crate::config::ExtensionConfig,
        class: &str,
        message: &str,
        request_id: &str,
        arn: &str,
    ) {
        let key = class.to_ascii_lowercase();
        if config.extension.ignore_errors.contains(&key) {
            debug!("{} ignored via NEW_RELIC_EXTENSION_IGNORE_ERRORS - not sending (request: {})", class, request_id);
            return;
        }
        let is_expected = config.extension.expected_errors.contains(&key);
        if let Err(e) = app.send_shutdown_error_event(class, message, request_id, arn, is_expected).await {
            error!("Failed to send {} error event to APM: {}", class, e);
        }
    }

    match reason {
        runtime::ShutdownReason::Timeout => {
            info!("Shutdown due to timeout - sending error event to APM for request: {}", request_id);
            send(app, config, "LambdaTimeout", "Task timed out", request_id, arn).await;
        }
        runtime::ShutdownReason::Failure => {
            info!("Shutdown due to failure - sending error event to APM for request: {}", request_id);
            send(app, config, "LambdaPlatformFault", "AWS Lambda platform fault caused a shutdown", request_id, arn).await;
        }
        runtime::ShutdownReason::Spindown => {
            debug!("Normal spindown shutdown - no error event needed");
        }
        runtime::ShutdownReason::Unknown => {
            warn!("Unknown shutdown reason - sending error event to APM for request: {}", request_id);
            send(app, config, "LambdaShutdown", "Lambda shutdown with unknown reason", request_id, arn).await;
        }
    }
}

/// After `platform.runtimeDone`, use remaining deadline budget to let an in-flight
/// APM handshake complete before calling /next. Sandbox stays active while the
/// extension has not yet called /next — no freeze risk. Bounded by deadline_ms.
/// Shared budget computation for every bounded wait in this file: how many
/// milliseconds are left to wait for something within this invocation, capped by
/// both the caller's configured timeout and the invocation's own remaining deadline
/// (minus `INVOKE_DEADLINE_SAFETY_MARGIN_MS`). Never returns more than either input.
fn bounded_wait_budget_ms(deadline_ms: i64, configured_timeout_ms: u64) -> u64 {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let deadline_budget_ms = if deadline_ms > now_ms {
        ((deadline_ms - now_ms) as u64).saturating_sub(INVOKE_DEADLINE_SAFETY_MARGIN_MS)
    } else {
        0
    };
    configured_timeout_ms.min(deadline_budget_ms)
}

async fn wait_for_apm_handshake_within_budget(
    reconnect_sender: &watch::Sender<bool>,
    deadline_ms: i64,
) {
    if !*reconnect_sender.borrow() {
        return;
    }
    // Handshake wait has no separate "configured timeout" of its own — it's bounded
    // purely by the invocation deadline, so pass u64::MAX as the "no cap" timeout input.
    let budget_ms = bounded_wait_budget_ms(deadline_ms, u64::MAX);
    if budget_ms == 0 {
        debug!("APM handshake in-flight but no deadline budget remaining — skipping wait");
        return;
    }
    debug!(
        "APM handshake in-flight after runtimeDone — waiting up to {}ms within invoke deadline",
        budget_ms
    );
    let mut rx = reconnect_sender.subscribe();
    let _ = tokio::time::timeout(
        Duration::from_millis(budget_ms),
        rx.wait_for(|in_flight| !in_flight),
    )
    .await;
}

/// Whether `execute_standard_mode_event_loop` should defer `process_request_concurrently`
/// into the background `pending_flush_handles` queue (the `pipeline_flush` optimization)
/// rather than synchronously awaiting it before calling `/next` again.
///
/// `synchronous_flush` always wins: deferring would let the sandbox freeze mid-send
/// (either the immediate per-payload send kicked off by
/// `request::route_payload_to_request_buffer`, or the batch send in
/// `maybe_spawn_batch_send`), silently defeating the delivery guarantee the customer
/// explicitly opted into. Pure function so the precedence rule is unit-testable without
/// spinning up the event loop.
fn should_defer_via_pipeline_flush(pipeline_flush: bool, synchronous_flush: bool) -> bool {
    pipeline_flush && !synchronous_flush
}

/// Standing (client, config, log processor) handle so code that runs independently of
/// `process_request_concurrently` — the standard-mode telemetry listener's
/// `platform.report` handler, and `request::route_payload_to_request_buffer`'s
/// immediate agent-payload send under `NEW_RELIC_EXTENSION_SYNCHRONOUS_FLUSH` — can
/// send without threading these values through their own call chains
/// (`setup_telemetry_listener`/`handle_telemetry_request` alone have ~20 test call
/// sites). Set once at startup in `main.rs` right after all three are constructed;
/// read-only thereafter.
pub(crate) static SERVERLESS_SEND_CONTEXT: std::sync::OnceLock<(
    Arc<NewRelicClient>,
    Arc<ExtensionConfig>,
    Arc<LogProcessor>,
)> = std::sync::OnceLock::new();

/// Add every payload in `payloads` to `AGENT_BATCH_BUFFER`, each paired with a clone of
/// `report_line` (or unpaired, if `None`). Shared by every arm of
/// `process_request_concurrently`'s send decision so the batching call is made exactly
/// the same way regardless of which arm supplied the payloads/report.
fn batch_all_payloads(
    request_id: &str,
    payloads: Vec<Vec<u8>>,
    report_line: Option<&str>,
    invoked_function_arn: &str,
) {
    for payload_bytes in payloads {
        add_to_batch(
            request_id.to_string(),
            payload_bytes,
            report_line.map(str::to_string),
            invoked_function_arn.to_string(),
        );
    }
}

/// Send every payload in `payloads` immediately and independently — decoupled from any
/// `platform.report` — as a single spawned task, for the rare orphaned-buffer edge case
/// where a payload ends up sitting in `agent_buffer` under
/// `NEW_RELIC_EXTENSION_SYNCHRONOUS_FLUSH` (it arrived before this request existed, so
/// `route_payload_to_request_buffer`'s own immediate-send path — the common case —
/// never saw it). Mirrors that same immediate-send behavior: on failure, buffers into
/// `AGENT_BATCH_BUFFER` (unpaired) rather than dropping the payload.
fn spawn_immediate_agent_payload_sends(
    request_id: String,
    payloads: Vec<Vec<u8>>,
    invoked_function_arn: String,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let version_info = version::VersionInfo::get_or_detect(config.new_relic.layer_version.clone());
        for payload_bytes in payloads {
            if let Err(e) = crate::agent::payload::send_agent_payload_to_newrelic(
                &payload_bytes,
                &request_id,
                &invoked_function_arn,
                &newrelic_client,
                &config,
                Some(&version_info),
            )
            .await
            {
                warn!(
                    "Serverless mode: immediate send failed for request {} — buffering for retry: {}",
                    request_id, e
                );
                add_to_batch(request_id.clone(), payload_bytes, None, invoked_function_arn.clone());
            }
        }
    })
}

/// Spawn the batch-send task. When `NEW_RELIC_EXTENSION_SYNCHRONOUS_FLUSH` is enabled,
/// every successful report pairing sends immediately — waiting for the 3+ count
/// threshold would mean a low-invocation-frequency function's report/billing data
/// still doesn't ship until `SHUTDOWN`. With the flag off, behavior is unchanged: only
/// spawn once the threshold is hit.
///
/// `pub(crate)` so `telemetry::listener`'s `platform.report` handler — which pairs
/// independently of `process_request_concurrently` — can reuse the same bypass via
/// `SERVERLESS_SEND_CONTEXT` instead of duplicating this condition.
pub(crate) fn maybe_spawn_batch_send(
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
) -> Option<tokio::task::JoinHandle<()>> {
    if config.new_relic.synchronous_flush || should_send_batch_by_threshold() {
        debug!("Sending payloads with report lines now");
        Some(tokio::spawn(async move {
            send_batched_payloads_with_reports_only(newrelic_client, config).await;
        }))
    } else {
        None
    }
}

pub async fn process_request_concurrently(
    request_id: String,
    invoked_function_arn: String,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
    deadline_ms: i64,
) {
    debug!(
        "Serverless mode: Starting processing for request: {}",
        request_id
    );


    let state = REQUEST_PROCESSORS.remove(&request_id).map(|(_, v)| v);

    let Some(state) = state else {
        error!("No processing state found for request: {}", request_id);
        return;
    };

    let invocation_start_time = chrono::Utc::now();
    global_log_processor.set_invocation_start_time(invocation_start_time);
    // trace.id buffering is per-request (keyed by request_id) inside LogProcessor — no
    // per-invocation reset needed; each request's held logs are stamped + flushed when
    // its deferred agent payload is processed.
    state
        .platform_processor
        .process_invoke_event(&request_id, &invoked_function_arn);

    // Drain whatever the background pipe listener has already routed to this request's buffer.
    // No waiting — if the agent payload hasn't arrived yet, the Telemetry listener will match
    // it with the platform.report when both arrive (same or next invocation) via the
    // agent_buffer → AGENT_BATCH_BUFFER pairing in listener.rs.
    let agent_payloads = {
        if let Ok(mut buffer) = state.agent_buffer.lock() {
            std::mem::take(&mut *buffer)
        } else {
            Vec::new()
        }
    };

    if agent_payloads.is_empty() {
        debug!("Serverless mode: No agent payload in buffer for request: {} - will be matched when it arrives", request_id);
    } else {
        debug!("Serverless mode: {} agent payload(s) in buffer for request: {}", agent_payloads.len(), request_id);
        // Extract this request's trace.id from its payload(s) and stamp + flush its
        // held logs. The trace lives in the payload (independent of the platform.report),
        // so do this as soon as the payload is in hand — even if it gets put back to
        // wait for its report below. Mirrors the APM path's extract_and_coordinate_trace_id.
        if config.new_relic.collect_trace_id {
            for payload_bytes in &agent_payloads {
                extract_and_coordinate_trace_id(payload_bytes, &request_id, &config, &global_log_processor).await;
            }
        }
    }

    let report_line = remove_pending_report(&request_id).map(|report| {
        debug!(
            "Found pending platform.report for request: {}",
            request_id
        );
        report
    });

    // Check if there was a detected error but no agent payload
    // This can happen when the function code has errors but doesn't send telemetry
    if agent_payloads.is_empty() {
        if let Ok(guard) = crate::error_synthesis::LAST_DETECTED_ERROR.lock() {
            if let Some(ref detected_error) = *guard {
                if detected_error.request_id == request_id {
                    debug!(
                        "Serverless mode: No agent payload for request {} but error detected: {} - sending to telemetry",
                        request_id, detected_error.error_type
                    );
                    // Error was already sent by log processor, just log this for visibility
                }
            }
        }
    }

    // Smart batching: only send complete payloads (with report). There is no wait for
    // a late agent payload or a late platform.report here: the Telemetry API only
    // emits platform.report after every extension — including this one — has already
    // called /next for the invocation, so a fresh request's own report can never be
    // "already arrived" at this point in practice, and waiting for a payload would
    // gain nothing either way. Under NEW_RELIC_EXTENSION_SYNCHRONOUS_FLUSH the agent
    // payload is instead sent the instant it's received, by
    // request::route_payload_to_request_buffer — so agent_payloads here is normally
    // empty except for the rare orphaned-buffer edge case (payload arrived before this
    // request existed) handled explicitly below. The common report-arrives-after-
    // payload case (flag off) is paired opportunistically by telemetry::listener's
    // platform.report handler, which runs independently of this function; see
    // SERVERLESS_SEND_CONTEXT.
    let send_agent_task = if agent_payloads.is_empty() {
        // No payload yet. If a report is already here, restore it so it isn't silently
        // lost — an UNCONDITIONAL correctness fix, independent of any flag: report_line
        // was already taken out of REQUEST_DATA via remove_pending_report() above, and
        // without restoring it here it would vanish (never found by
        // drain_late_paired_payloads_serverless or SHUTDOWN).
        if let Some(report) = report_line {
            debug!(
                "Serverless mode: No agent payload for request: {} - restoring report for next invocation",
                request_id
            );
            request::set_pending_report(&request_id, report);
        } else {
            debug!("Serverless mode: No agent payload for request: {}", request_id);
        }
        None
    } else if config.new_relic.synchronous_flush {
        // Payload(s) present — the orphaned-buffer edge case, since
        // route_payload_to_request_buffer no longer buffers under this flag on the
        // normal active-request path — and the flag is on: send immediately, decoupled
        // from any report (agent-payload delivery and platform.report handling are
        // independent under this flag; trace-id extraction already ran above,
        // unconditional whenever agent_payloads is non-empty). Restore report_line (if
        // any) rather than pairing it, so it's never lost and still gets swept up by
        // the untouched report-side path.
        if let Some(report) = report_line {
            request::set_pending_report(&request_id, report);
        }
        Some(spawn_immediate_agent_payload_sends(
            request_id.clone(),
            agent_payloads,
            invoked_function_arn.clone(),
            newrelic_client.clone(),
            config.clone(),
        ))
    } else if let Some(ref report) = report_line {
        // Both payload and report available - send now (complete data)
        debug!(
            "Serverless mode: Payload + report both ready for {} - adding to batch",
            request_id
        );
        batch_all_payloads(&request_id, agent_payloads, Some(report.as_str()), &invoked_function_arn);
        maybe_spawn_batch_send(newrelic_client.clone(), config.clone())
    } else {
        // Only payload, no report yet, flag off (default) - put back in buffer for next invocation
        debug!(
            "Serverless mode: Payload ready but NO report yet for {} - keeping in buffer",
            request_id
        );
        debug!(
            "Payload will be sent in next invocation when platform.report arrives"
        );

        // Put payloads back in buffer (they were taken out with mem::take)
        if let Some(buffer_ref) = get_agent_buffer(&request_id) {
            if let Ok(mut buffer) = buffer_ref.lock() {
                buffer.extend(agent_payloads);
            }
        }

        None
    };

    // This caller only ever needed the wait for log-flush purposes (unlike
    // process_apm_request, which now needs it unconditionally for agent-payload
    // correctness) — so preserve the original behavior of skipping the wait
    // entirely when function-log forwarding is off, rather than always paying for
    // the core wait and only gating the grace period.
    let runtime_done_signaled = if config.extension.send_function_logs {
        wait_for_runtime_done(&request_id, deadline_ms).await
    } else {
        false
    };
    wait_for_runtime_done_with_grace(
        &request_id,
        runtime_done_signaled,
        &config,
        &global_log_processor,
    )
    .await;

    let log_flushing = global_log_processor.flush();
    let platform_flushing = state.platform_processor.flush();

    // Immediate-send tasks kicked off for this request by
    // route_payload_to_request_buffer (NEW_RELIC_EXTENSION_SYNCHRONOUS_FLUSH) run
    // concurrently with the invocation's own handler code — await them here, bounded
    // by the remaining deadline, so a send in flight can't be frozen mid-POST when the
    // sandbox freezes after this function returns.
    let pending_sends_await = async {
        let pending_sends = request::take_pending_send_handles(&request_id);
        if pending_sends.is_empty() {
            return;
        }
        let budget_ms = bounded_wait_budget_ms(deadline_ms, u64::MAX);
        if budget_ms == 0 {
            debug!(
                "Serverless mode: no deadline budget remaining to await pending immediate sends for request: {}",
                request_id
            );
            return;
        }
        let _ = tokio::time::timeout(Duration::from_millis(budget_ms), async {
            for handle in pending_sends {
                if let Err(e) = handle.await {
                    error!(
                        "Immediate agent-payload send task panicked for request {}: {}",
                        request_id, e
                    );
                }
            }
        })
        .await;
    };

    let (log_result, platform_result, agent_result, ()) = tokio::join!(
        log_flushing,
        platform_flushing,
        async {
            if let Some(handle) = send_agent_task {
                handle.await
            } else {
                Ok(())
            }
        },
        pending_sends_await,
    );

    if let Err(e) = log_result {
        error!("Failed to flush logs for request {}: {}", request_id, e);
    }
    if let Err(e) = platform_result {
        error!("Failed to flush platform for request {}: {}", request_id, e);
    }
    if let Err(e) = agent_result {
        error!("Agent send task failed for request {}: {}", request_id, e);
    }

    // Unified cleanup: Always preserve buffers for late payload handling
    cleanup_request_processing_state_internal(&request_id, true);


    debug!(
        "Serverless mode: Completed processing for request: {}",
        request_id
    );
}

/// Tag Lambda function once on first invocation (or first LMI cold start).
///
/// On Normal Lambda this is called from the INVOKE arm on the first event.
/// On LMI, INVOKE is never delivered; the LMI heartbeat calls this after
/// `LMI_COLD_START_SEEN` is set by `platform.initReport`.
/// The internal `Once` guard ensures tagging fires exactly once regardless of
/// which path gets here first.
pub(crate) fn tag_lambda_function_once(invoked_function_arn: String, config: &config::ExtensionConfig) {
    static TAGGING_DONE: std::sync::Once = std::sync::Once::new();
    TAGGING_DONE.call_once(|| {
        debug!("Spawning background task to tag Lambda function with version information");
        let version_info = version::VersionInfo::get_or_detect(config.new_relic.layer_version.clone());
        let add_version_detail_tags = config.new_relic.add_version_detail_tags;
        let layer_version_from_config = config.new_relic.layer_version.clone();
        let function_name = config.aws.function_name.clone();
        version::tagging::tag_lambda_function_background(
            version_info.extension_version.clone(),
            version_info.agent_version.clone(),
            version_info.layer_version.clone(),
            invoked_function_arn,
            layer_version_from_config,
            add_version_detail_tags,
            function_name,
        );
    });
}

/// Update global invocation context for telemetry processors
fn update_global_invocation_context(request_id: &str, invoked_function_arn: &str) {
    if let Ok(mut global_context) = crate::CURRENT_INVOCATION_CONTEXT.write() {
        // Validate ARN before updating
        if invoked_function_arn.is_empty() {
            error!(
                "CRITICAL: Attempted to update global context with EMPTY invoked_function_arn for request_id: {}. Keeping previous ARN: {}",
                request_id,
                global_context.invoked_function_arn
            );
        } else {
            debug!(
                "Updating global context: request_id='{}', invoked_function_arn='{}' (previous ARN: '{}')",
                request_id,
                invoked_function_arn,
                global_context.invoked_function_arn
            );
            global_context.invoked_function_arn = invoked_function_arn.to_string();
        }
        global_context.request_id = request_id.to_string();
        global_context.trace_id = None;
    }
}

/// Extract trace ID from agent payload if enabled in config
pub(crate) async fn extract_and_coordinate_trace_id(
    payload_bytes: &[u8],
    request_id: &str,
    config: &Arc<ExtensionConfig>,
    log_processor: &Arc<LogProcessor>,
) {
    if !config.new_relic.collect_trace_id {
        return;
    }

    if let Ok(Some(trace_id)) = trace::extract_trace_id_from_payload(payload_bytes) {
        debug!("Extracted trace ID: {}, coordinating with logs", trace_id);
        if let Err(e) = log_processor.on_trace_id_extracted(request_id, &trace_id).await {
            error!("Failed to coordinate logs with trace ID: {}", e);
        }
    }
}

/// Serverless mode: drain previous-request buffers that have BOTH a pending agent payload
/// AND a platform.report. Called at the start of each warm INVOKE.
///
/// Race this fixes:
///   1. Invocation A completes — agent payload not yet in buffer.
///   2. platform.report for A arrives in invocation B → listener finds agent_buffer[A]
///      empty → stores as pending_report[A].
///   3. Agent payload for A arrives late via named pipe → routes to agent_buffer[A].
///   4. Neither the listener nor process_request_concurrently pairs them because
///      each only handles its own request.
///   5. This function (called at the start of invocation B+1) sees both, pairs them,
///      and calls add_to_batch so the batch sender can transmit them.
async fn drain_late_paired_payloads_serverless(
    current_request_id: &str,
    config: &Arc<ExtensionConfig>,
    global_log_processor: &Arc<LogProcessor>,
) -> usize {
    // Collect IDs of old entries that have BOTH pending_report AND non-empty agent_buffer.
    // Avoid holding DashMap refs across the subsequent get_mut calls.
    let candidates: Vec<String> = REQUEST_DATA
        .iter()
        .filter(|entry| {
            entry.key() != current_request_id
                && entry.pending_report.is_some()
                && entry.agent_buffer.lock().map(|b| !b.is_empty()).unwrap_or(false)
        })
        .map(|entry| entry.key().clone())
        .collect();

    if candidates.is_empty() {
        return 0;
    }

    debug!(
        "Serverless mode: {} previous request(s) have paired payload+report — batching before {}",
        candidates.len(),
        current_request_id
    );

    let mut batched_count = 0;

    for req_id in candidates {
        let Some(mut entry) = REQUEST_DATA.get_mut(&req_id) else { continue };

        // Take the report — if gone, someone else raced us (listener matched it already).
        let report = match entry.pending_report.take() {
            Some(r) => r,
            None => continue,
        };

        // Clone the Arc refs while we hold the write guard, then drop the guard
        // before locking the Mutex to avoid the borrow-while-mutably-borrowed error.
        let buffer_arc = entry.agent_buffer.clone();
        let arn = if !entry.invoked_function_arn.is_empty() {
            entry.invoked_function_arn.clone()
        } else {
            crate::get_global_fallback_arn()
        };

        drop(entry); // release DashMap write guard

        let payloads: Vec<Vec<u8>> = match buffer_arc.lock() {
            Ok(mut buf) => std::mem::take(&mut *buf),
            Err(_) => Vec::new(),
        };

        if payloads.is_empty() {
            // Buffer was empty (raced with listener or nothing arrived yet).
            // Restore the report so the listener can still pair it when payload arrives.
            if let Some(mut e) = REQUEST_DATA.get_mut(&req_id) {
                if e.pending_report.is_none() {
                    e.pending_report = Some(report);
                }
            }
            continue;
        }

        let payload_count = payloads.len();
        for payload_bytes in payloads {
            // Extract this late payload's trace.id and stamp + flush the request's held
            // logs before batching it for send.
            if config.new_relic.collect_trace_id {
                extract_and_coordinate_trace_id(&payload_bytes, &req_id, config, global_log_processor).await;
            }
            add_to_batch(req_id.clone(), payload_bytes, Some(report.clone()), arn.clone());
        }
        batched_count += payload_count;
        debug!(
            "Serverless mode: Batched {} late payload(s) for previous request: {}",
            payload_count, req_id
        );
    }

    batched_count
}

/// Process any pending agent payloads from previous invocation (APM mode only)
/// Excludes the current request ID to avoid processing empty buffer
pub(crate) async fn process_pending_agent_payloads(
    config: &Arc<ExtensionConfig>,
    global_log_processor: &Arc<LogProcessor>,
    apm_app: &crate::apm::SharedApmApp,
    current_request_id: &str,
) {
    let all_buffers: Vec<(String, usize)> = REQUEST_DATA
        .iter()
        .map(|entry| {
            let buffer_size = entry.agent_buffer.lock().map(|b| b.len()).unwrap_or(0);
            (entry.key().clone(), buffer_size)
        })
        .collect();

    debug!(
        "APM pending check: Total buffers={}, Details: {:?}",
        all_buffers.len(),
        all_buffers
    );

    let pending_requests: Vec<(String, Arc<Mutex<Vec<Vec<u8>>>>)> = REQUEST_DATA
        .iter()
        .filter(|entry| entry.key() != current_request_id)
        .map(|entry| (entry.key().clone(), entry.agent_buffer.clone()))
        .collect();

    if pending_requests.is_empty() {
        debug!(
            "No pending agent payload buffers from previous invocations (current request excluded: {})",
            current_request_id
        );
        return;
    }

    debug!(
        "Processing {} pending agent payload buffer(s) from previous invocations (excluding current: {})",
        pending_requests.len(),
        current_request_id
    );

    for (request_id, buffer) in pending_requests {
        // Captured before cleanup removes the REQUEST_DATA entry below — how late,
        // not just that it's late (see format_elapsed_for_log).
        let elapsed_ms = request::get_runtime_done_elapsed_ms(&request_id);

        let context = get_request_context(&request_id);

        let invoked_function_arn = if let Some(ctx) = context {
            if let Ok(ctx_guard) = ctx.lock() {
                if !ctx_guard.invoked_function_arn.is_empty() {
                    ctx_guard.invoked_function_arn.clone()
                } else {
                    crate::get_global_fallback_arn()
                }
            } else {
                crate::get_global_fallback_arn()
            }
        } else {
            crate::get_global_fallback_arn()
        };

        let payloads = {
            if let Ok(mut buf) = buffer.lock() {
                std::mem::take(&mut *buf)
            } else {
                Vec::new()
            }
        };

        if !payloads.is_empty() {
            let chunk_count = payloads.len();
            debug!(
                "Found {} pending agent payload(s) for previous request: {}",
                chunk_count, request_id
            );

            // Trace-id extraction is per-chunk (independent of telemetry-map
            // merging) — preserved here before chunks are merged/sent together.
            if config.new_relic.collect_trace_id {
                for payload_bytes in &payloads {
                    if let Ok(Some(trace_id)) = trace::extract_trace_id_from_payload(payload_bytes) {
                        if let Err(e) = global_log_processor.on_trace_id_extracted(&request_id, &trace_id).await {
                            error!("Failed to coordinate logs with trace ID: {}", e);
                        }
                    }
                }
            }

            // Merge mergeable event-list types across all of this request's
            // buffered chunks into one collector POST per type, rather than one
            // POST per chunk per type (see ApmApp::process_merged_agent_payloads).
            // Clone out and drop the guard before the send — see process_apm_request's
            // identical fix for why a read lock must not span the HTTP POSTs below.
            let app_owned = apm_app.read().await.clone();
            if let Some(app) = app_owned {
                let send_result = app.process_merged_agent_payloads(payloads.clone(), &request_id, &invoked_function_arn).await;
                match send_result {
                    // `sent_count` is how many chunks were actually parsed and
                    // attempted — a chunk that failed to parse is buffered
                    // internally by process_merged_agent_payloads and does NOT
                    // count here, so this can legitimately be 0 of chunk_count
                    // even though the call returned Ok (that function's contract:
                    // buffered-for-retry is a handled outcome, not a propagated
                    // error). Logging "Successfully sent" unconditionally on Ok
                    // would claim success even when every chunk failed to parse
                    // and was buffered instead — distinguish the two.
                    Ok(sent_count) if sent_count == chunk_count => {
                        info!(
                            "Successfully sent {} pending agent payload chunk(s) for request: {} (arrived {} after runtime.done)",
                            chunk_count, request_id, format_elapsed_for_log(elapsed_ms)
                        );
                    }
                    Ok(sent_count) => {
                        warn!(
                            "Sent {} of {} pending agent payload chunk(s) for request: {} (arrived {} after runtime.done) — the rest failed to parse and were buffered for retry",
                            sent_count, chunk_count, request_id, format_elapsed_for_log(elapsed_ms)
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to send {} pending agent payload chunk(s) for {} (arrived {} after runtime.done): {} - buffering for retry",
                            chunk_count, request_id, format_elapsed_for_log(elapsed_ms), e
                        );
                        for payload_bytes in &payloads {
                            buffer_failed_agent_payload(payload_bytes, &request_id, &invoked_function_arn);
                        }
                    }
                }
            } else {
                warn!(
                    "APM connection not established - buffering {} pending agent payload chunk(s) for request: {} (arrived {} after runtime.done)",
                    chunk_count, request_id, format_elapsed_for_log(elapsed_ms)
                );
                for payload_bytes in &payloads {
                    buffer_failed_agent_payload(payload_bytes, &request_id, &invoked_function_arn);
                }
            }
        }

        // Check for pending platform.report for this old request and send as metrics (APM mode)
        if let Some(report_line) = get_pending_report(&request_id) {
            debug!("APM mode: Found pending platform.report for previous request {} - converting to metrics", request_id);

            // Clone out and drop the guard before the send — same reasoning as
            // process_apm_request's agent-payload send fix.
            let app_owned = apm_app.read().await.clone();
            if let Some(app) = app_owned {
                // send_platform_report_metrics always returns Ok (permanent-drop and
                // buffered-for-retry are Ok too, by design) and already logs its own
                // accurate outcome internally — don't re-log here.
                let _ = app.send_platform_report_metrics(&report_line, &invoked_function_arn).await;
            }

            // Remove pending report after sending
            remove_pending_report(&request_id);
        }

        cleanup_request_processing_state_internal(&request_id, false);
    }
}

/// Process and send agent payload following our simple flow
async fn process_and_send_agent_payload(
    payload_bytes: &[u8],
    request_id: &str,
    invoked_function_arn: &str,
    log_processor: &Arc<LogProcessor>,
    config: &Arc<ExtensionConfig>,
    apm_app: &crate::apm::SharedApmApp,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if config.new_relic.collect_trace_id {
        if let Ok(Some(trace_id)) = trace::extract_trace_id_from_payload(payload_bytes) {
            debug!("Extracted trace ID: {}, coordinating with logs", trace_id);

            if let Err(e) = log_processor.on_trace_id_extracted(request_id, &trace_id).await {
                error!("Failed to coordinate logs with trace ID: {}", e);
            }
        } else {
            debug!("No trace ID found in agent payload or extraction failed");
        }
    } else {
        debug!("Trace ID collection disabled, skipping extraction");
    }

    let apm_app_guard = apm_app.read().await;
    if let Some(ref app) = *apm_app_guard {
        debug!(
            "APM mode: Processing agent payload for request: {} (size: {} bytes)",
            request_id,
            payload_bytes.len()
        );
        match app.process_agent_payload(payload_bytes.to_vec(), request_id).await {
            Ok(()) => {
                info!("APM mode: Agent payload sent successfully for request: {}", request_id);
            }
            Err(e) => {
                warn!("APM mode: Failed to send agent payload to APM collector: {}", e);
                buffer_failed_agent_payload(payload_bytes, request_id, invoked_function_arn);
                warn!(
                    "APM mode: Agent payload buffered for retry (size: {} bytes)",
                    payload_bytes.len()
                );
            }
        }
    } else {
        // APM connection not ready - buffer the payload, do NOT send to telemetry endpoint
        warn!(
            "APM connection not established - buffering agent payload for request: {} (size: {} bytes)",
            request_id,
            payload_bytes.len()
        );
        buffer_failed_agent_payload(payload_bytes, request_id, invoked_function_arn);
    }

    Ok(())
}

/// Build the shutdown "telemetry DROPPED" summary in two forms:
/// - the CloudWatch line (counts only — request_ids would bloat it and aren't
///   queryable there), returned first;
/// - the New Relic line (the same summary plus the affected request_ids for
///   correlation), returned second.
/// Pure (no I/O) so the wording and the CloudWatch-vs-NR split are unit-tested.
#[allow(clippy::too_many_arguments)]
/// Data for the shutdown "telemetry DROPPED" diagnostic. The human summary goes to the
/// CloudWatch line AND the NR log message; the request_ids and counts go to NR as
/// queryable attributes (not embedded in the message text).
struct ShutdownDropDiagnostic {
    message: String,
    arn: String,
    /// The last/current request_id (the invocation during which shutdown occurred);
    /// stamped as aws.lambda_request_id / faas.execution for consistency with other
    /// extension logs. The full dropped set is in `request_ids`.
    request_id: String,
    /// Distinct request_ids whose telemetry was dropped (the affected invocations).
    request_ids: Vec<String>,
    /// = request_ids.len() (distinct invocations affected).
    request_id_count: usize,
    /// Raw buffered item count across all APM buffers (can exceed request_id_count
    /// when one invocation has multiple buffered items).
    item_count: usize,
}

/// Max request_ids embedded in the `dropped.request_ids` attribute string. The numeric
/// `dropped.request_id_count` is always exact even if this list is truncated.
const MAX_DROPPED_IDS_IN_ATTR: usize = 100;

/// Build the human-readable shutdown drop summary (CloudWatch line and NR log message
/// body). Counts only — no request_ids. Worded so item/invocation counts read as
/// "N items across M invocations" rather than inviting an additive misread.
/// Pure (no I/O) so the wording is unit-tested.
fn build_shutdown_drop_summary(
    apm_connected: bool,
    affected: usize,
    remaining_count: usize,
    dropped_earlier: u64,
    reason: &str,
    cycles: u64,
    attempts: u64,
) -> String {
    let earlier_note = if dropped_earlier > 0 {
        format!(" (+{dropped_earlier} more dropped earlier)")
    } else {
        String::new()
    };
    if apm_connected {
        format!(
            "APM telemetry DROPPED at shutdown: {remaining_count} item(s) across {affected} invocation(s) could not be sent despite APM being connected{earlier_note}."
        )
    } else {
        format!(
            "APM telemetry DROPPED at shutdown: APM never connected (last failure: {reason}) after {cycles} reconnect cycle(s) / {attempts} handshake attempt(s) — {remaining_count} item(s) across {affected} invocation(s) lost{earlier_note}."
        )
    }
}

/// Build the New Relic diagnostic log: the summary as the message, plus the dropped
/// request_ids and counts as queryable attributes (`dropped.request_ids`,
/// `dropped.request_id_count`, `dropped.item_count`). Pure so attributes are unit-tested.
fn build_shutdown_drop_log(diag: &ShutdownDropDiagnostic) -> crate::newrelic::payload::LogMessage {
    let mut log = crate::newrelic::payload::LogMessage::diagnostic(
        "ERROR",
        format!("[NR_EXT] ERROR {}", diag.message),
    );
    // Stamp request_id (last invocation) so the diagnostic carries the same
    // aws.lambda_request_id / faas.execution as other extension logs.
    if !diag.request_id.is_empty() {
        let mut aws_attrs = serde_json::Map::new();
        aws_attrs.insert(
            "lambda_request_id".to_string(),
            serde_json::json!(diag.request_id),
        );
        log.attributes
            .insert("aws".to_string(), serde_json::Value::Object(aws_attrs));
        log.attributes
            .insert("faas.execution".to_string(), serde_json::json!(diag.request_id));
    }
    if !diag.request_ids.is_empty() {
        let joined = if diag.request_ids.len() > MAX_DROPPED_IDS_IN_ATTR {
            format!(
                "{},(+{} more)",
                diag.request_ids[..MAX_DROPPED_IDS_IN_ATTR].join(","),
                diag.request_ids.len() - MAX_DROPPED_IDS_IN_ATTR
            )
        } else {
            diag.request_ids.join(",")
        };
        log.attributes
            .insert("dropped.request_ids".to_string(), serde_json::json!(joined));
    }
    log.attributes.insert(
        "dropped.request_id_count".to_string(),
        serde_json::json!(diag.request_id_count),
    );
    log.attributes.insert(
        "dropped.item_count".to_string(),
        serde_json::json!(diag.item_count),
    );
    log
}

/// Buffer failed agent payload for retry across invocations
pub(crate) fn buffer_failed_agent_payload(
    payload_bytes: &[u8],
    request_id: &str,
    invoked_function_arn: &str,
) {
    let failed_payload = FailedAgentPayload {
        payload_bytes: payload_bytes.to_vec(),
        request_id: request_id.to_string(),
        invoked_function_arn: invoked_function_arn.to_string(),
        retry_count: 0,
        failed_at: chrono::Utc::now(),
    };

    if let Ok(mut failed_payloads) = FAILED_AGENT_PAYLOADS.lock() {
        push_failed_payload_capped(&mut failed_payloads, failed_payload);
        debug!(
            "Buffered failed agent payload for request {} (total failed: {})",
            request_id,
            failed_payloads.len()
        );
    } else {
        error!("Failed to lock FAILED_AGENT_PAYLOADS buffer - payload lost!");
    }
}

/// What happened to one buffered agent payload during a retry pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryPayloadOutcome {
    Success,
    Failed,
    /// APM had no session yet: kept buffered, not a failed send.
    NotConnected,
    /// Dropped for being absurdly old.
    Skipped,
}

/// Retry failed agent payloads via the APM collector (APM mode only).
///
/// `FAILED_AGENT_PAYLOADS` is populated solely by the APM payload path
/// (`process_and_send_agent_payload`) when the APM collector is unreachable or not yet
/// connected. Retries therefore go back to the **APM collector** — never the serverless
/// telemetry endpoint — so APM-mode telemetry can't leak into the serverless pipeline.
/// Payloads that still can't be sent (APM not connected) stay buffered for the next
/// invoke / shutdown. Mirrors the Err→re-buffer convention of the initial send path.
async fn retry_failed_agent_payloads(apm_app: crate::apm::SharedApmApp) {
    let failed_payloads = {
        if let Ok(mut failed_payloads) = FAILED_AGENT_PAYLOADS.lock() {
            std::mem::take(&mut *failed_payloads)
        } else {
            error!("Failed to lock FAILED_AGENT_PAYLOADS for retry");
            return;
        }
    };

    if failed_payloads.is_empty() {
        debug!("No failed agent payloads to retry");
        return;
    }

    debug!(
        "Retrying {} failed agent payloads during final flush",
        failed_payloads.len()
    );

    // Every payload is retried at once: a timeout on one must not make the others wait behind it.
    let apm_app = &apm_app;
    let outcomes = join_all_concurrently(
        failed_payloads
            .into_iter()
            .map(|mut failed_payload| {
                Box::pin(async move {
        failed_payload.retry_count += 1;

        // A sandbox lives at most ~30 min, so this 24h guard effectively never
        // fires — it's only a backstop against absurdly stale data. We do NOT
        // drop on retry count: a payload is kept (subject to the buffer cap) and
        // retried every invoke until it flushes on reconnect or the container dies.
        let age = chrono::Utc::now().signed_duration_since(failed_payload.failed_at);
        if age.num_hours() > 24 {
            warn!(
                "Dropping agent payload that's too old ({} hours) for request {}",
                age.num_hours(),
                failed_payload.request_id
            );
            DROPPED_AGENT_PAYLOADS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return RetryPayloadOutcome::Skipped;
        }

        debug!(
            "Retrying agent payload for request {} (arn: {}, attempt {}) via APM collector",
            failed_payload.request_id, failed_payload.invoked_function_arn, failed_payload.retry_count
        );

        // Send to the APM collector (NOT the serverless endpoint). If APM isn't
        // connected yet, keep the payload buffered for a later invoke / shutdown.
        let send_result = {
            let apm_guard = apm_app.read().await;
            match *apm_guard {
                Some(ref app) => Some(
                    app.process_agent_payload(
                        failed_payload.payload_bytes.clone(),
                        &failed_payload.request_id,
                    )
                    .await,
                ),
                None => None,
            }
        };

        match send_result {
            Some(Ok(())) => {
                info!(
                    "Successfully retried agent payload for request {} (APM collector)",
                    failed_payload.request_id
                );
                RetryPayloadOutcome::Success
            }
            Some(Err(e)) => {
                warn!("Failed to retry agent payload to APM collector: {}", e);
                // Keep it buffered (capped) so it retries on the next invoke / reconnect.
                if let Ok(mut failed_payloads) = FAILED_AGENT_PAYLOADS.lock() {
                    push_failed_payload_capped(&mut failed_payloads, failed_payload);
                }
                RetryPayloadOutcome::Failed
            }
            None => {
                // APM not connected yet — not a failed send; keep buffered for later.
                debug!(
                    "APM not connected — keeping agent payload buffered for request {}",
                    failed_payload.request_id
                );
                if let Ok(mut failed_payloads) = FAILED_AGENT_PAYLOADS.lock() {
                    push_failed_payload_capped(&mut failed_payloads, failed_payload);
                }
                RetryPayloadOutcome::NotConnected
            }
        }
                }) as BoxedFuture<'_, RetryPayloadOutcome>
            })
            .collect(),
    )
    .await;
    let retry_successful_count = outcomes.iter().filter(|o| **o == RetryPayloadOutcome::Success).count();
    let retry_failed_count = outcomes.iter().filter(|o| **o == RetryPayloadOutcome::Failed).count();

    if retry_successful_count > 0 || retry_failed_count > 0 {
        debug!(
            "Agent payload retry results: {} successful, {} still failed",
            retry_successful_count, retry_failed_count
        );
    }
}

/// Clean up old failed payloads (older than 24 hours)
pub fn cleanup_old_failed_payloads() {
    if let Ok(mut failed_payloads) = FAILED_AGENT_PAYLOADS.lock() {
        let initial_count = failed_payloads.len();
        let now = chrono::Utc::now();

        failed_payloads.retain(|payload| {
            let age = now.signed_duration_since(payload.failed_at);
            age.num_hours() <= 24
        });

        let removed_count = initial_count - failed_payloads.len();
        if removed_count > 0 {
            debug!(
                "Cleaned up {} old failed agent payloads (kept {} recent ones)",
                removed_count,
                failed_payloads.len()
            );
        }
    }
}

#[cfg(test)]
#[path = "event_loop_tests.rs"]
mod tests;
