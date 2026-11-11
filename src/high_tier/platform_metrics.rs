// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Platform-report-metrics sending for `SendTier::High`.
//!
//! Spawned the instant a `platform.report` arrives, resolved at this
//! invocation's own `runtimeDone` against the shared budget in
//! [`super::SCHEDULER`]. On timeout: aborted and buffered (with
//! `request_id` attribution) into this module's own retry queue — kept
//! separate from `apm::metric_api_buffer::FAILED_METRIC_API_BUFFER`, which
//! buffers a different, later failure mode (a real HTTP failure on an
//! already-parsed metric batch, with no `request_id` to attribute to),
//! not an abort of a raw `platform.report` line.

use super::scheduler::{BoxedSend, Outcome, ResolveResult, Scheduler, SendOutcome};
use crate::apm::SharedApmApp;
use std::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Write-ahead copy of the report line about to be sent — the metadata the
/// scheduler itself doesn't know about. `report_line`'s only other home is
/// inside the spawned future; an abort with nothing recorded here would
/// lose it entirely.
struct WriteAhead {
    report_line: String,
    request_id: String,
    invoked_function_arn: String,
}

static WRITE_AHEAD: once_cell::sync::Lazy<Mutex<Option<WriteAhead>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(None));

/// A platform.report whose send was aborted before completing — buffered
/// here for a background retry, attributed to the request that actually
/// produced it.
struct FailedReport {
    report_line: String,
    request_id: String,
    invoked_function_arn: String,
    retry_count: usize,
    failed_at: chrono::DateTime<chrono::Utc>,
}

static FAILED_REPORTS: once_cell::sync::Lazy<Mutex<Vec<FailedReport>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(Vec::new()));

/// Memory guard (a sandbox lives at most ~30 min) against a sustained
/// outage, same reasoning as `event_loop`'s `MAX_FAILED_AGENT_PAYLOADS`.
const MAX_FAILED_REPORTS: usize = 100;
/// A `platform.report` is a periodic snapshot — an arbitrarily stale one
/// retried forever has little value, unlike an agent payload.
const MAX_RETRIES: usize = 3;

/// If a `platform.report` is pending for this request, spawn its metrics
/// send (resolved later, at this invocation's own `runtimeDone`). Skips
/// entirely (new report simply dropped — logged, not silently lost) if a
/// previous send is still running, to avoid two concurrent sends for the
/// same kind; occasional loss here is an accepted tradeoff, same as the
/// pre-dual-path behavior this mirrors.
pub(super) async fn spawn_if_ready(
    scheduler: &Scheduler,
    apm_app: &SharedApmApp,
    request_id: &str,
    invoked_function_arn: &str,
) {
    let Some(report_line) = crate::request::get_pending_report(request_id) else {
        debug!(
            "APM mode (high): no platform.report found for request {} (may arrive in next invocation)",
            request_id
        );
        return;
    };
    debug!("APM mode (high): found platform.report for request {} - converting to metrics", request_id);
    crate::request::remove_pending_report(request_id);

    if scheduler.has_platform_metrics_pending().await {
        debug!(
            "APM mode (high): previous platform-metrics send still running — report for {} dropped \
             (occasional loss here is an accepted tradeoff)",
            request_id
        );
        return;
    }

    match WRITE_AHEAD.lock() {
        Ok(mut guard) => {
            *guard = Some(WriteAhead {
                report_line: report_line.clone(),
                request_id: request_id.to_string(),
                invoked_function_arn: invoked_function_arn.to_string(),
            });
        }
        Err(e) => error!("platform_metrics WRITE_AHEAD lock poisoned, dropping write-ahead snapshot: {}", e),
    }

    let apm_app_clone = apm_app.clone();
    let request_id_owned = request_id.to_string();
    let invoked_function_arn_owned = invoked_function_arn.to_string();
    let send: BoxedSend = Box::pin(async move {
        let apm_app_guard = apm_app_clone.read().await;
        let Some(ref app) = *apm_app_guard else {
            warn!("APM mode (high): APM app not ready - cannot send platform metrics for {}", request_id_owned);
            return SendOutcome::Failure;
        };
        match app.send_platform_report_metrics(&report_line, &invoked_function_arn_owned).await {
            Ok(()) => {
                info!("APM mode (high): successfully sent platform report metrics for request {}", request_id_owned);
                SendOutcome::Success
            }
            Err(e) => {
                error!("APM mode (high): failed to send platform report metrics for {}: {}", request_id_owned, e);
                SendOutcome::Failure
            }
        }
    });
    scheduler.spawn_platform_metrics(send).await;
}

/// Handle the outcome of resolving whatever was pending for
/// platform-metrics — called once, right after
/// [`Scheduler::resolve_both_within`][super::scheduler::Scheduler::resolve_both_within].
pub(super) fn handle_outcome(outcome: &Outcome) {
    let write_ahead = match WRITE_AHEAD.lock() {
        Ok(mut guard) => guard.take(),
        Err(e) => {
            error!("platform_metrics WRITE_AHEAD lock poisoned: {}", e);
            None
        }
    };

    match outcome.result {
        ResolveResult::NothingPending => {}
        ResolveResult::Sent => {
            debug!("APM mode (high): platform-metrics send completed successfully");
        }
        ResolveResult::Failed => {
            debug!("APM mode (high): platform-metrics send failed, already handled internally");
        }
        ResolveResult::TimedOut => {
            if let Some(write_ahead) = write_ahead {
                buffer_failed(write_ahead.report_line, write_ahead.request_id, write_ahead.invoked_function_arn);
            } else {
                warn!("APM mode (high): platform-metrics send timed out but WRITE_AHEAD was empty — that report is lost");
            }
        }
    }
}

fn buffer_failed(report_line: String, request_id: String, invoked_function_arn: String) {
    warn!(
        "APM mode (high): platform-metrics send for {} did not finish within the shared runtimeDone \
         budget and was aborted — re-queuing for retry on a later invocation",
        request_id
    );
    let entry = FailedReport { report_line, request_id, invoked_function_arn, retry_count: 0, failed_at: chrono::Utc::now() };
    match FAILED_REPORTS.lock() {
        Ok(mut buf) => {
            if buf.len() >= MAX_FAILED_REPORTS {
                buf.remove(0);
                warn!("high_tier::platform_metrics: failed-report buffer full ({}) - evicted oldest", MAX_FAILED_REPORTS);
            }
            buf.push(entry);
        }
        Err(e) => error!("FAILED_REPORTS lock poisoned, report lost: {}", e),
    }
}

/// Retry buffered platform-report sends. Spawned detached every
/// invocation by [`super::pending_sweep`] — never awaited inline, so a
/// backlog here never blocks `/next`. If `apm_app` isn't ready yet, puts
/// everything back untouched rather than burning a retry attempt on a
/// guaranteed failure.
pub(super) async fn retry_failed(apm_app: SharedApmApp) {
    let pending = match FAILED_REPORTS.lock() {
        Ok(mut buf) => std::mem::take(&mut *buf),
        Err(e) => {
            error!("Failed to lock FAILED_REPORTS for retry: {}", e);
            return;
        }
    };
    if pending.is_empty() {
        return;
    }
    debug!("APM mode (high): retrying {} buffered platform-report(s)", pending.len());

    let apm_guard = apm_app.read().await;
    let Some(ref app) = *apm_guard else {
        drop(apm_guard);
        if let Ok(mut buf) = FAILED_REPORTS.lock() {
            buf.extend(pending);
        }
        return;
    };

    for mut entry in pending {
        // A sandbox lives at most ~30 min -- this guard against absurdly
        // stale data essentially never fires in practice.
        let age = chrono::Utc::now().signed_duration_since(entry.failed_at);
        if age.num_hours() > 24 {
            warn!("Dropping platform-report for request {} that's too old ({} hours)", entry.request_id, age.num_hours());
            continue;
        }

        match app.send_platform_report_metrics(&entry.report_line, &entry.invoked_function_arn).await {
            Ok(()) => {
                info!("APM mode (high): successfully retried buffered platform-report for request {}", entry.request_id);
            }
            Err(e) => {
                entry.retry_count += 1;
                if entry.retry_count >= MAX_RETRIES {
                    warn!(
                        "Dropping platform-report for request {} after {} failed retries: {}",
                        entry.request_id, entry.retry_count, e
                    );
                    continue;
                }
                warn!("Retry failed for buffered platform-report (request {}, attempt {}): {}", entry.request_id, entry.retry_count, e);
                if let Ok(mut buf) = FAILED_REPORTS.lock() {
                    if buf.len() >= MAX_FAILED_REPORTS {
                        buf.remove(0);
                    }
                    buf.push(entry);
                }
            }
        }
    }
}
