// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Agent-payload (Flow-1) sending for `SendTier::High`.
//!
//! Reuses `event_loop`'s tier-agnostic primitives rather than duplicating
//! them: [`crate::event_loop::send_agent_payload_or_buffer`] (send one
//! payload, buffer into `FAILED_AGENT_PAYLOADS` on failure — identical
//! semantics regardless of tier), `crate::event_loop::FAILED_AGENT_PAYLOADS`
//! itself, and [`crate::event_loop::retry_failed_agent_payloads`] (the
//! existing detached, per-invocation retry sweep over that buffer). What's
//! new here is only the *scheduling*: spawn now, resolve at this
//! invocation's own `runtimeDone` against the shared budget in
//! [`super::SCHEDULER`], abort-and-buffer on timeout.

use super::scheduler::{BoxedSend, Outcome, ResolveResult, Scheduler};
use crate::apm::SharedApmApp;
use crate::config::ExtensionConfig;
use crate::logs::processor::LogProcessor;
use crate::request::RequestProcessingState;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{debug, error, warn};

/// Write-ahead record of the batch currently spawned into the scheduler —
/// the metadata the scheduler itself deliberately doesn't know about (see
/// `scheduler` module doc: it only tracks a `JoinHandle` + progress
/// cursor). Mirrors the dual-path branch's `AgentBatchMeta`, rebuilt fresh
/// here. At most one entry at a time: normal-mode Lambda never runs
/// concurrent invocations in one environment.
struct BatchMeta {
    payloads: Vec<Arc<Vec<u8>>>,
    request_id: String,
    invoked_function_arn: String,
}

static BATCH_META: once_cell::sync::Lazy<Mutex<Option<BatchMeta>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(None));

/// If `run_id` is available and a payload arrived this invocation, spawn
/// the send (resolved later, at this invocation's own `runtimeDone`, by
/// [`super::process_invocation`]). If `run_id` isn't ready yet, buffer the
/// payload back for when it is. If nothing arrived, no-op.
#[allow(clippy::too_many_arguments)]
pub(super) async fn spawn_if_ready(
    scheduler: &Scheduler,
    request_id: &str,
    invoked_function_arn: &str,
    config: &Arc<ExtensionConfig>,
    global_log_processor: &Arc<LogProcessor>,
    apm_app: &SharedApmApp,
    state: &RequestProcessingState,
    has_run_id: bool,
) {
    let agent_payloads = {
        if let Ok(mut buffer) = state.agent_buffer.lock() {
            std::mem::take(&mut *buffer)
        } else {
            Vec::new()
        }
    };
    let got_payload = !agent_payloads.is_empty();

    if got_payload && scheduler.has_agent_payload_pending().await {
        // A previous batch hasn't been resolved yet (should be rare --
        // resolution happens every invocation's own runtimeDone). Put this
        // payload back rather than spawning: Scheduler::spawn_agent_payload's
        // orphan-abort would otherwise hard-kill the still-running batch with
        // no cursor recovery, silently losing its unresolved tail. Queuing
        // behind is slower but never loses data.
        debug!(
            "APM mode (high): previous agent-payload batch still pending — queuing {} new payload(s) behind it",
            agent_payloads.len()
        );
        if let Some(buffer_ref) = crate::request::get_agent_buffer(request_id) {
            if let Ok(mut buffer) = buffer_ref.lock() {
                buffer.extend(agent_payloads);
            }
        }
        return;
    }

    if has_run_id && got_payload {
        debug!(
            "APM mode (high): run_id available + agent payload arrived ({} payload(s)) - spawning",
            agent_payloads.len()
        );

        let request_id_owned = request_id.to_string();
        let invoked_function_arn_owned = invoked_function_arn.to_string();
        let config_clone = config.clone();
        let log_processor_clone = global_log_processor.clone();
        let apm_app_clone = apm_app.clone();

        // Arc-wrap once (moves the bytes, no copy); cloning for the
        // spawned task is then a refcount bump. The original Arc vec is
        // retained in BATCH_META for the timeout-fallback buffering.
        let payloads: Vec<Arc<Vec<u8>>> = agent_payloads.into_iter().map(Arc::new).collect();
        let payloads_for_task = payloads.clone();

        // Progress cursor: incremented strictly after each payload's
        // send/buffer outcome is fully resolved.
        let cursor = Arc::new(AtomicUsize::new(0));
        let cursor_clone = cursor.clone();

        let send: BoxedSend = Box::pin(async move {
            let mut all_sent = true;
            for payload_bytes in &payloads_for_task {
                crate::event_loop::extract_and_coordinate_trace_id(
                    payload_bytes.as_slice(),
                    &request_id_owned,
                    &config_clone,
                    &log_processor_clone,
                )
                .await;

                // On failure the payload is buffered into FAILED_AGENT_PAYLOADS
                // (never silently dropped) by the shared helper; the existing
                // detached retry sweep resends it on a later invoke / at shutdown.
                if !crate::event_loop::send_agent_payload_or_buffer(
                    payload_bytes.as_slice(),
                    &request_id_owned,
                    &invoked_function_arn_owned,
                    &apm_app_clone,
                )
                .await
                {
                    all_sent = false;
                }
                cursor_clone.fetch_add(1, Ordering::SeqCst);
            }
            if all_sent {
                super::scheduler::SendOutcome::Success
            } else {
                super::scheduler::SendOutcome::Failure
            }
        });

        let meta = BatchMeta {
            payloads,
            request_id: request_id.to_string(),
            invoked_function_arn: invoked_function_arn.to_string(),
        };
        match BATCH_META.lock() {
            Ok(mut guard) => {
                if guard.is_some() {
                    // Defensive, should-not-happen: Scheduler::spawn_agent_payload's
                    // own orphan-abort already handles the handle side of this;
                    // this just keeps the metadata side in lockstep with it.
                    warn!(
                        "APM mode (high): overwriting unresolved agent-payload BATCH_META — \
                         the previous batch should have been resolved first"
                    );
                }
                *guard = Some(meta);
            }
            Err(e) => error!("agent_payload BATCH_META lock poisoned, dropping new entry: {}", e),
        }

        scheduler.spawn_agent_payload(send, Some(cursor)).await;
    } else if !has_run_id && got_payload {
        debug!(
            "APM mode (high): no run_id yet but agent payload arrived ({} payload(s)) - buffering for when run_id becomes available",
            agent_payloads.len()
        );
        if let Some(buffer_ref) = crate::request::get_agent_buffer(request_id) {
            if let Ok(mut buffer) = buffer_ref.lock() {
                buffer.extend(agent_payloads);
            }
        }
    } else if has_run_id {
        debug!(
            "APM mode (high): run_id available but no agent payload yet for request: {} - will catch in next invocation if it arrives late",
            request_id
        );
    } else {
        debug!(
            "APM mode (high): no run_id and no agent payload for request: {} - normal flow",
            request_id
        );
    }
}

/// Handle the outcome of resolving (bounded, abort-capable) whatever was
/// pending for the agent-payload batch — called once, right after
/// [`Scheduler::resolve_both_within`][super::scheduler::Scheduler::resolve_both_within].
pub(super) fn handle_outcome(outcome: &Outcome) {
    if outcome.result == ResolveResult::NothingPending {
        return;
    }

    let meta = match BATCH_META.lock() {
        Ok(mut guard) => guard.take(),
        Err(e) => {
            error!("agent_payload BATCH_META lock poisoned, cannot resolve: {}", e);
            return;
        }
    };
    let Some(meta) = meta else {
        error!(
            "high_tier scheduler had a pending agent batch but BATCH_META was empty \
             — payload metadata lost, cannot buffer any unresolved tail"
        );
        return;
    };

    match outcome.result {
        ResolveResult::Sent => {
            debug!("APM mode (high): agent send for {} completed successfully", meta.request_id);
        }
        ResolveResult::Failed | ResolveResult::TimedOut => {
            buffer_unresolved_tail(&meta.payloads, outcome.cursor.unwrap_or(0), &meta.request_id, &meta.invoked_function_arn);
        }
        ResolveResult::NothingPending => unreachable!("checked above"),
    }
}

/// Buffer `payloads[resolved_cursor..]` into `FAILED_AGENT_PAYLOADS` — a
/// no-op when `resolved_cursor >= payloads.len()` (the `Failed` case: the
/// task ran to completion and every payload already buffered its own HTTP
/// failure via `send_agent_payload_or_buffer` — only `TimedOut`, where the
/// task was aborted mid-batch, ever has a genuinely unresolved tail here).
fn buffer_unresolved_tail(payloads: &[Arc<Vec<u8>>], resolved_cursor: usize, request_id: &str, invoked_function_arn: &str) {
    let unresolved = payloads.get(resolved_cursor..).unwrap_or(&[]);
    if unresolved.is_empty() {
        return;
    }
    warn!(
        "APM mode (high): agent send for {} did not finish in time — buffering {} of {} payload(s) for retry (resolved {})",
        request_id,
        unresolved.len(),
        payloads.len(),
        resolved_cursor
    );
    for payload_bytes in unresolved {
        crate::event_loop::buffer_failed_agent_payload(payload_bytes.as_slice(), request_id, invoked_function_arn);
    }
}
