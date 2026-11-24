// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `SendScheduler`: tracks the `high` tier's in-flight sends (the INVOKE-time agent batch and
//! the log flush; `event_loop.rs`'s `SEND_SCHEDULER`). A caller `spawn`s a send, then resolves
//! it later with `resolve_pending_no_abort`, `resolve_pending_shared_budget` or `wait_finished`.
//!
//! **Batch/cursor support.** A spawned send may be a multi-item batch (e.g. the INVOKE-time
//! agent batch, see `event_loop::send_pending_groups`). The caller passes a shared
//! `Arc<AtomicUsize>` progress cursor alongside the future; [`DispatchOutcome::cursor`]
//! reports its value when the result was determined, so the caller can re-buffer only the
//! unresolved tail (`items[cursor..]`) on a timeout, without this module knowing what a
//! "payload" is. Single-item callers (logs) pass `None`.

use super::{BoxedSend, CircuitBreaker, ConsecutiveFailureBreaker, SendKind, SendOutcome, SendPermit};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// What happened to a dispatched (or resolved) send. Callers use this to
/// decide whether to buffer the original payload(s) into the kind-specific
/// durable buffer (`agent/batch.rs`, `apm/telemetry_buffer.rs`,
/// `apm/metric_api_buffer.rs`) — buffering itself is not this module's
/// concern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchResult {
    /// Completed successfully.
    Sent,
    /// Completed, but the send itself failed. Caller should buffer.
    Failed,
    /// Did not complete within the configured bound before it was aborted.
    /// Caller should buffer the unresolved payload(s); the task itself has
    /// been cancelled (`JoinHandle::abort`), not left running.
    TimedOut,
    /// [`SendScheduler::resolve_pending_no_abort`] only: still running, left
    /// alone (not aborted). Nothing to buffer — nothing was lost, it's just
    /// not done yet. The pending entry stays in place for a later call to
    /// check again.
    StillRunning,
}

/// A [`DispatchResult`] plus, for batch sends, the progress cursor's value
/// at the moment that result was determined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchOutcome {
    pub result: DispatchResult,
    /// `None` for single-item sends that never passed a cursor in. `Some(n)` for batch sends: items before `n` were
    /// already resolved (sent or self-buffered) by the task itself;
    /// `items[n..]` is the caller's responsibility on `TimedOut`/`Failed`.
    pub cursor: Option<usize>,
}

struct PendingEntry {
    handle: JoinHandle<SendOutcome>,
    cursor: Option<Arc<AtomicUsize>>,
    permit: SendPermit,
}

/// Cooperative poll interval used by `SendScheduler::resolve_pending_shared_budget`
/// to check whether any still-running send has finished early, without
/// pulling in an external crate for a proper select-over-a-dynamic-set.
const SHARED_BUDGET_POLL_TICK: std::time::Duration = std::time::Duration::from_millis(1);

/// The shared scheduler. One instance covers all three [`SendKind`]s via
/// the `pending` map — this is "one per send kind" at the data level
/// without needing three separate struct instances.
pub struct SendScheduler {
    breaker: Arc<ConsecutiveFailureBreaker>,
    pending: Mutex<HashMap<SendKind, PendingEntry>>,
}

impl SendScheduler {
    pub fn new(breaker: Arc<ConsecutiveFailureBreaker>) -> Self {
        Self {
            breaker,
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Spawn and return immediately, storing the handle (and cursor, if
    /// any) for a later `resolve_pending_*` / `wait_finished`.
    ///
    /// If a handle is already stored for this `kind` (callers should resolve
    /// the previous send first), abort the orphaned handle defensively
    /// rather than silently leaking it.
    ///
    /// **Circuit-breaker gated.** This is the single point where the breaker's
    /// `allow_send` is consulted for the spawn/resolve callers (the
    /// `high` tier), so they need no breaker check of their own. The breaker
    /// itself is the shared `DEFAULT_BREAKER`; default-tier call sites consult
    /// it directly. Returns:
    /// - `true`  → the breaker allowed the send; it was spawned and is now
    ///   tracked for a later `resolve_pending_*`.
    /// - `false` → the breaker is Open for `kind`; **nothing was spawned**. The
    ///   caller must buffer the payload for a later retry (and undo any
    ///   write-ahead/metadata it staged for this send). Counts as one
    ///   Open-window invocation for `kind`.
    pub async fn spawn(&self, kind: SendKind, send: BoxedSend, cursor: Option<Arc<AtomicUsize>>) -> bool {
        let Some(permit) = self.breaker.allow_send(kind) else {
            tracing::debug!(
                "SendScheduler: circuit breaker open for {:?} — send skipped, caller buffers",
                kind
            );
            return false;
        };
        self.spawn_inner(kind, permit, send, cursor).await;
        true
    }

    /// Un-gated spawn body — stores the handle (and cursor) for a later
    /// `resolve_pending_*`. Private; [`SendScheduler::spawn`] consults the breaker first.
    async fn spawn_inner(&self, kind: SendKind, permit: SendPermit, send: BoxedSend, cursor: Option<Arc<AtomicUsize>>) {
        let handle = tokio::spawn(send);
        let mut guard = self.pending.lock().await;
        if let Some(orphaned) = guard.insert(kind, PendingEntry { handle, cursor, permit }) {
            tracing::warn!(
                "SendScheduler: overwriting an unresolved pending send for {:?} — \
                 resolve_pending_* should have been called first; aborting the orphaned task",
                kind
            );
            orphaned.handle.abort();
        }
    }

    /// Like [`SendScheduler::resolve_pending_shared_budget`], but **never aborts** an
    /// unfinished send — some sends (e.g. the log flush, which holds its
    /// chunk list in a purely local variable with no externally visible
    /// progress cursor) aren't safe to abort-and-partially-recover the way
    /// the agent-payload batch is; killing them mid-flight would silently
    /// lose whatever wasn't yet sent, with nothing to buffer it. Use this
    /// for kinds where "leave it running, check again later" is strictly
    /// safer than "kill it and hope the caller can reconstruct what was
    /// lost."
    ///
    /// If still running: the entry stays in the pending map (not removed),
    /// and this returns `StillRunning`. Callers must not spawn a new send
    /// for the same `kind` while that's the case — check
    /// [`SendScheduler::has_pending`] first, or duplicate sends can overlap.
    /// Returns `None` if nothing was pending at all (including the
    /// first-invocation case).
    pub async fn resolve_pending_no_abort(&self, kind: SendKind) -> Option<DispatchOutcome> {
        let mut guard = self.pending.lock().await;
        let entry = guard.get(&kind)?;
        if !entry.handle.is_finished() {
            return Some(DispatchOutcome {
                result: DispatchResult::StillRunning,
                cursor: entry.cursor.as_deref().map(|c| c.load(Ordering::SeqCst)),
            });
        }

        // Finished: take it out of the map and join it for real.
        let PendingEntry { handle, cursor, permit } = guard.remove(&kind).expect("just checked present");
        drop(guard);
        Some(self.finish(kind, permit, handle.await, cursor.as_deref()))
    }

    /// Whether `kind` currently has a pending send (spawned, not yet
    /// resolved) — for callers using [`SendScheduler::resolve_pending_no_abort`]
    /// to decide whether it's safe to spawn a new send for the same kind
    /// without risking two overlapping in flight at once.
    pub async fn has_pending(&self, kind: SendKind) -> bool {
        self.pending.lock().await.contains_key(&kind)
    }

    /// Resolve every kind in `kinds` against **one shared** `budget`, not
    /// `budget` per kind. Used at a single invocation's own `runtimeDone`
    /// boundary, where the requirement is "never block the next `/next` by
    /// more than `budget` total," regardless of how many send kinds are
    /// in flight.
    ///
    /// Kinds already finished are resolved immediately at zero cost. Kinds
    /// still running are polled cooperatively (short internal tick, no
    /// extra dependency) until either all finish or `budget` elapses,
    /// whichever comes first — so a fast send doesn't force everyone else
    /// to wait out the full budget. Anything still unfinished when `budget`
    /// elapses is aborted and reported as [`DispatchResult::TimedOut`];
    /// the caller buffers its unresolved payload(s).
    ///
    /// Kinds with nothing pending are simply absent from the returned map
    /// (including the first-invocation case).
    pub async fn resolve_pending_shared_budget(
        &self,
        kinds: &[SendKind],
        budget: std::time::Duration,
    ) -> HashMap<SendKind, DispatchOutcome> {
        let mut entries: HashMap<SendKind, PendingEntry> = HashMap::new();
        {
            let mut guard = self.pending.lock().await;
            for &kind in kinds {
                if let Some(entry) = guard.remove(&kind) {
                    entries.insert(kind, entry);
                }
            }
        }

        let mut results = HashMap::new();
        let mut running: Vec<(SendKind, PendingEntry)> = Vec::new();
        for (kind, entry) in entries {
            if entry.handle.is_finished() {
                results.insert(kind, self.finish(kind, entry.permit, entry.handle.await, entry.cursor.as_deref()));
            } else {
                running.push((kind, entry));
            }
        }

        let deadline = tokio::time::Instant::now() + budget;
        while !running.is_empty() && tokio::time::Instant::now() < deadline {
            let mut i = 0;
            while i < running.len() {
                if running[i].1.handle.is_finished() {
                    let (kind, entry) = running.remove(i);
                    results.insert(kind, self.finish(kind, entry.permit, entry.handle.await, entry.cursor.as_deref()));
                } else {
                    i += 1;
                }
            }
            if running.is_empty() {
                break;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            tokio::time::sleep(remaining.min(SHARED_BUDGET_POLL_TICK)).await;
        }

        for (kind, entry) in running {
            entry.handle.abort();
            // Cut off by the caller's own limit, not answered by the backend: no result to
            // record, and a probe cut off here is given back.
            self.breaker.release_probe(kind, entry.permit);
            results.insert(
                kind,
                DispatchOutcome {
                    result: DispatchResult::TimedOut,
                    cursor: entry.cursor.as_deref().map(|c| c.load(Ordering::SeqCst)),
                },
            );
        }

        results
    }

    fn finish(
        &self,
        kind: SendKind,
        permit: SendPermit,
        res: Result<SendOutcome, tokio::task::JoinError>,
        cursor: Option<&AtomicUsize>,
    ) -> DispatchOutcome {
        let cursor_value = cursor.map(|c| c.load(Ordering::SeqCst));
        match res {
            Ok(outcome) => {
                self.breaker.record_result(kind, permit, outcome);
                let result = match outcome {
                    SendOutcome::Success => DispatchResult::Sent,
                    SendOutcome::Failure => DispatchResult::Failed,
                };
                DispatchOutcome { result, cursor: cursor_value }
            }
            Err(_join_err) => {
                // Task panicked or was otherwise cancelled unexpectedly.
                self.breaker.record_result(kind, permit, SendOutcome::Failure);
                DispatchOutcome {
                    result: DispatchResult::Failed,
                    cursor: cursor_value,
                }
            }
        }
    }
}

impl SendScheduler {
    /// Wait up to `budget` for each of `kinds` to finish, and hand back only the ones
    /// that did. Unlike `resolve_pending_shared_budget` this never aborts: a send still
    /// running stays registered and keeps going, so the caller can return to Lambda
    /// without killing a request that may already be at the collector (killing it would
    /// make the retry a duplicate). `biased` makes an already-finished send win even
    /// when the budget is spent.
    pub async fn wait_finished(
        &self,
        kinds: &[SendKind],
        budget: std::time::Duration,
    ) -> HashMap<SendKind, DispatchOutcome> {
        let deadline = tokio::time::Instant::now() + budget;
        let mut finished = HashMap::new();
        for &kind in kinds {
            let entry = self.pending.lock().await.remove(&kind);
            let Some(PendingEntry { mut handle, cursor, permit }) = entry else { continue };
            let result = tokio::select! {
                biased;
                res = &mut handle => Some(res),
                () = tokio::time::sleep_until(deadline) => None,
            };
            match result {
                Some(res) => {
                    finished.insert(kind, self.finish(kind, permit, res, cursor.as_deref()));
                }
                None => {
                    self.pending.lock().await.insert(kind, PendingEntry { handle, cursor, permit });
                }
            }
        }
        finished
    }

    /// Stop tracking `kind` without aborting it. For a send that outlived its useful
    /// life: its owner still finishes (and buffers its own failures), but a new send of
    /// the same kind may start. Returns whether anything was detached.
    pub async fn detach(&self, kind: SendKind) -> bool {
        let detached = self.pending.lock().await.remove(&kind);
        if let Some(entry) = &detached {
            // No result will be recorded for it; a probe that was in flight is given back.
            self.breaker.release_probe(kind, entry.permit);
        }
        detached.is_some()
    }

    /// Whether the breaker for `kind` is closed, i.e. sends are normal. Does not count an invocation.
    pub fn breaker_closed(&self, kind: SendKind) -> bool {
        self.breaker.state(kind) == crate::send::circuit_breaker::BreakerState::Closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::send::ConsecutiveFailureBreaker;
    use std::time::Duration;

    fn boxed(outcome: SendOutcome) -> BoxedSend {
        Box::pin(async move { outcome })
    }

    fn boxed_delayed(outcome: SendOutcome, delay: Duration) -> BoxedSend {
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            outcome
        })
    }

    fn boxed_never() -> BoxedSend {
        Box::pin(std::future::pending::<SendOutcome>())
    }

    #[tokio::test]
    async fn resolve_pending_no_abort_returns_none_when_nothing_spawned() {
        let scheduler = SendScheduler::new(Arc::new(ConsecutiveFailureBreaker::new(5)));
        assert_eq!(scheduler.resolve_pending_no_abort(SendKind::Logs).await, None);
    }

    #[tokio::test]
    async fn resolve_pending_no_abort_resolves_an_already_finished_send() {
        let scheduler = SendScheduler::new(Arc::new(ConsecutiveFailureBreaker::new(5)));
        scheduler.spawn(SendKind::Logs, boxed(SendOutcome::Success), None).await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        let outcome = scheduler.resolve_pending_no_abort(SendKind::Logs).await;
        assert_eq!(outcome.map(|o| o.result), Some(DispatchResult::Sent));
        // And it's gone from the pending map now - a second call finds nothing.
        assert!(!scheduler.has_pending(SendKind::Logs).await);
    }

    #[tokio::test]
    async fn resolve_pending_no_abort_never_kills_a_still_running_send() {
        let scheduler = SendScheduler::new(Arc::new(ConsecutiveFailureBreaker::new(5)));

        let still_running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let still_running_for_task = still_running.clone();
        let send: BoxedSend = Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            still_running_for_task.store(false, Ordering::SeqCst);
            SendOutcome::Success
        });
        scheduler.spawn(SendKind::Logs, send, None).await;

        let outcome = scheduler.resolve_pending_no_abort(SendKind::Logs).await;
        assert_eq!(outcome.map(|o| o.result), Some(DispatchResult::StillRunning));
        // Proof it wasn't aborted: the task is still actually running.
        assert!(
            still_running.load(Ordering::SeqCst),
            "the send must still be running - resolve_pending_no_abort must never abort it"
        );
        // And it's still tracked, so a later call can check it again.
        assert!(scheduler.has_pending(SendKind::Logs).await);

        // Let it actually finish and clean up, so the test doesn't leak a
        // detached task past its own scope.
        tokio::time::sleep(Duration::from_millis(250)).await;
        let final_outcome = scheduler.resolve_pending_no_abort(SendKind::Logs).await;
        assert_eq!(final_outcome.map(|o| o.result), Some(DispatchResult::Sent));
    }

    #[tokio::test]
    async fn has_pending_reflects_spawn_and_resolve() {
        let scheduler = SendScheduler::new(Arc::new(ConsecutiveFailureBreaker::new(5)));
        assert!(!scheduler.has_pending(SendKind::Logs).await);

        scheduler.spawn(SendKind::Logs, boxed(SendOutcome::Success), None).await;
        assert!(scheduler.has_pending(SendKind::Logs).await);

        tokio::time::sleep(Duration::from_millis(10)).await;
        scheduler.resolve_pending_no_abort(SendKind::Logs).await;
        assert!(!scheduler.has_pending(SendKind::Logs).await);
    }

    #[tokio::test]
    async fn shared_budget_resolves_all_fast_kinds_without_waiting_out_the_full_budget() {
        let scheduler = SendScheduler::new(Arc::new(ConsecutiveFailureBreaker::new(5)));
        scheduler.spawn(SendKind::ApmCollector, boxed(SendOutcome::Success), None).await;
        scheduler.spawn(SendKind::Logs, boxed(SendOutcome::Success), None).await;
        scheduler.spawn(SendKind::MetricApi, boxed(SendOutcome::Failure), None).await;

        let start = tokio::time::Instant::now();
        let outcomes = scheduler
            .resolve_pending_shared_budget(
                &[SendKind::ApmCollector, SendKind::Logs, SendKind::MetricApi],
                Duration::from_millis(100),
            )
            .await;
        let elapsed = start.elapsed();

        assert_eq!(outcomes.len(), 3);
        assert_eq!(outcomes[&SendKind::ApmCollector].result, DispatchResult::Sent);
        assert_eq!(outcomes[&SendKind::Logs].result, DispatchResult::Sent);
        assert_eq!(outcomes[&SendKind::MetricApi].result, DispatchResult::Failed);
        assert!(
            elapsed < Duration::from_millis(80),
            "already-finished/fast sends must not pay the full shared budget, took {:?}",
            elapsed
        );
        assert!(!scheduler.has_pending(SendKind::ApmCollector).await);
        assert!(!scheduler.has_pending(SendKind::Logs).await);
        assert!(!scheduler.has_pending(SendKind::MetricApi).await);
    }

    #[tokio::test]
    async fn shared_budget_is_combined_not_per_kind() {
        let scheduler = SendScheduler::new(Arc::new(ConsecutiveFailureBreaker::new(5)));
        // All three never finish on their own -- forces every kind to ride
        // out to the shared deadline together.
        scheduler.spawn(SendKind::ApmCollector, boxed_never(), None).await;
        scheduler.spawn(SendKind::Logs, boxed_never(), None).await;
        scheduler.spawn(SendKind::MetricApi, boxed_never(), None).await;

        let start = tokio::time::Instant::now();
        let outcomes = scheduler
            .resolve_pending_shared_budget(
                &[SendKind::ApmCollector, SendKind::Logs, SendKind::MetricApi],
                Duration::from_millis(60),
            )
            .await;
        let elapsed = start.elapsed();

        assert_eq!(outcomes.len(), 3);
        for kind in [SendKind::ApmCollector, SendKind::Logs, SendKind::MetricApi] {
            assert_eq!(outcomes[&kind].result, DispatchResult::TimedOut);
        }
        // The combined wait must land near ONE budget (~60ms), not
        // kinds.len() budgets (~180ms) -- the whole point of this method.
        assert!(
            elapsed < Duration::from_millis(150),
            "three still-pending kinds must share one budget, not pay it per kind, took {:?}",
            elapsed
        );
        assert!(!scheduler.has_pending(SendKind::ApmCollector).await);
        assert!(!scheduler.has_pending(SendKind::Logs).await);
        assert!(!scheduler.has_pending(SendKind::MetricApi).await);
    }

    #[tokio::test]
    async fn shared_budget_mix_of_fast_and_slow_waits_only_for_the_slow_one() {
        let scheduler = SendScheduler::new(Arc::new(ConsecutiveFailureBreaker::new(5)));
        scheduler.spawn(SendKind::ApmCollector, boxed(SendOutcome::Success), None).await;
        scheduler
            .spawn(
                SendKind::Logs,
                boxed_delayed(SendOutcome::Success, Duration::from_millis(20)),
                None,
            )
            .await;

        let start = tokio::time::Instant::now();
        let outcomes = scheduler
            .resolve_pending_shared_budget(
                &[SendKind::ApmCollector, SendKind::Logs],
                Duration::from_millis(100),
            )
            .await;
        let elapsed = start.elapsed();

        assert_eq!(outcomes[&SendKind::ApmCollector].result, DispatchResult::Sent);
        assert_eq!(outcomes[&SendKind::Logs].result, DispatchResult::Sent);
        assert!(
            elapsed < Duration::from_millis(80),
            "must return once the slower (but still-under-budget) send finishes, not wait out the full budget, took {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn shared_budget_returns_empty_map_when_nothing_pending() {
        let scheduler = SendScheduler::new(Arc::new(ConsecutiveFailureBreaker::new(5)));
        let outcomes = scheduler
            .resolve_pending_shared_budget(
                &[SendKind::ApmCollector, SendKind::Logs, SendKind::MetricApi],
                Duration::from_millis(100),
            )
            .await;
        assert!(outcomes.is_empty());
    }

    #[tokio::test]
    async fn wait_finished_returns_a_finished_send_and_clears_it() {
        let scheduler = SendScheduler::new(Arc::new(ConsecutiveFailureBreaker::new(5)));
        scheduler.spawn(SendKind::ApmCollector, boxed(SendOutcome::Success), None).await;

        let done = scheduler
            .wait_finished(&[SendKind::ApmCollector], Duration::from_millis(200))
            .await;

        assert_eq!(done.get(&SendKind::ApmCollector).map(|o| o.result), Some(DispatchResult::Sent));
        assert!(!scheduler.has_pending(SendKind::ApmCollector).await);
    }

    #[tokio::test]
    async fn wait_finished_leaves_a_slow_send_running_instead_of_aborting_it() {
        let scheduler = SendScheduler::new(Arc::new(ConsecutiveFailureBreaker::new(5)));
        scheduler
            .spawn(SendKind::ApmCollector, boxed_delayed(SendOutcome::Success, Duration::from_millis(300)), None)
            .await;

        let start = tokio::time::Instant::now();
        let done = scheduler
            .wait_finished(&[SendKind::ApmCollector], Duration::from_millis(50))
            .await;

        assert!(done.is_empty(), "an unfinished send must not be reported");
        assert!(start.elapsed() < Duration::from_millis(200), "must give up at the budget");
        assert!(scheduler.has_pending(SendKind::ApmCollector).await, "the send stays registered");

        // It was not aborted: it still completes and can be collected later.
        let later = scheduler
            .wait_finished(&[SendKind::ApmCollector], Duration::from_millis(500))
            .await;
        assert_eq!(later.get(&SendKind::ApmCollector).map(|o| o.result), Some(DispatchResult::Sent));
    }

    #[tokio::test]
    async fn wait_finished_shares_one_budget_across_kinds() {
        let scheduler = SendScheduler::new(Arc::new(ConsecutiveFailureBreaker::new(5)));
        scheduler.spawn(SendKind::ApmCollector, boxed_never(), None).await;
        scheduler.spawn(SendKind::Logs, boxed_never(), None).await;

        let start = tokio::time::Instant::now();
        let _ = scheduler
            .wait_finished(&[SendKind::ApmCollector, SendKind::Logs], Duration::from_millis(80))
            .await;

        assert!(
            start.elapsed() < Duration::from_millis(160),
            "two kinds must share one budget, took {:?}",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn detach_frees_the_slot_without_aborting() {
        let scheduler = SendScheduler::new(Arc::new(ConsecutiveFailureBreaker::new(5)));
        scheduler.spawn(SendKind::ApmCollector, boxed_never(), None).await;

        assert!(scheduler.detach(SendKind::ApmCollector).await);
        assert!(!scheduler.has_pending(SendKind::ApmCollector).await);
        assert!(!scheduler.detach(SendKind::ApmCollector).await, "nothing left to detach");
    }
}

#[cfg(test)]
#[path = "scheduler_tests.rs"]
mod scheduler_tests;
