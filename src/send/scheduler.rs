// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `SendScheduler`: the shared primitive both send tiers run through, and
//! the real integration point for the agent-payload Flow-1 batch (wired in
//! `event_loop.rs`'s `process_apm_request`, via `AGENT_SEND_SCHEDULER`).
//!
//! - `SendMode::Sync` (`default` tier): spawn immediately, then
//!   bounded-wait on it in the *current* invocation, before returning —
//!   today's behavior, generalized.
//! - `SendMode::Deferred` (`high` tier): spawn and return immediately,
//!   without waiting at all. The caller calls [`SendScheduler::resolve_pending`]
//!   for that kind at the **start of the next invocation**, before
//!   dispatching anything new for the same kind — that call either finds
//!   the prior send already finished (common case, zero added latency) or
//!   bounded-waits on it, aborting and reporting [`DispatchResult::TimedOut`]
//!   if it's still not done.
//!
//! **Batch/cursor support.** A dispatched send may be a multi-item batch
//! (e.g. the agent-payload Flow-1 send: a `Vec` of payloads, each buffering
//! its own per-item HTTP failure internally — see
//! `event_loop::send_agent_payload_or_buffer`). For those, the caller
//! passes a shared `Arc<AtomicUsize>` progress cursor alongside the future;
//! [`DispatchOutcome::cursor`] reports its value at the moment the result
//! was determined, so the caller can re-buffer only the *unresolved tail*
//! (`items[cursor..]`) on a timeout/abort, without this module needing to
//! know anything about what a "payload" is. Single-item callers (logs,
//! metric API) simply pass `None`.
//!
//! **Deadline clamp is the caller's responsibility.** [`SendTimeoutProvider`]
//! only knows about measured endpoint latency — it has no visibility into
//! the current invocation's `deadlineMs`. Callers must clamp
//! `timeout_provider.current_timeout(kind)` against the remaining time to
//! deadline before relying on it, the same way `wait_for_runtime_done_with_grace`
//! already clamps its own wait against `deadlineMs` (`event_loop.rs`).

#![allow(dead_code)]

use super::{BoxedSend, CircuitBreaker, SendKind, SendOutcome, SendTimeoutProvider};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// Which tier's behavior the scheduler runs. Both modes share every other
/// piece (breaker, timeout provider, buffering decision) — this is the only
/// thing that changes control flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendMode {
    Sync,
    Deferred,
}

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
    /// The breaker was open for this kind — the send was never attempted.
    /// Deliberately not buffered, per the team's "drop data rather than
    /// delay invokes" decision.
    Dropped,
    /// `Deferred` mode only: the send was spawned and nothing is being
    /// waited on right now. Resolution happens later via
    /// [`SendScheduler::resolve_pending`].
    Deferred,
}

/// A [`DispatchResult`] plus, for batch sends, the progress cursor's value
/// at the moment that result was determined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchOutcome {
    pub result: DispatchResult,
    /// `None` for single-item sends that never passed a cursor in, or for
    /// `Dropped`/`Deferred` (nothing ran long enough to make progress
    /// meaningful). `Some(n)` for batch sends: items before `n` were
    /// already resolved (sent or self-buffered) by the task itself;
    /// `items[n..]` is the caller's responsibility on `TimedOut`/`Failed`.
    pub cursor: Option<usize>,
}

struct PendingEntry {
    handle: JoinHandle<SendOutcome>,
    cursor: Option<Arc<AtomicUsize>>,
}

/// The shared scheduler. One instance covers all three [`SendKind`]s via
/// the `pending` map — this is "one per send kind" at the data level
/// without needing three separate struct instances.
pub struct SendScheduler {
    mode: SendMode,
    breaker: Arc<dyn CircuitBreaker>,
    timeout_provider: Arc<dyn SendTimeoutProvider>,
    pending: Mutex<HashMap<SendKind, PendingEntry>>,
}

impl SendScheduler {
    pub fn new(
        mode: SendMode,
        breaker: Arc<dyn CircuitBreaker>,
        timeout_provider: Arc<dyn SendTimeoutProvider>,
    ) -> Self {
        Self {
            mode,
            breaker,
            timeout_provider,
            pending: Mutex::new(HashMap::new()),
        }
    }

    pub fn mode(&self) -> SendMode {
        self.mode
    }

    /// Dispatch `send` for `kind` according to the configured mode. The
    /// breaker is checked first, before anything is spawned, so a dropped
    /// send never even starts. `cursor`: pass `Some(..)` for a batch send
    /// whose future advances it as items resolve; `None` for a single-item
    /// send.
    pub async fn dispatch(
        &self,
        kind: SendKind,
        send: BoxedSend,
        cursor: Option<Arc<AtomicUsize>>,
    ) -> DispatchOutcome {
        if !self.breaker.allow_send(kind) {
            return DispatchOutcome {
                result: DispatchResult::Dropped,
                cursor: None,
            };
        }

        match self.mode {
            SendMode::Sync => self.send_now(kind, send, cursor).await,
            SendMode::Deferred => {
                self.spawn(kind, send, cursor).await;
                DispatchOutcome {
                    result: DispatchResult::Deferred,
                    cursor: None,
                }
            }
        }
    }

    /// `Sync`-mode path: spawn, then bounded-wait on it in this call before
    /// returning. On timeout, abort the task (local cancel — this does not
    /// cancel anything server-side, it just stops us waiting on it) and
    /// report `TimedOut` so the caller buffers the unresolved payload(s).
    async fn send_now(
        &self,
        kind: SendKind,
        send: BoxedSend,
        cursor: Option<Arc<AtomicUsize>>,
    ) -> DispatchOutcome {
        let timeout = self.timeout_provider.current_timeout(kind);
        let mut handle = tokio::spawn(send);
        tokio::select! {
            res = &mut handle => self.finish(kind, res, cursor.as_deref()),
            () = tokio::time::sleep(timeout) => {
                handle.abort();
                self.breaker.record_result(kind, SendOutcome::Failure);
                DispatchOutcome {
                    result: DispatchResult::TimedOut,
                    cursor: cursor.as_deref().map(|c| c.load(Ordering::SeqCst)),
                }
            }
        }
    }

    /// Spawn and return immediately, storing the handle (and cursor, if
    /// any) for [`SendScheduler::resolve_pending`] to pick up later. Used
    /// directly by callers that need explicit two-phase control (spawn now,
    /// resolve at a time of their own choosing — e.g. `default` tier
    /// spawning the moment a payload is ready but resolving at *this*
    /// invocation's boundary, vs. `high` tier resolving at the *next*
    /// invocation's start) rather than going through `dispatch`'s
    /// mode-coupled timing.
    ///
    /// If a handle is already stored for this `kind` (should not happen if
    /// callers always call `resolve_pending` before spawning the next send
    /// of the same kind — see module docs), abort the orphaned handle
    /// defensively rather than silently leaking it.
    pub async fn spawn(&self, kind: SendKind, send: BoxedSend, cursor: Option<Arc<AtomicUsize>>) {
        let handle = tokio::spawn(send);
        let mut guard = self.pending.lock().await;
        if let Some(orphaned) = guard.insert(kind, PendingEntry { handle, cursor }) {
            tracing::warn!(
                "SendScheduler: overwriting an unresolved pending send for {:?} — \
                 resolve_pending should have been called first; aborting the orphaned task",
                kind
            );
            orphaned.handle.abort();
        }
    }

    /// Resolve whatever is currently pending for `kind` — either at the
    /// start of the next invocation (`Deferred` mode) or explicitly, right
    /// after spawning, at a caller-chosen point in the same invocation
    /// (`Sync`-mode callers using `spawn`+`resolve_pending` directly instead
    /// of `dispatch`). Returns `None` if nothing was pending — this is also
    /// exactly what happens on the very first invocation (the map starts
    /// empty), so first-invocation behavior falls out of this without any
    /// special-casing.
    pub async fn resolve_pending(&self, kind: SendKind) -> Option<DispatchOutcome> {
        let entry = {
            let mut guard = self.pending.lock().await;
            guard.remove(&kind)
        }?;
        let PendingEntry { handle, cursor } = entry;

        if handle.is_finished() {
            return Some(self.finish(kind, handle.await, cursor.as_deref()));
        }

        let timeout = self.timeout_provider.current_timeout(kind);
        let mut handle = handle;
        let result = tokio::select! {
            res = &mut handle => self.finish(kind, res, cursor.as_deref()),
            () = tokio::time::sleep(timeout) => {
                handle.abort();
                self.breaker.record_result(kind, SendOutcome::Failure);
                DispatchOutcome {
                    result: DispatchResult::TimedOut,
                    cursor: cursor.as_deref().map(|c| c.load(Ordering::SeqCst)),
                }
            }
        };
        Some(result)
    }

    /// Bounded drain of every still-pending kind — for `SHUTDOWN`, where
    /// there is no "next invocation" to resolve against otherwise. Each
    /// kind is resolved with the same bounded wait as `resolve_pending`;
    /// callers should still run this inside their own overall SHUTDOWN
    /// timeout budget.
    pub async fn drain_all(&self) -> Vec<(SendKind, DispatchOutcome)> {
        let kinds: Vec<SendKind> = {
            let guard = self.pending.lock().await;
            guard.keys().copied().collect()
        };

        let mut results = Vec::with_capacity(kinds.len());
        for kind in kinds {
            if let Some(result) = self.resolve_pending(kind).await {
                results.push((kind, result));
            }
        }
        results
    }

    fn finish(
        &self,
        kind: SendKind,
        res: Result<SendOutcome, tokio::task::JoinError>,
        cursor: Option<&AtomicUsize>,
    ) -> DispatchOutcome {
        let cursor_value = cursor.map(|c| c.load(Ordering::SeqCst));
        match res {
            Ok(outcome) => {
                self.breaker.record_result(kind, outcome);
                let result = match outcome {
                    SendOutcome::Success => DispatchResult::Sent,
                    SendOutcome::Failure => DispatchResult::Failed,
                };
                DispatchOutcome { result, cursor: cursor_value }
            }
            Err(_join_err) => {
                // Task panicked or was otherwise cancelled unexpectedly.
                self.breaker.record_result(kind, SendOutcome::Failure);
                DispatchOutcome {
                    result: DispatchResult::Failed,
                    cursor: cursor_value,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::send::circuit_breaker::BreakerState;
    use crate::send::FixedTimeoutProvider;
    use std::sync::atomic::{AtomicBool, AtomicUsize as StdAtomicUsize, Ordering as StdOrdering};
    use std::time::Duration;

    /// Test breaker that can be toggled closed/open and records every
    /// `allow_send`/`record_result` call for assertions.
    #[derive(Default)]
    struct ToggleBreaker {
        open: AtomicBool,
        allow_calls: StdAtomicUsize,
        record_calls: StdAtomicUsize,
        last_outcome: Mutex<Option<SendOutcome>>,
    }

    impl ToggleBreaker {
        fn open() -> Arc<Self> {
            let b = Self::default();
            b.open.store(true, StdOrdering::SeqCst);
            Arc::new(b)
        }
        fn closed() -> Arc<Self> {
            Arc::new(Self::default())
        }
    }

    impl CircuitBreaker for ToggleBreaker {
        fn allow_send(&self, _kind: SendKind) -> bool {
            self.allow_calls.fetch_add(1, StdOrdering::SeqCst);
            !self.open.load(StdOrdering::SeqCst)
        }
        fn record_result(&self, _kind: SendKind, outcome: SendOutcome) {
            self.record_calls.fetch_add(1, StdOrdering::SeqCst);
            *self.last_outcome.try_lock().unwrap() = Some(outcome);
        }
        fn state(&self, _kind: SendKind) -> BreakerState {
            if self.open.load(StdOrdering::SeqCst) {
                BreakerState::Open
            } else {
                BreakerState::Closed
            }
        }
    }

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
    async fn sync_mode_fast_success_returns_sent_with_zero_added_wait() {
        let scheduler = SendScheduler::new(
            SendMode::Sync,
            ToggleBreaker::closed(),
            Arc::new(FixedTimeoutProvider(Duration::from_millis(100))),
        );

        let start = tokio::time::Instant::now();
        let outcome = scheduler
            .dispatch(SendKind::Logs, boxed(SendOutcome::Success), None)
            .await;
        assert_eq!(outcome.result, DispatchResult::Sent);
        assert_eq!(outcome.cursor, None);
        assert!(
            start.elapsed() < Duration::from_millis(50),
            "healthy sync send should not wait anywhere near the configured bound"
        );
    }

    #[tokio::test]
    async fn sync_mode_slow_send_times_out_and_is_bounded() {
        let scheduler = SendScheduler::new(
            SendMode::Sync,
            ToggleBreaker::closed(),
            Arc::new(FixedTimeoutProvider(Duration::from_millis(50))),
        );

        let start = tokio::time::Instant::now();
        let outcome = scheduler.dispatch(SendKind::Logs, boxed_never(), None).await;
        let elapsed = start.elapsed();

        assert_eq!(outcome.result, DispatchResult::TimedOut);
        assert!(
            elapsed >= Duration::from_millis(50) && elapsed < Duration::from_millis(500),
            "wait should be bounded to roughly the configured timeout, got {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn breaker_open_drops_without_spawning() {
        let scheduler = SendScheduler::new(
            SendMode::Sync,
            ToggleBreaker::open(),
            Arc::new(FixedTimeoutProvider(Duration::from_millis(100))),
        );

        let outcome = scheduler
            .dispatch(SendKind::ApmCollector, boxed(SendOutcome::Success), None)
            .await;
        assert_eq!(outcome.result, DispatchResult::Dropped);
    }

    #[tokio::test]
    async fn deferred_mode_dispatch_returns_immediately() {
        let scheduler = SendScheduler::new(
            SendMode::Deferred,
            ToggleBreaker::closed(),
            Arc::new(FixedTimeoutProvider(Duration::from_millis(100))),
        );

        let start = tokio::time::Instant::now();
        let outcome = scheduler
            .dispatch(
                SendKind::MetricApi,
                boxed_delayed(SendOutcome::Success, Duration::from_millis(500)),
                None,
            )
            .await;
        assert_eq!(outcome.result, DispatchResult::Deferred);
        assert!(
            start.elapsed() < Duration::from_millis(50),
            "deferred dispatch must not wait on the send at all"
        );
    }

    #[tokio::test]
    async fn resolve_pending_returns_none_when_nothing_was_dispatched() {
        // This is also exactly the first-invocation case: nothing to defer
        // from yet, map is empty by construction.
        let scheduler = SendScheduler::new(
            SendMode::Deferred,
            ToggleBreaker::closed(),
            Arc::new(FixedTimeoutProvider(Duration::from_millis(100))),
        );

        assert_eq!(scheduler.resolve_pending(SendKind::Logs).await, None);
    }

    #[tokio::test]
    async fn resolve_pending_finds_already_finished_send_with_zero_added_wait() {
        let scheduler = SendScheduler::new(
            SendMode::Deferred,
            ToggleBreaker::closed(),
            Arc::new(FixedTimeoutProvider(Duration::from_millis(200))),
        );

        scheduler
            .dispatch(SendKind::Logs, boxed(SendOutcome::Success), None)
            .await;
        // Give the spawned task a moment to actually run and finish —
        // simulates it completing well before the "next invocation" check.
        tokio::time::sleep(Duration::from_millis(20)).await;

        let start = tokio::time::Instant::now();
        let outcome = scheduler.resolve_pending(SendKind::Logs).await;
        assert_eq!(outcome.map(|o| o.result), Some(DispatchResult::Sent));
        assert!(
            start.elapsed() < Duration::from_millis(20),
            "an already-finished deferred send must resolve with ~zero added wait"
        );
    }

    #[tokio::test]
    async fn resolve_pending_bounds_wait_on_a_straggler_and_aborts_it() {
        let scheduler = SendScheduler::new(
            SendMode::Deferred,
            ToggleBreaker::closed(),
            Arc::new(FixedTimeoutProvider(Duration::from_millis(50))),
        );

        scheduler.dispatch(SendKind::Logs, boxed_never(), None).await;

        let start = tokio::time::Instant::now();
        let outcome = scheduler.resolve_pending(SendKind::Logs).await;
        let elapsed = start.elapsed();

        assert_eq!(outcome.map(|o| o.result), Some(DispatchResult::TimedOut));
        assert!(
            elapsed >= Duration::from_millis(50) && elapsed < Duration::from_millis(500),
            "the n+1 wait on a straggler must be bounded, got {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn spawning_a_second_deferred_send_without_resolving_aborts_the_orphan() {
        let scheduler = SendScheduler::new(
            SendMode::Deferred,
            ToggleBreaker::closed(),
            Arc::new(FixedTimeoutProvider(Duration::from_millis(100))),
        );

        // First send never finishes — if left running it would prove the
        // defensive-abort path didn't fire.
        scheduler.dispatch(SendKind::Logs, boxed_never(), None).await;
        // Second dispatch for the same kind without an intervening
        // resolve_pending — the defensive path should abort the first one
        // instead of leaking it.
        scheduler
            .dispatch(SendKind::Logs, boxed(SendOutcome::Success), None)
            .await;

        let outcome = scheduler.resolve_pending(SendKind::Logs).await;
        assert_eq!(outcome.map(|o| o.result), Some(DispatchResult::Sent));
    }

    #[tokio::test]
    async fn drain_all_resolves_every_pending_kind() {
        let scheduler = SendScheduler::new(
            SendMode::Deferred,
            ToggleBreaker::closed(),
            Arc::new(FixedTimeoutProvider(Duration::from_millis(100))),
        );

        scheduler
            .dispatch(SendKind::Logs, boxed(SendOutcome::Success), None)
            .await;
        scheduler
            .dispatch(SendKind::ApmCollector, boxed(SendOutcome::Failure), None)
            .await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        let mut results = scheduler.drain_all().await;
        results.sort_by_key(|(kind, _)| format!("{kind:?}"));
        let results: Vec<(SendKind, DispatchResult)> =
            results.into_iter().map(|(k, o)| (k, o.result)).collect();

        assert_eq!(
            results,
            vec![
                (SendKind::ApmCollector, DispatchResult::Failed),
                (SendKind::Logs, DispatchResult::Sent),
            ]
        );
    }

    #[tokio::test]
    async fn breaker_records_result_for_both_success_and_failure() {
        let breaker = ToggleBreaker::closed();
        let scheduler = SendScheduler::new(
            SendMode::Sync,
            breaker.clone(),
            Arc::new(FixedTimeoutProvider(Duration::from_millis(100))),
        );

        scheduler
            .dispatch(SendKind::Logs, boxed(SendOutcome::Failure), None)
            .await;
        assert_eq!(breaker.record_calls.load(StdOrdering::SeqCst), 1);
        assert_eq!(*breaker.last_outcome.lock().await, Some(SendOutcome::Failure));
    }

    #[tokio::test]
    async fn cursor_is_reported_on_timeout_for_a_batch_send() {
        let scheduler = SendScheduler::new(
            SendMode::Sync,
            ToggleBreaker::closed(),
            Arc::new(FixedTimeoutProvider(Duration::from_millis(50))),
        );

        let cursor = Arc::new(AtomicUsize::new(0));
        let cursor_for_task = cursor.clone();
        let send: BoxedSend = Box::pin(async move {
            // Resolves 2 "items" then hangs - simulates a batch that made
            // partial progress before the bound expired.
            cursor_for_task.store(1, Ordering::SeqCst);
            cursor_for_task.store(2, Ordering::SeqCst);
            std::future::pending::<()>().await;
            SendOutcome::Success
        });

        let outcome = scheduler.dispatch(SendKind::ApmCollector, send, Some(cursor)).await;
        assert_eq!(outcome.result, DispatchResult::TimedOut);
        assert_eq!(outcome.cursor, Some(2));
    }

    #[tokio::test]
    async fn cursor_is_reported_on_success_for_a_batch_send() {
        let scheduler = SendScheduler::new(
            SendMode::Sync,
            ToggleBreaker::closed(),
            Arc::new(FixedTimeoutProvider(Duration::from_millis(200))),
        );

        let cursor = Arc::new(AtomicUsize::new(0));
        let cursor_for_task = cursor.clone();
        let send: BoxedSend = Box::pin(async move {
            cursor_for_task.store(3, Ordering::SeqCst);
            SendOutcome::Success
        });

        let outcome = scheduler.dispatch(SendKind::ApmCollector, send, Some(cursor)).await;
        assert_eq!(outcome.result, DispatchResult::Sent);
        assert_eq!(outcome.cursor, Some(3));
    }
}
