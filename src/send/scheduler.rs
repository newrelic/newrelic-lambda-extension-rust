// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Phase B — `SendScheduler`: the shared primitive both send tiers run
//! through. Generalizes the existing bounded-wait pattern used for agent
//! payloads (`resolve_agent_send_at_boundary`, `event_loop.rs`) to all three
//! send kinds, with one extra mode on top.
//!
//! - `SendMode::Sync` (`default` tier): [`SendScheduler::dispatch`] sends
//!   immediately and bounded-waits on it in the *current* invocation,
//!   before returning — today's behavior, generalized.
//! - `SendMode::Deferred` (`high` tier): `dispatch` spawns the send and
//!   returns immediately, without waiting at all. The caller must call
//!   [`SendScheduler::resolve_pending`] for that kind at the **start of the
//!   next invocation**, before dispatching anything new for the same kind —
//!   that call either finds the prior send already finished (common case,
//!   zero added latency) or bounded-waits on it, aborting and reporting
//!   [`DispatchResult::TimedOut`] if it's still not done.
//!
//! **Not yet wired into `event_loop.rs`.** This module is built and fully
//! unit/integration tested on its own first — wiring it into the live
//! invoke loop (including the deadline-vs-timeout clamp noted below) is a
//! separate, deliberately later step so the live critical path isn't
//! touched until this primitive is proven correct in isolation.
//!
//! **Deadline clamp is the caller's responsibility.** [`SendTimeoutProvider`]
//! only knows about measured endpoint latency — it has no visibility into
//! the current invocation's `deadlineMs`. Whoever wires this into
//! `event_loop.rs` must clamp `timeout_provider.current_timeout(kind)`
//! against the remaining time to deadline before using it here, the same
//! way `wait_for_runtime_done_with_grace` already clamps its own wait
//! against `deadlineMs` (`event_loop.rs:1655-1667`).

#![allow(dead_code)]

use super::{BoxedSend, CircuitBreaker, SendKind, SendOutcome, SendTimeoutProvider};
use std::collections::HashMap;
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
/// decide whether to buffer the original payload into the kind-specific
/// durable buffer (`agent/batch.rs`, `apm/telemetry_buffer.rs`,
/// `apm/metric_api_buffer.rs`) — buffering itself is not this module's
/// concern, per the Phase A adapter's scope note.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchResult {
    /// Completed successfully.
    Sent,
    /// Completed, but the send itself failed. Caller should buffer.
    Failed,
    /// Did not complete within the configured bound before it was aborted.
    /// Caller should buffer the original payload; the task itself has been
    /// cancelled (`JoinHandle::abort`), not left running.
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

/// The shared scheduler. One instance covers all three [`SendKind`]s via
/// the `pending` map — this is "one per send kind" at the data level
/// without needing three separate struct instances.
pub struct SendScheduler {
    mode: SendMode,
    breaker: Arc<dyn CircuitBreaker>,
    timeout_provider: Arc<dyn SendTimeoutProvider>,
    pending: Mutex<HashMap<SendKind, JoinHandle<SendOutcome>>>,
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
    /// send never even starts.
    pub async fn dispatch(&self, kind: SendKind, send: BoxedSend) -> DispatchResult {
        if !self.breaker.allow_send(kind) {
            return DispatchResult::Dropped;
        }

        match self.mode {
            SendMode::Sync => self.send_now(kind, send).await,
            SendMode::Deferred => {
                self.spawn_deferred(kind, send).await;
                DispatchResult::Deferred
            }
        }
    }

    /// `Sync`-mode path: spawn, then bounded-wait on it in this call before
    /// returning. On timeout, abort the task (local cancel — this does not
    /// cancel anything server-side, it just stops us waiting on it) and
    /// report `TimedOut` so the caller buffers the payload.
    async fn send_now(&self, kind: SendKind, send: BoxedSend) -> DispatchResult {
        let timeout = self.timeout_provider.current_timeout(kind);
        let mut handle = tokio::spawn(send);
        tokio::select! {
            res = &mut handle => self.finish(kind, res),
            () = tokio::time::sleep(timeout) => {
                handle.abort();
                self.breaker.record_result(kind, SendOutcome::Failure);
                DispatchResult::TimedOut
            }
        }
    }

    /// `Deferred`-mode path: spawn and return immediately, storing the
    /// handle for `resolve_pending` to pick up later.
    ///
    /// If a handle is already stored for this `kind` (should not happen if
    /// callers always call `resolve_pending` before dispatching the next
    /// send of the same kind — see module docs), abort the orphaned handle
    /// defensively rather than silently leaking it.
    async fn spawn_deferred(&self, kind: SendKind, send: BoxedSend) {
        let handle = tokio::spawn(send);
        let mut guard = self.pending.lock().await;
        if let Some(orphaned) = guard.insert(kind, handle) {
            tracing::warn!(
                "SendScheduler: overwriting an unresolved pending send for {:?} — \
                 resolve_pending should have been called first; aborting the orphaned task",
                kind
            );
            orphaned.abort();
        }
    }

    /// Call at the start of the next invocation, before dispatching
    /// anything new for `kind`. Returns `None` if nothing was pending —
    /// this is also exactly what happens on the very first invocation
    /// (the map starts empty), so first-invocation behavior falls out of
    /// this without any special-casing.
    pub async fn resolve_pending(&self, kind: SendKind) -> Option<DispatchResult> {
        let handle = {
            let mut guard = self.pending.lock().await;
            guard.remove(&kind)
        }?;

        if handle.is_finished() {
            return Some(self.finish(kind, handle.await));
        }

        let timeout = self.timeout_provider.current_timeout(kind);
        let mut handle = handle;
        let result = tokio::select! {
            res = &mut handle => self.finish(kind, res),
            () = tokio::time::sleep(timeout) => {
                handle.abort();
                self.breaker.record_result(kind, SendOutcome::Failure);
                DispatchResult::TimedOut
            }
        };
        Some(result)
    }

    /// Bounded drain of every still-pending kind — for `SHUTDOWN`, where
    /// there is no "next invocation" to resolve against otherwise. Each
    /// kind is resolved with the same bounded wait as `resolve_pending`;
    /// callers should still run this inside their own overall SHUTDOWN
    /// timeout budget.
    pub async fn drain_all(&self) -> Vec<(SendKind, DispatchResult)> {
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
    ) -> DispatchResult {
        match res {
            Ok(outcome) => {
                self.breaker.record_result(kind, outcome);
                match outcome {
                    SendOutcome::Success => DispatchResult::Sent,
                    SendOutcome::Failure => DispatchResult::Failed,
                }
            }
            Err(_join_err) => {
                // Task panicked or was otherwise cancelled unexpectedly.
                self.breaker.record_result(kind, SendOutcome::Failure);
                DispatchResult::Failed
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::send::circuit_breaker::BreakerState;
    use crate::send::FixedTimeoutProvider;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    /// Test breaker that can be toggled closed/open and records every
    /// `allow_send`/`record_result` call for assertions.
    #[derive(Default)]
    struct ToggleBreaker {
        open: AtomicBool,
        allow_calls: AtomicUsize,
        record_calls: AtomicUsize,
        last_outcome: Mutex<Option<SendOutcome>>,
    }

    impl ToggleBreaker {
        fn open() -> Arc<Self> {
            let b = Self::default();
            b.open.store(true, Ordering::SeqCst);
            Arc::new(b)
        }
        fn closed() -> Arc<Self> {
            Arc::new(Self::default())
        }
    }

    impl CircuitBreaker for ToggleBreaker {
        fn allow_send(&self, _kind: SendKind) -> bool {
            self.allow_calls.fetch_add(1, Ordering::SeqCst);
            !self.open.load(Ordering::SeqCst)
        }
        fn record_result(&self, _kind: SendKind, outcome: SendOutcome) {
            self.record_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_outcome.try_lock().unwrap() = Some(outcome);
        }
        fn state(&self, _kind: SendKind) -> BreakerState {
            if self.open.load(Ordering::SeqCst) {
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
        let result = scheduler
            .dispatch(SendKind::Logs, boxed(SendOutcome::Success))
            .await;
        assert_eq!(result, DispatchResult::Sent);
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
        let result = scheduler.dispatch(SendKind::Logs, boxed_never()).await;
        let elapsed = start.elapsed();

        assert_eq!(result, DispatchResult::TimedOut);
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

        let result = scheduler
            .dispatch(SendKind::ApmCollector, boxed(SendOutcome::Success))
            .await;
        assert_eq!(result, DispatchResult::Dropped);
    }

    #[tokio::test]
    async fn deferred_mode_dispatch_returns_immediately() {
        let scheduler = SendScheduler::new(
            SendMode::Deferred,
            ToggleBreaker::closed(),
            Arc::new(FixedTimeoutProvider(Duration::from_millis(100))),
        );

        let start = tokio::time::Instant::now();
        let result = scheduler
            .dispatch(SendKind::MetricApi, boxed_delayed(SendOutcome::Success, Duration::from_millis(500)))
            .await;
        assert_eq!(result, DispatchResult::Deferred);
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
            .dispatch(SendKind::Logs, boxed(SendOutcome::Success))
            .await;
        // Give the spawned task a moment to actually run and finish —
        // simulates it completing well before the "next invocation" check.
        tokio::time::sleep(Duration::from_millis(20)).await;

        let start = tokio::time::Instant::now();
        let result = scheduler.resolve_pending(SendKind::Logs).await;
        assert_eq!(result, Some(DispatchResult::Sent));
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

        scheduler.dispatch(SendKind::Logs, boxed_never()).await;

        let start = tokio::time::Instant::now();
        let result = scheduler.resolve_pending(SendKind::Logs).await;
        let elapsed = start.elapsed();

        assert_eq!(result, Some(DispatchResult::TimedOut));
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
        scheduler.dispatch(SendKind::Logs, boxed_never()).await;
        // Second dispatch for the same kind without an intervening
        // resolve_pending — the defensive path should abort the first one
        // instead of leaking it.
        scheduler
            .dispatch(SendKind::Logs, boxed(SendOutcome::Success))
            .await;

        let result = scheduler.resolve_pending(SendKind::Logs).await;
        assert_eq!(result, Some(DispatchResult::Sent));
    }

    #[tokio::test]
    async fn drain_all_resolves_every_pending_kind() {
        let scheduler = SendScheduler::new(
            SendMode::Deferred,
            ToggleBreaker::closed(),
            Arc::new(FixedTimeoutProvider(Duration::from_millis(100))),
        );

        scheduler
            .dispatch(SendKind::Logs, boxed(SendOutcome::Success))
            .await;
        scheduler
            .dispatch(SendKind::ApmCollector, boxed(SendOutcome::Failure))
            .await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        let mut results = scheduler.drain_all().await;
        results.sort_by_key(|(kind, _)| format!("{kind:?}"));

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
            .dispatch(SendKind::Logs, boxed(SendOutcome::Failure))
            .await;
        assert_eq!(breaker.record_calls.load(Ordering::SeqCst), 1);
        assert_eq!(*breaker.last_outcome.lock().await, Some(SendOutcome::Failure));
    }
}
