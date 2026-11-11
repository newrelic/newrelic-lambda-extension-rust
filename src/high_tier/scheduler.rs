// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A minimal, self-contained scheduling primitive for exactly the two send
//! kinds `high_tier` ever bounds a wait on: agent payload and
//! platform-metrics (logs are deliberately never bounded here — see the
//! `high_tier` module doc and `logs` submodule).
//!
//! Not a generalized "N named kinds" abstraction — two fixed, named slots.
//! Building a generic version for exactly two call sites would be the
//! premature abstraction this codebase's own style guidance warns against.
//!
//! One [`Scheduler`] instance lives for the whole process ([`super::SCHEDULER`]):
//! a shared budget across invocations wouldn't make sense per-invocation,
//! since what's "pending" at the top of invocation N is whatever invocation
//! N-1 spawned and didn't finish resolving.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::warn;

pub(crate) type BoxedSend = std::pin::Pin<Box<dyn std::future::Future<Output = SendOutcome> + Send>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendOutcome {
    Success,
    Failure,
}

/// What happened to a resolved send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResolveResult {
    /// Nothing was pending — including the common case where the previous
    /// invocation's own send already finished before this resolve ran.
    NothingPending,
    Sent,
    Failed,
    /// Did not finish within the shared budget — aborted. Caller buffers
    /// the unresolved tail (if any, via `cursor`) for a background retry.
    TimedOut,
}

/// A [`ResolveResult`] plus, for the batch agent-payload send, the progress
/// cursor's value at the moment the result was determined — items before
/// the cursor were already resolved (sent or self-buffered) by the task
/// itself; items at/after it are the caller's responsibility on `TimedOut`.
/// Always `None` for platform-metrics (single-item, no cursor).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Outcome {
    pub(crate) result: ResolveResult,
    pub(crate) cursor: Option<usize>,
}

#[derive(Debug)]
struct Pending {
    handle: JoinHandle<SendOutcome>,
    cursor: Option<Arc<AtomicUsize>>,
}

#[derive(Debug)]
pub(crate) struct Scheduler {
    agent_payload: Mutex<Option<Pending>>,
    platform_metrics: Mutex<Option<Pending>>,
}

fn finish(res: Result<SendOutcome, tokio::task::JoinError>, cursor: Option<&AtomicUsize>) -> Outcome {
    let cursor_value = cursor.map(|c| c.load(Ordering::SeqCst));
    match res {
        Ok(SendOutcome::Success) => Outcome { result: ResolveResult::Sent, cursor: cursor_value },
        Ok(SendOutcome::Failure) => Outcome { result: ResolveResult::Failed, cursor: cursor_value },
        Err(join_err) => {
            warn!("high_tier::scheduler: send task panicked or was cancelled: {}", join_err);
            Outcome { result: ResolveResult::Failed, cursor: cursor_value }
        }
    }
}

const POLL_TICK: Duration = Duration::from_millis(1);

impl Scheduler {
    pub(crate) fn new() -> Self {
        Self {
            agent_payload: Mutex::new(None),
            platform_metrics: Mutex::new(None),
        }
    }

    pub(crate) async fn has_agent_payload_pending(&self) -> bool {
        self.agent_payload.lock().await.is_some()
    }

    pub(crate) async fn has_platform_metrics_pending(&self) -> bool {
        self.platform_metrics.lock().await.is_some()
    }

    /// Spawn `send` for the agent-payload batch. If an unresolved entry is
    /// already there (should not happen if callers always resolve before
    /// spawning again for the same kind — see `has_agent_payload_pending`),
    /// the orphan is aborted defensively rather than silently leaked.
    pub(crate) async fn spawn_agent_payload(&self, send: BoxedSend, cursor: Option<Arc<AtomicUsize>>) {
        let handle = tokio::spawn(send);
        let mut guard = self.agent_payload.lock().await;
        if let Some(orphan) = guard.replace(Pending { handle, cursor }) {
            warn!("high_tier::scheduler: overwriting an unresolved agent-payload entry — aborting the orphan");
            orphan.handle.abort();
        }
    }

    /// Spawn `send` for platform-metrics. Same orphan-abort defensiveness
    /// as [`Scheduler::spawn_agent_payload`].
    pub(crate) async fn spawn_platform_metrics(&self, send: BoxedSend) {
        let handle = tokio::spawn(send);
        let mut guard = self.platform_metrics.lock().await;
        if let Some(orphan) = guard.replace(Pending { handle, cursor: None }) {
            warn!("high_tier::scheduler: overwriting an unresolved platform-metrics entry — aborting the orphan");
            orphan.handle.abort();
        }
    }

    /// Resolve both kinds against **one shared** `budget`, not `budget`
    /// each. Already-finished entries resolve immediately at zero cost;
    /// still-running entries are polled cooperatively until either both
    /// finish or `budget` elapses, whichever comes first — a fast send
    /// doesn't force the other to wait out the full budget. Anything still
    /// unfinished when `budget` elapses is aborted and reported `TimedOut`.
    pub(crate) async fn resolve_both_within(&self, budget: Duration) -> (Outcome, Outcome) {
        let agent_entry = self.agent_payload.lock().await.take();
        let metrics_entry = self.platform_metrics.lock().await.take();

        let mut agent_result = Outcome { result: ResolveResult::NothingPending, cursor: None };
        let mut metrics_result = Outcome { result: ResolveResult::NothingPending, cursor: None };
        let mut agent_running = None;
        let mut metrics_running = None;

        if let Some(Pending { handle, cursor }) = agent_entry {
            if handle.is_finished() {
                agent_result = finish(handle.await, cursor.as_deref());
            } else {
                agent_running = Some((handle, cursor));
            }
        }
        if let Some(Pending { handle, cursor }) = metrics_entry {
            if handle.is_finished() {
                metrics_result = finish(handle.await, cursor.as_deref());
            } else {
                metrics_running = Some((handle, cursor));
            }
        }

        if agent_running.is_none() && metrics_running.is_none() {
            return (agent_result, metrics_result);
        }

        let deadline = tokio::time::Instant::now() + budget;
        while (agent_running.is_some() || metrics_running.is_some())
            && tokio::time::Instant::now() < deadline
        {
            if agent_running.as_ref().is_some_and(|(h, _)| h.is_finished()) {
                let (handle, cursor) = agent_running.take().expect("just checked present");
                agent_result = finish(handle.await, cursor.as_deref());
            }
            if metrics_running.as_ref().is_some_and(|(h, _)| h.is_finished()) {
                let (handle, cursor) = metrics_running.take().expect("just checked present");
                metrics_result = finish(handle.await, cursor.as_deref());
            }
            if agent_running.is_none() && metrics_running.is_none() {
                break;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            tokio::time::sleep(remaining.min(POLL_TICK)).await;
        }

        if let Some((handle, cursor)) = agent_running {
            handle.abort();
            agent_result = Outcome {
                result: ResolveResult::TimedOut,
                cursor: cursor.as_deref().map(|c| c.load(Ordering::SeqCst)),
            };
        }
        if let Some((handle, cursor)) = metrics_running {
            handle.abort();
            metrics_result = Outcome {
                result: ResolveResult::TimedOut,
                cursor: cursor.as_deref().map(|c| c.load(Ordering::SeqCst)),
            };
        }

        (agent_result, metrics_result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boxed(outcome: SendOutcome) -> BoxedSend {
        Box::pin(async move { outcome })
    }

    fn boxed_never() -> BoxedSend {
        Box::pin(std::future::pending::<SendOutcome>())
    }

    fn boxed_delayed(outcome: SendOutcome, delay: Duration) -> BoxedSend {
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            outcome
        })
    }

    #[tokio::test]
    async fn nothing_pending_resolves_immediately() {
        let s = Scheduler::new();
        let (a, m) = s.resolve_both_within(Duration::from_millis(50)).await;
        assert_eq!(a.result, ResolveResult::NothingPending);
        assert_eq!(m.result, ResolveResult::NothingPending);
    }

    #[tokio::test]
    async fn fast_sends_resolve_without_waiting_out_the_full_budget() {
        let s = Scheduler::new();
        s.spawn_agent_payload(boxed(SendOutcome::Success), None).await;
        s.spawn_platform_metrics(boxed(SendOutcome::Failure)).await;

        let start = tokio::time::Instant::now();
        let (a, m) = s.resolve_both_within(Duration::from_millis(100)).await;
        let elapsed = start.elapsed();

        assert_eq!(a.result, ResolveResult::Sent);
        assert_eq!(m.result, ResolveResult::Failed);
        assert!(elapsed < Duration::from_millis(80), "took {:?}", elapsed);
        assert!(!s.has_agent_payload_pending().await);
        assert!(!s.has_platform_metrics_pending().await);
    }

    #[tokio::test]
    async fn budget_is_shared_not_per_kind() {
        let s = Scheduler::new();
        s.spawn_agent_payload(boxed_never(), None).await;
        s.spawn_platform_metrics(boxed_never()).await;

        let start = tokio::time::Instant::now();
        let (a, m) = s.resolve_both_within(Duration::from_millis(60)).await;
        let elapsed = start.elapsed();

        assert_eq!(a.result, ResolveResult::TimedOut);
        assert_eq!(m.result, ResolveResult::TimedOut);
        assert!(
            elapsed < Duration::from_millis(150),
            "two still-pending kinds must share one budget, not pay it twice, took {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn mix_of_fast_and_slow_waits_only_for_the_slow_one() {
        let s = Scheduler::new();
        s.spawn_agent_payload(boxed(SendOutcome::Success), None).await;
        s.spawn_platform_metrics(boxed_delayed(SendOutcome::Success, Duration::from_millis(20))).await;

        let start = tokio::time::Instant::now();
        let (a, m) = s.resolve_both_within(Duration::from_millis(100)).await;
        let elapsed = start.elapsed();

        assert_eq!(a.result, ResolveResult::Sent);
        assert_eq!(m.result, ResolveResult::Sent);
        assert!(elapsed < Duration::from_millis(80), "took {:?}", elapsed);
    }

    #[tokio::test]
    async fn spawn_agent_payload_aborts_unresolved_orphan() {
        let s = Scheduler::new();
        let aborted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        struct AbortFlag(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for AbortFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let flag = AbortFlag(aborted.clone());
        let orphan: BoxedSend = Box::pin(async move {
            let _flag = flag;
            std::future::pending::<()>().await;
            SendOutcome::Success
        });
        s.spawn_agent_payload(orphan, None).await;
        s.spawn_agent_payload(boxed(SendOutcome::Success), None).await;
        tokio::task::yield_now().await;
        assert!(aborted.load(Ordering::SeqCst), "the orphaned first entry must be aborted");
    }
}
