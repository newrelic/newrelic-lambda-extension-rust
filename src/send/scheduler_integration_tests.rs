// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests — the multi-invocation (n, n+1, n+2) sequence called
//! out in `REDESIGN_PLAN.md` §10.6, proving the two design invariants
//! directly rather than just the individual unit behaviors:
//!
//! 1. Zero added `/next`-equivalent latency in the healthy case.
//! 2. Bounded latency in the degraded case, for both tiers.
//!
//! These drive `SendScheduler` the same way `event_loop.rs` actually does:
//! dispatch at "runtimeDone", resolve at the start of the next simulated
//! invocation, before dispatching that invocation's own send.

use super::{AlwaysAllowBreaker, BoxedSend, DispatchResult, FixedTimeoutProvider, SendKind, SendMode, SendOutcome, SendScheduler};
use std::sync::Arc;
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

fn deferred_scheduler(timeout_ms: u64) -> SendScheduler {
    SendScheduler::new(
        SendMode::Deferred,
        Arc::new(AlwaysAllowBreaker),
        Arc::new(FixedTimeoutProvider(Duration::from_millis(timeout_ms))),
    )
}

/// Healthy path, `high` tier (deferred mode): invocation n dispatches and
/// returns instantly; by the time invocation n+1 starts, n's send has
/// already completed (it finished well within n's own handler + the gap
/// before n+1 arrives) — resolving it costs ~nothing. This is the
/// empirical proof design invariant 10.0.1 asks for: the common case must
/// measure as zero added latency, not just "small".
#[tokio::test]
async fn healthy_sequence_n_n1_n2_adds_zero_latency() {
    let scheduler = deferred_scheduler(200);

    // --- invocation n: runtimeDone fires, dispatch n's send ---
    let dispatch_n = scheduler
        .dispatch(SendKind::Logs, boxed_delayed(SendOutcome::Success, Duration::from_millis(10)), None)
        .await;
    assert_eq!(dispatch_n.result, DispatchResult::Deferred);

    // time passes between invocations (handler + cold network gap) - n's
    // send genuinely finishes in the background before n+1 starts.
    tokio::time::sleep(Duration::from_millis(30)).await;

    // --- invocation n+1 starts: resolve n's send first ---
    let start = tokio::time::Instant::now();
    let resolve_n = scheduler.resolve_pending(SendKind::Logs).await;
    assert_eq!(resolve_n.map(|o| o.result), Some(DispatchResult::Sent));
    assert!(
        start.elapsed() < Duration::from_millis(10),
        "n's send was already finished - resolving it must cost ~nothing, got {:?}",
        start.elapsed()
    );

    // --- invocation n+1: dispatch its own send, same shape ---
    let dispatch_n1 = scheduler
        .dispatch(SendKind::Logs, boxed_delayed(SendOutcome::Success, Duration::from_millis(10)), None)
        .await;
    assert_eq!(dispatch_n1.result, DispatchResult::Deferred);
    tokio::time::sleep(Duration::from_millis(30)).await;

    // --- invocation n+2 starts: resolve n+1's send ---
    let start = tokio::time::Instant::now();
    let resolve_n1 = scheduler.resolve_pending(SendKind::Logs).await;
    assert_eq!(resolve_n1.map(|o| o.result), Some(DispatchResult::Sent));
    assert!(start.elapsed() < Duration::from_millis(10));

    // --- invocation n+2: nothing pending beyond what n+1 already resolved ---
    assert_eq!(scheduler.resolve_pending(SendKind::Logs).await, None);
}

/// Degraded path, `high` tier: n's send never completes (simulates a hung
/// connection). Invocation n+1 must still only wait up to the configured
/// bound before giving up — this is design invariant 10.0.1's other half:
/// the one case that's allowed to block is bounded, not open-ended.
#[tokio::test]
async fn degraded_sequence_bounds_the_wait_at_n1_and_recovers_at_n2() {
    let scheduler = deferred_scheduler(50);

    // --- invocation n: dispatch a send that will never finish ---
    assert_eq!(
        scheduler.dispatch(SendKind::ApmCollector, boxed_never(), None).await.result,
        DispatchResult::Deferred
    );

    // --- invocation n+1 starts: resolving n's straggler is bounded ---
    let start = tokio::time::Instant::now();
    let resolve_n = scheduler.resolve_pending(SendKind::ApmCollector).await;
    let elapsed = start.elapsed();
    assert_eq!(resolve_n.map(|o| o.result), Some(DispatchResult::TimedOut));
    assert!(
        elapsed >= Duration::from_millis(50) && elapsed < Duration::from_millis(500),
        "n+1 must bound its wait to roughly the configured timeout, got {elapsed:?}"
    );

    // --- invocation n+1: dispatches its own (healthy) send normally ---
    assert_eq!(
        scheduler
            .dispatch(SendKind::ApmCollector, boxed(SendOutcome::Success), None)
            .await
            .result,
        DispatchResult::Deferred
    );

    // --- invocation n+2 starts: the system has recovered, n+1's send
    // resolves cleanly, proving one straggler doesn't poison later
    // invocations ---
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        scheduler.resolve_pending(SendKind::ApmCollector).await.map(|o| o.result),
        Some(DispatchResult::Sent)
    );
}

/// `default` tier (sync mode) healthy path: the send completes well inside
/// the bound, so dispatch() itself returns almost immediately - proving
/// sync mode's overhead is bounded by real latency, not the configured
/// ceiling, in the common case.
#[tokio::test]
async fn sync_tier_healthy_path_does_not_pay_the_full_configured_bound() {
    let scheduler = SendScheduler::new(
        SendMode::Sync,
        Arc::new(AlwaysAllowBreaker),
        Arc::new(FixedTimeoutProvider(Duration::from_millis(500))),
    );

    let start = tokio::time::Instant::now();
    let outcome = scheduler
        .dispatch(SendKind::MetricApi, boxed_delayed(SendOutcome::Success, Duration::from_millis(15)), None)
        .await;
    assert_eq!(outcome.result, DispatchResult::Sent);
    assert!(
        start.elapsed() < Duration::from_millis(100),
        "sync mode should take roughly as long as the real send, not the full 500ms ceiling"
    );
}

/// SHUTDOWN case: invocation n dispatches a deferred send and the sandbox
/// is reclaimed before any n+1 arrives. `drain_all` is the explicit
/// backstop for exactly this.
#[tokio::test]
async fn shutdown_drains_a_send_with_no_next_invocation_to_resolve_against() {
    let scheduler = deferred_scheduler(100);

    scheduler
        .dispatch(SendKind::Logs, boxed(SendOutcome::Success), None)
        .await;
    scheduler.dispatch(SendKind::MetricApi, boxed_never(), None).await;

    let mut drained = scheduler.drain_all().await;
    drained.sort_by_key(|(kind, _)| format!("{kind:?}"));
    let drained: Vec<(SendKind, DispatchResult)> =
        drained.into_iter().map(|(k, o)| (k, o.result)).collect();

    assert_eq!(
        drained,
        vec![
            (SendKind::Logs, DispatchResult::Sent),
            (SendKind::MetricApi, DispatchResult::TimedOut),
        ]
    );
}

/// Batch/cursor path, exercised the way the real agent-payload Flow-1 send
/// uses it: a multi-item send that advances a shared cursor as each item
/// resolves. On a bound-exceeded abort, the caller must be told exactly how
/// far it got, so it only re-buffers the unresolved tail.
#[tokio::test]
async fn batch_send_reports_cursor_progress_on_timeout() {
    let scheduler = SendScheduler::new(
        SendMode::Sync,
        Arc::new(AlwaysAllowBreaker),
        Arc::new(FixedTimeoutProvider(Duration::from_millis(50))),
    );

    let cursor = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let cursor_for_task = cursor.clone();
    let send: BoxedSend = Box::pin(async move {
        // 2 of 5 "items" resolve, then the batch hangs.
        cursor_for_task.store(1, std::sync::atomic::Ordering::SeqCst);
        cursor_for_task.store(2, std::sync::atomic::Ordering::SeqCst);
        std::future::pending::<()>().await;
        SendOutcome::Success
    });

    let outcome = scheduler.dispatch(SendKind::ApmCollector, send, Some(cursor)).await;
    assert_eq!(outcome.result, DispatchResult::TimedOut);
    assert_eq!(
        outcome.cursor,
        Some(2),
        "caller must know exactly how far the batch got, to buffer only items[2..]"
    );
}
