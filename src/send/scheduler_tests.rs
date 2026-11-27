// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tests for `send::scheduler`: a probe that is detached or cut off is given back to the breaker,
//! and a cut-off send is not a failure.

use super::*;
use crate::send::circuit_breaker::BreakerState;
use crate::send::ConsecutiveFailureBreaker;
use std::sync::Arc;
use std::time::Duration;

fn boxed_never() -> BoxedSend {
    Box::pin(std::future::pending::<SendOutcome>())
}

fn scheduler_with(breaker: Arc<ConsecutiveFailureBreaker>) -> SendScheduler {
    SendScheduler::new(breaker)
}

/// A breaker (N=1) that is half-open with its probe in flight for the collector.
async fn scheduler_with_probe_in_flight() -> (SendScheduler, Arc<ConsecutiveFailureBreaker>) {
    let breaker = Arc::new(ConsecutiveFailureBreaker::new(1));
    let scheduler = scheduler_with(breaker.clone());
    breaker.record_result(SendKind::ApmCollector, crate::send::SendPermit::Normal, SendOutcome::Failure);
    assert!(!scheduler.spawn(SendKind::ApmCollector, boxed_never(), None).await);
    assert!(!scheduler.spawn(SendKind::ApmCollector, boxed_never(), None).await);
    assert!(scheduler.spawn(SendKind::ApmCollector, boxed_never(), None).await, "the probe");
    assert_eq!(breaker.state(SendKind::ApmCollector), BreakerState::HalfOpen);
    (scheduler, breaker)
}

#[tokio::test]
async fn detaching_the_probe_gives_it_back() {
    let (scheduler, breaker) = scheduler_with_probe_in_flight().await;

    assert!(scheduler.detach(SendKind::ApmCollector).await);

    assert_eq!(breaker.state(SendKind::ApmCollector), BreakerState::Open, "not stuck half-open");
    assert!(breaker.allow_send(SendKind::ApmCollector).is_some(), "the next call probes again");
}

#[tokio::test]
async fn detaching_a_send_while_closed_changes_nothing() {
    let breaker = Arc::new(ConsecutiveFailureBreaker::new(1));
    let scheduler = scheduler_with(breaker.clone());
    scheduler.spawn(SendKind::Logs, boxed_never(), None).await;

    assert!(scheduler.detach(SendKind::Logs).await);

    assert_eq!(breaker.state(SendKind::Logs), BreakerState::Closed);
}

#[tokio::test]
async fn a_probe_cut_off_by_the_shared_budget_is_not_a_failure() {
    let (scheduler, breaker) = scheduler_with_probe_in_flight().await;

    let outcomes = scheduler
        .resolve_pending_shared_budget(&[SendKind::ApmCollector], Duration::from_millis(20))
        .await;

    assert_eq!(outcomes[&SendKind::ApmCollector].result, DispatchResult::TimedOut);
    assert_eq!(
        breaker.state(SendKind::ApmCollector),
        BreakerState::Open,
        "given back, not re-opened by a failure"
    );
    assert!(breaker.allow_send(SendKind::ApmCollector).is_some(), "the next call probes again at once");
}

#[tokio::test]
async fn a_send_cut_off_by_the_shared_budget_does_not_open_a_closed_breaker() {
    let breaker = Arc::new(ConsecutiveFailureBreaker::new(1));
    let scheduler = scheduler_with(breaker.clone());
    scheduler.spawn(SendKind::Logs, boxed_never(), None).await;

    let _ = scheduler.resolve_pending_shared_budget(&[SendKind::Logs], Duration::from_millis(20)).await;

    assert_eq!(breaker.state(SendKind::Logs), BreakerState::Closed);
    assert!(scheduler.breaker_closed(SendKind::Logs));
}

#[tokio::test]
async fn a_send_that_finishes_inside_the_shared_budget_still_records_its_result() {
    let breaker = Arc::new(ConsecutiveFailureBreaker::new(1));
    let scheduler = scheduler_with(breaker.clone());
    let failing: BoxedSend = Box::pin(async { SendOutcome::Failure });
    scheduler.spawn(SendKind::Logs, failing, None).await;

    let outcomes = scheduler.resolve_pending_shared_budget(&[SendKind::Logs], Duration::from_millis(200)).await;

    assert_eq!(outcomes[&SendKind::Logs].result, DispatchResult::Failed);
    assert_eq!(breaker.state(SendKind::Logs), BreakerState::Open, "a real failure still counts");
}

#[tokio::test]
async fn breaker_closed_is_false_while_the_breaker_is_open() {
    let breaker = Arc::new(ConsecutiveFailureBreaker::new(1));
    let scheduler = scheduler_with(breaker.clone());
    assert!(scheduler.breaker_closed(SendKind::Logs));

    breaker.record_result(SendKind::Logs, crate::send::SendPermit::Normal, SendOutcome::Failure);

    assert!(!scheduler.breaker_closed(SendKind::Logs));
    assert!(scheduler.breaker_closed(SendKind::ApmCollector), "other kinds are independent");
}

#[tokio::test]
async fn breaker_closed_does_not_count_an_invocation() {
    let breaker = Arc::new(ConsecutiveFailureBreaker::new(1));
    let scheduler = scheduler_with(breaker.clone());
    breaker.record_result(SendKind::Logs, crate::send::SendPermit::Normal, SendOutcome::Failure);

    for _ in 0..10 {
        assert!(!scheduler.breaker_closed(SendKind::Logs));
    }

    // Only 2 skips are needed before the probe; checking the state must not have used them.
    assert!(breaker.allow_send(SendKind::Logs).is_none());
    assert!(breaker.allow_send(SendKind::Logs).is_none());
    assert!(breaker.allow_send(SendKind::Logs).is_some(), "the probe still comes after exactly 2N skips");
}
