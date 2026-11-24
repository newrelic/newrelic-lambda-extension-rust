// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tests for `send::circuit_breaker`: giving back a probe that never ran, and reading N from the
//! environment.

use super::*;
use serial_test::serial;

fn fail_n(b: &ConsecutiveFailureBreaker, kind: SendKind, n: u32) {
    for _ in 0..n {
        b.record_result(kind, SendPermit::Normal, SendOutcome::Failure);
    }
}

// --- release_probe ---

#[test]
fn released_probe_goes_back_to_open_and_is_offered_again_at_once() {
    let b = ConsecutiveFailureBreaker::new(2);
    fail_n(&b, SendKind::Logs, 2);
    for _ in 0..4 {
        assert!(b.allow_send(SendKind::Logs).is_none());
    }
    assert!(b.allow_send(SendKind::Logs).is_some(), "the probe");
    assert_eq!(b.state(SendKind::Logs), BreakerState::HalfOpen);

    b.release_probe(SendKind::Logs, SendPermit::Probe);

    assert_eq!(b.state(SendKind::Logs), BreakerState::Open);
    assert!(b.allow_send(SendKind::Logs).is_some(), "no new 2N wait after a probe that never ran");
}

#[test]
fn release_probe_does_nothing_unless_half_open() {
    let b = ConsecutiveFailureBreaker::new(2);
    b.release_probe(SendKind::Logs, SendPermit::Probe);
    assert_eq!(b.state(SendKind::Logs), BreakerState::Closed);

    fail_n(&b, SendKind::Logs, 2);
    b.release_probe(SendKind::Logs, SendPermit::Probe);
    assert_eq!(b.state(SendKind::Logs), BreakerState::Open);
    assert!(b.allow_send(SendKind::Logs).is_none(), "still waiting out the 2N window");
}

#[test]
fn a_released_probe_that_then_succeeds_closes_the_breaker() {
    let b = ConsecutiveFailureBreaker::new(1);
    fail_n(&b, SendKind::ApmCollector, 1);
    for _ in 0..2 {
        assert!(b.allow_send(SendKind::ApmCollector).is_none());
    }
    assert!(b.allow_send(SendKind::ApmCollector).is_some());
    b.release_probe(SendKind::ApmCollector, SendPermit::Probe);

    assert_eq!(b.allow_send(SendKind::ApmCollector), Some(SendPermit::Probe), "probe offered again");
    b.record_result(SendKind::ApmCollector, SendPermit::Probe, SendOutcome::Success);

    assert_eq!(b.state(SendKind::ApmCollector), BreakerState::Closed);
}

#[test]
fn release_probe_only_affects_the_kind_it_is_given() {
    let b = ConsecutiveFailureBreaker::new(1);
    fail_n(&b, SendKind::Logs, 1);
    fail_n(&b, SendKind::MetricApi, 1);

    b.release_probe(SendKind::MetricApi, SendPermit::Probe);

    assert_eq!(b.state(SendKind::Logs), BreakerState::Open);
    assert_eq!(b.state(SendKind::MetricApi), BreakerState::Open);
    assert!(b.allow_send(SendKind::Logs).is_none(), "Logs is still skipping");
}

// --- only the probe resolves HalfOpen ---

/// Send A starts while Closed; the breaker then opens and lets probe B through.
/// Returns (A's permit, B's permit).
fn late_send_and_probe(b: &ConsecutiveFailureBreaker) -> (SendPermit, SendPermit) {
    let kind = SendKind::MetricApi;
    let a = b.allow_send(kind).expect("send A, while Closed");
    fail_n(b, kind, 2);
    for _ in 0..4 {
        assert!(b.allow_send(kind).is_none());
    }
    let probe = b.allow_send(kind).expect("probe B");
    assert_eq!(b.state(kind), BreakerState::HalfOpen);
    (a, probe)
}

#[test]
fn a_late_success_from_an_older_send_does_not_close_a_half_open_breaker() {
    let b = ConsecutiveFailureBreaker::new(2);
    let (a, probe) = late_send_and_probe(&b);

    b.record_result(SendKind::MetricApi, a, SendOutcome::Success);
    assert_eq!(b.state(SendKind::MetricApi), BreakerState::HalfOpen, "B is still in flight");

    b.record_result(SendKind::MetricApi, probe, SendOutcome::Success);
    assert_eq!(b.state(SendKind::MetricApi), BreakerState::Closed);
}

#[test]
fn a_late_failure_from_an_older_send_does_not_reopen_a_half_open_breaker() {
    let b = ConsecutiveFailureBreaker::new(2);
    let (a, probe) = late_send_and_probe(&b);

    b.record_result(SendKind::MetricApi, a, SendOutcome::Failure);
    assert_eq!(b.state(SendKind::MetricApi), BreakerState::HalfOpen, "B's result still counts");

    b.record_result(SendKind::MetricApi, probe, SendOutcome::Success);
    assert_eq!(b.state(SendKind::MetricApi), BreakerState::Closed);
}

#[test]
fn releasing_an_older_send_does_not_give_back_the_probe() {
    let b = ConsecutiveFailureBreaker::new(2);
    let (a, _probe) = late_send_and_probe(&b);

    b.release_probe(SendKind::MetricApi, a);
    assert_eq!(b.state(SendKind::MetricApi), BreakerState::HalfOpen);
}

// --- poisoned lock ---

#[test]
fn a_poisoned_lock_does_not_panic_the_breaker() {
    let b = std::sync::Arc::new(ConsecutiveFailureBreaker::new(1));
    let poisoner = b.clone();
    let _ = std::thread::spawn(move || {
        let _guard = poisoner.state.lock().unwrap();
        panic!("poison the breaker lock");
    })
    .join();
    assert!(b.state.is_poisoned());

    assert!(b.allow_send(SendKind::Logs).is_some());
    b.record_result(SendKind::Logs, SendPermit::Normal, SendOutcome::Failure);
    assert_eq!(b.state(SendKind::Logs), BreakerState::Open);
    b.release_probe(SendKind::Logs, SendPermit::Probe);
}

// --- threshold from the environment ---

#[test]
#[serial]
fn threshold_from_env_reads_the_variable_and_falls_back_to_the_default() {
    let saved = std::env::var(CIRCUIT_BREAKER_THRESHOLD_ENV_VAR).ok();

    std::env::remove_var(CIRCUIT_BREAKER_THRESHOLD_ENV_VAR);
    assert_eq!(threshold_from_env(10), 10, "unset: the tier default");

    std::env::set_var(CIRCUIT_BREAKER_THRESHOLD_ENV_VAR, "3");
    assert_eq!(threshold_from_env(10), 3, "set: the variable wins");

    std::env::set_var(CIRCUIT_BREAKER_THRESHOLD_ENV_VAR, "not-a-number");
    assert_eq!(threshold_from_env(5), 5, "invalid: the tier default");

    match saved {
        Some(v) => std::env::set_var(CIRCUIT_BREAKER_THRESHOLD_ENV_VAR, v),
        None => std::env::remove_var(CIRCUIT_BREAKER_THRESHOLD_ENV_VAR),
    }
}

#[test]
#[serial]
fn the_default_tier_breaker_built_from_the_environment_uses_n_10_when_unset() {
    let saved = std::env::var(CIRCUIT_BREAKER_THRESHOLD_ENV_VAR).ok();
    std::env::remove_var(CIRCUIT_BREAKER_THRESHOLD_ENV_VAR);

    let b = ConsecutiveFailureBreaker::from_env(DEFAULT_THRESHOLD_DEFAULT);
    fail_n(&b, SendKind::ApmCollector, 9);
    assert_eq!(b.state(SendKind::ApmCollector), BreakerState::Closed, "9 failures are not enough");
    fail_n(&b, SendKind::ApmCollector, 1);
    assert_eq!(b.state(SendKind::ApmCollector), BreakerState::Open, "the 10th opens it");

    if let Some(v) = saved {
        std::env::set_var(CIRCUIT_BREAKER_THRESHOLD_ENV_VAR, v);
    }
}
