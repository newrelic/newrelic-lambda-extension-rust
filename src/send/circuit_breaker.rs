// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `CircuitBreaker` trait contract (NR-643533).
//!
//! The real per-send-kind implementation (cap-5-consecutive-failures,
//! drop-while-open, half-open probe) is owned by Sashwat and lands in this
//! file, implementing this trait. `AlwaysAllowBreaker` is the stub the
//! scheduler is built and tested against until the real implementation is
//! ready to be wired in — see `TEAM_COLLAB_PLAN.md` §1/§3.
//!
//! `BreakerState`/`state()` have no production caller yet — observability
//! logging on breaker-state transitions (`REDESIGN_PLAN.md` §10.4) isn't
//! wired up until a real breaker exists to transition in the first place.

#![allow(dead_code)]

use super::{SendKind, SendOutcome};

/// Current breaker state, exposed for observability (logged on every
/// transition once the real implementation lands).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

pub trait CircuitBreaker: Send + Sync {
    /// Called by the scheduler before attempting a send. If `false`, the
    /// scheduler skips the send entirely and drops the data — deliberately
    /// lossy while open, per the team's "drop data rather than delay
    /// invokes" decision. Buffering is not attempted in this case.
    fn allow_send(&self, kind: SendKind) -> bool;

    /// Called by the scheduler after every send attempt completes,
    /// regardless of which mode (sync/deferred) triggered the attempt.
    fn record_result(&self, kind: SendKind, outcome: SendOutcome);

    /// Current state per kind, for logging state transitions.
    fn state(&self, kind: SendKind) -> BreakerState;
}

/// Stub implementation: always allows sends, never opens. The scheduler
/// (Phase B) is built and tested against this until the real breaker lands.
#[derive(Debug)]
pub struct AlwaysAllowBreaker;

impl CircuitBreaker for AlwaysAllowBreaker {
    fn allow_send(&self, _kind: SendKind) -> bool {
        true
    }

    fn record_result(&self, _kind: SendKind, _outcome: SendOutcome) {}

    fn state(&self, _kind: SendKind) -> BreakerState {
        BreakerState::Closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn always_allow_breaker_always_allows_and_stays_closed() {
        let breaker = AlwaysAllowBreaker;
        assert!(breaker.allow_send(SendKind::Logs));
        breaker.record_result(SendKind::Logs, SendOutcome::Failure);
        assert!(breaker.allow_send(SendKind::Logs));
        assert_eq!(breaker.state(SendKind::Logs), BreakerState::Closed);
    }
}
