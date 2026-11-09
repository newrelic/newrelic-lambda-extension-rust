// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Shared send-path primitives for the `NEW_RELIC_SEND_TIER` redesign.
//!
//! `circuit_breaker` and `timeout_provider` define the two trait contracts
//! that let the scheduler (added in a later phase) and their real
//! implementations be built independently and in parallel: the scheduler
//! depends only on the trait + the stub impls below; the real
//! `CircuitBreaker`/`SendTimeoutProvider` implementations are developed and
//! unit-tested against the same trait without needing the scheduler to
//! exist. See `TEAM_COLLAB_PLAN.md` for the collaboration contract this
//! module implements.
//!
//! Unused until Phase B (`SendScheduler`) wires these in — `allow(dead_code)`
//! on this module is intentional scaffolding, not a sign something's wrong,
//! and should be removed once Phase B lands.

#![allow(dead_code, unused_imports)]

pub mod circuit_breaker;
pub mod scheduler;
#[cfg(test)]
mod scheduler_integration_tests;
pub mod send_target;
pub mod tier;
pub mod timeout_provider;

pub use circuit_breaker::{AlwaysAllowBreaker, BreakerState, CircuitBreaker};
pub use scheduler::{DispatchResult, SendMode, SendScheduler};
pub use send_target::BoxedSend;
pub use tier::SendTier;
pub use timeout_provider::{FixedTimeoutProvider, SendTimeoutProvider};

/// Which telemetry path a send belongs to. Breakers and timeout bounds are
/// tracked per kind, not globally, since the three endpoints fail and
/// recover independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SendKind {
    Logs,
    ApmCollector,
    MetricApi,
}

/// Outcome of a single send attempt, reported to the circuit breaker after
/// every attempt regardless of which mode (sync/deferred) triggered it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendOutcome {
    Success,
    Failure,
}
