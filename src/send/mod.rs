// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Shared send-path primitives: the process-wide circuit breaker and `SendScheduler`,
//! which tracks the in-flight `high`-tier sends (`event_loop.rs`'s `SEND_SCHEDULER`).

pub mod circuit_breaker;
pub(crate) mod concurrent;
pub mod scheduler;
pub mod send_target;
pub mod short_invoke;
pub mod tier;

pub use circuit_breaker::{BreakerState, CircuitBreaker, ConsecutiveFailureBreaker, SendPermit};
pub use scheduler::{DispatchOutcome, DispatchResult, SendScheduler};
pub use send_target::BoxedSend;
pub use tier::SendTier;

/// Which telemetry path a send belongs to. Breaker state is tracked per kind,
/// not globally, since the three endpoints fail and recover independently.
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
