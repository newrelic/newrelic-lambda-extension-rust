// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `SendTimeoutProvider` trait contract (NR-643534).
//!
//! The real p99-driven implementation is owned by Sashwat, blocked on
//! latency numbers from `#help-tdp` for its final default but buildable now
//! against a placeholder constant. `FixedTimeoutProvider` is the stub the
//! scheduler is built and tested against until then — see
//! `TEAM_COLLAB_PLAN.md` §1/§3.

use super::SendKind;
use std::time::Duration;

pub trait SendTimeoutProvider: Send + Sync {
    /// The bound the scheduler uses both for sync-mode's inline wait and
    /// deferred-mode's wait-on-previous-invocation's-straggler.
    ///
    /// The provider is only responsible for the endpoint-latency side of
    /// this bound (e.g. p99 healthy-send latency once measured) — it has no
    /// knowledge of the current invocation's `deadlineMs`. The scheduler
    /// (Phase B) is responsible for additionally clamping whatever this
    /// returns against the invocation's own deadline so the wait can never
    /// approach it.
    fn current_timeout(&self, kind: SendKind) -> Duration;
}

/// Stub implementation: one fixed duration for every kind. The scheduler
/// (Phase B) is built and tested against this until the real p99-driven
/// provider lands.
#[derive(Debug)]
pub struct FixedTimeoutProvider(pub Duration);

impl SendTimeoutProvider for FixedTimeoutProvider {
    fn current_timeout(&self, _kind: SendKind) -> Duration {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_timeout_provider_returns_same_value_for_every_kind() {
        let provider = FixedTimeoutProvider(Duration::from_millis(100));
        assert_eq!(provider.current_timeout(SendKind::Logs), Duration::from_millis(100));
        assert_eq!(provider.current_timeout(SendKind::ApmCollector), Duration::from_millis(100));
        assert_eq!(provider.current_timeout(SendKind::MetricApi), Duration::from_millis(100));
    }
}
