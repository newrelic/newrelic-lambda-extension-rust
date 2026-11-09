// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Phase C — the one customer-facing knob: `NEW_RELIC_SEND_TIER`.
//!
//! Maps to the internal [`SendMode`] + timeout + blocking-connect knobs per
//! `REDESIGN_PLAN.md` §3.3/§10.2 — customers only ever see `high`/`default`;
//! everything else stays an internal implementation detail.
//!
//! **Placeholder timeout, by design.** [`DEFAULT_SEND_TIMEOUT_MS`] (100ms)
//! is the same fixed bound used for both tiers today, matching the current
//! shipped `resolve_agent_send_at_boundary` constant exactly, so this is a
//! behavior-preserving default, not a new guess. Once Sashwat's p99-driven
//! `SendTimeoutProvider` (NR-643534) lands, swapping it in is either (a)
//! changing this one constant, or (b) replacing [`FixedTimeoutProvider`]
//! with the real implementation at the construction site below — nothing
//! else in this module or in `SendScheduler` needs to change either way.
//!
//! **`ExtensionConfig.extension.send_tier`** (`config/mod.rs`) holds the
//! parsed value for the rest of the codebase to read — `event_loop.rs`
//! branches on it directly. This module's own `SendTier::from_env()` is
//! still called a second time, independently, by `AGENT_SEND_SCHEDULER`'s
//! `Lazy` initializer (`event_loop.rs`) — harmless, since it's a pure
//! function of the same env var and the value can't change mid-process;
//! kept that way so `event_loop.rs` doesn't need `config` threaded into
//! static initialization. Sashwat's circuit-breaker/timeout-provider env
//! vars (NR-643533/NR-643534) still land directly in `config/mod.rs`
//! instead of here (see `TEAM_COLLAB_PLAN.md` §2) — no merge collision
//! risk, since this knob's own parsing is already settled.
//!
//! **`blocking_connect` has no production caller yet.** Generalizing
//! `NEW_RELIC_APM_BLOCKING_HANDSHAKE` to logs/metric-API connect warm-up
//! (`REDESIGN_PLAN.md` §10.2) is separate, not-yet-started work — this
//! method is ready for that, tested, not wired in.

#![allow(dead_code)]

use super::{AlwaysAllowBreaker, CircuitBreaker, FixedTimeoutProvider, SendMode, SendScheduler, SendTimeoutProvider};
use std::sync::Arc;
use std::time::Duration;

pub const SEND_TIER_ENV_VAR: &str = "NEW_RELIC_SEND_TIER";

/// Behavior-preserving placeholder — matches the existing shipped
/// `resolve_agent_send_at_boundary` bound. Replace with the real
/// p99-driven value once measured (NR-643534).
pub const DEFAULT_SEND_TIMEOUT_MS: u64 = 100;

/// The one customer-facing tier. `Low` is explicitly out of scope for now
/// (deferred per manager decision, see `REDESIGN_PLAN.md` §1.1) — adding it
/// later means adding one more match arm here, not a new knob shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SendTier {
    /// Today's behavior: synchronous sends, strong delivery guarantee.
    /// Safe default — zero behavior change for existing customers.
    #[default]
    Default,
    /// High-throughput and/or ultra-fast-function customers: deferred
    /// sends, resolved at the start of the next invocation.
    High,
}

impl SendTier {
    /// Parse from the raw env var value. Case-insensitive, trims
    /// whitespace. Unknown/empty values fall back to `Default` — this
    /// function never panics, per the "warn and fall back, don't crash a
    /// customer's Lambda" requirement.
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "high" => SendTier::High,
            "default" | "" => SendTier::Default,
            other => {
                tracing::warn!(
                    "Unknown {} value '{}' - falling back to 'default'",
                    SEND_TIER_ENV_VAR,
                    other
                );
                SendTier::Default
            }
        }
    }

    /// Reads `NEW_RELIC_SEND_TIER` from the environment. Missing env var is
    /// not a warning case (it's the expected default-on-upgrade path) -
    /// only a *present but unrecognized* value warns.
    pub fn from_env() -> Self {
        match std::env::var(SEND_TIER_ENV_VAR) {
            Ok(raw) => Self::parse(&raw),
            Err(_) => SendTier::Default,
        }
    }

    /// The internal send mode this tier maps to.
    pub fn send_mode(self) -> SendMode {
        match self {
            SendTier::Default => SendMode::Sync,
            SendTier::High => SendMode::Deferred,
        }
    }

    /// Whether cold start should block until the first connection to every
    /// send endpoint is warm (generalizes `NEW_RELIC_APM_BLOCKING_HANDSHAKE`
    /// beyond just the APM collector — see `REDESIGN_PLAN.md` §10.2). `high`
    /// tier customers' connections stay warm naturally at that volume, so
    /// the extra cold-start cost isn't worth paying.
    pub fn blocking_connect(self) -> bool {
        match self {
            SendTier::Default => true,
            SendTier::High => false,
        }
    }

    /// Build a `SendScheduler` configured for this tier, against the
    /// placeholder fixed timeout and the stub breaker. Both are swapped
    /// for real implementations at this one call site once they land —
    /// see the integration-session task in `TEAM_COLLAB_PLAN.md` §3.
    pub fn build_scheduler(self) -> SendScheduler {
        let breaker: Arc<dyn CircuitBreaker> = Arc::new(AlwaysAllowBreaker);
        let timeout_provider: Arc<dyn SendTimeoutProvider> =
            Arc::new(FixedTimeoutProvider(Duration::from_millis(DEFAULT_SEND_TIMEOUT_MS)));
        SendScheduler::new(self.send_mode(), breaker, timeout_provider)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_is_case_insensitive_and_trims_whitespace() {
        assert_eq!(SendTier::parse("high"), SendTier::High);
        assert_eq!(SendTier::parse("HIGH"), SendTier::High);
        assert_eq!(SendTier::parse("  High  "), SendTier::High);
        assert_eq!(SendTier::parse("default"), SendTier::Default);
        assert_eq!(SendTier::parse("DEFAULT"), SendTier::Default);
    }

    #[test]
    fn parse_empty_string_falls_back_to_default() {
        assert_eq!(SendTier::parse(""), SendTier::Default);
    }

    #[test]
    fn parse_unknown_value_falls_back_to_default_without_panicking() {
        assert_eq!(SendTier::parse("ultra-mega-fast"), SendTier::Default);
        assert_eq!(SendTier::parse("low"), SendTier::Default); // not built yet, per §1.1
    }

    #[test]
    fn default_tier_maps_to_sync_and_blocking_connect() {
        assert_eq!(SendTier::Default.send_mode(), SendMode::Sync);
        assert!(SendTier::Default.blocking_connect());
    }

    #[test]
    fn high_tier_maps_to_deferred_and_non_blocking_connect() {
        assert_eq!(SendTier::High.send_mode(), SendMode::Deferred);
        assert!(!SendTier::High.blocking_connect());
    }

    #[test]
    fn default_impl_matches_default_variant() {
        assert_eq!(SendTier::default(), SendTier::Default);
    }

    #[tokio::test]
    async fn build_scheduler_produces_a_working_scheduler_for_each_tier() {
        for tier in [SendTier::Default, SendTier::High] {
            let scheduler = tier.build_scheduler();
            assert_eq!(scheduler.mode(), tier.send_mode());
        }
    }
}
