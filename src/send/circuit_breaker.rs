// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `CircuitBreaker` trait + the real `ConsecutiveFailureBreaker` (NR-643533).
//!
//! **Invocation-count model** (per the dual-path design):
//! - Open a `SendKind` after `threshold` (`N`) consecutive failures.
//! - While Open, **don't attempt** for `2N` invocations — each `allow_send`
//!   call while Open counts as one invocation and returns `false` (the send is
//!   skipped; the caller buffers the payload).
//! - After `2N` skipped invocations, the next `allow_send` returns `true` once
//!   (the retry) and the kind goes `HalfOpen`. Retry success → `Closed`; retry
//!   failure → `Open` again, restarting the `2N` count.
//!
//! Cooldown is counted in **invocations, not wall-clock time** — deliberately,
//! because the Lambda sandbox freezes between invokes, so a time-based cooldown
//! would measure only active (unfrozen) time. Per-tier numbers:
//!   - default tier: `N = 10` → open `2N = 20` → buffer cap `3N = 30`
//!   - high tier:    `N = 5`  → open `2N = 10` → buffer cap `3N = 15`
//! Only `N` is configured; `2N`/`3N` derive from it. (The `3N` buffer itself
//! lives in the send path, not here — this file is only the breaker state
//! machine: it decides *whether to attempt* a send.)

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Mutex;

use super::{SendKind, SendOutcome};

/// Current breaker state, exposed for observability (logged on every transition).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

pub trait CircuitBreaker: Send + Sync {
    /// Called before attempting a send. `false` → the caller skips the send
    /// (and buffers the payload). While Open this returns `false` for `2N`
    /// invocations, then `true` once (the retry probe).
    fn allow_send(&self, kind: SendKind) -> bool;

    /// Called after every send attempt completes (success or failure).
    fn record_result(&self, kind: SendKind, outcome: SendOutcome);

    /// Current state per kind, for logging state transitions.
    fn state(&self, kind: SendKind) -> BreakerState;

    /// The retry that `allow_send` let through never ran, so there is no result to record.
    /// Goes back to Open with the full `2N` already skipped, so the next call retries again.
    /// A no-op unless the kind is `HalfOpen`.
    fn release_probe(&self, _kind: SendKind) {}
}

/// Stub implementation: always allows sends, never opens. Kept for tests and as
/// the default when the breaker is not in play.
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

/// Per-`SendKind` state. Each endpoint (logs, APM collector, metric API) fails
/// and recovers independently.
#[derive(Debug, Clone)]
struct KindState {
    state: BreakerState,
    /// Consecutive failures while `Closed`. Reset to 0 on any success; at
    /// `threshold` the breaker opens.
    failures: u32,
    /// Number of invocations skipped while `Open`. Each `allow_send` call while
    /// Open increments this; once it reaches `2 * threshold`, the next
    /// `allow_send` lets one retry through (→ `HalfOpen`).
    skips_while_open: u32,
}

impl Default for KindState {
    fn default() -> Self {
        Self {
            state: BreakerState::Closed,
            failures: 0,
            skips_while_open: 0,
        }
    }
}

/// Real `CircuitBreaker` — invocation-count model (see module docs).
///
/// State is tracked independently per `SendKind` behind a single `Mutex`,
/// mirroring the pattern used by `SendScheduler.pending`. The trait methods are
/// synchronous (no `.await` held across the lock) and called ~once per send per
/// kind, so a plain `std::sync::Mutex` is correct and cheap here.
#[derive(Debug)]
pub struct ConsecutiveFailureBreaker {
    /// `N`: consecutive failures before Open. The open window is `2N`
    /// invocations (derived).
    threshold: u32,
    state: Mutex<HashMap<SendKind, KindState>>,
}

impl ConsecutiveFailureBreaker {
    /// Build with an explicit failure threshold (`N`). The open window (`2N`)
    /// derives from it. Each `SendKind` starts `Closed` lazily on first lookup.
    pub fn new(threshold: u32) -> Self {
        // A zero threshold would open immediately and never recover; clamp to 1.
        Self {
            threshold: threshold.max(1),
            state: Mutex::new(HashMap::new()),
        }
    }

    /// Build from `NEW_RELIC_EXTENSION_CIRCUIT_BREAKER_THRESHOLD`, falling back
    /// to `default_threshold` (the per-tier default — pass
    /// [`DEFAULT_THRESHOLD_HIGH`] for the high tier, [`DEFAULT_THRESHOLD_DEFAULT`]
    /// for the default tier).
    pub fn from_env(default_threshold: u32) -> Self {
        Self::new(threshold_from_env(default_threshold))
    }

    /// The open window in invocations (`2N`).
    fn open_window(&self) -> u32 {
        self.threshold.saturating_mul(2)
    }
}

impl CircuitBreaker for ConsecutiveFailureBreaker {
    fn allow_send(&self, kind: SendKind) -> bool {
        let open_window = self.open_window();
        let mut map = self.state.lock().unwrap();
        let entry = map.entry(kind).or_default();
        match entry.state {
            BreakerState::Closed => true,
            BreakerState::Open => {
                if entry.skips_while_open >= open_window {
                    // 2N invocations skipped — let exactly one retry through.
                    entry.state = BreakerState::HalfOpen;
                    tracing::info!(
                        "Circuit breaker for {:?} half-open: {} invocations skipped, allowing one retry",
                        kind,
                        entry.skips_while_open
                    );
                    true
                } else {
                    entry.skips_while_open += 1;
                    false
                }
            }
            // The single retry is already out for this kind — skip any other
            // send until record_result resolves it.
            BreakerState::HalfOpen => false,
        }
    }

    fn record_result(&self, kind: SendKind, outcome: SendOutcome) {
        let threshold = self.threshold;
        let mut map = self.state.lock().unwrap();
        let entry = map.entry(kind).or_default();
        match (entry.state, outcome) {
            (BreakerState::Closed, SendOutcome::Success) => {
                entry.failures = 0;
            }
            (BreakerState::Closed, SendOutcome::Failure) => {
                entry.failures += 1;
                if entry.failures >= threshold {
                    entry.state = BreakerState::Open;
                    entry.skips_while_open = 0;
                    tracing::warn!(
                        "Circuit breaker for {:?} opened after {} consecutive failures \
                         (skipping sends for {} invocations)",
                        kind,
                        entry.failures,
                        threshold.saturating_mul(2)
                    );
                }
            }
            (BreakerState::HalfOpen, SendOutcome::Success) => {
                entry.state = BreakerState::Closed;
                entry.failures = 0;
                entry.skips_while_open = 0;
                tracing::info!("Circuit breaker for {:?} closed: retry succeeded", kind);
            }
            (BreakerState::HalfOpen, SendOutcome::Failure) => {
                // Reopen and restart the 2N skip count from this failed retry.
                entry.state = BreakerState::Open;
                entry.skips_while_open = 0;
                tracing::warn!("Circuit breaker for {:?} reopened: retry failed", kind);
            }
            // allow_send returned false while Open, so no send should have been
            // attempted — a stray/late result here is a defensive no-op.
            (BreakerState::Open, _) => {}
        }
    }

    fn state(&self, kind: SendKind) -> BreakerState {
        self.state.lock().unwrap().get(&kind).map_or(BreakerState::Closed, |s| s.state)
    }

    fn release_probe(&self, kind: SendKind) {
        let open_window = self.open_window();
        let mut map = self.state.lock().unwrap();
        let entry = map.entry(kind).or_default();
        if entry.state == BreakerState::HalfOpen {
            entry.state = BreakerState::Open;
            entry.skips_while_open = open_window;
        }
    }
}

/// The one env var this module introduces: the failure threshold `N`.
pub const CIRCUIT_BREAKER_THRESHOLD_ENV_VAR: &str = "NEW_RELIC_EXTENSION_CIRCUIT_BREAKER_THRESHOLD";

/// Per-tier default thresholds. Only `N` is configured; open window (`2N`) and
/// buffer cap (`3N`, in the send path) derive from it.
pub const DEFAULT_THRESHOLD_HIGH: u32 = 5;
pub const DEFAULT_THRESHOLD_DEFAULT: u32 = 10;

/// Parse `NEW_RELIC_EXTENSION_CIRCUIT_BREAKER_THRESHOLD`. Empty/unset falls back
/// to `default` silently (the expected case); a present-but-invalid value warns
/// and falls back. Never panics.
pub fn parse_threshold(raw: &str, default: u32) -> u32 {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return default;
    }
    trimmed.parse::<u32>().unwrap_or_else(|_| {
        tracing::warn!(
            "Invalid {} value '{}' - falling back to {}",
            CIRCUIT_BREAKER_THRESHOLD_ENV_VAR,
            raw,
            default
        );
        default
    })
}

pub fn threshold_from_env(default: u32) -> u32 {
    std::env::var(CIRCUIT_BREAKER_THRESHOLD_ENV_VAR).map_or(default, |raw| parse_threshold(&raw, default))
}

#[cfg(test)]
mod tests {
    use super::*;

    // high-tier default for the tests
    const N: u32 = 5;

    fn breaker() -> ConsecutiveFailureBreaker {
        ConsecutiveFailureBreaker::new(N)
    }

    fn fail_n(b: &ConsecutiveFailureBreaker, kind: SendKind, n: u32) {
        for _ in 0..n {
            b.record_result(kind, SendOutcome::Failure);
        }
    }

    #[test]
    fn always_allow_breaker_always_allows_and_stays_closed() {
        let b = AlwaysAllowBreaker;
        assert!(b.allow_send(SendKind::Logs));
        b.record_result(SendKind::Logs, SendOutcome::Failure);
        assert!(b.allow_send(SendKind::Logs));
        assert_eq!(b.state(SendKind::Logs), BreakerState::Closed);
    }

    // --- Closed / threshold ---

    #[test]
    fn stays_closed_and_allows_below_threshold() {
        let b = breaker();
        for _ in 0..(N - 1) {
            assert!(b.allow_send(SendKind::Logs));
            b.record_result(SendKind::Logs, SendOutcome::Failure);
        }
        assert_eq!(b.state(SendKind::Logs), BreakerState::Closed);
        assert!(b.allow_send(SendKind::Logs));
    }

    #[test]
    fn opens_after_exactly_n_consecutive_failures() {
        let b = breaker();
        fail_n(&b, SendKind::Logs, N);
        assert_eq!(b.state(SendKind::Logs), BreakerState::Open);
    }

    #[test]
    fn success_resets_the_counter_even_mid_streak() {
        let b = breaker();
        fail_n(&b, SendKind::Logs, N - 1);
        b.record_result(SendKind::Logs, SendOutcome::Success);
        assert_eq!(b.state(SendKind::Logs), BreakerState::Closed);
        // Counting restarts: N-1 more fails stays Closed, the Nth opens.
        fail_n(&b, SendKind::Logs, N - 1);
        assert_eq!(b.state(SendKind::Logs), BreakerState::Closed);
        b.record_result(SendKind::Logs, SendOutcome::Failure);
        assert_eq!(b.state(SendKind::Logs), BreakerState::Open);
    }

    // --- Open / invocation-count cooldown ---

    #[test]
    fn skips_for_2n_invocations_then_allows_one_retry() {
        let b = breaker();
        fail_n(&b, SendKind::Logs, N);
        assert_eq!(b.state(SendKind::Logs), BreakerState::Open);

        // 2N skipped invocations, all refused.
        for _ in 0..(2 * N) {
            assert!(!b.allow_send(SendKind::Logs));
        }
        assert_eq!(b.state(SendKind::Logs), BreakerState::Open);

        // The next invocation is the retry — allowed once, and flips HalfOpen.
        assert!(b.allow_send(SendKind::Logs));
        assert_eq!(b.state(SendKind::Logs), BreakerState::HalfOpen);

        // No second retry while the first is still outstanding.
        assert!(!b.allow_send(SendKind::Logs));
    }

    #[test]
    fn half_open_success_closes_the_breaker() {
        let b = breaker();
        fail_n(&b, SendKind::Logs, N);
        for _ in 0..(2 * N) {
            b.allow_send(SendKind::Logs);
        }
        assert!(b.allow_send(SendKind::Logs)); // retry
        b.record_result(SendKind::Logs, SendOutcome::Success);
        assert_eq!(b.state(SendKind::Logs), BreakerState::Closed);
        // Fully reset: needs another full N fails to reopen.
        fail_n(&b, SendKind::Logs, N - 1);
        assert_eq!(b.state(SendKind::Logs), BreakerState::Closed);
    }

    #[test]
    fn half_open_failure_reopens_and_restarts_the_2n_count() {
        let b = breaker();
        fail_n(&b, SendKind::Logs, N);
        for _ in 0..(2 * N) {
            b.allow_send(SendKind::Logs);
        }
        assert!(b.allow_send(SendKind::Logs)); // retry
        b.record_result(SendKind::Logs, SendOutcome::Failure);
        assert_eq!(b.state(SendKind::Logs), BreakerState::Open);

        // Skip count restarted: must skip a fresh 2N before the next retry.
        for _ in 0..(2 * N) {
            assert!(!b.allow_send(SendKind::Logs));
        }
        assert!(b.allow_send(SendKind::Logs));
    }

    // --- Per-kind independence ---

    #[test]
    fn kinds_are_tracked_independently() {
        let b = breaker();
        fail_n(&b, SendKind::Logs, N);
        assert_eq!(b.state(SendKind::Logs), BreakerState::Open);
        assert_eq!(b.state(SendKind::ApmCollector), BreakerState::Closed);
        assert_eq!(b.state(SendKind::MetricApi), BreakerState::Closed);
        assert!(b.allow_send(SendKind::ApmCollector));
        assert!(b.allow_send(SendKind::MetricApi));
        assert!(!b.allow_send(SendKind::Logs));
    }

    // --- Per-tier N (open window derives as 2N) ---

    #[test]
    fn default_tier_threshold_opens_at_10_and_skips_20() {
        let b = ConsecutiveFailureBreaker::new(DEFAULT_THRESHOLD_DEFAULT); // 10
        fail_n(&b, SendKind::ApmCollector, 9);
        assert_eq!(b.state(SendKind::ApmCollector), BreakerState::Closed);
        b.record_result(SendKind::ApmCollector, SendOutcome::Failure); // 10th
        assert_eq!(b.state(SendKind::ApmCollector), BreakerState::Open);
        for _ in 0..20 {
            assert!(!b.allow_send(SendKind::ApmCollector));
        }
        assert!(b.allow_send(SendKind::ApmCollector)); // retry after 2N=20
    }

    #[test]
    fn zero_threshold_is_clamped_to_one() {
        let b = ConsecutiveFailureBreaker::new(0);
        b.record_result(SendKind::Logs, SendOutcome::Failure);
        assert_eq!(b.state(SendKind::Logs), BreakerState::Open);
    }

    // --- Env parsing ---

    #[test]
    fn parse_threshold_empty_falls_back_to_default() {
        assert_eq!(parse_threshold("", 5), 5);
        assert_eq!(parse_threshold("   ", 10), 10);
    }

    #[test]
    fn parse_threshold_valid_override() {
        assert_eq!(parse_threshold("3", 5), 3);
        assert_eq!(parse_threshold(" 8 ", 5), 8);
    }

    #[test]
    fn parse_threshold_garbage_falls_back_without_panicking() {
        assert_eq!(parse_threshold("not-a-number", 5), 5);
        assert_eq!(parse_threshold("-1", 7), 7);
    }
}

#[cfg(test)]
#[path = "circuit_breaker_tests.rs"]
mod circuit_breaker_tests;
