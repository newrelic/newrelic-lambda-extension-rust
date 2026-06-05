// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Bounded retry-with-backoff policy for the one-shot cold-start calls to the
//! Lambda Extensions API: extension **registration** and Telemetry API
//! **subscription**.
//!
//! These calls happen exactly once per execution environment, during init. A
//! single transient failure (a localhost connection race while the sandbox is
//! coming up, or a `5xx`/`429` from the runtime API) would otherwise abort
//! startup via `?` and leave the environment running blind — no telemetry for
//! its whole lifetime.
//!
//! Under **Lambda Managed Instances** the blast radius is larger: the execution
//! environment is long-lived and serves many concurrent invocations, so a
//! failed init is not one lost invoke but a long-lived blind instance. That is
//! why these one-shot calls warrant a retry that the per-invoke event loop does
//! not.
//!
//! The schedule mirrors the event-polling retry in
//! [`crate::runtime::fetch_next_event`] (the "standard Lambda" path): at most
//! [`MAX_ATTEMPTS`] attempts with a short escalating backoff. It is
//! intentionally a touch broader than `fetch_next_event` — these one-shot calls
//! also retry `5xx`/`429` *responses*, not only transport errors, because there
//! is no outer loop to re-drive them after a server-side blip.
//!
//! | Outcome | Retried? |
//! |---|---|
//! | transport / IO failure (connect, timeout, reset) | ✅ |
//! | HTTP `>= 500` | ✅ |
//! | HTTP `429` (Too Many Requests) | ✅ |
//! | other `4xx` | ❌ terminal (a client error will not fix itself; for the
//!   subscription a `400`/`404` is the schema-fallback signal and MUST surface) |
//! | missing env var / missing header / deserialize | ❌ terminal |

use std::time::Duration;

/// Maximum number of attempts (initial try + retries) for a cold-start call.
pub(crate) const MAX_ATTEMPTS: u32 = 3;

/// Backoff to wait before the next attempt, given the 1-based number of the
/// attempt that just failed. Mirrors `fetch_next_event`'s 200/400/900 ms
/// schedule; with [`MAX_ATTEMPTS`] = 3 the 900 ms arm is headroom for a future
/// bump rather than a reachable delay.
pub(crate) fn backoff(failed_attempt: u32) -> Duration {
    match failed_attempt {
        1 => Duration::from_millis(200),
        2 => Duration::from_millis(400),
        _ => Duration::from_millis(900),
    }
}

/// Whether an HTTP status code represents a transient failure worth retrying.
/// `5xx` (server error) and `429` (rate limited) are transient; every other
/// `4xx` is terminal.
pub(crate) fn status_is_retryable(status: u16) -> bool {
    status >= 500 || status == 429
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_escalates_then_holds() {
        assert_eq!(backoff(1), Duration::from_millis(200));
        assert_eq!(backoff(2), Duration::from_millis(400));
        // Unreachable with MAX_ATTEMPTS=3, but defined for headroom.
        assert_eq!(backoff(3), Duration::from_millis(900));
        assert_eq!(backoff(99), Duration::from_millis(900));
    }

    #[test]
    fn fivexx_and_429_are_retryable() {
        assert!(status_is_retryable(500));
        assert!(status_is_retryable(503));
        assert!(status_is_retryable(599));
        assert!(status_is_retryable(429));
    }

    #[test]
    fn other_4xx_and_2xx_are_terminal() {
        for status in [200, 201, 400, 401, 403, 404, 409, 422, 428, 499] {
            assert!(
                !status_is_retryable(status),
                "status {status} must be terminal (not retried)"
            );
        }
    }

    #[test]
    fn max_attempts_is_bounded() {
        // Guard against an accidental 0 (would skip the call entirely) or an
        // unbounded value (would hang cold start under a persistent outage).
        assert!((1..=5).contains(&MAX_ATTEMPTS));
    }
}
