// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

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
