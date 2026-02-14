//! Shared retry utilities for exponential backoff
//!
//! Centralizes retry constants and backoff logic used across:
//! - `newrelic::client` (HTTP payload sending)
//! - `logs::processor` (log auto-flush)
//! - `apm::app` (APM collector connection)

use std::time::Duration;

/// Maximum number of retry attempts before giving up
pub const MAX_RETRIES: usize = 3;

/// Calculate backoff delay for a given retry attempt (1-indexed).
///
/// Backoff schedule:
/// - Attempt 1: 200ms
/// - Attempt 2: 400ms
/// - Attempt 3+: 900ms
pub fn get_backoff_delay(retry_attempt: usize) -> Duration {
    match retry_attempt {
        1 => Duration::from_millis(200),
        2 => Duration::from_millis(400),
        _ => Duration::from_millis(900),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_delay_attempt_1() {
        assert_eq!(get_backoff_delay(1), Duration::from_millis(200));
    }

    #[test]
    fn test_backoff_delay_attempt_2() {
        assert_eq!(get_backoff_delay(2), Duration::from_millis(400));
    }

    #[test]
    fn test_backoff_delay_attempt_3() {
        assert_eq!(get_backoff_delay(3), Duration::from_millis(900));
    }

    #[test]
    fn test_backoff_delay_attempt_beyond_max() {
        // All attempts beyond 3 get the same max delay
        assert_eq!(get_backoff_delay(4), Duration::from_millis(900));
        assert_eq!(get_backoff_delay(10), Duration::from_millis(900));
        assert_eq!(get_backoff_delay(100), Duration::from_millis(900));
    }

    #[test]
    fn test_backoff_delay_attempt_zero() {
        // Edge case: attempt 0 falls into the catch-all
        assert_eq!(get_backoff_delay(0), Duration::from_millis(900));
    }

    #[test]
    fn test_backoff_is_monotonically_increasing() {
        let d1 = get_backoff_delay(1);
        let d2 = get_backoff_delay(2);
        let d3 = get_backoff_delay(3);
        assert!(d1 < d2, "delay(1) < delay(2)");
        assert!(d2 < d3, "delay(2) < delay(3)");
    }

    #[test]
    fn test_max_retries_constant() {
        assert_eq!(MAX_RETRIES, 3);
    }
}
