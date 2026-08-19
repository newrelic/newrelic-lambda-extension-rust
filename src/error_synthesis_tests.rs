// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tests for `error_synthesis`
//!
//! `store_platform_metrics` / `clear_sent_errors_for_request` are the pure, synchronous
//! pieces of this module - the `send_*` functions require a real/mocked `NewRelicClient`
//! performing an actual HTTP call, which this codebase has no mocking infrastructure
//! for (`client_tests.rs`'s own tests only cover its pure helpers, never `send_agent_payload`).

#[cfg(test)]
mod tests {
    use crate::error_synthesis::{
        clear_sent_errors_for_request, store_platform_metrics, LastDetectedError,
        LAST_DETECTED_ERROR, LAST_PLATFORM_METRICS, SENT_ERRORS,
    };
    use serial_test::serial;

    // #[serial] because these touch the module's process-wide Mutex-guarded statics.

    #[test]
    #[serial]
    fn store_platform_metrics_overwrites_previous_value() {
        store_platform_metrics("req-1".to_string(), Some(100.0), Some(128), Some(64));
        store_platform_metrics("req-2".to_string(), Some(200.0), Some(256), Some(128));

        let guard = LAST_PLATFORM_METRICS.lock().expect("lock should not be poisoned");
        let metrics = guard.as_ref().expect("metrics should be stored");
        assert_eq!(metrics.request_id, "req-2");
        assert_eq!(metrics.duration_ms, Some(200.0));
        assert_eq!(metrics.memory_size_mb, Some(256));
        assert_eq!(metrics.max_memory_used_mb, Some(128));
    }

    #[test]
    #[serial]
    fn store_platform_metrics_accepts_all_none_fields() {
        store_platform_metrics("req-none".to_string(), None, None, None);

        let guard = LAST_PLATFORM_METRICS.lock().expect("lock should not be poisoned");
        let metrics = guard.as_ref().expect("metrics should be stored");
        assert_eq!(metrics.request_id, "req-none");
        assert_eq!(metrics.duration_ms, None);
        assert_eq!(metrics.memory_size_mb, None);
        assert_eq!(metrics.max_memory_used_mb, None);
    }

    #[test]
    #[serial]
    fn clear_sent_errors_for_request_empties_sent_errors_and_last_detected_error() {
        {
            let mut sent = SENT_ERRORS.lock().expect("lock should not be poisoned");
            sent.insert(("req-x".to_string(), "LambdaTimeout".to_string()));
        }
        {
            let mut last = LAST_DETECTED_ERROR.lock().expect("lock should not be poisoned");
            *last = Some(LastDetectedError { request_id: "req-x".to_string(), error_type: "OOM".to_string() });
        }

        clear_sent_errors_for_request("req-x");

        assert!(SENT_ERRORS.lock().expect("lock should not be poisoned").is_empty());
        assert!(LAST_DETECTED_ERROR.lock().expect("lock should not be poisoned").is_none());
    }

    #[test]
    #[serial]
    fn clear_sent_errors_for_request_is_a_noop_on_already_empty_state() {
        // Must not panic when both statics are already empty/None.
        SENT_ERRORS.lock().expect("lock should not be poisoned").clear();
        *LAST_DETECTED_ERROR.lock().expect("lock should not be poisoned") = None;

        clear_sent_errors_for_request("req-empty");

        assert!(SENT_ERRORS.lock().expect("lock should not be poisoned").is_empty());
        assert!(LAST_DETECTED_ERROR.lock().expect("lock should not be poisoned").is_none());
    }
}
