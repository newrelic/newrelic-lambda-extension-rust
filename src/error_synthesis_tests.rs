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
    use std::sync::Arc;
    use std::time::Duration;
    use crate::config::ExtensionConfig;
    use crate::error_synthesis::{
        clear_sent_errors_for_request, retry_failed_errors, send_lambda_error,
        send_platform_fault_error, send_timeout_error, store_platform_metrics,
        FailedError, LastDetectedError,
        FAILED_ERRORS, LAST_DETECTED_ERROR, LAST_PLATFORM_METRICS, SENT_ERRORS,
    };
    use crate::newrelic::client::NewRelicClient;
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

    // ── helpers ────────────────────────────────────────────────────────────────

    fn noop_client_and_config() -> (Arc<crate::newrelic::client::NewRelicClient>, Arc<ExtensionConfig>) {
        let config = Arc::new(ExtensionConfig::default()); // license_key = None → send_agent_payload no-ops
        let client = Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        (client, config)
    }

    fn failing_client_and_config() -> (Arc<crate::newrelic::client::NewRelicClient>, Arc<ExtensionConfig>) {
        let mut cfg = ExtensionConfig::default();
        cfg.new_relic.license_key = Some("test-key".to_string());
        // Port 1 → immediate ECONNREFUSED; budget=0 → no retries
        cfg.new_relic.telemetry_endpoint = "http://127.0.0.1:1".to_string();
        cfg.new_relic.data_collection_timeout = Some(Duration::from_millis(0));
        let cfg = Arc::new(cfg);
        let client = Arc::new(crate::newrelic::client::NewRelicClient::new(&cfg));
        (client, cfg)
    }

    fn reset_globals() {
        SENT_ERRORS.lock().unwrap().clear();
        FAILED_ERRORS.lock().unwrap().clear();
        *LAST_PLATFORM_METRICS.lock().unwrap() = None;
        *LAST_DETECTED_ERROR.lock().unwrap() = None;
    }

    // ── retry_failed_errors ────────────────────────────────────────────────────

    #[tokio::test]
    #[serial]
    async fn retry_failed_errors_returns_false_when_queue_is_empty() {
        reset_globals();
        let (client, config) = noop_client_and_config();
        assert!(!retry_failed_errors(&client, &config).await);
    }

    #[tokio::test]
    #[serial]
    async fn retry_failed_errors_sends_queued_errors_and_returns_true() {
        reset_globals();
        FAILED_ERRORS.lock().unwrap().push(FailedError {
            request_id: "r1".to_string(),
            error_type: "LambdaTimeout".to_string(),
            error_message: "timed out".to_string(),
            invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:fn".to_string(),
            error_class: "LambdaTimeout".to_string(),
        });

        let (client, config) = noop_client_and_config();
        let result = retry_failed_errors(&client, &config).await;

        assert!(result);
        // Queue must be cleared regardless of outcome
        assert!(FAILED_ERRORS.lock().unwrap().is_empty());
        // Successful retry marks it as sent
        assert!(SENT_ERRORS.lock().unwrap().contains(&("r1".to_string(), "LambdaTimeout".to_string())));
    }

    #[tokio::test]
    #[serial]
    async fn retry_failed_errors_drops_error_after_one_retry_failure() {
        reset_globals();
        FAILED_ERRORS.lock().unwrap().push(FailedError {
            request_id: "r2".to_string(),
            error_type: "LambdaTimeout".to_string(),
            error_message: "timed out".to_string(),
            invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:fn".to_string(),
            error_class: "LambdaTimeout".to_string(),
        });

        let (client, config) = failing_client_and_config();
        let result = retry_failed_errors(&client, &config).await;

        assert!(result); // still returns true (retries were attempted)
        assert!(FAILED_ERRORS.lock().unwrap().is_empty()); // dropped, not re-queued
    }

    // ── send_timeout_error ─────────────────────────────────────────────────────

    #[tokio::test]
    #[serial]
    async fn send_timeout_error_uses_actual_duration_from_platform_metrics() {
        reset_globals();
        store_platform_metrics("req-t1".to_string(), Some(3500.0), Some(128), Some(64));

        let (client, config) = noop_client_and_config();
        send_timeout_error("req-t1", "arn:aws:lambda:us-east-1:123:function:fn", Some(10.0), &client, &config).await;

        assert!(SENT_ERRORS.lock().unwrap().contains(&("req-t1".to_string(), "LambdaTimeout".to_string())));
    }

    #[tokio::test]
    #[serial]
    async fn send_timeout_error_uses_provided_timeout_when_metrics_do_not_match() {
        reset_globals();
        // Metrics for a different request — must not be used
        store_platform_metrics("other-req".to_string(), Some(9000.0), None, None);

        let (client, config) = noop_client_and_config();
        send_timeout_error("req-t2", "arn:aws:lambda:us-east-1:123:function:fn", Some(5.0), &client, &config).await;

        assert!(SENT_ERRORS.lock().unwrap().contains(&("req-t2".to_string(), "LambdaTimeout".to_string())));
    }

    #[tokio::test]
    #[serial]
    async fn send_timeout_error_sends_without_timing_when_both_unavailable() {
        reset_globals();
        // No platform metrics and no timeout_seconds
        let (client, config) = noop_client_and_config();
        send_timeout_error("req-t3", "arn:aws:lambda:us-east-1:123:function:fn", None, &client, &config).await;

        assert!(SENT_ERRORS.lock().unwrap().contains(&("req-t3".to_string(), "LambdaTimeout".to_string())));
    }

    #[tokio::test]
    #[serial]
    async fn send_timeout_error_skips_duplicate_for_same_request() {
        reset_globals();
        SENT_ERRORS.lock().unwrap().insert(("req-dup".to_string(), "LambdaTimeout".to_string()));

        let (client, config) = noop_client_and_config();
        send_timeout_error("req-dup", "arn:aws:lambda:us-east-1:123:function:fn", Some(3.0), &client, &config).await;

        // Only one entry — the original; no new FAILED_ERRORS added
        assert_eq!(SENT_ERRORS.lock().unwrap().len(), 1);
        assert!(FAILED_ERRORS.lock().unwrap().is_empty());
    }

    #[tokio::test]
    #[serial]
    async fn send_timeout_error_stores_in_failed_errors_on_network_failure() {
        reset_globals();
        let (client, config) = failing_client_and_config();
        send_timeout_error("req-tf", "arn:aws:lambda:us-east-1:123:function:fn", Some(3.0), &client, &config).await;

        assert!(!FAILED_ERRORS.lock().unwrap().is_empty());
        let entry = FAILED_ERRORS.lock().unwrap();
        assert_eq!(entry[0].error_type, "LambdaTimeout");
        assert_eq!(entry[0].request_id, "req-tf");
    }

    // ── send_platform_fault_error ──────────────────────────────────────────────

    #[tokio::test]
    #[serial]
    async fn send_platform_fault_includes_memory_info_when_metrics_match() {
        reset_globals();
        store_platform_metrics("req-pf1".to_string(), None, Some(512), Some(480));

        let (client, config) = noop_client_and_config();
        send_platform_fault_error("req-pf1", "arn:aws:lambda:us-east-1:123:function:fn", &client, &config).await;

        assert!(SENT_ERRORS.lock().unwrap().contains(&("req-pf1".to_string(), "LambdaPlatformFault".to_string())));
    }

    #[tokio::test]
    #[serial]
    async fn send_platform_fault_omits_memory_info_when_metrics_do_not_match() {
        reset_globals();
        store_platform_metrics("other".to_string(), None, Some(128), Some(64));

        let (client, config) = noop_client_and_config();
        send_platform_fault_error("req-pf2", "arn:aws:lambda:us-east-1:123:function:fn", &client, &config).await;

        assert!(SENT_ERRORS.lock().unwrap().contains(&("req-pf2".to_string(), "LambdaPlatformFault".to_string())));
    }

    #[tokio::test]
    #[serial]
    async fn send_platform_fault_skips_duplicate_for_same_request() {
        reset_globals();
        SENT_ERRORS.lock().unwrap().insert(("req-pfdup".to_string(), "LambdaPlatformFault".to_string()));

        let (client, config) = noop_client_and_config();
        send_platform_fault_error("req-pfdup", "arn:aws:lambda:us-east-1:123:function:fn", &client, &config).await;

        assert_eq!(SENT_ERRORS.lock().unwrap().len(), 1);
        assert!(FAILED_ERRORS.lock().unwrap().is_empty());
    }

    #[tokio::test]
    #[serial]
    async fn send_platform_fault_stores_in_failed_errors_on_network_failure() {
        reset_globals();
        let (client, config) = failing_client_and_config();
        send_platform_fault_error("req-pff", "arn:aws:lambda:us-east-1:123:function:fn", &client, &config).await;

        assert!(!FAILED_ERRORS.lock().unwrap().is_empty());
        assert_eq!(FAILED_ERRORS.lock().unwrap()[0].error_type, "LambdaPlatformFault");
    }

    #[tokio::test]
    #[serial]
    async fn send_platform_fault_omits_memory_info_when_only_partial_fields_available() {
        reset_globals();
        // request_id matches but max_memory_used_mb is None → hits `_` wildcard branch
        store_platform_metrics("req-pfpartial".to_string(), None, None, None);

        let (client, config) = noop_client_and_config();
        send_platform_fault_error("req-pfpartial", "arn:aws:lambda:us-east-1:123:function:fn", &client, &config).await;

        assert!(SENT_ERRORS.lock().unwrap().contains(&("req-pfpartial".to_string(), "LambdaPlatformFault".to_string())));
    }

    // ── send_lambda_error ──────────────────────────────────────────────────────

    #[tokio::test]
    #[serial]
    async fn send_lambda_error_skips_duplicate_for_same_request_and_type() {
        reset_globals();
        SENT_ERRORS.lock().unwrap().insert(("req-le".to_string(), "CustomError".to_string()));

        let (client, config) = noop_client_and_config();
        send_lambda_error("msg", "req-le", "arn:aws:lambda:us-east-1:123:function:fn", "CustomError", &client, &config).await;

        assert_eq!(SENT_ERRORS.lock().unwrap().len(), 1);
        assert!(FAILED_ERRORS.lock().unwrap().is_empty());
    }

    #[tokio::test]
    #[serial]
    async fn send_lambda_error_succeeds_and_marks_sent() {
        reset_globals();
        let (client, config) = noop_client_and_config();
        send_lambda_error(
            "2024-01-01T00:00:00Z req-le2 Error: something failed",
            "req-le2",
            "arn:aws:lambda:us-east-1:123:function:fn",
            "RuntimeError",
            &client,
            &config,
        ).await;

        assert!(SENT_ERRORS.lock().unwrap().contains(&("req-le2".to_string(), "RuntimeError".to_string())));
    }

    #[tokio::test]
    #[serial]
    async fn send_lambda_error_stores_in_failed_errors_on_network_failure() {
        reset_globals();
        let (client, config) = failing_client_and_config();
        send_lambda_error("msg", "req-lef", "arn:aws:lambda:us-east-1:123:function:fn", "RuntimeError", &client, &config).await;

        assert!(!FAILED_ERRORS.lock().unwrap().is_empty());
        assert_eq!(FAILED_ERRORS.lock().unwrap()[0].error_type, "RuntimeError");
        assert_eq!(FAILED_ERRORS.lock().unwrap()[0].request_id, "req-lef");
    }

    #[tokio::test]
    #[serial]
    async fn send_lambda_error_uses_last_resort_arn_when_invoked_arn_and_fallback_both_empty() {
        reset_globals();
        // Empty invoked ARN + no global fallback → last-resort format using config fields
        let mut cfg = ExtensionConfig::default();
        cfg.aws.account_id = Some("123456789012".to_string());
        cfg.aws.function_name = "my-fn".to_string();
        // license_key = None → send_agent_payload no-ops (success)
        let cfg = Arc::new(cfg);
        let client = Arc::new(NewRelicClient::new_noop());

        send_lambda_error("msg", "req-lastresort", "", "RuntimeError", &client, &cfg).await;

        assert!(SENT_ERRORS.lock().unwrap().contains(&("req-lastresort".to_string(), "RuntimeError".to_string())));
    }
}
