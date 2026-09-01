// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tests for `newrelic::client`

use super::*;

#[test]
fn test_get_extension_name_with_version() {
    let name = get_extension_name_with_version();
    assert!(name.starts_with("newrelic-lambda-extension:"));
    assert_eq!(name, format!("{EXTENSION_NAME}:{EXTENSION_VERSION}"));
}

#[test]
fn test_get_backoff_delay_schedule() {
    assert_eq!(get_backoff_delay(1), std::time::Duration::from_millis(200));
    assert_eq!(get_backoff_delay(2), std::time::Duration::from_millis(400));
    assert_eq!(get_backoff_delay(3), std::time::Duration::from_millis(900));
    // Anything beyond the explicit schedule falls back to the same ceiling as attempt 3.
    assert_eq!(get_backoff_delay(4), std::time::Duration::from_millis(900));
    assert_eq!(get_backoff_delay(0), std::time::Duration::from_millis(900));
}

#[test]
fn test_build_outbound_client_without_proxy() {
    // Must not panic - `.expect()` inside build_outbound_client would fail the test.
    let _client = build_outbound_client(None);
}

#[test]
fn test_build_outbound_client_with_valid_proxy() {
    let _client = build_outbound_client(Some("http://proxy.internal:8080"));
}

#[test]
fn test_build_outbound_client_with_invalid_proxy_falls_back_to_no_proxy() {
    // An invalid proxy URL must not panic; build_proxy() logs a warning and
    // build_outbound_client() proceeds without a proxy.
    let _client = build_outbound_client(Some("not a valid url"));
}

#[test]
fn test_mask_proxy_url_with_credentials() {
    assert_eq!(
        mask_proxy_url("http://user:pass@proxy.internal:8080"),
        "http://***:***@proxy.internal:8080"
    );
}

#[test]
fn test_mask_proxy_url_without_credentials() {
    assert_eq!(
        mask_proxy_url("http://proxy.internal:8080"),
        "http://proxy.internal:8080"
    );
}

#[test]
fn test_mask_proxy_url_https_with_credentials() {
    assert_eq!(
        mask_proxy_url("https://admin:secret123@proxy:3128"),
        "https://***:***@proxy:3128"
    );
}

#[test]
fn test_mask_proxy_url_with_path() {
    assert_eq!(
        mask_proxy_url("http://u:p@proxy:8080/path"),
        "http://***:***@proxy:8080/path"
    );
}

#[test]
fn redact_url_strips_license_key_query() {
    let url = "https://collector.newrelic.com/agent_listener/invoke_raw_method?marshal_format=json&method=connect&license_key=NRAK-SECRET123&run_id=42";
    let redacted = redact_url(url);
    assert_eq!(
        redacted,
        "https://collector.newrelic.com/agent_listener/invoke_raw_method"
    );
    // The secret must not survive redaction.
    assert!(!redacted.contains("license_key"));
    assert!(!redacted.contains("NRAK-SECRET123"));
}

#[test]
fn redact_url_keeps_url_without_query() {
    let url = "https://collector.newrelic.com/agent_listener/invoke_raw_method";
    assert_eq!(redact_url(url), url);
}

#[test]
fn redact_url_strips_fragment_too() {
    assert_eq!(
        redact_url("https://host/path#section?license_key=KEY"),
        "https://host/path"
    );
}

#[test]
fn test_build_proxy_valid_url() {
    let proxy = build_proxy("http://proxy:8080");
    assert!(proxy.is_some());
}

#[test]
fn test_build_proxy_empty_url() {
    // Empty string is the one case reqwest::Proxy::all() rejects
    let proxy = build_proxy("");
    assert!(proxy.is_none());
}

#[test]
fn test_mask_proxy_url_never_leaks_credentials() {
    let test_cases = vec![
        ("http://myuser:mypassword@proxy:8080", "myuser", "mypassword"),
        ("https://admin:s3cret!@proxy.internal:3128", "admin", "s3cret!"),
        ("http://deploy-bot:token%40abc@corp-proxy:80/path", "deploy-bot", "token%40abc"),
        ("socks5://svc_account:P@$$w0rd@socks-proxy:1080", "svc_account", "P@$$w0rd"),
    ];

    for (url, username, password) in test_cases {
        let masked = mask_proxy_url(url);
        assert!(!masked.contains(username),
            "Credential leak: masked URL '{}' still contains the original username", masked);
        assert!(!masked.contains(password),
            "Credential leak: masked URL '{}' still contains the original password", masked);
        // Host must still be visible for debugging
        assert!(masked.contains("@"), "Masked URL should preserve @ separator: {}", masked);
        assert!(masked.contains("***:***"), "Masked URL should contain '***:***': {}", masked);
    }
}

#[test]
fn test_send_error_display_network() {
    let inner = reqwest::Client::builder()
        .build().unwrap()
        .get("http://[::1]:1/bad")
        .header("bad\nheader", "value")
        .build()
        .unwrap_err();
    let err = SendError::Network(inner);
    let display = format!("{}", err);
    assert!(display.starts_with("network error:"), "got: {}", display);
}

#[test]
fn test_send_error_display_server_exhausted() {
    let err = SendError::ServerExhausted { status: 503 };
    assert_eq!(format!("{}", err), "server error 503 after max retries");
}

#[test]
fn test_send_error_display_client_rejected() {
    let err = SendError::ClientRejected { status: 413 };
    assert_eq!(format!("{}", err), "client error 413 (not retryable)");
}

#[test]
fn test_send_error_debug_impl() {
    let err = SendError::ServerExhausted { status: 500 };
    let debug = format!("{:?}", err);
    assert!(debug.contains("ServerExhausted"), "got: {}", debug);
    assert!(debug.contains("500"), "got: {}", debug);
}

// ========================================================================
// NEW_RELIC_DATA_COLLECTION_TIMEOUT / NEW_RELIC_HTTP_TIMEOUT
// ========================================================================

#[test]
fn test_get_growing_backoff_delay_schedule() {
    // 200ms for attempts 1-3, doubling every 3 attempts, capped at 3s.
    for attempt in 1..=3 {
        assert_eq!(get_growing_backoff_delay(attempt), std::time::Duration::from_millis(200));
    }
    for attempt in 4..=6 {
        assert_eq!(get_growing_backoff_delay(attempt), std::time::Duration::from_millis(400));
    }
    for attempt in 7..=9 {
        assert_eq!(get_growing_backoff_delay(attempt), std::time::Duration::from_millis(800));
    }
    for attempt in 10..=12 {
        assert_eq!(get_growing_backoff_delay(attempt), std::time::Duration::from_millis(1600));
    }
    // Stage caps at 4 (3000ms) from attempt 13 onward, including well past 20.
    for attempt in [13, 14, 15, 20, 100] {
        assert_eq!(get_growing_backoff_delay(attempt), std::time::Duration::from_millis(3000));
    }
}

#[test]
fn test_retry_allowed_none_budget_uses_fixed_count() {
    // Unset env var: unchanged fixed-retry-count behavior, budget/elapsed ignored.
    assert!(retry_allowed(0, std::time::Duration::from_secs(999), None, 3));
    assert!(retry_allowed(2, std::time::Duration::from_secs(999), None, 3));
    assert!(!retry_allowed(3, std::time::Duration::ZERO, None, 3));
}

#[test]
fn test_retry_allowed_some_budget_uses_elapsed_time() {
    let budget = Some(std::time::Duration::from_secs(10));
    // Under budget, few retries so far -> allowed regardless of max_retries.
    assert!(retry_allowed(5, std::time::Duration::from_secs(5), budget, 3));
    // Budget elapsed -> not allowed even with few retries.
    assert!(!retry_allowed(1, std::time::Duration::from_secs(10), budget, 3));
    assert!(!retry_allowed(1, std::time::Duration::from_secs(11), budget, 3));
}

#[test]
fn test_retry_allowed_some_budget_caps_at_20_attempts() {
    // 20-attempt safety net fires even when the time budget hasn't elapsed.
    let budget = Some(std::time::Duration::from_secs(999));
    assert!(retry_allowed(19, std::time::Duration::from_secs(1), budget, 3));
    assert!(!retry_allowed(20, std::time::Duration::from_secs(1), budget, 3));
    assert!(!retry_allowed(25, std::time::Duration::from_secs(1), budget, 3));
}

#[test]
fn test_summarize_response_body_extracts_title() {
    let body = "<html><head><title>503 Service Unavailable</title></head><body>...</body></html>";
    assert_eq!(summarize_response_body(body), "503 Service Unavailable");
}

#[test]
fn test_summarize_response_body_no_title_truncates() {
    let body = "a".repeat(500);
    let summary = summarize_response_body(&body);
    assert_eq!(summary.chars().count(), 200);
}

#[test]
fn test_summarize_response_body_short_plain_text_unchanged() {
    assert_eq!(summarize_response_body("  plain error  "), "plain error");
}
