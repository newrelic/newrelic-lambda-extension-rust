// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;

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
