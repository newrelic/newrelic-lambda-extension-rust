use super::*;
use crate::config::deployment::TelemetryMode;
use crate::telemetry::managed_instance::ManagedInstanceMetadata;
use anyhow::anyhow;
use serial_test::serial;

#[test]
fn labels_include_islmi_true_on_lmi() {
    let labels = get_labels("arn:aws:lambda:us-east-1:123456789012:function:test", "python", DeploymentContext::Lmi);
    assert!(
        labels.iter().any(|l| l.label_type == "isLMI" && l.label_value == "true"),
        "expected an isLMI:true label on LMI, got {labels:?}"
    );
}

#[test]
fn labels_omit_islmi_on_normal_serverless() {
    let deployment = DeploymentContext::Normal { mode: TelemetryMode::Serverless };
    let labels = get_labels("arn:aws:lambda:us-east-1:123456789012:function:test", "python", deployment);
    assert!(
        !labels.iter().any(|l| l.label_type == "isLMI"),
        "isLMI label must be absent on Normal Lambda, got {labels:?}"
    );
}

#[test]
fn labels_omit_islmi_on_normal_apm() {
    let deployment = DeploymentContext::Normal { mode: TelemetryMode::Apm };
    let labels = get_labels("arn:aws:lambda:us-east-1:123456789012:function:test", "python", deployment);
    assert!(
        !labels.iter().any(|l| l.label_type == "isLMI"),
        "isLMI label must be absent on Normal Lambda regardless of telemetry mode, got {labels:?}"
    );
}

#[test]
fn labels_on_lmi_have_exactly_one_more_than_normal() {
    // isLMI must be strictly additive: everything Normal Lambda sends
    // (aws.arn, isLambdaFunction, newrelic.extension.version, ...) still
    // goes out on LMI, plus exactly one new label.
    let arn = "arn:aws:lambda:us-east-1:123456789012:function:test";
    let normal = get_labels(arn, "python", DeploymentContext::Normal { mode: TelemetryMode::Apm });
    let lmi = get_labels(arn, "python", DeploymentContext::Lmi);

    assert_eq!(lmi.len(), normal.len() + 1, "LMI: {lmi:?}, Normal: {normal:?}");
    for label in &normal {
        assert!(
            lmi.iter().any(|l| l.label_type == label.label_type && l.label_value == label.label_value),
            "LMI labels are missing a label Normal Lambda sends: {} = {}",
            label.label_type,
            label.label_value
        );
    }
}

#[test]
fn labels_on_lmi_include_new_relic_labels_alongside_islmi() {
    // NEW_RELIC_LABELS (main) and isLMI (LMI) must coexist — neither feature
    // should silently overwrite the other under DeploymentContext::Lmi.
    let arn = "arn:aws:lambda:us-east-1:123456789012:function:test";
    let labels = get_labels(arn, "python", DeploymentContext::Lmi);

    assert!(
        labels.iter().any(|l| l.label_type == "isLMI" && l.label_value == "true"),
        "expected isLMI:true under LMI, got {labels:?}"
    );
    for (key, value) in crate::config::get_new_relic_labels() {
        assert!(
            labels.iter().any(|l| &l.label_type == key && &l.label_value == value),
            "expected NEW_RELIC_LABELS entry {key}={value} to survive under LMI, got {labels:?}"
        );
    }
}

#[test]
fn permanent_auth_error_detected_through_context_chain() {
    // Mirrors how try_connect wraps the error: `.context("PreConnect failed")`.
    let err = anyhow::Error::new(PermanentAuthError { status: 401 })
        .context("PreConnect failed");
    assert_eq!(is_permanent_auth_error(&err), Some(401));
}

#[test]
fn transient_error_is_not_permanent() {
    let err = anyhow!("Connect failed with HTTP 503 - service unavailable");
    assert_eq!(is_permanent_auth_error(&err), None);
}

#[test]
fn permanent_auth_error_display_has_no_secret() {
    let msg = PermanentAuthError { status: 403 }.to_string();
    assert!(msg.contains("403"));
    assert!(!msg.contains("license_key"));
}

#[test]
#[serial]
fn handshake_fatal_latch_roundtrips() {
    reset_handshake_fatal_for_test();
    assert!(!is_handshake_fatal());
    signal_handshake_fatal();
    assert!(is_handshake_fatal());
    reset_handshake_fatal_for_test();
    assert!(!is_handshake_fatal());
}

#[test]
#[serial]
fn connect_stats_accumulate_and_reset() {
    reset_connect_stats();
    record_connect_cycle();
    record_connect_attempt();
    record_connect_attempt();
    record_failure_reason("HTTP 503");
    assert_eq!(connect_cycles(), 1);
    assert_eq!(connect_attempts_total(), 2);
    assert_eq!(last_failure_reason().as_deref(), Some("HTTP 503"));
    // A successful connect resets the disconnected-streak diagnostics.
    reset_connect_stats();
    assert_eq!(connect_cycles(), 0);
    assert_eq!(connect_attempts_total(), 0);
    assert_eq!(last_failure_reason(), None);
}

#[test]
fn http_failure_reason_uses_api_body_and_truncates() {
    // Empty body → just the code.
    assert_eq!(http_failure_reason(503, "   "), "HTTP 503");
    // Real collector message is surfaced (trimmed), not a hardcoded phrase.
    assert_eq!(
        http_failure_reason(401, "  Invalid license key.  "),
        "HTTP 401: Invalid license key."
    );
    // A verbose body is truncated so it can't bloat the log line.
    let long = "x".repeat(500);
    let r = http_failure_reason(500, &long);
    assert!(r.starts_with("HTTP 500: "));
    assert!(r.len() <= "HTTP 500: ".len() + 300);
}

#[test]
fn compress_inline_roundtrips_via_gzip_decoder() {
    use std::io::Read;

    let original = b"the quick brown fox jumps over the lazy dog ".repeat(50);
    let compressed = compress_inline(&original).expect("compression should succeed");

    let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).expect("decompression should succeed");

    assert_eq!(decompressed, original);
}

#[test]
fn compress_inline_shrinks_repetitive_data() {
    let original = vec![b'a'; 10_000];
    let compressed = compress_inline(&original).expect("compression should succeed");
    assert!(compressed.len() < original.len());
}

#[test]
fn compress_inline_handles_empty_input() {
    let compressed = compress_inline(&[]).expect("compression of empty input should succeed");
    assert!(!compressed.is_empty(), "gzip stream still has header/footer bytes");
}

// ── get_labels: lambda.runtime.version branch ────────────────────────────────

#[test]
#[serial]
fn labels_include_runtime_version_when_execution_env_provides_detail() {
    // AWS_EXECUTION_ENV=AWS_Lambda_python3.11 → get_runtime_version() returns "python3.11"
    // which is longer than "python", so the label must be emitted.
    let prev = std::env::var("AWS_EXECUTION_ENV").ok();
    std::env::set_var("AWS_EXECUTION_ENV", "AWS_Lambda_python3.11");

    let labels = get_labels("arn:aws:lambda:us-east-1:123:function:fn", "python", DeploymentContext::Normal { mode: TelemetryMode::Apm });

    match prev {
        Some(v) => std::env::set_var("AWS_EXECUTION_ENV", v),
        None => std::env::remove_var("AWS_EXECUTION_ENV"),
    }

    assert!(
        labels.iter().any(|l| l.label_type == "lambda.runtime.version" && l.label_value == "python3.11"),
        "expected lambda.runtime.version=python3.11, got {labels:?}"
    );
}

#[test]
#[serial]
fn labels_omit_runtime_version_when_execution_env_is_absent() {
    let prev = std::env::var("AWS_EXECUTION_ENV").ok();
    std::env::remove_var("AWS_EXECUTION_ENV");

    let labels = get_labels("arn:aws:lambda:us-east-1:123:function:fn", "unknown", DeploymentContext::Normal { mode: TelemetryMode::Apm });

    match prev {
        Some(v) => std::env::set_var("AWS_EXECUTION_ENV", v),
        None => std::env::remove_var("AWS_EXECUTION_ENV"),
    }

    assert!(
        !labels.iter().any(|l| l.label_type == "lambda.runtime.version"),
        "lambda.runtime.version must be absent when runtime is 'unknown', got {labels:?}"
    );
}

// ── preconnect / connect: network error paths ─────────────────────────────────
// The success and HTTP-response paths (200/401/403/5xx) require intercepting
// HTTPS traffic. Without a test-only hook in production code, those paths are
// covered by integration tests against a real or containerised collector.
// Port 1 always refuses at the TCP level (before TLS), so connection-error
// handling is fully exercisable here without touching production code.

fn normal_apm() -> DeploymentContext {
    DeploymentContext::Normal { mode: TelemetryMode::Apm }
}

#[tokio::test]
#[serial]
async fn preconnect_connection_refused_records_failure_reason() {
    reset_connect_stats();
    let client = reqwest::Client::new();
    let err = preconnect(&client, "test-key", "127.0.0.1:1", 5).await.unwrap_err();

    assert!(is_permanent_auth_error(&err).is_none());
    let reason = last_failure_reason().expect("failure reason must be recorded on connection error");
    assert!(
        reason.contains("connection error") || reason.contains("PreConnect"),
        "unexpected reason: {reason}"
    );
}

#[tokio::test]
#[serial]
async fn connect_connection_refused_records_failure_reason() {
    reset_connect_stats();
    let client = reqwest::Client::new();
    let err = connect(
        &client, "test-key", "127.0.0.1:1",
        "fn", "arn:test", "123", "us-east-1", "1", "python", "1.0.0", 5, None, normal_apm(),
    ).await.unwrap_err();

    assert!(is_permanent_auth_error(&err).is_none());
    let reason = last_failure_reason().expect("failure reason must be recorded on connection error");
    assert!(
        reason.contains("connection error") || reason.contains("Connect"),
        "unexpected reason: {reason}"
    );
}

#[tokio::test]
#[serial]
async fn connect_with_lmi_metadata_covers_some_branch() {
    // Port 1 → ECONNREFUSED. The test's only goal is to enter the
    // `Some(meta) => (Some(meta.instance_id), meta.instance_max_memory)` arm,
    // which is skipped by every other test that passes `None` for lmi_metadata.
    reset_connect_stats();
    let client = reqwest::Client::new();
    let meta = ManagedInstanceMetadata {
        instance_id: "lmi-host-42".into(),
        instance_max_memory: Some(2_147_483_648),
    };
    let result = connect(
        &client, "test-key", "127.0.0.1:1",
        "fn", "arn:test", "123", "us-east-1", "1", "python", "1.0.0", 5,
        Some(meta), normal_apm(),
    ).await;
    assert!(result.is_err());
}

#[tokio::test]
#[serial]
async fn preconnect_timeout_covers_is_timeout_branch() {
    // A TCP listener that accepts the connection but never sends any data
    // forces the TLS handshake to stall until the 1-second request timeout
    // fires — exercising the `is_timeout()` arm in preconnect's map_err.
    reset_connect_stats();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    // Intentional: the spawned task holds the connection open with a 30s sleep so
    // the TLS handshake stalls and the client-side 1s timeout fires. The runtime
    // drops the task when the test-scoped tokio runtime is torn down — do not
    // await or abort it, as that would defeat the purpose of the stall.
    tokio::spawn(async move {
        if let Ok((_stream, _)) = listener.accept().await {
            tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
        }
    });
    let client = reqwest::Client::new();
    let result = preconnect(&client, "test-key", &format!("127.0.0.1:{port}"), 1).await;
    assert!(result.is_err());
    let reason = last_failure_reason().expect("failure reason must be recorded");
    assert!(
        reason.starts_with("PreConnect"),
        "unexpected reason: {reason}"
    );
}

#[tokio::test]
#[serial]
async fn connect_timeout_covers_is_timeout_branch() {
    // Same stall pattern as preconnect_timeout — exercises the `is_timeout()`
    // arm in connect's map_err.
    reset_connect_stats();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    // Intentional: see preconnect_timeout_covers_is_timeout_branch above —
    // same stall pattern; the task is dropped by the test runtime, not awaited.
    tokio::spawn(async move {
        if let Ok((_stream, _)) = listener.accept().await {
            tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
        }
    });
    let client = reqwest::Client::new();
    let result = connect(
        &client, "test-key", &format!("127.0.0.1:{port}"),
        "fn", "arn:test", "123", "us-east-1", "1", "python", "1.0.0", 1,
        None, normal_apm(),
    ).await;
    assert!(result.is_err());
    let reason = last_failure_reason().expect("failure reason must be recorded");
    assert!(
        reason.starts_with("Connect"),
        "unexpected reason: {reason}"
    );
}
