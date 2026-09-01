use super::*;
use crate::config::deployment::TelemetryMode;
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
