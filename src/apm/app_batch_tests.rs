// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Combo tests for `NEW_RELIC_APM_BATCH_SIZE` exercising `ApmApp::process_agent_payload`
//! end-to-end (parse -> batch-or-send -> failure re-buffer), per `LMI_SUPPORT.md` §5-style
//! coverage. See `batch_buffer_tests.rs` for pure merge-logic unit tests.
//!
//! Network sends are exercised against a deliberately-invalid `collector_host` (same
//! convention as `event_loop_lmi_tests.rs::build_fake_apm_app`), so every send fails
//! fast and deterministically — no live server or wiremock needed. This still fully
//! exercises the send-attempt vs. absorbed-into-batch distinction and the
//! failure-then-rebuffer path, which is what these tests verify.
//!
//! `process_pending_agent_payloads` (Normal Lambda's warm-start catch-up, and LMI's
//! heartbeat) is unchanged by this feature — it already loops and calls
//! `process_agent_payload` once per pending payload. Since batching lives entirely
//! inside `process_agent_payload`, exercising it directly here also proves the
//! "LMI heartbeat merges across pending requests" claim from the design doc without
//! needing to stand up `REQUEST_DATA` fixtures.

use super::*;
use base64::{engine::general_purpose, Engine as _};
use flate2::write::GzEncoder;
use flate2::Compression;
use serial_test::serial;
use std::io::Write;
use std::time::Duration;

/// Build a fake `ApmApp` whose `collector_host` makes every `send_apm_telemetry`
/// call fail immediately (malformed URL — `https://` gets prepended on top of an
/// already-`http://`-prefixed host, so `reqwest` rejects it before any I/O). Mirrors
/// `event_loop_lmi_tests.rs::build_fake_apm_app`.
fn build_fake_apm_app() -> ApmApp {
    ApmApp {
        run_id: "test-run-id".to_string(),
        entity_guid: "test-entity-guid".to_string(),
        collector_host: "http://unreachable.invalid.test".to_string(),
        license_key: "fake-license-key-for-unit-test".to_string(),
        metric_endpoint: "http://metric.invalid.test/metric/v1".to_string(),
        client: Client::builder()
            .timeout(Duration::from_millis(200))
            .build()
            .unwrap_or_default(),
        deployment: DeploymentContext::Normal {
            mode: crate::config::deployment::TelemetryMode::Apm,
        },
    }
}

/// Build a valid protocol-v2 wire payload carrying one `span_event_data` entry
/// (a `MERGEABLE_TYPES` member) tagged so contributors can be told apart in
/// assertions.
fn make_span_payload(tag: &str) -> Vec<u8> {
    let test_data = format!(
        r#"{{"span_event_data": [null, {{"reservoir_size": 1000, "events_seen": 1}}, [{{"tag": "{tag}"}}]]}}"#
    );

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(test_data.as_bytes())
        .expect("gzip write into an in-memory Vec cannot fail");
    let compressed = encoder.finish().expect("gzip finish into an in-memory Vec cannot fail");

    let encoded = general_purpose::STANDARD.encode(&compressed);
    format!(r#"["2", "NR_LAMBDA_MONITORING", "{encoded}"]"#).into_bytes()
}

/// Reset batch_buffer's global state via its public API only (no internal access
/// needed) — drain whatever's held, then set the batch size under test.
fn reset_batch_buffer(batch_size: usize) {
    let _ = crate::apm::batch_buffer::force_flush();
    crate::apm::batch_buffer::set_batch_size(batch_size);
}

#[tokio::test]
#[serial]
async fn normal_apm_batch_size_1_identical_to_today() {
    reset_batch_buffer(1);
    let app = build_fake_apm_app();

    let outcome_a = app
        .process_agent_payload(make_span_payload("b1-a"), "b1-req-a")
        .await
        .expect("parse must succeed");
    let outcome_b = app
        .process_agent_payload(make_span_payload("b1-b"), "b1-req-b")
        .await
        .expect("parse must succeed");

    // batch_size=1 never touches the batch buffer — every call sends (and, given
    // the unreachable host, fails) immediately and independently.
    assert_eq!(outcome_a, ProcessOutcome::Sent);
    assert_eq!(outcome_b, ProcessOutcome::Sent);

    let buffered = crate::apm::telemetry_buffer::buffered_request_ids();
    assert!(buffered.contains(&"b1-req-a".to_string()));
    assert!(buffered.contains(&"b1-req-b".to_string()));
}

#[tokio::test]
#[serial]
async fn normal_apm_batch_size_n_merges_and_failure_rebuffers_per_request() {
    reset_batch_buffer(3);
    let app = build_fake_apm_app();

    let outcome_a = app
        .process_agent_payload(make_span_payload("b3-a"), "b3-req-a")
        .await
        .expect("parse must succeed");
    let outcome_b = app
        .process_agent_payload(make_span_payload("b3-b"), "b3-req-b")
        .await
        .expect("parse must succeed");

    // Absorbed into the open batch — no network I/O, nothing buffered yet.
    assert_eq!(outcome_a, ProcessOutcome::Batched);
    assert_eq!(outcome_b, ProcessOutcome::Batched);
    let buffered_before = crate::apm::telemetry_buffer::buffered_request_ids();
    assert!(!buffered_before.contains(&"b3-req-a".to_string()));
    assert!(!buffered_before.contains(&"b3-req-b".to_string()));

    // Third call reaches batch_size=3 — one merged send is attempted (and fails,
    // since the host is unreachable), so the failure path re-buffers all three
    // ORIGINAL request_ids individually, not one synthetic merged id.
    let outcome_c = app
        .process_agent_payload(make_span_payload("b3-c"), "b3-req-c")
        .await
        .expect("parse must succeed");
    assert_eq!(outcome_c, ProcessOutcome::Sent);

    let buffered_after = crate::apm::telemetry_buffer::buffered_request_ids();
    assert!(buffered_after.contains(&"b3-req-a".to_string()));
    assert!(buffered_after.contains(&"b3-req-b".to_string()));
    assert!(buffered_after.contains(&"b3-req-c".to_string()));
}

#[tokio::test]
#[serial]
async fn shutdown_force_flushes_partial_batch() {
    reset_batch_buffer(5);
    let app = build_fake_apm_app();

    let outcome_a = app
        .process_agent_payload(make_span_payload("sd-a"), "shutdown-req-a")
        .await
        .expect("parse must succeed");
    let outcome_b = app
        .process_agent_payload(make_span_payload("sd-b"), "shutdown-req-b")
        .await
        .expect("parse must succeed");
    assert_eq!(outcome_a, ProcessOutcome::Batched);
    assert_eq!(outcome_b, ProcessOutcome::Batched);

    let buffered_before = crate::apm::telemetry_buffer::buffered_request_ids();
    assert!(!buffered_before.contains(&"shutdown-req-a".to_string()));
    assert!(!buffered_before.contains(&"shutdown-req-b".to_string()));

    // Only 2/5 of the batch threshold reached — without a force-flush this would
    // sit unsent. The shutdown path calls this unconditionally as the safety net.
    app.flush_batched_telemetry().await;

    let buffered_after = crate::apm::telemetry_buffer::buffered_request_ids();
    assert!(
        buffered_after.contains(&"shutdown-req-a".to_string()),
        "partial batch must not be silently dropped at shutdown"
    );
    assert!(buffered_after.contains(&"shutdown-req-b".to_string()));
}

fn make_metric_only_payload(call_count: i64) -> Vec<u8> {
    let test_data = format!(
        r#"{{"metric_data": [null, 1000.0, 1001.0, [[{{"name": "Custom/Test", "scope": ""}}, [{call_count}, 1.0, 1.0, 1.0, 1.0, 1.0]]]]}}"#
    );
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(test_data.as_bytes())
        .expect("gzip write into an in-memory Vec cannot fail");
    let compressed = encoder.finish().expect("gzip finish into an in-memory Vec cannot fail");
    let encoded = general_purpose::STANDARD.encode(&compressed);
    format!(r#"["2", "NR_LAMBDA_MONITORING", "{encoded}"]"#).into_bytes()
}

#[tokio::test]
#[serial]
async fn metric_data_batches_with_other_types_not_sent_unconditionally() {
    reset_batch_buffer(3);
    let app = build_fake_apm_app();

    // metric_data-only invocations now go through the SAME batchable path as
    // event-list types — merged via real per-metric stat aggregation, not always
    // sent immediately. First two calls must be absorbed (Batched), not Sent.
    let outcome_a = app
        .process_agent_payload(make_metric_only_payload(1), "metric-req-a")
        .await
        .expect("parse must succeed");
    let outcome_b = app
        .process_agent_payload(make_metric_only_payload(2), "metric-req-b")
        .await
        .expect("parse must succeed");
    assert_eq!(outcome_a, ProcessOutcome::Batched);
    assert_eq!(outcome_b, ProcessOutcome::Batched);

    let buffered_before = crate::apm::telemetry_buffer::buffered_request_ids();
    assert!(!buffered_before.contains(&"metric-req-a".to_string()));
    assert!(!buffered_before.contains(&"metric-req-b".to_string()));

    // Third call reaches batch_size=3 — one merged metric_data send is attempted
    // (and fails, given the unreachable host), re-buffering all three original
    // request_ids individually.
    let outcome_c = app
        .process_agent_payload(make_metric_only_payload(3), "metric-req-c")
        .await
        .expect("parse must succeed");
    assert_eq!(outcome_c, ProcessOutcome::Sent);

    let buffered_after = crate::apm::telemetry_buffer::buffered_request_ids();
    assert!(buffered_after.contains(&"metric-req-a".to_string()));
    assert!(buffered_after.contains(&"metric-req-b".to_string()));
    assert!(buffered_after.contains(&"metric-req-c".to_string()));
}

#[tokio::test]
#[serial]
async fn log_event_data_still_sent_unconditionally_even_when_batch_size_is_high() {
    reset_batch_buffer(5);
    let app = build_fake_apm_app();

    let test_data = r#"{"log_event_data": [{"common": {"attributes": {}}, "logs": []}]}"#;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(test_data.as_bytes())
        .expect("gzip write into an in-memory Vec cannot fail");
    let compressed = encoder.finish().expect("gzip finish into an in-memory Vec cannot fail");
    let encoded = general_purpose::STANDARD.encode(&compressed);
    let payload = format!(r#"["2", "NR_LAMBDA_MONITORING", "{encoded}"]"#).into_bytes();

    // log_event_data is still excluded from MERGEABLE_TYPES (unverified merge
    // safety) — always sent immediately regardless of NEW_RELIC_APM_BATCH_SIZE.
    let outcome = app
        .process_agent_payload(payload, "log-only-req")
        .await
        .expect("parse must succeed");
    assert_eq!(outcome, ProcessOutcome::Sent);

    let buffered = crate::apm::telemetry_buffer::buffered_request_ids();
    assert!(buffered.contains(&"log-only-req".to_string()));
}
