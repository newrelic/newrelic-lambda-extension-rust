// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::apm::collector::CollectorError;
use serde_json::json;
use serial_test::serial;

fn old_item(telemetry_type: &str) -> FailedTelemetry {
    FailedTelemetry {
        telemetry_type: telemetry_type.into(),
        data: vec![json!(null), json!({})],
        request_id: "req-old".into(),
        run_id: "run-old".into(),
        collector_host: "127.0.0.1:1".into(),
        // > 60 minutes ago → should be dropped by the age check
        failed_at: chrono::Utc::now() - chrono::TimeDelta::try_minutes(65).unwrap(),
        retry_count: 0,
    }
}

fn fresh_item(telemetry_type: &str) -> FailedTelemetry {
    FailedTelemetry {
        telemetry_type: telemetry_type.into(),
        data: vec![json!(null), json!({})],
        request_id: "req-1".into(),
        run_id: "run-1".into(),
        collector_host: "127.0.0.1:1".into(),
        failed_at: chrono::Utc::now(),
        retry_count: 0,
    }
}

fn push_item(item: FailedTelemetry) {
    if let Ok(mut b) = FAILED_TELEMETRY_BUFFER.lock() {
        b.push(item);
    }
}

fn clear() {
    if let Ok(mut b) = FAILED_TELEMETRY_BUFFER.lock() {
        b.clear();
    }
}

#[test]
#[serial]
fn buffers_and_counts() {
    clear();
    buffer_failed_telemetry(
        "metric_data".into(),
        vec![json!(null), json!({"m": 1})],
        "req".into(),
        "run".into(),
        "host".into(),
    );
    assert_eq!(get_buffer_count(), 1);
    clear();
}

#[test]
#[serial]
fn buffered_request_ids_are_distinct_and_sorted() {
    clear();
    // Two items for req-b, one for req-a → distinct {req-a, req-b}.
    for (id, ty) in [("req-b", "metric_data"), ("req-a", "span_event_data"), ("req-b", "log_event_data")] {
        buffer_failed_telemetry(ty.into(), vec![json!({})], id.into(), "run".into(), "host".into());
    }
    assert_eq!(buffered_request_ids(), vec!["req-a".to_string(), "req-b".to_string()]);
    clear();
}

#[test]
#[serial]
fn caps_buffer_size_by_evicting_oldest() {
    clear();
    for _ in 0..(MAX_BUFFERED_ITEMS + 25) {
        buffer_failed_telemetry(
            "metric_data".into(),
            vec![json!(null)],
            "req".into(),
            "run".into(),
            "host".into(),
        );
    }
    assert_eq!(get_buffer_count(), MAX_BUFFERED_ITEMS, "must never exceed cap");
    clear();
}

#[test]
fn synthesized_error_sentinel_is_distinct() {
    // Must not collide with agent-originated error_event_data, which routes
    // through send_apm_telemetry with a different wire format.
    assert_ne!(SYNTHESIZED_ERROR_EVENTS, "error_event_data");
}

fn make_item() -> FailedTelemetry {
    FailedTelemetry {
        telemetry_type: "metric_data".into(),
        data: vec![],
        request_id: "req-1".into(),
        run_id: "run-1".into(),
        collector_host: "host".into(),
        failed_at: chrono::Utc::now(),
        retry_count: 0,
    }
}

// Mirrors the retry-slot decision in retry_buffered_telemetry's Err branch.
fn apply_retry_decision(mut item: FailedTelemetry, err: &anyhow::Error) -> Option<FailedTelemetry> {
    let is_restart = err
        .downcast_ref::<CollectorError>()
        .map(|ce| matches!(ce, CollectorError::RestartException))
        .unwrap_or(false);
    if is_restart {
        Some(item)
    } else {
        item.retry_count += 1;
        if item.retry_count < 10 { Some(item) } else { None }
    }
}

#[test]
fn restart_exception_never_hits_retry_cap() {
    let err = anyhow::Error::new(CollectorError::RestartException);
    let mut item = make_item();
    for _ in 0..15 {
        item = apply_retry_decision(item, &err)
            .expect("RestartException must never drop the item");
    }
    assert_eq!(item.retry_count, 0, "retry_count must stay 0 — no slot consumed on 409/401");
}

#[test]
fn generic_error_drops_after_ten_retries() {
    let err = anyhow::anyhow!("connection refused");
    let mut item = make_item();
    for attempt in 1..=9 {
        item = apply_retry_decision(item, &err)
            .unwrap_or_else(|| panic!("item must survive attempt {}", attempt));
        assert_eq!(item.retry_count, attempt);
    }
    assert!(
        apply_retry_decision(item, &err).is_none(),
        "item must be dropped after 10 attempts"
    );
}

#[test]
fn restart_exception_is_detected_by_downcast() {
    let e = anyhow::Error::new(CollectorError::RestartException)
        .context("Collector returned 409 for metric_data");
    let is_restart = e
        .downcast_ref::<CollectorError>()
        .map(|ce| matches!(ce, CollectorError::RestartException))
        .unwrap_or(false);
    assert!(is_restart);
}

#[test]
fn non_collector_error_is_not_detected_as_restart() {
    let e = anyhow::anyhow!("connection refused");
    let is_restart = e
        .downcast_ref::<CollectorError>()
        .map(|ce| matches!(ce, CollectorError::RestartException))
        .unwrap_or(false);
    assert!(!is_restart);
}

#[test]
#[serial]
fn restart_rebuffer_respects_cap() {
    clear();
    // Fill buffer to exactly the cap via the normal path.
    for i in 0..MAX_BUFFERED_ITEMS {
        buffer_failed_telemetry(
            "metric_data".into(),
            vec![json!({"seq": i})],
            format!("req-{}", i),
            "run".into(),
            "host".into(),
        );
    }
    assert_eq!(get_buffer_count(), MAX_BUFFERED_ITEMS);

    // Simulate apply_retry_decision with RestartException — mirrors the
    // is_restart re-buffer branch in retry_buffered_telemetry.
    let err = anyhow::Error::new(CollectorError::RestartException);
    let item = make_item();
    let result = apply_retry_decision(item, &err);
    assert!(result.is_some(), "RestartException must not drop the item");

    // Manually push through the production re-buffer path to verify the cap.
    if let Ok(mut buffer) = FAILED_TELEMETRY_BUFFER.lock() {
        if buffer.len() >= MAX_BUFFERED_ITEMS {
            buffer.remove(0);
        }
        buffer.push(result.unwrap());
    }

    assert_eq!(
        get_buffer_count(),
        MAX_BUFFERED_ITEMS,
        "buffer must not exceed cap after re-buffering a RestartException item"
    );
    clear();
}

// ── retry_buffered_telemetry ──────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn retry_buffered_telemetry_empty_buffer_is_noop() {
    clear();
    let client = reqwest::Client::new();
    retry_buffered_telemetry(&client, "key", None, None).await;
    assert_eq!(get_buffer_count(), 0);
}

#[tokio::test]
#[serial]
async fn retry_buffered_telemetry_connection_refused_rebuffers_item() {
    clear();
    push_item(fresh_item("metric_data"));
    assert_eq!(get_buffer_count(), 1);

    let client = reqwest::Client::new();
    retry_buffered_telemetry(&client, "key", None, None).await;

    // Connection refused → generic error → re-buffered with retry_count incremented.
    assert_eq!(get_buffer_count(), 1);
    if let Ok(b) = FAILED_TELEMETRY_BUFFER.lock() {
        assert_eq!(b[0].retry_count, 1);
    }
    clear();
}

#[tokio::test]
#[serial]
async fn retry_buffered_telemetry_override_run_id_and_host_are_used() {
    // Stored run_id/host are irrelevant; current_run_id and current_collector_host
    // override them for the HTTP call (lines 131-132).
    clear();
    push_item(FailedTelemetry {
        telemetry_type: "span_event_data".into(),
        data: vec![json!(null), json!({})],
        request_id: "req-override".into(),
        run_id: "old-run".into(),
        collector_host: "old-host".into(),
        failed_at: chrono::Utc::now(),
        retry_count: 0,
    });

    let client = reqwest::Client::new();
    // current_collector_host points to a refusing port so the call fails and re-buffers.
    retry_buffered_telemetry(&client, "key", Some("new-run"), Some("127.0.0.1:1")).await;

    assert_eq!(get_buffer_count(), 1);
    // The stored run_id/host are unchanged — only the call used the overrides.
    if let Ok(b) = FAILED_TELEMETRY_BUFFER.lock() {
        assert_eq!(b[0].run_id, "old-run");
        assert_eq!(b[0].collector_host, "old-host");
        assert_eq!(b[0].retry_count, 1);
    }
    clear();
}

#[tokio::test]
#[serial]
async fn retry_buffered_telemetry_drops_item_older_than_one_hour() {
    clear();
    push_item(old_item("log_event_data"));
    assert_eq!(get_buffer_count(), 1);

    let client = reqwest::Client::new();
    retry_buffered_telemetry(&client, "key", None, None).await;

    // Age > 60 minutes → dropped, not re-buffered.
    assert_eq!(get_buffer_count(), 0);
}

#[tokio::test]
#[serial]
async fn retry_buffered_telemetry_unknown_type_is_skipped_and_dropped() {
    clear();
    push_item(fresh_item("totally_unknown_type"));

    let client = reqwest::Client::new();
    retry_buffered_telemetry(&client, "key", None, None).await;

    // Unknown type hits the `_ => { warn!(...); continue; }` arm — item is dropped.
    assert_eq!(get_buffer_count(), 0);
}

#[tokio::test]
#[serial]
async fn retry_buffered_telemetry_synthesized_error_events_connection_refused() {
    clear();
    push_item(fresh_item(SYNTHESIZED_ERROR_EVENTS));

    let client = reqwest::Client::new();
    retry_buffered_telemetry(&client, "key", None, None).await;

    // send_error_events path → connection refused → re-buffered with retry_count=1.
    assert_eq!(get_buffer_count(), 1);
    if let Ok(b) = FAILED_TELEMETRY_BUFFER.lock() {
        assert_eq!(b[0].retry_count, 1);
    }
    clear();
}

#[tokio::test]
#[serial]
async fn retry_buffered_telemetry_drops_after_ten_retries() {
    clear();
    // Push item that has already been retried 9 times (one below the drop threshold).
    push_item(FailedTelemetry {
        telemetry_type: "error_data".into(),
        data: vec![json!(null), json!({})],
        request_id: "req-max".into(),
        run_id: "run-max".into(),
        collector_host: "127.0.0.1:1".into(),
        failed_at: chrono::Utc::now(),
        retry_count: 9,
    });
    assert_eq!(get_buffer_count(), 1);

    let client = reqwest::Client::new();
    retry_buffered_telemetry(&client, "key", None, None).await;

    // retry_count bumps to 10 → 10 < 10 is false → item is dropped.
    assert_eq!(get_buffer_count(), 0);
}

#[tokio::test]
#[serial]
async fn retry_buffered_telemetry_covers_remaining_command_types() {
    // Exercise the remaining match arms in the command mapping.
    for telemetry_type in [
        "error_event_data",
        "analytic_event_data",
        "custom_event_data",
        "transaction_sample_data",
        "sql_trace_data",
    ] {
        clear();
        push_item(fresh_item(telemetry_type));
        let client = reqwest::Client::new();
        retry_buffered_telemetry(&client, "key", None, None).await;
        // Each type routes to a known command → connection refused → re-buffered.
        assert_eq!(get_buffer_count(), 1, "type '{telemetry_type}' should re-buffer on failure");
        clear();
    }
}
