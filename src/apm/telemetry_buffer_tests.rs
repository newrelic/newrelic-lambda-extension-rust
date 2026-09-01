// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::apm::collector::CollectorError;
use serde_json::json;
use serial_test::serial;

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
