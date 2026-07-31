// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
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
