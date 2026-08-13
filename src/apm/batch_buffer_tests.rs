// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use serde_json::json;
use serial_test::serial;
use std::collections::HashMap;
use std::thread;

fn clear() {
    if let Ok(mut state) = BATCH.lock() {
        state.entries.clear();
        state.request_count = 0;
    }
    set_batch_size(1);
}

fn reservoir_payload(events: Vec<Value>, reservoir_size: u64, events_seen: u64) -> Vec<Value> {
    vec![
        Value::Null,
        json!({ "reservoir_size": reservoir_size, "events_seen": events_seen }),
        Value::Array(events),
    ]
}

fn no_metadata_payload(events: Vec<Value>) -> Vec<Value> {
    vec![Value::Null, Value::Array(events)]
}

#[test]
#[serial]
fn batch_size_1_flushes_immediately() {
    clear();
    let mut map = HashMap::new();
    map.insert(
        "analytic_event_data".to_string(),
        reservoir_payload(vec![json!({"a": 1})], 100, 1),
    );

    let flushed = add_request_and_maybe_flush("req-1", map, 1);
    assert!(flushed.is_some(), "batch_size=1 must flush on the very first call");
    clear();
}

#[test]
#[serial]
fn merges_n_requests_reservoir_types_sums_events_seen_passes_through_reservoir_size() {
    clear();
    let mut map1 = HashMap::new();
    map1.insert(
        "analytic_event_data".to_string(),
        reservoir_payload(vec![json!({"a": 1})], 100, 1),
    );
    let mut map2 = HashMap::new();
    map2.insert(
        "analytic_event_data".to_string(),
        reservoir_payload(vec![json!({"a": 2}), json!({"a": 3})], 100, 2),
    );

    assert!(add_request_and_maybe_flush("req-1", map1, 2).is_none());
    let flushed = add_request_and_maybe_flush("req-2", map2, 2);
    let flushed = flushed.expect("batch_size=2 must flush on the second call");

    assert_eq!(flushed.types.len(), 1);
    let merged = &flushed.types[0];
    assert_eq!(merged.telemetry_type, "analytic_event_data");
    assert_eq!(merged.merged_data[1]["reservoir_size"], json!(100));
    assert_eq!(merged.merged_data[1]["events_seen"], json!(3)); // 1 + 2
    let events = merged.merged_data[2].as_array().expect("events array");
    assert_eq!(events.len(), 3);
    assert_eq!(events[0], json!({"a": 1}));
    assert_eq!(events[1], json!({"a": 2}));
    assert_eq!(events[2], json!({"a": 3}));
    assert_eq!(merged.contributors.len(), 2);
    clear();
}

#[test]
#[serial]
fn merges_n_requests_no_metadata_types() {
    clear();
    let mut map1 = HashMap::new();
    map1.insert(
        "error_data".to_string(),
        no_metadata_payload(vec![json!(["err1"])]),
    );
    let mut map2 = HashMap::new();
    map2.insert(
        "transaction_sample_data".to_string(),
        no_metadata_payload(vec![json!(["sample1"])]),
    );

    assert!(add_request_and_maybe_flush("req-1", map1, 2).is_none());
    let flushed = add_request_and_maybe_flush("req-2", map2, 2).expect("must flush at size 2");

    assert_eq!(flushed.types.len(), 2);
    for merged in &flushed.types {
        // Each type in this test only had one contributor, so the merge is just
        // that contributor's events, but shaped correctly with no metadata slot.
        assert_eq!(merged.merged_data.len(), 2);
        assert_eq!(merged.merged_data[0], Value::Null);
        assert!(merged.merged_data[1].is_array());
    }
    clear();
}

#[test]
#[serial]
fn sql_trace_data_concatenates_flat() {
    clear();
    let mut map1 = HashMap::new();
    map1.insert("sql_trace_data".to_string(), vec![json!(["trace1"])]);
    let mut map2 = HashMap::new();
    map2.insert(
        "sql_trace_data".to_string(),
        vec![json!(["trace2"]), json!(["trace3"])],
    );

    assert!(add_request_and_maybe_flush("req-1", map1, 2).is_none());
    let flushed = add_request_and_maybe_flush("req-2", map2, 2).expect("must flush at size 2");

    assert_eq!(flushed.types.len(), 1);
    let merged = &flushed.types[0];
    assert_eq!(merged.telemetry_type, "sql_trace_data");
    assert_eq!(
        merged.merged_data,
        vec![json!(["trace1"]), json!(["trace2"]), json!(["trace3"])]
    );
    clear();
}

#[test]
#[serial]
fn log_event_data_never_enters_the_batch_buffer() {
    assert!(MERGEABLE_TYPES.contains(&"metric_data"));
    assert!(!MERGEABLE_TYPES.contains(&"log_event_data"));

    clear();
    let mut map = HashMap::new();
    map.insert(
        "log_event_data".to_string(),
        vec![json!({"common": {}, "logs": []})],
    );

    // batch_size=5 so this call alone can't trigger a flush — if the type were
    // accidentally routed into the buffer, `request_count` would still be 1 < 5 and
    // this assertion wouldn't catch it, so we additionally assert directly that no
    // entry was recorded for it.
    assert!(add_request_and_maybe_flush("req-1", map, 5).is_none());
    if let Ok(state) = BATCH.lock() {
        assert!(!state.entries.contains_key("log_event_data"));
    }
    clear();
}

fn metric_data_payload(start: f64, end: f64, metrics: Vec<Value>) -> Vec<Value> {
    vec![Value::Null, json!(start), json!(end), Value::Array(metrics)]
}

fn metric_entry(name: &str, scope: &str, stats: [f64; 6]) -> Value {
    json!([
        { "name": name, "scope": scope },
        [stats[0], stats[1], stats[2], stats[3], stats[4], stats[5]]
    ])
}

/// Verified against the Python agent's `TimeStats.merge_stats` semantics
/// (`newrelic/core/stats_engine.py`): call_count/total_call_time/
/// total_exclusive_call_time/sum_of_squares sum; min_call_time is the min of the
/// two EXCEPT when the accumulator's call_count was still zero (then take the
/// other side's min verbatim); max_call_time is the max.
#[test]
#[serial]
fn merges_metric_data_via_real_stat_aggregation_not_concatenation() {
    clear();
    let map1 = {
        let mut m = HashMap::new();
        m.insert(
            "metric_data".to_string(),
            metric_data_payload(
                100.0,
                110.0,
                vec![metric_entry("HttpDispatcher", "", [1.0, 5.0, 5.0, 5.0, 5.0, 25.0])],
            ),
        );
        m
    };
    let map2 = {
        let mut m = HashMap::new();
        m.insert(
            "metric_data".to_string(),
            metric_data_payload(
                108.0,
                120.0,
                vec![
                    metric_entry("HttpDispatcher", "", [2.0, 3.0, 3.0, 1.0, 2.0, 5.0]),
                    metric_entry("Custom/Only/InSecond", "", [1.0, 9.0, 9.0, 9.0, 9.0, 81.0]),
                ],
            ),
        );
        m
    };

    assert!(add_request_and_maybe_flush("req-1", map1, 2).is_none());
    let flushed = add_request_and_maybe_flush("req-2", map2, 2).expect("must flush at size 2");

    assert_eq!(flushed.types.len(), 1);
    let merged = &flushed.types[0];
    assert_eq!(merged.telemetry_type, "metric_data");
    assert_eq!(merged.merged_data[0], Value::Null);
    // Harvest window widens to [min(starts), max(ends)].
    assert_eq!(merged.merged_data[1], json!(100.0));
    assert_eq!(merged.merged_data[2], json!(120.0));

    let metrics = merged.merged_data[3].as_array().expect("metrics array");
    assert_eq!(metrics.len(), 2, "one shared metric + one second-only metric");

    let http = metrics
        .iter()
        .find(|m| m[0]["name"] == "HttpDispatcher")
        .expect("HttpDispatcher present");
    // call_count: 1+2=3, total: 5+3=8, exclusive: 5+3=8, min: min(5,1)=1,
    // max: max(5,2)=5, sum_of_squares: 25+5=30.
    assert_eq!(http[1], json!([3, 8.0, 8.0, 1.0, 5.0, 30.0]));

    let custom_only = metrics
        .iter()
        .find(|m| m[0]["name"] == "Custom/Only/InSecond")
        .expect("second-only metric present, not dropped");
    // Only one contributor ever had this metric — passed through unmodified
    // (had_prior_samples is false, so min is taken verbatim, not min(0, 9)).
    assert_eq!(custom_only[1], json!([1, 9.0, 9.0, 9.0, 9.0, 81.0]));

    clear();
}

#[test]
#[serial]
fn partial_batch_holds_until_size_reached() {
    clear();
    for i in 0..4 {
        let mut map = HashMap::new();
        map.insert(
            "span_event_data".to_string(),
            reservoir_payload(vec![json!({"i": i})], 1000, 1),
        );
        let flushed = add_request_and_maybe_flush(&format!("req-{i}"), map, 5);
        assert!(flushed.is_none(), "must not flush before batch_size is reached");
    }
    if let Ok(state) = BATCH.lock() {
        assert_eq!(state.request_count, 4);
        assert_eq!(
            state
                .entries
                .get("span_event_data")
                .map(|e| e.0.len())
                .unwrap_or(0),
            4
        );
    }
    clear();
}

#[test]
#[serial]
fn force_flush_drains_regardless_of_count() {
    clear();
    for i in 0..2 {
        let mut map = HashMap::new();
        map.insert(
            "custom_event_data".to_string(),
            reservoir_payload(vec![json!({"i": i})], 1200, 1),
        );
        assert!(add_request_and_maybe_flush(&format!("req-{i}"), map, 5).is_none());
    }

    let flushed = force_flush().expect("force_flush must drain a partial batch");
    assert_eq!(flushed.types.len(), 1);
    assert_eq!(flushed.types[0].contributors.len(), 2);

    // Buffer is empty after force_flush.
    assert!(force_flush().is_none());
    clear();
}

#[test]
#[serial]
fn failure_split_preserves_original_per_request_arrays_for_rebuffering() {
    clear();
    let original_1 = reservoir_payload(vec![json!({"a": 1})], 100, 1);
    let original_2 = reservoir_payload(vec![json!({"a": 2})], 100, 1);

    let mut map1 = HashMap::new();
    map1.insert("analytic_event_data".to_string(), original_1.clone());
    let mut map2 = HashMap::new();
    map2.insert("analytic_event_data".to_string(), original_2.clone());

    assert!(add_request_and_maybe_flush("req-1", map1, 2).is_none());
    let flushed = add_request_and_maybe_flush("req-2", map2, 2).expect("must flush at size 2");

    let merged = &flushed.types[0];
    assert_eq!(merged.contributors[0], ("req-1".to_string(), original_1));
    assert_eq!(merged.contributors[1], ("req-2".to_string(), original_2));
    clear();
}

#[test]
#[serial]
fn concurrent_add_request_from_two_callers_is_serialized() {
    clear();

    let handles: Vec<_> = (0..2)
        .map(|i| {
            thread::spawn(move || {
                let mut map = HashMap::new();
                map.insert(
                    "error_event_data".to_string(),
                    reservoir_payload(vec![json!({"i": i})], 100, 1),
                );
                add_request_and_maybe_flush(&format!("req-{i}"), map, 2)
            })
        })
        .collect();

    let results: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().unwrap_or(None))
        .collect();

    let flushed_count = results.iter().filter(|r| r.is_some()).count();
    assert_eq!(
        flushed_count, 1,
        "exactly one of the two concurrent calls must observe the batch reaching size 2"
    );
    clear();
}
