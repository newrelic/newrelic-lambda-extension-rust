// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Cross-invocation batching for agent-payload telemetry in APM mode.
//!
//! Every invocation's agent payload normally triggers one HTTP POST per telemetry
//! type to the APM collector. When `NEW_RELIC_APM_BATCH_SIZE` > 1, this module holds
//! several consecutive invocations' event-list telemetry in memory and merges it into
//! one POST per type once `batch_size` invocations have contributed — trading a small,
//! bounded delivery-latency risk (capped by `batch_size`, with the shutdown drain path
//! as the safety net for a partial batch) for fewer collector round trips.
//!
//! Global static, not a field on `ApmApp`: `ApmApp` is wholesale-replaced on APM
//! reconnect, so a buffer living on it would silently drop everything on reconnect —
//! same reasoning as `FAILED_TELEMETRY_BUFFER` in `telemetry_buffer.rs`.

use once_cell::sync::Lazy;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tracing::error;

/// Telemetry types safe to merge across invocations. `metric_data` is included —
/// merged via REAL per-metric stat aggregation (see [`merge_metric_data_shaped`]),
/// not concatenation. Deliberately excludes:
///
/// - `log_event_data`: confirmed (via the Node agent's `ServerlessCollector`/
///   `LogAggregator`) to be `[{common: {attributes}, logs: [...]}]` — not the
///   `[placeholder, {reservoir}, [events]]` shape the other event types use, and
///   with no `run_id` placeholder at all. Not verified safe to merge across
///   invocations (e.g. whether `common.attributes` is invocation-invariant).
///   Always sent immediately, unbatched.
pub const MERGEABLE_TYPES: &[&str] = &[
    "metric_data",
    "analytic_event_data",
    "span_event_data",
    "error_event_data",
    "custom_event_data",
    "transaction_sample_data",
    "error_data",
    "sql_trace_data",
];

/// `metric_data`'s shape: `[placeholder, start_epoch, end_epoch, [[metric_id, stats], ...]]`.
/// Merged via real per-metric stat aggregation — see [`merge_metric_data_shaped`].
const METRIC_SHAPED_TYPES: &[&str] = &["metric_data"];

/// Event-list types shaped `[placeholder, {reservoir_size, events_seen}, [events]]`.
const RESERVOIR_SHAPED_TYPES: &[&str] = &[
    "analytic_event_data",
    "span_event_data",
    "error_event_data",
    "custom_event_data",
];

/// Event-list types shaped `[placeholder, [events]]` — no metadata object.
const NO_METADATA_SHAPED_TYPES: &[&str] = &["transaction_sample_data", "error_data"];

/// One telemetry type's contributions to the open batch, in arrival order.
/// Storing the original per-request arrays (rather than a pre-merged running
/// total) means the merge is computed once, lazily, at flush time, and a failed
/// send can re-buffer the exact original per-request data for retry.
#[derive(Default)]
struct TypeEntries(Vec<(String, Vec<Value>)>);

#[derive(Default)]
struct BatchState {
    entries: HashMap<String, TypeEntries>,
    /// Distinct `process_agent_payload` calls merged into the open batch so far —
    /// what `NEW_RELIC_APM_BATCH_SIZE` counts against. Bumped once per call
    /// regardless of whether that call carried any mergeable telemetry, so "batch
    /// size = N invocations" matches the config var's plain-English meaning.
    request_count: usize,
}

static BATCH: Lazy<Mutex<BatchState>> = Lazy::new(|| Mutex::new(BatchState::default()));

/// Effective batch size, set once at startup via [`set_batch_size`]. Default `1`
/// (today's unbatched behavior).
static BATCH_SIZE: AtomicUsize = AtomicUsize::new(1);

/// Set the effective batch size (called once at startup from the parsed
/// `NEW_RELIC_APM_BATCH_SIZE` config value). Floored at 1 — zero would mean "never
/// flush," which is never the intended behavior.
pub fn set_batch_size(size: usize) {
    BATCH_SIZE.store(size.max(1), Ordering::Relaxed);
}

/// Current effective batch size.
pub fn get_batch_size() -> usize {
    BATCH_SIZE.load(Ordering::Relaxed)
}

/// One telemetry type's merged, ready-to-send payload, plus the original
/// per-request data it was built from (for re-buffering on send failure — see
/// `apm/app.rs`'s batched-send path).
pub struct MergedType {
    pub telemetry_type: String,
    pub merged_data: Vec<Value>,
    pub contributors: Vec<(String, Vec<Value>)>,
}

/// A batch ready to send: one [`MergedType`] per telemetry type that had data.
pub struct FlushedBatch {
    pub types: Vec<MergedType>,
}

/// Add one request's mergeable-type telemetry to the open batch. `telemetry_map`
/// should already be filtered to `MERGEABLE_TYPES` and disabled-type-excluded by
/// the caller (`metric_data`/`log_event_data` never reach this function).
///
/// Returns `Some(FlushedBatch)` — drained and merged under the lock — iff this call
/// pushed the open batch's request count to `batch_size`; `None` means the payload
/// was absorbed into the open batch with no network I/O.
pub fn add_request_and_maybe_flush(
    request_id: &str,
    telemetry_map: HashMap<String, Vec<Value>>,
    batch_size: usize,
) -> Option<FlushedBatch> {
    let Ok(mut state) = BATCH.lock() else {
        error!(
            "Failed to lock APM batch buffer - telemetry for request {} lost from batching path",
            request_id
        );
        return None;
    };

    for (telemetry_type, data) in telemetry_map {
        if !MERGEABLE_TYPES.contains(&telemetry_type.as_str()) {
            continue;
        }
        state
            .entries
            .entry(telemetry_type)
            .or_default()
            .0
            .push((request_id.to_string(), data));
    }
    state.request_count += 1;

    if state.request_count >= batch_size.max(1) {
        drain_and_merge(&mut state)
    } else {
        None
    }
}

/// Unconditionally drain and merge whatever is held, regardless of the count
/// reached so far. Used only by the shutdown path (Normal Lambda's final drain,
/// LMI's terminal `SHUTDOWN` heartbeat) as the safety net for a partial batch.
/// Returns `None` if nothing is buffered.
pub fn force_flush() -> Option<FlushedBatch> {
    let Ok(mut state) = BATCH.lock() else {
        error!("Failed to lock APM batch buffer for force_flush - partial batch may be lost");
        return None;
    };
    drain_and_merge(&mut state)
}

/// Drain the held entries and reset the counter. Returns `None` if nothing was held
/// (e.g. every invocation since the last flush was metric_data/log_event_data-only).
fn drain_and_merge(state: &mut BatchState) -> Option<FlushedBatch> {
    let entries = std::mem::take(&mut state.entries);
    state.request_count = 0;

    if entries.is_empty() {
        return None;
    }

    let types = entries
        .into_iter()
        .map(|(telemetry_type, TypeEntries(list))| merge_type(&telemetry_type, &list))
        .collect();

    Some(FlushedBatch { types })
}

/// Merge one telemetry type's per-request contributions into a single wire-ready
/// payload. Pure data transformation — no I/O, no `ApmApp`/`DeploymentContext`
/// dependency, so this is unit-testable without network mocks.
fn merge_type(telemetry_type: &str, entries: &[(String, Vec<Value>)]) -> MergedType {
    let contributors = entries.to_vec();

    let merged_data = if METRIC_SHAPED_TYPES.contains(&telemetry_type) {
        merge_metric_data_shaped(entries)
    } else if RESERVOIR_SHAPED_TYPES.contains(&telemetry_type) {
        merge_reservoir_shaped(entries)
    } else if NO_METADATA_SHAPED_TYPES.contains(&telemetry_type) {
        merge_no_metadata_shaped(entries)
    } else {
        // sql_trace_data (flat, no placeholder) — and a defensive fallback for any
        // other type that somehow reached here despite MERGEABLE_TYPES filtering
        // at the caller: flat concatenation loses no data either way.
        merge_flat_shaped(entries)
    };

    MergedType {
        telemetry_type: telemetry_type.to_string(),
        merged_data,
        contributors,
    }
}

/// `[placeholder, {reservoir_size, events_seen}, [events]]` merge: concat the
/// event arrays in arrival order; `events_seen` sums across contributors (lossless
/// — no re-sampling happens here, so this is a true combined count, unlike the
/// probabilistic-eviction case a live reservoir merge would need); `reservoir_size`
/// is a fixed per-account config value, so it's passed through from the first
/// contributor rather than summed or maxed.
fn merge_reservoir_shaped(entries: &[(String, Vec<Value>)]) -> Vec<Value> {
    let mut events_seen_total: u64 = 0;
    let mut reservoir_size: Option<Value> = None;
    let mut merged_events: Vec<Value> = Vec::new();

    for (_, data) in entries {
        if let Some(meta) = data.get(1) {
            if reservoir_size.is_none() {
                reservoir_size = meta.get("reservoir_size").cloned();
            }
            if let Some(seen) = meta.get("events_seen").and_then(Value::as_u64) {
                events_seen_total += seen;
            }
        }
        if let Some(events) = data.get(2).and_then(Value::as_array) {
            merged_events.extend(events.iter().cloned());
        }
    }

    let metadata = serde_json::json!({
        "reservoir_size": reservoir_size.unwrap_or_else(|| serde_json::json!(0)),
        "events_seen": events_seen_total,
    });

    vec![Value::Null, metadata, Value::Array(merged_events)]
}

/// `[placeholder, [events]]` merge (`transaction_sample_data`, `error_data`): concat
/// the event arrays directly, no metadata to combine.
fn merge_no_metadata_shaped(entries: &[(String, Vec<Value>)]) -> Vec<Value> {
    let mut merged_events: Vec<Value> = Vec::new();
    for (_, data) in entries {
        if let Some(events) = data.get(1).and_then(Value::as_array) {
            merged_events.extend(events.iter().cloned());
        }
    }
    vec![Value::Null, Value::Array(merged_events)]
}

/// Flat-array merge (`sql_trace_data`): each contributor's data is already its
/// full list of trace tuples with no placeholder — concatenate directly.
/// `collector::send_apm_telemetry` prepends the `run_id` once at send time, same
/// as it already does for a single unbatched request.
fn merge_flat_shaped(entries: &[(String, Vec<Value>)]) -> Vec<Value> {
    let mut merged: Vec<Value> = Vec::new();
    for (_, data) in entries {
        merged.extend(data.iter().cloned());
    }
    merged
}

/// `metric_data` merge: `[placeholder, start_epoch, end_epoch, [[metric_id, stats], ...]]`
/// where `metric_id = {"name": ..., "scope": ...}` and `stats = [call_count,
/// total_call_time, total_exclusive_call_time, min_call_time, max_call_time,
/// sum_of_squares]`. This is REAL per-metric stat aggregation, not concatenation —
/// verified against the Python agent's `TimeStats.merge_stats`
/// (`newrelic/core/stats_engine.py`): `call_count`/`total_call_time`/
/// `total_exclusive_call_time`/`sum_of_squares` sum; `min_call_time` is the min of
/// the two mins EXCEPT when the accumulator's `call_count` is still zero (take the
/// other side's min verbatim — a zero-initialized min is not a real sample);
/// `max_call_time` is the max. `call_count` is updated LAST, matching the Python
/// source's explicit ordering comment (the min-update above depends on whether
/// `call_count` was already nonzero, so bumping it early would corrupt that check).
/// The harvest window widens to `[min(starts), max(ends)]` across contributors,
/// same as merging any two adjacent (or overlapping) reporting periods into one.
fn merge_metric_data_shaped(entries: &[(String, Vec<Value>)]) -> Vec<Value> {
    let mut combined_start: Option<f64> = None;
    let mut combined_end: Option<f64> = None;
    // Preserve first-seen order for deterministic output; keyed by (name, scope).
    let mut order: Vec<(String, String)> = Vec::new();
    let mut stats_by_key: HashMap<(String, String), [f64; 6]> = HashMap::new();

    for (_, data) in entries {
        if let Some(start) = data.get(1).and_then(Value::as_f64) {
            combined_start = Some(combined_start.map_or(start, |s| s.min(start)));
        }
        if let Some(end) = data.get(2).and_then(Value::as_f64) {
            combined_end = Some(combined_end.map_or(end, |e| e.max(end)));
        }

        let Some(metrics) = data.get(3).and_then(Value::as_array) else {
            continue;
        };

        for metric_entry in metrics {
            let Some(pair) = metric_entry.as_array() else {
                continue;
            };
            if pair.len() < 2 {
                continue;
            }
            let name = pair[0]
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let scope = pair[0]
                .get("scope")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let Some(stats) = pair[1].as_array() else {
                continue;
            };
            if stats.len() < 6 {
                continue;
            }
            let vals: Vec<f64> = stats.iter().map(|v| v.as_f64().unwrap_or(0.0)).collect();

            let key = (name, scope);
            let acc = stats_by_key.entry(key.clone()).or_insert_with(|| {
                order.push(key.clone());
                [0.0; 6]
            });
            let had_prior_samples = acc[0] != 0.0;

            acc[1] += vals[1]; // total_call_time
            acc[2] += vals[2]; // total_exclusive_call_time
            acc[3] = if had_prior_samples {
                acc[3].min(vals[3])
            } else {
                vals[3]
            }; // min_call_time
            acc[4] = acc[4].max(vals[4]); // max_call_time
            acc[5] += vals[5]; // sum_of_squares
            acc[0] += vals[0]; // call_count — updated last, see doc comment above
        }
    }

    let metrics_array: Vec<Value> = order
        .into_iter()
        .map(|key| {
            let stats = stats_by_key.get(&key).copied().unwrap_or([0.0; 6]);
            let (name, scope) = key;
            // call_count is a summed event count; it will not approach i64::MAX in
            // one Lambda invocation's harvest window.
            #[allow(clippy::cast_possible_truncation)]
            let call_count = stats[0] as i64;
            serde_json::json!([
                { "name": name, "scope": scope },
                [call_count, stats[1], stats[2], stats[3], stats[4], stats[5]]
            ])
        })
        .collect();

    vec![
        Value::Null,
        combined_start.map_or(Value::Null, Value::from),
        combined_end.map_or(Value::Null, Value::from),
        Value::Array(metrics_array),
    ]
}

#[cfg(test)]
#[path = "batch_buffer_tests.rs"]
mod batch_buffer_tests;
