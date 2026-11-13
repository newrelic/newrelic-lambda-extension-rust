// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Trace ID extraction module for New Relic Lambda Extension
//!
//! This module provides optimized functions to extract trace IDs from New Relic agent payloads.
//! It only processes payloads when trace ID collection is enabled via NEW_RELIC_COLLECT_TRACE_ID.

use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;
use tracing::{debug, trace};

/// Extract trace ID from agent payload, if present and collection is enabled.
///
/// Delegates to `apm::payload_parser::parse_agent_payload_cached` instead of
/// maintaining a second, independent base64/gzip/JSON decoder — this function
/// used to fully re-parse the payload from scratch even when the exact same
/// bytes had already been (or were about to be) parsed elsewhere for routing or
/// sending. Reusing the shared cache means a payload parsed once, anywhere,
/// never pays for a second full decode just to find a trace.id. The shared
/// parser already tolerates the double-base64-encoded envelope shape this
/// function used to handle itself (see its doc comment).
pub fn extract_trace_id_from_payload(payload_bytes: &[u8]) -> Result<Option<String>> {
    trace!("Starting trace ID extraction from payload ({} bytes)", payload_bytes.len());

    let parsed = match crate::apm::payload_parser::parse_agent_payload_cached(payload_bytes) {
        Ok(parsed) => parsed,
        Err(e) => {
            debug!("Failed to parse agent payload for trace extraction: {}", e);
            return Ok(None);
        }
    };
    let telemetry_map = &parsed.0;

    if let Some(trace_id) = extract_trace_id_from_analytics(telemetry_map) {
        debug!("Successfully extracted trace ID from analytic events: {}", trace_id);
        return Ok(Some(trace_id));
    }

    if let Some(trace_id) = extract_trace_id_from_spans(telemetry_map) {
        debug!("Successfully extracted trace ID from span events: {}", trace_id);
        return Ok(Some(trace_id));
    }

    debug!("No trace ID found in agent payload");
    Ok(None)
}

/// Shape of each mergeable event-list type after `payload_parser::parse_agent_payload`
/// has already unwrapped the outer array: `[run_id_or_placeholder, metadata, events]`.
type TelemetryMap = HashMap<String, Vec<Value>>;

/// Extract trace ID from analytic event data. `events[2]` is already a plain
/// `Vec<Value>` (unwrapped by `parse_agent_payload`) — no second
/// `serde_json::from_value` round-trip needed to get there, unlike the
/// now-removed standalone decoder this replaced.
fn extract_trace_id_from_analytics(data: &TelemetryMap) -> Option<String> {
    extract_trace_id_from_event_list(data, "analytic_event_data")
}

/// Extract trace ID from span event data. Same shape/logic as
/// `extract_trace_id_from_analytics`, different telemetry type.
fn extract_trace_id_from_spans(data: &TelemetryMap) -> Option<String> {
    extract_trace_id_from_event_list(data, "span_event_data")
}

fn extract_trace_id_from_event_list(data: &TelemetryMap, telemetry_type: &str) -> Option<String> {
    let parsed_events = data.get(telemetry_type)?;
    if parsed_events.len() <= 2 {
        return None;
    }

    let events_array = parsed_events[2].as_array()?;
    let first_event = events_array.first()?.as_array()?;
    let event_obj = first_event.first()?.as_object()?;
    event_obj.get("traceId")?.as_str().map(|s| s.to_string())
}

#[cfg(test)]
mod mod_tests;
