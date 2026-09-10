// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Error event generation for platform faults and timeouts
//!
//! Based on error_event.go from Go implementation

use super::id_generator::TraceIDGenerator;
use serde_json::{json, Value};
use tracing::debug;

/// Generate error event directly from error class and message
/// Used for shutdown events (timeout, failure) where we don't parse from log lines
pub fn generate_error_event(
    error_class: &str,
    error_message: &str,
    request_id: &str,
    function_arn: &str,
) -> Vec<Value> {
    generate_error_event_internal(error_class, error_message, request_id, function_arn)
        .unwrap_or_else(Vec::new)
}

/// Internal function to generate error event structure
fn generate_error_event_internal(
    error_class: &str,
    error_message: &str,
    request_id: &str,
    function_arn: &str,
) -> Option<Vec<Value>> {
    debug!(
        "Generating error event for request {}: {} - {}",
        request_id, error_class, error_message
    );

    let trace_gen = TraceIDGenerator::new(1453);
    let span_id = trace_gen.generate_span_id();
    let trace_id = trace_gen.generate_trace_id();
    let guid = trace_gen.generate_trace_id();
    let priority = trace_gen.float32() as f64 * 2.0;

    let timestamp_ms = chrono::Utc::now().timestamp_millis();

    let function_name = extract_function_name(function_arn);
    let transaction_name = format!("OtherTransaction/Function/{}", function_name);

    let function_version = extract_function_version(function_arn);

    let event_detail = json!({
        "duration": 0.1,
        "error.class": error_class,
        "error.expected": false,
        "error.message": error_message,
        "guid": guid,
        "nr.transactionGuid": guid,
        "priority": priority,
        "sampled": true,
        "spanId": span_id,
        "timestamp": timestamp_ms,
        "traceId": trace_id,
        "transactionName": transaction_name,
        "type": "TransactionError",
    });

    let agent_attrs = json!({});

    let user_attrs = json!({
        "aws.lambda.arn": function_arn,
        "aws.lambda.functionVersion": function_version,
        "aws.requestId": request_id,
    });

    let event_array = vec![json!([event_detail, agent_attrs, user_attrs])];

    Some(event_array)
}

/// Extract function name from ARN
fn extract_function_name(arn: &str) -> String {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() >= 7 {
        parts[6].to_string()
    } else {
        "unknown".to_string()
    }
}

/// Extract function version from ARN
fn extract_function_version(arn: &str) -> String {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() >= 8 {
        parts[7].to_string()
    } else {
        "$LATEST".to_string()
    }
}

#[cfg(test)]
#[path = "error_event_tests.rs"]
mod tests;
