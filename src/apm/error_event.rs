//! Error event generation for platform faults and timeouts
//!
//! Based on error_event.go from Go implementation

use super::id_generator::TraceIDGenerator;
use serde_json::{json, Value};
use tracing::debug;

/// Generate error event from platform fault/timeout log
/// Returns full APM error event structure matching Go implementation
pub fn generate_error_event_from_fault(
    log_line: &str,
    request_id: &str,
    function_arn: &str,
) -> Option<Vec<Value>> {
    let is_timeout = log_line.contains("Task timed out");
    let is_fault = log_line.contains("error")
        || log_line.contains("ERROR")
        || log_line.contains("Error")
        || log_line.contains("exception")
        || log_line.contains("Exception");

    if !is_timeout && !is_fault {
        return None;
    }

    let error_message = if is_timeout {
        "Task timed out".to_string()
    } else {
        extract_error_message(log_line)
    };

    let error_class = if is_timeout {
        "LambdaTimeout"
    } else {
        "LambdaError"
    };

    generate_error_event_internal(error_class, &error_message, request_id, function_arn)
}

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

/// Extract error message from log line
fn extract_error_message(log_line: &str) -> String {
    if let Some(pos) = log_line.find("error:") {
        log_line[pos..].chars().take(200).collect()
    } else if let Some(pos) = log_line.find("ERROR") {
        log_line[pos..].chars().take(200).collect()
    } else if let Some(pos) = log_line.find("Error:") {
        log_line[pos..].chars().take(200).collect()
    } else if let Some(pos) = log_line.find("Exception:") {
        log_line[pos..].chars().take(200).collect()
    } else {
        log_line.chars().take(200).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_error_event_from_timeout() {
        let log = "2024-01-15T12:34:56.789Z abc123 Task timed out after 30.00 seconds";
        let events = generate_error_event_from_fault(
            log,
            "abc123",
            "arn:aws:lambda:us-east-1:123456789:function:my-function:1",
        );
        
        assert!(events.is_some());
        let events = events.unwrap();
        assert_eq!(events.len(), 1);
        
        let event_array = &events[0];
        assert!(event_array.is_array());
        let inner_array = event_array.as_array().unwrap();
        assert_eq!(inner_array.len(), 3);
        
        let event_detail = &inner_array[0];
        let user_attrs = &inner_array[2];
        
        assert_eq!(event_detail["error.class"], "LambdaTimeout");
        assert_eq!(event_detail["error.message"], "Task timed out");
        assert_eq!(event_detail["type"], "TransactionError");
        assert_eq!(event_detail["error.expected"], false);
        assert_eq!(event_detail["sampled"], true);
        assert!(event_detail["spanId"].is_string());
        assert!(event_detail["traceId"].is_string());
        assert!(event_detail["guid"].is_string());
        assert!(event_detail["priority"].is_number());
        
        assert_eq!(user_attrs["aws.requestId"], "abc123");
        assert_eq!(user_attrs["aws.lambda.functionVersion"], "1");
    }

    #[test]
    fn test_extract_function_name() {
        assert_eq!(
            extract_function_name("arn:aws:lambda:us-east-1:123456789:function:my-function"),
            "my-function"
        );
        assert_eq!(
            extract_function_name("arn:aws:lambda:us-east-1:123456789:function:my-function:2"),
            "my-function"
        );
        assert_eq!(extract_function_name("unknown"), "unknown");
    }

    #[test]
    fn test_extract_function_version() {
        assert_eq!(
            extract_function_version("arn:aws:lambda:us-east-1:123456789:function:my-function:2"),
            "2"
        );
        assert_eq!(
            extract_function_version("arn:aws:lambda:us-east-1:123456789:function:my-function"),
            "$LATEST"
        );
    }

    #[test]
    fn test_no_error_event_for_normal_log() {
        let log = "INFO: Processing request";
        let events = generate_error_event_from_fault(log, "abc", "arn");
        assert!(events.is_none());
    }
    
    #[test]
    fn test_error_event_with_fault() {
        let log = "ERROR: Something went wrong in the function";
        let events = generate_error_event_from_fault(
            log,
            "xyz789",
            "arn:aws:lambda:us-west-2:987654321:function:error-function",
        );

        assert!(events.is_some());
        let events = events.expect("should have events");
        let event_array = events[0].as_array().expect("should be array");
        let event_detail = &event_array[0];

        assert_eq!(event_detail["error.class"], "LambdaError");
        assert!(event_detail["error.message"].as_str().expect("string").contains("ERROR"));
    }

    // ========================================================================
    // extract_error_message — direct tests
    // ========================================================================

    #[test]
    fn test_extract_error_message_with_error_colon_prefix() {
        let msg = extract_error_message("2024-01-15 error: connection refused");
        assert!(msg.starts_with("error:"));
    }

    #[test]
    fn test_extract_error_message_with_uppercase_error() {
        let msg = extract_error_message("2024 ERROR Something went wrong");
        assert!(msg.starts_with("ERROR"));
    }

    #[test]
    fn test_extract_error_message_with_error_colon_mixed() {
        let msg = extract_error_message("module Error: bad config value");
        assert!(msg.starts_with("Error:"));
    }

    #[test]
    fn test_extract_error_message_with_exception_prefix() {
        let msg = extract_error_message("RuntimeException: null pointer at line 42");
        assert!(msg.contains("Exception:"));
    }

    #[test]
    fn test_extract_error_message_no_known_prefix() {
        let msg = extract_error_message("Something completely different happened");
        assert_eq!(msg, "Something completely different happened");
    }

    #[test]
    fn test_extract_error_message_very_long_input() {
        let long_input = "ERROR ".to_string() + &"x".repeat(500);
        let msg = extract_error_message(&long_input);
        assert_eq!(msg.chars().count(), 200);
        assert!(msg.starts_with("ERROR"));
    }

    #[test]
    fn test_generate_error_event_returns_non_empty() {
        let events = generate_error_event("TestError", "test message", "req-1", "arn:test");
        assert!(!events.is_empty());
    }

    #[test]
    fn test_generate_error_event_from_fault_lowercase_exception() {
        let log = "Unhandled exception in handler";
        let events = generate_error_event_from_fault(log, "req-1", "arn:test");
        assert!(events.is_some());
        let events = events.expect("should be some");
        let detail = &events[0].as_array().expect("array")[0];
        assert_eq!(detail["error.class"], "LambdaError");
    }

    #[test]
    fn test_extract_function_name_empty_string() {
        assert_eq!(extract_function_name(""), "unknown");
    }

    #[test]
    fn test_extract_function_version_empty_string() {
        assert_eq!(extract_function_version(""), "$LATEST");
    }
}
