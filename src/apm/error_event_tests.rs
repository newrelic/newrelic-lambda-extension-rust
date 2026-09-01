// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

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
    let events = events.unwrap();
    let event_array = &events[0].as_array().unwrap();
    let event_detail = &event_array[0];
    
    assert_eq!(event_detail["error.class"], "LambdaError");
    assert!(event_detail["error.message"].as_str().unwrap().contains("ERROR"));
}
