// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;

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

// NR-609043: `generate_error_event` (used for real Lambda shutdown timeout/failure
// events, see ApmApp::send_shutdown_error_event) builds the event from an explicit
// error class/message the caller already knows is a real invocation failure — it
// does not sniff log content for "error"-like substrings, unlike the now-removed
// generate_error_event_from_fault, which used to misclassify handled/logged
// exceptions in function logs as LambdaError events.
#[test]
fn test_generate_error_event_for_shutdown_timeout() {
    let events = generate_error_event(
        "LambdaTimeout",
        "Task timed out",
        "abc123",
        "arn:aws:lambda:us-east-1:123456789:function:my-function:1",
    );

    assert_eq!(events.len(), 1);
    let event_array = events[0].as_array().unwrap();
    let event_detail = &event_array[0];
    let user_attrs = &event_array[2];

    assert_eq!(event_detail["error.class"], "LambdaTimeout");
    assert_eq!(event_detail["error.message"], "Task timed out");
    assert_eq!(event_detail["type"], "TransactionError");
    assert_eq!(user_attrs["aws.requestId"], "abc123");
}
