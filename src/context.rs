// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The context module holds shared state for the duration of an invocation.

/// Holds state for a specific invocation
#[derive(Debug, Clone)]
pub struct InvocationContext {
    pub invoked_function_arn: String,
    pub request_id: String,
    pub trace_id: Option<String>,
}



impl Default for InvocationContext {
    fn default() -> Self {
        Self {
            invoked_function_arn: String::new(),
            request_id: String::new(),
            trace_id: None,
        }
    }
}

