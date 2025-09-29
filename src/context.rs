//! The context module holds shared state for the duration of an invocation.

/// Holds state for a specific invocation
#[derive(Debug, Clone)]
pub struct InvocationContext {
    pub invoked_function_arn: String,
    pub request_id: String,
    pub trace_id: Option<String>,
}

impl InvocationContext {
    /// Creates a new context for a specific invocation
    pub fn new(request_id: String, invoked_function_arn: String) -> Self {
        Self {
            invoked_function_arn,
            request_id,
            trace_id: None,
        }
    }
}

impl Default for InvocationContext {
    fn default() -> Self {
        Self {
            invoked_function_arn: "arn:aws:lambda:unknown:unknown:function:unknown".to_string(),
            request_id: "unknown".to_string(),
            trace_id: None,
        }
    }
}

