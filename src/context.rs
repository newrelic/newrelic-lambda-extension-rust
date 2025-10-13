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
            invoked_function_arn: String::new(), // Empty string instead of "unknown"
            request_id: String::new(), // Empty string instead of "unknown"
            trace_id: None,
        }
    }
}

