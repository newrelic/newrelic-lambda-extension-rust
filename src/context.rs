//! The context module holds shared state for the duration of an invocation.

/// Holds state for the current invocation
#[derive(Debug, Clone)]
pub struct InvocationContext {
    pub invoked_function_arn: String,
    pub request_id: String,
}

impl Default for InvocationContext {
    fn default() -> Self {
        Self {
            invoked_function_arn: "arn:aws:lambda:unknown:unknown:function:unknown".to_string(),
            request_id: "unknown".to_string(),
        }
    }
}

