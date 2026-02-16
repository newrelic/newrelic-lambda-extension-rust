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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invocation_context_default() {
        let ctx = InvocationContext::default();
        assert!(ctx.request_id.is_empty());
        assert!(ctx.invoked_function_arn.is_empty());
        assert!(ctx.trace_id.is_none());
    }

    #[test]
    fn test_invocation_context_clone() {
        let ctx = InvocationContext {
            request_id: "req-123".to_string(),
            invoked_function_arn: "arn:aws:lambda:us-east-1:123:function:fn".to_string(),
            trace_id: Some("trace-456".to_string()),
        };
        let cloned = ctx.clone();
        assert_eq!(cloned.request_id, "req-123");
        assert_eq!(
            cloned.invoked_function_arn,
            "arn:aws:lambda:us-east-1:123:function:fn"
        );
        assert_eq!(cloned.trace_id, Some("trace-456".to_string()));
    }

    #[test]
    fn test_invocation_context_debug() {
        let ctx = InvocationContext::default();
        let debug_str = format!("{ctx:?}");
        assert!(debug_str.contains("InvocationContext"));
        assert!(debug_str.contains("request_id"));
        assert!(debug_str.contains("invoked_function_arn"));
        assert!(debug_str.contains("trace_id"));
    }

    #[test]
    fn test_invocation_context_with_trace_id() {
        let ctx = InvocationContext {
            request_id: "req".to_string(),
            invoked_function_arn: "arn".to_string(),
            trace_id: Some("abc123".to_string()),
        };
        assert_eq!(ctx.trace_id, Some("abc123".to_string()));
    }
}

