use newrelic_lambda_extension::context_manager::{ContextManager, RequestContext};
use newrelic_lambda_extension::context::InvocationContext;

#[test]
fn test_function_arn_set_once() {
    let manager = ContextManager::new();
    
    // Set ARN (simulating cold start)
    manager.set_function_arn("arn:aws:lambda:us-east-1:123456789012:function:test".to_string());
    
    // Get ARN
    let arn = manager.get_function_arn();
    assert_eq!(arn, Some("arn:aws:lambda:us-east-1:123456789012:function:test".to_string()));
    
    // Set same ARN again (should be no-op)
    manager.set_function_arn("arn:aws:lambda:us-east-1:123456789012:function:test".to_string());
    assert_eq!(manager.get_function_arn(), arn);
}

#[test]
fn test_per_request_isolation() {
    let manager = ContextManager::new();
    manager.set_function_arn("arn:aws:lambda:us-east-1:123456789012:function:test".to_string());
    
    // Create two concurrent requests
    manager.set_request("request-1".to_string(), None);
    manager.set_request("request-2".to_string(), None);
    
    // Update trace_id for request-1
    manager.update_trace_id("request-1", "trace-1".to_string());
    
    // Verify isolation - request-2 should not have trace_id
    let ctx1 = manager.get_request("request-1").unwrap();
    let ctx2 = manager.get_request("request-2").unwrap();
    
    assert_eq!(ctx1.request_id, "request-1");
    assert_eq!(ctx1.trace_id, Some("trace-1".to_string()));
    
    assert_eq!(ctx2.request_id, "request-2");
    assert_eq!(ctx2.trace_id, None);
}

#[test]
fn test_get_invocation_context() {
    let manager = ContextManager::new();
    manager.set_function_arn("arn:aws:lambda:us-east-1:123456789012:function:test".to_string());
    manager.set_request("request-1".to_string(), Some("trace-1".to_string()));
    
    let ctx = manager.get_invocation_context("request-1").unwrap();
    assert_eq!(ctx.request_id, "request-1");
    assert_eq!(ctx.invoked_function_arn, "arn:aws:lambda:us-east-1:123456789012:function:test");
    assert_eq!(ctx.trace_id, Some("trace-1".to_string()));
}

#[test]
fn test_remove_request() {
    let manager = ContextManager::new();
    manager.set_request("request-1".to_string(), None);
    
    assert_eq!(manager.active_request_count(), 1);
    assert!(manager.has_request("request-1"));
    
    manager.remove_request("request-1");
    
    assert_eq!(manager.active_request_count(), 0);
    assert!(!manager.has_request("request-1"));
}

#[test]
fn test_concurrent_requests_no_interference() {
    let manager = ContextManager::new();
    manager.set_function_arn("arn:aws:lambda:us-east-1:123456789012:function:test".to_string());
    
    // Simulate 3 concurrent requests
    manager.set_request("req-a".to_string(), None);
    manager.set_request("req-b".to_string(), None);
    manager.set_request("req-c".to_string(), None);
    
    // Update trace_ids independently
    manager.update_trace_id("req-a", "trace-a".to_string());
    manager.update_trace_id("req-b", "trace-b".to_string());
    manager.update_trace_id("req-c", "trace-c".to_string());
    
    // Verify no cross-contamination
    let ctx_a = manager.get_invocation_context("req-a").unwrap();
    let ctx_b = manager.get_invocation_context("req-b").unwrap();
    let ctx_c = manager.get_invocation_context("req-c").unwrap();
    
    assert_eq!(ctx_a.request_id, "req-a");
    assert_eq!(ctx_a.trace_id, Some("trace-a".to_string()));
    
    assert_eq!(ctx_b.request_id, "req-b");
    assert_eq!(ctx_b.trace_id, Some("trace-b".to_string()));
    
    assert_eq!(ctx_c.request_id, "req-c");
    assert_eq!(ctx_c.trace_id, Some("trace-c".to_string()));
    
    // All share same ARN
    assert_eq!(ctx_a.invoked_function_arn, ctx_b.invoked_function_arn);
    assert_eq!(ctx_b.invoked_function_arn, ctx_c.invoked_function_arn);
}
