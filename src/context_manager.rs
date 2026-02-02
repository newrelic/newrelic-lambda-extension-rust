/// Context Manager - Single source of truth for request contexts
///
/// This module provides centralized context management for Lambda invocations.
/// Key design decisions:
/// 1. Function ARN is set once per cold start and reused for all requests (never changes)
/// 2. Per-request contexts stored in DashMap for concurrent request isolation
/// 3. Replaces scattered context storage (CURRENT_INVOCATION_CONTEXT, processor contexts)

use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::sync::{Arc, RwLock};
use tracing::{debug, warn};

use crate::context::InvocationContext;

/// Request-specific context containing request_id and trace_id
#[derive(Debug, Clone)]
pub struct RequestContext {
    pub request_id: String,
    pub trace_id: Option<String>,
}

/// Centralized context manager for Lambda invocations
///
/// Architecture:
/// - `function_arn`: Set once per cold start, shared across all requests
/// - `contexts`: Per-request map, provides isolation for concurrent requests
pub struct ContextManager {
    /// Per-request contexts keyed by request_id
    contexts: Arc<DashMap<String, RequestContext>>,
    
    /// Function ARN - set once per cold start, never changes during warm starts
    /// Uses RwLock for optimal read performance (many reads, single write at cold start)
    function_arn: Arc<RwLock<Option<String>>>,
}

impl ContextManager {
    /// Get the global ContextManager instance (singleton pattern)
    pub fn global() -> &'static ContextManager {
        static INSTANCE: Lazy<ContextManager> = Lazy::new(|| ContextManager::new());
        &INSTANCE
    }

    /// Create a new ContextManager (used internally by global())
    pub fn new() -> Self {
        Self {
            contexts: Arc::new(DashMap::new()),
            function_arn: Arc::new(RwLock::new(None)),
        }
    }

    /// Set the function ARN (called once during cold start)
    ///
    /// The ARN never changes after cold start. If AWS changes the ARN, 
    /// Lambda creates a new cold start with a new extension instance.
    pub fn set_function_arn(&self, arn: String) {
        match self.function_arn.write() {
            Ok(mut guard) => {
                if guard.is_none() {
                    debug!("Setting function ARN (cold start): {}", arn);
                    *guard = Some(arn);
                } else if guard.as_ref() != Some(&arn) {
                    // This should never happen unless AWS behavior changes
                    warn!(
                        "Function ARN changed from {:?} to {} - this indicates a new cold start",
                        *guard, arn
                    );
                    *guard = Some(arn);
                }
            }
            Err(e) => {
                warn!("Failed to set function ARN (lock poisoned): {}", e);
            }
        }
    }

    /// Get the function ARN (available after first INVOKE event)
    pub fn get_function_arn(&self) -> Option<String> {
        self.function_arn
            .read()
            .ok()
            .and_then(|guard| guard.clone())
    }

    /// Set request context (called when /next returns with new request_id)
    ///
    /// This creates a new entry in the per-request map. Each request gets
    /// its own isolated context, preventing concurrent request interference.
    pub fn set_request(&self, request_id: String, trace_id: Option<String>) {
        let context = RequestContext {
            request_id: request_id.clone(),
            trace_id,
        };
        
        self.contexts.insert(request_id.clone(), context);
        debug!("Set context for request: {}", request_id);
    }

    /// Get request context by request_id
    ///
    /// Returns None if request_id not found (request not yet created or already cleaned up)
    pub fn get_request(&self, request_id: &str) -> Option<RequestContext> {
        self.contexts.get(request_id).map(|entry| entry.value().clone())
    }

    /// Update trace_id for existing request
    ///
    /// Called when trace_id is extracted from logs after request is created
    pub fn update_trace_id(&self, request_id: &str, trace_id: String) {
        if let Some(mut entry) = self.contexts.get_mut(request_id) {
            entry.trace_id = Some(trace_id.clone());
            debug!("Updated trace_id for request {}: {}", request_id, trace_id);
        } else {
            warn!("Cannot update trace_id - request {} not found", request_id);
        }
    }

    /// Get full InvocationContext for compatibility with existing code
    ///
    /// Combines per-request context with global function_arn
    pub fn get_invocation_context(&self, request_id: &str) -> Option<InvocationContext> {
        let request_ctx = self.get_request(request_id)?;
        let arn = self.get_function_arn().unwrap_or_default();
        
        Some(InvocationContext {
            request_id: request_ctx.request_id,
            invoked_function_arn: arn,
            trace_id: request_ctx.trace_id,
        })
    }

    /// Remove request context (cleanup after request completes)
    ///
    /// Should be called when request processing is complete to prevent memory leaks
    pub fn remove_request(&self, request_id: &str) {
        if self.contexts.remove(request_id).is_some() {
            debug!("Removed context for request: {}", request_id);
        }
    }

    /// Get count of active requests (for monitoring/debugging)
    pub fn active_request_count(&self) -> usize {
        self.contexts.len()
    }

    /// Check if a request context exists
    pub fn has_request(&self, request_id: &str) -> bool {
        self.contexts.contains_key(request_id)
    }
}
