# Simple Fix Plan - Request ID Issues

**Goal:** Update ONE place for request_id, eliminate duplicated context, fix all log issues

---

## Quick Visual

### Current (Broken)

```
Event → Updates 3 places → Processors read from different places → CHAOS!

       ┌─→ CURRENT_INVOCATION_CONTEXT (global) ❌
       │
Event─┼─→ REQUEST_CONTEXTS (map) ⚠️
       │
       └─→ processor.invocation_context ❌

LogProcessor reads from → ??? (random place)
PlatformProcessor reads → ??? (different place)
Result: Wrong request_id!
```

### Proposed (Fixed)

```
Event → Updates 1 place → All processors read from same place → WORKS!

Event ───→ ContextManager.set_request(id, arn) ✅
           │
           │
           ├─→ LogProcessor.stamp_log() → reads ContextManager ✅
           │
           └─→ PlatformProcessor.create_log() → reads ContextManager ✅

Result: Correct request_id always!
```

---

## Current Problem (Messy!)

Right now we have **3 different places** storing the same information:

```rust
// Place 1: Global shared (BAD - causes Bug #2)
static CURRENT_INVOCATION_CONTEXT = ...;

// Place 2: Per-request map (GOOD)
static REQUEST_CONTEXTS: DashMap<String, Context> = ...;

// Place 3: Inside each processor (REDUNDANT)
struct LogProcessor {
    invocation_context: Arc<Mutex<InvocationContext>>,  // Duplicate!
}
```

**Result:** Code reads from different places, gets confused, logs have wrong request_id!

---

## Simple Solution: ONE Source of Truth

### New Design - Single Context Manager

```
┌─────────────────────────────────────────┐
│      ContextManager (ONE PLACE)         │
│                                          │
│  • Stores request_id per request        │
│  • Stores function ARN (once)           │
│  • Thread-safe per-request access       │
│  • NO global shared state               │
└─────────────────────────────────────────┘
           ↓         ↓         ↓
    LogProcessor  Platform  EventLoop
    (reads only)  (reads)   (writes)
```

---

## Step-by-Step Implementation

### Step 1: Create ContextManager (New File)

**File:** `src/context_manager.rs`

```rust
use dashmap::DashMap;
use std::sync::Arc;
use once_cell::sync::Lazy;

/// ONE PLACE to manage all request contexts
pub struct ContextManager {
    // Per-request contexts - thread safe, no overwrites
    contexts: Arc<DashMap<String, RequestContext>>,
    
    // Function ARN - set once at startup
    function_arn: Arc<std::sync::RwLock<Option<String>>>,
}

#[derive(Clone)]
pub struct RequestContext {
    pub request_id: String,
    pub invoked_function_arn: String,
    pub trace_id: Option<String>,
}

// Global instance
static CONTEXT_MANAGER: Lazy<ContextManager> = Lazy::new(|| {
    ContextManager {
        contexts: Arc::new(DashMap::new()),
        function_arn: Arc::new(std::sync::RwLock::new(None)),
    }
});

impl ContextManager {
    /// Get the global instance
    pub fn global() -> &'static ContextManager {
        &CONTEXT_MANAGER
    }
    
    /// Store context for a request
    pub fn set_request(&self, request_id: String, arn: String) {
        self.contexts.insert(
            request_id.clone(),
            RequestContext {
                request_id,
                invoked_function_arn: arn,
                trace_id: None,
            }
        );
    }
    
    /// Get context for a request
    pub fn get_request(&self, request_id: &str) -> Option<RequestContext> {
        self.contexts.get(request_id).map(|r| r.value().clone())
    }
    
    /// Update trace_id for a request
    pub fn set_trace_id(&self, request_id: &str, trace_id: String) {
        if let Some(mut ctx) = self.contexts.get_mut(request_id) {
            ctx.trace_id = Some(trace_id);
        }
    }
    
    /// Clean up after request completes
    pub fn remove_request(&self, request_id: &str) {
        self.contexts.remove(request_id);
    }
    
    /// Set function ARN once at startup
    pub fn set_function_arn(&self, arn: String) {
        if let Ok(mut guard) = self.function_arn.write() {
            *guard = Some(arn);
        }
    }
    
    /// Get function ARN
    pub fn get_function_arn(&self) -> Option<String> {
        self.function_arn.read().ok()?.clone()
    }
}
```

---

### Step 2: Update LogProcessor (Simplify)

**File:** `src/logs/processor.rs`

```rust
// REMOVE this field:
// invocation_context: Arc<Mutex<InvocationContext>>,  ❌ DELETE

// ADD current request_id tracking:
current_request_id: Arc<RwLock<Option<String>>>,

impl LogProcessor {
    pub fn new(client: Arc<NewRelicClient>, config: Arc<ExtensionConfig>) -> Self {
        Self {
            current_request_id: Arc::new(RwLock::new(None)),
            // ... other fields
        }
    }
    
    /// Set which request this processor is handling
    pub fn set_current_request(&self, request_id: String) {
        if let Ok(mut guard) = self.current_request_id.write() {
            *guard = Some(request_id);
        }
    }
    
    /// Stamp log with AWS attributes
    fn stamp_log(&self, mut log: LogMessage) -> LogMessage {
        // Read current request_id
        let request_id = if let Ok(guard) = self.current_request_id.read() {
            guard.clone()
        } else {
            None
        };
        
        // Get context from ContextManager (ONE PLACE!)
        if let Some(req_id) = request_id {
            if let Some(ctx) = ContextManager::global().get_request(&req_id) {
                // Stamp the log
                log.attributes.insert("aws", json!({
                    "lambda_request_id": ctx.request_id
                }));
                log.attributes.insert("faas.execution", json!(ctx.request_id));
                log.attributes.insert("faas.arn", json!(ctx.invoked_function_arn));
                
                if let Some(trace_id) = ctx.trace_id {
                    log.attributes.insert("trace.id", json!(trace_id));
                }
            }
        }
        
        log
    }
}
```

---

### Step 3: Update PlatformProcessor

**File:** `src/platform/processor.rs`

```rust
// REMOVE this field:
// invocation_context: Arc<Mutex<InvocationContext>>,  ❌ DELETE

// ADD current request_id:
current_request_id: Arc<RwLock<Option<String>>>,

impl PlatformProcessor {
    /// Set which request this processor is handling
    pub fn set_current_request(&self, request_id: String) {
        if let Ok(mut guard) = self.current_request_id.write() {
            *guard = Some(request_id);
        }
    }
    
    fn process_platform_event_internal(&self, event: &TelemetryEvent) {
        // ... metrics and errors ...
        
        if self.config.extension.send_platform_logs {
            let log = self.create_basic_log(event);
            
            // Use LogProcessor's stamping (ONE WAY!)
            self.log_processor.stamp_and_add_log(log);  // ✅ Consistent!
        }
    }
}
```

---

### Step 4: Update EventLoop (Simplify)

**File:** `src/event_loop.rs`

```rust
// DELETE this function entirely:
// fn update_global_invocation_context(...) { }  ❌ DELETE

// REPLACE with simple call:
match runtime_event {
    runtime::LambdaRuntimeEvent::Invoke { request_id, invoked_function_arn } => {
        
        // ONE place to store context ✅
        ContextManager::global().set_request(
            request_id.clone(),
            invoked_function_arn.clone()
        );
        
        // Tell processors which request they're handling
        components.global_log_processor.set_current_request(request_id.clone());
        
        // Create request state (for buffers, coordination)
        let request_state = create_request_processing_state(...);
        
        // ... rest of processing ...
    }
}

// Cleanup when done
fn cleanup_request(request_id: &str) {
    ContextManager::global().remove_request(request_id);
    // ... other cleanup ...
}
```

---

## What This Fixes

### ✅ Bug #1 (Platform Logs) - Partially Fixed
- Platform logs now use same stamping as function logs
- Still can't fix timing issue (AWS constraint)
- But logs will be **consistent** - either all have request_id or none do

### ✅ Bug #2 (Concurrent Requests) - Fully Fixed!
- Each request has its own context in DashMap
- No shared global variable
- Request A can't overwrite Request B
- Logs always get correct request_id

---

## Files to Change

```
1. src/context_manager.rs     [CREATE]  - New context manager
2. src/lib.rs                  [MODIFY]  - Add pub mod context_manager;
3. src/logs/processor.rs       [MODIFY]  - Remove invocation_context field
4. src/platform/processor.rs   [MODIFY]  - Remove invocation_context field
5. src/event_loop.rs           [MODIFY]  - Remove update_global_invocation_context
6. src/main.rs                 [MODIFY]  - Remove CURRENT_INVOCATION_CONTEXT
7. src/request.rs              [MODIFY]  - Remove REQUEST_CONTEXTS usage
```

---

## Migration Steps (Safe)

### Phase 1: Add ContextManager (No Breaking Changes)
1. Create `src/context_manager.rs`
2. Add to `src/lib.rs`
3. Deploy & test - should work same as before

### Phase 2: Update Processors
4. Update LogProcessor to use ContextManager
5. Update PlatformProcessor to use ContextManager
6. Deploy & test

### Phase 3: Remove Old Code
7. Remove CURRENT_INVOCATION_CONTEXT from main.rs
8. Remove update_global_invocation_context from event_loop.rs
9. Remove REQUEST_CONTEXTS from request.rs
10. Deploy & test

---

## Testing

```rust
#[tokio::test]
async fn test_concurrent_requests_fixed() {
    // Create 3 requests at same time
    ContextManager::global().set_request("req-A".into(), "arn-A".into());
    ContextManager::global().set_request("req-B".into(), "arn-B".into());
    ContextManager::global().set_request("req-C".into(), "arn-C".into());
    
    // Each request gets its own context ✅
    let ctx_a = ContextManager::global().get_request("req-A").unwrap();
    let ctx_b = ContextManager::global().get_request("req-B").unwrap();
    let ctx_c = ContextManager::global().get_request("req-C").unwrap();
    
    assert_eq!(ctx_a.request_id, "req-A");
    assert_eq!(ctx_b.request_id, "req-B");
    assert_eq!(ctx_c.request_id, "req-C");
}
```

---

## Summary

**Before:**
- 3 places storing context → confusion
- Global shared state → Bug #2
- Platform logs bypass stamping → Bug #1

**After:**
- 1 place (ContextManager) → simple
- Per-request storage → Bug #2 fixed ✅
- Consistent stamping → Bug #1 improved ✅

**Code Changes:** ~300 lines changed, ~200 lines deleted = **Simpler code!**
