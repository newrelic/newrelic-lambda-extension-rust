# Current Code Situation - What's Actually Happening

This document shows REAL code from the project right now and what's wrong with it.

---

## Problem 1: Multiple Context Storage Locations

### Location 1: Global Shared Context (main.rs:60)

```rust
// src/main.rs line 60
static CURRENT_INVOCATION_CONTEXT: Lazy<Arc<RwLock<InvocationContext>>> = 
    Lazy::new(|| {
        Arc::new(RwLock::new(InvocationContext {
            request_id: String::new(),  // ❌ SHARED across ALL requests
            invoked_function_arn: String::new(),
            trace_id: None,
        }))
    });
```

**Problem:** Every request OVERWRITES this same variable!

### Location 2: Per-Request Map (request.rs:35)

```rust
// src/request.rs line 35
pub static REQUEST_CONTEXTS: Lazy<Arc<DashMap<String, Arc<Mutex<InvocationContext>>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));
```

**Good:** Each request gets its own entry  
**Problem:** Not used consistently - some code still reads from Location 1!

### Location 3: Per-Processor Context (logs/processor.rs:47)

```rust
// src/logs/processor.rs line 47
pub struct LogProcessor {
    log_batch: Arc<Mutex<Vec<payload::LogMessage>>>,
    invocation_context: Arc<Mutex<InvocationContext>>,  // ❌ DUPLICATE!
    // ... more fields
}
```

**Problem:** Each processor has its own copy, needs manual synchronization

---

## Problem 2: Context Updates Everywhere

### Update Point 1: event_loop.rs (Line 157)

```rust
// src/event_loop.rs line 157 (APM mode)
update_global_invocation_context(&request_id, &invoked_function_arn);
```

### Update Point 2: event_loop.rs (Line 533)

```rust
// src/event_loop.rs line 533 (Standard mode)
update_global_invocation_context(&request_id, &invoked_function_arn);
```

### The Update Function (event_loop.rs:1254)

```rust
// src/event_loop.rs line 1254
fn update_global_invocation_context(request_id: &str, invoked_function_arn: &str) {
    if let Ok(mut global_context) = crate::CURRENT_INVOCATION_CONTEXT.write() {
        global_context.request_id = request_id.to_string();  // ❌ OVERWRITES!
        global_context.invoked_function_arn = invoked_function_arn.to_string();
    }
}
```

### Update Point 3: LogProcessor Update (event_loop.rs:170)

```rust
// src/event_loop.rs line 170 (APM mode)
components.global_log_processor
    .update_invocation_context(request_state.context.clone());
```

---

## Problem 3: Inconsistent Reads

### Read Point 1: Fallback to Global (event_loop.rs:362)

```rust
// src/event_loop.rs line 362
if let Ok(global_ctx) = CURRENT_INVOCATION_CONTEXT.read() {
    global_ctx.invoked_function_arn.clone()
}
```

### Read Point 2: Try Per-Request First (event_loop.rs:354)

```rust
// src/event_loop.rs line 354
let invoked_function_arn = REQUEST_CONTEXTS
    .get(&request_id)
    .and_then(|entry| {
        // Try per-request context first
    })
    .unwrap_or_else(|| {
        // Fall back to global ❌ WRONG!
        if let Ok(global_ctx) = CURRENT_INVOCATION_CONTEXT.read() {
            global_ctx.invoked_function_arn.clone()
        }
    });
```

**Problem:** Code tries per-request first, falls back to global (which might be wrong!)

---

## Problem 4: Platform Logs Bypass Stamping

### Current Flow (platform/processor.rs)

```rust
// Simplified from src/platform/processor.rs
fn process_platform_event_internal(&self, event: &TelemetryEvent) {
    // 1. Store metrics ✅
    self.store_platform_metrics(event);
    
    // 2. Check errors ✅
    self.check_and_send_platform_errors(event);
    
    // 3. Create log
    if self.config.extension.send_platform_logs {
        let log_message = LogMessage {
            timestamp: event.time,
            message: event.message.clone(),
            attributes: basic_attrs,  // ❌ NO AWS ATTRIBUTES!
        };
        
        // 4. Add directly to batch - BYPASSES STAMPING!
        self.log_processor.add_log_to_batch(log_message);  // ❌
    }
}
```

**Should be:**

```rust
// Use stamping like function logs
self.log_processor.stamp_and_add_log(log_message);  // ✅
```

---

## Real Example: What Happens During Concurrent Requests

### Timeline

```
Time   | Request A           | Request B           | Global Context
-------|---------------------|---------------------|------------------
T0     | Start               |                     | empty
T1     | update_global("A")  |                     | "A"
T2     | Processing logs...  | Start               | "A"
T3     |                     | update_global("B")  | "B" ← OVERWROTE!
T4     | Read context        |                     | "B" ← WRONG!
T5     | Log stamped with B  | Processing logs...  | "B"
```

### Actual Code Path

```rust
// Request A - event_loop.rs line 157
update_global_invocation_context("req-A", "arn-A");
// Global now has "req-A" ✅

// Request B arrives - event_loop.rs line 157
update_global_invocation_context("req-B", "arn-B");
// Global now has "req-B" ← OVERWROTE req-A! ❌

// Request A tries to log - logs/processor.rs
if let Ok(ctx) = self.invocation_context.lock() {
    // Reads "req-B" because global was overwritten!
    log.request_id = ctx.request_id;  // ❌ WRONG!
}
```

---

## What Needs to Change

### Delete These

```rust
// 1. Delete from main.rs
static CURRENT_INVOCATION_CONTEXT: ...;  // ❌ DELETE

// 2. Delete from event_loop.rs
fn update_global_invocation_context(...) { }  // ❌ DELETE

// 3. Delete from logs/processor.rs
invocation_context: Arc<Mutex<InvocationContext>>,  // ❌ DELETE
```

### Add This

```rust
// 1. New file: src/context_manager.rs
pub struct ContextManager {
    contexts: Arc<DashMap<String, RequestContext>>,  // ✅ Per-request
}

static CONTEXT_MANAGER: Lazy<ContextManager> = ...;
```

### Replace Everywhere

```rust
// Old (many places):
update_global_invocation_context(&request_id, &arn);
self.invocation_context.lock().unwrap()...

// New (one place):
ContextManager::global().set_request(request_id, arn);
ContextManager::global().get_request(&request_id)
```

---

## Files with Most Issues

1. **src/event_loop.rs** - 17 references to contexts
2. **src/logs/processor.rs** - Duplicate context storage
3. **src/platform/processor.rs** - Bypass stamping
4. **src/telemetry/listener.rs** - Mixed context reads
5. **src/main.rs** - Global shared context

**All fixed by ContextManager!**

---

## Next Steps

1. Read [SIMPLE_FIX_PLAN.md](./SIMPLE_FIX_PLAN.md) for complete solution
2. Start with creating ContextManager
3. Gradually replace old code
4. Test with concurrent requests
5. Delete old global context

**Result:** Simpler, faster, no bugs!
