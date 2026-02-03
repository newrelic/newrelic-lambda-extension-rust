# Visual Solution - Request ID Fix

## Problem Today (3 Different Places!)

```
┌─────────────────────────────────────────────────────────────┐
│ main.rs:60                                                  │
│ static CURRENT_INVOCATION_CONTEXT ❌                        │
│ (Global - WRONG! Shared between ALL requests)               │
└─────────────────────────────────────────────────────────────┘
        ↑ overwrites each other in concurrent requests!
        
┌─────────────────────────────────────────────────────────────┐
│ request.rs:35                                               │
│ static REQUEST_CONTEXTS: DashMap ⚠️                         │
│ (Good idea but underused)                                   │
└─────────────────────────────────────────────────────────────┘
        ↑ some code uses this, some doesn't
        
┌─────────────────────────────────────────────────────────────┐
│ processor.rs:47                                             │
│ struct LogProcessor { invocation_context ❌ }               │
│ (Duplicate copy - gets out of sync!)                        │
└─────────────────────────────────────────────────────────────┘
        ↑ each processor has its own copy = synchronization nightmare!
```

**Result:** Logs get wrong request_id because everyone reads from different places!

---

## Solution (1 Single Place!)

```
┌─────────────────────────────────────────────────────────────┐
│ NEW: src/context_manager.rs                                 │
│                                                             │
│ ContextManager {                                            │
│   contexts: DashMap<request_id, RequestContext> ✅          │
│   function_arn: RwLock<Option<String>>                      │
│ }                                                           │
│                                                             │
│ Methods:                                                    │
│   set_request(id, arn)    → Store context                  │
│   get_request(id)         → Retrieve context               │
│   remove_request(id)      → Cleanup                        │
│   set_function_arn(arn)   → Store function ARN             │
│                                                             │
│ Access: ContextManager::global()                            │
└─────────────────────────────────────────────────────────────┘
         ↑
         │ Everyone reads from HERE and ONLY here!
         │
    ┌────┴────┬────────────┬────────────┐
    │         │            │            │
    V         V            V            V
event_loop  LogProc   PlatformProc  request.rs
```

**Result:** ALL code reads from same place → Always correct request_id!

---

## How Concurrent Requests Work

### Before (Broken)

```
Request A arrives → Updates CURRENT_INVOCATION_CONTEXT = "A"
  ├─ Logs start processing with request_id = "A" ✅
  │
Request B arrives → Updates CURRENT_INVOCATION_CONTEXT = "B" ❌
  │
  ├─ Request A logs NOW read "B" ❌ WRONG!
  └─ Request B logs read "B" ✅

Result: Request A logs get stamped with Request B's ID!
```

### After (Fixed)

```
Request A arrives → ContextManager.set_request("A", arn_a)
  │                 contexts["A"] = { request_id: "A", arn: ... }
  ├─ Logs call: get_request("A") → Always returns "A" ✅
  │
Request B arrives → ContextManager.set_request("B", arn_b)
  │                 contexts["B"] = { request_id: "B", arn: ... }
  │
  ├─ Request A logs: get_request("A") → Still returns "A" ✅
  └─ Request B logs: get_request("B") → Returns "B" ✅

Result: Each request isolated in map, no cross-contamination!
```

---

## Code Changes (Simple!)

### Step 1: Create ContextManager

```rust
// NEW FILE: src/context_manager.rs

pub struct ContextManager {
    contexts: Arc<DashMap<String, RequestContext>>,
    function_arn: Arc<RwLock<Option<String>>>,
}

impl ContextManager {
    pub fn global() -> &'static ContextManager {
        static INSTANCE: Lazy<ContextManager> = Lazy::new(|| ContextManager::new());
        &INSTANCE
    }
    
    pub fn set_request(&self, request_id: String, arn: String) {
        self.contexts.insert(request_id.clone(), RequestContext { 
            request_id, 
            arn 
        });
    }
    
    pub fn get_request(&self, request_id: &str) -> Option<RequestContext> {
        self.contexts.get(request_id).map(|r| r.clone())
    }
    
    pub fn remove_request(&self, request_id: &str) {
        self.contexts.remove(request_id);
    }
}
```

### Step 2: Update Event Loop

```rust
// event_loop.rs - BEFORE
fn update_global_invocation_context(request_id: String, arn: String) {
    let mut ctx = CURRENT_INVOCATION_CONTEXT.lock().unwrap(); // ❌ Global shared
    *ctx = Some(InvocationContext { request_id, arn });
}

// event_loop.rs - AFTER
fn update_context(request_id: String, arn: String) {
    ContextManager::global().set_request(request_id, arn); // ✅ Per-request map
}
```

### Step 3: Update Log Processors

```rust
// logs/processor.rs - BEFORE
pub struct LogProcessor {
    invocation_context: Arc<Mutex<Option<InvocationContext>>>, // ❌ Duplicate
}

impl LogProcessor {
    fn stamp_and_add_log(&mut self, log: LogMessage) {
        let ctx = self.invocation_context.lock().unwrap(); // ❌ Stale data
        // ...
    }
}

// logs/processor.rs - AFTER
pub struct LogProcessor {
    current_request_id: Option<String>, // ✅ Just track which request we're in
}

impl LogProcessor {
    fn stamp_and_add_log(&mut self, log: LogMessage) {
        if let Some(request_id) = &self.current_request_id {
            if let Some(ctx) = ContextManager::global().get_request(request_id) { // ✅ Fresh data
                // Stamp log with ctx.request_id and ctx.arn
            }
        }
        // ...
    }
}
```

### Step 4: Delete Old Code

```rust
// main.rs:60 - DELETE THIS
static CURRENT_INVOCATION_CONTEXT: Mutex<Option<InvocationContext>> = ...;  // ❌ DELETE

// event_loop.rs - DELETE THIS FUNCTION
fn update_global_invocation_context(...) { ... }  // ❌ DELETE
```

---

## Files to Change

| File | Action | Lines Changed |
|------|--------|---------------|
| **src/context_manager.rs** | CREATE | +80 new |
| **src/lib.rs** | Add module | +1 |
| **src/main.rs** | Delete global context | -10 |
| **src/event_loop.rs** | Replace update function | ~30 |
| **src/logs/processor.rs** | Remove context field | ~40 |
| **src/platform/processor.rs** | Remove context field | ~40 |
| **src/request.rs** | Update or merge | ~20 |

**Total:** ~221 lines changed, 1 new file

---

## Testing Plan

### Test 1: Single Request (Basic)
```bash
# Send 1 request
curl -X POST lambda-url

# Check logs: Should have correct request_id
✅ Pass if: All logs show same request_id
```

### Test 2: Concurrent Requests (Key Test!)
```bash
# Send 3 requests at same time
curl -X POST lambda-url/req1 &
curl -X POST lambda-url/req2 &
curl -X POST lambda-url/req3 &

# Check logs: Each request should have its own ID
✅ Pass if: 
  - Request 1 logs all have request_id_1
  - Request 2 logs all have request_id_2
  - Request 3 logs all have request_id_3
  - NO mixing of IDs!
```

### Test 3: High Load
```bash
# Send 500 concurrent requests
for i in {1..500}; do
  curl -X POST lambda-url &
done

# Check: No crashes, all logs stamped correctly
✅ Pass if: All logs have valid request_ids
```

---

## Timeline

| Phase | Time | Tasks |
|-------|------|-------|
| **Phase 1: Create Foundation** | 1 day | Create ContextManager, add module, write tests |
| **Phase 2: Refactor Processors** | 1 day | Update LogProcessor, PlatformProcessor |
| **Phase 3: Clean Event Loop** | 0.5 day | Replace update calls, remove old function |
| **Phase 4: Delete Old Code** | 0.5 day | Remove CURRENT_INVOCATION_CONTEXT, cleanup |
| **Testing** | 1 day | Run all tests, verify concurrent behavior |
| **Total** | **4 days** | With buffer for issues |

---

## Why This Works

### Before: 3 Places (Chaos)
```
Request A → Updates Place 1
Request B → Overwrites Place 1  ❌
           Updates Place 2       ❌
           Forgets Place 3       ❌
           
LogProcessor reads Place 1  → Gets Request B's data! WRONG!
```

### After: 1 Place (Clean)
```
Request A → ContextManager.set_request("A", ...)
           contexts["A"] = {...}  ✅
           
Request B → ContextManager.set_request("B", ...)
           contexts["B"] = {...}  ✅
           
LogProcessor for A → get_request("A") → Always correct! ✅
LogProcessor for B → get_request("B") → Always correct! ✅
```

**Key Insight:** Using a map (DashMap) with request_id as key gives us natural isolation. Each request has its own entry, no overwrites!

---

## Summary

**What:** Replace 3 scattered context stores with 1 ContextManager

**Why:** Eliminate race conditions in concurrent requests

**How:** 
1. Create ContextManager with DashMap (per-request storage)
2. All code calls ContextManager.get_request(id) instead of reading from different places
3. Delete old global context and duplicates

**Result:** ✅ All logs get correct request_id, even with 500 concurrent requests!
