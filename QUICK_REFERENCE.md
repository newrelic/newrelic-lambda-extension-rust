# Quick Reference - Request ID Fix

## The Problem in 30 Seconds

**Current:** 3 different places store request_id → They get out of sync → Logs have wrong request_id

**Solution:** 1 place stores request_id (ContextManager) → Everyone reads from there → Always correct

---

## Current vs Proposed

### Current (Broken)

```rust
// ❌ Global shared - Bug #2
static CURRENT_INVOCATION_CONTEXT = ...;

// ❌ Duplicate in every processor
struct LogProcessor {
    invocation_context: Arc<Mutex<...>>,
}

// ❌ Platform logs bypass stamping - Bug #1
self.log_processor.add_log_to_batch(log);  // No request_id!
```

### Proposed (Fixed)

```rust
// ✅ ONE place, per-request storage
static CONTEXT_MANAGER = ContextManager::new();

// ✅ Processors just track current request
struct LogProcessor {
    current_request_id: Arc<RwLock<Option<String>>>,
}

// ✅ All logs stamped consistently
self.log_processor.stamp_and_add_log(log);  // Has request_id!
```

---

## Key Changes

| Component | Change | Result |
|-----------|--------|--------|
| **ContextManager** | Create new | Single source of truth |
| **LogProcessor** | Remove `invocation_context` | Simpler, no duplication |
| **PlatformProcessor** | Remove `invocation_context` | Simpler, no duplication |
| **EventLoop** | Remove global context updates | Cleaner code |
| **main.rs** | Remove `CURRENT_INVOCATION_CONTEXT` | No shared state |

---

## Implementation Order

1. **Create** `ContextManager` (new file)
2. **Update** LogProcessor to use it
3. **Update** PlatformProcessor to use it
4. **Update** EventLoop to use it
5. **Delete** old global context
6. **Test** concurrent requests

---

## How to Update Logs

### Old Way (Multiple Places)

```rust
// Event loop
update_global_invocation_context(&request_id, &arn);  // Place 1

// Processor creation
let context = Arc::new(Mutex::new(InvocationContext { ... }));  // Place 2
processor.update_invocation_context(context);  // Place 3
```

### New Way (One Place)

```rust
// Event loop - ONE LINE!
ContextManager::global().set_request(request_id, arn);
```

### Usage in Processors

```rust
// Any processor can get context
if let Some(ctx) = ContextManager::global().get_request(&request_id) {
    // Stamp log with ctx.request_id, ctx.invoked_function_arn
}
```

---

## Testing Concurrent Requests

```bash
# Before fix - logs get mixed
curl -X POST lambda-url & curl -X POST lambda-url & curl -X POST lambda-url
# Result: All 3 requests have same request_id ❌

# After fix - logs are correct
curl -X POST lambda-url & curl -X POST lambda-url & curl -X POST lambda-url
# Result: Each request has its own request_id ✅
```

---

## Estimated Effort

- **Design & Code:** 1 day
- **Testing:** 1 day
- **Review & Deploy:** 1 day
- **Total:** 3 days (as part of 5 SP)

---

## Files Modified

```
✅ Create:  src/context_manager.rs         (~150 lines)
✅ Modify:  src/lib.rs                     (+1 line)
✅ Modify:  src/logs/processor.rs          (~50 lines changed, ~30 deleted)
✅ Modify:  src/platform/processor.rs      (~30 lines changed, ~20 deleted)
✅ Modify:  src/event_loop.rs              (~40 lines changed, ~60 deleted)
✅ Delete:  CURRENT_INVOCATION_CONTEXT     (from main.rs)
✅ Modify:  src/request.rs                 (~20 lines changed)
```

**Net Result:** Fewer lines of code, simpler to understand!

---

## Read More

- **[SIMPLE_FIX_PLAN.md](./SIMPLE_FIX_PLAN.md)** - Complete implementation guide
- **[REQUEST_ID_CONSISTENCY_BUG.md](./REQUEST_ID_CONSISTENCY_BUG.md)** - Bug explanation
