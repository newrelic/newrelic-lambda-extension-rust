# Request ID Consistency Bugs - 5 Story Points

**Status:** Known Issue - Solution Available  
**Severity:** HIGH  
**Last Updated:** February 2, 2026

---

## 📖 Documentation Index

- **[QUICK_REFERENCE.md](./QUICK_REFERENCE.md)** - 30-second overview, start here!
- **[CURRENT_CODE_SITUATION.md](./CURRENT_CODE_SITUATION.md)** - What's wrong with current code (with line numbers)
- **[SIMPLE_FIX_PLAN.md](./SIMPLE_FIX_PLAN.md)** - Complete implementation guide
- This document - Bug explanation

---

## Summary

There are **TWO bugs** causing logs to have wrong or missing request IDs:

1. **Race Condition Bug** - Platform logs arrive before we get the request_id from AWS
2. **Concurrent Request Bug** - Multiple requests overwrite each other's request_id

---

## Bug #1: Race Condition (Platform Logs)

### What Happens

AWS sends us logs BEFORE it tells us the request ID.

```
Time 1: AWS Lambda starts Request B
Time 2: Platform log "START Request B" arrives ← We DON'T have req-B yet!
Time 3: AWS tells us request_id = "req-B"      ← Too late!
Time 4: Function logs arrive                    ← These work fine
```

### Diagram

```
AWS Lambda                    Extension
─────────────────────────────────────────────────
1. Request B starts
2. Sends platform.start  ──→  ❌ No request_id yet
                              Logs missing req-B
3. Sends request_id ─────→   ✅ Now we have req-B
4. Function logs arrive ─→   ✅ These get req-B
```

### Result

```json
[
  {"message": "START Request B", "request_id": "❌ missing"},
  {"message": "Processing order", "request_id": "✅ req-B"},
  {"message": "REPORT Request B", "request_id": "❌ missing"}
]
```

### Why No Fix?

AWS controls when events arrive. We can't make them wait.

---

## Bug #2: Concurrent Requests

### What Happens

When multiple requests run at the same time, they share one global variable for request_id. Last one to update WINS and overwrites others.

```
Request A: Sets request_id = "aaa"
Request B: Sets request_id = "bbb" ← Overwrites aaa!
Request C: Sets request_id = "ccc" ← Overwrites bbb!

All logs now get stamped with "ccc" even if they're from Request A or B!
```

### Simple Diagram

```
Request A          Request B          Request C          Global Variable
──────────────────────────────────────────────────────────────────────────
Start              
request_id="aaa" ─────────────────────────────────────→  "aaa"
Logs arriving...   Start
                   request_id="bbb" ──────────────────→  "bbb" (overwrites!)
Reads context ──────────────────────────────────────→  Gets "bbb" ❌ WRONG!
                   Logs arriving...   Start
                                      request_id="ccc"→  "ccc" (overwrites!)
                   Reads context ─────────────────────→  Gets "ccc" ❌ WRONG!
```

### Result

```json
[
  {"message": "[Request A] Order 1", "request_id": "ccc"},  // ❌ Should be aaa
  {"message": "[Request B] Order 2", "request_id": "ccc"},  // ❌ Should be bbb
  {"message": "[Request C] Order 3", "request_id": "ccc"}   // ✅ Correct
]
```

All logs look like they're from Request C!

### When This Happens

- SQS batch processing (3+ messages at once)
- High traffic APIs
- Any time Lambda processes multiple requests simultaneously

---

## Why 5 Story Points?

- **Bug #1**: Can't control AWS timing - no guaranteed fix
- **Bug #2**: Requires refactoring global state to per-request state
- Complex testing with concurrent scenarios
- Risk of data loss or cross-contamination

**Breakdown:** Research & Design (2 SP) + Implementation (2 SP) + Testing (1 SP) = **5 SP**

---

## Proposed Fixes

### Fix for Bug #1 (Partial)

Make platform logs use the same stamping process as function logs. Won't eliminate timing issue but will be consistent.

### Fix for Bug #2 (Full Fix Possible!)

**Stop using shared global variable. Use per-request context.**

```rust
// Current (BAD)
static CURRENT_INVOCATION_CONTEXT = ...;  // ❌ One for ALL requests

// Proposed (GOOD)
REQUEST_CONTEXTS.insert(request_id, context);  // ✅ One per request
```

---

## Code Files to Change

- `src/event_loop.rs` - Remove global context updates
- `src/logs/processor.rs` - Use only per-request context
- `src/platform/processor.rs` - Add stamping logic

---

## Related Documentation

- [SIMPLE_FIX_PLAN.md](./SIMPLE_FIX_PLAN.md) - **START HERE** - Simple solution to fix both bugs
- [REQUEST_ID_FLOW_ANALYSIS.md](./REQUEST_ID_FLOW_ANALYSIS.md) - Detailed technical analysis
- [LOG_SYSTEM_IMPROVEMENT_PLAN.md](./LOG_SYSTEM_IMPROVEMENT_PLAN.md) - Full refactoring plan

---

## Current Situation Summary

### What's Wrong Now

```
Multiple Places Storing Context = CONFUSION!

┌─────────────────────────────────────────────────────┐
│  CURRENT_INVOCATION_CONTEXT (Global Shared) ❌      │
│  ↓                                                   │
│  Gets overwritten by every request                  │
│  → Concurrent requests break each other             │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│  REQUEST_CONTEXTS (Per-request Map) ✅               │
│  ↓                                                   │
│  Good idea but not used everywhere                  │
│  → Sometimes code reads global instead              │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│  LogProcessor.invocation_context (Duplicate) ❌      │
│  ↓                                                   │
│  Each processor has its own copy                    │
│  → Synchronization nightmare                        │
└─────────────────────────────────────────────────────┘
```

### How Logs Get Updated (Current)

```
❌ CURRENT FLOW - Multiple paths, inconsistent

EventLoop receives request_id
    ├→ Updates CURRENT_INVOCATION_CONTEXT (global)
    ├→ Creates context in REQUEST_CONTEXTS (map)
    ├→ Updates LogProcessor.invocation_context (instance)
    └→ Updates PlatformProcessor.invocation_context (instance)

When logging happens:
    Function logs → Read from LogProcessor.invocation_context ✅
    Platform logs → Bypass context, no stamping ❌
    Some code    → Read from CURRENT_INVOCATION_CONTEXT ❌
```

### What We Need (Simple!)

```
✅ PROPOSED FLOW - One path, always consistent

EventLoop receives request_id
    └→ ContextManager.set_request(request_id, arn)  [ONE PLACE!]

When logging happens:
    All logs → ContextManager.get_request(request_id) ✅
            → Stamp with AWS attributes ✅
            → Send to New Relic ✅
```

---

## Solution: Read the Fix Plan

See [SIMPLE_FIX_PLAN.md](./SIMPLE_FIX_PLAN.md) for:
- Complete ContextManager implementation
- Step-by-step migration guide  
- Before/after code examples
- Testing strategy
