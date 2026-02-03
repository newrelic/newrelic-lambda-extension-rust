# How Concurrent Request Isolation Works

## The Key: DashMap with request_id as Key

```rust
// ContextManager structure:
pub struct ContextManager {
    contexts: DashMap<String, RequestContext>,  // ← Key is request_id!
    function_arn: RwLock<Option<String>>,
}
```

## Example: 3 Concurrent Requests

### Time T1: Request A Arrives
```
Event Loop receives: { request_id: "req-a-123", arn: "arn:..." }

1. ContextManager.set_request("req-a-123", None)
   → contexts["req-a-123"] = { request_id: "req-a-123", trace_id: None }

2. create_request_processing_state("req-a-123", ...)
   → Creates LogProcessor instance for "req-a-123"
   → Creates PlatformProcessor instance for "req-a-123"  
   → Stores in REQUEST_PROCESSORS["req-a-123"]
```

### Time T2: Request B Arrives (while A is still running!)
```
Event Loop receives: { request_id: "req-b-456", arn: "arn:..." }

1. ContextManager.set_request("req-b-456", None)
   → contexts["req-b-456"] = { request_id: "req-b-456", trace_id: None }

2. create_request_processing_state("req-b-456", ...)
   → Creates SEPARATE LogProcessor instance for "req-b-456"
   → Creates SEPARATE PlatformProcessor instance for "req-b-456"
   → Stores in REQUEST_PROCESSORS["req-b-456"]
```

### Time T3: Request C Arrives (A and B still running!)
```
Event Loop receives: { request_id: "req-c-789", arn: "arn:..." }

1. ContextManager.set_request("req-c-789", None)
   → contexts["req-c-789"] = { request_id: "req-c-789", trace_id: None }

2. create_request_processing_state("req-c-789", ...)
   → Creates SEPARATE LogProcessor instance for "req-c-789"
   → Creates SEPARATE PlatformProcessor instance for "req-c-789"
   → Stores in REQUEST_PROCESSORS["req-c-789"]
```

## Current State in Memory

```
ContextManager.contexts (DashMap):
├─ "req-a-123" → { request_id: "req-a-123", trace_id: None }
├─ "req-b-456" → { request_id: "req-b-456", trace_id: None }
└─ "req-c-789" → { request_id: "req-c-789", trace_id: None }

REQUEST_PROCESSORS (DashMap):
├─ "req-a-123" → { log_processor_a, platform_processor_a, ... }
├─ "req-b-456" → { log_processor_b, platform_processor_b, ... }
└─ "req-c-789" → { log_processor_c, platform_processor_c, ... }

REQUEST_AGENT_BUFFERS (DashMap):
├─ "req-a-123" → [payload1, payload2]
├─ "req-b-456" → [payload3]
└─ "req-c-789" → []
```

## How Logs Get Isolated

### Request A's Log Processing
```
1. Telemetry API receives log: { message: "Processing order", ... }
   → Log doesn't have request_id yet!

2. Find which request this log belongs to:
   → Uses timestamps and request timing to associate log with "req-a-123"

3. LogProcessor for "req-a-123" stamps the log:
   → Needs to call: ContextManager.get_request("req-a-123")
   → Gets back: { request_id: "req-a-123", trace_id: None }
   → Stamps log with request_id="req-a-123"

4. Log sent to New Relic with correct request_id ✅
```

### Request B's Log Processing (concurrent!)
```
1. Telemetry API receives log: { message: "Validating user", ... }
   → Log doesn't have request_id yet!

2. Find which request this log belongs to:
   → Uses timestamps and request timing to associate log with "req-b-456"

3. LogProcessor for "req-b-456" stamps the log:
   → Needs to call: ContextManager.get_request("req-b-456")  ← Different key!
   → Gets back: { request_id: "req-b-456", trace_id: None }
   → Stamps log with request_id="req-b-456"

4. Log sent to New Relic with correct request_id ✅
```

**No interference!** Each processor looks up its OWN request_id from the map.

## How Agent Payloads Get Isolated

### Request A's Agent Payload
```
1. Agent sends payload (lacks request_id)
2. route_payload_to_request_buffer() determines this belongs to "req-a-123"
3. Payload added to REQUEST_AGENT_BUFFERS["req-a-123"]
4. When sending:
   → get_request("req-a-123") → { request_id: "req-a-123", ... }
   → Payload stamped with "req-a-123" ✅
```

### Request B's Agent Payload (concurrent!)
```
1. Agent sends payload (lacks request_id)
2. route_payload_to_request_buffer() determines this belongs to "req-b-456"
3. Payload added to REQUEST_AGENT_BUFFERS["req-b-456"]  ← Different buffer!
4. When sending:
   → get_request("req-b-456") → { request_id: "req-b-456", ... }
   → Payload stamped with "req-b-456" ✅
```

**No cross-contamination!** Each payload goes to the correct buffer.

## The Old Problem (Before ContextManager)

```
CURRENT_INVOCATION_CONTEXT (Global shared variable):
{ request_id: ???, arn: "...", trace_id: None }  ← Only ONE value!

Request A arrives:
  CURRENT_INVOCATION_CONTEXT = { request_id: "req-a-123", ... }

Request B arrives:  CURRENT_INVOCATION_CONTEXT = { request_id: "req-b-456", ... }  ← Overwrites A!

Request A tries to stamp logs:
  Reads CURRENT_INVOCATION_CONTEXT
  Gets request_id="req-b-456"  ← WRONG! Should be "req-a-123"
  Logs stamped with wrong request_id ❌
```

## The New Solution (With ContextManager)

```
ContextManager.contexts (DashMap - can hold multiple entries):
{
  "req-a-123": { request_id: "req-a-123", ... },
  "req-b-456": { request_id: "req-b-456", ... },
  "req-c-789": { request_id: "req-c-789", ... },
}

Request A stamps logs:
  Calls: ContextManager.get_request("req-a-123")
  Gets: { request_id: "req-a-123", ... }  ← Correct!

Request B stamps logs (concurrent):
  Calls: ContextManager.get_request("req-b-456")
  Gets: { request_id: "req-b-456", ... }  ← Also correct!

Request C stamps logs (concurrent):
  Calls: ContextManager.get_request("req-c-789")
  Gets: { request_id: "req-c-789", ... }  ← Also correct!
```

**Key difference:** Instead of ONE shared variable, we have a MAP with separate entries per request!

## Phase 3 (Next): Update Processors

Currently processors still do this:
```rust
// OLD WAY (what we need to fix in Phase 3):
fn stamp_log(&self, log: LogMessage) {
    let ctx = self.invocation_context.lock().unwrap();  ← Shared state!
    log.request_id = ctx.request_id;
}
```

After Phase 3, processors will do this:
```rust
// NEW WAY (what Phase 3 will implement):
fn stamp_log(&self, log: LogMessage, current_request_id: &str) {
    if let Some(ctx) = ContextManager::global().get_request(current_request_id) {
        log.request_id = ctx.request_id;  ← Looks up from map!
    }
}
```

## Summary

**Your question:** "How do concurrent requests isolate their logs and payloads?"

**Answer:**
1. Each request gets its own entry in ContextManager.contexts DashMap
2. Each request gets its own processors in REQUEST_PROCESSORS DashMap
3. Each request gets its own buffer in REQUEST_AGENT_BUFFERS DashMap
4. When processing logs/payloads, we pass the **request_id as a parameter**
5. Processors use that request_id to look up the **correct entry** from the map
6. No shared global state → No overwriting → No cross-contamination!

**The magic:** DashMap allows concurrent reads/writes without locking, so Request A looking up "req-a-123" doesn't block Request B looking up "req-b-456"!
