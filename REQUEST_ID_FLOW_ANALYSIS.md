# Request ID Flow Analysis - APM Mode vs Standard Mode

## Overview

This document details how request IDs are stamped on logs in both APM and Standard modes, highlighting the critical bug where platform logs bypass AWS attribute stamping.

---

## 1. APM Mode - Invoke Event Flow

```mermaid
sequenceDiagram
    participant AWS as AWS Lambda Runtime
    participant Listener as Telemetry Listener<br/>(listener.rs)
    participant EventLoop as Event Loop<br/>(event_loop.rs)
    participant PlatProc as Platform Processor<br/>(platform/processor.rs)
    participant LogProc as Log Processor<br/>(logs/processor.rs)
    participant Batch as Log Batch

    Note over AWS: Request A arrives
    
    AWS->>EventLoop: INVOKE event<br/>request_id: "aaa-111"
    
    par Platform Event (Async)
        AWS->>Listener: platform.start<br/>"START RequestId: aaa-111"
        Listener->>PlatProc: process_platform_event()
        Note over PlatProc: ❌ BUG: Uses OLD context<br/>from previous request
        PlatProc->>PlatProc: Create log with<br/>basic attributes
        PlatProc->>LogProc: add_log_to_batch()<br/>❌ NO AWS ATTRIBUTES
        LogProc->>Batch: Push log<br/>Missing aws.lambda_request_id
    end
    
    EventLoop->>EventLoop: update_last_request_context()<br/>Line 156
    EventLoop->>LogProc: process_buffered_logs()<br/>Line 157-159
    EventLoop->>EventLoop: create_request_processing_state()<br/>Line 162-166<br/>✅ NEW context with "aaa-111"
    EventLoop->>LogProc: update_invocation_context()<br/>Line 169<br/>✅ LogProc now has correct context
    EventLoop->>LogProc: process_pre_invoke_logs()<br/>Line 170-171
    
    Note over AWS: Lambda function executes
    
    AWS->>Listener: Function log: "hello world"
    Listener->>LogProc: add_telemetry_record()
    LogProc->>LogProc: apply_current_invocation_metadata()<br/>✅ STAMPS aws.lambda_request_id: "aaa-111"
    LogProc->>Batch: Push log with AWS attributes
    
    AWS->>Listener: platform.report<br/>RequestId: aaa-111
    Listener->>PlatProc: process_platform_event()
    PlatProc->>PlatProc: Create log with<br/>basic attributes
    PlatProc->>LogProc: add_log_to_batch()<br/>❌ NO AWS ATTRIBUTES
    LogProc->>Batch: Push log<br/>Missing aws.lambda_request_id
    
    EventLoop->>EventLoop: cleanup_request_processing_state()<br/>Line 988
    EventLoop->>LogProc: send_batch()
    
    Note over Batch: Batch contains MIXED request IDs:<br/>Platform logs: no aws.lambda_request_id<br/>Function logs: aws.lambda_request_id: "aaa-111"
```

---

## 2. Standard Mode - Invoke Event Flow

```mermaid
sequenceDiagram
    participant AWS as AWS Lambda Runtime
    participant Listener as Telemetry Listener
    participant EventLoop as Event Loop<br/>(event_loop.rs)
    participant PlatProc as Platform Processor
    participant LogProc as Log Processor
    participant Batch as Log Batch
    participant Channel as runtime.done<br/>Channel

    Note over AWS: Request A arrives
    
    AWS->>EventLoop: INVOKE event<br/>request_id: "bbb-222"
    
    EventLoop->>EventLoop: create_request_processing_state()<br/>✅ NEW context with "bbb-222"
    EventLoop->>Channel: Create RUNTIME_DONE_CHANNELS["bbb-222"]
    
    par Platform Events (Async)
        AWS->>Listener: platform.start<br/>"START RequestId: bbb-222"
        Listener->>PlatProc: process_platform_event()
        Note over PlatProc: ❌ BUG: May use stale context
        PlatProc->>PlatProc: Store metrics (always)
        PlatProc->>LogProc: add_log_to_batch()<br/>❌ NO AWS ATTRIBUTES
        LogProc->>Batch: Push log
    end
    
    Note over AWS: Lambda function executes
    
    AWS->>Listener: Function log: "processing..."
    Listener->>LogProc: add_telemetry_record()
    LogProc->>LogProc: apply_current_invocation_metadata()<br/>✅ STAMPS aws.lambda_request_id: "bbb-222"
    LogProc->>Batch: Push log with AWS attributes
    
    AWS->>Listener: platform.runtimeDone<br/>RequestId: bbb-222
    Listener->>Channel: Send signal to RUNTIME_DONE_CHANNELS["bbb-222"]
    Channel->>EventLoop: Wake up waiting task
    
    AWS->>Listener: platform.report<br/>RequestId: bbb-222
    Listener->>PlatProc: process_platform_event()
    PlatProc->>PlatProc: Store metrics
    PlatProc->>LogProc: add_log_to_batch()<br/>❌ NO AWS ATTRIBUTES
    LogProc->>Batch: Push log
    
    EventLoop->>LogProc: send_batch()
    
    Note over Batch: Batch contains MIXED request IDs:<br/>Platform logs: no aws.lambda_request_id<br/>Function logs: aws.lambda_request_id: "bbb-222"
```

---

## 3. Current Bug - Platform Log Processing Path

```mermaid
flowchart TD
    A[Platform Event Arrives<br/>platform.start, platform.report] --> B{Event Type?}
    
    B -->|platform.start<br/>platform.report<br/>platform.runtimeDone| C[telemetry/listener.rs<br/>Line 243]
    
    C --> D[platform_processor.process_platform_event<br/>platform/processor.rs Line 55]
    
    D --> E[process_platform_event_internal<br/>Line 62]
    
    E --> F[Store platform metrics<br/>Lines 59-74<br/>✅ Always happens]
    
    E --> G[Check for errors<br/>Line 78<br/>✅ Always happens]
    
    E --> H{send_platform_logs<br/>flag enabled?}
    
    H -->|Yes| I[Create log_message<br/>Lines 82-95<br/>❌ NO AWS ATTRIBUTES]
    
    I --> J[log_processor.add_log_to_batch<br/>Line 103<br/>❌ BYPASSES STAMPING]
    
    J --> K[Log Batch<br/>Missing aws.lambda_request_id]
    
    style I fill:#ffcccc
    style J fill:#ffcccc
    style K fill:#ffcccc
```

---

## 4. Correct Flow - Function/Extension Log Processing Path

```mermaid
flowchart TD
    A[Function/Extension Log Arrives] --> B[telemetry/listener.rs<br/>handle_telemetry_request]
    
    B --> C[log_processor.add_telemetry_record<br/>logs/processor.rs Line 203]
    
    C --> D{Log Type?}
    
    D -->|platform.*| E[Skip - handled by<br/>platform processor]
    
    D -->|function<br/>extension| F[Parse telemetry record<br/>Lines 205-408]
    
    F --> G[apply_current_invocation_metadata<br/>Line 410<br/>✅ STAMPS AWS ATTRIBUTES]
    
    G --> H[Stamping Logic<br/>Lines 155-169]
    
    H --> I{Has valid<br/>request_id?}
    
    I -->|Yes| J[Create nested structure<br/>aws.lambda_request_id<br/>faas.execution<br/>faas.arn]
    
    J --> K{is_log_complete?<br/>Line 430}
    
    K -->|Yes| L[add_log_to_batch<br/>Line 445]
    
    K -->|No| M[Buffer in pre_invoke_logs<br/>Line 437]
    
    L --> N[Log Batch<br/>✅ WITH AWS ATTRIBUTES]
    
    style G fill:#ccffcc
    style J fill:#ccffcc
    style N fill:#ccffcc
```

---

## 5. The Problem: Two Different Code Paths

### Function/Extension Logs (CORRECT PATH)
```rust
// logs/processor.rs
pub fn add_telemetry_record(&self, record: TelemetryRecord) {
    // ... parse record ...
    
    // ✅ STAMPS AWS ATTRIBUTES HERE
    let log_message = self.apply_current_invocation_metadata(log_message);
    
    if self.is_log_complete(&log_message) {
        self.add_log_to_batch(log_message);  // Now has AWS attributes
    }
}

fn apply_current_invocation_metadata(&self, mut log_message: payload::LogMessage) 
    -> payload::LogMessage 
{
    if let Some(context) = self.invocation_context.safe_lock() {
        if !context.request_id.is_empty() && context.request_id != "unknown" {
            // ✅ Creates nested AWS structure
            let mut aws_attrs = serde_json::Map::new();
            aws_attrs.insert("lambda_request_id".to_string(),
                serde_json::Value::String(context.request_id.clone()));
            log_message.attributes.insert("aws".to_string(),
                serde_json::Value::Object(aws_attrs));
            
            log_message.attributes.insert("faas.execution".to_string(),
                serde_json::Value::String(context.request_id.clone()));
            log_message.attributes.insert("faas.arn".to_string(),
                serde_json::Value::String(context.invoked_function_arn.clone()));
        }
    }
    log_message
}
```

### Platform Logs (BROKEN PATH)
```rust
// platform/processor.rs
fn process_platform_event_internal(&self, event: &TelemetryEvent) {
    // ✅ Store metrics (always)
    self.store_platform_metrics(event);
    
    // ✅ Check for errors (always)
    self.check_and_send_platform_errors(event);
    
    // Create log if flag enabled
    if self.config.extension.send_platform_logs {
        let mut attributes = serde_json::Map::new();
        attributes.insert("log_type".to_string(), serde_json::json!("platform"));
        // ... add other attributes ...
        
        // ❌ NO AWS ATTRIBUTES ADDED HERE
        
        let log_message = crate::newrelic::payload::LogMessage {
            timestamp,
            message,
            attributes,  // ❌ Missing aws.lambda_request_id
        };
        
        // ❌ DIRECTLY TO BATCH - BYPASSES apply_current_invocation_metadata
        self.log_processor.add_log_to_batch(log_message);
    }
}
```

---

## 6. Why Request IDs Get Mixed Up

### Scenario Timeline

| Time | Event | Context State | What Gets Logged |
|------|-------|---------------|------------------|
| T0 | Request A completes | request_id: "aaa-111" | - |
| T1 | Request B INVOKE arrives | request_id: "aaa-111" (old) | - |
| T2 | platform.start for B arrives async | request_id: "aaa-111" (old) | ❌ Platform log with NO aws.lambda_request_id |
| T3 | Event loop creates new context | request_id: "bbb-222" (new) | - |
| T4 | Event loop updates LogProc context | request_id: "bbb-222" (new) | - |
| T5 | Function log arrives | request_id: "bbb-222" (new) | ✅ Function log with aws.lambda_request_id: "bbb-222" |
| T6 | platform.report arrives | request_id: "bbb-222" (new) | ❌ Platform log with NO aws.lambda_request_id |
| T7 | Batch sent | - | **MIXED: Some logs have correct request_id, others have none** |

### Result in Batch
```json
[
  {
    "message": "START RequestId: bbb-222",
    "log_type": "platform",
    // ❌ NO aws.lambda_request_id
    // ❌ NO faas.execution
    // ❌ NO faas.arn
  },
  {
    "message": "hello from function",
    "log_type": "function",
    "aws": { "lambda_request_id": "bbb-222" },  // ✅ Correct
    "faas.execution": "bbb-222",                 // ✅ Correct
    "faas.arn": "arn:aws:lambda:..."             // ✅ Correct
  },
  {
    "message": "REPORT RequestId: bbb-222...",
    "log_type": "platform",
    // ❌ NO aws.lambda_request_id
    // ❌ NO faas.execution
    // ❌ NO faas.arn
  }
]
```

### What Happens at New Relic Ingestion

When this batch is sent to New Relic Logs API, the validation might:

1. **Reject incomplete logs** → Platform logs get dropped
2. **Try to infer request_id** → Platform logs get wrong request_id from pre-invoke processing
3. **Group by common attributes** → Logs from different requests get mixed together
4. **Fail batch** → Entire batch rejected

---

## 7. Context Update Timing

### APM Mode Context Flow

```mermaid
flowchart TD
    A[INVOKE Event Arrives<br/>event_loop.rs Line 156] --> B[update_last_request_context<br/>Updates LAST_REQUEST_CONTEXT]
    
    B --> C[process_buffered_logs<br/>Lines 157-159<br/>Sends previous request logs]
    
    C --> D[create_request_processing_state<br/>Lines 162-166<br/>Creates NEW context]
    
    D --> E[global_log_processor.update_invocation_context<br/>Line 169<br/>Updates LogProc context]
    
    E --> F[process_pre_invoke_logs<br/>Lines 170-171<br/>Stamps INIT logs with new context]
    
    style D fill:#90EE90
    style E fill:#90EE90
    
    G[Platform Event Arrives<br/>ASYNC - ANY TIME] --> H{When does it arrive?}
    
    H -->|Before Line 169| I[❌ Uses OLD context<br/>Platform log has wrong/no request_id]
    
    H -->|After Line 169| J[✅ Uses NEW context<br/>But still bypasses stamping!]
    
    style I fill:#ffcccc
    style J fill:#ffffcc
```

### Standard Mode Context Flow

```mermaid
flowchart TD
    A[INVOKE Event Arrives<br/>event_loop.rs] --> B[create_request_processing_state<br/>Creates NEW context immediately]
    
    B --> C[Store in REQUEST_PROCESSORS<br/>Per-request context]
    
    C --> D[Create RUNTIME_DONE_CHANNELS<br/>For this request_id]
    
    style B fill:#90EE90
    
    E[Platform Event Arrives<br/>ASYNC] --> F{When does it arrive?}
    
    F -->|Early| G[❌ Context might not be ready<br/>Platform log has no request_id]
    
    F -->|After context created| H[✅ Context available<br/>But still bypasses stamping!]
    
    style G fill:#ffcccc
    style H fill:#ffffcc
```

---

## 8. The Fix Required

### Make Platform Logs Go Through Same Stamping Path

```mermaid
flowchart TD
    A[Platform Event Arrives] --> B[platform_processor.process_platform_event]
    
    B --> C[Store metrics ✅]
    B --> D[Check errors ✅]
    
    B --> E{send_platform_logs?}
    
    E -->|Yes| F[Create log_message<br/>with basic attributes]
    
    F --> G[log_processor.stamp_and_add_log<br/>NEW METHOD]
    
    G --> H[apply_current_invocation_metadata<br/>✅ STAMPS AWS ATTRIBUTES]
    
    H --> I{is_log_complete?}
    
    I -->|Yes| J[add_log_to_batch<br/>✅ WITH AWS ATTRIBUTES]
    
    I -->|No| K[Buffer in pre_invoke_logs<br/>Wait for request_id]
    
    style G fill:#90EE90
    style H fill:#90EE90
    style J fill:#90EE90
```

### Changes Needed

1. **logs/processor.rs**: Add public method `stamp_and_add_log()` that:
   - Calls `apply_current_invocation_metadata()`
   - Validates with `is_log_complete()`
   - Adds to batch or buffers appropriately

2. **platform/processor.rs**: Change line 103 from:
   ```rust
   self.log_processor.add_log_to_batch(log_message);
   ```
   To:
   ```rust
   self.log_processor.stamp_and_add_log(log_message);
   ```

3. **Consider buffering platform logs**: If context isn't ready, buffer them like function logs instead of adding to batch immediately.

---

## 9. Log Validation Flow

### Current Validation in logs/processor.rs

```rust
fn is_log_complete(&self, log: &payload::LogMessage) -> bool {
    // Check for faas.arn
    let has_arn = log.attributes.get("faas.arn")
        .and_then(|v| v.as_str())
        .map(|s| !s.is_empty())
        .unwrap_or(false);
    
    // Check for nested aws.lambda_request_id
    let has_request_id = log.attributes.get("aws")
        .and_then(|v| v.as_object())
        .and_then(|obj| obj.get("lambda_request_id"))
        .and_then(|v| v.as_str())
        .map(|s| !s.is_empty() && s != "unknown")
        .unwrap_or(false);
    
    has_arn && has_request_id
}
```

### Platform Logs FAIL This Check

Because platform logs bypass `apply_current_invocation_metadata()`, they:
- ❌ Don't have `aws.lambda_request_id`
- ❌ Don't have `faas.execution`
- ❌ Don't have `faas.arn`

This means they're INCOMPLETE and should be buffered or rejected, but currently they go straight to the batch!

---

## 10. Summary

### Current State
- **Function/Extension logs**: ✅ Correctly stamped with AWS attributes via `apply_current_invocation_metadata()`
- **Platform logs**: ❌ Bypass stamping, go directly to batch with incomplete metadata

### Root Cause
Platform logs take a different code path that calls `add_log_to_batch()` directly instead of going through the stamping logic.

### Impact
- Platform logs have no `aws.lambda_request_id` attribute
- Logs from different requests get mixed in the same batch
- New Relic ingestion may reject incomplete logs
- User sees mismatched request IDs in their logs

### Solution
Make platform logs go through the same stamping and validation flow as function/extension logs by creating a new public method in LogProcessor that platform processor can call.

---

## Code References

### Key Files
- **event_loop.rs**: Lines 140-200 (APM mode invoke handling)
- **logs/processor.rs**: 
  - Lines 155-169 (apply_current_invocation_metadata)
  - Lines 203-450 (add_telemetry_record with stamping)
  - Lines 577-597 (is_log_complete validation)
- **platform/processor.rs**: Lines 55-105 (process_platform_event_internal)
- **telemetry/listener.rs**: Lines 100-250 (async HTTP listener)

### Context Management
- **request.rs**: Lines 80-135 (create_request_processing_state)
- **context.rs**: InvocationContext structure with request_id, arn, trace_id
