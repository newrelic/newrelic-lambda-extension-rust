# Real-Time Telemetry Processing

This document explains how to use the `RealTimeTelemetryProcessor` to handle Lambda agent telemetry data and send it immediately to New Relic.

## Overview

The `RealTimeTelemetryProcessor` provides:

- **Real-time processing**: Processes telemetry data as soon as it's received from the Lambda agent
- **Compressed data handling**: Automatically decompresses base64 + gzip compressed NR_LAMBDA_MONITORING data
- **Immediate forwarding**: Sends each log event to New Relic immediately (no batching delays)
- **Unix domain socket communication**: Uses efficient IPC with the Lambda agent

## Quick Start

### 1. Create and Configure the Processor

```rust
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::newrelic::telemetry_channel::RealTimeTelemetryProcessor;

// Initialize your components
let config = Arc::new(your_extension_config);
let newrelic_client = Arc::new(NewRelicClient::new());
let invocation_context = Arc::new(Mutex::new(InvocationContext::default()));

// Create the processor
let mut telemetry_processor = RealTimeTelemetryProcessor::new(
    Arc::clone(&newrelic_client),
    Arc::clone(&config),
    Arc::clone(&invocation_context),
);
```

### 2. Start the Processor

```rust
// Start listening for agent telemetry data
telemetry_processor.start().await?;
```

### 3. Stop the Processor (when shutting down)

```rust
// Clean shutdown
telemetry_processor.stop().await;
```

## Integration with Lambda Extension

Add this to your main extension loop:

```rust
async fn main() -> Result<()> {
    // ... your existing setup ...
    
    // Create telemetry processor
    let mut telemetry_processor = RealTimeTelemetryProcessor::new(
        Arc::clone(&newrelic_client),
        Arc::clone(&config),
        Arc::clone(&invocation_context),
    );
    
    // Start it before your main loop
    telemetry_processor.start().await?;
    
    // Your main extension loop
    loop {
        match next_event(&client, &ext_id).await? {
            NextEventResponse::Invoke { request_id, invoked_function_arn } => {
                // Update context for telemetry processor
                {
                    let mut ctx = invocation_context.lock().await;
                    ctx.request_id = request_id;
                    ctx.invoked_function_arn = invoked_function_arn;
                }
                // ... rest of invoke handling ...
            }
            NextEventResponse::Shutdown { shutdown_reason } => {
                // Stop telemetry processor
                telemetry_processor.stop().await;
                break;
            }
        }
    }
    
    Ok(())
}
```

## Agent Payload Format

The processor handles Lambda agent payloads in this format:

```json
{
    "context": {
        "function_name": "my-lambda-function",
        "invoked_function_arn": "arn:aws:lambda:us-east-1:123456789012:function:my-lambda-function",
        "log_group_name": "/aws/lambda/my-lambda-function",
        "log_stream_name": "2024/01/15/[$LATEST]abcd1234efgh5678"
    },
    "entry": "{\"logEvents\":[{\"id\":\"12345\",\"message\":\"[1,\\\"NR_LAMBDA_MONITORING\\\",\\\"H4sIAAAAAAAAA...\\\"]\",\"timestamp\":1705123456789}]}"
}
```

### NR_LAMBDA_MONITORING Format

The processor automatically handles compressed telemetry data in this format:

```json
[1, "NR_LAMBDA_MONITORING", "<base64_gzipped_data>"]
```

The processor will:
1. Detect this format in log messages
2. Decode the base64 data
3. Decompress the gzip data
4. Send the decompressed content to New Relic

## Environment Variables

Configure the processor with these environment variables:

```bash
# Enable/disable telemetry processing
NR_TELEMETRY_ENABLED=true  # default: true

# Standard New Relic configuration
NEW_RELIC_LICENSE_KEY=your_license_key
NEW_RELIC_LOG_ENDPOINT=https://log-api.newrelic.com/log/v1
```

## Socket Configuration

The processor creates a Unix domain socket at:
```
/tmp/newrelic-telemetry.sock
```

The Lambda agent should connect to this socket to send telemetry data.

## Performance Characteristics

- **Low latency**: Processes and forwards data immediately upon receipt
- **Memory efficient**: No batching - processes one event at a time
- **Cold start optimized**: Minimal initialization overhead
- **Async processing**: Non-blocking I/O operations

## Error Handling

The processor includes comprehensive error handling:

- **Connection errors**: Logs and continues listening for new connections
- **Parse errors**: Logs invalid data but continues processing other events
- **Network errors**: Retries failed New Relic API calls
- **Resource cleanup**: Automatically cleans up sockets on shutdown

## Monitoring and Debugging

Enable debug logging to see detailed processing information:

```rust
// The processor logs at various levels:
// INFO: Connection events, successful sends
// DEBUG: Data parsing, compression details  
// WARN: Parse failures, timeouts
// ERROR: Critical failures, API errors
```

## Thread Safety

The processor is fully thread-safe and designed for concurrent access:

- Uses `Arc<Mutex<>>` for shared state
- Spawns background tasks for I/O operations
- Safe to use across multiple async tasks

## Comparison with Batching Approaches

| Feature | RealTimeTelemetryProcessor | Batching Processor |
|---------|---------------------------|-------------------|
| Latency | Immediate | Delayed (batch interval) |
| Memory Usage | Low (single events) | Higher (batch accumulation) |
| Throughput | High (parallel processing) | Moderate (batch overhead) |
| Reliability | High (immediate send) | Risk of data loss |
| Complexity | Simple | More complex state management |

## Troubleshooting

### Common Issues

1. **Socket permission errors**
   - Ensure the Lambda environment allows socket creation in `/tmp/`
   - Check file permissions

2. **Agent not connecting**
   - Verify the agent is configured to use `/tmp/newrelic-telemetry.sock`
   - Check agent logs for connection errors

3. **Data not appearing in New Relic**
   - Verify `NEW_RELIC_LICENSE_KEY` is set correctly
   - Check network connectivity to New Relic endpoints
   - Review processor logs for API errors

4. **High memory usage**
   - The processor is designed for low memory usage
   - If seeing high usage, check for connection leaks
   - Monitor agent connection patterns

### Debug Mode

Set log level to debug for detailed information:

```bash
RUST_LOG=debug ./your-extension
```

This will show:
- Socket creation and binding
- Agent connections and disconnections  
- Data parsing and compression details
- New Relic API calls and responses
