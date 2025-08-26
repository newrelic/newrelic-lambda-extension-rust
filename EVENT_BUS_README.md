# New Relic Lambda Extension - Event Bus System

## Architecture

```
AWS Lambda Runtime → Telemetry API → Extension Telemetry Server → Event Bus → New Relic APIs
```

### Components

1. **Event Bus** (`src/event_bus/mod.rs`)
   - Central message passing system for all extension events
   - Handles telemetry events, logs, metrics, and system events
   - Configurable with New Relic endpoint settings

2. **Processors** (`src/event_bus/processor.rs`)
   - **TelemetryProcessor**: Processes AWS Lambda telemetry events
   - **LogProcessor**: Handles function and extension logs
   - **MetricProcessor**: Processes performance metrics

3. **Forwarder** (`src/event_bus/forwarder.rs`)
   - **NewRelicForwarder**: Sends data to New Relic APIs
   - Handles authentication with New Relic license keys
   - Supports multiple endpoints (telemetry, logs, metrics)

4. **Telemetry Listener** (`src/telemetry/listener.rs`)
   - HTTP server that receives telemetry from AWS Lambda
   - Parses telemetry events and sends them to the event bus
   - Supports both legacy and new telemetry formats

## Event Flow

### 1. Telemetry Reception
```rust
AWS Lambda Runtime → POST /telemetry → TelemetryServer → Event Bus
```

### 2. Event Processing
```rust
Event Bus → TelemetryProcessor → NewRelicForwarder → New Relic APIs
```

### 3. Data Transformation
- AWS Lambda telemetry events are converted to New Relic format
- Logs are extracted from telemetry events
- Metrics are extracted from platform reports
- All data includes Lambda context (function name, version, region)

## Configuration

The system uses environment variables for configuration:

### New Relic Endpoints
- `NEW_RELIC_TELEMETRY_ENDPOINT` - Telemetry data endpoint
- `NEW_RELIC_LOG_ENDPOINT` - Log data endpoint  
- `NEW_RELIC_METRIC_ENDPOINT` - Metric data endpoint

### Authentication
- `NEW_RELIC_LICENSE_KEY` - Required for API authentication

### Feature Flags
- `NEW_RELIC_LAMBDA_EXTENSION_ENABLED` - Enable/disable extension
- `NEW_RELIC_EXTENSION_SEND_FUNCTION_LOGS` - Forward function logs
- `NEW_RELIC_EXTENSION_SEND_EXTENSION_LOGS` - Forward extension logs

## Event Types

### Telemetry Events
- `platform.start` - Function invocation start
- `platform.end` - Function invocation end
- `platform.report` - Function execution metrics
- `platform.initStart` - Cold start initialization
- `platform.initReport` - Initialization metrics
- `function` - Function log output
- `extension` - Extension log output

### Processed Data Formats

#### Telemetry Data
```json
{
  "timestamp": 1693123456789,
  "eventType": "LambdaTelemetry",
  "source": "newrelic-lambda-extension",
  "functionName": "my-function",
  "functionVersion": "$LATEST",
  "region": "us-east-1",
  "telemetryType": "platform.report",
  "requestId": "abc-123-def",
  "metrics": {
    "durationMs": 2599.4,
    "billedDurationMs": 2600,
    "memorySizeMB": 128,
    "maxMemoryUsedMB": 94
  }
}
```

#### Log Data
```json
{
  "timestamp": 1693123456789,
  "level": "INFO",
  "message": "Function executed successfully",
  "logType": "function",
  "functionName": "my-function",
  "functionVersion": "$LATEST",
  "requestId": "abc-123-def",
  "source": "lambda-function"
}
```

#### Metric Data
```json
{
  "timestamp": 1693123456789,
  "name": "lambda.duration",
  "value": 2599.4,
  "unit": "milliseconds",
  "tags": {
    "functionName": "my-function",
    "functionVersion": "$LATEST",
    "metricType": "duration"
  }
}
```

## Usage

### Basic Setup
```bash
# Set required environment variables
export NEW_RELIC_LICENSE_KEY="your-license-key"
export NEW_RELIC_TELEMETRY_ENDPOINT="https://log-api.newrelic.com/log/v1"
export NEW_RELIC_LOG_ENDPOINT="https://log-api.newrelic.com/log/v1"
export NEW_RELIC_METRIC_ENDPOINT="https://metric-api.newrelic.com/metric/v1"

# Deploy the extension
./deploy.sh
```

### Development
```bash
# Build the project
cargo build

# Run with debug logging
RUST_LOG=debug cargo run
```

## Monitoring

The extension provides detailed logging for monitoring data flow:

```
🚌 [EventBus] Starting event bus with telemetry and log forwarding
📡 [EventBus] Telemetry endpoint: https://log-api.newrelic.com/log/v1
📝 [EventBus] Log endpoint: https://log-api.newrelic.com/log/v1
📊 [EventBus] Metric endpoint: https://metric-api.newrelic.com/metric/v1
🔄 [TelemetryProcessor] Processing telemetry event: PlatformStart
📤 [TelemetryProcessor] Forwarding telemetry to New Relic
✅ [TelemetryProcessor] Successfully processed telemetry event
```

## Error Handling

- Network failures are logged but don't stop the extension
- Malformed telemetry events are logged and skipped
- Missing license keys result in warning logs
- The extension continues to operate even with New Relic API failures

## Performance

- Asynchronous processing of all events
- Non-blocking telemetry forwarding
- Configurable batch sizes and timeouts
- Memory-efficient event processing

## Future Enhancements

1. **Batching**: Implement batch sending for improved performance
2. **Retries**: Add retry logic for failed API calls
3. **Buffering**: Add local buffering for network outages
4. **Metrics**: Add extension self-monitoring metrics
5. **Health Checks**: Periodic New Relic API health checks

## Troubleshooting

### Common Issues

1. **No data in New Relic**
   - Check `NEW_RELIC_LICENSE_KEY` is set
   - Verify endpoint URLs are correct
   - Check extension logs for errors

2. **High memory usage**
   - Adjust `MAX_EVENTS` in constants.rs
   - Monitor event bus queue size

3. **Network timeouts**
   - Check New Relic API connectivity
   - Adjust timeout settings in configuration

### Debug Logging
```bash
export RUST_LOG=newrelic_lambda_extension=debug
```

This enables detailed logging of the event flow and API interactions.
