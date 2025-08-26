# Event Bus System Documentation

## Overview

The New Relic Lambda Extension now includes a comprehensive event bus system that handles telemetry data forwarding to New Relic endpoints based on environment configuration.

### Components

1. **Event Bus (`src/event_bus/mod.rs`)**
   - Central message passing system
   - Handles routing of different event types
   - Manages event processing lifecycle

2. **Event Processors (`src/event_bus/processor.rs`)**
   - `TelemetryProcessor`: Processes AWS Lambda telemetry events
   - `LogProcessor`: Handles function and extension logs
   - `MetricProcessor`: Processes metric data

3. **Forwarder (`src/event_bus/forwarder.rs`)**
   - `NewRelicForwarder`: Sends data to New Relic APIs
   - Handles authentication and endpoint routing
   - Supports batching and retry logic

4. **Telemetry Events (`src/telemetry/events.rs`)**
   - Data structures for AWS Lambda telemetry
   - Comprehensive event type definitions
   - Serde serialization/deserialization

## Configuration

The system uses environment variables to configure New Relic endpoints:

### Telemetry Endpoint
```bash
export NEW_RELIC_TELEMETRY_ENDPOINT="https://telemetry-api.newrelic.com/telemetry/v1"
```

### Log Endpoint
```bash
export NEW_RELIC_LOG_ENDPOINT="https://log-api.newrelic.com/log/v1"
```

### Metric Endpoint
```bash
export NEW_RELIC_METRIC_ENDPOINT="https://metric-api.newrelic.com/metric/v1"
```

### License Key
```bash
export NEW_RELIC_LICENSE_KEY="your-license-key-here"
```

## Event Flow

```
AWS Lambda Runtime
       ↓
Telemetry API → Telemetry Server → Event Bus → Processors → Forwarder → New Relic APIs
                                       ↓
                              Event Processing:
                              - Extract logs
                              - Extract metrics  
                              - Convert formats
                              - Route to endpoints
```

## Event Types

The event bus handles the following event types:

### 1. Telemetry Events
- Platform events (start, end, report, etc.)
- Function logs
- Extension logs
- Initialization events

### 2. Function Logs
- Application log messages
- Request-scoped logging
- Structured logging support

### 3. Extension Logs
- Extension system messages
- Debug information
- Status updates

### 4. Metrics
- Duration metrics
- Memory usage
- Custom metrics
- Performance counters

### 5. Special Events
- Out of memory detection
- Shutdown signals

## Features

### Data Transformation
- Converts AWS Lambda telemetry to New Relic format
- Extracts structured data from logs
- Generates metrics from platform events
- Adds context and metadata

### Endpoint Routing
- **Telemetry data** → `NEW_RELIC_TELEMETRY_ENDPOINT`
- **Log data** → `NEW_RELIC_LOG_ENDPOINT`
- **Metric data** → `NEW_RELIC_METRIC_ENDPOINT`

### Error Handling
- Graceful degradation on endpoint failures
- Comprehensive error logging
- Retry logic for transient failures

### Performance
- Asynchronous processing
- Efficient memory usage
- Configurable batching
- Non-blocking operation

## Usage Example

The event bus is automatically started in `main.rs`:

```rust
// Create event bus with configuration
let event_bus = EventBus::new(Arc::clone(&config_arc));
let event_bus_sender = event_bus.get_sender();

// Start processing loop
let event_bus_handle = tokio::spawn(async move {
    event_bus.run().await;
});

// Integrate with telemetry server
let telemetry_server = Arc::new(TelemetryServer::with_event_bus(event_bus_sender));
```

## New Relic Integration

### Authentication
All requests to New Relic APIs include:
- `X-License-Key`: Your New Relic license key
- `Content-Type`: application/json
- `User-Agent`: newrelic-lambda-extension/version

### Data Format
Data sent to New Relic follows this structure:

#### Telemetry Events
```json
{
  "timestamp": 1640995200000,
  "eventType": "LambdaTelemetry",
  "source": "newrelic-lambda-extension",
  "functionName": "my-function",
  "telemetryType": "platform.start",
  "requestId": "abc-123"
}
```

#### Log Events
```json
{
  "timestamp": 1640995200000,
  "level": "INFO",
  "message": "Function executed successfully",
  "logType": "function",
  "functionName": "my-function",
  "source": "lambda-function"
}
```

#### Metric Events
```json
{
  "timestamp": 1640995200000,
  "name": "lambda.duration",
  "value": 1234.5,
  "unit": "milliseconds",
  "tags": {
    "functionName": "my-function",
    "metricType": "duration"
  }
}
```

## Configuration Options

The system respects several configuration options:

- `NEW_RELIC_LAMBDA_EXTENSION_ENABLED`: Enable/disable the extension
- `NEW_RELIC_EXTENSION_SEND_FUNCTION_LOGS`: Control function log forwarding
- `NEW_RELIC_EXTENSION_SEND_EXTENSION_LOGS`: Control extension log forwarding
- `NEW_RELIC_DATA_COLLECTION_TIMEOUT`: Set collection timeout
- `NEW_RELIC_EXTENSION_LOG_LEVEL`: Set logging level

## Monitoring

The event bus provides detailed logging for monitoring:

```
🚌 [EventBus] Starting event bus with telemetry and log forwarding
📡 [EventBus] Telemetry endpoint: https://telemetry-api.newrelic.com/telemetry/v1
📝 [EventBus] Log endpoint: https://log-api.newrelic.com/log/v1
📊 [EventBus] Metric endpoint: https://metric-api.newrelic.com/metric/v1
🔄 [EventBus] Processing event #1: Telemetry(...)
📤 [NewRelicForwarder] Sending telemetry to endpoint
✅ [NewRelicForwarder] Successfully sent telemetry to New Relic
```

## Development

To extend the event bus:

1. Add new event types to `Event` enum in `mod.rs`
2. Implement processing logic in appropriate processor
3. Update forwarder if new endpoints are needed
4. Add configuration options in `config/mod.rs`

## Testing

The system includes comprehensive telemetry event parsing and processing. Test events can be found in the `events.rs` module tests.

## Compatibility

The event bus maintains backward compatibility with existing telemetry record formats while providing enhanced processing capabilities.
