/// Test module to help debug telemetry processing locally
use crate::telemetry::listener::TelemetryRecord;
use chrono::Utc;
use serde_json::json;

/// Creates sample function log events for testing
pub fn create_sample_function_logs() -> Vec<TelemetryRecord> {
    vec![
        TelemetryRecord {
            time: Utc::now(),
            record_type: "function".to_string(),
            record: json!({
                "requestId": "test-request-123",
                "level": "INFO",
                "message": "This is a test function log message"
            }),
        },
        TelemetryRecord {
            time: Utc::now(),
            record_type: "function".to_string(),
            record: json!({
                "requestId": "test-request-123",
                "level": "ERROR",
                "message": "This is a test error log message"
            }),
        },
    ]
}

/// Creates sample extension log events for testing
pub fn create_sample_extension_logs() -> Vec<TelemetryRecord> {
    vec![
        TelemetryRecord {
            time: Utc::now(),
            record_type: "extension".to_string(),
            record: json!({
                "requestId": "test-request-123",
                "level": "INFO",
                "message": "Extension is running normally"
            }),
        },
    ]
}

/// Creates sample platform events for testing
pub fn create_sample_platform_events() -> Vec<TelemetryRecord> {
    vec![
        TelemetryRecord {
            time: Utc::now(),
            record_type: "platform.start".to_string(),
            record: json!({
                "requestId": "test-request-123",
                "version": "$LATEST"
            }),
        },
        TelemetryRecord {
            time: Utc::now(),
            record_type: "platform.end".to_string(),
            record: json!({
                "requestId": "test-request-123",
                "status": "success",
                "duration": 1500.0
            }),
        },
    ]
}

/// Simulates processing telemetry records for testing
pub async fn simulate_telemetry_processing(
    log_processor: &crate::logs::processor::LogProcessor,
    platform_processor: &crate::platform::processor::PlatformProcessor,
) {
    tracing::info!("[Test] Simulating telemetry processing...");
    
    // Process function logs
    for record in create_sample_function_logs() {
        log_processor.process_record(record);
    }
    
    // Process extension logs
    for record in create_sample_extension_logs() {
        log_processor.process_record(record);
    }
    
    // Process platform events
    for record in create_sample_platform_events() {
        platform_processor.process_record(record);
    }
    
    tracing::info!("[Test] Finished processing sample telemetry data");
}
