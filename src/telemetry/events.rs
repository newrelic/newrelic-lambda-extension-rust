//! Telemetry Events Module
//!
//! This module contains the data structures for AWS Lambda telemetry events
//! that are received from the Lambda Telemetry API.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Display;

/// Main telemetry event structure received from AWS Lambda
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct TelemetryEvent {
    /// Time when the telemetry was generated
    pub time: DateTime<Utc>,
    /// Telemetry record entry
    #[serde(flatten)]
    pub record: TelemetryRecord,
}

/// Telemetry record types from AWS Lambda
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum TelemetryRecord {
    /// Function log record
    #[serde(rename = "function")]
    Function(Value),

    /// Extension log record
    #[serde(rename = "extension")]
    Extension(Value),

    /// Platform start record
    #[serde(rename = "platform.start")]
    PlatformStart {
        /// Request identifier
        #[serde(rename = "requestId")]
        request_id: String,
        /// Function version
        version: Option<String>,
    },

    /// Platform end record
    #[serde(rename = "platform.end")]
    PlatformEnd {
        /// Request identifier
        #[serde(rename = "requestId")]
        request_id: String,
    },

    /// Platform initialization start record
    #[serde(rename = "platform.initStart")]
    PlatformInitStart {
        /// Type of initialization
        #[serde(rename = "initializationType")]
        initialization_type: InitType,
        /// Initialization phase
        phase: InitPhase,
        /// Runtime version (optional)
        #[serde(rename = "runtimeVersion")]
        runtime_version: Option<String>,
        /// Runtime version ARN (optional)
        #[serde(rename = "runtimeVersionArn")]
        runtime_version_arn: Option<String>,
    },

    /// Platform initialization runtime done record
    #[serde(rename = "platform.initRuntimeDone")]
    PlatformInitRuntimeDone {
        /// Type of initialization
        #[serde(rename = "initializationType")]
        initialization_type: InitType,
        /// Status of the initialization
        status: Status,
        /// Initialization phase (optional)
        phase: Option<InitPhase>,
        /// Error type if initialization failed (optional)
        #[serde(rename = "errorType")]
        error_type: Option<String>,
    },

    /// Platform initialization report record
    #[serde(rename = "platform.initReport")]
    PlatformInitReport {
        /// Type of initialization
        #[serde(rename = "initializationType")]
        initialization_type: InitType,
        /// Initialization phase
        phase: InitPhase,
        /// Metrics for the initialization
        metrics: InitReportMetrics,
    },

    /// Platform runtime done record
    #[serde(rename = "platform.runtimeDone")]
    PlatformRuntimeDone {
        /// Request identifier
        #[serde(rename = "requestId")]
        request_id: String,
        /// Status of the invocation
        status: Status,
        /// When unsuccessful, the error_type describes what kind of error occurred
        #[serde(rename = "errorType")]
        error_type: Option<String>,
        /// Metrics corresponding to the runtime
        metrics: Option<RuntimeDoneMetrics>,
    },

    /// Platform report record
    #[serde(rename = "platform.report")]
    PlatformReport {
        /// Request identifier
        #[serde(rename = "requestId")]
        request_id: String,
        /// Status of the invocation
        status: Status,
        /// When unsuccessful, the error_type describes what kind of error occurred
        #[serde(rename = "errorType")]
        error_type: Option<String>,
        /// Report metrics
        metrics: ReportMetrics,
    },

    /// Extension-specific record
    #[serde(rename = "platform.extension")]
    PlatformExtension {
        /// Name of the extension
        name: String,
        /// State of the extension
        state: String,
        /// Events sent to the extension
        events: Vec<String>,
    },

    /// Telemetry processor-specific record
    #[serde(rename = "platform.telemetrySubscription")]
    PlatformTelemetrySubscription {
        /// Name of the extension
        name: String,
        /// State of the extension
        state: String,
        /// Types of records sent to the extension
        types: Vec<String>,
    },

    /// Record generated when the telemetry processor is falling behind
    #[serde(rename = "platform.logsDropped", rename_all = "camelCase")]
    PlatformLogsDropped {
        /// Reason for dropping the logs
        reason: String,
        /// Number of records dropped
        dropped_records: u64,
        /// Total size of the dropped records
        dropped_bytes: u64,
    },
}

/// Initialization type
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum InitType {
    /// On-demand initialization
    OnDemand,
    /// Provisioned concurrency initialization
    ProvisionedConcurrency,
    /// SnapStart initialization
    SnapStart,
}

impl Display for InitType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let style = match self {
            InitType::OnDemand => "on-demand",
            InitType::ProvisionedConcurrency => "provisioned-concurrency",
            InitType::SnapStart => "SnapStart",
        };
        write!(f, "{style}")
    }
}

/// Initialization phase
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum InitPhase {
    /// Init phase
    Init,
    /// Invoke phase
    Invoke,
}

impl Display for InitPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let style = match self {
            InitPhase::Init => "init",
            InitPhase::Invoke => "invoke",
        };
        write!(f, "{style}")
    }
}

/// Status of an operation
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    /// Successful operation
    Success,
    /// Failed operation
    Error,
    /// Timeout
    Timeout,
}

impl Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let style = match self {
            Status::Success => "success",
            Status::Error => "error",
            Status::Timeout => "timeout",
        };
        write!(f, "{style}")
    }
}

/// Init report metrics
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InitReportMetrics {
    /// Duration of initialization in milliseconds
    pub duration_ms: f64,
}

/// Runtime done metrics
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeDoneMetrics {
    /// Duration of the runtime execution in milliseconds
    pub duration_ms: f64,
    /// Number of bytes produced (optional)
    pub produced_bytes: Option<u64>,
}

/// Platform report metrics
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReportMetrics {
    /// Duration of the invocation in milliseconds
    pub duration_ms: f64,
    /// Billed duration in milliseconds
    pub billed_duration_ms: u64,
    /// Memory size in MB
    pub memory_size_mb: u64,
    /// Maximum memory used in MB
    pub max_memory_used_mb: u64,
    /// Initialization duration in milliseconds (optional)
    pub init_duration_ms: Option<f64>,
    /// Restore duration in milliseconds (optional, for SnapStart)
    pub restore_duration_ms: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_deserialize_platform_start() {
        let json = r#"{
            "time": "2022-10-21T14:05:03.165Z",
            "type": "platform.start",
            "record": {
                "requestId": "459921b5-681c-4a96-beb0-81e0aa586026",
                "version": "$LATEST"
            }
        }"#;

        let event: TelemetryEvent = serde_json::from_str(json).expect("Failed to deserialize");
        
        match event.record {
            TelemetryRecord::PlatformStart { request_id, version } => {
                assert_eq!(request_id, "459921b5-681c-4a96-beb0-81e0aa586026");
                assert_eq!(version, Some("$LATEST".to_string()));
            }
            _ => panic!("Expected PlatformStart record"),
        }
    }

    #[test]
    fn test_deserialize_function_log() {
        let json = r#"{
            "time": "2024-04-24T12:34:56.789Z",
            "type": "function",
            "record": "Hello from Lambda function"
        }"#;

        let event: TelemetryEvent = serde_json::from_str(json).expect("Failed to deserialize");
        
        match event.record {
            TelemetryRecord::Function(log_data) => {
                assert_eq!(log_data, Value::String("Hello from Lambda function".to_string()));
            }
            _ => panic!("Expected Function record"),
        }
    }

    #[test]
    fn test_deserialize_platform_report() {
        let json = r#"{
            "time": "2022-10-21T14:05:05.766Z",
            "type": "platform.report",
            "record": {
                "requestId": "459921b5-681c-4a96-beb0-81e0aa586026",
                "status": "success",
                "metrics": {
                    "durationMs": 2599.4,
                    "billedDurationMs": 2600,
                    "memorySizeMB": 128,
                    "maxMemoryUsedMB": 94,
                    "initDurationMs": 549.04
                }
            }
        }"#;

        let event: TelemetryEvent = serde_json::from_str(json).expect("Failed to deserialize");
        
        match event.record {
            TelemetryRecord::PlatformReport { request_id, status, metrics, .. } => {
                assert_eq!(request_id, "459921b5-681c-4a96-beb0-81e0aa586026");
                assert_eq!(status, Status::Success);
                assert_eq!(metrics.duration_ms, 2599.4);
                assert_eq!(metrics.billed_duration_ms, 2600);
                assert_eq!(metrics.memory_size_mb, 128);
                assert_eq!(metrics.max_memory_used_mb, 94);
                assert_eq!(metrics.init_duration_ms, Some(549.04));
            }
            _ => panic!("Expected PlatformReport record"),
        }
    }
}
