//! Event Processors
//! 
//! This module contains processors for different types of events
//! in the New Relic Lambda Extension event bus.

use std::sync::Arc;

use serde_json::{json, Value};

use crate::config::ExtensionConfig;
use crate::telemetry::events::{TelemetryEvent, TelemetryRecord};
use crate::event_bus::forwarder::NewRelicForwarder;

/// Processor for telemetry events
pub struct TelemetryProcessor {
    config: Arc<ExtensionConfig>,
}

impl TelemetryProcessor {
    pub fn new(config: Arc<ExtensionConfig>) -> Self {
        Self { config }
    }

    /// Process a telemetry event and forward it to New Relic
    pub async fn process(
        &self,
        event: TelemetryEvent,
        forwarder: &NewRelicForwarder,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("🔄 [TelemetryProcessor] Processing telemetry event: {:?}", event.record);
        
        // Convert telemetry event to New Relic format
        let nr_event = self.convert_to_newrelic_format(&event)?;
        
        // Forward to New Relic telemetry endpoint
        tracing::info!("📤 [TelemetryProcessor] Forwarding telemetry to New Relic");
        forwarder.send_telemetry(nr_event).await?;
        
        // Extract logs if this is a function or extension log
        if let Some(log_event) = self.extract_log_from_telemetry(&event) {
            tracing::info!("📝 [TelemetryProcessor] Forwarding extracted log to New Relic");
            forwarder.send_log(log_event).await?;
        }
        
        // Extract metrics if this telemetry event contains metrics
        if let Some(metrics) = self.extract_metrics_from_telemetry(&event) {
            tracing::info!("📊 [TelemetryProcessor] Forwarding {} metrics to New Relic", metrics.len());
            for metric in metrics {
                forwarder.send_metric(metric).await?;
            }
        }
        
        tracing::info!("✅ [TelemetryProcessor] Successfully processed telemetry event");
        Ok(())
    }

    /// Convert telemetry event to New Relic format
    fn convert_to_newrelic_format(&self, event: &TelemetryEvent) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let mut nr_event = json!({
            "timestamp": event.time.timestamp_millis(),
            "eventType": "LambdaTelemetry",
            "source": "newrelic-lambda-extension",
            "functionName": self.config.aws.function_name,
            "functionVersion": self.config.aws.function_version,
            "region": self.config.aws.region.as_deref().unwrap_or("unknown"),
        });

        // Add telemetry-specific fields based on record type
        match &event.record {
            TelemetryRecord::Function(log_data) => {
                nr_event["telemetryType"] = json!("function");
                nr_event["logData"] = log_data.clone();
            }
            TelemetryRecord::Extension(log_data) => {
                nr_event["telemetryType"] = json!("extension");
                nr_event["logData"] = log_data.clone();
            }
            TelemetryRecord::PlatformStart { request_id, version } => {
                nr_event["telemetryType"] = json!("platform.start");
                nr_event["requestId"] = json!(request_id);
                if let Some(v) = version {
                    nr_event["version"] = json!(v);
                }
            }
            TelemetryRecord::PlatformEnd { request_id } => {
                nr_event["telemetryType"] = json!("platform.end");
                nr_event["requestId"] = json!(request_id);
            }
            TelemetryRecord::PlatformReport { request_id, metrics, .. } => {
                nr_event["telemetryType"] = json!("platform.report");
                nr_event["requestId"] = json!(request_id);
                nr_event["metrics"] = json!(metrics);
            }
            TelemetryRecord::PlatformInitStart { initialization_type, phase, .. } => {
                nr_event["telemetryType"] = json!("platform.initStart");
                nr_event["initializationType"] = json!(initialization_type);
                nr_event["phase"] = json!(phase);
            }
            TelemetryRecord::PlatformInitReport { initialization_type, metrics, .. } => {
                nr_event["telemetryType"] = json!("platform.initReport");
                nr_event["initializationType"] = json!(initialization_type);
                nr_event["metrics"] = json!(metrics);
            }
            TelemetryRecord::PlatformInitRuntimeDone { initialization_type, status, .. } => {
                nr_event["telemetryType"] = json!("platform.initRuntimeDone");
                nr_event["initializationType"] = json!(initialization_type);
                nr_event["status"] = json!(status);
            }
            TelemetryRecord::PlatformRuntimeDone { request_id, status, metrics, .. } => {
                nr_event["telemetryType"] = json!("platform.runtimeDone");
                nr_event["requestId"] = json!(request_id);
                nr_event["status"] = json!(status);
                if let Some(m) = metrics {
                    nr_event["metrics"] = json!(m);
                }
            }
            TelemetryRecord::PlatformExtension { name, state, events } => {
                nr_event["telemetryType"] = json!("platform.extension");
                nr_event["extensionName"] = json!(name);
                nr_event["extensionState"] = json!(state);
                nr_event["extensionEvents"] = json!(events);
            }
            TelemetryRecord::PlatformTelemetrySubscription { name, state, types } => {
                nr_event["telemetryType"] = json!("platform.telemetrySubscription");
                nr_event["subscriptionName"] = json!(name);
                nr_event["subscriptionState"] = json!(state);
                nr_event["subscriptionTypes"] = json!(types);
            }
            TelemetryRecord::PlatformLogsDropped { reason, dropped_records, dropped_bytes } => {
                nr_event["telemetryType"] = json!("platform.logsDropped");
                nr_event["reason"] = json!(reason);
                nr_event["droppedRecords"] = json!(dropped_records);
                nr_event["droppedBytes"] = json!(dropped_bytes);
            }
        }

        Ok(nr_event)
    }

    /// Extract log data from telemetry event if applicable
    fn extract_log_from_telemetry(&self, event: &TelemetryEvent) -> Option<Value> {
        match &event.record {
            TelemetryRecord::Function(log_data) => {
                Some(json!({
                    "timestamp": event.time.timestamp_millis(),
                    "level": "INFO",
                    "message": log_data,
                    "logType": "function",
                    "functionName": self.config.aws.function_name,
                    "functionVersion": self.config.aws.function_version,
                    "source": "lambda-function"
                }))
            }
            TelemetryRecord::Extension(log_data) => {
                Some(json!({
                    "timestamp": event.time.timestamp_millis(),
                    "level": "INFO", 
                    "message": log_data,
                    "logType": "extension",
                    "extensionName": self.config.extension.name,
                    "source": "lambda-extension"
                }))
            }
            _ => None,
        }
    }

    /// Extract metrics from telemetry event if applicable
    fn extract_metrics_from_telemetry(&self, event: &TelemetryEvent) -> Option<Vec<Value>> {
        let mut metrics = Vec::new();
        let timestamp = event.time.timestamp_millis();

        match &event.record {
            TelemetryRecord::PlatformReport { metrics: report_metrics, .. } => {
                // Duration metric
                metrics.push(json!({
                    "timestamp": timestamp,
                    "name": "lambda.duration",
                    "value": report_metrics.duration_ms,
                    "unit": "milliseconds",
                    "tags": {
                        "functionName": self.config.aws.function_name,
                        "functionVersion": self.config.aws.function_version,
                        "metricType": "duration"
                    }
                }));

                // Billed duration metric
                metrics.push(json!({
                    "timestamp": timestamp,
                    "name": "lambda.billedDuration",
                    "value": report_metrics.billed_duration_ms,
                    "unit": "milliseconds",
                    "tags": {
                        "functionName": self.config.aws.function_name,
                        "functionVersion": self.config.aws.function_version,
                        "metricType": "billing"
                    }
                }));

                // Memory metrics
                metrics.push(json!({
                    "timestamp": timestamp,
                    "name": "lambda.memorySize",
                    "value": report_metrics.memory_size_mb,
                    "unit": "megabytes",
                    "tags": {
                        "functionName": self.config.aws.function_name,
                        "functionVersion": self.config.aws.function_version,
                        "metricType": "memory"
                    }
                }));

                metrics.push(json!({
                    "timestamp": timestamp,
                    "name": "lambda.maxMemoryUsed",
                    "value": report_metrics.max_memory_used_mb,
                    "unit": "megabytes",
                    "tags": {
                        "functionName": self.config.aws.function_name,
                        "functionVersion": self.config.aws.function_version,
                        "metricType": "memory"
                    }
                }));

                // Init duration if available
                if let Some(init_duration) = report_metrics.init_duration_ms {
                    metrics.push(json!({
                        "timestamp": timestamp,
                        "name": "lambda.initDuration",
                        "value": init_duration,
                        "unit": "milliseconds",
                        "tags": {
                            "functionName": self.config.aws.function_name,
                            "functionVersion": self.config.aws.function_version,
                            "metricType": "coldStart"
                        }
                    }));
                }
            }
            TelemetryRecord::PlatformInitReport { metrics: init_metrics, .. } => {
                metrics.push(json!({
                    "timestamp": timestamp,
                    "name": "lambda.initReportDuration",
                    "value": init_metrics.duration_ms,
                    "unit": "milliseconds",
                    "tags": {
                        "functionName": self.config.aws.function_name,
                        "functionVersion": self.config.aws.function_version,
                        "metricType": "initialization"
                    }
                }));
            }
            TelemetryRecord::PlatformRuntimeDone { metrics: Some(runtime_metrics), .. } => {
                metrics.push(json!({
                    "timestamp": timestamp,
                    "name": "lambda.runtimeDuration",
                    "value": runtime_metrics.duration_ms,
                    "unit": "milliseconds",
                    "tags": {
                        "functionName": self.config.aws.function_name,
                        "functionVersion": self.config.aws.function_version,
                        "metricType": "runtime"
                    }
                }));

                if let Some(produced_bytes) = runtime_metrics.produced_bytes {
                    metrics.push(json!({
                        "timestamp": timestamp,
                        "name": "lambda.producedBytes",
                        "value": produced_bytes,
                        "unit": "bytes",
                        "tags": {
                            "functionName": self.config.aws.function_name,
                            "functionVersion": self.config.aws.function_version,
                            "metricType": "output"
                        }
                    }));
                }
            }
            _ => {}
        }

        if metrics.is_empty() {
            None
        } else {
            Some(metrics)
        }
    }
}

/// Processor for log events
pub struct LogProcessor {
    config: Arc<ExtensionConfig>,
}

impl LogProcessor {
    pub fn new(config: Arc<ExtensionConfig>) -> Self {
        Self { config }
    }

    /// Process a function log event
    pub async fn process_function_log(
        &self,
        timestamp: i64,
        request_id: Option<String>,
        message: String,
        level: String,
        forwarder: &NewRelicForwarder,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let log_event = json!({
            "timestamp": timestamp,
            "level": level,
            "message": message,
            "logType": "function",
            "functionName": self.config.aws.function_name,
            "functionVersion": self.config.aws.function_version,
            "requestId": request_id,
            "source": "lambda-function"
        });

        forwarder.send_log(log_event).await
    }

    /// Process an extension log event
    pub async fn process_extension_log(
        &self,
        timestamp: i64,
        message: String,
        level: String,
        forwarder: &NewRelicForwarder,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let log_event = json!({
            "timestamp": timestamp,
            "level": level,
            "message": message,
            "logType": "extension",
            "extensionName": self.config.extension.name,
            "source": "lambda-extension"
        });

        forwarder.send_log(log_event).await
    }
}

/// Processor for metric events
pub struct MetricProcessor {
    config: Arc<ExtensionConfig>,
}

impl MetricProcessor {
    pub fn new(config: Arc<ExtensionConfig>) -> Self {
        Self { config }
    }

    /// Process a metric event
    pub async fn process(
        &self,
        timestamp: i64,
        name: String,
        value: f64,
        tags: Vec<(String, String)>,
        forwarder: &NewRelicForwarder,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut metric_tags = json!({
            "functionName": self.config.aws.function_name,
            "functionVersion": self.config.aws.function_version,
        });

        // Add custom tags
        for (key, val) in tags {
            metric_tags[key] = json!(val);
        }

        let metric_event = json!({
            "timestamp": timestamp,
            "name": name,
            "value": value,
            "tags": metric_tags
        });

        forwarder.send_metric(metric_event).await
    }
}
