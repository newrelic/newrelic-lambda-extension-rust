//! Platform metrics conversion for APM mode
//!
//! Converts AWS Lambda platform REPORT logs to New Relic APM metrics
//! Based on metric_api.go ParseLambdaReportLog() and ConvertToMetrics()

use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::{json, Value};
use tracing::{debug, warn};

/// Regex for parsing platform REPORT logs
static REPORT_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"RequestId: (\S+)\s+Duration: ([\d.]+) ms\s+Billed Duration: (\d+) ms\s+Memory Size: (\d+) MB\s+Max Memory Used: (\d+) MB"
    ).unwrap()
});

/// Regex for extracting optional Init Duration
static INIT_DURATION_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"Init Duration: ([\d.]+) ms").unwrap()
});

/// Regex for parsing platform fault logs (Status: error, ErrorType: ...)
static FAULT_LOG_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"RequestId: (\S+)\s+Status: (\S+)(?:\s+ErrorType: (\S+))?").unwrap()
});

/// Lambda metrics extracted from REPORT log
#[derive(Debug)]
pub struct LambdaMetrics {
    pub request_id: String,
    pub duration: Option<f64>,
    pub billed_duration: Option<f64>,
    pub memory_size: Option<i64>,
    pub max_memory_used: Option<i64>,
    pub init_duration: Option<f64>,
    pub error: Option<String>,
    pub error_type: Option<String>,
}

/// Parse Lambda REPORT log line
pub fn parse_lambda_report_log(log_line: &str) -> Option<LambdaMetrics> {
    if let Some(captures) = REPORT_REGEX.captures(log_line) {
        let request_id = captures.get(1)?.as_str().to_string();
        let duration = captures.get(2)?.as_str().parse::<f64>().ok();
        let billed_duration = captures.get(3)?.as_str().parse::<i64>().ok().map(|v| v as f64);
        let memory_size = captures.get(4)?.as_str().parse::<i64>().ok();
        let max_memory_used = captures.get(5)?.as_str().parse::<i64>().ok();

        let init_duration = INIT_DURATION_REGEX.captures(log_line)
            .and_then(|c| c.get(1))
            .and_then(|m| m.as_str().parse::<f64>().ok());

        debug!("Parsed REPORT log: request_id={}, duration={:?}", request_id, duration);

        return Some(LambdaMetrics {
            request_id,
            duration,
            billed_duration,
            memory_size,
            max_memory_used,
            init_duration,
            error: None,
            error_type: None,
        });
    }

    if let Some(captures) = FAULT_LOG_REGEX.captures(log_line) {
        let request_id = captures.get(1)?.as_str().to_string();
        let error = captures.get(2)?.as_str().to_string();
        let error_type = captures.get(3).map(|m| m.as_str().to_string());

        debug!("Parsed fault log: request_id={}, error={}", request_id, error);

        return Some(LambdaMetrics {
            request_id,
            duration: None,
            billed_duration: None,
            memory_size: None,
            max_memory_used: None,
            init_duration: None,
            error: Some(error),
            error_type,
        });
    }

    warn!("Failed to parse Lambda REPORT/fault log: {}", log_line);
    None
}

/// Convert Lambda metrics to New Relic APM metrics
pub fn convert_to_apm_metrics(
    metrics: &LambdaMetrics,
    entity_guid: &str,
    function_name: &str,
) -> Vec<Value> {
    let timestamp = chrono::Utc::now().timestamp_millis();
    
    let mut common_attrs = serde_json::Map::new();
    common_attrs.insert("aws.requestId".to_string(), json!(metrics.request_id));
    common_attrs.insert("entity.guid".to_string(), json!(entity_guid));
    common_attrs.insert("entity.name".to_string(), json!(function_name));
    common_attrs.insert("entity.type".to_string(), json!("APM"));

    let mut apm_metrics = Vec::new();

    if let Some(duration) = metrics.duration {
        apm_metrics.push(json!({
            "name": "apm.lambda.transaction.duration",
            "type": "gauge",
            "value": duration,
            "timestamp": timestamp,
            "attributes": common_attrs
        }));
    }

    if let Some(billed_duration) = metrics.billed_duration {
        apm_metrics.push(json!({
            "name": "apm.lambda.transaction.billed_duration",
            "type": "gauge",
            "value": billed_duration,
            "timestamp": timestamp,
            "attributes": common_attrs
        }));
    }

    if let Some(memory_size) = metrics.memory_size {
        apm_metrics.push(json!({
            "name": "apm.lambda.transaction.memory_size",
            "type": "gauge",
            "value": memory_size,
            "timestamp": timestamp,
            "attributes": common_attrs
        }));
    }

    if let Some(max_memory) = metrics.max_memory_used {
        apm_metrics.push(json!({
            "name": "apm.lambda.transaction.max_memory_used",
            "type": "gauge",
            "value": max_memory,
            "timestamp": timestamp,
            "attributes": common_attrs
        }));
    }

    if let Some(init_duration) = metrics.init_duration {
        apm_metrics.push(json!({
            "name": "apm.lambda.transaction.init_duration",
            "type": "gauge",
            "value": init_duration,
            "timestamp": timestamp,
            "attributes": common_attrs
        }));
    }

    if metrics.error.is_some() {
        let mut error_attrs = common_attrs.clone();
        if let Some(ref error_type) = metrics.error_type {
            error_attrs.insert("Error Type".to_string(), json!(error_type));
        }

        apm_metrics.push(json!({
            "name": "apm.lambda.transaction.error",
            "type": "count",
            "value": 1,
            "timestamp": timestamp,
            "interval.ms": 10000,
            "attributes": error_attrs
        }));
    }

    debug!("Converted to {} APM metrics", apm_metrics.len());
    apm_metrics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_report_log_basic() {
        let log = "REPORT RequestId: abc123\tDuration: 123.45 ms\tBilled Duration: 124 ms\tMemory Size: 512 MB\tMax Memory Used: 256 MB";
        let metrics = parse_lambda_report_log(log).unwrap();

        assert_eq!(metrics.request_id, "abc123");
        assert_eq!(metrics.duration, Some(123.45));
        assert_eq!(metrics.billed_duration, Some(124.0));
        assert_eq!(metrics.memory_size, Some(512));
        assert_eq!(metrics.max_memory_used, Some(256));
        assert_eq!(metrics.init_duration, None);
    }

    #[test]
    fn test_parse_report_log_with_init() {
        let log = "REPORT RequestId: abc123\tDuration: 123.45 ms\tBilled Duration: 124 ms\tMemory Size: 512 MB\tMax Memory Used: 256 MB\tInit Duration: 456.78 ms";
        let metrics = parse_lambda_report_log(log).unwrap();

        assert_eq!(metrics.init_duration, Some(456.78));
    }

    #[test]
    fn test_parse_fault_log() {
        let log = "RequestId: abc123 Status: error ErrorType: Runtime.ExitError";
        let metrics = parse_lambda_report_log(log).unwrap();

        assert_eq!(metrics.request_id, "abc123");
        assert_eq!(metrics.error, Some("error".to_string()));
        assert_eq!(metrics.error_type, Some("Runtime.ExitError".to_string()));
    }

    #[test]
    fn test_convert_to_apm_metrics() {
        let metrics = LambdaMetrics {
            request_id: "abc123".to_string(),
            duration: Some(123.45),
            billed_duration: Some(124.0),
            memory_size: Some(512),
            max_memory_used: Some(256),
            init_duration: Some(456.78),
            error: None,
            error_type: None,
        };

        let apm_metrics = convert_to_apm_metrics(&metrics, "entity-guid-123", "my-function");

        assert_eq!(apm_metrics.len(), 5);

        let first_metric = &apm_metrics[0];
        assert_eq!(first_metric["name"], "apm.lambda.transaction.duration");
        assert_eq!(first_metric["type"], "gauge");
        assert_eq!(first_metric["value"], 123.45);
        assert_eq!(first_metric["attributes"]["entity.guid"], "entity-guid-123");
    }
}
