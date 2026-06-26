// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Platform metrics conversion for APM mode
//!
//! Converts AWS Lambda platform REPORT logs to New Relic APM metrics
//! Based on metric_api.go ParseLambdaReportLog() and ConvertToMetrics()

use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::{json, Value};
use tracing::{debug, warn};

use crate::config::deployment::DeploymentContext;

/// Strict REPORT regex — the full line Normal Lambda always emits. This is the
/// ORIGINAL pattern, used ONLY on the `Normal` path so Standard Lambda parsing is
/// byte-identical to before.
static REPORT_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"RequestId: (\S+)\s+Duration: ([\d.]+) ms\s+Billed Duration: (\d+) ms\s+Memory Size: (\d+) MB\s+Max Memory Used: (\d+) MB"
    ).unwrap()
});

/// LMI-ONLY core REPORT fields: RequestId + Duration. AWS strips Billed Duration /
/// Memory Size / Max Memory Used from the LMI `platform.report` (only `durationMs`
/// survives), so on LMI the line is just `REPORT RequestId: X  Duration: N ms`. These
/// regexes are consulted only on the `Lmi` path; Normal keeps the strict regex above.
static REPORT_CORE_REGEX_LMI: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"RequestId: (\S+)\s+Duration: ([\d.]+) ms").unwrap()
});
static BILLED_DURATION_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"Billed Duration: (\d+) ms").unwrap()
});
static MEMORY_SIZE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"Memory Size: (\d+) MB").unwrap()
});
static MAX_MEMORY_USED_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"Max Memory Used: (\d+) MB").unwrap()
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

/// Parse a Lambda REPORT/fault log line into metrics. **Type-driven on deployment**:
///
/// - `Normal` → strict full-format parse (unchanged original behavior).
/// - `Lmi` → lenient parse: only RequestId + Duration are required; Billed Duration /
///   Memory Size / Max Memory Used are optional because AWS strips them from the LMI
///   report. This is why every LMI report previously failed to parse (NR-579361 follow-up).
pub fn parse_lambda_report_log(log_line: &str, deployment: DeploymentContext) -> Option<LambdaMetrics> {
    let report = match deployment {
        DeploymentContext::Normal { .. } => parse_report_normal_strict(log_line),
        DeploymentContext::Lmi => parse_report_lmi_lenient(log_line),
    };
    if report.is_some() {
        return report;
    }

    if let Some(metrics) = parse_fault_log(log_line) {
        return Some(metrics);
    }

    // Normal expects a full report, so a parse miss is a genuine warning. On LMI, short
    // or variant report lines are expected — keep it at debug to avoid log spam.
    match deployment {
        DeploymentContext::Normal { .. } => {
            warn!("Failed to parse Lambda REPORT/fault log: {}", log_line)
        }
        DeploymentContext::Lmi => {
            debug!("LMI: REPORT/fault log not parsed (no RequestId+Duration): {}", log_line)
        }
    }
    None
}

/// Normal Lambda — strict full-format extraction (verbatim original logic).
fn parse_report_normal_strict(log_line: &str) -> Option<LambdaMetrics> {
    let captures = REPORT_REGEX.captures(log_line)?;
    let request_id = captures.get(1)?.as_str().to_string();
    let duration = captures.get(2)?.as_str().parse::<f64>().ok();
    let billed_duration = captures.get(3)?.as_str().parse::<i64>().ok().map(|v| v as f64);
    let memory_size = captures.get(4)?.as_str().parse::<i64>().ok();
    let max_memory_used = captures.get(5)?.as_str().parse::<i64>().ok();

    let init_duration = INIT_DURATION_REGEX.captures(log_line)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<f64>().ok());

    debug!("Parsed REPORT log: request_id={}, duration={:?}", request_id, duration);

    Some(LambdaMetrics {
        request_id,
        duration,
        billed_duration,
        memory_size,
        max_memory_used,
        init_duration,
        error: None,
        error_type: None,
    })
}

/// LMI — lenient extraction: RequestId + Duration required; Billed/Memory/Max optional
/// (AWS strips them from the LMI report). LMI-only path; never runs for Normal Lambda.
fn parse_report_lmi_lenient(log_line: &str) -> Option<LambdaMetrics> {
    let captures = REPORT_CORE_REGEX_LMI.captures(log_line)?;
    let request_id = captures.get(1)?.as_str().to_string();
    let duration = captures.get(2)?.as_str().parse::<f64>().ok();

    let billed_duration = BILLED_DURATION_REGEX.captures(log_line)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<i64>().ok())
        .map(|v| v as f64);
    let memory_size = MEMORY_SIZE_REGEX.captures(log_line)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<i64>().ok());
    let max_memory_used = MAX_MEMORY_USED_REGEX.captures(log_line)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<i64>().ok());

    let init_duration = INIT_DURATION_REGEX.captures(log_line)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<f64>().ok());

    debug!("LMI: parsed stripped REPORT log: request_id={}, duration={:?}", request_id, duration);

    Some(LambdaMetrics {
        request_id,
        duration,
        billed_duration,
        memory_size,
        max_memory_used,
        init_duration,
        error: None,
        error_type: None,
    })
}

/// Shared fault-log parse (`Status: error  ErrorType: …`). Same for both deployments.
fn parse_fault_log(log_line: &str) -> Option<LambdaMetrics> {
    let captures = FAULT_LOG_REGEX.captures(log_line)?;
    let request_id = captures.get(1)?.as_str().to_string();
    let error = captures.get(2)?.as_str().to_string();
    let error_type = captures.get(3).map(|m| m.as_str().to_string());

    debug!("Parsed fault log: request_id={}, error={}", request_id, error);

    Some(LambdaMetrics {
        request_id,
        duration: None,
        billed_duration: None,
        memory_size: None,
        max_memory_used: None,
        init_duration: None,
        error: Some(error),
        error_type,
    })
}

/// Convert Lambda metrics to New Relic APM metrics
pub fn convert_to_apm_metrics(
    metrics: &LambdaMetrics,
    entity_guid: &str,
    function_name: &str,
    function_arn: &str,
) -> Vec<Value> {
    let timestamp = chrono::Utc::now().timestamp_millis();

    let mut common_attrs = serde_json::Map::new();
    common_attrs.insert("aws.requestId".to_string(), json!(metrics.request_id));
    common_attrs.insert("entity.guid".to_string(), json!(entity_guid));
    common_attrs.insert("entity.name".to_string(), json!(function_name));
    common_attrs.insert("entity.type".to_string(), json!("APM"));
    if !function_arn.is_empty() {
        common_attrs.insert("aws.lambda.arn".to_string(), json!(function_arn));
    }

    // LMI host metadata. None on Standard Lambda; populated once at
    // cold-start init when the listener processes platform.initStart.
    if let Some(meta) = crate::telemetry::managed_instance::try_read_metadata() {
        common_attrs.insert(
            "aws.lambda.managedInstance.instanceId".to_string(),
            json!(meta.instance_id),
        );
        if let Some(max_memory) = meta.instance_max_memory {
            common_attrs.insert(
                "aws.lambda.managedInstance.instanceMaxMemory".to_string(),
                json!(max_memory),
            );
        }
    }

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
    use crate::config::deployment::{DeploymentContext, TelemetryMode};

    const NORMAL: DeploymentContext = DeploymentContext::Normal { mode: TelemetryMode::Apm };
    const LMI: DeploymentContext = DeploymentContext::Lmi;

    #[test]
    fn test_parse_report_log_basic() {
        let log = "REPORT RequestId: abc123\tDuration: 123.45 ms\tBilled Duration: 124 ms\tMemory Size: 512 MB\tMax Memory Used: 256 MB";
        let metrics = parse_lambda_report_log(log, NORMAL).unwrap();

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
        let metrics = parse_lambda_report_log(log, NORMAL).unwrap();

        assert_eq!(metrics.init_duration, Some(456.78));
    }

    #[test]
    fn test_parse_fault_log() {
        let log = "RequestId: abc123 Status: error ErrorType: Runtime.ExitError";
        let metrics = parse_lambda_report_log(log, NORMAL).unwrap();

        assert_eq!(metrics.request_id, "abc123");
        assert_eq!(metrics.error, Some("error".to_string()));
        assert_eq!(metrics.error_type, Some("Runtime.ExitError".to_string()));
    }

    /// LMI strips Billed Duration / Memory Size / Max Memory Used from the report —
    /// only Duration survives. This previously failed to parse on every LMI invoke.
    #[test]
    fn test_parse_report_log_lmi_stripped_duration_only() {
        let log = "REPORT RequestId: abc123\tDuration: 21.33 ms";
        let metrics = parse_lambda_report_log(log, LMI).expect("stripped LMI report must parse");

        assert_eq!(metrics.request_id, "abc123");
        assert_eq!(metrics.duration, Some(21.33));
        assert_eq!(metrics.billed_duration, None);
        assert_eq!(metrics.memory_size, None);
        assert_eq!(metrics.max_memory_used, None);
        assert_eq!(metrics.init_duration, None);
        assert_eq!(metrics.error, None);
    }

    /// CRITICAL guarantee — Standard Lambda is NOT relaxed: the strict `Normal` path
    /// must REJECT a stripped report (only the `Lmi` path accepts it).
    #[test]
    fn test_normal_rejects_stripped_report() {
        let stripped = "REPORT RequestId: abc123\tDuration: 21.33 ms";
        assert!(
            parse_lambda_report_log(stripped, NORMAL).is_none(),
            "Normal Lambda must keep the strict full-format parse (no relaxation)"
        );
        // Same line parses on LMI.
        assert!(parse_lambda_report_log(stripped, LMI).is_some());
    }

    /// A stripped LMI report converts to exactly one metric (duration), no billed/memory.
    #[test]
    fn test_lmi_stripped_report_yields_duration_metric_only() {
        let metrics = parse_lambda_report_log("REPORT RequestId: abc123\tDuration: 21.33 ms", LMI).unwrap();
        let apm = convert_to_apm_metrics(&metrics, "guid", "fn", "arn");
        let names: Vec<&str> = apm.iter().filter_map(|m| m["name"].as_str()).collect();

        assert!(names.contains(&"apm.lambda.transaction.duration"), "duration metric expected");
        assert!(
            !names.iter().any(|n| n.contains("billed_duration") || n.contains("memory")),
            "no billed/memory metrics when those fields are stripped: {names:?}"
        );
    }

    /// Regression: a FULL report parses identically on BOTH paths (all fields populated).
    #[test]
    fn test_parse_report_log_full_unchanged_both_modes() {
        let log = "REPORT RequestId: abc123\tDuration: 123.45 ms\tBilled Duration: 124 ms\tMemory Size: 512 MB\tMax Memory Used: 256 MB\tInit Duration: 456.78 ms";
        for ctx in [NORMAL, LMI] {
            let metrics = parse_lambda_report_log(log, ctx).unwrap();
            assert_eq!(metrics.request_id, "abc123");
            assert_eq!(metrics.duration, Some(123.45));
            assert_eq!(metrics.billed_duration, Some(124.0));
            assert_eq!(metrics.memory_size, Some(512));
            assert_eq!(metrics.max_memory_used, Some(256));
            assert_eq!(metrics.init_duration, Some(456.78));
        }
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

        let apm_metrics = convert_to_apm_metrics(&metrics, "entity-guid-123", "my-function", "arn:aws:lambda:us-east-1:123456789012:function:my-function");

        assert_eq!(apm_metrics.len(), 5);

        let first_metric = &apm_metrics[0];
        assert_eq!(first_metric["name"], "apm.lambda.transaction.duration");
        assert_eq!(first_metric["type"], "gauge");
        assert_eq!(first_metric["value"], 123.45);
        assert_eq!(first_metric["attributes"]["entity.guid"], "entity-guid-123");
    }
}
