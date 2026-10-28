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
///
/// When the report is stripped (the normal LMI case), memory fields are back-filled:
/// - `memory_size`    ← `AWS_LAMBDA_FUNCTION_MEMORY_SIZE` env var (always set by runtime)
/// - `max_memory_used` ← cgroup memory stats (total container usage, cgroupsv2 first)
///
/// `billed_duration` is intentionally left as `None`: LMI billing is by vCPU-hour for
/// the execution-environment lifetime, not per-invocation milliseconds. There is no
/// meaningful per-request "billed duration" to report.
fn parse_report_lmi_lenient(log_line: &str) -> Option<LambdaMetrics> {
    let captures = REPORT_CORE_REGEX_LMI.captures(log_line)?;
    let request_id = captures.get(1)?.as_str().to_string();
    let duration = captures.get(2)?.as_str().parse::<f64>().ok();

    // billed_duration: parse from log if present (shouldn't appear on real LMI); stays
    // None otherwise. See doc comment above — no per-invocation billed duration on LMI.
    let billed_duration = BILLED_DURATION_REGEX.captures(log_line)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<i64>().ok())
        .map(|v| v as f64);

    // memory_size: parse from log if present; fall back to the runtime env var.
    let memory_size = MEMORY_SIZE_REGEX.captures(log_line)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<i64>().ok())
        .or_else(|| {
            let mb = read_env_memory_size_mb();
            if let Some(v) = mb {
                debug!("LMI: memory_size from AWS_LAMBDA_FUNCTION_MEMORY_SIZE: {} MB", v);
            }
            mb
        });

    // max_memory_used: parse from log if present; fall back to live cgroup stats.
    let max_memory_used = MAX_MEMORY_USED_REGEX.captures(log_line)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<i64>().ok())
        .or_else(|| {
            let mb = read_cgroup_memory_mb();
            match mb {
                Some(v) => {
                    debug!("LMI: max_memory_used from cgroup: {} MB", v);
                    Some(v)
                }
                None => {
                    debug!("LMI: cgroup memory unavailable (non-Lambda environment?)");
                    None
                }
            }
        });

    let init_duration = INIT_DURATION_REGEX.captures(log_line)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<f64>().ok());

    debug!(
        "LMI: parsed REPORT: request_id={}, duration={:?}ms, memory_size={:?}MB, max_memory_used={:?}MB",
        request_id, duration, memory_size, max_memory_used
    );

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

/// Reads the configured Lambda memory allocation (MB) from the runtime environment.
/// `AWS_LAMBDA_FUNCTION_MEMORY_SIZE` is always set by the Lambda runtime for every
/// execution environment including LMI.
pub(crate) fn read_env_memory_size_mb() -> Option<i64> {
    std::env::var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
}

/// Reads the current container memory usage (MB) from the Linux cgroup hierarchy.
///
/// Tries cgroupsv2 (`/sys/fs/cgroup/memory.current`) first (Amazon Linux 2023 /
/// newer Lambda runtimes), then falls back to cgroupsv1
/// (`/sys/fs/cgroup/memory/memory.usage_in_bytes`). The value is the total bytes
/// used by the Lambda execution environment (function + extension + runtime),
/// which corresponds to "Max Memory Used" on Normal Lambda REPORT lines.
///
/// Returns `None` in non-Lambda environments (local tests, CI) where cgroup files
/// are absent.
pub(crate) fn read_cgroup_memory_mb() -> Option<i64> {
    let bytes_str = std::fs::read_to_string("/sys/fs/cgroup/memory.current")
        .or_else(|_| std::fs::read_to_string("/sys/fs/cgroup/memory/memory.usage_in_bytes"))
        .ok()?;
    let bytes = bytes_str.trim().parse::<i64>().ok()?;
    Some(bytes / (1024 * 1024))
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
#[path = "metric_converter_memory_fallback_tests.rs"]
mod memory_fallback_tests;

#[cfg(test)]
#[path = "metric_converter_tests.rs"]
mod tests;
