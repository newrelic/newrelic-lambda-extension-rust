// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::config::deployment::{DeploymentContext, TelemetryMode};
use serial_test::serial;

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
///
/// Unsets the fallback env var so memory fields remain None, testing the
/// bare-parse path in isolation (env-var back-fill is tested separately).
///
/// #[serial] (default key): mutates the process-wide AWS_LAMBDA_FUNCTION_MEMORY_SIZE
/// env var, same key as metric_converter_memory_fallback_tests.rs — must not run
/// concurrently with those or with each other, or a set_var/remove_var race can flip
/// the memory_size fallback mid-test (NR flake: see PR history).
#[test]
#[serial]
fn test_parse_report_log_lmi_stripped_duration_only() {
    std::env::remove_var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE");
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

/// A stripped LMI report converts to exactly one metric (duration) when the
/// fallback env var is absent.  (Env-var back-fill is tested in metric_converter_tests.rs.)
///
/// #[serial] (default key): see test_parse_report_log_lmi_stripped_duration_only above.
#[test]
#[serial]
fn test_lmi_stripped_report_yields_duration_metric_only() {
    std::env::remove_var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE");
    let metrics = parse_lambda_report_log("REPORT RequestId: abc123\tDuration: 21.33 ms", LMI).unwrap();
    let apm = convert_to_apm_metrics(&metrics, "guid", "fn", "arn");
    let names: Vec<&str> = apm.iter().filter_map(|m| m["name"].as_str()).collect();

    assert!(names.contains(&"apm.lambda.transaction.duration"), "duration metric expected");
    assert!(
        !names.iter().any(|n| n.contains("billed_duration") || n.contains("memory")),
        "no billed/memory metrics when env var absent: {names:?}"
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
