// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::config::deployment::{DeploymentContext, TelemetryMode};
use crate::telemetry::managed_instance::{MANAGED_INSTANCE_METADATA, ManagedInstanceMetadata};
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

#[test]
fn convert_to_apm_metrics_error_with_error_type_adds_error_metric() {
    let metrics = LambdaMetrics {
        request_id: "req-err".to_string(),
        duration: Some(10.0),
        billed_duration: None,
        memory_size: None,
        max_memory_used: None,
        init_duration: None,
        error: Some("error".to_string()),
        error_type: Some("Runtime.ExitError".to_string()),
    };
    let apm = convert_to_apm_metrics(
        &metrics,
        "guid",
        "fn",
        "arn:aws:lambda:us-east-1:123:function:fn",
    );
    // duration metric + error metric = 2
    assert_eq!(apm.len(), 2);
    let err_m = apm
        .iter()
        .find(|m| m["name"] == "apm.lambda.transaction.error")
        .expect("error metric must be present");
    assert_eq!(err_m["type"], "count");
    assert_eq!(err_m["value"], 1);
    assert_eq!(err_m["attributes"]["Error Type"], "Runtime.ExitError");
}

#[test]
fn convert_to_apm_metrics_error_without_error_type_omits_error_type_attr() {
    let metrics = LambdaMetrics {
        request_id: "req-err2".to_string(),
        duration: None,
        billed_duration: None,
        memory_size: None,
        max_memory_used: None,
        init_duration: None,
        error: Some("error".to_string()),
        error_type: None,
    };
    let apm = convert_to_apm_metrics(&metrics, "guid", "fn", "arn");
    assert_eq!(apm.len(), 1);
    assert_eq!(apm[0]["name"], "apm.lambda.transaction.error");
    let attrs = apm[0]["attributes"].as_object().expect("attributes must be an object");
    assert!(
        !attrs.contains_key("Error Type"),
        "Error Type must be absent when error_type is None"
    );
}

#[test]
fn convert_to_apm_metrics_empty_arn_omits_arn_attribute() {
    let metrics = LambdaMetrics {
        request_id: "req-1".to_string(),
        duration: Some(5.0),
        billed_duration: None,
        memory_size: None,
        max_memory_used: None,
        init_duration: None,
        error: None,
        error_type: None,
    };
    let apm = convert_to_apm_metrics(&metrics, "guid", "fn", "");
    assert_eq!(apm.len(), 1);
    let attrs = apm[0]["attributes"].as_object().expect("attributes must be an object");
    assert!(
        !attrs.contains_key("aws.lambda.arn"),
        "empty ARN must not produce an aws.lambda.arn attribute"
    );
}

/// Covers the LMI metadata block (lines 265-276 of metric_converter.rs).
/// MANAGED_INSTANCE_METADATA is a public RwLock so tests can populate it directly,
/// simulating a cold-start platform.initStart event without modifying production code.
#[tokio::test]
#[serial]
async fn convert_to_apm_metrics_attaches_lmi_metadata_when_present() {
    {
        let mut guard = MANAGED_INSTANCE_METADATA.write().await;
        *guard = Some(ManagedInstanceMetadata {
            instance_id: "i-lmi-host-abc".to_string(),
            instance_max_memory: Some(2147483648),
        });
    }

    let metrics = LambdaMetrics {
        request_id: "req-lmi-meta".to_string(),
        duration: Some(10.0),
        billed_duration: None,
        memory_size: None,
        max_memory_used: None,
        init_duration: None,
        error: None,
        error_type: None,
    };
    let apm = convert_to_apm_metrics(&metrics, "guid", "fn", "arn");

    let attrs = apm[0]["attributes"].as_object().expect("attributes must be an object");
    assert_eq!(attrs["aws.lambda.managedInstance.instanceId"], "i-lmi-host-abc");
    assert_eq!(attrs["aws.lambda.managedInstance.instanceMaxMemory"], 2147483648u64);

    // Cleanup so other tests see None.
    {
        let mut guard = MANAGED_INSTANCE_METADATA.write().await;
        *guard = None;
    }
}
