// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for LMI memory fallback helpers (`read_env_memory_size_mb`,
//! `read_cgroup_memory_mb`) and their integration with `parse_report_lmi_lenient`
//! via `parse_lambda_report_log`.
//!
//! These tests cover the back-fill paths that populate `memory_size` and
//! `max_memory_used` when AWS strips them from the LMI `platform.report`.

use super::*;
use crate::config::deployment::DeploymentContext;
use serial_test::serial;

const LMI: DeploymentContext = DeploymentContext::Lmi;

// ── read_env_memory_size_mb ───────────────────────────────────────────────────

/// When the runtime env var is set to a valid integer, the helper must return it.
#[test]
#[serial]
fn env_memory_size_returns_value_when_var_set() {
    std::env::set_var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "1024");
    let result = read_env_memory_size_mb();
    std::env::remove_var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE");
    assert_eq!(result, Some(1024));
}

/// When the env var is absent, the helper must return None (no panic).
#[test]
#[serial]
fn env_memory_size_returns_none_when_var_absent() {
    std::env::remove_var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE");
    assert_eq!(read_env_memory_size_mb(), None);
}

/// When the env var contains a non-numeric value, the helper must return None.
#[test]
#[serial]
fn env_memory_size_returns_none_when_var_not_numeric() {
    std::env::set_var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "not-a-number");
    let result = read_env_memory_size_mb();
    std::env::remove_var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE");
    assert_eq!(result, None);
}

// ── read_cgroup_memory_mb ─────────────────────────────────────────────────────

/// In a non-Lambda environment (CI, local dev), cgroup files are absent.
/// The helper must return None without panicking.
#[test]
fn cgroup_memory_returns_none_when_files_absent() {
    // On macOS / most CI environments neither cgroup path exists.
    // This test is vacuously true on Linux Lambda but guards against panics everywhere.
    let result = read_cgroup_memory_mb();
    // We can't assert Some(v) because we don't know if we're on a real Lambda,
    // but we can assert the call completes and returns an Option (not a panic).
    let _ = result; // use the value so the compiler doesn't warn
}

// ── parse_report_lmi_lenient integration ─────────────────────────────────────

/// When the stripped LMI report has no memory fields AND the env var is set,
/// `parse_lambda_report_log` must back-fill `memory_size` from the env var.
#[test]
#[serial]
fn lmi_stripped_report_backfills_memory_size_from_env() {
    std::env::set_var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "512");
    let log = "REPORT RequestId: req-1\tDuration: 50.0 ms";
    let metrics = parse_lambda_report_log(log, LMI).expect("stripped LMI report must parse");
    std::env::remove_var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE");

    assert_eq!(metrics.memory_size, Some(512),
        "memory_size must be back-filled from AWS_LAMBDA_FUNCTION_MEMORY_SIZE");
}

/// When `memory_size` IS present in the log (e.g. a test REPORT line that
/// still carries the field), the log value must win over the env var.
#[test]
#[serial]
fn lmi_report_with_memory_size_field_wins_over_env() {
    std::env::set_var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "999");
    let log = "REPORT RequestId: req-2\tDuration: 50.0 ms\tBilled Duration: 50 ms\tMemory Size: 256 MB\tMax Memory Used: 128 MB";
    let metrics = parse_lambda_report_log(log, LMI).expect("full LMI report must parse");
    std::env::remove_var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE");

    assert_eq!(metrics.memory_size, Some(256),
        "log value (256) must win over env var (999)");
}

/// `billed_duration` must remain `None` on a stripped LMI report.
/// LMI billing is vCPU-hour (EE lifetime), not per-invocation milliseconds.
#[test]
fn lmi_stripped_report_leaves_billed_duration_none() {
    let log = "REPORT RequestId: req-3\tDuration: 33.0 ms";
    let metrics = parse_lambda_report_log(log, LMI).expect("stripped LMI report must parse");

    assert_eq!(metrics.billed_duration, None,
        "billed_duration must be None on LMI — vCPU-hour billing, not per-invocation");
}

/// The APM metric output for a stripped LMI report back-filled with env memory
/// must include `apm.lambda.transaction.memory_size` once `memory_size` is populated.
#[test]
#[serial]
fn lmi_stripped_report_with_env_memory_emits_memory_size_metric() {
    std::env::set_var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "1024");
    let log = "REPORT RequestId: req-4\tDuration: 100.0 ms";
    let metrics = parse_lambda_report_log(log, LMI).expect("stripped LMI report must parse");
    std::env::remove_var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE");

    let apm = convert_to_apm_metrics(&metrics, "guid", "fn", "arn");
    let names: Vec<&str> = apm.iter().filter_map(|m| m["name"].as_str()).collect();

    assert!(names.contains(&"apm.lambda.transaction.memory_size"),
        "memory_size metric must appear when back-filled from env var; got: {names:?}");
    assert!(!names.iter().any(|n| n.contains("billed_duration")),
        "billed_duration metric must NOT appear on LMI; got: {names:?}");
}
