// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Lambda Managed Instances (LMI) host-level metadata.
//!
//! Two AWS-documented fields are surfaced on the Lambda Telemetry API
//! `platform.initStart` record (`2025-01-29` schema) for LMI functions:
//!
//! - `instanceId` — string identifying the managed-instance host.
//! - `instanceMaxMemory` — `uint64` maximum memory for the instance. Captured
//!   verbatim (no unit conversion): AWS documents the type but not the unit,
//!   and live LMI runtimes report it in **bytes** (e.g. `2147483648` = 2 GiB)
//!   even though the AWS doc example (`256`) looks like MB.
//!
//! Both are optional in the schema. (An earlier revision modelled a `hostGroup`
//! field here — that field does **not** exist in any AWS Telemetry API schema
//! version and has been removed.)
//!
//! They arrive in the `record` body of a `platform.initStart` telemetry event
//! during the cold-start init phase, before the first user invocation. The
//! listener captures them once into the global [`MANAGED_INSTANCE_METADATA`]
//! static, and downstream attribute composition reads from it on every
//! outbound payload.
//!
//! The global is `tokio::sync::RwLock` rather than `parking_lot` so that the
//! async listener can `write().await` without blocking the executor; the
//! synchronous attribute-composition sites use `try_read()` (the same pattern
//! already used for `crate::APM_APP` in `main.rs`).

use std::sync::Arc;

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// Host-level metadata that AWS attaches to LMI functions. Fields mirror the
/// AWS-documented `platform.initStart` record (`2025-01-29` schema) — no
/// non-documented fields are modelled here.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedInstanceMetadata {
    /// Unique identifier for the underlying managed-instance host. Present on
    /// LMI; never present on Standard Lambda.
    pub instance_id: String,
    /// Maximum memory available to the managed instance — the AWS `uint64`
    /// `instanceMaxMemory` field, captured verbatim (no unit conversion).
    /// `None` when AWS omits it or sends a non-integer value.
    pub instance_max_memory: Option<u64>,
}

/// Global, set-once snapshot of the managed-instance metadata, populated by
/// the telemetry listener on the first `platform.initStart` event of the
/// cold start.
///
/// `Option::None` until populated. Stays `None` for Standard Lambda runtimes,
/// where the `platform.initStart` record carries neither field.
pub static MANAGED_INSTANCE_METADATA: Lazy<Arc<RwLock<Option<ManagedInstanceMetadata>>>> =
    Lazy::new(|| Arc::new(RwLock::new(None)));

/// Extract managed-instance metadata from a `platform.initStart` record body.
///
/// Returns `None` when `instanceId` is absent (Standard Lambda case) so the
/// caller can leave the global metadata untouched.
#[must_use]
pub fn extract_managed_instance_metadata(
    record: &serde_json::Value,
) -> Option<ManagedInstanceMetadata> {
    let instance_id = record.get("instanceId").and_then(|v| v.as_str())?;
    if instance_id.is_empty() {
        return None;
    }

    // `as_u64` returns None for any non-integer, negative, or out-of-range
    // value, so a malformed `instanceMaxMemory` degrades to absent rather than
    // erroring — the field is optional in the AWS schema.
    let instance_max_memory = record
        .get("instanceMaxMemory")
        .and_then(serde_json::Value::as_u64);

    Some(ManagedInstanceMetadata {
        instance_id: instance_id.to_string(),
        instance_max_memory,
    })
}

/// Synchronous, lock-free read for sync call sites. Returns `None` if the
/// lock is currently held for writing (in practice only during the listener's
/// initStart write, which is a single short critical section). Sync sites
/// that miss this race will simply omit the attributes for that one payload —
/// acceptable, since the metadata is set once at cold start and read for the
/// remainder of the container lifetime.
#[must_use]
pub fn try_read_metadata() -> Option<ManagedInstanceMetadata> {
    MANAGED_INSTANCE_METADATA.try_read().ok().and_then(|guard| guard.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use serial_test::serial;

    #[test]
    fn extracts_both_fields_when_present() {
        // Real LMI shape: instanceMaxMemory is the raw uint64 AWS sends — the
        // observed 2 GiB value is in bytes, captured without conversion.
        let record = json!({
            "initializationType": "lambda-managed-instances",
            "instanceId": "2026/05/27/fn[$LATEST]abc123",
            "instanceMaxMemory": 2147483648u64
        });
        let meta = extract_managed_instance_metadata(&record).expect("metadata should parse");
        assert_eq!(meta.instance_id, "2026/05/27/fn[$LATEST]abc123");
        assert_eq!(meta.instance_max_memory, Some(2147483648));
    }

    #[test]
    fn extracts_only_instance_id_when_max_memory_missing() {
        // instanceId present (always for LMI), instanceMaxMemory absent.
        let record = json!({
            "initializationType": "lambda-managed-instances",
            "instanceId": "2026/05/27/fn[$LATEST]abc123"
        });
        let meta = extract_managed_instance_metadata(&record).expect("metadata should parse");
        assert_eq!(meta.instance_id, "2026/05/27/fn[$LATEST]abc123");
        assert!(meta.instance_max_memory.is_none());
    }

    #[test]
    fn returns_none_when_instance_id_absent() {
        // Standard Lambda case: instanceId not present. Listener should not
        // write to the global static.
        let record = json!({
            "initializationType": "on-demand",
            "runtimeVersion": "python:3.14.v43"
        });
        assert!(extract_managed_instance_metadata(&record).is_none());
    }

    #[test]
    fn returns_none_when_instance_id_empty_string() {
        let record = json!({
            "instanceId": ""
        });
        assert!(extract_managed_instance_metadata(&record).is_none());
    }

    #[test]
    fn ignores_non_integer_max_memory() {
        // A non-integer instanceMaxMemory degrades to absent — instanceId still
        // captured, no panic, no error.
        for bad in [json!("2147483648"), json!(-1), json!(1.5), json!(["x"])] {
            let record = json!({ "instanceId": "id", "instanceMaxMemory": bad });
            let meta = extract_managed_instance_metadata(&record).expect("must extract");
            assert_eq!(meta.instance_id, "id");
            assert!(
                meta.instance_max_memory.is_none(),
                "non-integer instanceMaxMemory {bad} should be treated as absent"
            );
        }
    }

    #[test]
    fn ignores_non_string_instance_id() {
        let record = json!({
            "instanceId": 42,
            "instanceMaxMemory": 2147483648u64
        });
        assert!(extract_managed_instance_metadata(&record).is_none());
    }

    #[tokio::test]
    #[serial]
    async fn try_read_returns_none_until_written() {
        // Reset the global to a clean state for this serial test.
        {
            let mut guard = MANAGED_INSTANCE_METADATA.write().await;
            *guard = None;
        }
        assert!(try_read_metadata().is_none());

        let meta = ManagedInstanceMetadata {
            instance_id: "test-id".to_string(),
            instance_max_memory: Some(2147483648),
        };
        {
            let mut guard = MANAGED_INSTANCE_METADATA.write().await;
            *guard = Some(meta.clone());
        }

        let read = try_read_metadata().expect("should read after write");
        assert_eq!(read, meta);

        // Clean up so other tests start fresh.
        let mut guard = MANAGED_INSTANCE_METADATA.write().await;
        *guard = None;
    }
}
