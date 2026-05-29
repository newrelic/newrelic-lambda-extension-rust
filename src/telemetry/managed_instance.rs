// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Lambda Managed Instances (LMI) host-level metadata.
//!
//! Two pieces of metadata are surfaced by the Lambda Telemetry API for LMI
//! functions:
//!
//! - `instanceId` — present on every LMI runtime, regardless of subscription
//!   schema.
//! - `hostGroup` — present *only* when the extension subscribed with the
//!   `2025-01-29` Telemetry API schema (see [`crate::runtime::TelemetrySchema`]).
//!
//! Both arrive in the `record` body of a `platform.initStart` telemetry event
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

/// Host-level metadata that AWS attaches to LMI functions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedInstanceMetadata {
    /// Unique identifier for the underlying managed-instance host. Always
    /// present on LMI; never present on Standard Lambda.
    pub instance_id: String,
    /// Logical grouping the instance belongs to. Only delivered on the
    /// `2025-01-29` subscription schema.
    pub host_group: Option<String>,
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

    let host_group = record
        .get("hostGroup")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    Some(ManagedInstanceMetadata {
        instance_id: instance_id.to_string(),
        host_group,
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

    #[test]
    fn extracts_both_fields_when_present() {
        let record = json!({
            "initializationType": "lambda-managed-instances",
            "instanceId": "2026/05/27/fn[$LATEST]abc123",
            "hostGroup": "default-host-group"
        });
        let meta = extract_managed_instance_metadata(&record).expect("metadata should parse");
        assert_eq!(meta.instance_id, "2026/05/27/fn[$LATEST]abc123");
        assert_eq!(meta.host_group.as_deref(), Some("default-host-group"));
    }

    #[test]
    fn extracts_only_instance_id_when_host_group_missing() {
        // 2022-07-01 schema: instanceId present (always for LMI), hostGroup absent.
        let record = json!({
            "initializationType": "lambda-managed-instances",
            "instanceId": "2026/05/27/fn[$LATEST]abc123"
        });
        let meta = extract_managed_instance_metadata(&record).expect("metadata should parse");
        assert_eq!(meta.instance_id, "2026/05/27/fn[$LATEST]abc123");
        assert!(meta.host_group.is_none());
    }

    #[test]
    fn returns_none_when_instance_id_absent() {
        // Standard Lambda case: neither field present. Listener should not
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
    fn ignores_empty_host_group() {
        let record = json!({
            "instanceId": "id",
            "hostGroup": ""
        });
        let meta = extract_managed_instance_metadata(&record).expect("must extract");
        assert!(
            meta.host_group.is_none(),
            "empty hostGroup string should be treated as absent"
        );
    }

    #[test]
    fn ignores_non_string_fields() {
        let record = json!({
            "instanceId": 42,
            "hostGroup": ["a", "b"]
        });
        assert!(extract_managed_instance_metadata(&record).is_none());
    }

    #[tokio::test]
    async fn try_read_returns_none_until_written() {
        // Reset the global to a clean state for this serial test.
        {
            let mut guard = MANAGED_INSTANCE_METADATA.write().await;
            *guard = None;
        }
        assert!(try_read_metadata().is_none());

        let meta = ManagedInstanceMetadata {
            instance_id: "test-id".to_string(),
            host_group: Some("test-group".to_string()),
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
