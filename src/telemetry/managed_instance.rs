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
#[path = "managed_instance_tests.rs"]
mod tests;
