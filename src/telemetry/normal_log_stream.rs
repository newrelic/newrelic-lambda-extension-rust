// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Best-effort `CloudWatch` log stream identifier for Standard (non-LMI) Lambda.
//!
//! Extensions cannot read `AWS_LAMBDA_LOG_STREAM_NAME` (or
//! `AWS_LAMBDA_LOG_GROUP_NAME`) directly: AWS's Extensions API docs explicitly
//! exclude both from the extension process's environment — they are
//! "specific to the runtime process." The only channel available to an
//! extension is the Telemetry API, whose `platform.initStart` event carries
//! an optional `instanceId` field. That field is documented generically (NOT
//! LMI-exclusive, despite an earlier assumption in this codebase — see
//! `managed_instance.rs`), but on live Standard Lambda traffic its value has
//! been observed to equal the real `CloudWatch` log stream name for that
//! execution environment (verified 2026-09-08 against actual ingested logs).
//!
//! This is an AWS implementation detail, not a documented guarantee — treated
//! as best-effort. LMI's `instanceId` means something different there (the
//! managed-instance host id; see `managed_instance.rs`), so this capture is
//! gated to Standard Lambda only (`!is_lmi` at the call site in
//! `telemetry::listener`).
//!
//! Live verification (2026-09-08) was against a plain on-demand cold start
//! only. `DeploymentContext::Normal` also covers Provisioned Concurrency and
//! `SnapStart`, whose init lifecycle differs (`SnapStart` in particular
//! restores from a pre-taken snapshot) — whether `platform.initStart` re-fires
//! per restored/provisioned environment the same way it does for a fresh
//! on-demand cold start has not been verified. If it doesn't, `aws.logStream`
//! would be absent (or carry a stale value from the snapshot's original
//! environment) for that function's invocations — consistent with the
//! best-effort framing above, not a correctness bug, but worth a follow-up
//! live check against those deployment shapes.

use std::sync::Arc;

use once_cell::sync::Lazy;
use tokio::sync::RwLock;

/// Global, set-once snapshot of the best-effort log stream identifier,
/// populated by the telemetry listener on the first `platform.initStart`
/// event of the cold start. `None` until populated, and stays `None` if AWS
/// omits `instanceId` for this execution environment.
pub static NORMAL_LAMBDA_LOG_STREAM: Lazy<Arc<RwLock<Option<String>>>> =
    Lazy::new(|| Arc::new(RwLock::new(None)));

/// Synchronous, lock-free read for sync call sites. Returns `None` if the
/// lock is currently held for writing (in practice only during the
/// listener's initStart write, a single short critical section) — the
/// caller simply omits `aws.logStream` for that one payload, since the
/// value is set once at cold start and read for the remainder of the
/// container lifetime.
#[must_use]
pub fn try_read() -> Option<String> {
    NORMAL_LAMBDA_LOG_STREAM.try_read().ok().and_then(|guard| guard.clone())
}

#[cfg(test)]
#[path = "normal_log_stream_tests.rs"]
mod tests;
