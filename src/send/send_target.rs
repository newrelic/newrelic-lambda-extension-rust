// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Phase A — interface-only adapter over the two existing send
//! implementations (`newrelic::client::NewRelicClient`,
//! `apm::collector::{send_error_events, send_apm_telemetry,
//! send_platform_metrics}`), per `REDESIGN_PLAN.md` §3.1/Phase A.
//!
//! Deliberately **not** a rewrite of either implementation's internals —
//! their retry loops, backoff, gzip, and distinct error types
//! (`newrelic::client::SendError`, `apm::collector::CollectorError`,
//! `apm::collector::MetricApiError`) are untouched. This module only adds:
//!
//! 1. [`BoxedSend`] — the one future shape `SendScheduler` (Phase B) spawns
//!    and polls generically, regardless of which of the three kinds it
//!    wraps.
//! 2. `from_*_result` — pure mapping functions from each existing `Result`
//!    type into the shared [`SendOutcome`], so the scheduler and circuit
//!    breaker can observe a uniform `Success`/`Failure` signal without
//!    needing to know about three different error enums.
//!
//! Buffering a failed/timed-out payload into the right `FAILED_*` buffer
//! remains kind-specific and is **not** part of this adapter — that logic
//! stays where it already lives (`agent/batch.rs`, `apm/telemetry_buffer.rs`,
//! `apm/metric_api_buffer.rs`); the scheduler calls back into the
//! appropriate buffer function itself when a send fails or times out.

#![allow(dead_code)]

use super::SendOutcome;
use std::future::Future;
use std::pin::Pin;

/// The uniform future shape the scheduler spawns/polls for any of the three
/// send kinds. `Send + 'static` so it can be handed to `tokio::spawn`.
pub type BoxedSend = Pin<Box<dyn Future<Output = SendOutcome> + Send>>;

/// Map a `newrelic::client` send result (log batch / agent payload) to the
/// shared outcome. Every `SendError` variant — network failure, server
/// exhaustion after retries, or client rejection — is a `Failure` for
/// breaker/scheduler purposes; the distinction between them still matters
/// upstream (e.g. for logging) and is preserved by the caller, not lost
/// here.
pub fn from_client_result<T>(result: &Result<T, crate::newrelic::client::SendError>) -> SendOutcome {
    match result {
        Ok(_) => SendOutcome::Success,
        Err(_) => SendOutcome::Failure,
    }
}

/// Map an `apm::collector` `CollectorError`-based send result (error events
/// / APM telemetry) to the shared outcome. `anyhow::Result` is used upstream
/// so any error — including a `CollectorError::Disconnect`/`RestartException`
/// downcast — counts as `Failure` here; reconnect signaling is handled
/// separately via `signal_reconnect_needed`/`take_reconnect_needed`, not by
/// this mapping.
pub fn from_collector_result<T>(result: &anyhow::Result<T>) -> SendOutcome {
    match result {
        Ok(_) => SendOutcome::Success,
        Err(_) => SendOutcome::Failure,
    }
}

/// Map a Metric API send result to the shared outcome. `MetricApiError`'s
/// retryable/permanent/network distinction is preserved by the caller for
/// buffering purposes (`apm/metric_api_buffer.rs` already differentiates on
/// this) — this mapping only answers the breaker's single yes/no question.
pub fn from_metric_api_result<T>(
    result: &Result<T, crate::apm::collector::MetricApiError>,
) -> SendOutcome {
    match result {
        Ok(_) => SendOutcome::Success,
        Err(_) => SendOutcome::Failure,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apm::collector::MetricApiError;
    use crate::newrelic::client::SendError;

    #[test]
    fn client_result_success_maps_to_success() {
        let ok: Result<(), SendError> = Ok(());
        assert_eq!(from_client_result(&ok), SendOutcome::Success);
    }

    #[test]
    fn client_result_any_error_variant_maps_to_failure() {
        let network: Result<(), SendError> = Err(SendError::ServerExhausted { status: 503 });
        assert_eq!(from_client_result(&network), SendOutcome::Failure);

        let rejected: Result<(), SendError> = Err(SendError::ClientRejected { status: 400 });
        assert_eq!(from_client_result(&rejected), SendOutcome::Failure);
    }

    #[test]
    fn collector_result_maps_success_and_failure() {
        let ok: anyhow::Result<()> = Ok(());
        assert_eq!(from_collector_result(&ok), SendOutcome::Success);

        let err: anyhow::Result<()> = Err(anyhow::anyhow!("boom"));
        assert_eq!(from_collector_result(&err), SendOutcome::Failure);
    }

    #[test]
    fn metric_api_result_maps_success_and_every_failure_variant() {
        let ok: Result<(), MetricApiError> = Ok(());
        assert_eq!(from_metric_api_result(&ok), SendOutcome::Success);

        let retryable: Result<(), MetricApiError> = Err(MetricApiError::Retryable {
            status: 503,
            retry_after: None,
        });
        assert_eq!(from_metric_api_result(&retryable), SendOutcome::Failure);

        let permanent: Result<(), MetricApiError> =
            Err(MetricApiError::Permanent { status: 400 });
        assert_eq!(from_metric_api_result(&permanent), SendOutcome::Failure);

        let network: Result<(), MetricApiError> =
            Err(MetricApiError::Network(anyhow::anyhow!("boom")));
        assert_eq!(from_metric_api_result(&network), SendOutcome::Failure);
    }
}
