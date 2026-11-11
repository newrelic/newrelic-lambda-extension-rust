// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `SendTier::High` — a from-scratch, standalone implementation of
//! APM-mode request processing that never blocks the next `/next` call by
//! more than one small, explicit, shared budget.
//!
//! Built fresh on a branch cut directly from `main`, specifically so this
//! module shares no code with `default`/`medium` tier's existing,
//! unmodified request-processing flow (`event_loop::process_apm_request`) —
//! see `ARCHITECTURE.md` (if present) or the branch's own commit history
//! for the full rationale. `default` tier is untouched: zero lines of its
//! own logic changed to make room for this module.
//!
//! ## Design, in one paragraph per concern
//!
//! - **Agent payload + platform-metrics**: checked for and spawned *after*
//!   `runtimeDone` (not at the top of the invocation) — the agent delivers
//!   its payload over a named pipe, asynchronously, and checking too early
//!   can miss a payload that arrives a few/tens of ms into the same
//!   invocation, pushing it needlessly into the next invocation's
//!   late-payload sweep instead of this invocation's own bounded
//!   spawn+resolve. Resolved against **one shared** budget (not one budget
//!   each) — see [`RUNTIME_DONE_BUDGET_MS`]. Whatever's still running when
//!   the budget elapses is aborted and buffered for a background retry on
//!   a later invocation, attributed to the request that actually produced
//!   it.
//! - **Logs**: deliberately excluded from that budget. Lambda freezes the
//!   execution environment almost immediately after the extension calls
//!   `/next`, so starting a log-flush network call right at `runtimeDone`
//!   has essentially no chance of finishing before freeze — it would just
//!   resume mid-handshake, unreliably, whenever the *next* invocation
//!   thaws. Instead: never started at `runtimeDone` at all. The batch sits
//!   untouched until the *next* invocation's own start, where the real
//!   send begins — environment guaranteed live, running concurrently with
//!   that invocation's own work, never blocking it either. See
//!   [`logs`] module doc.
//! - **Late-arriving payloads for *other* requests** (a request's payload
//!   that showed up after that request's own invocation already returned):
//!   swept detached, never joined with the current invocation's own
//!   processing — see [`pending_sweep`].
//!
//! Reuses `event_loop`'s tier-agnostic helpers where they're genuinely
//! tier-agnostic (sending one payload, waiting for `runtimeDone`, the
//! agent-payload failure buffer + its retry loop) rather than duplicating
//! them — see each submodule's own doc for exactly what it reuses.

mod agent_payload;
mod logs;
mod pending_sweep;
mod platform_metrics;
mod scheduler;

use crate::apm::SharedApmApp;
use crate::config::ExtensionConfig;
use crate::logs::processor::LogProcessor;
use crate::request::{self, RequestProcessingState};
use scheduler::Scheduler;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

/// The one customer-facing knob. `Default` is the safe default — zero
/// behavior change for existing customers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SendTier {
    /// Today's behavior: `event_loop::process_apm_request`, unmodified.
    #[default]
    Default,
    /// This module.
    High,
}

pub const SEND_TIER_ENV_VAR: &str = "NEW_RELIC_SEND_TIER";

impl SendTier {
    /// Parse from the raw env var value. Case-insensitive, trims
    /// whitespace. Unknown/empty values fall back to `Default` — never
    /// panics (a customer's Lambda must never crash over a typo in this
    /// knob).
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "high" => SendTier::High,
            "default" | "" => SendTier::Default,
            other => {
                tracing::warn!(
                    "Unknown {} value '{}' - falling back to 'default'",
                    SEND_TIER_ENV_VAR,
                    other
                );
                SendTier::Default
            }
        }
    }

    pub fn from_env() -> Self {
        match std::env::var(SEND_TIER_ENV_VAR) {
            Ok(raw) => Self::parse(&raw),
            Err(_) => SendTier::Default,
        }
    }
}

/// Hard cap on how long [`process_invocation`] may hold `/next` past
/// `runtimeDone` waiting on agent-payload/platform-metrics sends,
/// combined — not one per kind. Whatever hasn't sent by the time this
/// elapses is aborted and buffered for a background retry, never carried
/// forward as a blocking wait on some future invocation's own boundary.
///
/// Logs are not part of this budget — see this module's own doc for why.
pub const RUNTIME_DONE_BUDGET_MS: u64 = 100;

/// The scheduler driving agent-payload + platform-metrics resolution.
/// One instance for the process's lifetime — see `scheduler` module doc
/// for why a single shared budget needs one shared instance rather than
/// one per invocation.
static SCHEDULER: once_cell::sync::Lazy<Scheduler> = once_cell::sync::Lazy::new(Scheduler::new);

/// Entry point for `SendTier::High` — the sole thing `event_loop` calls
/// into. Mirrors `event_loop::process_apm_request`'s role for `default`
/// tier, but every line past this signature is new.
pub async fn process_invocation(
    request_id: String,
    invoked_function_arn: String,
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
    apm_app: SharedApmApp,
    deadline_ms: i64,
) {
    debug!("APM mode (high): starting processing for request: {}", request_id);

    // Logs: send whatever the *previous* invocation left in the batch, now
    // that this invocation's environment is guaranteed live. Never blocks
    // -- runs concurrently with everything below. See `logs` module doc.
    logs::send_previous_invocation_batch(&global_log_processor, &request_id);

    // Late-arriving payloads for OTHER requests: detached, never blocks
    // this invocation's own /next. See `pending_sweep` module doc.
    pending_sweep::spawn_detached(config.clone(), global_log_processor.clone(), apm_app.clone(), request_id.clone());

    let Some(state) = request::REQUEST_PROCESSORS.remove(&request_id).map(|(_k, v)| v) else {
        tracing::error!("APM mode (high): no processing state found for request: {}", request_id);
        return;
    };
    let state: RequestProcessingState = state;

    global_log_processor.set_invocation_start_time(chrono::Utc::now());
    state.platform_processor.process_invoke_event(&request_id, &invoked_function_arn);

    event_loop_wait_for_runtime_done(&request_id, deadline_ms, &config, &global_log_processor).await;

    // Checked here, right before the bounded resolve below -- not at the
    // top of the invocation. The agent sends its payload over a named
    // pipe, asynchronously; checking early can miss a payload that arrives
    // a few/tens of ms into the same invocation (confirmed in practice --
    // it would otherwise fall through to the next invocation's late-payload
    // sweep entirely, skipping the bounded-wait/retry path below for no
    // reason). Checking after runtimeDone instead gives the payload the
    // rest of the invocation's own real time to arrive, maximizing the
    // chance it's actually here to spawn+resolve this cycle.
    let has_run_id = apm_app.read().await.is_some();

    agent_payload::spawn_if_ready(
        &SCHEDULER,
        &request_id,
        &invoked_function_arn,
        &config,
        &global_log_processor,
        &apm_app,
        &state,
        has_run_id,
    )
    .await;

    platform_metrics::spawn_if_ready(&SCHEDULER, &apm_app, &request_id, &invoked_function_arn).await;

    // Single shared bound covering agent payload + platform-metrics
    // together -- never `2 * RUNTIME_DONE_BUDGET_MS`. Whatever's still
    // running when it elapses is aborted here, in *this* invocation, so a
    // later invocation's own /next never pays for it.
    let (agent_outcome, metrics_outcome) = SCHEDULER
        .resolve_both_within(Duration::from_millis(RUNTIME_DONE_BUDGET_MS))
        .await;
    agent_payload::handle_outcome(&agent_outcome);
    platform_metrics::handle_outcome(&metrics_outcome);

    // Logs/platform processors: no end-of-invocation flush call here --
    // logs are handled entirely via send_previous_invocation_batch
    // (above, at the top of a future invocation); platform.* lines already
    // live in LogProcessor's own batch, and PlatformProcessor::flush() is a
    // hardcoded no-op in this codebase.

    request::cleanup_request_processing_state_internal(&request_id, true);
    debug!("APM mode (high): completed processing for request: {}", request_id);
}

/// Bound on the SHUTDOWN-only drain below. There's no "next invocation" to
/// defer to at SHUTDOWN, so this is deliberately more generous than
/// [`RUNTIME_DONE_BUDGET_MS`] — the caller runs this inside its own overall
/// SHUTDOWN timeout anyway (`event_loop::SHUTDOWN_TIMEOUT_MS`), so this is
/// just an inner bound to guarantee forward progress, not a tight latency
/// target like the per-invocation one is.
const SHUTDOWN_DRAIN_BUDGET_MS: u64 = 500;

/// Final drain at SHUTDOWN. Call sites: `event_loop`'s own SHUTDOWN handling
/// is completely tier-agnostic and has no idea this module's statics exist
/// ([`SCHEDULER`]'s in-flight entries, `logs`'s in-flight send,
/// `platform_metrics`'s failure buffer) — without this, anything caught
/// mid-flight or buffered in them at the moment a container shuts down
/// would be silently lost. `event_loop`'s own sweep of raw `REQUEST_DATA`
/// agent buffers and `LogProcessor`'s `log_batch`/`failed_logs_buffer` is
/// unaffected by this and still runs regardless of tier — this only covers
/// what's specific to `high_tier`.
pub async fn drain_at_shutdown(apm_app: SharedApmApp) {
    debug!("APM mode (high): draining at shutdown");

    let (agent_outcome, metrics_outcome) = SCHEDULER
        .resolve_both_within(Duration::from_millis(SHUTDOWN_DRAIN_BUDGET_MS))
        .await;
    agent_payload::handle_outcome(&agent_outcome);
    platform_metrics::handle_outcome(&metrics_outcome);

    // No "next invocation" to send this in parallel with -- just wait for
    // it, bounded by the budget above (via the resolve already having spent
    // some of the caller's overall SHUTDOWN timeout; logs::await_in_flight
    // itself is unbounded, but the caller's own outer SHUTDOWN_TIMEOUT_MS
    // timeout wraps this whole function).
    logs::await_in_flight().await;

    // One last attempt at whatever platform-metrics reports are still
    // buffered — mirrors event_loop's own "process ALL remaining pending
    // agent payloads before shutdown" for the agent-payload buffer.
    platform_metrics::retry_failed(apm_app).await;
}

/// Thin re-export so `high_tier`'s submodules don't reach across to
/// `event_loop` directly for this one tier-agnostic helper (identical
/// behavior/bound for both tiers: wait for `platform.runtimeDone`, then a
/// short grace for trailing telemetry).
async fn event_loop_wait_for_runtime_done(
    request_id: &str,
    deadline_ms: i64,
    config: &ExtensionConfig,
    log_processor: &Arc<LogProcessor>,
) {
    crate::event_loop::wait_for_runtime_done_with_grace(request_id, deadline_ms, config, log_processor).await;
}
