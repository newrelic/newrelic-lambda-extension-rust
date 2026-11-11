// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Sweep for agent payloads that arrived late for *other* requests (a
//! request's payload showing up only after that request's own invocation
//! already returned) — and the retry pass for this module's own buffered
//! platform-metrics failures.
//!
//! Reuses [`crate::event_loop::process_pending_agent_payloads`] (the exact
//! tier-agnostic sweep `default` tier also uses) rather than duplicating
//! it. The only thing `high_tier` changes is *how* it's run: spawned here
//! and never joined/awaited by the caller, so it can never block this
//! invocation's own `/next` — unlike `default` tier's event-loop dispatch,
//! which spawns an equivalent task and then `tokio::join!`s it together
//! with the current invocation's own processing.

use crate::apm::SharedApmApp;
use crate::config::ExtensionConfig;
use crate::logs::processor::LogProcessor;
use std::sync::Arc;

/// Spawn the late-payload sweep (for requests other than
/// `current_request_id`) and the platform-metrics retry pass, both
/// detached. Called once at the top of every `high`-tier invocation.
pub(super) fn spawn_detached(
    config: Arc<ExtensionConfig>,
    global_log_processor: Arc<LogProcessor>,
    apm_app: SharedApmApp,
    current_request_id: String,
) {
    let apm_app_for_metrics = apm_app.clone();
    tokio::spawn(async move {
        crate::event_loop::process_pending_agent_payloads(&config, &global_log_processor, &apm_app, &current_request_id).await;
    });

    tokio::spawn(async move {
        super::platform_metrics::retry_failed(apm_app_for_metrics).await;
    });
}
