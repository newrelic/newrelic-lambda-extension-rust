// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Log sending for `SendTier::High` — deliberately **not** part of the
//! shared `runtimeDone` budget in [`super::SCHEDULER`].
//!
//! Lambda freezes the execution environment almost immediately after the
//! extension calls `/next`. Starting a log-flush network call right at
//! `runtimeDone` (as a bounded-wait design would) has essentially no
//! chance of finishing before freeze — it would just resume mid-handshake,
//! unreliably, whenever the *next* invocation thaws. So: the batch is
//! never touched at `runtimeDone` at all. It sits in `LogProcessor`'s own
//! `log_batch` until the *next* invocation's own start — environment
//! guaranteed live — where [`send_previous_invocation_batch`] spawns the
//! real send, running concurrently with that invocation's own work,
//! never blocking it.
//!
//! Uses [`LogProcessor::flush_with_request_id`] (not the plain `flush()`
//! trait method `default` tier uses) so the retry-attribution bookkeeping
//! pins to the request that actually owns the batch — the *previous*
//! invocation, tracked by [`LAST_OWNER`] — rather than whatever's live in
//! `LogProcessor`'s shared `invocation_context` by the time this runs
//! (which, by construction, already belongs to the *current* invocation
//! by this point in the shared dispatch flow).

use crate::logs::processor::LogProcessor;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

/// The request_id that owns whatever's currently in `log_batch` — i.e.
/// whichever invocation last called [`send_previous_invocation_batch`].
/// Updated to the *caller's* request_id on every call; read (and *not*
/// yet updated) at the top of each call, so it always reflects the
/// previous invocation's id when a new flush is pinned.
static LAST_OWNER: once_cell::sync::Lazy<Mutex<Option<String>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(None));

/// Guards against two overlapping `flush_with_request_id` sends: if one is
/// still running, skip spawning a new one entirely — new logs simply stay
/// in `LogProcessor`'s own batch to go out on the invocation after this
/// one, same as if the threshold-triggered auto-flush path were busy.
static IN_FLIGHT: once_cell::sync::Lazy<Mutex<Option<JoinHandle<()>>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(None));

/// Spawn the real send for whatever's sitting in `log_batch` right now —
/// never waited on here. `current_request_id` is the caller's own id, used
/// only to seed [`LAST_OWNER`] for the *next* call; the pinned id used for
/// *this* send is whatever [`LAST_OWNER`] held *before* this call.
pub(super) fn send_previous_invocation_batch(log_processor: &Arc<LogProcessor>, current_request_id: &str) {
    {
        let mut in_flight = IN_FLIGHT.lock().unwrap();
        if in_flight.as_ref().is_some_and(|h| !h.is_finished()) {
            debug!("APM mode (high): a log flush is still running — leaving the batch as-is for next time");
            return;
        }
        *in_flight = None;
    }

    let pinned_request_id = {
        let mut guard = LAST_OWNER.lock().unwrap();
        let previous = guard.clone().unwrap_or_else(|| current_request_id.to_string());
        *guard = Some(current_request_id.to_string());
        previous
    };

    let log_processor_clone = log_processor.clone();
    let handle = tokio::spawn(async move {
        if let Err(e) = log_processor_clone.flush_with_request_id(&pinned_request_id).await {
            error!("APM mode (high): failed to flush logs (pinned request {}): {}", pinned_request_id, e);
        }
    });

    match IN_FLIGHT.lock() {
        Ok(mut guard) => {
            if let Some(orphan) = guard.replace(handle) {
                // Should not happen given the is_finished() check above, but
                // never silently leak a handle.
                if !orphan.is_finished() {
                    warn!("APM mode (high): overwriting an unresolved log-flush handle — aborting the orphan");
                    orphan.abort();
                }
            }
        }
        Err(e) => error!("logs IN_FLIGHT lock poisoned: {}", e),
    }
}
