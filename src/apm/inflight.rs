// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Lets the high tier cancel a batch send that is already running, without losing or
//! duplicating what it was doing.
//!
//! Aborting a batch's own task does not stop the per-type POSTs it started: each one is a
//! detached task. So every POST started *inside a batch* is registered here under that
//! batch's owner id, and `abort_owner` kills exactly those and buffers only the ones that
//! had not finished. A POST that already got its answer is gone from the registry and is
//! never touched, so it is never sent twice.
//!
//! Sends started anywhere else (retries, the default tier) run outside a batch scope, are
//! never registered, and so can never be claimed.

use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use tokio::task::{AbortHandle, JoinHandle};

tokio::task_local! {
    /// Owner id of the batch whose task is currently running, if any.
    static BATCH_OWNER: u64;
}

static NEXT_ID: AtomicU64 = AtomicU64::new(0);
static INFLIGHT: once_cell::sync::Lazy<Mutex<HashMap<u64, Entry>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(HashMap::new()));

struct Entry {
    owner: u64,
    abort: AbortHandle,
    telemetry_type: String,
    data: Vec<Value>,
    request_id: String,
    run_id: String,
    collector_host: String,
}

/// A poisoned lock must not disable cancellation, so recover the data.
fn lock() -> MutexGuard<'static, HashMap<u64, Entry>> {
    INFLIGHT.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Run `fut` as batch `owner`: POSTs it starts become cancellable through `abort_owner`.
pub async fn scope<F: Future>(owner: u64, fut: F) -> F::Output {
    BATCH_OWNER.scope(owner, fut).await
}

/// What a tracked POST needs to be buffered if it is cancelled. `None` outside a batch.
pub(super) struct Prepared {
    owner: u64,
    telemetry_type: String,
    data: Vec<Value>,
    request_id: String,
    run_id: String,
    collector_host: String,
}

/// Copies the data (an aborted task cannot buffer itself) when called inside a batch.
pub(super) fn prepare(
    telemetry_type: &str,
    data: &[Value],
    request_id: &str,
    run_id: &str,
    collector_host: &str,
) -> Option<Prepared> {
    let owner = BATCH_OWNER.try_with(|o| *o).ok()?;
    Some(Prepared {
        owner,
        telemetry_type: telemetry_type.to_string(),
        data: data.to_vec(),
        request_id: request_id.to_string(),
        run_id: run_id.to_string(),
        collector_host: collector_host.to_string(),
    })
}

/// Held by a POST task. It reports whether the task still owns its outcome and, when
/// dropped (finished, panicked or aborted), removes the registry entry.
pub(super) struct Guard {
    id: Option<u64>,
}

impl Guard {
    /// `true` if this task still owns the outcome and must buffer its own failure;
    /// `false` if `abort_owner` already claimed it and will buffer it instead. Exactly one
    /// of the two ever buffers.
    pub(super) fn finish(&mut self) -> bool {
        match self.id.take() {
            Some(id) => lock().remove(&id).is_some(),
            None => true,
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            lock().remove(&id);
        }
    }
}

/// Start the POST and register it in one step under the lock, so a cancel can never see
/// a registered task without its abort handle, or a started task that is not registered.
pub(super) fn spawn_tracked<F>(prepared: Option<Prepared>, spawn: F) -> JoinHandle<()>
where
    F: FnOnce(Guard) -> JoinHandle<()>,
{
    let Some(p) = prepared else {
        return spawn(Guard { id: None });
    };
    let mut map = lock();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let handle = spawn(Guard { id: Some(id) });
    map.insert(
        id,
        Entry {
            owner: p.owner,
            abort: handle.abort_handle(),
            telemetry_type: p.telemetry_type,
            data: p.data,
            request_id: p.request_id,
            run_id: p.run_id,
            collector_host: p.collector_host,
        },
    );
    handle
}

/// Kill every unfinished POST of batch `owner` and buffer each for retry as it was
/// (original request id, retry count 0). Returns how many were cancelled. A POST killed
/// after its request reached the collector may repeat on retry; that window is small
/// because this runs within tens of milliseconds of the batch starting.
pub fn abort_owner(owner: u64) -> usize {
    let claimed: Vec<Entry> = {
        let mut map = lock();
        let ids: Vec<u64> = map.iter().filter(|(_, e)| e.owner == owner).map(|(id, _)| *id).collect();
        ids.into_iter()
            .filter_map(|id| map.remove(&id))
            .inspect(|entry| entry.abort.abort())
            .collect()
    };
    let count = claimed.len();
    for e in claimed {
        super::telemetry_buffer::buffer_failed_telemetry(
            e.telemetry_type,
            e.data,
            e.request_id,
            e.run_id,
            e.collector_host,
        );
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::time::Duration;

    fn start_hanging(prepared: Option<Prepared>) -> (JoinHandle<()>, Option<u64>) {
        let id_out = std::sync::Arc::new(Mutex::new(None));
        let id_in = id_out.clone();
        let handle = spawn_tracked(prepared, move |guard| {
            *id_in.lock().unwrap() = guard.id;
            tokio::spawn(async move {
                let _guard = guard; // dropped on abort or completion
                tokio::time::sleep(Duration::from_secs(60)).await;
            })
        });
        let id = *id_out.lock().unwrap();
        (handle, id)
    }

    fn prepared_in(owner: u64, kind: &str) -> Option<Prepared> {
        Some(Prepared {
            owner,
            telemetry_type: kind.to_string(),
            data: vec![],
            request_id: format!("req-{owner}-{kind}"),
            run_id: "run".to_string(),
            collector_host: "host".to_string(),
        })
    }

    #[tokio::test]
    async fn outside_a_batch_nothing_is_tracked() {
        assert!(prepare("metric_data", &[], "r", "run", "host").is_none());
        let mut guard = Guard { id: None };
        assert!(guard.finish(), "an untracked task always owns its outcome");
    }

    #[tokio::test]
    async fn inside_a_batch_a_post_is_prepared_with_the_batch_owner() {
        let owner = scope(7, async { prepare("metric_data", &[], "r", "run", "host").map(|p| p.owner) }).await;
        assert_eq!(owner, Some(7));
    }

    #[tokio::test]
    #[serial]
    async fn finish_hands_ownership_to_exactly_one_party() {
        lock().clear();
        let (handle, id) = start_hanging(prepared_in(1, "a"));
        let mut guard = Guard { id };
        assert!(guard.finish(), "first to finish owns the outcome");
        let mut again = Guard { id };
        assert!(!again.finish(), "the entry is already gone");
        handle.abort();
    }

    #[tokio::test]
    #[serial]
    async fn abort_owner_kills_only_that_batchs_unfinished_posts() {
        lock().clear();
        crate::apm::telemetry_buffer::FAILED_TELEMETRY_BUFFER.lock().unwrap().clear();

        let (mine, _) = start_hanging(prepared_in(10, "span_event_data"));
        let (other, other_id) = start_hanging(prepared_in(11, "metric_data"));
        let (done, done_id) = start_hanging(prepared_in(10, "analytic_event_data"));
        // `done` finished before the cancel: it must not be touched or buffered.
        assert!(Guard { id: done_id }.finish());

        assert_eq!(abort_owner(10), 1);

        assert!(mine.await.unwrap_err().is_cancelled(), "the unfinished post is killed");
        assert!(!other.is_finished(), "another batch's post is left alone");
        assert!(lock().contains_key(&other_id.unwrap()));
        let buffered: Vec<String> = crate::apm::telemetry_buffer::FAILED_TELEMETRY_BUFFER
            .lock()
            .unwrap()
            .iter()
            .map(|t| t.telemetry_type.clone())
            .collect();
        assert_eq!(buffered, vec!["span_event_data".to_string()], "only the unfinished type is buffered");

        other.abort();
        done.abort();
        lock().clear();
        crate::apm::telemetry_buffer::FAILED_TELEMETRY_BUFFER.lock().unwrap().clear();
    }

    #[tokio::test]
    #[serial]
    async fn dropping_the_guard_removes_the_entry_even_if_the_task_never_finishes() {
        lock().clear();
        let (handle, id) = start_hanging(prepared_in(20, "metric_data"));
        assert!(lock().contains_key(&id.unwrap()));
        handle.abort();
        let _ = handle.await;
        assert!(!lock().contains_key(&id.unwrap()), "an aborted task leaves nothing behind");
    }
}
