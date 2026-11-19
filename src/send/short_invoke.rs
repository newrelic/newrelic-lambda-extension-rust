// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `high` tier: what became of each short (no-op) invoke.
//!
//! A short invoke is decided at the moment its duration is known, but its telemetry keeps
//! arriving afterwards (the agent payload, `platform.report` and logs can all come later,
//! up to the Telemetry API's delivery lag). Remembering the verdict per request id lets
//! every later stage drop, or keep, that late data the same way the verdict did.
//!
//! Bounded: past [`MAX_REMEMBERED`] requests the oldest verdict is forgotten.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, MutexGuard, PoisonError};

const MAX_REMEMBERED: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShortOutcome {
    /// No error was recorded: nothing of this request is sent.
    Dropped,
    /// An error was recorded: its agent payload and report are kept, and of its logs only
    /// the ERROR-level ones.
    ErrorKept,
}

#[derive(Default)]
struct Remembered {
    outcomes: HashMap<String, ShortOutcome>,
    order: VecDeque<String>,
}

static SHORT_INVOKES: once_cell::sync::Lazy<Mutex<Remembered>> =
    once_cell::sync::Lazy::new(|| Mutex::new(Remembered::default()));

fn lock() -> MutexGuard<'static, Remembered> {
    SHORT_INVOKES.lock().unwrap_or_else(PoisonError::into_inner)
}

pub fn mark(request_id: &str, outcome: ShortOutcome) {
    let mut guard = lock();
    if guard.outcomes.insert(request_id.to_string(), outcome).is_none() {
        guard.order.push_back(request_id.to_string());
    }
    while guard.order.len() > MAX_REMEMBERED {
        if let Some(oldest) = guard.order.pop_front() {
            guard.outcomes.remove(&oldest);
        }
    }
}

pub fn get(request_id: &str) -> Option<ShortOutcome> {
    lock().outcomes.get(request_id).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remembers_the_verdict_per_request() {
        mark("si-a", ShortOutcome::Dropped);
        mark("si-b", ShortOutcome::ErrorKept);
        assert_eq!(get("si-a"), Some(ShortOutcome::Dropped));
        assert_eq!(get("si-b"), Some(ShortOutcome::ErrorKept));
        assert_eq!(get("si-unknown"), None);
    }

    #[test]
    fn forgets_the_oldest_past_the_bound() {
        mark("si-old", ShortOutcome::Dropped);
        for i in 0..MAX_REMEMBERED {
            mark(&format!("si-fill-{i}"), ShortOutcome::Dropped);
        }
        assert_eq!(get("si-old"), None, "the oldest verdict is evicted");
        assert_eq!(get(&format!("si-fill-{}", MAX_REMEMBERED - 1)), Some(ShortOutcome::Dropped));
    }
}
