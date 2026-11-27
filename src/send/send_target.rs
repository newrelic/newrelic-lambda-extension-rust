// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`BoxedSend`]: the one future shape `SendScheduler` spawns and polls, whichever send
//! kind it wraps. `event_loop.rs` builds the `SendOutcome` inline because the batch and
//! log-flush tasks buffer their own failures.

use super::SendOutcome;
use std::future::Future;
use std::pin::Pin;

/// The uniform future shape the scheduler spawns/polls for any of the three
/// send kinds. `Send + 'static` so it can be handed to `tokio::spawn`.
pub type BoxedSend = Pin<Box<dyn Future<Output = SendOutcome> + Send>>;
