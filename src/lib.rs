//! Library facade for benchmarks and external tooling.
//!
//! The main binary logic lives in `main.rs`. This file re-exports
//! the hot-path modules needed by criterion benchmarks in `benches/`.

#![allow(clippy::all)]
#![allow(clippy::pedantic)]
#![allow(clippy::unwrap_used)]
#![allow(missing_debug_implementations)]
#![allow(dead_code)]
#![allow(unused_imports)]

pub mod config;
pub mod agent;
pub mod apm;
pub mod retry;
pub mod version;
mod context;
mod newrelic;
mod credentials;
mod trace;
mod runtime;
mod logs;
mod platform;
mod request;
mod telemetry;
mod event_loop;
mod error_synthesis;

use std::sync::{Arc, RwLock};
use once_cell::sync::Lazy;

pub const EXTENSION_NAME: &str = env!("CARGO_PKG_NAME");
pub const EXTENSION_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Globals required by event_loop (mirrors main.rs definitions)
static CURRENT_INVOCATION_CONTEXT: Lazy<Arc<RwLock<context::InvocationContext>>> = Lazy::new(|| {
    Arc::new(RwLock::new(context::InvocationContext::default()))
});

static IS_WARM_START: Lazy<Arc<std::sync::atomic::AtomicBool>> =
    Lazy::new(|| Arc::new(std::sync::atomic::AtomicBool::new(false)));

static APM_APP: Lazy<Arc<tokio::sync::RwLock<Option<apm::ApmApp>>>> =
    Lazy::new(|| Arc::new(tokio::sync::RwLock::new(None)));
