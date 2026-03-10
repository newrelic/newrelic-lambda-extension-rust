// src/agent/mod.rs
pub mod ipc;
pub mod batch;
pub mod payload;

#[cfg(test)]
mod payload_tests;
#[cfg(test)]
mod batch_tests;