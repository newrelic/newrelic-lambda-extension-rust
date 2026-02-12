// src/agent/mod.rs
pub mod ipc;
pub mod batch;
pub mod payload;

#[cfg(test)]
mod batch_tests;
#[cfg(test)]
mod payload_tests;