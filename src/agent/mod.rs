// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// src/agent/mod.rs
pub mod ipc;
pub mod batch;
pub mod payload;

#[cfg(test)]
mod payload_tests;
#[cfg(test)]
mod batch_tests;