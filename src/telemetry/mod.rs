// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Telemetry Module
//! 
//! This module contains all telemetry-related functionality for the New Relic
//! Lambda Extension, including the telemetry listener server and subscription
//! management.

pub mod listener;
pub mod managed_instance;
pub mod normal_log_stream;

#[cfg(test)]
mod listener_tests;
