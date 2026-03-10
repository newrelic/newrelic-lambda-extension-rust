//! Telemetry Module
//! 
//! This module contains all telemetry-related functionality for the New Relic
//! Lambda Extension, including the telemetry listener server and subscription
//! management.

pub mod listener;

#[cfg(test)]
mod listener_tests;
