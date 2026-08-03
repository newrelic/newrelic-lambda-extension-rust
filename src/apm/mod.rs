// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! APM (Application Performance Monitoring) mode implementation
//!
//! This module implements the APM Lambda mode, which parses agent payloads
//! and sends them to the APM collector instead of the serverless ingest API.

pub mod app;
pub mod collector;
pub mod connection;
pub mod error_event;
pub mod id_generator;
pub mod metric_api_buffer;
pub mod metric_converter;
pub mod otlp;
pub mod otlp_buffer;
pub mod payload_parser;
pub mod telemetry_buffer;
mod id_generator_tests;

pub use app::{ApmApp, SharedApmApp};

