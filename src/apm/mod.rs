//! APM (Application Performance Monitoring) mode implementation
//!
//! This module implements the APM Lambda mode, which parses agent payloads
//! and sends them to the APM collector instead of the serverless ingest API.

pub mod app;
pub mod collector;
pub mod connection;
pub mod error_event;
pub mod id_generator;
pub mod metric_converter;
pub mod payload_parser;

pub use app::{ApmApp, SharedApmApp};

