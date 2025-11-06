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

// Re-export main types
pub use app::{ApmApp, SharedApmApp};
pub use collector::CollectorError;
pub use connection::{ConnectResponse, PreconnectResponse};
pub use error_event::generate_error_event_from_fault;
