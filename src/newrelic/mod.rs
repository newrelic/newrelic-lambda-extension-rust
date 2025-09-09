//! The New Relic module contains all components for interacting with New Relic APIs,
//! including the client for sending data, payload definitions, and the harvesting logic.

pub mod client;
pub mod harvester;
pub mod payload;
pub mod flush;
pub mod agent_payload_processor;