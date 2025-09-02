//! New Relic Client
//!
//! This module contains the NewRelicClient, which is responsible for sending
//! telemetry data to the New Relic endpoints.

use crate::config::get_config;
use reqwest::Client;
use std::io::{Error, ErrorKind, Result};

/// A client for sending data to New Relic.
#[derive(Debug)]
pub struct NewRelicClient {
    http_client: Client,
}

impl NewRelicClient {
    /// Creates a new New Relic client.
    pub fn new() -> Self {
        Self {
            http_client: Client::new(),
        }
    }

    /// Sends a batch of compressed logs to the New Relic Log API.
    pub async fn send_logs(&self, compressed_payload: Vec<u8>) -> Result<()> {
        let config = get_config();
        let license_key = config.new_relic.license_key.as_ref().ok_or_else(|| {
            Error::new(ErrorKind::NotFound, "New Relic License Key not found")
        })?;

        let response = self
            .http_client
            .post(&config.new_relic.log_endpoint)
            .header("Content-Encoding", "gzip")
            .header("Content-Type", "application/json")
            .header("X-License-Key", license_key)
            .header("X-Event-Source", "logs")
            .body(compressed_payload)
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e))?;

        if !response.status().is_success() {
            let err_msg = format!(
                "Failed to send logs to New Relic: status {}, body {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
            return Err(Error::new(ErrorKind::Other, err_msg));
        }

        Ok(())
    }
}
