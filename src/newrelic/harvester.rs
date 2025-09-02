//! Harvester
//!
//! This module is responsible for collecting data from the processors,
//! building the final payload, and sending it to New Relic.

use crate::{
    config::ExtensionConfig,
    newrelic::{client::NewRelicClient, payload},
};
use flate2::{write::GzEncoder, Compression};
use std::collections::HashMap;
use std::io::Write;
use tracing::{error, info};
use crate::newrelic::payload::LogMessage;

/// Builds a payload from a batch of logs, compresses it, and sends it to New Relic.
pub async fn send_log_batch(
    config: &ExtensionConfig,
    log_batch: Vec<LogMessage>,
    client: &NewRelicClient,
    invoked_function_arn: &str,
) {
    if log_batch.is_empty() {
        return;
    }

    info!("[Harvester] Sending {} log entries", log_batch.len());

    // Build the common block of attributes
    let mut common_block = HashMap::new();
    common_block.insert("plugin.type", "newrelic-lambda-extension".into());
    common_block.insert("faas.arn", invoked_function_arn.into());
    common_block.insert("faas.name", config.aws.function_name.clone().into());
    
    // Add custom tags if they are configured TO DO
    // if let Some(tags_str) = &config.new_relic.nr_tags {
    //     let delimiter = &config.new_relic.nr_env_delimiter;
    //     for tag in tags_str.split(delimiter) {
    //         let parts: Vec<&str> = tag.split(':').collect();
    //         if parts.len() == 2 {
    //             common_block.insert(parts[0], parts[1].into());
    //         }
    //     }
    // }

    let payload_data = vec![payload::DetailedLog {
        common: payload::Common {
            attributes: common_block,
        },
        logs: log_batch,
    }];

    // Serialize and compress the payload
    match serde_json::to_vec(&payload_data) {
        Ok(json_bytes) => {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            if let Err(e) = encoder.write_all(&json_bytes) {
                error!("[Harvester] Failed to compress log payload: {}", e);
                return;
            }
            match encoder.finish() {
                Ok(compressed_bytes) => {
                    // Send the compressed payload
                    if let Err(e) = client.send_logs(compressed_bytes).await {
                        error!("[Harvester] Failed to send logs: {}", e);
                    } else {
                        info!("[Harvester] Successfully sent logs to New Relic");
                    }
                }
                Err(e) => error!("[Harvester] Failed to finish compression: {}", e),
            }
        }
        Err(e) => error!("[Harvester] Failed to serialize log payload: {}", e),
    }
}

