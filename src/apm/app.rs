//! Main APM app orchestrator
//!
//! Based on internal_app.go NewApp(), connectRoutine(), doHarvest()

use super::collector::{
    send_apm_telemetry, send_error_events, send_platform_metrics, CMD_ANALYTIC_EVENTS, 
    CMD_CUSTOM_EVENTS, CMD_ERROR_DATA, CMD_LOG_EVENTS, CMD_METRICS, CMD_SPAN_EVENTS, 
    CMD_TRANSACTION_SAMPLES,
};
use super::connection::{connect, preconnect};
use super::metric_converter::{convert_to_apm_metrics, parse_lambda_report_log};
use super::payload_parser::parse_agent_payload;
use anyhow::{Context, Result};
use reqwest::Client;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

#[derive(Debug)]
pub struct ApmApp {
    pub run_id: String,
    pub entity_guid: String,
    pub collector_host: String,
    pub license_key: String,
    pub client: Client,
}

impl ApmApp {
    pub async fn new(
        license_key: String,
        apm_host: String,
        _metric_endpoint: String,
        client: Client,
    ) -> Result<Self> {
        info!("Initializing APM app connection");

        // Retry connection up to 3 times with backoff
        let backoff_ms = [200, 500, 900];
        let mut last_error = None;

        for (attempt, delay) in backoff_ms.iter().enumerate() {
            debug!("APM connection attempt {} of {}", attempt + 1, 3);

            match Self::try_connect(&license_key, &apm_host, &client).await {
                Ok(app) => {
                    info!(
                        "APM connection successful: run_id={}, entity_guid={}",
                        app.run_id, app.entity_guid
                    );
                    return Ok(app);
                }
                Err(e) => {
                    warn!("APM connection attempt {} failed: {}", attempt + 1, e);
                    last_error = Some(e);

                    if attempt < backoff_ms.len() - 1 {
                        debug!("Retrying in {}ms", delay);
                        tokio::time::sleep(tokio::time::Duration::from_millis(*delay)).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Failed to connect to APM collector")))
    }

    /// Attempt a single connection
    async fn try_connect(
        license_key: &str,
        apm_host: &str,
        client: &Client,
    ) -> Result<ApmApp> {
        // Step 1: PreConnect to get collector host
        let collector_host = preconnect(client, license_key, apm_host)
            .await
            .context("PreConnect failed")?;

        debug!("PreConnect returned collector host: {}", collector_host);

        // Get AWS Lambda function info from environment
        let function_name = std::env::var("AWS_LAMBDA_FUNCTION_NAME")
            .unwrap_or_else(|_| "unknown".to_string());
        let function_version = std::env::var("AWS_LAMBDA_FUNCTION_VERSION")
            .unwrap_or_else(|_| "$LATEST".to_string());
        let function_arn = std::env::var("AWS_LAMBDA_FUNCTION_ARN")
            .unwrap_or_else(|_| format!("arn:aws:lambda:us-east-1:000000000000:function:{}", function_name));
        
        // Parse region and account ID from ARN (format: arn:aws:lambda:region:account-id:function:function-name)
        let arn_parts: Vec<&str> = function_arn.split(':').collect();
        let region = if arn_parts.len() > 3 { arn_parts[3].to_string() } else { "us-east-1".to_string() };
        let account_id = if arn_parts.len() > 4 { arn_parts[4].to_string() } else { "000000000000".to_string() };

        // Detect runtime and agent version
        let runtime = super::connection::detect_runtime();
        let agent_version = super::connection::detect_agent_version(&runtime);

        // Step 2: Connect to get run_id and entity_guid
        let connect_resp = connect(
            client, 
            license_key, 
            &collector_host,
            &function_name,
            &function_arn,
            &account_id,
            &region,
            &function_version,
            &runtime,
            &agent_version,
        )
        .await
        .context("Connect failed")?;

        let run_id = connect_resp.return_value.agent_run_id.clone();

        let entity_guid = connect_resp
            .return_value
            .entity_guid
            .context("Missing entity_guid in Connect response")?;

        Ok(ApmApp {
            run_id,
            entity_guid,
            collector_host,
            license_key: license_key.to_string(),
            client: client.clone(),
        })
    }

    pub async fn process_agent_payload(&self, payload: Vec<u8>) -> Result<()> {
        debug!("Processing agent payload ({} bytes)", payload.len());

        // Parse agent payload into telemetry types
        let (telemetry_map, protocol_version) =
            parse_agent_payload(&payload).context("Failed to parse agent payload")?;

        debug!(
            "Parsed agent payload: protocol_v{}, {} telemetry types",
            protocol_version,
            telemetry_map.len()
        );

        // Send all telemetry types in parallel (like Go implementation)
        let mut send_tasks = Vec::new();

        for (telemetry_type, data) in telemetry_map {
            if data.is_empty() {
                continue;
            }

            debug!(
                "Sending {} telemetry items as {}",
                data.len(),
                telemetry_type
            );

            // Clone Arc'd data for parallel sending
            let client = self.client.clone();
            let license_key = self.license_key.clone();
            let collector_host = self.collector_host.clone();
            let run_id = self.run_id.clone();

            // Spawn parallel task for each telemetry type
            let task = tokio::spawn(async move {
                // Error events need special handling (different payload structure)
                if telemetry_type == "error_event_data" {
                    if let Err(e) = send_error_events(
                        &client,
                        &license_key,
                        &collector_host,
                        &run_id,
                        &data,
                    )
                    .await
                    {
                        warn!("Failed to send {}: {}", telemetry_type, e);
                    }
                    return;
                }

                // All other telemetry types use standard format
                let command = match telemetry_type.as_str() {
                    "metric_data" => CMD_METRICS,
                    "span_event_data" => CMD_SPAN_EVENTS,
                    "error_data" => CMD_ERROR_DATA,
                    "analytic_event_data" => CMD_ANALYTIC_EVENTS,
                    "custom_event_data" => CMD_CUSTOM_EVENTS,
                    "log_event_data" => CMD_LOG_EVENTS,
                    "transaction_sample_data" => CMD_TRANSACTION_SAMPLES,
                    _ => {
                        warn!("Unknown telemetry type: {}", telemetry_type);
                        return;
                    }
                };

                if let Err(e) = send_apm_telemetry(
                    &client,
                    &license_key,
                    &collector_host,
                    &run_id,
                    command,
                    &data,
                )
                .await
                {
                    warn!("Failed to send {}: {}", telemetry_type, e);
                }
            });

            send_tasks.push(task);
        }

        // Wait for all sends to complete
        for task in send_tasks {
            let _ = task.await;
        }

        Ok(())
    }

    /// Convert and send platform REPORT log metrics
    ///
    /// Based on metric_api.go ParseLambdaReportLog() and ConvertToMetrics()
    pub async fn send_platform_report_metrics(&self, log_line: &str) -> Result<()> {
        // Parse REPORT log
        let metrics_data = match parse_lambda_report_log(log_line) {
            Some(data) => data,
            None => {
                debug!("Not a REPORT log or parse failed");
                return Ok(());
            }
        };

        debug!(
            "Parsed REPORT log: duration={:?}ms, billed_duration={:?}ms, memory_size={:?}MB, max_memory_used={:?}MB",
            metrics_data.duration,
            metrics_data.billed_duration,
            metrics_data.memory_size,
            metrics_data.max_memory_used
        );

        // Get function name from environment
        let function_name = std::env::var("AWS_LAMBDA_FUNCTION_NAME")
            .unwrap_or_else(|_| "unknown".to_string());

        // Convert to APM metrics with entity GUID and function name
        let metrics = convert_to_apm_metrics(&metrics_data, &self.entity_guid, &function_name);

        send_platform_metrics(
            &self.client,
            &self.license_key,
            "https://metric-api.newrelic.com/metric/v1",
            metrics,
        )
        .await
    }

    pub async fn send_error_event_from_fault(
        &self,
        log_line: &str,
        request_id: &str,
        function_arn: &str,
    ) -> Result<()> {
        use super::error_event::generate_error_event_from_fault;
        
        // Generate error event from fault log
        let error_events = match generate_error_event_from_fault(log_line, request_id, function_arn) {
            Some(events) => events,
            None => {
                debug!("Not a fault/timeout log, skipping error event generation");
                return Ok(());
            }
        };

        info!(
            "Sending error event for fault/timeout in request: {}",
            request_id
        );

        // Send error event to APM collector using special error event format
        send_error_events(
            &self.client,
            &self.license_key,
            &self.collector_host,
            &self.run_id,
            &error_events,
        )
        .await
    }

    /// Get entity GUID for log correlation
    pub fn get_entity_guid(&self) -> &str {
        &self.entity_guid
    }
}

/// Shared APM app state
pub type SharedApmApp = Arc<RwLock<Option<ApmApp>>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apm_app_creation() {
        let client = Client::new();
        let app = ApmApp {
            run_id: "test_run_id".to_string(),
            entity_guid: "test_guid".to_string(),
            collector_host: "collector.newrelic.com".to_string(),
            license_key: "test_key".to_string(),
            client,
        };

        assert_eq!(app.run_id, "test_run_id");
        assert_eq!(app.entity_guid, "test_guid");
        assert_eq!(app.get_entity_guid(), "test_guid");
    }
}
