//! Main APM app orchestrator
//!
//! Based on internal_app.go NewApp(), connectRoutine(), doHarvest()

use super::collector::{
    send_apm_telemetry, send_error_events, send_platform_metrics,
    resolve_collector_command, CMD_ERROR_EVENTS,
};
use super::connection::{connect, preconnect};
use super::metric_converter::{convert_to_apm_metrics, parse_lambda_report_log};
use super::payload_parser::parse_agent_payload;
use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

#[derive(Debug)]
pub struct ApmApp {
    pub run_id: String,
    pub entity_guid: String,
    pub collector_host: String,
    pub license_key: String,
    pub metric_endpoint: String,
    pub client: Client,
}

impl ApmApp {
    pub async fn new(
        license_key: String,
        apm_host: String,
        metric_endpoint: String,
        client: Client,
        function_name: String,
        function_version: String,
        account_id: Option<String>,
        region: Option<String>,
    ) -> Result<Self> {
        debug!("Initializing APM app connection");

        let backoff_ms = [200, 500, 900];
        let mut last_error = None;

        for (attempt, delay) in backoff_ms.iter().enumerate() {
            debug!("APM connection attempt {} of {}", attempt + 1, 3);

            match Self::try_connect(
                &license_key,
                &apm_host,
                &metric_endpoint,
                &client,
                &function_name,
                &function_version,
                &account_id,
                &region,
            )
            .await
            {
                Ok(app) => {
                    debug!(
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
        metric_endpoint: &str,
        client: &Client,
        function_name: &str,
        function_version: &str,
        account_id_opt: &Option<String>,
        region_opt: &Option<String>,
    ) -> Result<ApmApp> {
        // OPTIMIZATION: Runtime and agent version are now cached (detected once per container)
        // No need for spawn_blocking or parallelization - instant access
        let version_info = crate::version::VersionInfo::get_or_detect(None);
        
        let runtime = if let Some(agent_name) = &version_info.agent_name {
            match agent_name.as_str() {
                "Node" => "nodejs".to_string(),
                "Python" => "python".to_string(),
                "Ruby" => "ruby".to_string(),
                "Dotnet" => "dotnet".to_string(),
                _ => agent_name.to_lowercase(),
            }
        } else {
            let detected_runtime = crate::version::get_runtime_name();
            if detected_runtime == "unknown" {
                "go".to_string()
            } else {
                detected_runtime.to_string()
            }
        };
        
        // Pass "unknown" if no agent detected - will be filtered out from labels
        let agent_version = version_info.agent_version.as_deref().unwrap_or("unknown");

        // Run preconnect while we have the cached values
        let collector_host = preconnect(client, license_key, apm_host)
            .await
            .context("PreConnect failed")?;

        debug!("PreConnect returned collector host: {}", collector_host);

        // Use provided config data instead of environment variables
        // Environment variables like AWS_LAMBDA_FUNCTION_ARN are not available during INIT
        let region = region_opt
            .clone()
            .or_else(|| std::env::var("AWS_REGION").ok())
            .unwrap_or_else(|| "us-east-1".to_string());

        let account_id = account_id_opt
            .clone()
            .unwrap_or_else(|| {
                warn!("Account ID not available from registration, using placeholder. Transactions may not appear in APM.");
                "000000000000".to_string()
            });

        // Construct ARN using the correct account_id from registration
        let function_arn = format!(
            "arn:aws:lambda:{}:{}:function:{}",
            region, account_id, function_name
        );

        debug!(
            "Connecting to APM with function_name={}, account_id={}, region={}",
            function_name, account_id, region
        );

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
            metric_endpoint: metric_endpoint.to_string(),
            client: client.clone(),
        })
    }

    pub async fn process_agent_payload(&self, payload: Vec<u8>, request_id: &str) -> Result<()> {
        debug!("Processing agent payload ({} bytes) for request {}", payload.len(), request_id);

        let (mut telemetry_map, protocol_version) =
            parse_agent_payload(&payload).context("Failed to parse agent payload")?;

        debug!(
            "Parsed agent payload: protocol_v{}, {} telemetry types",
            protocol_version,
            telemetry_map.len()
        );

        // Normalize transaction names for Ruby v2 payloads only
        // Ruby agent sends transaction names without proper "OtherTransaction/Ruby/" prefix
        if protocol_version == 2 {
            let runtime = crate::version::get_runtime_name();
            if runtime == "ruby" {
                debug!("Ruby v2 payload detected - normalizing transaction names");
                
                if let Some(data) = telemetry_map.get_mut("analytic_event_data") {
                    normalize_analytic_event_data(data);
                }
                
                if let Some(data) = telemetry_map.get_mut("span_event_data") {
                    normalize_span_event_data(data);
                }
                
                if let Some(data) = telemetry_map.get_mut("metric_data") {
                    normalize_metric_data(data);
                }
                
                // Normalize error events - they may contain transaction names
                if let Some(data) = telemetry_map.get_mut("error_event_data") {
                    normalize_error_event_data(data);
                }
                
                // Normalize custom events - they may contain transaction names
                if let Some(data) = telemetry_map.get_mut("custom_event_data") {
                    normalize_custom_event_data(data);
                }
                
                // Normalize transaction samples - they contain transaction names
                if let Some(data) = telemetry_map.get_mut("transaction_sample_data") {
                    normalize_transaction_sample_data(data);
                }
            }
        }

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

            let client = self.client.clone();
            let license_key = self.license_key.clone();
            let collector_host = self.collector_host.clone();
            let run_id = self.run_id.clone();
            let request_id_owned = request_id.to_string();

            let task = tokio::spawn(async move {
                let request_id = request_id_owned;
                let send_result = if telemetry_type == CMD_ERROR_EVENTS {
                    send_error_events(
                        &client,
                        &license_key,
                        &collector_host,
                        &run_id,
                        &data,
                    )
                    .await
                } else if let Some(command) = resolve_collector_command(&telemetry_type) {
                    send_apm_telemetry(
                        &client,
                        &license_key,
                        &collector_host,
                        &run_id,
                        command,
                        &data,
                    )
                    .await
                } else {
                    warn!("Unknown telemetry type: {}", telemetry_type);
                    return;
                };

                if let Err(e) = send_result {
                    warn!("Failed to send {} for request {}: {} - buffering for retry", telemetry_type, request_id, e);
                    super::telemetry_buffer::buffer_failed_telemetry(
                        telemetry_type.clone(),
                        data,
                        request_id,
                        run_id,
                        collector_host,
                    );
                }
            });

            send_tasks.push(task);
        }

        for task in send_tasks {
            let _ = task.await;
        }

        Ok(())
    }

    /// Convert and send platform REPORT log metrics
    ///
    /// Based on metric_api.go ParseLambdaReportLog() and ConvertToMetrics()
    pub async fn send_platform_report_metrics(&self, log_line: &str) -> Result<()> {
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

        let function_name = std::env::var("AWS_LAMBDA_FUNCTION_NAME")
            .unwrap_or_else(|_| "unknown".to_string());

        let metrics = convert_to_apm_metrics(&metrics_data, &self.entity_guid, &function_name);
        
        debug!("APM: Sending {} platform metrics to Metric API", metrics.len());

        send_platform_metrics(
            &self.client,
            &self.license_key,
            &self.metric_endpoint,
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
        
        let error_events = match generate_error_event_from_fault(log_line, request_id, function_arn) {
            Some(events) => events,
            None => {
                debug!("Not a fault/timeout log, skipping error event generation");
                return Ok(());
            }
        };

        debug!(
            "Sending error event for fault/timeout in request: {}",
            request_id
        );

        send_error_events(
            &self.client,
            &self.license_key,
            &self.collector_host,
            &self.run_id,
            &error_events,
        )
        .await
    }

    /// Send error event for shutdown events (timeout, failure)
    /// Used when Lambda shuts down due to timeout or platform fault
    pub async fn send_shutdown_error_event(
        &self,
        error_class: &str,
        error_message: &str,
        request_id: &str,
        function_arn: &str,
    ) -> Result<()> {
        use super::error_event::generate_error_event;

        let error_events = generate_error_event(error_class, error_message, request_id, function_arn);

        if error_events.is_empty() {
            return Ok(());
        }

        debug!(
            "Sending shutdown error event ({}) for request: {}",
            error_class, request_id
        );

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

/// Check if transaction name needs normalization (doesn't contain '/')
pub(crate) fn needs_normalization(name: &str) -> bool {
    !name.contains('/')
}

/// Normalize transaction name by prepending "OtherTransaction/Ruby/"
pub(crate) fn normalize_transaction_name(original: &str) -> String {
    format!("OtherTransaction/Ruby/{}", original)
}

/// Normalize transaction names in analytic_event_data
/// Structure: [run_id, {metadata}, [[[event_obj, {}, {}]], ...]]
pub(crate) fn normalize_analytic_event_data(data: &mut Vec<Value>) {
    if data.len() < 3 {
        return;
    }
    
    let events_array = match data[2].as_array_mut() {
        Some(arr) => arr,
        None => return,
    };
    
    for event_tuple in events_array.iter_mut() {
        let event_array = match event_tuple.as_array_mut() {
            Some(arr) if !arr.is_empty() => arr,
            _ => continue,
        };
        
        let event_obj = match event_array[0].as_object_mut() {
            Some(obj) => obj,
            None => continue,
        };
        
        let is_transaction = event_obj
            .get("type")
            .and_then(|v| v.as_str())
            .map(|t| t == "Transaction")
            .unwrap_or(false);
        
        if !is_transaction {
            continue;
        }
        
        // Check and normalize the transaction name
        if let Some(name_value) = event_obj.get("name") {
            if let Some(name) = name_value.as_str() {
                if needs_normalization(name) {
                    debug!("Normalizing transaction name: '{}'", name);
                    let normalized = normalize_transaction_name(name);
                    debug!("Normalized analytic_event name: '{}' -> '{}'", name, normalized);
                    event_obj.insert("name".to_string(), Value::String(normalized));
                }
            }
        }
    }
}

/// Normalize transaction names in span_event_data
/// Structure: [run_id, {metadata}, [[[span_obj, {}, {}]], ...]]
pub(crate) fn normalize_span_event_data(data: &mut Vec<Value>) {
    // Check we have the expected structure: data[2] should be the spans array
    if data.len() < 3 {
        return;
    }
    
    let spans_array = match data[2].as_array_mut() {
        Some(arr) => arr,
        None => return,
    };
    
    // Iterate through all spans
    for span_tuple in spans_array.iter_mut() {
        let span_array = match span_tuple.as_array_mut() {
            Some(arr) if !arr.is_empty() => arr,
            _ => continue,
        };
        
        let span_obj = match span_array[0].as_object_mut() {
            Some(obj) => obj,
            None => continue,
        };
        
        let is_span = span_obj
            .get("type")
            .and_then(|v| v.as_str())
            .map(|t| t == "Span")
            .unwrap_or(false);
        
        if !is_span {
            continue;
        }
        
        // Normalize the span name if needed
        if let Some(name_value) = span_obj.get("name") {
            if let Some(name) = name_value.as_str() {
                if needs_normalization(name) {
                    debug!("Normalizing span name: '{}'", name);
                    let normalized = normalize_transaction_name(name);
                    debug!("Normalized span name: '{}' -> '{}'", name, normalized);
                    span_obj.insert("name".to_string(), Value::String(normalized.clone()));
                    
                    // Also update transaction.name field if it exists
                    if span_obj.contains_key("transaction.name") {
                        span_obj.insert("transaction.name".to_string(), Value::String(normalized));
                    }
                }
            }
        }
    }
}

/// Normalize transaction names in metric_data
/// Structure: [run_id, timestamp_start, timestamp_end, [[[{name: "..."}, [values]]], ...]]
pub(crate) fn normalize_metric_data(data: &mut Vec<Value>) {
    // Check we have the expected structure: data[3] should be the metrics array
    if data.len() < 4 {
        return;
    }
    
    let metrics_array = match data[3].as_array_mut() {
        Some(arr) => arr,
        None => return,
    };
    
    // Iterate through all metrics
    for metric_tuple in metrics_array.iter_mut() {
        let metric_array = match metric_tuple.as_array_mut() {
            Some(arr) if !arr.is_empty() => arr,
            _ => continue,
        };
        
        let metric_obj = match metric_array[0].as_object_mut() {
            Some(obj) => obj,
            None => continue,
        };
        
        // Get the metric name
        let name = match metric_obj.get("name").and_then(|v| v.as_str()) {
            Some(n) => n,
            None => continue,
        };
        
        // Only normalize metrics that reference transaction names
        // These typically start with "OtherTransaction" or similar prefixes
        if name.starts_with("OtherTransaction") {
            // Use first '/' to split prefix from the rest. If the rest has no further '/',
            // it's a bare function name that needs normalization. If it already contains '/',
            // it's already structured (e.g. "OtherTransactionTotalTime/Ruby/ruby-hw").
            if let Some(first_slash_pos) = name.find('/') {
                let prefix = &name[..first_slash_pos];
                let suffix = &name[first_slash_pos + 1..];

                if needs_normalization(suffix) {
                    debug!("Normalizing metric name: '{}'", name);
                    let normalized = format!("{}/Ruby/{}", prefix, suffix);
                    debug!("Normalized metric name: '{}' -> '{}'", name, normalized);
                    metric_obj.insert("name".to_string(), Value::String(normalized));
                }
            }
        } else if needs_normalization(name) {
            // Handle standalone metrics that are just the function name
            // Example: "ruby-hw-x86-hw" should become "OtherTransaction/Ruby/ruby-hw-x86-hw"
            debug!("Normalizing standalone metric name: '{}'", name);
            let normalized = normalize_transaction_name(name);
            metric_obj.insert("name".to_string(), Value::String(normalized));
        }
    }
}

/// Normalize transaction names in error_event_data
/// Structure: [run_id, {metadata}, [[[error_obj, {}, {}]], ...]]
pub(crate) fn normalize_error_event_data(data: &mut Vec<Value>) {
    if data.len() < 3 {
        return;
    }
    
    let events_array = match data[2].as_array_mut() {
        Some(arr) => arr,
        None => return,
    };
    
    for event_tuple in events_array.iter_mut() {
        let event_array = match event_tuple.as_array_mut() {
            Some(arr) if !arr.is_empty() => arr,
            _ => continue,
        };
        
        let event_obj = match event_array[0].as_object_mut() {
            Some(obj) => obj,
            None => continue,
        };
        
        // Error events may have transaction.name field
        if let Some(name_value) = event_obj.get("transaction.name") {
            if let Some(name) = name_value.as_str() {
                if needs_normalization(name) {
                    debug!("Normalizing error event transaction.name: '{}'", name);
                    let normalized = normalize_transaction_name(name);
                    event_obj.insert("transaction.name".to_string(), Value::String(normalized));
                }
            }
        }
        
        // Error events may also have transactionName field (alternative naming)
        if let Some(name_value) = event_obj.get("transactionName") {
            if let Some(name) = name_value.as_str() {
                if needs_normalization(name) {
                    debug!("Normalizing error event transactionName: '{}'", name);
                    let normalized = normalize_transaction_name(name);
                    event_obj.insert("transactionName".to_string(), Value::String(normalized));
                }
            }
        }
    }
}

/// Normalize transaction names in custom_event_data
/// Structure: [run_id, {metadata}, [[[event_obj, {}, {}]], ...]]
pub(crate) fn normalize_custom_event_data(data: &mut Vec<Value>) {
    if data.len() < 3 {
        return;
    }
    
    let events_array = match data[2].as_array_mut() {
        Some(arr) => arr,
        None => return,
    };
    
    for event_tuple in events_array.iter_mut() {
        let event_array = match event_tuple.as_array_mut() {
            Some(arr) if !arr.is_empty() => arr,
            _ => continue,
        };
        
        let event_obj = match event_array[0].as_object_mut() {
            Some(obj) => obj,
            None => continue,
        };
        
        // Custom events may have transaction.name field
        if let Some(name_value) = event_obj.get("transaction.name") {
            if let Some(name) = name_value.as_str() {
                if needs_normalization(name) {
                    debug!("Normalizing custom event transaction.name: '{}'", name);
                    let normalized = normalize_transaction_name(name);
                    event_obj.insert("transaction.name".to_string(), Value::String(normalized));
                }
            }
        }
    }
}

/// Normalize transaction names in transaction_sample_data
/// Structure: [run_id, [[transaction_id, timestamp, name, duration, encoded_data], ...]]
pub(crate) fn normalize_transaction_sample_data(data: &mut Vec<Value>) {
    if data.len() < 2 {
        return;
    }
    
    let samples_array = match data[1].as_array_mut() {
        Some(arr) => arr,
        None => return,
    };
    
    for sample in samples_array.iter_mut() {
        let sample_array = match sample.as_array_mut() {
            Some(arr) if arr.len() >= 3 => arr,
            _ => continue,
        };
        
        // Transaction sample format: [transaction_id, timestamp, name, duration, encoded_data]
        // Index 2 is the transaction name
        if let Some(name_value) = sample_array.get(2) {
            if let Some(name) = name_value.as_str() {
                if needs_normalization(name) {
                    debug!("Normalizing transaction sample name: '{}'", name);
                    let normalized = normalize_transaction_name(name);
                    sample_array[2] = Value::String(normalized);
                }
            }
        }
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
            metric_endpoint: "https://metric-api.newrelic.com/metric/v1".to_string(),
            client,
        };

        assert_eq!(app.run_id, "test_run_id");
        assert_eq!(app.entity_guid, "test_guid");
        assert_eq!(app.get_entity_guid(), "test_guid");
    }
}
