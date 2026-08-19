// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Main APM app orchestrator
//!
//! Based on internal_app.go NewApp(), connectRoutine(), doHarvest()

use super::collector::{
    send_apm_telemetry, send_error_events, send_platform_metrics, CMD_ANALYTIC_EVENTS,
    CMD_CUSTOM_EVENTS, CMD_ERROR_DATA, CMD_ERROR_EVENTS, CMD_LOG_EVENTS, CMD_METRICS, CMD_SLOW_SQLS,
    CMD_SPAN_EVENTS, CMD_TRANSACTION_SAMPLES,
};
use super::connection::{
    connect, is_handshake_fatal, is_permanent_auth_error, last_failure_reason, preconnect,
    record_connect_attempt, record_connect_cycle, reset_connect_stats, signal_handshake_fatal,
};
use super::metric_converter::{convert_to_apm_metrics, parse_lambda_report_log};
use super::payload_parser::parse_agent_payload;
use crate::config::deployment::DeploymentContext;
use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

#[derive(Debug)]
pub struct ApmApp {
    pub run_id: String,
    pub entity_guid: String,
    pub app_name: String,
    pub collector_host: String,
    pub license_key: String,
    pub metric_endpoint: String,
    pub client: Client,
    /// Deployment context (detected once at startup). Drives type-driven LMI vs Normal
    /// behavior in this app — e.g. strict vs lenient `platform.report` parsing.
    pub deployment: DeploymentContext,
}

impl ApmApp {
    pub async fn new(
        license_key: String,
        apm_host: String,
        metric_endpoint: String,
        client: Client,
        function_name: String,
        lambda_function_name: String,
        function_version: String,
        account_id: Option<String>,
        region: Option<String>,
        timeout_secs: u64,
        deployment: DeploymentContext,
    ) -> Result<Self> {
        debug!("Initializing APM app connection");

        // If a prior handshake was permanently rejected (bad license key / no
        // permission), don't keep hammering the collector for this container's life.
        if is_handshake_fatal() {
            return Err(anyhow::anyhow!(
                "APM handshake permanently disabled for this container (auth previously rejected)"
            ));
        }

        // Count this reconnect cycle (one per new(): startup / per-invoke / shutdown).
        record_connect_cycle();

        let backoff_ms = [200, 500, 900];
        let total_attempts = backoff_ms.len();
        let mut last_error = None;

        for (attempt, delay) in backoff_ms.iter().enumerate() {
            debug!("APM connection attempt {} of {}", attempt + 1, total_attempts);

            match Self::try_connect(
                &license_key,
                &apm_host,
                &metric_endpoint,
                &client,
                &function_name,
                &lambda_function_name,
                &function_version,
                &account_id,
                &region,
                timeout_secs,
                deployment,
            )
            .await
            {
                Ok(app) => {
                    debug!(
                        "APM connection successful: run_id={}, entity_guid={}",
                        app.run_id, app.entity_guid
                    );
                    // Connected — clear the disconnected-streak diagnostics.
                    reset_connect_stats();
                    return Ok(app);
                }
                Err(e) => {
                    record_connect_attempt();
                    // Reason captured at the failure site — the collector's actual
                    // response (e.g. "HTTP 401: {body}") or the network cause.
                    let reason = last_failure_reason().unwrap_or_else(|| format!("{e:#}"));

                    // Permanent auth failure (401/403) is the ONLY case we stop on:
                    // latch APM off so the per-invoke loop won't keep retrying.
                    // Every other failure (timeout, 5xx, connection error) retries.
                    if is_permanent_auth_error(&e).is_some() {
                        error!(
                            "APM handshake rejected ({}) - disabling APM connection attempts for this container.",
                            reason
                        );
                        signal_handshake_fatal();
                        return Err(e);
                    }

                    // Transient failure: this is a retry — warn with the attempt count
                    // and the actual reason (incl. HTTP code) so it's clearly a retry.
                    warn!(
                        "APM handshake attempt {}/{} failed ({}) - retrying",
                        attempt + 1,
                        total_attempts,
                        reason
                    );
                    last_error = Some(e);

                    if attempt < total_attempts - 1 {
                        debug!("Retrying in {}ms", delay);
                        tokio::time::sleep(tokio::time::Duration::from_millis(*delay)).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Failed to connect to APM collector")))
    }

    /// Attempt a single connection
    #[allow(clippy::too_many_arguments)]
    async fn try_connect(
        license_key: &str,
        apm_host: &str,
        metric_endpoint: &str,
        client: &Client,
        function_name: &str,
        lambda_function_name: &str,
        function_version: &str,
        account_id_opt: &Option<String>,
        region_opt: &Option<String>,
        timeout_secs: u64,
        deployment: DeploymentContext,
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
        
        // Pass "unknown" if no-agent detected - will be filtered out from labels
        let agent_version = version_info.agent_version.as_deref().unwrap_or("unknown");

        // Run preconnect while we have the cached values
        let collector_host = preconnect(client, license_key, apm_host, timeout_secs)
            .await
            .context("PreConnect failed")?;

        debug!("PreConnect returned collector host: {}", collector_host);

        // When the user explicitly set a non-default host (e.g. staging-collector.newrelic.com),
        // honor that override for connect instead of following the preconnect redirect.
        // Staging preconnect redirects to prod collector; following it silently breaks staging tests.
        let connect_host = if apm_host != "collector.newrelic.com" {
            debug!(
                "Honoring explicit apm_host ({}) for connect, ignoring preconnect redirect to {}",
                apm_host, collector_host
            );
            apm_host.to_string()
        } else {
            collector_host
        };

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

        // Construct ARN using actual Lambda function name, not the app name override
        let function_arn = format!(
            "arn:aws:lambda:{}:{}:function:{}",
            region, account_id, lambda_function_name
        );

        debug!(
            "Connecting to APM with function_name={}, account_id={}, region={}",
            function_name, account_id, region
        );

        let lmi_metadata = crate::telemetry::managed_instance::try_read_metadata();

        let connect_resp = connect(
            client,
            license_key,
            &connect_host,
            &function_name,
            &function_arn,
            &account_id,
            &region,
            &function_version,
            &runtime,
            &agent_version,
            timeout_secs,
            lmi_metadata,
            deployment,
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
            app_name: function_name.to_string(),
            collector_host: connect_host,
            license_key: license_key.to_string(),
            metric_endpoint: metric_endpoint.to_string(),
            client: client.clone(),
            deployment,
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

        apply_custom_tag_injection(&mut telemetry_map);

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

        for (telemetry_type, mut data) in telemetry_map {
            if data.is_empty() {
                continue;
            }

            // Customer opted out of this telemetry type — drop it (no send, no buffer).
            if super::collector::is_telemetry_disabled(&telemetry_type) {
                debug!("Telemetry type {} disabled - skipping", telemetry_type);
                continue;
            }

            // metric_data[1] and [2] are the harvest epoch window (start, end).
            // The Java agent serverless mode writes these in milliseconds; the APM
            // collector protocol expects seconds. Values above this threshold are
            // unambiguously milliseconds (the year ~33658 as seconds — no real epoch
            // will exceed this in seconds for centuries).
            const JAVA_AGENT_MS_EPOCH_THRESHOLD: f64 = 1_000_000_000_000.0;
            if telemetry_type == "metric_data" && data.len() >= 3 {
                for i in 1..=2usize {
                    if let Some(ts) = data[i].as_f64() {
                        if ts > JAVA_AGENT_MS_EPOCH_THRESHOLD {
                            data[i] = serde_json::Value::from((ts / 1000.0) as i64);
                        }
                    }
                }
                debug!(
                    "metric_data epoch range after normalisation: [{}, {}]",
                    data[1], data[2]
                );
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
                let command = match telemetry_type.as_str() {
                    "metric_data" => CMD_METRICS,
                    "span_event_data" => CMD_SPAN_EVENTS,
                    "error_data" => CMD_ERROR_DATA,
                    "error_event_data" => CMD_ERROR_EVENTS,
                    "analytic_event_data" => CMD_ANALYTIC_EVENTS,
                    "custom_event_data" => CMD_CUSTOM_EVENTS,
                    "log_event_data" => CMD_LOG_EVENTS,
                    "transaction_sample_data" => CMD_TRANSACTION_SAMPLES,
                    "sql_trace_data" => CMD_SLOW_SQLS,
                    _ => {
                        warn!("Unknown telemetry type: {}", telemetry_type);
                        return;
                    }
                };
                let send_result = send_apm_telemetry(
                    &client,
                    &license_key,
                    &collector_host,
                    &run_id,
                    command,
                    &data,
                )
                .await;

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
    pub async fn send_platform_report_metrics(
        &self,
        log_line: &str,
        function_arn: &str,
    ) -> Result<()> {
        // Customer disabled platform metrics (NEW_RELIC_APM_DISABLE_TELEMETRY contains
        // platform_metrics): skip conversion and the Metric API send entirely.
        // Error-synthesis memory capture is a separate path and is unaffected.
        if super::collector::is_telemetry_disabled("platform_metrics") {
            debug!("APM platform metrics disabled - skipping REPORT conversion/send");
            return Ok(());
        }

        let metrics_data = match parse_lambda_report_log(log_line, self.deployment) {
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

        let function_name = std::env::var("NEW_RELIC_APP_NAME")
            .unwrap_or_else(|_| std::env::var("AWS_LAMBDA_FUNCTION_NAME")
                .unwrap_or_else(|_| "unknown".to_string()));

        let metrics = convert_to_apm_metrics(&metrics_data, &self.entity_guid, &function_name, function_arn);
        
        debug!("APM: Sending {} platform metrics to Metric API", metrics.len());

        match send_platform_metrics(
            &self.client,
            &self.license_key,
            &self.metric_endpoint,
            &metrics,
        )
        .await
        {
            Ok(()) => Ok(()),
            // Permanent failures (non-retryable 4xx) are dropped at the send site — nothing to retry.
            Err(e) if e.is_permanent() => {
                error!("Platform metrics dropped (permanent): {}", e);
                Ok(())
            }
            // Transient/network failures: buffer for retry on a later invoke / at shutdown.
            Err(e) => {
                let retry_after = e.retry_after();
                super::metric_api_buffer::buffer_failed_metric_api(
                    metrics,
                    self.metric_endpoint.clone(),
                    retry_after,
                );
                Ok(())
            }
        }
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

        self.send_error_events_buffered(error_events, request_id).await
    }

    /// Send synthesized error events, buffering them for retry on failure so a
    /// transient collector error or stale run_id does not silently drop them.
    async fn send_error_events_buffered(
        &self,
        error_events: Vec<serde_json::Value>,
        request_id: &str,
    ) -> Result<()> {
        // Customer opted out of error events — drop synthesized timeout/fault errors too.
        if super::collector::is_telemetry_disabled("error_event_data") {
            debug!("error_event_data disabled - skipping synthesized error event");
            return Ok(());
        }

        let result = send_error_events(
            &self.client,
            &self.license_key,
            &self.collector_host,
            &self.run_id,
            &error_events,
        )
        .await;

        if let Err(e) = result {
            warn!(
                "Failed to send error events for request {}: {} - buffering for retry",
                request_id, e
            );
            super::telemetry_buffer::buffer_failed_telemetry(
                super::telemetry_buffer::SYNTHESIZED_ERROR_EVENTS.to_string(),
                error_events,
                request_id.to_string(),
                self.run_id.clone(),
                self.collector_host.clone(),
            );
        }
        Ok(())
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

        self.send_error_events_buffered(error_events, request_id).await
    }

    /// Get entity GUID for log correlation
    pub fn get_entity_guid(&self) -> &str {
        &self.entity_guid
    }

    /// Get the app name sent to Connect, for `entity.name` log correlation
    pub fn get_app_name(&self) -> &str {
        &self.app_name
    }
}

/// Applies `inject_custom_tag_attributes` to `analytic_event_data` (Transaction events)
/// and `span_event_data` (Span events), if present. Split out of `process_agent_payload`
/// to keep that function under the line-count lint, not because it's reused elsewhere.
fn apply_custom_tag_injection(telemetry_map: &mut std::collections::HashMap<String, Vec<Value>>) {
    let tags = get_custom_tag_attributes();
    if let Some(data) = telemetry_map.get_mut("analytic_event_data") {
        inject_custom_tag_attributes(data, "Transaction", tags);
    }
    if let Some(data) = telemetry_map.get_mut("span_event_data") {
        inject_custom_tag_attributes(data, "Span", tags);
    }
}

/// `NEW_RELIC_LABELS` attribute set for Transaction/Span injection (NR-600651), built
/// once and reused for every invocation - "only check once", not re-parsed or rebuilt
/// per payload.
///
/// `NR_TAGS` is deliberately excluded here - it stays exactly as it's always been
/// (connect payload Entity Tags + log-forwarding attributes only, both unchanged). This
/// is a hard constraint carried over from earlier in this feature's development, not an
/// oversight: `NR_TAGS`'s behavior must never gain scope beyond what it already does.
///
/// Keys are `tags.`-prefixed (e.g. `tags.team`), not raw (`team`) - a deliberate choice
/// for uniformity with the log-forwarding attributes (`client.rs::get_or_build_common_json`),
/// which are `tags.`-prefixed too. Entity Tags (`connection.rs::get_labels`) stay
/// unprefixed regardless, since that's a structured `label_type`/`label_value` field in
/// the connect payload, not a flat attribute map a prefix could apply to.
static CUSTOM_TAG_ATTRIBUTES: OnceLock<serde_json::Map<String, Value>> = OnceLock::new();

fn get_custom_tag_attributes() -> &'static serde_json::Map<String, Value> {
    CUSTOM_TAG_ATTRIBUTES.get_or_init(|| {
        let mut map = serde_json::Map::new();
        for (k, v) in crate::config::get_new_relic_labels() {
            map.insert(format!("tags.{k}"), Value::String(v.clone()));
        }
        map
    })
}

/// Inject `tags` (already `tags.`-prefixed by the caller, see `get_custom_tag_attributes`)
/// as custom attributes into every event of the given intrinsic `type` ("Transaction" or
/// "Span") in an `analytic_event_data`/`span_event_data` payload. This function itself is
/// prefix-agnostic - it inserts whatever keys `tags` contains verbatim - so its tests can
/// exercise plain keys without needing to know about the prefixing convention.
/// Structure: `data[2]` is the events array; each event is `[intrinsics, user_attrs,
/// agent_attrs]` (the standard NR agent event triple). `user_attrs` is the same slot a
/// customer's own `newrelic.addCustomAttributes()` call would populate, which is why an
/// attribute the agent already set there is never overwritten (agent wins on collision).
/// Takes `tags` as a parameter (rather than reading the cache directly) so it stays
/// testable without needing the process-wide `OnceLock` cache - see `get_nr_tags`/
/// `get_new_relic_labels` for why that cache can't be exercised more than once per
/// test binary.
pub(crate) fn inject_custom_tag_attributes(
    data: &mut [Value],
    event_type: &str,
    tags: &serde_json::Map<String, Value>,
) {
    if tags.is_empty() {
        return; // zero overhead when neither env var is set
    }
    if data.len() < 3 {
        return;
    }
    let Some(events_array) = data[2].as_array_mut() else {
        return;
    };

    for event_tuple in events_array.iter_mut() {
        let Some(fields) = event_tuple.as_array_mut() else {
            continue;
        };
        if fields.len() < 2 {
            continue;
        }

        let is_match = fields[0]
            .as_object()
            .and_then(|o| o.get("type"))
            .and_then(|v| v.as_str())
            .is_some_and(|t| t == event_type);
        if !is_match {
            continue;
        }

        if let Some(user_attrs) = fields[1].as_object_mut() {
            for (k, v) in tags {
                user_attrs.entry(k.clone()).or_insert_with(|| v.clone());
            }
        } else {
            let mut user_attrs = serde_json::Map::new();
            for (k, v) in tags {
                user_attrs.insert(k.clone(), v.clone());
            }
            fields[1] = Value::Object(user_attrs);
        }
    }
}

/// Check if transaction name needs normalization (doesn't contain '/')
fn needs_normalization(name: &str) -> bool {
    !name.contains('/')
}

/// Normalize transaction name by prepending "OtherTransaction/Ruby/"
fn normalize_transaction_name(original: &str) -> String {
    format!("OtherTransaction/Ruby/{}", original)
}

/// Normalize transaction names in analytic_event_data
/// Structure: [run_id, {metadata}, [[[event_obj, {}, {}]], ...]]
fn normalize_analytic_event_data(data: &mut Vec<Value>) {
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
fn normalize_span_event_data(data: &mut Vec<Value>) {
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
            // Check if the last segment needs normalization
            // Example: "OtherTransactionTotalTime/ruby-hw" should become
            //          "OtherTransactionTotalTime/Ruby/ruby-hw"
            if let Some(last_slash_pos) = name.rfind('/') {
                let prefix = &name[..last_slash_pos];
                let suffix = &name[last_slash_pos + 1..];
                
                // If suffix has no '/', it needs normalization
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
#[path = "app_tests.rs"]
mod tests;
