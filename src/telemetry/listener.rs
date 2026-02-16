use crate::{
    logs::processor::LogProcessor,
    platform::processor::PlatformProcessor,
    agent::batch::DEFAULT_BATCH_BUFFER,
    request::{REQUEST_AGENT_BUFFERS, REQUEST_CONTEXTS, PENDING_REPORTS, RUNTIME_DONE_CHANNELS},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    io::{Error, Result},
    net::SocketAddr,
    sync::Arc,
    convert::Infallible,
};
use tracing::{debug, error, trace, warn};
use hyper::{Request, Response, StatusCode};
use hyper::body::{Incoming, Bytes};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use http_body_util::{BodyExt, Full};
use tokio::net::TcpListener;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TelemetryRecord {
    pub time: DateTime<Utc>,
    #[serde(rename = "type")]
    pub record_type: String,
    pub record: serde_json::Value,
}

/// Starts a simple HTTP listener to receive telemetry events.
pub async fn setup_telemetry_listener(
    log_processor: Arc<LogProcessor>,
    platform_processor: Arc<PlatformProcessor>,
    runtime_done_tx: Option<tokio::sync::mpsc::UnboundedSender<()>>,
    is_apm_mode: bool,
) -> Result<SocketAddr> {
    let addr = "0.0.0.0:0";
    let listener = TcpListener::bind(addr).await.map_err(|e| Error::new(std::io::ErrorKind::AddrInUse, e))?;
    let local_addr = listener.local_addr()?;

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let log_processor = log_processor.clone();
                    let platform_processor = platform_processor.clone();
                    let runtime_done_tx_clone = runtime_done_tx.clone();
                    let is_apm_mode_clone = is_apm_mode;
                    
                    tokio::spawn(async move {
                        let io = TokioIo::new(stream);
                        let service = service_fn(move |req| {
                            handle_telemetry_request(
                                req,
                                log_processor.clone(),
                                platform_processor.clone(),
                                runtime_done_tx_clone.clone(),
                                is_apm_mode_clone,
                            )
                        });
                        
                        if let Err(e) = hyper::server::conn::http1::Builder::new()
                            .serve_connection(io, service)
                            .await 
                        {
                            error!("Error serving connection: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    });

    debug!("Telemetry listener started on {}", local_addr);
    Ok(local_addr)
}

/// Handles an incoming HTTP request for the telemetry listener.
async fn handle_telemetry_request(
    req: Request<Incoming>,
    log_processor: Arc<LogProcessor>,
    platform_processor: Arc<PlatformProcessor>,
    runtime_done_tx: Option<tokio::sync::mpsc::UnboundedSender<()>>,
    is_apm_mode: bool,
) -> std::result::Result<Response<Full<Bytes>>, Infallible> {
    let body_bytes = match req.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from("Failed to read body")))
                .unwrap())
        }
    };
    
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap_or_default();

    match serde_json::from_str::<Vec<TelemetryRecord>>(&body_str) {
        Ok(records) => {

            
            let mut function_completed = false;
            let mut runtime_done_request_id: Option<String> = None;
            let mut function_count = 0;
            let mut extension_count = 0;
            let mut platform_count = 0;

            for record in records {
                match record.record_type.as_str() {
                    "function" => {
                        function_count += 1;
                        log_processor.process_record(record).await;
                    }
                    "extension" => {
                        extension_count += 1;
                        log_processor.process_record(record).await;
                    }
                    "platform.runtimeDone" => {
                        if let Some(request_id_value) = record.record.get("requestId") {
                            if let Some(request_id_str) = request_id_value.as_str() {
                                runtime_done_request_id = Some(request_id_str.to_string());
                                debug!("platform.runtimeDone received for request: {}", request_id_str);
                            }
                        }
                        platform_processor.process_record(record);
                        function_completed = true;
                    }
                    "platform.report" => {
                        if let Some(request_id_value) = record.record.get("requestId") {
                            if let Some(request_id_str) = request_id_value.as_str() {
                                if let Some(report_line) = platform_processor.convert_platform_report_to_log_line(&record) {
                                    
                                    if is_apm_mode {
                                        // APM MODE: Send platform.report as metrics immediately, NO matching with agent payloads
                                        let apm_app_read = crate::APM_APP.read().await;
                                        let send_failed = if let Some(ref app) = *apm_app_read {
                                            if let Err(e) = app.send_platform_report_metrics(&report_line).await {
                                                warn!("APM mode: Failed to send platform.report metrics for {}: {} - will retry", request_id_str, e);
                                                true
                                            } else {
                                                debug!("APM mode: Sent platform.report metrics for request: {}", request_id_str);
                                                false
                                            }
                                        } else {
                                            warn!("APM mode: APM app not ready - storing report for retry");
                                            true
                                        };

                                        if send_failed {
                                            PENDING_REPORTS.insert(request_id_str.to_string(), report_line);
                                            debug!("APM mode: Stored failed platform.report for request: {} (will retry later)", request_id_str);
                                        }
                                        // In APM mode, platform.report and agent payloads are INDEPENDENT
                                        // Agent payloads go directly to APM collector when run_id is available
                                    } else {
                                        // STANDARD MODE: Match platform.report with agent payloads for batching
                                        if let Some(mut batch_item) = DEFAULT_BATCH_BUFFER.buffer.get_mut(request_id_str) {
                                            batch_item.report_line = Some(report_line);
                                            debug!("Standard mode: Matched platform.report with batched agent for request: {}", request_id_str);
                                        }
                                        else if let Some(buffer) = REQUEST_AGENT_BUFFERS.get(request_id_str) {
                                            let has_agent = buffer.lock().ok().map(|b| !b.is_empty()).unwrap_or(false);
                                            if has_agent {
                                                debug!("Standard mode: Found agent payload in buffer for platform.report: {} - adding to batch", request_id_str);
                                                
                                                let arn = REQUEST_CONTEXTS.get(request_id_str)
                                                    .map(|ctx_ref| {
                                                        ctx_ref.lock()
                                                            .ok()
                                                            .map(|ctx| ctx.invoked_function_arn.clone())
                                                            .unwrap_or_else(|| {
                                                                // Fallback to global context ARN (set from registration)
                                                                if let Ok(global_ctx) = crate::CURRENT_INVOCATION_CONTEXT.read() {
                                                                    global_ctx.invoked_function_arn.clone()
                                                                } else {
                                                                    String::new()
                                                                }
                                                            })
                                                    })
                                                    .unwrap_or_else(|| {
                                                        // Fallback to global context ARN (set from registration)
                                                        if let Ok(global_ctx) = crate::CURRENT_INVOCATION_CONTEXT.read() {
                                                            global_ctx.invoked_function_arn.clone()
                                                        } else {
                                                            String::new()
                                                        }
                                                    });
                                                
                                                if let Ok(buffer_guard) = buffer.lock() {
                                                    for payload_bytes in buffer_guard.iter() {
                                                        DEFAULT_BATCH_BUFFER.add_to_batch(
                                                            request_id_str.to_string(),
                                                            payload_bytes.clone(),
                                                            Some(report_line.clone()),
                                                            arn.clone(),
                                                        );
                                                    }
                                                }
                                                
                                                drop(buffer);
                                                REQUEST_AGENT_BUFFERS.remove(request_id_str);
                                                debug!("Standard mode: Cleared agent buffer for request {} after matching with report", request_id_str);
                                            } else {
                                                PENDING_REPORTS.insert(request_id_str.to_string(), report_line);
                                                debug!("Standard mode: Stored platform.report for request: {} (will be matched with agent payload)", request_id_str);
                                            }
                                        }
                                        else {
                                            PENDING_REPORTS.insert(request_id_str.to_string(), report_line);
                                            debug!("Standard mode: Stored platform.report for request: {} (will be matched with agent payload)", request_id_str);
                                        }
                                    }
                                }
                            }
                        }
                        platform_processor.process_record(record);
                        function_completed = true;
                    }
                    "platform.end" => {
                        platform_processor.process_record(record);
                        function_completed = true;
                    }
                    _ => {
                        platform_count += 1;
                        platform_processor.process_record(record);
                    }
                }
            }
            
            if function_count > 0 || extension_count > 0 || platform_count > 0 {
                let is_cold_start = !crate::IS_WARM_START.load(std::sync::atomic::Ordering::Relaxed);
                if is_cold_start && function_count > 0 {
                    debug!("COLD START: Successfully received {} function logs via telemetry API!", function_count);
                }
               
            } else {
                debug!("No telemetry records processed in this batch");
            }
            
            if let Some(request_id) = runtime_done_request_id {
                if let Some(tx) = RUNTIME_DONE_CHANNELS.get(&request_id) {
                    if let Err(e) = tx.send(()) {
                        warn!("Failed to send runtime.done signal for request {}: {}", request_id, e);
                    } else {
                        debug!("Successfully sent runtime.done signal for request: {}", request_id);
                    }
                } else {
                    // Only warn in standard mode - APM mode doesn't use runtime.done channels
                    if !is_apm_mode {
                        debug!("No runtime.done channel found for request: {} (channel may have been cleaned up)", request_id);
                    }
                }

                if let Some(ref tx) = runtime_done_tx {
                    let _ = tx.send(());
                }
            }
            
            if function_completed {
                trace!("Function execution completed - telemetry accumulated for main loop flush");
            }
        }
        Err(e) => {
            error!("Failed to parse telemetry records: {}", e);

        }
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::from("OK")))
        .unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_telemetry_record_deserialize_function_type() {
        let json = r#"{
            "time": "2024-01-15T12:34:56.789Z",
            "type": "function",
            "record": {"message": "hello world"}
        }"#;
        let record: TelemetryRecord =
            serde_json::from_str(json).expect("should deserialize function record");
        assert_eq!(record.record_type, "function");
        assert!(record.record.is_object());
    }

    #[test]
    fn test_telemetry_record_deserialize_platform_report() {
        let json = r#"{
            "time": "2024-01-15T12:34:56.789Z",
            "type": "platform.report",
            "record": {"requestId": "abc-123", "metrics": {"durationMs": 100.5}}
        }"#;
        let record: TelemetryRecord =
            serde_json::from_str(json).expect("should deserialize platform.report record");
        assert_eq!(record.record_type, "platform.report");
        assert_eq!(
            record.record.get("requestId").and_then(|v| v.as_str()),
            Some("abc-123")
        );
    }

    #[test]
    fn test_telemetry_record_deserialize_platform_runtime_done() {
        let json = r#"{
            "time": "2024-01-15T12:34:56.789Z",
            "type": "platform.runtimeDone",
            "record": {"requestId": "req-456", "status": "success"}
        }"#;
        let record: TelemetryRecord =
            serde_json::from_str(json).expect("should deserialize platform.runtimeDone record");
        assert_eq!(record.record_type, "platform.runtimeDone");
        assert_eq!(
            record.record.get("requestId").and_then(|v| v.as_str()),
            Some("req-456")
        );
    }

    #[test]
    fn test_telemetry_record_deserialize_extension_type() {
        let json = r#"{
            "time": "2024-01-15T12:34:56.000Z",
            "type": "extension",
            "record": {"message": "extension log"}
        }"#;
        let record: TelemetryRecord =
            serde_json::from_str(json).expect("should deserialize extension record");
        assert_eq!(record.record_type, "extension");
    }

    #[test]
    fn test_telemetry_record_deserialize_array() {
        let json = r#"[
            {"time": "2024-01-15T12:34:56.000Z", "type": "function", "record": {}},
            {"time": "2024-01-15T12:34:57.000Z", "type": "extension", "record": {}}
        ]"#;
        let records: Vec<TelemetryRecord> =
            serde_json::from_str(json).expect("should deserialize array of records");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].record_type, "function");
        assert_eq!(records[1].record_type, "extension");
    }

    #[test]
    fn test_telemetry_record_time_parsing() {
        let json = r#"{
            "time": "2024-06-15T10:30:00.123Z",
            "type": "function",
            "record": {}
        }"#;
        let record: TelemetryRecord =
            serde_json::from_str(json).expect("should parse ISO 8601 time");
        assert_eq!(record.time.year(), 2024);
        assert_eq!(record.time.month(), 6);
        assert_eq!(record.time.day(), 15);
    }

    // ========================================================================
    // handle_telemetry_request — via real HTTP to telemetry listener
    // ========================================================================

    #[tokio::test]
    async fn test_telemetry_listener_accepts_function_logs() {
        let config = std::sync::Arc::new(crate::config::ExtensionConfig::default());
        let client = std::sync::Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = std::sync::Arc::new(tokio::sync::RwLock::new(None));
        let factory = crate::request::ProcessorFactory::new(client, config.clone(), apm_app);
        let ctx = std::sync::Arc::new(std::sync::Mutex::new(crate::context::InvocationContext::default()));
        let log_processor = factory.create_log_processor(ctx.clone());
        let platform_processor = factory.create_platform_processor(ctx, log_processor.clone());

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            None,
            false,
        )
        .await
        .expect("should start listener");

        // Send a function log via HTTP
        let http_client = reqwest::Client::new();
        let body = serde_json::json!([
            {
                "time": "2024-01-15T12:00:00.000Z",
                "type": "function",
                "record": {"message": "hello from function"}
            }
        ]);

        let resp = http_client
            .post(format!("http://127.0.0.1:{}/telemetry", addr.port()))
            .json(&body)
            .send()
            .await
            .expect("should send request");

        assert_eq!(resp.status(), 200, "Listener should return 200");
    }

    #[tokio::test]
    async fn test_telemetry_listener_accepts_mixed_records() {
        let config = std::sync::Arc::new(crate::config::ExtensionConfig::default());
        let client = std::sync::Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = std::sync::Arc::new(tokio::sync::RwLock::new(None));
        let factory = crate::request::ProcessorFactory::new(client, config.clone(), apm_app);
        let ctx = std::sync::Arc::new(std::sync::Mutex::new(crate::context::InvocationContext::default()));
        let log_processor = factory.create_log_processor(ctx.clone());
        let platform_processor = factory.create_platform_processor(ctx, log_processor.clone());

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            None,
            false,
        )
        .await
        .expect("should start listener");

        let http_client = reqwest::Client::new();
        let body = serde_json::json!([
            {"time": "2024-01-15T12:00:00.000Z", "type": "function", "record": {"message": "fn log"}},
            {"time": "2024-01-15T12:00:01.000Z", "type": "extension", "record": {"message": "ext log"}},
            {"time": "2024-01-15T12:00:02.000Z", "type": "platform.start", "record": {"requestId": "req-1"}}
        ]);

        let resp = http_client
            .post(format!("http://127.0.0.1:{}/telemetry", addr.port()))
            .json(&body)
            .send()
            .await
            .expect("should send request");

        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn test_telemetry_listener_handles_malformed_json() {
        let config = std::sync::Arc::new(crate::config::ExtensionConfig::default());
        let client = std::sync::Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = std::sync::Arc::new(tokio::sync::RwLock::new(None));
        let factory = crate::request::ProcessorFactory::new(client, config.clone(), apm_app);
        let ctx = std::sync::Arc::new(std::sync::Mutex::new(crate::context::InvocationContext::default()));
        let log_processor = factory.create_log_processor(ctx.clone());
        let platform_processor = factory.create_platform_processor(ctx, log_processor.clone());

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            None,
            false,
        )
        .await
        .expect("should start listener");

        let http_client = reqwest::Client::new();
        let resp = http_client
            .post(format!("http://127.0.0.1:{}/telemetry", addr.port()))
            .body("this is not valid json at all")
            .header("Content-Type", "application/json")
            .send()
            .await
            .expect("should send request");

        // Should still return 200 (errors are logged, not propagated)
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn test_telemetry_listener_handles_empty_array() {
        let config = std::sync::Arc::new(crate::config::ExtensionConfig::default());
        let client = std::sync::Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = std::sync::Arc::new(tokio::sync::RwLock::new(None));
        let factory = crate::request::ProcessorFactory::new(client, config.clone(), apm_app);
        let ctx = std::sync::Arc::new(std::sync::Mutex::new(crate::context::InvocationContext::default()));
        let log_processor = factory.create_log_processor(ctx.clone());
        let platform_processor = factory.create_platform_processor(ctx, log_processor.clone());

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            None,
            false,
        )
        .await
        .expect("should start listener");

        let http_client = reqwest::Client::new();
        let resp = http_client
            .post(format!("http://127.0.0.1:{}/telemetry", addr.port()))
            .json(&serde_json::json!([]))
            .send()
            .await
            .expect("should send request");

        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn test_telemetry_listener_runtime_done_sends_signal() {
        let config = std::sync::Arc::new(crate::config::ExtensionConfig::default());
        let client = std::sync::Arc::new(crate::newrelic::client::NewRelicClient::new_noop());
        let apm_app = std::sync::Arc::new(tokio::sync::RwLock::new(None));
        let factory = crate::request::ProcessorFactory::new(client, config.clone(), apm_app);
        let ctx = std::sync::Arc::new(std::sync::Mutex::new(crate::context::InvocationContext::default()));
        let log_processor = factory.create_log_processor(ctx.clone());
        let platform_processor = factory.create_platform_processor(ctx, log_processor.clone());

        let (runtime_done_tx, mut runtime_done_rx) = tokio::sync::mpsc::unbounded_channel();

        let addr = setup_telemetry_listener(
            log_processor,
            platform_processor,
            Some(runtime_done_tx),
            false,
        )
        .await
        .expect("should start listener");

        let http_client = reqwest::Client::new();
        let body = serde_json::json!([
            {
                "time": "2024-01-15T12:00:00.000Z",
                "type": "platform.runtimeDone",
                "record": {"requestId": "req-signal-test", "status": "success"}
            }
        ]);

        let resp = http_client
            .post(format!("http://127.0.0.1:{}/telemetry", addr.port()))
            .json(&body)
            .send()
            .await
            .expect("should send request");

        assert_eq!(resp.status(), 200);

        // The runtime_done_tx should have received a signal
        let signal = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            runtime_done_rx.recv(),
        )
        .await;

        assert!(signal.is_ok(), "Should have received runtime_done signal within 500ms");
    }
}
