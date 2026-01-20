use crate::{
    logs::processor::LogProcessor,
    platform::processor::PlatformProcessor,
    agent::batch::{AGENT_BATCH_BUFFER, add_to_batch},
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
                                        if let Some(mut batch_item) = AGENT_BATCH_BUFFER.get_mut(request_id_str) {
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
                                                            .unwrap_or_else(|| "unknown".to_string())
                                                    })
                                                    .unwrap_or_else(|| "unknown".to_string());
                                                
                                                if let Ok(buffer_guard) = buffer.lock() {
                                                    for payload_bytes in buffer_guard.iter() {
                                                        add_to_batch(
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
                    warn!("No runtime.done channel found for request: {}", request_id);
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
