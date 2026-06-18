// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    logs::processor::LogProcessor,
    platform::processor::PlatformProcessor,
    agent::batch::{AGENT_BATCH_BUFFER, add_to_batch},
    request::{
        get_agent_buffer, get_request_context, get_runtime_done_notify, set_pending_report,
        CURRENT_ACTIVE_REQUEST_ID, TELEMETRY_CURRENT_REQUEST_ID,
    },
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
                    let is_apm_mode_clone = is_apm_mode;

                    tokio::spawn(async move {
                        let io = TokioIo::new(stream);
                        let service = service_fn(move |req| {
                            handle_telemetry_request(
                                req,
                                log_processor.clone(),
                                platform_processor.clone(),
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
            let mut function_count = 0;
            let mut extension_count = 0;
            let mut platform_count = 0;

            for record in records {
                match record.record_type.as_str() {
                    "platform.start" => {
                        // Update TELEMETRY_CURRENT_REQUEST_ID - this is the SOURCE OF TRUTH
                        // for stamping function/extension logs with the correct request_id.
                        // platform.start always arrives BEFORE function logs for that request,
                        // so this ensures late logs from request_A don't get stamped with request_B.
                        if let Some(request_id_value) = record.record.get("requestId") {
                            if let Some(request_id_str) = request_id_value.as_str() {
                                if let Ok(mut telemetry_req) = TELEMETRY_CURRENT_REQUEST_ID.lock() {
                                    *telemetry_req = Some(request_id_str.to_string());
                                }
                                // LMI: INVOKE never arrives, so set the active request and buffer slot
                                // here. In normal APM mode INVOKE already did this — no-op.
                                if is_apm_mode {
                                    if let Ok(mut active) = CURRENT_ACTIVE_REQUEST_ID.lock() {
                                        *active = Some(request_id_str.to_string());
                                    }
                                    crate::request::ensure_lmi_request_slot(request_id_str);
                                }
                                debug!("platform.start: Updated telemetry request_id to: {}", request_id_str);
                            }
                        }
                        platform_processor.process_record(record);
                    }
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
                                debug!("platform.runtimeDone received for request: {}", request_id_str);
                                // Wake the event loop's end-of-invocation flush waiter.
                                // On fast functions (< ~50 ms), runtimeDone can arrive before
                                // the event loop has created per-request state — get_runtime_done_notify
                                // returns None in that case. Record it in PREFIRED_RUNTIME_DONE so
                                // create_request_processing_state() fires the notify immediately.
                                if let Some(notify) = get_runtime_done_notify(request_id_str) {
                                    notify.notify_one();
                                } else {
                                    crate::request::record_prefired_runtime_done(request_id_str);
                                }
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
                                        let current_arn = crate::get_global_fallback_arn();
                                        let send_failed = if let Some(ref app) = *apm_app_read {
                                            if let Err(e) = app.send_platform_report_metrics(&report_line, &current_arn).await {
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
                                            set_pending_report(request_id_str, report_line);
                                            debug!("APM mode: Stored failed platform.report for request: {} (will retry later)", request_id_str);
                                        }
                                        // In APM mode, platform.report and agent payloads are INDEPENDENT
                                        // Agent payloads go directly to APM collector when run_id is available
                                    } else {
                                        // STANDARD MODE: Match platform.report with agent payloads for batching
                                        // platform.report may arrive in same or next invocation (after freeze/thaw)
                                        if let Some(mut batch_item) = AGENT_BATCH_BUFFER.get_mut(request_id_str) {
                                            batch_item.report_line = Some(report_line);
                                            debug!("Serverless mode: Matched platform.report with batched agent for request: {}", request_id_str);
                                        }
                                        else if let Some(buffer) = get_agent_buffer(request_id_str) {
                                            let arn = get_request_context(request_id_str)
                                                .and_then(|ctx_ref| {
                                                    ctx_ref.lock()
                                                        .ok()
                                                        .map(|ctx| ctx.invoked_function_arn.clone())
                                                        .filter(|arn| !arn.is_empty())
                                                })
                                                .unwrap_or_else(crate::get_global_fallback_arn);
                                            let batched = if let Ok(mut buffer_guard) = buffer.lock() {
                                                if buffer_guard.is_empty() {
                                                    false
                                                } else {
                                                    debug!("Serverless mode: Found agent payload in buffer for platform.report: {} - adding to batch", request_id_str);
                                                    for payload_bytes in buffer_guard.iter() {
                                                        add_to_batch(
                                                            request_id_str.to_string(),
                                                            payload_bytes.clone(),
                                                            Some(report_line.clone()),
                                                            arn.clone(),
                                                        );
                                                    }
                                                    buffer_guard.clear();
                                                    true
                                                }
                                            } else {
                                                false
                                            };

                                            if batched {
                                                debug!("Serverless mode: Cleared agent buffer for request {} after matching with report", request_id_str);
                                            } else {
                                                set_pending_report(request_id_str, report_line);
                                                debug!("Serverless mode: Stored platform.report for request: {} (will be matched with agent payload)", request_id_str);
                                            }
                                        }
                                        else {
                                            set_pending_report(request_id_str, report_line);
                                            debug!("Serverless mode: Stored platform.report for request: {} (will be matched with agent payload)", request_id_str);
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
                    "platform.initStart" => {
                        // LMI host metadata (instanceId + instanceMaxMemory,
                        // both AWS-documented on the 2025-01-29 schema). Captured
                        // once into the global static, then read by every
                        // outbound attribute composer. See
                        // src/telemetry/managed_instance.rs.
                        if let Some(meta) =
                            crate::telemetry::managed_instance::extract_managed_instance_metadata(&record.record)
                        {
                            let mut guard =
                                crate::telemetry::managed_instance::MANAGED_INSTANCE_METADATA
                                    .write()
                                    .await;
                            debug!(
                                "Captured managed-instance metadata: instance_id={} instance_max_memory={:?}",
                                meta.instance_id, meta.instance_max_memory
                            );
                            *guard = Some(meta);
                        }
                        // Existing log-emission path stays the source of truth
                        // for surfacing platform.initStart as a log entry.
                        platform_processor.process_record(record);
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
