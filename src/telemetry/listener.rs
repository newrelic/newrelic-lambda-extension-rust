use crate::{
    logs::processor::LogProcessor,
    platform::processor::PlatformProcessor,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::{
    io::{Error, Result},
    net::SocketAddr,
    sync::Arc,
    convert::Infallible,
};
use tracing::{error, info};
use hyper::{Request, Response, StatusCode};
use hyper::body::{Incoming, Bytes};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use http_body_util::{BodyExt, Full};
use tokio::net::TcpListener;

#[derive(Deserialize, Debug, Clone)]
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
                    
                    tokio::spawn(async move {
                        let io = TokioIo::new(stream);
                        let service = service_fn(move |req| {
                            handle_telemetry_request(
                                req,
                                log_processor.clone(),
                                platform_processor.clone(),
                                runtime_done_tx_clone.clone(),
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

    info!("Telemetry listener started on {}", local_addr);
    Ok(local_addr)
}

/// Handles an incoming HTTP request for the telemetry listener.
async fn handle_telemetry_request(
    req: Request<Incoming>,
    log_processor: Arc<LogProcessor>,
    platform_processor: Arc<PlatformProcessor>,
    runtime_done_tx: Option<tokio::sync::mpsc::UnboundedSender<()>>,
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

    tracing::debug!("Received telemetry data: {} bytes", body_str.len());

    match serde_json::from_str::<Vec<TelemetryRecord>>(&body_str) {
        Ok(records) => {
            tracing::info!("Successfully parsed {} telemetry records", records.len());
            let mut function_completed = false;
            let mut runtime_done_received = false;
            
            for record in records {
                tracing::debug!("Processing telemetry record: type={}", record.record_type);
                match record.record_type.as_str() {
                    "function" => {
                        tracing::debug!("Routing function log to LogProcessor");
                        log_processor.process_record(record);
                    }
                    "extension" => {
                        tracing::debug!("Routing extension log to LogProcessor");
                        log_processor.process_record(record);
                    }
                    "platform.runtimeDone" => {
                        tracing::info!("🎯 Runtime completed (platform.runtimeDone), signaling agent telemetry processing");
                        platform_processor.process_record(record);
                        runtime_done_received = true;
                        function_completed = true;
                    }
                    "platform.report" | "platform.end" => {
                        tracing::info!("📋 Function execution completed ({}), will flush after processing", record.record_type);
                        platform_processor.process_record(record);
                        function_completed = true;
                    }
                    _ => {
                        tracing::debug!("Routing platform event ({}) to PlatformProcessor", record.record_type);
                        platform_processor.process_record(record);
                    }
                }
            }
            
            // If runtime is done, signal the main loop to process agent telemetry
            if runtime_done_received {
                if let Some(ref tx) = runtime_done_tx {
                    if let Err(e) = tx.send(()) {
                        tracing::warn!("Failed to send runtime done signal: {}", e);
                    } else {
                        tracing::info!("🚀 Sent runtime done signal to main loop for agent telemetry processing");
                    }
                }
            }
            
            // If function execution completed, send all accumulated data immediately
            if function_completed {
                tracing::info!("🚀 Function execution completed, flushing all data immediately!");
                let log_proc_clone = Arc::clone(&log_processor);
                let platform_proc_clone = Arc::clone(&platform_processor);
                
                tokio::spawn(async move {
                    if let Err(e) = log_proc_clone.send_and_clear_batch_simple().await {
                        tracing::error!("Failed to send logs after function completion: {}", e);
                    }
                    if let Err(e) = platform_proc_clone.send_and_clear_batch_simple().await {
                        tracing::error!("Failed to send platform events after function completion: {}", e);
                    }
                });
            }
        }
        Err(e) => {
            error!("Failed to parse telemetry records: {}", e);
            tracing::debug!("Raw telemetry data that failed to parse: {}", body_str);
        }
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::from("OK")))
        .unwrap())
}

