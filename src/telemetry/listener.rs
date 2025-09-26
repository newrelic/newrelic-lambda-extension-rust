use crate::{
    logs::processor::LogProcessor,
    platform::processor::PlatformProcessor,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    io::{Error, Result},
    net::SocketAddr,
    sync::Arc,
    convert::Infallible,
};
use tracing::{debug, error, info, trace, warn};
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



    match serde_json::from_str::<Vec<TelemetryRecord>>(&body_str) {
        Ok(records) => {

            
            let mut function_completed = false;
            let mut runtime_done_received = false;
            let mut function_count = 0;
            let mut extension_count = 0;
            let mut platform_count = 0;
            
            for record in records {
                match record.record_type.as_str() {
                    "function" => {
                        function_count += 1;
                        log_processor.process_record(record);
                    }
                    "extension" => {
                        extension_count += 1;
                        log_processor.process_record(record);
                    }
                    "platform.runtimeDone" => {

                        platform_processor.process_record(record);
                        runtime_done_received = true;
                        function_completed = true;
                    }
                    "platform.report" | "platform.end" => {

                        platform_processor.process_record(record);
                        function_completed = true;
                    }
                    _ => {
                        platform_count += 1;
                        platform_processor.process_record(record);
                    }
                }
            }
            
            // Summary logging instead of per-record logging
            if function_count > 0 || extension_count > 0 || platform_count > 0 {
                trace!("Processed records - function: {}, extension: {}, platform: {}", 
                       function_count, extension_count, platform_count);
            }
            
            // If runtime is done, signal the main loop to process agent telemetry
            if runtime_done_received {
                if let Some(ref tx) = runtime_done_tx {
                    if let Err(e) = tx.send(()) {
                        warn!("Failed to send runtime done signal: {}", e);
                    } else {

                    }
                }
            }
            
            // If function execution completed, send all accumulated data immediately
            if function_completed {

                let log_proc_clone = Arc::clone(&log_processor);
                let platform_proc_clone = Arc::clone(&platform_processor);
                
                tokio::spawn(async move {
                    if let Err(e) = log_proc_clone.send_and_clear_batch_simple().await {
                        error!("Failed to send logs after function completion: {}", e);
                    }
                    if let Err(e) = platform_proc_clone.send_and_clear_batch_simple().await {
                        error!("Failed to send platform events after function completion: {}", e);
                    }
                });
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

