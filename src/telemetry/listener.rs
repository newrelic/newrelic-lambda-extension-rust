use crate::{
    logs::processor::LogProcessor,
    platform::processor::PlatformProcessor,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::{
    io::{Error, Result},
    net::{SocketAddr, TcpListener},
    sync::Arc,
};
use tracing::{error, info, debug};

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
) -> Result<SocketAddr> {
    let addr = "0.0.0.0:0";
    let listener = TcpListener::bind(addr).map_err(|e| Error::new(std::io::ErrorKind::AddrInUse, e))?;
    let local_addr = listener.local_addr()?;

    tokio::spawn(async move {
        let server = hyper::Server::from_tcp(listener)
            .unwrap()
            .serve(hyper::service::make_service_fn(move |_| {
                let log_processor = log_processor.clone();
                let platform_processor = platform_processor.clone();
                async move {
                    Ok::<_, hyper::Error>(hyper::service::service_fn(move |req| {
                        handle_telemetry_request(
                            req,
                            log_processor.clone(),
                            platform_processor.clone(),
                        )
                    }))
                }
            }));

        info!("Telemetry listener started on {}", local_addr);

        if let Err(e) = server.await {
            error!("Telemetry listener server error: {}", e);
        }
    });

    Ok(local_addr)
}

/// Handles an incoming HTTP request for the telemetry listener.
async fn handle_telemetry_request(
    req: hyper::Request<hyper::Body>,
    log_processor: Arc<LogProcessor>,
    platform_processor: Arc<PlatformProcessor>,
) -> std::result::Result<hyper::Response<hyper::Body>, hyper::Error> {
    let body_bytes = hyper::body::to_bytes(req.into_body()).await?;
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap_or_default();

    debug!("Received telemetry data: {} bytes", body_str.len());

    match serde_json::from_str::<Vec<TelemetryRecord>>(&body_str) {
        Ok(records) => {
            info!("Successfully parsed {} telemetry records", records.len());
            let mut function_completed = false;
            
            for record in records {
                debug!("Processing telemetry record: type={}", record.record_type);
                
                match record.record_type.as_str() {
                    "function" => {
                        debug!("Routing function log to LogProcessor");
                        log_processor.process_record(record);
                    }
                    "extension" => {
                        debug!("Routing extension log to LogProcessor");
                        log_processor.process_record(record);
                    }
                    "platform.report" | "platform.end" => {
                        debug!("Function execution completed, will flush after processing");
                        platform_processor.process_record(record);
                        function_completed = true;
                    }
                    _ => {
                        debug!("Routing platform event ({}) to PlatformProcessor", record.record_type);
                        platform_processor.process_record(record);
                    }
                }
            }
            
            // If function execution completed, send all accumulated data immediately
            if function_completed {
                info!("Function execution completed, flushing all data immediately!");
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
            debug!("Raw telemetry data that failed to parse: {}", body_str);
        }
    }

    Ok(hyper::Response::new(hyper::Body::from("OK")))
}

