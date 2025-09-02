use std::{
    io::{Error, Result},
    net::{SocketAddr, TcpListener},
    sync::Arc,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use tracing::{error, info};
use crate::{
    logs::processor::LogProcessor,
    platform::processor::PlatformProcessor,
};

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
    let listener = TcpListener::bind(addr)
        .map_err(|e| Error::new(std::io::ErrorKind::AddrInUse, e))?;
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

    match serde_json::from_str::<Vec<TelemetryRecord>>(&body_str) {
        Ok(records) => {
            for record in records {
                match record.record_type.as_str() {
                    "function" | "extension" => {
                        log_processor.process_record(record);
                        // After processing a log, check if a batch is ready to be harvested
                        platform_processor.harvest();
                    }
                    _ => {
                        platform_processor.process_record(record);
                    }
                }
            }
        }
        Err(e) => {
            error!("Failed to parse telemetry records: {}", e);
        }
    }

    Ok(hyper::Response::new(hyper::Body::from("OK")))
}

