use std::net::{SocketAddr, TcpListener};
use std::io::{Error, Result};
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::mpsc::error::TrySendError;
use tracing::{error, info, warn};
use serde::Deserialize;

// --- Telemetry API Constants ---
const TELEMETRY_PORT: u16 = 0; // Port 0 will let the OS assign a free port
const TELEMETRY_HOST_IP: &str = "0.0.0.0";

#[derive(Deserialize, Debug)]
pub struct TelemetryRecord {
    // A representation of a telemetry record
    #[serde(rename = "type")]
    record_type: String,
    record: serde_json::Value,
}


/// Starts a simple HTTP listener to receive telemetry events.
pub async fn setup_telemetry_listener() -> Result<SocketAddr> {
    let addr = format!("{}:{}", TELEMETRY_HOST_IP, TELEMETRY_PORT);
    let listener = TcpListener::bind(&addr)
        .map_err(|e| Error::new(std::io::ErrorKind::AddrInUse, e))?;
    let local_addr = listener.local_addr()?;

    tokio::spawn(async move {
        let (tx, mut rx) = mpsc::channel::<String>(100);
        let server = hyper::Server::from_tcp(listener)
            .unwrap()
            .serve(hyper::service::make_service_fn(move |_| {
                let tx = tx.clone();
                async move {
                    Ok::<_, hyper::Error>(hyper::service::service_fn(move |req| {
                        handle_telemetry_request(req, tx.clone())
                    }))
                }
            }));

        info!("Telemetry listener started on {}", local_addr);

        // This task will process the received telemetry bodies
        tokio::spawn(async move {
            while let Some(body) = rx.recv().await {
                match serde_json::from_str::<Vec<TelemetryRecord>>(&body) {
                     Ok(records) => {
                        for record in records {
                            let payload_str = serde_json::to_string(&record.record)
                                .unwrap_or_else(|_| "Failed to serialize payload".to_string());

                            match record.record_type.as_str() {
                                "platform.initStart" => info!("[Init Start] - Payload: {}", payload_str),
                                "platform.initRuntimeDone" => info!("[Init Runtime Done] - Payload: {}", payload_str),
                                "platform.initReport" => info!("[Init Report] - Payload: {}", payload_str),
                                "platform.start" => info!("[Invoke Start] - Payload: {}", payload_str),
                                "platform.runtimeDone" => info!("[Invoke Runtime Done] - Payload: {}", payload_str),
                                "platform.report" => info!("[Invoke Report] - Payload: {}", payload_str),
                                "platform.restoreStart" => info!("[Restore Start] - Payload: {}", payload_str),
                                "platform.restoreRuntimeDone" => info!("[Restore Runtime Done] - Payload: {}", payload_str),
                                "platform.restoreReport" => info!("[Restore Report] - Payload: {}", payload_str),
                                "platform.telemetrySubscription" => info!("[Telemetry Subscription] - Payload: {}", payload_str),
                                "platform.logsDropped" => warn!("[Logs Dropped] - Payload: {}", payload_str),
                                "function" => info!("[Function Log] - Payload: {}", payload_str),
                                "extension" => info!("[Extension Log] - Payload: {}", payload_str),
                                _ => warn!("[Unknown Record Type: {}] - Payload: {}", record.record_type, payload_str),
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to parse telemetry records: {}", e);
                    }
                }
            }
        });

        if let Err(e) = server.await {
            error!("Telemetry listener server error: {}", e);
        }
    });

    Ok(local_addr)
}

/// Handles an incoming HTTP request for the telemetry listener.
async fn handle_telemetry_request(
    req: hyper::Request<hyper::Body>,
    tx: Sender<String>,
) -> std::result::Result<hyper::Response<hyper::Body>, hyper::Error> {
    let body_bytes = hyper::body::to_bytes(req.into_body()).await?;
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap_or_default();

    // Use a non-blocking send to prevent deadlocks from log feedback
    if let Err(e) = tx.try_send(body_str) {
        match e {
            TrySendError::Full(_) => {
                warn!("Telemetry channel is full; logs may be dropped.");
            }
            TrySendError::Closed(_) => {
                error!("Telemetry channel has been closed.");
            }
        }
    }

    Ok(hyper::Response::new(hyper::Body::from("OK")))
}
