//! Telemetry Listener Module
//! 
//! This module handles receiving and processing telemetry events from AWS Lambda
//! Runtime API. It includes the HTTP server that receives telemetry batches and
//! processes individual telemetry records through the event bus.

use hyper::{Method, Request, Response, StatusCode, body::Incoming};
use hyper::service::service_fn as hyper_service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ServerBuilder;
use http_body_util::{BodyExt, Full};
use std::sync::Arc;
use std::net::SocketAddr;
use bytes::Bytes;
use tokio::net::TcpListener;
use tokio::sync::mpsc::Sender;

use crate::telemetry::events::TelemetryEvent;
use crate::event_bus::Event;
use chrono;

/// Legacy telemetry record structure for compatibility
#[derive(Debug, Clone, serde::Deserialize)]
pub struct TelemetryRecord {
    #[serde(rename = "type")]
    pub record_type: String,
    pub time: String,
    pub record: serde_json::Value,
}

/// Telemetry HTTP Server with event bus integration
pub struct TelemetryServer {
    event_bus_sender: Option<Sender<Event>>,
}

impl TelemetryServer {
    /// Create telemetry server with event bus integration
    pub fn with_event_bus(event_bus_sender: Sender<Event>) -> Self {
        Self {
            event_bus_sender: Some(event_bus_sender),
        }
    }

    /// Health check endpoint to verify server is ready
    async fn handle_health_check() -> Result<Response<Full<Bytes>>, Box<dyn std::error::Error + Send + Sync>> {
        let response = Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(r#"{"status":"healthy","ready":true}"#)))?;
        Ok(response)
    }

    /// Handle incoming telemetry HTTP requests
    async fn handle_telemetry(
        &self,
        req: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, Box<dyn std::error::Error + Send + Sync>> {
        match (req.method(), req.uri().path()) {
            (&Method::POST, "/telemetry") => {
                // Collect the request body
                let body_bytes = req.into_body().collect().await?.to_bytes();
                
                tracing::info!("📨 [TelemetryServer] Received telemetry batch: {} bytes", body_bytes.len());
                
                // Parse telemetry records
                match self.process_telemetry_batch(&body_bytes).await {
                    Ok(count) => {
                        tracing::info!("✅ [TelemetryServer] Successfully processed {} telemetry records", count);
                        let response = Response::builder()
                            .status(StatusCode::OK)
                            .header("Content-Type", "application/json")
                            .body(Full::new(Bytes::from(r#"{"status":"ok"}"#)))?;
                        Ok(response)
                    }
                    Err(e) => {
                        tracing::error!("❌ [TelemetryServer] Error processing telemetry: {}", e);
                        let response = Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .header("Content-Type", "application/json")
                            .body(Full::new(Bytes::from(format!(r#"{{"error":"{}"}}"#, e))))?;
                        Ok(response)
                    }
                }
            }
            (&Method::GET, "/health") => {
                // Health check endpoint
                Self::handle_health_check().await
            }
            _ => {
                // Return 404 for other paths
                let response = Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Full::new(Bytes::from("Not Found")))?;
                Ok(response)
            }
        }
    }

    /// Process a batch of telemetry records
    async fn process_telemetry_batch(
        &self,
        body_bytes: &Bytes,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        // Parse the telemetry batch
        let body_str = std::str::from_utf8(body_bytes)?;
        
        // Log raw telemetry data for debugging
        tracing::debug!("📋 [TelemetryServer] Raw telemetry payload: {}", body_str);

        // Try to parse as array of telemetry events using the new event structure
        let events: Result<Vec<TelemetryEvent>, _> = serde_json::from_str(body_str);
        
        match events {
            Ok(telemetry_events) => {
                let record_count = telemetry_events.len();
                tracing::info!("🔄 [TelemetryServer] Processing batch of {} telemetry events", record_count);

                // Send each telemetry event to the event bus
                for (index, event) in telemetry_events.into_iter().enumerate() {
                    tracing::debug!("📝 [TelemetryServer] Processing event {}/{}: {:?}", index + 1, record_count, event.record);
                    
                    if let Some(ref sender) = self.event_bus_sender {
                        // Send telemetry event to event bus for New Relic forwarding
                        if let Err(e) = sender.send(Event::Telemetry(event.clone())).await {
                            tracing::error!("❌ [TelemetryServer] Failed to send event to event bus: {}", e);
                        } else {
                            tracing::debug!("✅ [TelemetryServer] Successfully sent event to event bus");
                        }
                    } else {
                        // Fallback to direct processing if no event bus
                        self.process_single_event_direct(event).await?;
                    }
                }

                Ok(record_count)
            }
            Err(parse_error) => {
                // Fallback to legacy record format for compatibility
                tracing::debug!("🔄 [TelemetryServer] Falling back to legacy record format due to: {}", parse_error);
                let legacy_records: Vec<TelemetryRecord> = serde_json::from_str(body_str)?;
                let record_count = legacy_records.len();
                
                tracing::info!("🔄 [TelemetryServer] Processing batch of {} legacy telemetry records", record_count);

                for (index, record) in legacy_records.into_iter().enumerate() {
                    tracing::debug!("📝 [TelemetryServer] Processing legacy record {}/{}: {}", index + 1, record_count, record.record_type);
                    
                    // Convert legacy record to telemetry event and send to event bus
                    if let Some(ref sender) = self.event_bus_sender {
                        if let Ok(telemetry_event) = self.convert_legacy_to_event(&record) {
                            if let Err(e) = sender.send(Event::Telemetry(telemetry_event)).await {
                                tracing::error!("❌ [TelemetryServer] Failed to send converted event to event bus: {}", e);
                            } else {
                                tracing::debug!("✅ [TelemetryServer] Successfully sent converted event to event bus");
                            }
                        } else {
                            tracing::warn!("⚠️ [TelemetryServer] Failed to convert legacy record to telemetry event");
                        }
                    } else {
                        self.process_legacy_record(record).await?;
                    }
                }

                Ok(record_count)
            }
        }
    }

    /// Process a single telemetry event directly (when no event bus is available)
    async fn process_single_event_direct(
        &self,
        event: TelemetryEvent,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("🎯 [TelemetryServer] Processing telemetry event at {}", event.time);
        tracing::debug!("   📊 Event Data: {:?}", event.record);

        // Here you would implement direct New Relic forwarding
        // For now, just log the event
        Ok(())
    }

    /// Process legacy telemetry record format
    async fn process_legacy_record(
        &self,
        record: TelemetryRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("🎯 [TelemetryServer] Processing legacy {} record at {}", record.record_type, record.time);

        // Here you would convert legacy format to new format and process
        // For now, just log detailed information as before
        match record.record_type.as_str() {
            "platform.initStart" => {
                tracing::info!("🔧 [Lambda Platform] Initialization started");
                tracing::debug!("   📊 Initialization Start Data: {}", serde_json::to_string_pretty(&record.record)?);
            }
            "platform.initRuntimeDone" => {
                tracing::info!("✅ [Lambda Platform] Initialization finished");
                tracing::debug!("   📊 Initialization Runtime Done Data: {}", serde_json::to_string_pretty(&record.record)?);
            }
            "platform.initReport" => {
                tracing::info!("📄 [Lambda Platform] Initialization report");
                tracing::debug!("   📊 Initialization Report Data: {}", serde_json::to_string_pretty(&record.record)?);
            }
            "platform.start" => {
                tracing::info!("🚀 [Lambda Platform] Function execution started");
                tracing::debug!("   📊 Platform Start Data: {}", serde_json::to_string_pretty(&record.record)?);
            }
            "platform.runtimeDone" => {
                tracing::info!("🏁 [Lambda Platform] Runtime finished");
                tracing::debug!("   📊 Platform Runtime Done Data: {}", serde_json::to_string_pretty(&record.record)?);
            }
            "platform.end" => {
                tracing::info!("🏁 [Lambda Platform] Function execution ended");
                tracing::debug!("   📊 Platform End Data: {}", serde_json::to_string_pretty(&record.record)?);
            }
            "platform.report" => {
                tracing::info!("📈 [Lambda Platform] Execution report generated");
                tracing::debug!("   📊 Platform Report Data: {}", serde_json::to_string_pretty(&record.record)?);
            }
            "platform.restoreStart" => {
                tracing::info!("⏳ [Lambda Platform] Restore started");
                tracing::debug!("   📊 Restore Start Data: {}", serde_json::to_string_pretty(&record.record)?);
            }
            "platform.restoreRuntimeDone" => {
                tracing::info!("✅ [Lambda Platform] Restore finished");
                tracing::debug!("   📊 Restore Runtime Done Data: {}", serde_json::to_string_pretty(&record.record)?);
            }
            "platform.restoreReport" => {
                tracing::info!("📄 [Lambda Platform] Restore report");
                tracing::debug!("   📊 Restore Report Data: {}", serde_json::to_string_pretty(&record.record)?);
            }
            "platform.telemetrySubscription" => {
                tracing::info!("🔔 [Lambda Platform] Telemetry subscription");
                tracing::debug!("   📊 Telemetry Subscription Data: {}", serde_json::to_string_pretty(&record.record)?);
            }
            "platform.logsDropped" => {
                tracing::warn!("🗑️ [Lambda Platform] Logs dropped");
                tracing::debug!("   📊 Logs Dropped Data: {}", serde_json::to_string_pretty(&record.record)?);
            }
            "function" => {
                tracing::info!("📝 [Function Log] Application log message");
                tracing::debug!("   🔍 Function Log Data: {}", serde_json::to_string_pretty(&record.record)?);
            }
            "extension" => {
                tracing::info!("🔧 [Extension Log] Extension system log");
                tracing::debug!("   🔍 Extension Log Data: {}", serde_json::to_string_pretty(&record.record)?);
            }
            _ => {
                tracing::warn!("❓ [Unknown] Unrecognized telemetry record type: {}", record.record_type);
                tracing::debug!("   🔍 Unknown Record Data: {}", serde_json::to_string_pretty(&record.record)?);
            }
        }

        Ok(())
    }

    /// Start the telemetry HTTP server
    pub async fn start_server(
        self: Arc<Self>,
        addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(addr).await?;
        tracing::info!("🌐 [TelemetryServer] Telemetry HTTP server listening on {}", addr);
        tracing::info!("📡 [TelemetryServer] Ready to receive telemetry events from AWS Lambda Runtime API");

        loop {
            let (stream, remote_addr) = listener.accept().await?;
            let server_clone = Arc::clone(&self);

            tracing::debug!("🔗 [TelemetryServer] Accepted connection from {}", remote_addr);

            // Spawn a task to handle each connection
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = hyper_service_fn(move |req| {
                    let server = Arc::clone(&server_clone);
                    async move {
                        server.handle_telemetry(req).await.map_err(|e| {
                            tracing::error!("❌ [TelemetryServer] Request handling error: {}", e);
                            format!("Internal server error: {}", e)
                        })
                    }
                });

                if let Err(err) = ServerBuilder::new(TokioExecutor::new())
                    .serve_connection(io, service)
                    .await
                {
                    tracing::error!("💥 [TelemetryServer] Connection error: {}", err);
                }
            });
        }
    }

    /// Convert legacy telemetry record to new telemetry event format
    fn convert_legacy_to_event(&self, record: &TelemetryRecord) -> Result<TelemetryEvent, Box<dyn std::error::Error + Send + Sync>> {
        // Parse the time string to DateTime<Utc>
        let time = chrono::DateTime::parse_from_rfc3339(&record.time)?.with_timezone(&chrono::Utc);
        
        // Create a basic telemetry event with the raw data
        // For now, we'll convert it to a Function record with the raw JSON
        let telemetry_record = crate::telemetry::events::TelemetryRecord::Function(record.record.clone());
        
        Ok(TelemetryEvent {
            time,
            record: telemetry_record,
        })
    }
}
