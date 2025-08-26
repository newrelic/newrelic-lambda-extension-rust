//! Event Bus Module
//! 
//! This module provides the event bus system for the New Relic Lambda Extension.
//! It handles telemetry events, log forwarding, and metric collection with
//! configurable endpoints and processing capabilities.

use tokio::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;

use crate::event_bus::constants::MAX_EVENTS;
use crate::telemetry::events::TelemetryEvent;
use crate::config::ExtensionConfig;

pub mod constants;
pub mod processor;
pub mod forwarder;

/// Event types that can be sent through the event bus
#[derive(Debug, Clone)]
pub enum Event {
    /// Telemetry event from AWS Lambda
    Telemetry(TelemetryEvent),
    
    /// Function log event
    FunctionLog {
        timestamp: i64,
        request_id: Option<String>,
        message: String,
        level: String,
    },
    
    /// Extension log event  
    ExtensionLog {
        timestamp: i64,
        message: String,
        level: String,
    },
    
    /// Metric event
    Metric {
        timestamp: i64,
        name: String,
        value: f64,
        tags: Vec<(String, String)>,
    },
    
    /// Out of memory error event
    OutOfMemory(i64),
    
    /// Shutdown signal
    Shutdown,
}

/// The main event bus for handling all extension events
#[allow(clippy::module_name_repetitions)]
pub struct EventBus {
    tx: Sender<Event>,
    pub rx: Receiver<Event>,
    config: Arc<ExtensionConfig>,
}

impl EventBus {
    /// Create a new event bus with the given configuration
    #[must_use]
    pub fn new(config: Arc<ExtensionConfig>) -> EventBus {
        let (tx, rx) = mpsc::channel(MAX_EVENTS);
        EventBus { tx, rx, config }
    }

    /// Get a sender copy for sending events to the bus
    #[must_use]
    pub fn get_sender(&self) -> Sender<Event> {
        self.tx.clone()
    }

    /// Get the configuration
    #[must_use]
    pub fn get_config(&self) -> Arc<ExtensionConfig> {
        Arc::clone(&self.config)
    }

    /// Send an event to the bus
    pub async fn send(&self, event: Event) -> Result<(), tokio::sync::mpsc::error::SendError<Event>> {
        self.tx.send(event).await
    }

    /// Receive the next event from the bus
    pub async fn recv(&mut self) -> Option<Event> {
        self.rx.recv().await
    }

    /// Start the event processing loop
    pub async fn run(mut self) {
        tracing::info!("🚌 [EventBus] Starting event bus with telemetry and log forwarding");
        
        let config = Arc::clone(&self.config);
        
        // Initialize processors
        let telemetry_processor = processor::TelemetryProcessor::new(Arc::clone(&config));
        let log_processor = processor::LogProcessor::new(Arc::clone(&config));
        let metric_processor = processor::MetricProcessor::new(Arc::clone(&config));
        
        // Initialize forwarders
        let newrelic_forwarder = forwarder::NewRelicForwarder::new(Arc::clone(&config));
        
        tracing::info!("📡 [EventBus] Telemetry endpoint: {}", config.new_relic.telemetry_endpoint);
        tracing::info!("📝 [EventBus] Log endpoint: {}", config.new_relic.log_endpoint);
        tracing::info!("📊 [EventBus] Metric endpoint: {}", config.new_relic.metric_endpoint);
        
        let mut event_count = 0;
        
        // Main event processing loop
        while let Some(event) = self.recv().await {
            event_count += 1;
            tracing::debug!("🔄 [EventBus] Processing event #{}: {:?}", event_count, event);
            
            match event {
                Event::Telemetry(telemetry_event) => {
                    // Process telemetry events and extract logs/metrics
                    if let Err(e) = telemetry_processor.process(telemetry_event, &newrelic_forwarder).await {
                        tracing::error!("❌ [EventBus] Failed to process telemetry event: {}", e);
                    }
                }
                
                Event::FunctionLog { timestamp, request_id, message, level } => {
                    // Process function logs if enabled
                    if config.should_process_function_logs() {
                        if let Err(e) = log_processor.process_function_log(
                            timestamp,
                            request_id,
                            message,
                            level,
                            &newrelic_forwarder
                        ).await {
                            tracing::error!("❌ [EventBus] Failed to process function log: {}", e);
                        }
                    }
                }
                
                Event::ExtensionLog { timestamp, message, level } => {
                    // Process extension logs if enabled
                    if config.should_process_extension_logs() {
                        if let Err(e) = log_processor.process_extension_log(
                            timestamp,
                            message,
                            level,
                            &newrelic_forwarder
                        ).await {
                            tracing::error!("❌ [EventBus] Failed to process extension log: {}", e);
                        }
                    }
                }
                
                Event::Metric { timestamp, name, value, tags } => {
                    // Process metrics
                    if let Err(e) = metric_processor.process(
                        timestamp,
                        name,
                        value,
                        tags,
                        &newrelic_forwarder
                    ).await {
                        tracing::error!("❌ [EventBus] Failed to process metric: {}", e);
                    }
                }
                
                Event::OutOfMemory(timestamp) => {
                    tracing::error!("💀 [EventBus] Out of memory detected at timestamp: {}", timestamp);
                    // Send this as a special metric or alert
                    if let Err(e) = metric_processor.process(
                        timestamp,
                        "lambda.oom".to_string(),
                        1.0,
                        vec![
                            ("event_type".to_string(), "out_of_memory".to_string()),
                            ("severity".to_string(), "critical".to_string()),
                        ],
                        &newrelic_forwarder
                    ).await {
                        tracing::error!("❌ [EventBus] Failed to process OOM metric: {}", e);
                    }
                }
                
                Event::Shutdown => {
                    tracing::info!("🛑 [EventBus] Shutdown signal received, stopping event bus");
                    break;
                }
            }
        }
        
        tracing::info!("👋 [EventBus] Event bus stopped after processing {} events", event_count);
    }
}
