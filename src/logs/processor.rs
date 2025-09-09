use crate::{
    config::ExtensionConfig,
    context::InvocationContext,
    newrelic::{client::NewRelicClient, flush::Flush, payload},
    telemetry::listener::TelemetryRecord,
};
use async_trait::async_trait;
use std::{
    io::Result,
    sync::{Arc, Mutex},
};
use tracing::{info, debug, error, warn};

/// The LogProcessor is responsible for handling and transforming function and extension logs.
#[derive(Debug, Clone)]
pub struct LogProcessor {
    log_batch: Arc<Mutex<Vec<payload::LogMessage>>>,
    newrelic_client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
    invocation_context: Arc<Mutex<InvocationContext>>,
}

impl LogProcessor {
    /// Creates a new LogProcessor.
    pub fn new(
        newrelic_client: Arc<NewRelicClient>,
        config: Arc<ExtensionConfig>,
        invocation_context: Arc<Mutex<InvocationContext>>,
    ) -> Self {
        Self {
            log_batch: Arc::new(Mutex::new(Vec::new())),
            newrelic_client,
            config,
            invocation_context,
        }
    }



    /// Processes a single log telemetry record, adding it to the batch if valid.
    pub fn process_record(&self, record: TelemetryRecord) {
        let message_str = record.record.to_string();
        
        // Avoid recursive logging from our own processors
        if message_str.contains("[LogProcessor]") || message_str.contains("[PlatformProcessor]") || message_str.contains("[AgentPayloadProcessor]") {
            return;
        }



        debug!("Processing log record: type={}, time={}, message_preview={}...", 
            record.record_type, 
            record.time,
            if message_str.len() > 100 { &message_str[..100] } else { &message_str }
        );

        if let Some(log_message) = self.to_log_message(record) {
            let mut batch = self.log_batch.lock().unwrap();
            batch.push(log_message);
            let batch_size = batch.len();
            debug!("Added log to batch. Current batch size: {}", batch_size);

            // Send immediately if we have 3+ logs (simple batch condition)
            if batch_size >= 3 {
                drop(batch); // Release the lock before async operation
                //info!("Batch size reached, sending logs immediately!");
                let processor = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = processor.send_and_clear_batch_simple().await {
                        error!("Failed to send logs: {}", e);
                    }
                });
            }
        } else {
            warn!("Failed to convert telemetry record to log message");
        }
    }

    /// Cleans log message by removing ANSI escape sequences and formatting (optimized for performance)
    fn clean_log_message(&self, raw_message: &str) -> String {
        // Fast path: check if we need to do any cleaning at all
        if !raw_message.contains("\\u001b[") && !raw_message.contains('\x1b') {
            // No ANSI sequences, just do basic parsing
            return self.parse_clean_message(raw_message);
        }
        
        // Only do expensive cleaning if needed
        let cleaned = self.remove_ansi_sequences(raw_message);
        self.parse_clean_message(&cleaned)
    }
    
    /// Fast ANSI sequence removal (only called when needed)
    fn remove_ansi_sequences(&self, message: &str) -> String {
        let mut result = String::with_capacity(message.len());
        let mut chars = message.chars();
        
        while let Some(ch) = chars.next() {
            if ch == '\\' {
                // Handle literal \u001b sequences
                if let Some('u') = chars.next() {
                    if chars.as_str().starts_with("001b[") {
                        // Skip the ANSI sequence
                        chars.nth(4); // skip "001b["
                        while let Some(c) = chars.next() {
                            if c == 'm' { break; }
                        }
                        continue;
                    } else {
                        result.push('\\');
                        result.push('u');
                    }
                } else {
                    result.push('\\');
                }
            } else if ch == '\x1b' {
                // Handle actual ANSI escape sequences
                if chars.as_str().starts_with("[") {
                    chars.next(); // skip '['
                    while let Some(c) = chars.next() {
                        if c == 'm' { break; }
                    }
                    continue;
                }
            } else if ch == '\n' || (ch == '\\' && chars.as_str().starts_with("n")) {
                // Skip newlines
                if ch == '\\' { chars.next(); } // skip 'n'
                continue;
            } else {
                result.push(ch);
            }
        }
        
        result
    }
    
    /// Parse the cleaned message to extract log level, module, and message
    fn parse_clean_message(&self, cleaned: &str) -> String {
        // Quick log level detection using byte comparison (faster than string contains)
        let log_level = if cleaned.as_bytes().windows(6).any(|w| w == b" INFO ") { "INFO" }
        else if cleaned.as_bytes().windows(7).any(|w| w == b" DEBUG ") { "DEBUG" }
        else if cleaned.as_bytes().windows(6).any(|w| w == b" WARN ") { "WARN" }
        else if cleaned.as_bytes().windows(7).any(|w| w == b" ERROR ") { "ERROR" }
        else if cleaned.as_bytes().windows(7).any(|w| w == b" TRACE ") { "TRACE" }
        else { return format!("[NR_EXT]::{}", cleaned.trim()); };
        
        // Extract module and message using single pass
        if let Some(module_start) = cleaned.find("newrelic_lambda_extension::") {
            if let Some(colon_pos) = cleaned[module_start..].find(':') {
                let module_end = module_start + colon_pos;
                let module = &cleaned[module_start..module_end];
                
                // Extract just the filename (last component)
                let filename = module.rfind("::").map_or(module, |pos| &module[pos + 2..]);
                
                // Find message after ": "
                if let Some(msg_start) = cleaned[module_end..].find(": ") {
                    let message = cleaned[module_end + msg_start + 2..].trim();
                    return format!("[NR_EXT]::[{}]::[{}]::{}", log_level, filename, message);
                }
            }
        }
        
        format!("[NR_EXT]::[{}]::{}", log_level, cleaned.trim())
    }

    /// Converts a TelemetryRecord into a LogMessage, if applicable.
    fn to_log_message(&self, record: TelemetryRecord) -> Option<payload::LogMessage> {
        let timestamp = record.time.timestamp_millis();
        let raw_message = record.record.to_string();
        let message = self.clean_log_message(&raw_message);
        let mut attributes = serde_json::Map::new();
        if let Some(request_id) = record.record.get("requestId").and_then(|v| v.as_str()) {
            attributes.insert("request_id".to_string(), request_id.into());
        }

        Some(payload::LogMessage {
            timestamp,
            message,
            attributes,
        })
    }

    /// Check if we should send logs immediately (simple batching)
    pub fn should_send_immediately(&self) -> bool {
        let batch = self.log_batch.lock().unwrap();
        batch.len() >= 5 // Send every 5 logs
    }

    /// Get current batch size
    pub fn get_batch_size(&self) -> usize {
        let batch = self.log_batch.lock().unwrap();
        batch.len()
    }

    /// Simple synchronous send method - just send the data without complex async handling
    pub async fn send_and_clear_batch_simple(&self) -> Result<()> {
        let batch = {
            let mut batch_guard = self.log_batch.lock().unwrap();
            std::mem::take(&mut *batch_guard)
        };
        
        if batch.is_empty() {
            debug!("[LogProcessor] No logs to send");
            return Ok(());
        }

        info!("[LogProcessor] Sending {} logs to New Relic NOW", batch.len());

        let client = Arc::clone(&self.newrelic_client);
        let config = Arc::clone(&self.config);
        let context = self.invocation_context.lock().unwrap().clone();
        
        // Send directly without spawning - simpler and more reliable
        match client.send_logs(&config, batch, &context.invoked_function_arn).await {
            Ok(()) => {
                info!("[LogProcessor] Successfully sent logs to New Relic");
                Ok(())
            },
            Err(e) => {
                error!("[LogProcessor] Failed to send logs: {}", e);
                Err(std::io::Error::new(std::io::ErrorKind::Other, e))
            }
        }
    }
}

#[async_trait]
impl Flush for LogProcessor {
    async fn flush(&self) -> Result<()> {
        self.send_and_clear_batch_simple().await
    }

    async fn final_flush(&self) -> Result<()> {
        self.send_and_clear_batch_simple().await
    }
}

