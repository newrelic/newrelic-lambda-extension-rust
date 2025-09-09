use crate::newrelic::client::NewRelicClient;
use crate::config::ExtensionConfig;
use std::sync::Arc;
use serde_json::Value;
use base64::{Engine as _, engine::general_purpose};
use flate2::read::GzDecoder;
use std::io::Read;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{info, error, warn};

const AGENT_PIPE_PATH: &str = "/tmp/newrelic-telemetry";

pub struct AgentPayloadProcessor {
    client: Arc<NewRelicClient>,
    config: Arc<ExtensionConfig>,
}

impl AgentPayloadProcessor {
    pub fn new(client: Arc<NewRelicClient>, config: Arc<ExtensionConfig>) -> Self {
        Self { client, config }
    }

    /// Start the agent payload pipeline: Listen -> Process -> Send
    /// This runs in background and processes agent payloads immediately
    pub async fn start_pipeline(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("[AgentPayloadProcessor] Starting agent payload pipeline");
        
        // Create the named pipe
        self.create_named_pipe(AGENT_PIPE_PATH)?;
        
        // Start the pipeline: continuously listen for agent data
        loop {
            match self.listen_and_process_once().await {
                Ok(_) => {
                    // Successfully processed one payload, continue listening
                    info!("[AgentPayloadProcessor] Processed one payload, continuing to listen");
                }
                Err(e) => {
                    error!("[AgentPayloadProcessor] Pipeline error: {}", e);
                    // Sleep briefly before retrying
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
        }
    }

    /// Listen for one agent payload, process it, and send it immediately
    async fn listen_and_process_once(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("[AgentPayloadProcessor] Opening pipe for reading: {}", AGENT_PIPE_PATH);
        
        // Open the pipe (this will block until agent writes to it)
        let file = File::open(AGENT_PIPE_PATH).await?;
        let mut reader = BufReader::new(file);
        
        // Read one line from the pipe
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line).await?;
        
        if bytes_read == 0 {
            // EOF - pipe was closed by writer
            return Ok(());
        }
        
        let line = line.trim();
        if line.is_empty() {
            return Ok(());
        }
        
        info!("[AgentPayloadProcessor] Received agent payload: {} bytes", line.len());
        
        // Process and send immediately
        self.process_and_send_payload(line).await?;
        
        Ok(())
    }

    /// Process agent payload and send to New Relic immediately
    async fn process_and_send_payload(&self, payload: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("[AgentPayloadProcessor] Processing agent payload: {} bytes", payload.len());
        
        // Parse JSON array: [version, "NR_LAMBDA_MONITORING", metadata, compressed_data]
        let json_array: Value = serde_json::from_str(payload)?;
        let array = json_array.as_array()
            .ok_or("Agent payload is not a JSON array")?;
        
        if array.len() < 4 {
            return Err("Agent payload array has insufficient elements".into());
        }
        
        // Check if this is NR_LAMBDA_MONITORING payload
        let payload_type = array[1].as_str()
            .ok_or("Second element is not a string")?;
        
        if payload_type != "NR_LAMBDA_MONITORING" {
            warn!("[AgentPayloadProcessor] Skipping non-monitoring payload: {}", payload_type);
            return Ok(());
        }
        
        info!("[AgentPayloadProcessor] Found NR_LAMBDA_MONITORING payload");
        
        // Get compressed data (4th element)
        let compressed_data = array[3].as_str()
            .ok_or("Compressed data is not a string")?;
        
        // Decode base64
        let decoded_bytes = general_purpose::STANDARD.decode(compressed_data)?;
        info!("[AgentPayloadProcessor] Base64 decoded {} bytes", decoded_bytes.len());
        
        // Decompress gzip
        let mut decoder = GzDecoder::new(&decoded_bytes[..]);
        let mut decompressed = String::new();
        decoder.read_to_string(&mut decompressed)?;
        info!("[AgentPayloadProcessor] Gzip decompressed {} bytes", decompressed.len());
        
        // Send to New Relic immediately with timeout
        info!("[AgentPayloadProcessor] Sending to New Relic: {} bytes", decompressed.len());
        
        // Add timeout to prevent hanging during Lambda shutdown
        let send_result = tokio::time::timeout(
            tokio::time::Duration::from_secs(5),
            self.client.send_agent_payload(&self.config, &decompressed)
        ).await;
        
        match send_result {
            Ok(Ok(())) => {
                info!("[AgentPayloadProcessor] Successfully sent agent payload to New Relic");
            }
            Ok(Err(e)) => {
                error!("[AgentPayloadProcessor] Failed to send agent payload: {}", e);
                return Err(e.into());
            }
            Err(_) => {
                error!("[AgentPayloadProcessor] Agent payload send timed out after 5 seconds");
                return Err("Send timeout".into());
            }
        }
        
        Ok(())
    }

    /// Create named pipe for agent communication
    fn create_named_pipe(&self, pipe_path: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if Path::new(pipe_path).exists() {
            info!("[AgentPayloadProcessor] Named pipe already exists: {}", pipe_path);
            return Ok(());
        }

        info!("[AgentPayloadProcessor] Creating named pipe: {}", pipe_path);
        
        // Create named pipe using libc
        use std::ffi::CString;
        let path_cstring = CString::new(pipe_path)?;
        
        let result = unsafe {
            libc::mkfifo(path_cstring.as_ptr(), 0o666)
        };
        
        if result != 0 {
            let error = std::io::Error::last_os_error();
            return Err(format!("Failed to create named pipe: {}", error).into());
        }
        
        info!("[AgentPayloadProcessor] Named pipe created successfully: {}", pipe_path);
        Ok(())
    }
}
