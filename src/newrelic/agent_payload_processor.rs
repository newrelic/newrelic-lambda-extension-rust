use crate::newrelic::client::NewRelicClient;
use crate::config::ExtensionConfig;
use std::sync::Arc;
use serde_json::json;
use base64::{Engine as _, engine::general_purpose};
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{info, error};

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
        let mut payload_count = 0;
        loop {
            info!("[AgentPayloadProcessor] Pipeline iteration {}, waiting for agent data...", payload_count + 1);
            match self.listen_and_process_once().await {
                Ok(_) => {
                    // Successfully processed one payload, continue listening
                    payload_count += 1;
                    info!("[AgentPayloadProcessor] Successfully processed payload #{}, continuing to listen", payload_count);
                }
                Err(e) => {
                    error!("[AgentPayloadProcessor] Pipeline error on iteration {}: {}", payload_count + 1, e);
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
        info!("[AgentPayloadProcessor] Complete agent payload: {}", payload);
        
        // Wrap the raw payload with context (no decompression needed)
        info!("[AgentPayloadProcessor] Starting to wrap payload with context...");
        let wrapped_payload = match self.wrap_payload_with_context(payload) {
            Ok(payload) => {
                info!("[AgentPayloadProcessor] Successfully wrapped payload with context");
                payload
            },
            Err(e) => {
                error!("[AgentPayloadProcessor] Failed to wrap payload with context: {}", e);
                return Err(e);
            }
        };
        info!("[AgentPayloadProcessor] Wrapped payload: {} bytes", wrapped_payload.len());
        
        
        // Send to New Relic immediately with timeout
        info!("[AgentPayloadProcessor] Sending to New Relic: {} bytes", wrapped_payload.len());
        let send_start = std::time::Instant::now();

        // Add timeout to prevent hanging during Lambda shutdown
        let send_result = tokio::time::timeout(
            tokio::time::Duration::from_secs(5),
            self.client.send_agent_payload(&self.config, &wrapped_payload)
        ).await;
        
        let send_duration = send_start.elapsed();
        
        match send_result {
            Ok(Ok(())) => {
                info!("[AgentPayloadProcessor] Successfully sent agent payload to New Relic in {:?}", send_duration);
            }
            Ok(Err(e)) => {
                error!("[AgentPayloadProcessor] Failed to send agent payload after {:?}: {}", send_duration, e);
                return Err(e.into());
            }
            Err(_) => {
                error!("[AgentPayloadProcessor] Agent payload send timed out after {:?} (5 second limit)", send_duration);
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

    /// Wrap the decompressed payload with Lambda context information
    fn wrap_payload_with_context(&self, payload: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // Create context information from configuration
        let function_name = &self.config.aws.function_name;
        let log_group_name = format!("/aws/lambda/{}", function_name);
        
        // For the invoked_function_arn, we need to construct it from available info
        // In a real Lambda environment, this would come from AWS_LAMBDA_FUNCTION_NAME and other env vars
        let invoked_function_arn = std::env::var("AWS_LAMBDA_FUNCTION_NAME")
            .map(|name| format!("arn:aws:lambda:{}:{}:function:{}", 
                std::env::var("AWS_REGION").unwrap_or_else(|_| "unknown".to_string()),
                std::env::var("AWS_ACCOUNT_ID").unwrap_or_else(|_| "unknown".to_string()),
                name))
            .unwrap_or_else(|_| format!("arn:aws:lambda:unknown:unknown:function:{}", function_name));
        
        let log_stream_name = format!("newrelic-lambda-extension:{}", 
            env!("CARGO_PKG_VERSION"));

        // Create the wrapped payload structure
        let wrapped_payload = json!({
            "context": {
                "function_name": function_name,
                "invoked_function_arn": invoked_function_arn,
                "log_group_name": log_group_name,
                "log_stream_name": log_stream_name
            },
            "entry": payload
        });

        Ok(wrapped_payload.to_string())
    }

}
