use std::sync::{Arc, Mutex};
use serde::Serialize;
use tokio::sync::mpsc;
use tracing::{debug, error, info};
use crate::{
    context::InvocationContext,
    newrelic::client::NewRelicClient,
};
use chrono::Utc;

// --- Structs for building the wrapped payload ---

#[derive(Serialize)]
struct WrappedPayload<'a> {
    context: Context<'a>,
    entry: String, // This will be a JSON string itself
}

#[derive(Serialize)]
struct Context<'a> {
    function_name: &'a str,
    invoked_function_arn: &'a str,
    log_group_name: String,
    log_stream_name: &'a str,
}

#[derive(Serialize)]
struct EntryPayload<'a> {
    #[serde(rename = "logEvents")]
    log_events: Vec<LogEvent<'a>>,
    #[serde(rename = "logGroup")]
    log_group: String,
    #[serde(rename = "logStream")]
    log_stream: &'a str,
    #[serde(rename = "messageType")]
    message_type: &'a str,
    owner: &'a str,
}

#[derive(Serialize)]
struct LogEvent<'a> {
    id: &'a str,
    message: &'a str,
    timestamp: i64,
}


/// Spawns a new asynchronous task to process telemetry payloads from the agent.
pub fn start_agent_payload_processor(
    mut receiver: mpsc::Receiver<Vec<u8>>,
    newrelic_client: Arc<NewRelicClient>,
    invocation_context: Arc<Mutex<InvocationContext>>,
) {
    tokio::spawn(async move {
        info!("✅ Agent payload processor has started and is waiting for data.");

        while let Some(payload_bytes) = receiver.recv().await {
            info!("Received agent telemetry payload ({} bytes).", payload_bytes.len());

            let payload_str = match String::from_utf8(payload_bytes) {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to parse agent payload as UTF-8: {}", e);
                    continue;
                }
            };

            // 1. Lock the invocation context to get metadata.
            let context_guard = invocation_context.lock().unwrap();
            let request_id = &context_guard.request_id;
            let invoked_function_arn = &context_guard.invoked_function_arn;

            // Extract function_name from the ARN. The ARN format is arn:aws:lambda:REGION:ACCOUNT_ID:function:FUNCTION_NAME.
            let function_name = invoked_function_arn.split(':').last().unwrap_or("");

            if request_id.is_empty() || function_name.is_empty() {
                error!("Received agent payload but invocation context is incomplete. Skipping.");
                continue;
            }

            // 2. Build the inner `entry` JSON string.
            let log_group_name = format!("/aws/lambda/{}", function_name);
            let timestamp = Utc::now().timestamp_millis();

            let log_event = LogEvent {
                id: request_id,
                message: &payload_str,
                timestamp,
            };

            let entry_payload = EntryPayload {
                log_events: vec![log_event],
                log_group: log_group_name.clone(),
                log_stream: "",
                message_type: "",
                owner: "",
            };

            let entry_string = match serde_json::to_string(&entry_payload) {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to serialize entry payload to JSON: {}", e);
                    continue;
                }
            };

            // 3. Build the final wrapped payload.
            let context = Context {
                function_name,
                invoked_function_arn,
                log_group_name,
                log_stream_name: "newrelic-lambda-extension:2.3.19", // Example version
            };

            let wrapped_payload = WrappedPayload {
                context,
                entry: entry_string,
            };
            
            // 4. Serialize the final object to a pretty JSON string and log it.
            match serde_json::to_string_pretty(&wrapped_payload) {
                Ok(final_json) => {
                    info!("Wrapped Agent Payload:\n{}", final_json);
                }
                Err(e) => {
                    error!("Failed to serialize final wrapped payload: {}", e);
                }
            }

            debug!("Would process payload for request_id: {}", request_id);
            let _ = (Arc::clone(&newrelic_client), request_id);
        }

        info!("Agent payload processor channel closed. Shutting down.");
    });
}

