use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tracing::{info};

/// Spawns a new asynchronous task that continuously receives telemetry payloads
/// from the agent and stores them in a shared buffer.
///
/// This function's only job is to act as a collector. The main application loop
/// is responsible for processing the data in the buffer.
///
/// # Arguments
///
/// * `receiver` - The receiving end of the MPSC channel from `ipc::init_telemetry_channel`.
/// * `payload_buffer` - A shared, thread-safe buffer where received payloads are stored.
pub fn start_agent_payload_collector(
    mut receiver: mpsc::Receiver<Vec<u8>>,
    payload_buffer: Arc<Mutex<Vec<Vec<u8>>>>,
) {
    tokio::spawn(async move {
        info!("Agent payload collector has started and is waiting for data.");

        while let Some(payload_bytes) = receiver.recv().await {
            info!("[agentsend] Collector received agent telemetry payload ({} bytes).", payload_bytes.len());
            // Lock the buffer and add the new payload
            let mut buffer = payload_buffer.lock().unwrap();
            buffer.push(payload_bytes);
        }

        info!("Agent payload collector channel closed. Shutting down.");
    });
}

