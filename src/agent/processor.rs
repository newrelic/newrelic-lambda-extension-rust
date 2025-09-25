use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tracing::{debug, trace};

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
        debug!("Agent payload collector started and waiting for data.");
        let mut payload_count = 0;

        while let Some(payload_bytes) = receiver.recv().await {
            payload_count += 1;
            
            // Only log every 10th payload to reduce noise, or log first few
            if payload_count <= 3 || payload_count % 10 == 0 {
                trace!("Collector received agent telemetry payload ({} bytes) - count: {}", payload_bytes.len(), payload_count);
            }
            
            // Lock the buffer and add the new payload
            let mut buffer = payload_buffer.lock().unwrap();
            buffer.push(payload_bytes);
        }

        debug!("Agent payload collector channel closed. Shutting down.");
    });
}

