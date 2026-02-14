use nix::sys::stat;
use nix::unistd;
use std::fs;
use std::io::{Error, ErrorKind, Read, Result};
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task;
use tracing::{debug, error, trace, warn};

pub const TELEMETRY_NAMED_PIPE_PATH: &str = "/tmp/newrelic-telemetry";
const TELEMETRY_NAMED_PIPE_RETRIES: u32 = 10;
const TELEMETRY_NAMED_PIPE_RETRY_DELAY: Duration = Duration::from_millis(10);
const CHANNEL_BUFFER_SIZE: usize = 100;

/// Initializes the named pipe (FIFO) for telemetry and returns a receiver channel.
///
/// This function creates a named pipe at `/tmp/newrelic-telemetry` and spawns a background
/// task that continuously listens for data on the pipe. The data is then sent through an
/// MPSC channel, the receiver of which is returned by this function.
pub async fn init_telemetry_channel() -> Result<mpsc::Receiver<Vec<u8>>> {
    let path = Path::new(TELEMETRY_NAMED_PIPE_PATH);

    match fs::remove_file(path) {
        Ok(_) => {},
        Err(e) if e.kind() == ErrorKind::NotFound => (),
        Err(e) => return Err(e),
    }

    let mode = stat::Mode::from_bits(0o666).unwrap();
    unistd::mkfifo(path, mode)
        .map_err(|e| Error::new(ErrorKind::Other, format!("Failed to create FIFO: {}", e)))?;
    debug!("Created new telemetry pipe at {}", TELEMETRY_NAMED_PIPE_PATH);

    let mut tries = 0;
    while !path.exists() {
        if tries >= TELEMETRY_NAMED_PIPE_RETRIES {
            return Err(Error::new(ErrorKind::TimedOut, "Failed to confirm pipe creation"));
        }
        tries += 1;
        tokio::time::sleep(TELEMETRY_NAMED_PIPE_RETRY_DELAY).await;
    }

    let (tx, rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);

    tokio::spawn(async move {
        debug!("Starting telemetry pipe listener loop.");
        let mut consecutive_errors = 0;
        let mut bytes_received_count = 0;
        
        loop {
            let bytes_result = poll_for_telemetry().await;

            match bytes_result {
                Ok(bytes) => {
                    consecutive_errors = 0;
                    
                    if bytes.is_empty() {
                        continue;
                    }
                    
                    bytes_received_count += 1;
                    if bytes_received_count % 10 == 1 {
                        trace!("Received {} bytes from telemetry pipe (count: {})", bytes.len(), bytes_received_count);
                    }
                    
                    if tx.send(bytes).await.is_err() {
                        warn!("Telemetry channel closed by receiver. Shutting down listener.");
                        break;
                    }
                }
                Err(e) => {
                    consecutive_errors += 1;
                    if consecutive_errors % 5 == 1 {
                        error!("Error polling for telemetry: {} (count: {}). Retrying in 1s.", e, consecutive_errors);
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });

    Ok(rx)
}

/// Reads data from the named pipe in a blocking-safe context.
/// This function will block until a writer opens the other end of the pipe and writes data.
async fn poll_for_telemetry() -> Result<Vec<u8>> {
    task::spawn_blocking(|| {
        let mut pipe = fs::File::open(TELEMETRY_NAMED_PIPE_PATH)?;
        let mut buffer = Vec::new();
        pipe.read_to_end(&mut buffer)?;
        Ok(buffer)
    })
    .await
    .unwrap_or_else(|join_error| {
        Err(Error::new(ErrorKind::Other, join_error.to_string()))
    })
}