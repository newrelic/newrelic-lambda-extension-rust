use nix::sys::stat;
use nix::unistd;
use std::fs;
use std::io::{Error, ErrorKind, Read, Result};
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task;
use tracing::{error, info, warn};

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

    // 1. Remove the pipe if it already exists, ignoring "Not Found" errors.
    match fs::remove_file(path) {
        Ok(_) => info!("Removed existing telemetry pipe."),
        Err(e) if e.kind() == ErrorKind::NotFound => (),
        Err(e) => return Err(e),
    }

    // 2. Create the new named pipe (FIFO) with 0666 permissions.
    let mode = stat::Mode::from_bits(0o666).unwrap(); // 0o666 is always valid
    unistd::mkfifo(path, mode)
        .map_err(|e| Error::new(ErrorKind::Other, format!("Failed to create FIFO: {}", e)))?;
    info!("Created new telemetry pipe at {}", TELEMETRY_NAMED_PIPE_PATH);


    // 3. Wait for the pipe to be visible in the filesystem to avoid race conditions.
    let mut tries = 0;
    while !path.exists() {
        if tries >= TELEMETRY_NAMED_PIPE_RETRIES {
            return Err(Error::new(ErrorKind::TimedOut, "Failed to confirm pipe creation"));
        }
        tries += 1;
        tokio::time::sleep(TELEMETRY_NAMED_PIPE_RETRY_DELAY).await;
    }

    // 4. Create an MPSC channel to send data from the pipe listener to the main application.
    let (tx, rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);

    // 5. Spawn a background task to poll the pipe.
    tokio::spawn(async move {
        info!("Starting telemetry pipe listener loop.");
        loop {
            // Reading from the pipe is a blocking operation. We offload it to a
            // blocking-safe thread pool to avoid starving the async executor.
            let bytes_result = poll_for_telemetry().await;

            match bytes_result {
                Ok(bytes) => {
                    if bytes.is_empty() {
                        // This can happen if the writer closes the pipe immediately.
                        // We just continue to the next read attempt.
                        continue;
                    }
                    // Send the received data through the channel. If it fails,
                    // it means the receiver has been dropped, so we can exit the loop.
                    if tx.send(bytes).await.is_err() {
                        warn!("Telemetry channel closed by receiver. Shutting down listener.");
                        break;
                    }
                }
                Err(e) => {
                    error!("Error polling for telemetry: {}. Retrying in 1s.", e);
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
        // This closure runs on a dedicated thread, so blocking is okay.
        let mut pipe = fs::File::open(TELEMETRY_NAMED_PIPE_PATH)?;
        let mut buffer = Vec::new();
        pipe.read_to_end(&mut buffer)?;
        Ok(buffer)
    })
    .await
    .unwrap_or_else(|join_error| {
        // The task panicked, which is a critical error.
        Err(Error::new(ErrorKind::Other, join_error.to_string()))
    })
}