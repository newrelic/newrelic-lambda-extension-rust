// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the telemetry named pipe (FIFO) setup in `agent::ipc`.
//!
//! These tests exercise real filesystem/FIFO behavior (no mocking needed —
//! `init_telemetry_channel` only touches the local filesystem and a tokio
//! mpsc channel). All tests share the same fixed OS path
//! (`TELEMETRY_NAMED_PIPE_PATH`), so they run `#[serial]` to avoid racing.
//!
//! `init_telemetry_channel` spawns a background task that loops forever
//! polling the pipe (by design — it only stops when the process exits, same
//! as in production). Once that loop has actually opened the FIFO for read at
//! least once, its `spawn_blocking` closure is parked in a real blocking
//! `open()` syscall waiting for the *next* writer, and never returns. A plain
//! `#[tokio::test]`'s implicit `Runtime::drop` at the end of the test
//! function waits for exactly that thread to finish and hangs forever, so
//! these tests build their own runtime and call `shutdown_background()`
//! instead — which detaches without waiting, leaking one dormant OS thread
//! (harmless; it stays parked on the pipe until the process exits, and
//! `cleanup()` still frees the path itself for the next test).

#[cfg(test)]
mod tests {
    use crate::agent::ipc::{init_telemetry_channel, TELEMETRY_NAMED_PIPE_PATH};
    use serial_test::serial;
    use std::io::Write;
    use std::os::unix::fs::FileTypeExt;
    use std::time::Duration;

    fn cleanup() {
        let _ = std::fs::remove_file(TELEMETRY_NAMED_PIPE_PATH);
    }

    fn multi_thread_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("should be able to build a tokio runtime")
    }

    #[test]
    #[serial]
    fn init_creates_a_fifo_at_the_well_known_path() {
        cleanup();
        let rt = multi_thread_runtime();

        rt.block_on(async {
            let _rx = init_telemetry_channel().await.expect("pipe init should succeed");

            let file_type = std::fs::metadata(TELEMETRY_NAMED_PIPE_PATH)
                .expect("pipe path should exist after init")
                .file_type();
            assert!(file_type.is_fifo(), "path must be a FIFO, not a regular file");
        });

        rt.shutdown_background();
        cleanup();
    }

    #[test]
    #[serial]
    fn bytes_written_to_the_pipe_are_delivered_on_the_channel() {
        cleanup();
        let rt = multi_thread_runtime();

        rt.block_on(async {
            let mut rx = init_telemetry_channel().await.expect("pipe init should succeed");

            // Opening a FIFO for write blocks until a reader opens the other
            // end. The reader here is the background task spawned by
            // init_telemetry_channel (via poll_for_telemetry's
            // spawn_blocking), so this must run on a real OS thread rather
            // than being awaited inline, or a single-threaded runtime could
            // deadlock waiting on itself.
            let writer = std::thread::spawn(|| {
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(TELEMETRY_NAMED_PIPE_PATH)
                    .expect("should be able to open the FIFO for write");
                f.write_all(b"hello-telemetry").expect("write should succeed");
                // Dropping f closes the write end, so the reader's
                // read_to_end sees EOF and returns the bytes written above.
            });

            let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("should receive bytes before the timeout")
                .expect("channel should not be closed");

            assert_eq!(received, b"hello-telemetry");

            writer.join().expect("writer thread should not panic");
        });

        rt.shutdown_background();
        cleanup();
    }

    #[test]
    #[serial]
    fn stale_regular_file_at_the_path_is_replaced_by_a_fifo() {
        cleanup();
        std::fs::write(TELEMETRY_NAMED_PIPE_PATH, b"stale leftover content from a previous run")
            .expect("should be able to create a stale regular file");
        let rt = multi_thread_runtime();

        rt.block_on(async {
            let _rx = init_telemetry_channel()
                .await
                .expect("pipe init should succeed despite stale file");

            let file_type = std::fs::metadata(TELEMETRY_NAMED_PIPE_PATH)
                .expect("path should exist after init")
                .file_type();
            assert!(
                file_type.is_fifo(),
                "the stale regular file must be removed and replaced by a FIFO"
            );
        });

        rt.shutdown_background();
        cleanup();
    }
}
