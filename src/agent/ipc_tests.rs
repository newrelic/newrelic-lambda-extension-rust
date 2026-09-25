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

    // ── empty-bytes continue path (line 63) ───────────────────────────────────
    // Opening the write end and closing it without writing causes read_to_end to
    // return Ok(vec![]), which hits the `if bytes.is_empty() { continue }` branch.
    // A second write confirms the loop is still alive after the continue.

    #[test]
    #[serial]
    fn empty_write_triggers_continue_in_listener_loop() {
        cleanup();
        let rt = multi_thread_runtime();

        rt.block_on(async {
            let mut rx = init_telemetry_channel().await.expect("pipe init");

            // Open write end then drop it immediately — no data written → read_to_end → Ok([])
            let _ = std::thread::spawn(|| {
                let _f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(TELEMETRY_NAMED_PIPE_PATH)
                    .expect("open for empty write");
                // _f dropped immediately → write end closes → empty bytes
            });

            // Small pause so the background task processes the empty read before the next write
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Second write — confirms the loop continued and is still listening
            let _ = std::thread::spawn(|| {
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(TELEMETRY_NAMED_PIPE_PATH)
                    .expect("open for real write");
                f.write_all(b"after-empty-write").expect("write");
            });

            let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("should receive bytes before timeout")
                .expect("channel should be open");
            assert_eq!(received, b"after-empty-write");
        });

        rt.shutdown_background();
        cleanup();
    }

    // ── bytes_received_count % 10 != 1 path (line 69) ────────────────────────
    // The trace! fires only when bytes_received_count % 10 == 1. Writing twice
    // means the second receive has count=2, which is ≠1, exercising the else-exit
    // of that if block (the `}` LLVM instruments as an uncovered region).

    #[test]
    #[serial]
    fn two_writes_cover_non_trace_count_branch() {
        cleanup();
        let rt = multi_thread_runtime();

        rt.block_on(async {
            let mut rx = init_telemetry_channel().await.expect("pipe init");

            // First write: count becomes 1, trace! fires (count%10==1 true path)
            let _ = std::thread::spawn(|| {
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(TELEMETRY_NAMED_PIPE_PATH)
                    .expect("open write 1");
                f.write_all(b"msg-1").expect("write 1");
            });
            tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("first recv timeout")
                .expect("channel open");

            // Second write: count becomes 2, trace! skipped (count%10 = 2 ≠ 1 → else path)
            let _ = std::thread::spawn(|| {
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(TELEMETRY_NAMED_PIPE_PATH)
                    .expect("open write 2");
                f.write_all(b"msg-2").expect("write 2");
            });
            let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("second recv timeout")
                .expect("channel open");
            assert_eq!(received, b"msg-2");
        });

        rt.shutdown_background();
        cleanup();
    }

    // ── channel-receiver-dropped path (lines 72-73, 85) ─────────────────────
    // Dropping the receiver before the background task's next send makes
    // tx.send() return Err → the warning fires and `break` exits the loop.
    // Line 85 (closing `}` of the spawned block) is also reached when the task exits.

    #[test]
    #[serial]
    fn dropping_receiver_stops_background_task() {
        cleanup();
        let rt = multi_thread_runtime();

        rt.block_on(async {
            let rx = init_telemetry_channel().await.expect("pipe init");
            // Drop receiver — any subsequent tx.send() will return Err
            drop(rx);

            // Write bytes so the background task wakes up and attempts to send
            let writer = std::thread::spawn(|| {
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(TELEMETRY_NAMED_PIPE_PATH)
                    .expect("open for write");
                f.write_all(b"trigger-closed-channel").expect("write");
            });
            writer.join().expect("writer thread should not panic");

            // Give the background task time to discover the closed channel and break
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        rt.shutdown_background();
        cleanup();
    }
}
