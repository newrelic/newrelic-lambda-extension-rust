// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tests for `apm::connection`

#[cfg(test)]
mod tests {
    use crate::apm::connection::{
        compress_inline, connect_attempts_total, connect_cycles, http_failure_reason,
        is_handshake_fatal, is_permanent_auth_error, last_failure_reason, record_connect_attempt,
        record_connect_cycle, record_failure_reason, reset_connect_stats,
        reset_handshake_fatal_for_test, signal_handshake_fatal, PermanentAuthError,
    };
    use anyhow::anyhow;
    use serial_test::serial;

    #[test]
    fn permanent_auth_error_detected_through_context_chain() {
        // Mirrors how try_connect wraps the error: `.context("PreConnect failed")`.
        let err = anyhow::Error::new(PermanentAuthError { status: 401 })
            .context("PreConnect failed");
        assert_eq!(is_permanent_auth_error(&err), Some(401));
    }

    #[test]
    fn transient_error_is_not_permanent() {
        let err = anyhow!("Connect failed with HTTP 503 - service unavailable");
        assert_eq!(is_permanent_auth_error(&err), None);
    }

    #[test]
    fn permanent_auth_error_display_has_no_secret() {
        let msg = PermanentAuthError { status: 403 }.to_string();
        assert!(msg.contains("403"));
        assert!(!msg.contains("license_key"));
    }

    #[test]
    #[serial]
    fn handshake_fatal_latch_roundtrips() {
        reset_handshake_fatal_for_test();
        assert!(!is_handshake_fatal());
        signal_handshake_fatal();
        assert!(is_handshake_fatal());
        reset_handshake_fatal_for_test();
        assert!(!is_handshake_fatal());
    }

    #[test]
    #[serial]
    fn connect_stats_accumulate_and_reset() {
        reset_connect_stats();
        record_connect_cycle();
        record_connect_attempt();
        record_connect_attempt();
        record_failure_reason("HTTP 503");
        assert_eq!(connect_cycles(), 1);
        assert_eq!(connect_attempts_total(), 2);
        assert_eq!(last_failure_reason().as_deref(), Some("HTTP 503"));
        // A successful connect resets the disconnected-streak diagnostics.
        reset_connect_stats();
        assert_eq!(connect_cycles(), 0);
        assert_eq!(connect_attempts_total(), 0);
        assert_eq!(last_failure_reason(), None);
    }

    #[test]
    fn compress_inline_roundtrips_via_gzip_decoder() {
        use std::io::Read;

        let original = b"the quick brown fox jumps over the lazy dog ".repeat(50);
        let compressed = compress_inline(&original).expect("compression should succeed");

        let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).expect("decompression should succeed");

        assert_eq!(decompressed, original);
    }

    #[test]
    fn compress_inline_shrinks_repetitive_data() {
        let original = vec![b'a'; 10_000];
        let compressed = compress_inline(&original).expect("compression should succeed");
        assert!(compressed.len() < original.len());
    }

    #[test]
    fn compress_inline_handles_empty_input() {
        let compressed = compress_inline(&[]).expect("compression of empty input should succeed");
        assert!(!compressed.is_empty(), "gzip stream still has header/footer bytes");
    }

    #[test]
    fn http_failure_reason_uses_api_body_and_truncates() {
        // Empty body → just the code.
        assert_eq!(http_failure_reason(503, "   "), "HTTP 503");
        // Real collector message is surfaced (trimmed), not a hardcoded phrase.
        assert_eq!(
            http_failure_reason(401, "  Invalid license key.  "),
            "HTTP 401: Invalid license key."
        );
        // A verbose body is truncated so it can't bloat the log line.
        let long = "x".repeat(500);
        let r = http_failure_reason(500, &long);
        assert!(r.starts_with("HTTP 500: "));
        assert!(r.len() <= "HTTP 500: ".len() + 300);
    }
}
