// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use super::super::credentials::{merge_ca_bundle_if_needed, MERGED_BUNDLE_PATH, SYSTEM_BUNDLE_PATHS};

    /// Returns true if at least one system CA bundle path exists on this machine.
    /// Tests that require a system bundle are skipped when none is found (e.g. minimal CI images).
    fn system_bundle_available() -> bool {
        SYSTEM_BUNDLE_PATHS.iter().any(|p| std::path::Path::new(p).exists())
    }

    fn cleanup() {
        std::env::remove_var("SSL_CERT_FILE");
        let _ = std::fs::remove_file(MERGED_BUNDLE_PATH);
    }

    // -------------------------------------------------------------------------
    // No-op paths
    // -------------------------------------------------------------------------

    /// SSL_CERT_FILE not set — function must be a complete no-op.
    #[test]
    #[serial]
    fn test_no_op_when_ssl_cert_file_not_set() {
        cleanup();
        merge_ca_bundle_if_needed();
        assert!(std::env::var("SSL_CERT_FILE").is_err());
        assert!(!std::path::Path::new(MERGED_BUNDLE_PATH).exists());
    }

    /// SSL_CERT_FILE set to empty string — treated same as unset.
    #[test]
    #[serial]
    fn test_no_op_when_ssl_cert_file_empty() {
        cleanup();
        std::env::set_var("SSL_CERT_FILE", "");
        merge_ca_bundle_if_needed();
        assert_ne!(
            std::env::var("SSL_CERT_FILE").unwrap_or_default(),
            MERGED_BUNDLE_PATH
        );
        assert!(!std::path::Path::new(MERGED_BUNDLE_PATH).exists());
    }

    // -------------------------------------------------------------------------
    // Change 4: Idempotency guard
    // -------------------------------------------------------------------------

    /// If SSL_CERT_FILE already points at the merged bundle, the function must
    /// return immediately without touching anything.
    #[test]
    #[serial]
    fn test_idempotent_when_already_pointing_at_merged_path() {
        cleanup();
        std::env::set_var("SSL_CERT_FILE", MERGED_BUNDLE_PATH);
        std::fs::write(MERGED_BUNDLE_PATH, b"already merged content").unwrap();

        merge_ca_bundle_if_needed();

        assert_eq!(std::env::var("SSL_CERT_FILE").unwrap(), MERGED_BUNDLE_PATH);
        let content = std::fs::read(MERGED_BUNDLE_PATH).unwrap();
        assert_eq!(content, b"already merged content");
    }

    // -------------------------------------------------------------------------
    // Original fix: unset SSL_CERT_FILE when cert file is missing/unreadable
    // -------------------------------------------------------------------------

    /// SSL_CERT_FILE points at a non-existent path — must be unset so the AWS
    /// SDK falls back to system CAs instead of crashing with zero root CAs.
    #[test]
    #[serial]
    fn test_unsets_ssl_cert_file_when_cert_file_missing() {
        cleanup();
        std::env::set_var("SSL_CERT_FILE", "/nonexistent/path/proxy_ca.pem");
        merge_ca_bundle_if_needed();
        assert!(
            std::env::var("SSL_CERT_FILE").is_err(),
            "SSL_CERT_FILE should be unset when cert file is missing"
        );
    }

    // -------------------------------------------------------------------------
    // Change 3: PEM validation
    // -------------------------------------------------------------------------

    /// SSL_CERT_FILE points at a file that exists but is not PEM-encoded.
    /// The function must skip the merge and leave SSL_CERT_FILE pointing at
    /// the original path (so the user can see the misconfiguration in logs).
    #[test]
    #[serial]
    fn test_skips_merge_for_non_pem_file() {
        if !system_bundle_available() {
            eprintln!("SKIP: no system CA bundle found on this machine");
            return;
        }
        cleanup();
        let fake_cert_path = "/tmp/nr_test_fake_cert.bin";
        std::fs::write(fake_cert_path, b"this is not a certificate, just binary garbage").unwrap();
        std::env::set_var("SSL_CERT_FILE", fake_cert_path);

        merge_ca_bundle_if_needed();

        assert!(
            std::env::var("SSL_CERT_FILE").is_err(),
            "SSL_CERT_FILE should be unset when file is not PEM"
        );
        assert!(
            !std::path::Path::new(MERGED_BUNDLE_PATH).exists(),
            "Merged bundle must not be created for non-PEM input"
        );
        let _ = std::fs::remove_file(fake_cert_path);
    }

    // -------------------------------------------------------------------------
    // Happy path + Change 5: double newline separator
    // -------------------------------------------------------------------------

    /// Full happy path: valid PEM cert + system bundle present.
    /// SSL_CERT_FILE must be updated to point at the merged bundle.
    #[test]
    #[serial]
    fn test_successful_merge_updates_ssl_cert_file() {
        if !system_bundle_available() {
            eprintln!("SKIP: no system CA bundle found on this machine");
            return;
        }
        cleanup();
        let fake_pem_path = "/tmp/nr_test_proxy_ca.pem";
        std::fs::write(
            fake_pem_path,
            b"-----BEGIN CERTIFICATE-----\nMIIFakeCertContentHere\n-----END CERTIFICATE-----\n",
        )
        .unwrap();
        std::env::set_var("SSL_CERT_FILE", fake_pem_path);

        merge_ca_bundle_if_needed();

        assert_eq!(
            std::env::var("SSL_CERT_FILE").unwrap(),
            MERGED_BUNDLE_PATH,
            "SSL_CERT_FILE must point at merged bundle after successful merge"
        );
        assert!(
            std::path::Path::new(MERGED_BUNDLE_PATH).exists(),
            "Merged bundle file must exist"
        );
        let _ = std::fs::remove_file(fake_pem_path);
    }

    /// Merged bundle must contain the custom cert content appended after the system bundle,
    /// separated by a double newline so PEM parsers across all TLS libraries handle it correctly.
    #[test]
    #[serial]
    fn test_merged_bundle_has_double_newline_separator() {
        if !system_bundle_available() {
            eprintln!("SKIP: no system CA bundle found on this machine");
            return;
        }
        cleanup();
        let fake_pem_path = "/tmp/nr_test_proxy_ca2.pem";
        let custom_cert = b"-----BEGIN CERTIFICATE-----\nMIICustomCertData\n-----END CERTIFICATE-----\n";
        std::fs::write(fake_pem_path, custom_cert).unwrap();
        std::env::set_var("SSL_CERT_FILE", fake_pem_path);

        merge_ca_bundle_if_needed();

        let merged = std::fs::read(MERGED_BUNDLE_PATH).expect("merged bundle must exist");
        let merged_str = String::from_utf8_lossy(&merged);

        assert!(
            merged_str.contains("MIICustomCertData"),
            "Merged bundle must contain custom cert content"
        );
        assert!(
            merged_str.contains("\n\n-----BEGIN CERTIFICATE-----"),
            "Merged bundle must have double newline before custom cert BEGIN marker"
        );

        let _ = std::fs::remove_file(fake_pem_path);
    }
}
