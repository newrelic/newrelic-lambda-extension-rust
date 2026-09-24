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

    // =========================================================================
    // get_new_relic_license_key — non-Lambda early-return path
    // =========================================================================

    /// Without AWS_LAMBDA_RUNTIME_API set, initialize_aws_clients returns Err
    /// immediately (not-in-Lambda guard), so get_new_relic_license_key returns
    /// early without touching the OnceLock — safe to run any time.
    #[tokio::test]
    #[serial]
    async fn test_get_license_key_fails_when_not_in_lambda_env() {
        std::env::remove_var("AWS_LAMBDA_RUNTIME_API");

        let conf = crate::config::Configuration {
            license_key: String::new(),
            license_key_secret_id: String::new(),
            license_key_ssm_parameter_name: String::new(),
        };

        let result = super::super::get_new_relic_license_key(&conf).await;
        assert!(result.is_err(), "expected Err when not in Lambda env");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("Failed to initialize AWS clients"),
            "unexpected error message: {}",
            msg
        );
    }

    // =========================================================================
    // get_new_relic_license_key — full AWS path (OnceLock initialized once)
    //
    // AWS_CLIENTS is a static OnceLock: it is set on the first successful call
    // to initialize_aws_clients() and remains set for the life of the process.
    //
    // Consequence: only the FIRST test that reaches the "AWS_LAMBDA_RUNTIME_API
    // is set AND credentials are available" branch can initialize the clients
    // and proceed past the init guard. Subsequent calls to
    // get_new_relic_license_key always fail at initialize_aws_clients() because
    // AWS_CLIENTS.set() returns Err when the slot is already occupied.
    //
    // This test is therefore the only one that can exercise the full code path
    // beyond the init check. It uses wiremock as the AWS Secrets Manager
    // endpoint (via AWS_ENDPOINT_URL) so no real AWS credentials are needed.
    // =========================================================================

    fn set_aws_credentials_env(endpoint_uri: &str) {
        std::env::set_var("AWS_LAMBDA_RUNTIME_API", "127.0.0.1:9001");
        std::env::set_var("AWS_ENDPOINT_URL", endpoint_uri);
        std::env::set_var("AWS_ACCESS_KEY_ID", "test-key-id");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test-secret-key");
        std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
        // Provide a CA bundle so TLS trust-store initialization works in
        // environments that lack native root CAs (same pattern as aws_layer_tests.rs).
        for path in &[
            "/etc/ssl/cert.pem",
            "/etc/ssl/certs/ca-certificates.crt",
            "/etc/pki/tls/certs/ca-bundle.crt",
        ] {
            if std::path::Path::new(path).exists() {
                std::env::set_var("AWS_CA_BUNDLE", path);
                break;
            }
        }
    }

    fn clear_aws_credentials_env() {
        std::env::remove_var("AWS_LAMBDA_RUNTIME_API");
        std::env::remove_var("AWS_ENDPOINT_URL");
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        std::env::remove_var("AWS_DEFAULT_REGION");
        std::env::remove_var("AWS_CA_BUNDLE");
        std::env::remove_var("NEW_RELIC_LICENSE_KEY_SECRET");
        std::env::remove_var("NEW_RELIC_LICENSE_KEY_SSM_PARAMETER_NAME");
    }

    /// Full code path: AWS_LAMBDA_RUNTIME_API set, env credentials present,
    /// NEW_RELIC_LICENSE_KEY_SECRET env var set, wiremock returns a valid
    /// Secrets Manager GetSecretValue response.
    ///
    /// Covers (via the call chain):
    ///   initialize_aws_clients success path, get_aws_clients OnceLock-hit path,
    ///   try_license_key_from_secret, decode_license_key success path,
    ///   DefaultSecretsManager::new, DefaultSecretsManager::get_secret_value,
    ///   get_new_relic_license_key ENV_LICENSE_KEY_SECRET branch.
    #[tokio::test]
    #[serial]
    async fn test_get_license_key_from_secrets_manager_via_env_var() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock = MockServer::start().await;
        // AWS SDK sends a POST to the overridden endpoint for GetSecretValue.
        // Wiremock accepts any POST and returns a minimal valid response.
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "application/x-amz-json-1.1")
                    .set_body_string(
                        r#"{"SecretString":"{\"LicenseKey\":\"test-license-key-12345\"}"}"#,
                    ),
            )
            .mount(&mock)
            .await;

        set_aws_credentials_env(&mock.uri());
        std::env::set_var("NEW_RELIC_LICENSE_KEY_SECRET", "my-test-secret-id");

        let conf = crate::config::Configuration {
            license_key: String::new(),
            license_key_secret_id: String::new(),
            license_key_ssm_parameter_name: String::new(),
        };

        let result = super::super::get_new_relic_license_key(&conf).await;
        clear_aws_credentials_env();

        assert!(
            result.is_ok(),
            "expected Ok(license_key)"
        );
        assert_eq!(result.unwrap(), "test-license-key-12345");
    }
}
