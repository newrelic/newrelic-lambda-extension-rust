// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Automatic fallback from post-quantum to classical-only TLS key exchange.
//!
//! Some TLS-inspecting middleboxes (seen: AWS Network Firewall doing
//! `SNI`/`HTTP_HOST` matching) hang on the larger/fragmented `ClientHello`
//! that `aws-lc-rs`'s default post-quantum hybrid key-exchange group
//! produces (NR-617743). Rather than requiring an opt-in environment
//! variable, the extension tries post-quantum TLS first — today's default,
//! byte-identical for every customer who never hits this — and falls back
//! to a classical-only `rustls` `ClientConfig` automatically, the first time
//! a send exhibits the specific failure signature this module exists to
//! catch: TCP connects, the `ClientHello` is sent, then silence until our
//! own client-side timeout fires (`reqwest::Error::is_connect()` or
//! `is_timeout()`). An HTTP-status error (4xx/5xx) is never a trigger — that
//! case is handled entirely separately, by `SendError::ClientRejected`/
//! `ServerExhausted`.
//!
//! The fallback is a one-way, process-global latch: once flipped, every
//! subsequent send in this warm execution environment uses the classical
//! client, for the rest of that environment's life — no repeated timeout
//! cost per invocation. A fresh cold start begins again assuming
//! post-quantum TLS works.

use rustls::crypto::aws_lc_rs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{debug, warn};

/// Sticky, process-global: once true, every [`DualTlsClient::active`] call in
/// this warm execution environment returns the classical-only client. Never
/// reset within a process's life.
static USE_CLASSICAL_TLS: AtomicBool = AtomicBool::new(false);

/// Builds the classical-only `rustls::ClientConfig`: the same TLS protocol
/// versions and certificate verification reqwest's own default `"rustls"`
/// feature path uses (`rustls_platform_verifier`, mirroring the
/// `TlsBackend::Rustls` branch in `reqwest::async_impl::client`), but with
/// `kx_groups` restricted to `[X25519, SECP256R1, SECP384R1]` — no
/// post-quantum/hybrid group offered in the `ClientHello`.
fn build_classical_client_config() -> Result<rustls::ClientConfig, String> {
    let provider = Arc::new(rustls::crypto::CryptoProvider {
        kx_groups: vec![
            aws_lc_rs::kx_group::X25519,
            aws_lc_rs::kx_group::SECP256R1,
            aws_lc_rs::kx_group::SECP384R1,
        ],
        ..aws_lc_rs::default_provider()
    });

    let verifier = rustls_platform_verifier::Verifier::new(provider.clone())
        .map_err(|e| format!("failed to build platform certificate verifier: {e}"))?;

    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("invalid TLS protocol versions: {e}"))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();

    Ok(config)
}

/// Classifies a failed send: returns `true` only for the connect/handshake-
/// hang signature this fallback exists to work around. On a true positive,
/// flips [`USE_CLASSICAL_TLS`] (idempotent — a no-op if already flipped) and
/// logs once per flip, not once per call.
pub fn classify_and_maybe_flip(err: &reqwest::Error) -> bool {
    if !(err.is_connect() || err.is_timeout()) {
        return false;
    }

    if USE_CLASSICAL_TLS.swap(true, Ordering::Relaxed) {
        return true;
    }

    debug!(
        "Outbound send to {} failed to connect/complete its TLS handshake ({err}) — switching \
         to classical-only TLS key exchange (no post-quantum hybrid group) for the rest of this \
         execution environment's life (NR-617743)",
        err.url().map_or("<unknown>", |u| u.as_str()),
    );
    true
}

/// Two pre-built `reqwest::Client`s sharing identical non-TLS settings
/// (headers, timeouts, proxy) — `primary` uses reqwest's untouched default
/// TLS setup (post-quantum enabled, byte-identical to before this module
/// existed), `classical` uses the override above. [`active`](Self::active)
/// picks between them based on the sticky flag, re-read fresh on every call
/// so a retry loop within a single invocation picks up a flip made by an
/// earlier attempt in that same loop.
#[derive(Debug, Clone)]
pub struct DualTlsClient {
    primary: reqwest::Client,
    classical: reqwest::Client,
}

impl DualTlsClient {
    /// Builds both clients from a builder factory (non-TLS settings only —
    /// headers/timeouts/proxy — invoked twice since `reqwest::ClientBuilder`
    /// doesn't implement `Clone`). If the classical config fails to build
    /// (should not happen in practice), falls back to using the primary
    /// client for both, logging a warning — the extension still works, it
    /// just won't have this specific fallback available in that execution
    /// environment.
    pub fn build(make_base: impl Fn() -> reqwest::ClientBuilder) -> Self {
        let primary = make_base()
            .build()
            .expect("failed to build primary (post-quantum) outbound HTTP client");

        let classical = match build_classical_client_config() {
            Ok(tls_config) => make_base()
                .tls_backend_preconfigured(tls_config)
                .build()
                .expect("failed to build classical-only outbound HTTP client"),
            Err(e) => {
                warn!(
                    "Failed to build classical-only TLS fallback client ({e}) — the NR-617743 \
                     post-quantum TLS fallback will not be available for this execution \
                     environment; outbound sends are otherwise unaffected"
                );
                primary.clone()
            }
        };

        Self { primary, classical }
    }

    /// Returns the client to use for the next send, based on the current
    /// (process-global, sticky) fallback state.
    pub fn active(&self) -> &reqwest::Client {
        if USE_CLASSICAL_TLS.load(Ordering::Relaxed) {
            &self.classical
        } else {
            &self.primary
        }
    }
}

#[cfg(test)]
pub(crate) fn reset_for_test() {
    USE_CLASSICAL_TLS.store(false, Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) fn is_classical_tls_active_for_test() -> bool {
    USE_CLASSICAL_TLS.load(Ordering::Relaxed)
}

/// Cheap `DualTlsClient` for tests elsewhere in the crate that just need a
/// well-typed client to satisfy a signature (e.g. `ApmApp`/`NewRelicClient`
/// test fixtures) — not for exercising the fallback mechanism itself.
#[cfg(test)]
pub(crate) fn test_client() -> DualTlsClient {
    DualTlsClient::build(reqwest::Client::builder)
}

/// Not run by default (`cargo test`) — requires real outbound internet access
/// and hits a live third-party endpoint, which this crate's test suite
/// otherwise never does (everything else uses `wiremock` locally). Exists to
/// answer one specific question that no local mock can: does the
/// classical-only `ClientConfig` (custom `CryptoProvider` + hand-wired
/// `rustls_platform_verifier`) actually complete a real TLS handshake and
/// certificate verification against a real endpoint, or did something in
/// its manual construction break normal connectivity for the 100% of
/// customers who'd only ever see this path invoked for real after a flip.
/// Run on demand: `cargo test --lib tls_fallback -- --ignored`.
#[cfg(test)]
#[ignore = "requires real outbound internet access; run on demand, not in CI"]
#[tokio::test]
async fn classical_only_client_completes_a_real_tls_handshake() {
    let client = DualTlsClient::build(reqwest::Client::builder);
    let response = client
        .classical
        .get("https://collector.newrelic.com/")
        .send()
        .await
        .expect(
            "classical-only TLS config must be able to complete a real handshake \
             against a real endpoint — any HTTP response (even non-2xx) proves \
             TLS + cert verification succeeded",
        );
    // Any status at all (404 is expected for a bare GET to this path) proves the
    // full TLS handshake and certificate chain verification completed — that's
    // the only thing this test is checking.
    let _ = response.status();
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn classical_kx_groups_exclude_all_hybrid_pq_groups() {
        let config = build_classical_client_config().expect("classical config should build");
        let classical_groups: Vec<rustls::NamedGroup> =
            config.crypto_provider().kx_groups.iter().map(|g| g.name()).collect();

        let forbidden_hybrid_groups = [
            aws_lc_rs::kx_group::X25519MLKEM768.name(),
            aws_lc_rs::kx_group::SECP256R1MLKEM768.name(),
            aws_lc_rs::kx_group::MLKEM768.name(),
        ];
        for hybrid in forbidden_hybrid_groups {
            assert!(
                !classical_groups.contains(&hybrid),
                "classical-only kx_groups must never contain the hybrid/post-quantum group {hybrid:?}"
            );
        }
    }

    #[test]
    #[serial]
    fn active_returns_primary_before_any_flip() {
        reset_for_test();
        let client = DualTlsClient::build(reqwest::Client::builder);
        assert!(std::ptr::eq(client.active(), &raw const client.primary));
    }

    #[test]
    #[serial]
    fn active_returns_classical_after_flip() {
        reset_for_test();
        let client = DualTlsClient::build(reqwest::Client::builder);
        USE_CLASSICAL_TLS.store(true, Ordering::Relaxed);
        assert!(std::ptr::eq(client.active(), &raw const client.classical));
        reset_for_test();
    }

    #[tokio::test]
    #[serial]
    async fn classify_and_maybe_flip_ignores_non_network_errors() {
        reset_for_test();
        // A successful request against an httpbin-style 404 is an HTTP-status
        // outcome (Ok(response), not Err(reqwest::Error)) — classify_and_maybe_flip
        // only ever sees an Err at all for genuine connect/timeout failures,
        // so there's nothing to "ignore" at the type level for HTTP statuses.
        // This test instead confirms a connect-refused error IS classified.
        let err = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_millis(50))
            .build()
            .expect("client builds")
            .get("http://127.0.0.1:1") // nothing listens here — connection refused/timeout
            .send()
            .await
            .expect_err("connecting to a closed port must fail");

        let flipped = classify_and_maybe_flip(&err);
        assert!(flipped, "a connect-level failure must be classified as fallback-triggering");
        assert!(USE_CLASSICAL_TLS.load(Ordering::Relaxed));
        reset_for_test();
    }

    /// Faithfully reproduces the NR-617743 failure *signature* at the TCP
    /// level: the connection is accepted (unlike `is_connect()` failures,
    /// where it's refused outright), but the server then sends nothing back
    /// at all — the same "`ClientHello` goes out, then silence" shape as a
    /// middlebox swallowing the post-quantum `ClientHello`, just without real
    /// TLS involved (a local test can't engage real TLS/firewall behavior;
    /// see the module doc). This specifically exercises `is_timeout()`,
    /// which `classify_and_maybe_flip_ignores_non_network_errors` above does
    /// not — that test only covers the immediate-refusal (`is_connect()`)
    /// case, a different reqwest error path.
    #[tokio::test]
    #[serial]
    async fn classify_and_maybe_flip_detects_connect_then_silence_timeout() {
        reset_for_test();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind an ephemeral port");
        let addr = listener.local_addr().expect("listener has a local addr");

        // Accept the connection and then just hold it open, sending nothing —
        // the client's request will hang until its own timeout fires.
        tokio::spawn(async move {
            if let Ok((socket, _)) = listener.accept().await {
                // Keep the socket alive (don't drop it, which would send a RST/FIN)
                // for longer than the client's timeout below.
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                drop(socket);
            }
        });

        let err = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(100))
            .build()
            .expect("client builds")
            .get(format!("http://{addr}"))
            .send()
            .await
            .expect_err("a server that accepts and never responds must time out");

        assert!(err.is_timeout(), "expected a timeout error, got: {err:?}");

        let flipped = classify_and_maybe_flip(&err);
        assert!(
            flipped,
            "connect-then-silence-then-timeout is exactly the signature this fallback exists to catch"
        );
        assert!(USE_CLASSICAL_TLS.load(Ordering::Relaxed));
        reset_for_test();
    }

    #[tokio::test]
    #[serial]
    async fn classify_and_maybe_flip_ignores_decode_errors() {
        use wiremock::{Mock, MockServer, ResponseTemplate};

        reset_for_test();

        let mock = MockServer::start().await;
        Mock::given(wiremock::matchers::method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&mock)
            .await;

        let err = reqwest::Client::new()
            .get(mock.uri())
            .send()
            .await
            .expect("mock server responds")
            .json::<serde_json::Value>()
            .await
            .expect_err("body is not valid JSON");

        let flipped = classify_and_maybe_flip(&err);
        assert!(!flipped, "a body-decode error is not the connect/timeout hang signature");
        assert!(!USE_CLASSICAL_TLS.load(Ordering::Relaxed));
    }
}
