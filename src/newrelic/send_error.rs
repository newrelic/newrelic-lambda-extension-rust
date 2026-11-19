// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Spells out why an HTTP send failed.
//!
//! `reqwest::Error`'s own `Display` is just "error sending request": the real cause (refused,
//! reset, closed mid-request, timeout, TLS) sits behind it in the error chain. This prints the
//! kind flags, the whole chain and the OS error code, so a warning says what actually happened.
//! The request URL is never printed: the collector URL carries the license key.

use std::error::Error;

/// What a failed reqwest send was, in one line:
/// `kind=[connect] chain=[error sending request -> client error (Connect) -> ...] io=ConnectionReset os_error=104`.
pub fn describe_reqwest_error(err: &reqwest::Error) -> String {
    let url = err.url().map(ToString::to_string);
    let scrub = |text: String| match &url {
        Some(u) => text.replace(u.as_str(), "<url>"),
        None => text,
    };

    let mut kinds: Vec<String> = Vec::new();
    for (flag, name) in [
        (err.is_timeout(), "timeout"),
        (err.is_connect(), "connect"),
        (err.is_request(), "request"),
        (err.is_body(), "body"),
        (err.is_decode(), "decode"),
        (err.is_builder(), "builder"),
        (err.is_redirect(), "redirect"),
    ] {
        if flag {
            kinds.push(name.to_string());
        }
    }
    if let Some(status) = err.status() {
        kinds.push(format!("status {}", status.as_u16()));
    }

    let mut chain = vec![scrub(err.to_string())];
    let mut io_detail = None;
    let mut source = err.source();
    while let Some(cause) = source {
        chain.push(scrub(cause.to_string()));
        if io_detail.is_none() {
            if let Some(io) = cause.downcast_ref::<std::io::Error>() {
                io_detail = Some(format!(
                    "io={:?} os_error={}",
                    io.kind(),
                    io.raw_os_error().map_or_else(|| "none".to_string(), |code| code.to_string())
                ));
            }
        }
        source = cause.source();
    }

    let mut out = format!("kind=[{}] chain=[{}]", kinds.join(","), chain.join(" -> "));
    if let Some(io) = io_detail {
        out.push(' ');
        out.push_str(&io);
    }
    out
}

/// Same for an `anyhow::Error` that may wrap a reqwest error (the collector send path); any
/// other error prints with its full context chain.
pub fn describe_anyhow(err: &anyhow::Error) -> String {
    match err.chain().find_map(|cause| cause.downcast_ref::<reqwest::Error>()) {
        Some(reqwest_err) => describe_reqwest_error(reqwest_err),
        None => format!("{err:#}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// A port nothing listens on, so connecting is refused.
    fn refused_url(path: &str) -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        format!("http://127.0.0.1:{port}{path}")
    }

    #[tokio::test]
    async fn a_refused_connection_says_connect_and_gives_the_os_error() {
        let err = reqwest::Client::new().get(refused_url("/")).send().await.unwrap_err();

        let text = describe_reqwest_error(&err);

        assert!(text.contains("connect"), "kind flag missing: {text}");
        assert!(text.contains("ConnectionRefused"), "io kind missing: {text}");
        assert!(text.contains("os_error="), "os error code missing: {text}");
        assert!(text.contains("->"), "the cause chain is missing: {text}");
    }

    #[tokio::test]
    async fn a_timeout_says_timeout() {
        // Accepts the connection but never answers.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _held = listener.accept().await;
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let client = reqwest::Client::builder().timeout(Duration::from_millis(100)).build().unwrap();

        let err = client.get(format!("http://127.0.0.1:{port}/")).send().await.unwrap_err();

        assert!(describe_reqwest_error(&err).contains("timeout"), "{}", describe_reqwest_error(&err));
    }

    #[tokio::test]
    async fn the_url_and_the_license_key_in_it_are_never_printed() {
        let err = reqwest::Client::new()
            .get(refused_url("/agent_listener?license_key=SECRETKEY123&run_id=abc"))
            .send()
            .await
            .unwrap_err();

        let text = describe_reqwest_error(&err);

        assert!(!text.contains("SECRETKEY123"), "license key leaked: {text}");
        assert!(!text.contains("127.0.0.1"), "url leaked: {text}");
    }

    #[tokio::test]
    async fn an_anyhow_wrapping_reqwest_is_described_like_reqwest() {
        let err = reqwest::Client::new().get(refused_url("/")).send().await.unwrap_err();
        let wrapped = anyhow::Error::new(err).context("sending metric_data");

        let text = describe_anyhow(&wrapped);

        assert!(text.contains("ConnectionRefused") && text.contains("kind=[connect"), "{text}");
    }

    #[test]
    fn any_other_error_prints_with_its_whole_context() {
        let err = anyhow::anyhow!("inner cause").context("outer step");

        assert_eq!(describe_anyhow(&err), "outer step: inner cause");
    }
}
