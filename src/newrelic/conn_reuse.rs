// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Debug log of whether each successful send reused a pooled connection or opened a new one.
//! A connection is identified by its (local, remote) address pair, which reqwest exposes on
//! the response. Sends that fail or time out have no response, so they are not counted.

use hyper_util::client::legacy::connect::HttpInfo;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Mutex;
use tracing::debug;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum ConnUse {
    New,
    Reused,
    Unknown,
}

/// Connections seen so far. shortcut: cleared at 256 entries, so one connection can be
/// reported as new again after that; upgrade if the counts must be exact.
static SEEN: Mutex<Option<HashSet<(SocketAddr, SocketAddr)>>> = Mutex::new(None);
const SEEN_MAX: usize = 256;

pub(crate) fn note_connection(label: &str, response: &reqwest::Response) -> ConnUse {
    let Some(info) = response.extensions().get::<HttpInfo>() else {
        return ConnUse::Unknown;
    };
    let conn = (info.local_addr(), info.remote_addr());
    let mut guard = SEEN.lock().unwrap_or_else(|p| p.into_inner());
    let seen = guard.get_or_insert_with(HashSet::new);
    if seen.len() >= SEEN_MAX {
        seen.clear();
    }
    let used = if seen.insert(conn) { ConnUse::New } else { ConnUse::Reused };
    debug!("[conn] {label}: {used:?} local={} remote={}", conn.0, conn.1);
    used
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::any;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn server() -> MockServer {
        let s = MockServer::start().await;
        Mock::given(any()).respond_with(ResponseTemplate::new(200)).mount(&s).await;
        s
    }

    #[tokio::test]
    async fn second_send_on_the_same_client_reuses_the_connection() {
        let s = server().await;
        let client = reqwest::Client::new();
        let first = client.get(s.uri()).send().await.unwrap();
        assert_eq!(note_connection("t", &first), ConnUse::New);
        drop(first.bytes().await.unwrap());
        let second = client.get(s.uri()).send().await.unwrap();
        assert_eq!(note_connection("t", &second), ConnUse::Reused);
    }

    #[tokio::test]
    async fn a_new_client_opens_a_new_connection() {
        let s = server().await;
        let a = reqwest::Client::new().get(s.uri()).send().await.unwrap();
        let b = reqwest::Client::new().get(s.uri()).send().await.unwrap();
        assert_eq!(note_connection("t", &a), ConnUse::New);
        assert_eq!(note_connection("t", &b), ConnUse::New);
    }
}
