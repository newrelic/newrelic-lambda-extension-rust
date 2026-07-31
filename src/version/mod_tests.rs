// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn test_version_info_creation() {
    let version_info = VersionInfo {
        agent_version: Some("9.5.0".to_string()),
        agent_name: Some("python".to_string()),
        extension_version: "0.1.0".to_string(),
        layer_version: Some("NewRelicPython313X86:93".to_string()),
        runtime_version: None,
    };

    let tags = version_info.as_tags();
    assert!(tags.len() >= 2);
}

#[test]
fn user_agent_tracks_cargo_version() {
    let ua = user_agent();
    // Must carry the real crate version, never the old hardcoded placeholder.
    assert_eq!(ua, format!("NewRelic-Rust-Lambda-Extension/{}", env!("CARGO_PKG_VERSION")));
    assert!(ua.contains(env!("CARGO_PKG_VERSION")));
    assert_ne!(ua, "NewRelic-Rust-Lambda-Extension/0.1.0");
}
