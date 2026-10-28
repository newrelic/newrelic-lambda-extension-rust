// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use serial_test::serial;

async fn clear() {
    let mut guard = NORMAL_LAMBDA_LOG_STREAM.write().await;
    *guard = None;
}

#[tokio::test]
#[serial]
async fn try_read_returns_none_before_capture() {
    clear().await;
    assert_eq!(try_read(), None);
}

#[tokio::test]
#[serial]
async fn try_read_returns_captured_value() {
    clear().await;
    {
        let mut guard = NORMAL_LAMBDA_LOG_STREAM.write().await;
        *guard = Some("2026/09/08/[$LATEST]abc123".to_string());
    }
    assert_eq!(try_read(), Some("2026/09/08/[$LATEST]abc123".to_string()));
    clear().await;
}
