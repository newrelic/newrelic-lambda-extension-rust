// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use serde::Serialize;
use serde_json::Map;

#[derive(Debug, Clone, Serialize)]
pub struct LogMessage {
    pub timestamp: i64,
    pub message: String,
    pub attributes: Map<String, serde_json::Value>,
}
