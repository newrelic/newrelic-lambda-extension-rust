use serde::Serialize;
use serde_json::Map;

#[derive(Debug, Serialize)]
pub struct LogPayload {
    pub common: Common,
    pub logs: Vec<LogMessage>,
}

#[derive(Debug, Serialize)]
pub struct Common {
    pub attributes: Map<String, serde_json::Value>,
}

/// Tracks the origin of a log message for smart retry decisions.
/// Function logs (customer data) are always retried on failure.
/// Extension logs are dropped on failure (except ERROR level) to save memory.
#[derive(Debug, Clone, PartialEq)]
pub enum LogSource {
    Function,
    Extension,
    Platform,
    Unknown,
}

#[derive(Debug, Clone, Serialize)]
pub struct LogMessage {
    pub timestamp: i64,
    pub message: String,
    pub attributes: Map<String, serde_json::Value>,
    /// Internal tracking only — not sent to New Relic
    #[serde(skip)]
    pub log_source: LogSource,
}

