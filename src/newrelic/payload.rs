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

#[derive(Debug, Clone, Serialize)]
pub struct LogMessage {
    pub timestamp: i64,
    pub message: String,
    pub attributes: Map<String, serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_message_serialization() {
        let mut attrs = Map::new();
        attrs.insert("level".to_string(), serde_json::json!("INFO"));

        let msg = LogMessage {
            timestamp: 1700000000000,
            message: "test log message".to_string(),
            attributes: attrs,
        };

        let json = serde_json::to_value(&msg).expect("should serialize");
        assert_eq!(json["timestamp"], 1700000000000_i64);
        assert_eq!(json["message"], "test log message");
        assert_eq!(json["attributes"]["level"], "INFO");
    }

    #[test]
    fn test_log_payload_serialization() {
        let mut common_attrs = Map::new();
        common_attrs.insert("plugin".to_string(), serde_json::json!("test-plugin"));

        let payload = LogPayload {
            common: Common {
                attributes: common_attrs,
            },
            logs: vec![LogMessage {
                timestamp: 1700000000000,
                message: "hello".to_string(),
                attributes: Map::new(),
            }],
        };

        let json = serde_json::to_value(&payload).expect("should serialize");
        assert_eq!(json["common"]["attributes"]["plugin"], "test-plugin");
        assert!(json["logs"].is_array());
        assert_eq!(json["logs"][0]["message"], "hello");
    }

    #[test]
    fn test_log_message_clone() {
        let msg = LogMessage {
            timestamp: 100,
            message: "clone me".to_string(),
            attributes: Map::new(),
        };
        let cloned = msg.clone();
        assert_eq!(cloned.timestamp, 100);
        assert_eq!(cloned.message, "clone me");
    }
}
