// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod tests {
    use crate::platform::processor::normalize_platform_runtime_version;

    #[test]
    fn test_normalize_platform_runtime_version() {
        // Node.js - use .x suffix
        assert_eq!(normalize_platform_runtime_version("nodejs:18.v98"), "nodejs18.x");
        assert_eq!(normalize_platform_runtime_version("nodejs:20.v15"), "nodejs20.x");
        assert_eq!(normalize_platform_runtime_version("nodejs:22.v2"), "nodejs22.x");

        // Python - keep major.minor
        assert_eq!(normalize_platform_runtime_version("python:3.13"), "python3.13");
        assert_eq!(normalize_platform_runtime_version("python:3.12.5"), "python3.12");

        // Ruby - keep major.minor
        assert_eq!(normalize_platform_runtime_version("ruby:3.3"), "ruby3.3");
        assert_eq!(normalize_platform_runtime_version("ruby:3.2.0"), "ruby3.2");

        // Java - keep major only
        assert_eq!(normalize_platform_runtime_version("java:17"), "java17");
        assert_eq!(normalize_platform_runtime_version("java:21"), "java21");

        // .NET - keep major only
        assert_eq!(normalize_platform_runtime_version("dotnet:8"), "dotnet8");
        assert_eq!(normalize_platform_runtime_version("dotnet:6"), "dotnet6");

        // No colon - return as-is
        assert_eq!(normalize_platform_runtime_version("unknown"), "unknown");
        assert_eq!(normalize_platform_runtime_version("go1.x"), "go1.x");
    }
}

/// NR-579360: platform.report -> log line under the stripped LMI report.
/// LMI omits billedDurationMs / memorySizeMB / maxMemoryUsedMB; the report must
/// still produce a REPORT line (not be dropped as "missing required fields").
/// Standard Lambda keeps the strict all-fields-required behavior verbatim.
#[cfg(test)]
mod report_log_line_tests {
    use crate::platform::processor::PlatformProcessor;
    use crate::config::deployment::{DeploymentContext, TelemetryMode};
    use crate::config::ExtensionConfig;
    use crate::context::InvocationContext;
    use crate::newrelic::client::NewRelicClient;
    use crate::logs::processor::LogProcessor;
    use crate::telemetry::listener::TelemetryRecord;
    use std::sync::{Arc, Mutex};

    fn processor(deployment: DeploymentContext) -> PlatformProcessor {
        let mut config = ExtensionConfig::default();
        config.deployment = deployment;
        let config = Arc::new(config);
        let client = Arc::new(NewRelicClient::new(&config));
        let ctx = Arc::new(Mutex::new(InvocationContext::default()));
        let log_processor = Arc::new(LogProcessor::new(
            Arc::clone(&client), Arc::clone(&config), Arc::clone(&ctx), None,
        ));
        PlatformProcessor::new(client, config, ctx, log_processor)
    }

    fn report_record(metrics: serde_json::Value) -> TelemetryRecord {
        TelemetryRecord {
            time: chrono::DateTime::from_timestamp(0, 0).expect("epoch"),
            record_type: "platform.report".to_string(),
            record: serde_json::json!({ "requestId": "req-1", "metrics": metrics }),
        }
    }

    // LMI: durationMs only (the stripped report) must still yield a REPORT line.
    #[test]
    fn lmi_report_with_only_duration_produces_line() {
        let p = processor(DeploymentContext::Lmi);
        let line = p
            .convert_platform_report_to_log_line(&report_record(serde_json::json!({ "durationMs": 140.0 })))
            .expect("LMI report with durationMs should still format");
        assert_eq!(line, "REPORT RequestId: req-1\tDuration: 140.00 ms");
        assert!(!line.contains("Billed Duration"));
        assert!(!line.contains("Memory Size"));
    }

    // LMI: any optional fields that ARE present get appended.
    #[test]
    fn lmi_report_appends_present_optional_fields() {
        let p = processor(DeploymentContext::Lmi);
        let line = p
            .convert_platform_report_to_log_line(&report_record(
                serde_json::json!({ "durationMs": 12.5, "maxMemoryUsedMB": 84 }),
            ))
            .expect("should format");
        assert_eq!(line, "REPORT RequestId: req-1\tDuration: 12.50 ms\tMax Memory Used: 84 MB");
    }

    // Normal: full metric set -> byte-identical to the original strict format.
    #[test]
    fn normal_report_full_is_unchanged() {
        let p = processor(DeploymentContext::Normal { mode: TelemetryMode::Serverless });
        let line = p
            .convert_platform_report_to_log_line(&report_record(serde_json::json!({
                "durationMs": 693.92, "billedDurationMs": 694, "memorySizeMB": 128,
                "maxMemoryUsedMB": 84, "initDurationMs": 397.68
            })))
            .expect("should format");
        assert_eq!(
            line,
            "REPORT RequestId: req-1\tDuration: 693.92 ms\tBilled Duration: 694 ms\tMemory Size: 128 MB\tMax Memory Used: 84 MB\tInit Duration: 397.68 ms"
        );
    }

    // Normal regression: a report missing the billed/memory fields still returns None,
    // exactly as before this change (strict behavior preserved for Standard Lambda).
    #[test]
    fn normal_report_missing_fields_returns_none() {
        let p = processor(DeploymentContext::Normal { mode: TelemetryMode::Apm });
        assert!(p
            .convert_platform_report_to_log_line(&report_record(serde_json::json!({ "durationMs": 140.0 })))
            .is_none());
    }
}
