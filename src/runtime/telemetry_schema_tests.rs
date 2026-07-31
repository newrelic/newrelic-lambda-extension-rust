// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn schema_version_matches_name() {
    for schema in [TelemetrySchema::V2025_01_29, TelemetrySchema::V2022_07_01] {
        assert_eq!(schema.schema_version(), schema.name());
    }
}

#[test]
fn schemas_have_distinct_versions() {
    assert_ne!(
        TelemetrySchema::V2025_01_29.schema_version(),
        TelemetrySchema::V2022_07_01.schema_version()
    );
}

#[test]
fn body_schema_version_is_distinct_from_api_path_version() {
    // Regression guard for the LMI subscription `404 page not found`: the
    // `2025-01-29` schema belongs in the request body, never in the URL
    // path. The path stays on the fixed Telemetry API endpoint version.
    assert_eq!(TelemetrySchema::V2025_01_29.schema_version(), "2025-01-29");
    assert_ne!(
        crate::runtime::TELEMETRY_API_VERSION,
        TelemetrySchema::V2025_01_29.schema_version(),
        "preferred body schema must differ from the fixed API path version"
    );
}

#[test]
fn rejected_display_includes_schema_status_and_body() {
    let err = TelemetrySubscriptionError::Rejected {
        status: 400,
        body: "Schema not supported".to_string(),
        schema: TelemetrySchema::V2025_01_29,
    };
    let msg = format!("{err}");
    assert!(msg.contains("2025-01-29"));
    assert!(msg.contains("status=400"));
    assert!(msg.contains("Schema not supported"));
}

#[test]
fn missing_runtime_api_display_is_actionable() {
    let err = TelemetrySubscriptionError::MissingRuntimeApi;
    assert!(format!("{err}").contains("AWS_LAMBDA_RUNTIME_API"));
}

#[test]
fn rejected_5xx_and_429_are_retryable() {
    for status in [500, 502, 503, 504, 429] {
        let err = TelemetrySubscriptionError::Rejected {
            status,
            body: String::new(),
            schema: TelemetrySchema::V2025_01_29,
        };
        assert!(err.is_retryable(), "status {status} should be retryable");
    }
}

#[test]
fn rejected_400_and_404_are_terminal_for_retry() {
    // 400/404 must NOT be retried — they are the schema-fallback signal that
    // `subscribe_to_telemetry` needs to see (V2025 → V2022).
    for status in [400, 401, 403, 404, 409, 422] {
        let err = TelemetrySubscriptionError::Rejected {
            status,
            body: String::new(),
            schema: TelemetrySchema::V2025_01_29,
        };
        assert!(
            !err.is_retryable(),
            "status {status} must be terminal so the fallback path can act"
        );
    }
}

#[test]
fn missing_runtime_api_is_terminal_for_retry() {
    assert!(!TelemetrySubscriptionError::MissingRuntimeApi.is_retryable());
}
