// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Lambda Telemetry API: schema versions.
//!
//! Two schema versions are supported. The newer `2025-01-29` adds the
//! `hostGroup` field on `platform.initStart` records and is required to
//! capture full host-level metadata for Lambda Managed Instances. The older
//! `2022-07-01` is the universal fallback for Standard Lambda runtimes that
//! AWS has not yet upgraded to accept the newer schema (those return HTTP
//! 400 to a `2025-01-29` subscription).
//!
//! Schema choice is independent of [`crate::config::deployment::DeploymentContext`]
//! — the extension always tries `2025-01-29` first and falls back only when
//! AWS rejects it. Tying the choice to the deployment context would conflate
//! two orthogonal axes (see `LMI_SUPPORT.md` §6).

/// Lambda Telemetry API schema version.
///
/// Each variant captures both the URL path segment AWS expects on `PUT
/// /{schema}/telemetry` and the matching `schemaVersion` field in the
/// subscription body — they always move together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetrySchema {
    /// Preferred. Adds `hostGroup` on `platform.initStart` records (LMI-only field).
    V2025_01_29,
    /// Legacy fallback. Used when AWS returns HTTP 400 on the newer schema —
    /// Standard Lambda runtimes that have not yet been upgraded.
    V2022_07_01,
}

impl TelemetrySchema {
    /// URL path segment, e.g. `"2025-01-29"`. Used to build the PUT URL.
    #[must_use]
    pub fn url_segment(self) -> &'static str {
        match self {
            Self::V2025_01_29 => "2025-01-29",
            Self::V2022_07_01 => "2022-07-01",
        }
    }

    /// Value for the `schemaVersion` field in the subscription request body.
    /// Always identical to [`Self::url_segment`] — kept as a separate accessor
    /// so a future schema with a divergent body field is a one-line change.
    #[must_use]
    pub fn schema_version(self) -> &'static str {
        match self {
            Self::V2025_01_29 => "2025-01-29",
            Self::V2022_07_01 => "2022-07-01",
        }
    }

    /// Human-readable label for log lines.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::V2025_01_29 => "2025-01-29",
            Self::V2022_07_01 => "2022-07-01",
        }
    }
}

/// Typed errors from `subscribe_to_telemetry`.
///
/// Replaces the previous `Box<dyn std::error::Error + Send + Sync>` per the
/// project's Zero-Panic policy (CLAUDE.md §2): callers can pattern-match on
/// `Rejected { status: 400, .. }` to drive the schema fallback without
/// resorting to string matching on error messages.
#[derive(Debug)]
pub enum TelemetrySubscriptionError {
    /// `AWS_LAMBDA_RUNTIME_API` env var not set — extension launched outside
    /// of the Lambda Extensions environment.
    MissingRuntimeApi,
    /// Network/IO failure talking to the runtime API.
    Transport(reqwest::Error),
    /// Runtime API returned non-2xx. `status` is the HTTP code, `body` is the
    /// raw response payload (truncated by reqwest if very large), `schema` is
    /// which schema we attempted (so the caller can decide whether to fall
    /// back).
    Rejected {
        status: u16,
        body: String,
        schema: TelemetrySchema,
    },
}

impl std::fmt::Display for TelemetrySubscriptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingRuntimeApi => write!(f, "AWS_LAMBDA_RUNTIME_API not set"),
            Self::Transport(e) => write!(f, "HTTP transport failure: {e}"),
            Self::Rejected {
                status,
                body,
                schema,
            } => write!(
                f,
                "Telemetry subscription rejected: schema={} status={status} body={body}",
                schema.name()
            ),
        }
    }
}

impl std::error::Error for TelemetrySubscriptionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_segment_and_schema_version_match() {
        for schema in [TelemetrySchema::V2025_01_29, TelemetrySchema::V2022_07_01] {
            assert_eq!(schema.url_segment(), schema.schema_version());
            assert_eq!(schema.url_segment(), schema.name());
        }
    }

    #[test]
    fn schemas_have_distinct_versions() {
        assert_ne!(
            TelemetrySchema::V2025_01_29.url_segment(),
            TelemetrySchema::V2022_07_01.url_segment()
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
}
