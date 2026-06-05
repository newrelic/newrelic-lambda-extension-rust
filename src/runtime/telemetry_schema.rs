// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Lambda Telemetry API: event schema versions.
//!
//! The Telemetry API has **two independent version axes**, and conflating them
//! routes the subscription to a non-existent path (`404 page not found`):
//!
//! - **API endpoint version** — the fixed segment in the subscription URL
//!   (`PUT /2022-07-01/telemetry`). It identifies the API itself and never
//!   varies, exactly like the `2020-01-01` Extensions API path used for
//!   registration and event polling. It lives as
//!   [`crate::runtime::TELEMETRY_API_VERSION`], not here.
//! - **Event schema version** — the `schemaVersion` field in the subscription
//!   request *body*, modelled by this enum. The newer `2025-01-29` carries the
//!   host-level metadata AWS surfaces for Lambda Managed Instances; the older
//!   `2022-07-01` is the universal fallback for Standard Lambda runtimes AWS
//!   has not yet upgraded (those reject a `2025-01-29` body with HTTP 400).
//!
//! Schema choice is independent of [`crate::config::deployment::DeploymentContext`]
//! — the extension always tries `2025-01-29` first and falls back only when
//! AWS rejects the body. Tying the choice to the deployment context would
//! conflate two orthogonal axes (see `LMI_SUPPORT.md` §6).

/// Lambda Telemetry API event schema version — the `schemaVersion` field in
/// the subscription request *body*.
///
/// This is **not** the URL path version: the subscription always PUTs to the
/// fixed [`crate::runtime::TELEMETRY_API_VERSION`] endpoint regardless of which
/// schema variant is sent in the body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetrySchema {
    /// Preferred. Carries the host-level metadata AWS surfaces for Lambda
    /// Managed Instances on `platform.initStart`.
    V2025_01_29,
    /// Legacy fallback. Used when AWS returns HTTP 400 on the newer schema —
    /// Standard Lambda runtimes that have not yet been upgraded.
    V2022_07_01,
}

impl TelemetrySchema {
    /// Value for the `schemaVersion` field in the subscription request body.
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

impl TelemetrySubscriptionError {
    /// Whether this failure is transient and worth retrying (see
    /// [`crate::runtime::retry`]).
    ///
    /// Transport failures and `5xx`/`429` responses are retried. Every other
    /// `Rejected` status is terminal — in particular `400`/`404`, which
    /// [`crate::runtime::subscribe_to_telemetry`] needs to surface to drive the
    /// schema fallback rather than retry away. `MissingRuntimeApi` is terminal
    /// (the env var will not appear on a retry).
    #[must_use]
    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Rejected { status, .. } => crate::runtime::retry::status_is_retryable(*status),
            Self::MissingRuntimeApi => false,
        }
    }
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
}
