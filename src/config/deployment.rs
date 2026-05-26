// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Deployment context detection.
//!
//! The extension can run on standard AWS Lambda or on Lambda Managed Instances
//! (LMI). These environments differ at the Extensions API level — most notably,
//! LMI does not deliver `INVOKE` events and rejects `INVOKE` registration:
//!
//! > Extensions for Lambda Managed Instances functions can only register for the
//! > SHUTDOWN event. Attempting to register for the INVOKE event will result in
//! > an error.
//! > <https://docs.aws.amazon.com/lambda/latest/dg/runtimes-extensions-api.html>
//!
//! Detection happens once at startup by reading
//! [`INIT_TYPE_ENV`](AWS_LAMBDA_INITIALIZATION_TYPE). All other modules read
//! `ExtensionConfig.deployment` instead of re-parsing env vars (single source of
//! truth — see `LMI_SUPPORT.md` §2).

// Wired into the binary by follow-up commits on NR-555608 (registration
// dispatcher + main.rs). Allow dead_code in the meantime so the staging commit
// stays warning-clean.
#![allow(dead_code)]

use std::env;
use tracing::{debug, info, warn};

/// AWS-set environment variable that names the execution environment type.
///
/// Documented values: `on-demand`, `provisioned-concurrency`, `snap-start`,
/// `lambda-managed-instances`.
///
/// Source: <https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html>
pub const INIT_TYPE_ENV: &str = "AWS_LAMBDA_INITIALIZATION_TYPE";

/// The exact value AWS sets when running on Lambda Managed Instances.
pub const LMI_INIT_TYPE: &str = "lambda-managed-instances";

/// Environment variable that opts a Normal-Lambda function into APM mode.
/// Ignored on LMI (APM is forced).
pub const APM_MODE_ENV: &str = "NEW_RELIC_APM_LAMBDA_MODE";

/// Telemetry mode the extension uses when reporting to New Relic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetryMode {
    /// New Relic Serverless monitoring — Lambda appears as a serverless entity.
    Serverless,
    /// APM mode — Lambda appears as an APM application entity.
    Apm,
}

/// The deployment environment the extension is running in.
///
/// `Lmi` is intentionally a unit variant. Lambda Managed Instances supports
/// only APM mode; adding a `mode` field would re-open the illegal
/// `Lmi + Serverless` state. See `LMI_SUPPORT.md` §6.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentContext {
    /// Standard AWS Lambda (`on-demand`, `provisioned-concurrency`, or
    /// `snap-start`). Both telemetry modes are legal.
    Normal { mode: TelemetryMode },
    /// Lambda Managed Instances. APM mode is forced.
    Lmi,
}

impl DeploymentContext {
    pub fn telemetry_mode(self) -> TelemetryMode {
        match self {
            Self::Normal { mode } => mode,
            Self::Lmi => TelemetryMode::Apm,
        }
    }

    pub fn is_apm(self) -> bool {
        matches!(self.telemetry_mode(), TelemetryMode::Apm)
    }

    pub fn is_lmi(self) -> bool {
        matches!(self, Self::Lmi)
    }
}

/// Detect the deployment context from environment variables.
///
/// `AWS_LAMBDA_INITIALIZATION_TYPE` selects between Normal Lambda and LMI;
/// `NEW_RELIC_APM_LAMBDA_MODE` toggles APM on Normal Lambda. On LMI the latter
/// is ignored (APM is forced); a warning is emitted if it was explicitly set
/// to `false` so the override is visible in `CloudWatch`.
pub fn detect() -> DeploymentContext {
    let init_type = env::var(INIT_TYPE_ENV).ok();
    let apm_requested = parse_bool_env(APM_MODE_ENV);

    match init_type.as_deref() {
        Some(LMI_INIT_TYPE) => {
            if apm_requested == Some(false) {
                warn!(
                    "[NR_EXT] Running on Lambda Managed Instances — APM mode is forced; \
                     {APM_MODE_ENV}=false is being ignored."
                );
            }
            info!("[NR_EXT] deployment context: LMI (APM mode)");
            DeploymentContext::Lmi
        }
        other => {
            if let Some(t) = other {
                debug!("[NR_EXT] {INIT_TYPE_ENV}='{t}' (treated as Normal Lambda)");
            } else {
                debug!("[NR_EXT] {INIT_TYPE_ENV} unset (treated as Normal Lambda)");
            }
            let mode = if apm_requested == Some(true) {
                TelemetryMode::Apm
            } else {
                TelemetryMode::Serverless
            };
            info!("[NR_EXT] deployment context: Normal ({mode:?} mode)");
            DeploymentContext::Normal { mode }
        }
    }
}

fn parse_bool_env(name: &str) -> Option<bool> {
    let raw = env::var(name).ok()?;
    match raw.trim().to_lowercase().as_str() {
        "true" | "1" | "yes" => Some(true),
        "false" | "0" | "no" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
#[path = "deployment_test.rs"]
mod deployment_test;
