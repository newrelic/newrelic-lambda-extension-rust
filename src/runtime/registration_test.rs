// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::{
    schema_for, ManagedSchema, RegistrationError, RegistrationSchema, StandardSchema,
};
use crate::config::deployment::{DeploymentContext, TelemetryMode};

#[test]
fn standard_schema_subscribes_to_invoke_and_shutdown() {
    let s = StandardSchema;
    assert_eq!(s.events(), &["INVOKE", "SHUTDOWN"]);
    assert_eq!(s.name(), "standard");
}

#[test]
fn managed_schema_subscribes_to_shutdown_only() {
    // Per AWS docs: "Extensions for Lambda Managed Instances functions can
    // only register for the SHUTDOWN event. Attempting to register for the
    // INVOKE event will result in an error."
    let s = ManagedSchema;
    assert_eq!(s.events(), &["SHUTDOWN"]);
    assert_eq!(s.name(), "managed");
    assert!(!s.events().contains(&"INVOKE"));
}

#[test]
fn dispatcher_normal_serverless_picks_standard() {
    let ctx = DeploymentContext::Normal {
        mode: TelemetryMode::Serverless,
    };
    assert_eq!(schema_for(ctx).name(), "standard");
}

#[test]
fn dispatcher_normal_apm_picks_standard() {
    let ctx = DeploymentContext::Normal {
        mode: TelemetryMode::Apm,
    };
    assert_eq!(schema_for(ctx).name(), "standard");
}

#[test]
fn dispatcher_lmi_picks_managed() {
    assert_eq!(schema_for(DeploymentContext::Lmi).name(), "managed");
}

#[test]
fn registration_error_display_is_actionable() {
    let cases = [
        (RegistrationError::MissingRuntimeApi, "AWS_LAMBDA_RUNTIME_API"),
        (
            RegistrationError::MissingExtensionId,
            "Lambda-Extension-Identifier",
        ),
        (
            RegistrationError::Rejected {
                status: 403,
                body: "forbidden".to_string(),
            },
            "status=403",
        ),
    ];
    for (err, expected_substring) in cases {
        let msg = format!("{err}");
        assert!(
            msg.contains(expected_substring),
            "error message {msg:?} should contain {expected_substring:?}"
        );
    }
}

#[test]
fn registration_error_rejected_includes_body() {
    let err = RegistrationError::Rejected {
        status: 400,
        body: "Invalid event INVOKE for Lambda Managed Instances".to_string(),
    };
    let msg = format!("{err}");
    assert!(msg.contains("Invalid event INVOKE"));
    assert!(msg.contains("status=400"));
}

#[test]
fn payload_serializes_events_array_correctly() {
    // Asserts the exact JSON shape AWS expects, for both schemas.
    let standard_payload = serde_json::json!({ "events": StandardSchema.events() });
    assert_eq!(
        standard_payload.to_string(),
        r#"{"events":["INVOKE","SHUTDOWN"]}"#
    );

    let managed_payload = serde_json::json!({ "events": ManagedSchema.events() });
    assert_eq!(
        managed_payload.to_string(),
        r#"{"events":["SHUTDOWN"]}"#
    );
}
