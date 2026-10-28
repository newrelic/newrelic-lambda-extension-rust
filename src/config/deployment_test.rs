// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use serial_test::serial;
use std::env;

fn clear_env() {
    env::remove_var(INIT_TYPE_ENV);
    env::remove_var(APM_MODE_ENV);
}

#[test]
#[serial]
fn lmi_value_yields_lmi_context() {
    clear_env();
    env::set_var(INIT_TYPE_ENV, LMI_INIT_TYPE);

    let ctx = detect();

    assert_eq!(ctx, DeploymentContext::Lmi);
    assert!(ctx.is_lmi());
    assert!(ctx.is_apm());
    assert_eq!(ctx.telemetry_mode(), TelemetryMode::Apm);
    clear_env();
}

#[test]
#[serial]
fn lmi_forces_apm_even_when_apm_env_disables_it() {
    clear_env();
    env::set_var(INIT_TYPE_ENV, LMI_INIT_TYPE);
    env::set_var(APM_MODE_ENV, "false");

    assert!(detect().is_apm());
    clear_env();
}

#[test]
#[serial]
fn normal_on_demand_defaults_to_serverless() {
    clear_env();
    env::set_var(INIT_TYPE_ENV, "on-demand");

    assert_eq!(
        detect(),
        DeploymentContext::Normal {
            mode: TelemetryMode::Serverless
        }
    );
    clear_env();
}

#[test]
#[serial]
fn normal_provisioned_concurrency_with_apm_opt_in() {
    clear_env();
    env::set_var(INIT_TYPE_ENV, "provisioned-concurrency");
    env::set_var(APM_MODE_ENV, "true");

    assert_eq!(
        detect(),
        DeploymentContext::Normal {
            mode: TelemetryMode::Apm
        }
    );
    clear_env();
}

#[test]
#[serial]
fn normal_snap_start_recognised() {
    clear_env();
    env::set_var(INIT_TYPE_ENV, "snap-start");

    assert!(matches!(
        detect(),
        DeploymentContext::Normal {
            mode: TelemetryMode::Serverless
        }
    ));
    clear_env();
}

#[test]
#[serial]
fn missing_init_type_falls_back_to_normal_serverless() {
    clear_env();

    assert_eq!(
        detect(),
        DeploymentContext::Normal {
            mode: TelemetryMode::Serverless
        }
    );
}

#[test]
#[serial]
fn unknown_init_type_treated_as_normal() {
    clear_env();
    env::set_var(INIT_TYPE_ENV, "future-value-not-yet-known");

    assert!(matches!(detect(), DeploymentContext::Normal { .. }));
    clear_env();
}

#[test]
#[serial]
fn apm_env_accepts_truthy_variants() {
    for v in ["true", "1", "yes", "TRUE", "Yes"] {
        clear_env();
        env::set_var(INIT_TYPE_ENV, "on-demand");
        env::set_var(APM_MODE_ENV, v);

        assert!(
            detect().is_apm(),
            "APM mode should be enabled by {APM_MODE_ENV}={v}"
        );
    }
    clear_env();
}

#[test]
#[serial]
fn apm_env_rejects_garbage_values_silently() {
    clear_env();
    env::set_var(INIT_TYPE_ENV, "on-demand");
    env::set_var(APM_MODE_ENV, "maybe");

    // Unparseable values are treated as "not requested" — Serverless wins.
    assert!(matches!(
        detect(),
        DeploymentContext::Normal {
            mode: TelemetryMode::Serverless
        }
    ));
    clear_env();
}
