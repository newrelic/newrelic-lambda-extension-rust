// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn test_apm_app_creation() {
    let client = Client::new();
    let app = ApmApp {
        run_id: "test_run_id".to_string(),
        entity_guid: "test_guid".to_string(),
        collector_host: "collector.newrelic.com".to_string(),
        license_key: "test_key".to_string(),
        metric_endpoint: "https://metric-api.newrelic.com/metric/v1".to_string(),
        client,
        deployment: DeploymentContext::Normal {
            mode: crate::config::deployment::TelemetryMode::Apm,
        },
    };

    assert_eq!(app.run_id, "test_run_id");
    assert_eq!(app.entity_guid, "test_guid");
    assert_eq!(app.get_entity_guid(), "test_guid");
}
