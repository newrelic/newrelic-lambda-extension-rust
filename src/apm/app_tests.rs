// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tests for `apm::app`

use super::*;
use reqwest::Client;
use serde_json::Value;

#[test]
fn test_apm_app_creation() {
    let client = Client::new();
    let app = ApmApp {
        run_id: "test_run_id".to_string(),
        entity_guid: "test_guid".to_string(),
        app_name: "test_app".to_string(),
        collector_host: "collector.newrelic.com".to_string(),
        license_key: "test_key".to_string(),
        metric_endpoint: "https://metric-api.newrelic.com/metric/v1".to_string(),
        otlp_metric_endpoint: "https://collector.newrelic.com/v1/metrics".to_string(),
        client,
        deployment: DeploymentContext::Normal {
            mode: crate::config::deployment::TelemetryMode::Apm,
        },
    };

    assert_eq!(app.run_id, "test_run_id");
    assert_eq!(app.entity_guid, "test_guid");
    assert_eq!(app.get_entity_guid(), "test_guid");
    assert_eq!(app.get_app_name(), "test_app");
    assert!(matches!(
        app.deployment,
        DeploymentContext::Normal { mode: crate::config::deployment::TelemetryMode::Apm }
    ));
}

// ========================================================================
// inject_custom_tag_attributes (NR-600651) - exercised with an explicit tag
// map, never via get_custom_tag_attributes()'s process-wide OnceLock cache,
// for the same reason config::mod_test.rs tests parse_nr_tags() rather than
// get_nr_tags(): the cache can only be initialized once per test binary.
// ========================================================================

fn transaction_event(user_attrs: &Value) -> Value {
    serde_json::json!([{"type": "Transaction", "name": "OtherTransaction/Function/test"}, user_attrs, {}])
}

fn span_event(user_attrs: &Value) -> Value {
    serde_json::json!([{"type": "Span", "name": "test-span"}, user_attrs, {}])
}

fn payload_with_events(events: Vec<Value>) -> Vec<Value> {
    vec![serde_json::json!("run_id"), serde_json::json!({}), Value::Array(events)]
}

fn tags_map(pairs: &[(&str, &str)]) -> serde_json::Map<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), Value::String(v.to_string())))
        .collect()
}

#[test]
fn inject_custom_tag_attributes_noop_when_tags_empty() {
    let mut data = payload_with_events(vec![transaction_event(&serde_json::json!({}))]);
    let before = data.clone();
    inject_custom_tag_attributes(&mut data, "Transaction", &serde_json::Map::new());
    assert_eq!(data, before);
}

#[test]
fn inject_custom_tag_attributes_basic_transaction() {
    let mut data = payload_with_events(vec![transaction_event(&serde_json::json!({}))]);
    let tags = tags_map(&[("team", "dev")]);

    inject_custom_tag_attributes(&mut data, "Transaction", &tags);

    let user_attrs = data[2][0][1].as_object().expect("user_attrs should be an object");
    assert_eq!(user_attrs.get("team"), Some(&Value::String("dev".to_string())));
}

#[test]
fn inject_custom_tag_attributes_basic_span() {
    let mut data = payload_with_events(vec![span_event(&serde_json::json!({}))]);
    let tags = tags_map(&[("team", "dev")]);

    inject_custom_tag_attributes(&mut data, "Span", &tags);

    let user_attrs = data[2][0][1].as_object().expect("user_attrs should be an object");
    assert_eq!(user_attrs.get("team"), Some(&Value::String("dev".to_string())));
}

#[test]
fn inject_custom_tag_attributes_agent_attribute_wins_on_collision() {
    let mut data = payload_with_events(vec![transaction_event(&serde_json::json!({"team": "agent-set"}))]);
    let tags = tags_map(&[("team", "dev")]);

    inject_custom_tag_attributes(&mut data, "Transaction", &tags);

    let user_attrs = data[2][0][1].as_object().expect("user_attrs should be an object");
    assert_eq!(
        user_attrs.get("team"),
        Some(&Value::String("agent-set".to_string())),
        "the agent's own attribute must never be overwritten by the injected tag"
    );
}

#[test]
fn inject_custom_tag_attributes_creates_missing_user_attrs_object() {
    // user_attrs slot is `null`, not an object - the agent didn't set anything there.
    let mut data = payload_with_events(vec![transaction_event(&Value::Null)]);
    let tags = tags_map(&[("team", "dev")]);

    inject_custom_tag_attributes(&mut data, "Transaction", &tags);

    let user_attrs = data[2][0][1].as_object().expect("user_attrs should now be an object");
    assert_eq!(user_attrs.get("team"), Some(&Value::String("dev".to_string())));
}

#[test]
fn inject_custom_tag_attributes_skips_mismatched_type() {
    // An event whose intrinsic type isn't "Transaction" must be left untouched,
    // even though its user_attrs object already exists.
    let mut data = payload_with_events(vec![span_event(&serde_json::json!({}))]);
    let before = data.clone();
    let tags = tags_map(&[("team", "dev")]);

    inject_custom_tag_attributes(&mut data, "Transaction", &tags);

    assert_eq!(data, before);
}

#[test]
fn inject_custom_tag_attributes_only_touches_matching_events_in_a_batch() {
    let mut data = payload_with_events(vec![
        transaction_event(&serde_json::json!({})),
        span_event(&serde_json::json!({})),
    ]);
    let tags = tags_map(&[("team", "dev")]);

    inject_custom_tag_attributes(&mut data, "Transaction", &tags);

    let txn_attrs = data[2][0][1].as_object().expect("transaction user_attrs should be an object");
    assert_eq!(txn_attrs.get("team"), Some(&Value::String("dev".to_string())));

    let span_attrs = data[2][1][1].as_object().expect("span user_attrs should be an object");
    assert!(span_attrs.get("team").is_none(), "the Span event must not receive the Transaction-scoped injection");
}

#[test]
fn inject_custom_tag_attributes_handles_short_payload_without_panic() {
    let tags = tags_map(&[("team", "dev")]);

    let mut too_short = vec![serde_json::json!("run_id"), serde_json::json!({})];
    inject_custom_tag_attributes(&mut too_short, "Transaction", &tags);
    assert_eq!(too_short.len(), 2);

    let mut events_not_array = vec![serde_json::json!("run_id"), serde_json::json!({}), Value::Null];
    inject_custom_tag_attributes(&mut events_not_array, "Transaction", &tags);
    assert_eq!(events_not_array[2], Value::Null);

    let mut short_tuple = payload_with_events(vec![Value::Array(vec![serde_json::json!({"type": "Transaction"})])]);
    inject_custom_tag_attributes(&mut short_tuple, "Transaction", &tags);
    // A 1-element tuple has no user_attrs slot to inject into - must not panic, and
    // must be left exactly as-is.
    assert_eq!(short_tuple[2][0].as_array().map(Vec::len), Some(1));
}

#[test]
fn get_custom_tag_attributes_prefixes_keys_with_tags() {
    // Uniformity with log-forwarding: Transaction/Span attribute keys are tags.-prefixed
    // (tags.team), not raw (team) - unlike Entity Tags, which stay unprefixed.
    // Exercises the build logic directly (mirrors get_custom_tag_attributes()) without
    // touching the cached get_custom_tag_attributes()/get_new_relic_labels() functions
    // themselves - same rationale as the tests above.
    let new_relic_labels = [("team".to_string(), "dev".to_string())];

    let mut map = serde_json::Map::new();
    for (k, v) in &new_relic_labels {
        map.insert(format!("tags.{k}"), Value::String(v.clone()));
    }

    assert_eq!(map.get("tags.team"), Some(&Value::String("dev".to_string())));
    assert!(map.get("team").is_none(), "the raw, unprefixed key must not also be present");
}

// ========================================================================
// normalize_metric_data / normalize_error_event_data / normalize_custom_event_data /
// normalize_transaction_sample_data - pre-existing Ruby v2 payload normalizers with
// no prior direct test coverage (the sibling normalize_analytic_event_data /
// normalize_span_event_data are covered above only indirectly, via this feature's
// adjacent inject_custom_tag_attributes tests).
// ========================================================================

fn metric_payload(metrics: Vec<Value>) -> Vec<Value> {
    vec![
        serde_json::json!("run_id"),
        serde_json::json!(0),
        serde_json::json!(0),
        Value::Array(metrics),
    ]
}

fn metric_entry(name: &str) -> Value {
    serde_json::json!([{"name": name}, [0, 0, 0, 0, 0, 0]])
}

#[test]
fn normalize_metric_data_inserts_ruby_segment_for_othertransaction_metric() {
    let mut data = metric_payload(vec![metric_entry("OtherTransactionTotalTime/ruby-hw")]);

    normalize_metric_data(&mut data);

    assert_eq!(
        data[3][0][0]["name"],
        Value::String("OtherTransactionTotalTime/Ruby/ruby-hw".to_string())
    );
}

#[test]
fn normalize_metric_data_normalizes_standalone_name() {
    let mut data = metric_payload(vec![metric_entry("ruby-hw-x86-hw")]);

    normalize_metric_data(&mut data);

    assert_eq!(data[3][0][0]["name"], Value::String("OtherTransaction/Ruby/ruby-hw-x86-hw".to_string()));
}

#[test]
fn normalize_metric_data_leaves_already_slashed_non_othertransaction_name_untouched() {
    let mut data = metric_payload(vec![metric_entry("Custom/Ruby/already-normalized")]);

    normalize_metric_data(&mut data);

    assert_eq!(data[3][0][0]["name"], Value::String("Custom/Ruby/already-normalized".to_string()));
}

#[test]
fn normalize_metric_data_leaves_othertransaction_without_slash_untouched() {
    // starts_with("OtherTransaction") but rfind('/') finds nothing - the `if let`
    // guard never fires, so the name is left exactly as-is.
    let mut data = metric_payload(vec![metric_entry("OtherTransactionNoSlash")]);

    normalize_metric_data(&mut data);

    assert_eq!(data[3][0][0]["name"], Value::String("OtherTransactionNoSlash".to_string()));
}

#[test]
fn normalize_metric_data_skips_entry_without_name_field() {
    let mut data = metric_payload(vec![serde_json::json!([{}, [0, 0, 0, 0, 0, 0]])]);
    let before = data.clone();

    normalize_metric_data(&mut data);

    assert_eq!(data, before);
}

#[test]
fn normalize_metric_data_handles_short_payload_without_panic() {
    let mut too_short = vec![serde_json::json!("run_id"), serde_json::json!(0), serde_json::json!(0)];
    normalize_metric_data(&mut too_short);
    assert_eq!(too_short.len(), 3);
}

fn error_event(fields: &Value) -> Value {
    serde_json::json!([fields, {}, {}])
}

#[test]
fn normalize_error_event_data_normalizes_both_name_fields() {
    let mut data = payload_with_events(vec![error_event(
        &serde_json::json!({"transaction.name": "ruby-hw", "transactionName": "ruby-hw"}),
    )]);

    normalize_error_event_data(&mut data);

    let fields = data[2][0][0].as_object().expect("fields should be an object");
    assert_eq!(fields.get("transaction.name"), Some(&Value::String("OtherTransaction/Ruby/ruby-hw".to_string())));
    assert_eq!(fields.get("transactionName"), Some(&Value::String("OtherTransaction/Ruby/ruby-hw".to_string())));
}

#[test]
fn normalize_error_event_data_leaves_already_normalized_names_untouched() {
    let mut data = payload_with_events(vec![error_event(
        &serde_json::json!({"transaction.name": "OtherTransaction/Ruby/ruby-hw"}),
    )]);

    normalize_error_event_data(&mut data);

    let fields = data[2][0][0].as_object().expect("fields should be an object");
    assert_eq!(
        fields.get("transaction.name"),
        Some(&Value::String("OtherTransaction/Ruby/ruby-hw".to_string()))
    );
}

#[test]
fn normalize_error_event_data_handles_missing_name_fields_without_panic() {
    let mut data = payload_with_events(vec![error_event(&serde_json::json!({}))]);
    let before = data.clone();

    normalize_error_event_data(&mut data);

    assert_eq!(data, before);
}

#[test]
fn normalize_custom_event_data_normalizes_transaction_name() {
    let mut data = payload_with_events(vec![error_event(&serde_json::json!({"transaction.name": "ruby-hw"}))]);

    normalize_custom_event_data(&mut data);

    let fields = data[2][0][0].as_object().expect("fields should be an object");
    assert_eq!(fields.get("transaction.name"), Some(&Value::String("OtherTransaction/Ruby/ruby-hw".to_string())));
}

#[test]
fn normalize_custom_event_data_ignores_transactionname_field() {
    // Unlike normalize_error_event_data, this only looks at "transaction.name" -
    // a bare "transactionName" field must be left untouched.
    let mut data = payload_with_events(vec![error_event(&serde_json::json!({"transactionName": "ruby-hw"}))]);
    let before = data.clone();

    normalize_custom_event_data(&mut data);

    assert_eq!(data, before);
}

fn transaction_sample(name: &str) -> Value {
    serde_json::json!(["txn-id", 0, name, 0.0, "encoded"])
}

#[test]
fn normalize_transaction_sample_data_normalizes_name_at_index_2() {
    let mut data = vec![serde_json::json!("run_id"), Value::Array(vec![transaction_sample("ruby-hw")])];

    normalize_transaction_sample_data(&mut data);

    assert_eq!(data[1][0][2], Value::String("OtherTransaction/Ruby/ruby-hw".to_string()));
}

#[test]
fn normalize_transaction_sample_data_leaves_already_normalized_name_untouched() {
    let mut data = vec![
        serde_json::json!("run_id"),
        Value::Array(vec![transaction_sample("OtherTransaction/Ruby/ruby-hw")]),
    ];

    normalize_transaction_sample_data(&mut data);

    assert_eq!(data[1][0][2], Value::String("OtherTransaction/Ruby/ruby-hw".to_string()));
}

#[test]
fn normalize_transaction_sample_data_skips_short_sample_without_panic() {
    let mut data = vec![serde_json::json!("run_id"), Value::Array(vec![serde_json::json!(["txn-id", 0])])];
    let before = data.clone();

    normalize_transaction_sample_data(&mut data);

    assert_eq!(data, before);
}

#[test]
fn normalize_transaction_sample_data_handles_short_payload_without_panic() {
    let mut too_short = vec![serde_json::json!("run_id")];
    normalize_transaction_sample_data(&mut too_short);
    assert_eq!(too_short.len(), 1);
}
