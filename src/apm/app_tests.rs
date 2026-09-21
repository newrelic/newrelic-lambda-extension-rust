// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tests for `apm::app`

use super::*;
use base64::Engine as _;
use flate2::write::GzEncoder;
use flate2::Compression;
use reqwest::Client;
use serde_json::Value;
use serial_test::serial;
use std::io::Write;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

// ── Test payload helpers ──────────────────────────────────────────────────────

/// Build a valid protocol-v1 binary payload from a telemetry data object.
/// Format: `["1","<base64(gzip({"data":<data>}))>"]`
fn make_v1_payload(data: serde_json::Value) -> Vec<u8> {
    let wrapper = serde_json::json!({"data": data});
    let json = serde_json::to_string(&wrapper).unwrap();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(json.as_bytes()).unwrap();
    let compressed = encoder.finish().unwrap();
    let b64 = base64::engine::general_purpose::STANDARD.encode(&compressed);
    format!("[\"1\",\"{b64}\"]").into_bytes()
}

/// Build a valid protocol-v2 binary payload from a LambdaData JSON object.
/// Format: `["2","<base64(gzip(<data>))>"]`
fn make_v2_payload(data: serde_json::Value) -> Vec<u8> {
    let json = serde_json::to_string(&data).unwrap();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(json.as_bytes()).unwrap();
    let compressed = encoder.finish().unwrap();
    let b64 = base64::engine::general_purpose::STANDARD.encode(&compressed);
    format!("[\"2\",\"{b64}\"]").into_bytes()
}

/// Construct an ApmApp pointing at a dead network address (port 1).
/// Any send will fail with ECONNREFUSED, triggering the buffer path.
fn test_apm_app() -> ApmApp {
    ApmApp {
        run_id: "run-1".to_string(),
        entity_guid: "guid-1".to_string(),
        app_name: "test-app".to_string(),
        collector_host: "127.0.0.1:1".to_string(),
        license_key: "test-key".to_string(),
        metric_endpoint: "http://127.0.0.1:1/metrics".to_string(),
        client: Client::new(),
        deployment: DeploymentContext::Normal {
            mode: crate::config::deployment::TelemetryMode::Apm,
        },
    }
}

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

// ── normalize_analytic_event_data ─────────────────────────────────────────────

#[test]
fn normalize_analytic_event_data_normalizes_transaction_name() {
    let mut data = payload_with_events(vec![
        serde_json::json!([{"type": "Transaction", "name": "ruby-hw"}, {}, {}]),
    ]);
    normalize_analytic_event_data(&mut data);
    assert_eq!(
        data[2][0][0]["name"],
        Value::String("OtherTransaction/Ruby/ruby-hw".to_string())
    );
}

#[test]
fn normalize_analytic_event_data_leaves_already_normalized_name_untouched() {
    let mut data = payload_with_events(vec![
        serde_json::json!([{"type": "Transaction", "name": "OtherTransaction/Ruby/fn"}, {}, {}]),
    ]);
    normalize_analytic_event_data(&mut data);
    assert_eq!(
        data[2][0][0]["name"],
        Value::String("OtherTransaction/Ruby/fn".to_string())
    );
}

#[test]
fn normalize_analytic_event_data_skips_non_transaction_type() {
    let mut data = payload_with_events(vec![
        serde_json::json!([{"type": "Span", "name": "ruby-hw"}, {}, {}]),
    ]);
    let before = data.clone();
    normalize_analytic_event_data(&mut data);
    assert_eq!(data, before);
}

#[test]
fn normalize_analytic_event_data_skips_event_without_name_field() {
    let mut data = payload_with_events(vec![
        serde_json::json!([{"type": "Transaction"}, {}, {}]),
    ]);
    let before = data.clone();
    normalize_analytic_event_data(&mut data);
    assert_eq!(data, before);
}

#[test]
fn normalize_analytic_event_data_handles_short_payload_without_panic() {
    let mut too_short = vec![serde_json::json!("run_id"), serde_json::json!({})];
    normalize_analytic_event_data(&mut too_short);
    assert_eq!(too_short.len(), 2);
}

// ── normalize_span_event_data ─────────────────────────────────────────────────

#[test]
fn normalize_span_event_data_normalizes_span_name() {
    let mut data = payload_with_events(vec![
        serde_json::json!([{"type": "Span", "name": "ruby-hw"}, {}, {}]),
    ]);
    normalize_span_event_data(&mut data);
    assert_eq!(
        data[2][0][0]["name"],
        Value::String("OtherTransaction/Ruby/ruby-hw".to_string())
    );
}

#[test]
fn normalize_span_event_data_also_updates_transaction_name_field() {
    let mut data = payload_with_events(vec![
        serde_json::json!([{"type": "Span", "name": "ruby-hw", "transaction.name": "ruby-hw"}, {}, {}]),
    ]);
    normalize_span_event_data(&mut data);
    let obj = data[2][0][0].as_object().unwrap();
    assert_eq!(obj["name"], Value::String("OtherTransaction/Ruby/ruby-hw".to_string()));
    assert_eq!(obj["transaction.name"], Value::String("OtherTransaction/Ruby/ruby-hw".to_string()));
}

#[test]
fn normalize_span_event_data_skips_non_span_type() {
    let mut data = payload_with_events(vec![
        serde_json::json!([{"type": "Transaction", "name": "ruby-hw"}, {}, {}]),
    ]);
    let before = data.clone();
    normalize_span_event_data(&mut data);
    assert_eq!(data, before);
}

#[test]
fn normalize_span_event_data_leaves_already_normalized_name_untouched() {
    let mut data = payload_with_events(vec![
        serde_json::json!([{"type": "Span", "name": "OtherTransaction/Ruby/fn"}, {}, {}]),
    ]);
    normalize_span_event_data(&mut data);
    assert_eq!(
        data[2][0][0]["name"],
        Value::String("OtherTransaction/Ruby/fn".to_string())
    );
}

#[test]
fn normalize_span_event_data_handles_short_payload_without_panic() {
    let mut too_short = vec![serde_json::json!("run_id"), serde_json::json!({})];
    normalize_span_event_data(&mut too_short);
    assert_eq!(too_short.len(), 2);
}

// ── needs_normalization / normalize_transaction_name ──────────────────────────

#[test]
fn needs_normalization_returns_true_for_plain_name() {
    assert!(needs_normalization("ruby-hw"));
}

#[test]
fn needs_normalization_returns_false_when_slash_present() {
    assert!(!needs_normalization("OtherTransaction/Ruby/fn"));
}

// ── ApmApp::new ───────────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn apm_app_new_returns_error_immediately_when_handshake_is_fatal() {
    crate::apm::connection::reset_handshake_fatal_for_test();
    crate::apm::connection::signal_handshake_fatal();

    let result = ApmApp::new(
        "key".to_string(), "127.0.0.1:1".to_string(),
        "http://127.0.0.1:1/metrics".to_string(), Client::new(),
        "fn".to_string(), "fn".to_string(), "1".to_string(),
        None, None, 1,
        DeploymentContext::Normal { mode: crate::config::deployment::TelemetryMode::Apm },
    ).await;

    assert!(result.is_err());
    assert!(format!("{:#}", result.unwrap_err()).contains("permanently disabled"));
    crate::apm::connection::reset_handshake_fatal_for_test();
}

#[tokio::test]
#[serial]
async fn apm_app_new_retries_all_attempts_and_returns_error_on_connection_refused() {
    crate::apm::connection::reset_handshake_fatal_for_test();
    crate::apm::connection::reset_connect_stats();

    let result = ApmApp::new(
        "key".to_string(), "127.0.0.1:1".to_string(),
        "http://127.0.0.1:1/metrics".to_string(), Client::new(),
        "fn".to_string(), "fn".to_string(), "1".to_string(),
        None, None, 1,
        DeploymentContext::Normal { mode: crate::config::deployment::TelemetryMode::Apm },
    ).await;

    // All 3 attempts fail → must return Err (not panic, not hang).
    assert!(result.is_err());
}

// ── try_connect ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn try_connect_wraps_preconnect_failure_with_context() {
    let result = ApmApp::try_connect(
        "key", "127.0.0.1:1", "http://metric", &Client::new(),
        "fn", "fn", "1", &None, &None, 1,
        DeploymentContext::Normal { mode: crate::config::deployment::TelemetryMode::Apm },
    ).await;
    assert!(result.is_err());
    assert!(
        format!("{:#}", result.unwrap_err()).contains("PreConnect"),
        "error must be wrapped with PreConnect context"
    );
}

#[tokio::test]
async fn try_connect_covers_explicit_account_id_and_region_path() {
    // Passes Some(..) for both optional args so the non-default branches run.
    let result = ApmApp::try_connect(
        "key", "127.0.0.1:1", "http://metric", &Client::new(),
        "fn", "fn", "1",
        &Some("123456789012".to_string()),
        &Some("eu-west-1".to_string()),
        1,
        DeploymentContext::Normal { mode: crate::config::deployment::TelemetryMode::Apm },
    ).await;
    assert!(result.is_err()); // preconnect still fails
}

// ── process_agent_payload ─────────────────────────────────────────────────────

#[tokio::test]
async fn process_agent_payload_empty_bytes_returns_ok() {
    let result = test_apm_app().process_agent_payload(b"".to_vec(), "req-1").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn process_agent_payload_invalid_utf8_returns_error() {
    let result = test_apm_app().process_agent_payload(vec![0xFF, 0xFE], "req-1").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn process_agent_payload_empty_telemetry_map_returns_ok() {
    let payload = make_v1_payload(serde_json::json!({}));
    let result = test_apm_app().process_agent_payload(payload, "req-1").await;
    assert!(result.is_ok());
}

#[tokio::test]
#[serial]
async fn process_agent_payload_disabled_type_is_skipped() {
    let mut types = std::collections::HashSet::new();
    types.insert("metric_data".to_string());
    crate::apm::collector::set_disabled_telemetry(types);

    let payload = make_v1_payload(serde_json::json!({
        "metric_data": [null, 0, 0, []]
    }));
    let result = test_apm_app().process_agent_payload(payload, "req-1").await;
    assert!(result.is_ok());

    crate::apm::collector::set_disabled_telemetry(std::collections::HashSet::new());
}

#[tokio::test]
async fn process_agent_payload_all_known_types_buffer_on_network_failure() {
    // Port 1 → every send fails → buffers → returns Ok(())
    let payload = make_v1_payload(serde_json::json!({
        "metric_data":            [null, 0, 0, []],
        "analytic_event_data":    [null, {}, []],
        "error_event_data":       [null, {}, []],
        "span_event_data":        [null, {}, []],
        "error_data":             [null, {}, []],
        "custom_event_data":      [null, {}, []],
        "log_event_data":         [null, {}, []],
        "transaction_sample_data":[null, []],
        "sql_trace_data":         [null, {}, []]
    }));
    let result = test_apm_app().process_agent_payload(payload, "req-1").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn process_agent_payload_normalizes_java_millisecond_epoch_timestamps() {
    // metric_data[1] and [2] > 1e12 are in ms and must be divided by 1000
    let payload = make_v1_payload(serde_json::json!({
        "metric_data": [null, 1_700_000_000_000_u64, 1_700_000_001_000_u64, []]
    }));
    let result = test_apm_app().process_agent_payload(payload, "req-1").await;
    assert!(result.is_ok());
}

#[tokio::test]
#[serial]
async fn process_agent_payload_v2_ruby_triggers_normalizers() {
    let prev = std::env::var("AWS_EXECUTION_ENV").ok();
    std::env::set_var("AWS_EXECUTION_ENV", "AWS_Lambda_ruby3.2");

    let payload = make_v2_payload(serde_json::json!({
        "analytic_event_data": [null, {}, [[{"type": "Transaction", "name": "ruby-hw"}, {}, {}]]],
        "span_event_data":     [null, {}, [[{"type": "Span", "name": "ruby-hw"}, {}, {}]]]
    }));
    let result = test_apm_app().process_agent_payload(payload, "req-1").await;

    match prev {
        Some(v) => std::env::set_var("AWS_EXECUTION_ENV", v),
        None    => std::env::remove_var("AWS_EXECUTION_ENV"),
    }
    assert!(result.is_ok());
}

// ── send_platform_report_metrics ──────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn send_platform_report_metrics_noop_when_disabled() {
    let mut types = std::collections::HashSet::new();
    types.insert("platform_metrics".to_string());
    crate::apm::collector::set_disabled_telemetry(types);

    let result = test_apm_app()
        .send_platform_report_metrics("REPORT RequestId: x Duration: 100 ms Billed Duration: 100 ms Memory Size: 128 MB Max Memory Used: 64 MB", "arn:test")
        .await;
    assert!(result.is_ok());

    crate::apm::collector::set_disabled_telemetry(std::collections::HashSet::new());
}

#[tokio::test]
async fn send_platform_report_metrics_noop_on_non_report_line() {
    let result = test_apm_app()
        .send_platform_report_metrics("START RequestId: x Version: $LATEST", "arn:test")
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn send_platform_report_metrics_success_on_202() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;

    let mut app = test_apm_app();
    app.metric_endpoint = server.uri();

    let result = app
        .send_platform_report_metrics(
            "REPORT RequestId: abc Duration: 150.5 ms Billed Duration: 200 ms Memory Size: 128 MB Max Memory Used: 64 MB",
            "arn:aws:lambda:us-east-1:123:function:fn",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn send_platform_report_metrics_permanent_4xx_is_dropped() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;

    let mut app = test_apm_app();
    app.metric_endpoint = server.uri();

    let result = app
        .send_platform_report_metrics(
            "REPORT RequestId: abc Duration: 150.5 ms Billed Duration: 200 ms Memory Size: 128 MB Max Memory Used: 64 MB",
            "arn:aws:lambda:us-east-1:123:function:fn",
        )
        .await;
    assert!(result.is_ok()); // permanent errors are logged and dropped, not propagated
}

#[tokio::test]
async fn send_platform_report_metrics_transient_5xx_is_buffered() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let mut app = test_apm_app();
    app.metric_endpoint = server.uri();

    let result = app
        .send_platform_report_metrics(
            "REPORT RequestId: abc Duration: 150.5 ms Billed Duration: 200 ms Memory Size: 128 MB Max Memory Used: 64 MB",
            "arn:aws:lambda:us-east-1:123:function:fn",
        )
        .await;
    assert!(result.is_ok()); // transient errors are buffered, not propagated
}

// ── send_error_events_buffered ────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn send_error_events_buffered_noop_when_disabled() {
    let mut types = std::collections::HashSet::new();
    types.insert("error_event_data".to_string());
    crate::apm::collector::set_disabled_telemetry(types);

    let app = test_apm_app();
    let result = app
        .send_error_events_buffered(vec![serde_json::json!({"type": "TransactionError"})], "req-1")
        .await;
    assert!(result.is_ok());

    crate::apm::collector::set_disabled_telemetry(std::collections::HashSet::new());
}

#[tokio::test]
async fn send_error_events_buffered_buffers_on_network_failure() {
    // Port 1 → ECONNREFUSED → buffers → returns Ok(())
    let result = test_apm_app()
        .send_error_events_buffered(vec![serde_json::json!({"type": "TransactionError"})], "req-1")
        .await;
    assert!(result.is_ok());
}

// ── process_agent_payload: remaining branches ─────────────────────────────────

#[tokio::test]
async fn process_agent_payload_empty_data_array_is_skipped() {
    // An entry whose Vec is empty hits the `if data.is_empty() { continue; }` guard.
    let payload = make_v1_payload(serde_json::json!({
        "metric_data": []
    }));
    let result = test_apm_app().process_agent_payload(payload, "req-empty-data").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn process_agent_payload_unknown_type_hits_warn_arm_in_spawn() {
    // A type that isn't in the match table flows into the spawned task and hits the `_ =>` arm.
    let payload = make_v1_payload(serde_json::json!({
        "totally_unknown_type": [{"event": "data"}]
    }));
    let result = test_apm_app().process_agent_payload(payload, "req-unknown-type").await;
    assert!(result.is_ok());
}

#[tokio::test]
#[serial]
async fn process_agent_payload_v2_ruby_normalizes_all_six_data_types() {
    // Covers the four `if let Some(data) = telemetry_map.get_mut(...)` branches in the
    // Ruby v2 normalizer path that the existing test doesn't reach:
    // metric_data, error_event_data, custom_event_data, transaction_sample_data.
    let prev = std::env::var("AWS_EXECUTION_ENV").ok();
    std::env::set_var("AWS_EXECUTION_ENV", "AWS_Lambda_ruby3.2");

    let payload = make_v2_payload(serde_json::json!({
        "analytic_event_data":    [null, {}, [[{"type": "Transaction", "name": "ruby-hw"}, {}, {}]]],
        "span_event_data":        [null, {}, [[{"type": "Span",        "name": "ruby-hw"}, {}, {}]]],
        "metric_data":            [null, 0, 0, [[{"name": "ruby-hw"}, [0,0,0,0,0,0]]]],
        "error_event_data":       [null, {}, [[{"transaction.name": "ruby-hw"}, {}, {}]]],
        "custom_event_data":      [null, {}, [[{"transaction.name": "ruby-hw"}, {}, {}]]],
        "transaction_sample_data":[null, [["txn-id", 0, "ruby-hw", 0.0, "encoded"]]]
    }));
    let result = test_apm_app().process_agent_payload(payload, "req-v2-all").await;

    match prev {
        Some(v) => std::env::set_var("AWS_EXECUTION_ENV", v),
        None => std::env::remove_var("AWS_EXECUTION_ENV"),
    }
    assert!(result.is_ok());
}

// ── send_shutdown_error_event ─────────────────────────────────────────────────

#[tokio::test]
async fn send_shutdown_error_event_buffers_on_network_failure() {
    let result = test_apm_app()
        .send_shutdown_error_event(
            "Lambda.Timeout",
            "Task timed out after 30.00 seconds",
            "req-1",
            "arn:aws:lambda:us-east-1:123:function:fn",
        )
        .await;
    assert!(result.is_ok());
}
