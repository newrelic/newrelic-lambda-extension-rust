#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use crate::apm::app::{
        needs_normalization, normalize_transaction_name,
        normalize_analytic_event_data, normalize_span_event_data,
        normalize_metric_data, normalize_error_event_data,
        normalize_custom_event_data, normalize_transaction_sample_data,
    };

    // ========================================================================
    // needs_normalization
    // ========================================================================

    #[test]
    fn test_needs_normalization_true_for_plain_name() {
        assert!(needs_normalization("ruby-hw"));
        assert!(needs_normalization("my-function"));
        assert!(needs_normalization("handler"));
    }

    #[test]
    fn test_needs_normalization_false_when_contains_slash() {
        assert!(!needs_normalization("OtherTransaction/Ruby/ruby-hw"));
        assert!(!needs_normalization("WebTransaction/Function/handler"));
        assert!(!needs_normalization("a/b"));
    }

    // ========================================================================
    // normalize_transaction_name
    // ========================================================================

    #[test]
    fn test_normalize_transaction_name_prepends_prefix() {
        assert_eq!(
            normalize_transaction_name("ruby-hw"),
            "OtherTransaction/Ruby/ruby-hw"
        );
    }

    #[test]
    fn test_normalize_transaction_name_empty_string() {
        assert_eq!(
            normalize_transaction_name(""),
            "OtherTransaction/Ruby/"
        );
    }

    // ========================================================================
    // normalize_analytic_event_data
    // ========================================================================

    fn make_analytic_event(name: &str, event_type: &str) -> Value {
        // Structure: [run_id, {metadata}, [[[event_obj, {}, {}]], ...]]
        json!([
            "run-id",
            {},
            [[
                json!({"type": event_type, "name": name}),
                {},
                {}
            ]]
        ])
    }

    #[test]
    fn test_normalize_analytic_event_data_normalizes_transaction() {
        let mut data: Vec<Value> = serde_json::from_value(
            make_analytic_event("ruby-hw", "Transaction")
        ).expect("valid array");

        normalize_analytic_event_data(&mut data);

        let event = &data[2][0][0];
        assert_eq!(event["name"], "OtherTransaction/Ruby/ruby-hw");
    }

    #[test]
    fn test_normalize_analytic_event_data_skips_non_transaction() {
        let mut data: Vec<Value> = serde_json::from_value(
            make_analytic_event("ruby-hw", "Span")
        ).expect("valid array");

        normalize_analytic_event_data(&mut data);

        let event = &data[2][0][0];
        // Should NOT be normalized because type != "Transaction"
        assert_eq!(event["name"], "ruby-hw");
    }

    #[test]
    fn test_normalize_analytic_event_data_skips_already_normalized() {
        let mut data: Vec<Value> = serde_json::from_value(
            make_analytic_event("OtherTransaction/Ruby/ruby-hw", "Transaction")
        ).expect("valid array");

        normalize_analytic_event_data(&mut data);

        let event = &data[2][0][0];
        // Already has '/', should be unchanged
        assert_eq!(event["name"], "OtherTransaction/Ruby/ruby-hw");
    }

    #[test]
    fn test_normalize_analytic_event_data_short_data_no_panic() {
        let mut data: Vec<Value> = vec![json!("run-id")];
        normalize_analytic_event_data(&mut data); // Should not panic

        let mut data: Vec<Value> = vec![];
        normalize_analytic_event_data(&mut data); // Should not panic
    }

    // ========================================================================
    // normalize_span_event_data
    // ========================================================================

    #[test]
    fn test_normalize_span_event_data_normalizes_span() {
        let mut data: Vec<Value> = vec![
            json!("run-id"),
            json!({}),
            json!([[
                json!({"type": "Span", "name": "ruby-hw", "transaction.name": "ruby-hw"}),
                {},
                {}
            ]]),
        ];

        normalize_span_event_data(&mut data);

        let span = &data[2][0][0];
        assert_eq!(span["name"], "OtherTransaction/Ruby/ruby-hw");
        assert_eq!(span["transaction.name"], "OtherTransaction/Ruby/ruby-hw");
    }

    #[test]
    fn test_normalize_span_event_data_skips_non_span() {
        let mut data: Vec<Value> = vec![
            json!("run-id"),
            json!({}),
            json!([[
                json!({"type": "Transaction", "name": "ruby-hw"}),
                {},
                {}
            ]]),
        ];

        normalize_span_event_data(&mut data);

        let span = &data[2][0][0];
        assert_eq!(span["name"], "ruby-hw"); // Unchanged
    }

    #[test]
    fn test_normalize_span_event_data_short_data_no_panic() {
        let mut data: Vec<Value> = vec![json!("run-id")];
        normalize_span_event_data(&mut data);
    }

    // ========================================================================
    // normalize_metric_data
    // ========================================================================

    #[test]
    fn test_normalize_metric_data_other_transaction_prefix() {
        // Structure: [run_id, ts_start, ts_end, [[[{name: "..."}, [values]]]] ]
        let mut data: Vec<Value> = vec![
            json!("run-id"),
            json!(1000),
            json!(2000),
            json!([[
                json!({"name": "OtherTransactionTotalTime/ruby-hw"}),
                json!([1, 2, 3])
            ]]),
        ];

        normalize_metric_data(&mut data);

        let metric = &data[3][0][0];
        assert_eq!(metric["name"], "OtherTransactionTotalTime/Ruby/ruby-hw");
    }

    #[test]
    fn test_normalize_metric_data_standalone_name() {
        let mut data: Vec<Value> = vec![
            json!("run-id"),
            json!(1000),
            json!(2000),
            json!([[
                json!({"name": "ruby-hw-x86"}),
                json!([1, 2, 3])
            ]]),
        ];

        normalize_metric_data(&mut data);

        let metric = &data[3][0][0];
        assert_eq!(metric["name"], "OtherTransaction/Ruby/ruby-hw-x86");
    }

    #[test]
    fn test_normalize_metric_data_already_normalized() {
        let mut data: Vec<Value> = vec![
            json!("run-id"),
            json!(1000),
            json!(2000),
            json!([[
                json!({"name": "OtherTransactionTotalTime/Ruby/ruby-hw"}),
                json!([1, 2, 3])
            ]]),
        ];

        normalize_metric_data(&mut data);

        let metric = &data[3][0][0];
        // Already structured (suffix after first '/' contains '/') — no change
        assert_eq!(metric["name"], "OtherTransactionTotalTime/Ruby/ruby-hw");
    }

    #[test]
    fn test_normalize_metric_data_already_normalized_other_runtime() {
        // Not just Ruby — any structured path after first '/' should be preserved
        let mut data: Vec<Value> = vec![
            json!("run-id"),
            json!(1000),
            json!(2000),
            json!([[
                json!({"name": "OtherTransaction/Function/handler"}),
                json!([1, 2, 3])
            ]]),
        ];

        normalize_metric_data(&mut data);

        let metric = &data[3][0][0];
        assert_eq!(metric["name"], "OtherTransaction/Function/handler");
    }

    #[test]
    fn test_normalize_metric_data_short_data_no_panic() {
        let mut data: Vec<Value> = vec![json!("run-id"), json!(1000)];
        normalize_metric_data(&mut data);
    }

    // ========================================================================
    // normalize_error_event_data
    // ========================================================================

    #[test]
    fn test_normalize_error_event_data_both_fields() {
        let mut data: Vec<Value> = vec![
            json!("run-id"),
            json!({}),
            json!([[
                json!({
                    "transaction.name": "ruby-hw",
                    "transactionName": "ruby-hw",
                    "error.class": "RuntimeError"
                }),
                {},
                {}
            ]]),
        ];

        normalize_error_event_data(&mut data);

        let event = &data[2][0][0];
        assert_eq!(event["transaction.name"], "OtherTransaction/Ruby/ruby-hw");
        assert_eq!(event["transactionName"], "OtherTransaction/Ruby/ruby-hw");
        // Other fields preserved
        assert_eq!(event["error.class"], "RuntimeError");
    }

    #[test]
    fn test_normalize_error_event_data_already_normalized() {
        let mut data: Vec<Value> = vec![
            json!("run-id"),
            json!({}),
            json!([[
                json!({"transaction.name": "OtherTransaction/Ruby/ruby-hw"}),
                {},
                {}
            ]]),
        ];

        normalize_error_event_data(&mut data);

        let event = &data[2][0][0];
        assert_eq!(event["transaction.name"], "OtherTransaction/Ruby/ruby-hw");
    }

    #[test]
    fn test_normalize_error_event_data_short_data_no_panic() {
        let mut data: Vec<Value> = vec![json!("run-id")];
        normalize_error_event_data(&mut data);
    }

    // ========================================================================
    // normalize_custom_event_data
    // ========================================================================

    #[test]
    fn test_normalize_custom_event_data_normalizes() {
        let mut data: Vec<Value> = vec![
            json!("run-id"),
            json!({}),
            json!([[
                json!({"transaction.name": "ruby-hw", "type": "Custom"}),
                {},
                {}
            ]]),
        ];

        normalize_custom_event_data(&mut data);

        let event = &data[2][0][0];
        assert_eq!(event["transaction.name"], "OtherTransaction/Ruby/ruby-hw");
    }

    #[test]
    fn test_normalize_custom_event_data_no_transaction_name_field() {
        let mut data: Vec<Value> = vec![
            json!("run-id"),
            json!({}),
            json!([[
                json!({"type": "Custom", "value": 42}),
                {},
                {}
            ]]),
        ];

        normalize_custom_event_data(&mut data);

        let event = &data[2][0][0];
        // No transaction.name field — nothing to normalize
        assert_eq!(event["value"], 42);
    }

    #[test]
    fn test_normalize_custom_event_data_short_data_no_panic() {
        let mut data: Vec<Value> = vec![];
        normalize_custom_event_data(&mut data);
    }

    // ========================================================================
    // normalize_transaction_sample_data
    // ========================================================================

    #[test]
    fn test_normalize_transaction_sample_data_normalizes() {
        // Structure: [run_id, [[tx_id, timestamp, name, duration, encoded_data], ...]]
        let mut data: Vec<Value> = vec![
            json!("run-id"),
            json!([
                ["tx-1", 1000, "ruby-hw", 0.5, "encoded"]
            ]),
        ];

        normalize_transaction_sample_data(&mut data);

        let sample = &data[1][0];
        assert_eq!(sample[2], "OtherTransaction/Ruby/ruby-hw");
        // Other fields preserved
        assert_eq!(sample[0], "tx-1");
        assert_eq!(sample[3], 0.5);
    }

    #[test]
    fn test_normalize_transaction_sample_data_already_normalized() {
        let mut data: Vec<Value> = vec![
            json!("run-id"),
            json!([
                ["tx-1", 1000, "OtherTransaction/Ruby/ruby-hw", 0.5, "encoded"]
            ]),
        ];

        normalize_transaction_sample_data(&mut data);

        let sample = &data[1][0];
        assert_eq!(sample[2], "OtherTransaction/Ruby/ruby-hw");
    }

    #[test]
    fn test_normalize_transaction_sample_data_short_data_no_panic() {
        let mut data: Vec<Value> = vec![json!("run-id")];
        normalize_transaction_sample_data(&mut data);

        // Sample with less than 3 elements
        let mut data: Vec<Value> = vec![
            json!("run-id"),
            json!([["tx-1", 1000]]),
        ];
        normalize_transaction_sample_data(&mut data);
    }
}
