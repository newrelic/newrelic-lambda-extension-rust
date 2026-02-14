//! Criterion benchmarks for Lambda extension hot paths
//!
//! Per-invocation hot paths (standard mode):
//! - Threshold detection (should_send_batch_by_threshold) — runs 1-2x per invoke
//! - Batch buffer insert (add_to_batch) — runs 1x per invoke (when payload + report ready)
//! - Payload JSON construction (build_newrelic_payload) — runs when threshold hit (~every 3 invokes)
//! - Full invocation cycle simulation — add + threshold + build + clear
//!
//! Per-invocation hot paths (APM mode):
//! - Agent payload parsing (parse_agent_payload) — runs 1x per invoke
//!
//! Cold paths (shutdown / retry only):
//! - Chunk splitting (split_into_chunks) — only on shutdown
//! - Backoff calculation (get_backoff_delay) — only on retry
//!
//! Run with: cargo bench

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use std::sync::Arc;

use newrelic_lambda_extension::agent::batch::{
    BatchBuffer, BatchedAgentPayload, build_newrelic_payload,
};
use newrelic_lambda_extension::config::ExtensionConfig;
use newrelic_lambda_extension::retry::get_backoff_delay;

// ============================================================================
// Helpers
// ============================================================================

fn make_payload(id: &str, report: Option<&str>, data_size: usize) -> BatchedAgentPayload {
    BatchedAgentPayload {
        request_id: id.to_string(),
        agent_payload_bytes: Arc::from(vec![b'X'; data_size]),
        report_line: report.map(|s| s.to_string()),
        invoked_function_arn: "arn:aws:lambda:us-east-1:123456789012:function:bench-fn".to_string(),
        timestamp: chrono::Utc::now(),
    }
}

fn make_config() -> ExtensionConfig {
    let mut config = ExtensionConfig::default();
    config.aws.function_name = "bench-function".to_string();
    config
}

// ============================================================================
// Benchmark: build_newrelic_payload — the #1 hot path
// ============================================================================

fn bench_build_payload(c: &mut Criterion) {
    let config = make_config();
    let mut group = c.benchmark_group("build_newrelic_payload");

    // Vary batch size: 1, 5, 10, 50, 100 payloads
    for &count in &[1, 5, 10, 50, 100] {
        let items: Vec<BatchedAgentPayload> = (0..count)
            .map(|i| {
                let report = if i % 2 == 0 { Some("REPORT Duration: 123.45 ms") } else { None };
                make_payload(&format!("req-{i}"), report, 256)
            })
            .collect();

        group.bench_with_input(
            BenchmarkId::new("items", count),
            &items,
            |b, items| b.iter(|| build_newrelic_payload(black_box(items), black_box(&config), None)),
        );
    }

    group.finish();
}

// Vary payload data size: 64B, 256B, 1KB, 4KB, 16KB
fn bench_build_payload_data_size(c: &mut Criterion) {
    let config = make_config();
    let mut group = c.benchmark_group("build_payload_data_size");

    for &size in &[64, 256, 1024, 4096, 16384] {
        let items: Vec<BatchedAgentPayload> = (0..10)
            .map(|i| make_payload(&format!("req-{i}"), Some("REPORT Duration: 100 ms"), size))
            .collect();

        group.bench_with_input(
            BenchmarkId::new("bytes", size),
            &items,
            |b, items| b.iter(|| build_newrelic_payload(black_box(items), black_box(&config), None)),
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark: add_to_batch — runs on every warm invocation
// ============================================================================

fn bench_add_to_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("add_to_batch");

    // Single insert into empty buffer
    group.bench_function("single_insert", |b| {
        let buffer = BatchBuffer::new();
        let mut counter = 0u64;
        b.iter(|| {
            counter += 1;
            buffer.add_to_batch(
                format!("req-{counter}"),
                black_box(vec![0u8; 256]),
                Some("REPORT Duration: 100 ms".to_string()),
                "arn:bench".to_string(),
            );
        });
    });

    // Insert into buffer with 100 existing entries
    group.bench_function("insert_into_100", |b| {
        let buffer = BatchBuffer::new();
        for i in 0..100 {
            buffer.add_to_batch(
                format!("pre-{i}"), vec![0u8; 256], None, "arn:bench".to_string(),
            );
        }
        let mut counter = 100u64;
        b.iter(|| {
            counter += 1;
            buffer.add_to_batch(
                format!("req-{counter}"),
                black_box(vec![0u8; 256]),
                Some("REPORT".to_string()),
                "arn:bench".to_string(),
            );
        });
    });

    group.finish();
}

// ============================================================================
// Benchmark: should_send_batch_by_threshold — runs on every invocation
// ============================================================================

fn bench_threshold_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("threshold_check");

    // Below threshold (2 reports in 10 items)
    group.bench_function("below_threshold_10_items", |b| {
        let buffer = BatchBuffer::new();
        for i in 0..10 {
            let report = if i < 2 { Some("REPORT".to_string()) } else { None };
            buffer.add_to_batch(format!("req-{i}"), vec![1], report, "arn:bench".to_string());
        }
        b.iter(|| black_box(buffer.should_send_batch_by_threshold()));
    });

    // At threshold (5 reports in 20 items)
    group.bench_function("at_threshold_20_items", |b| {
        let buffer = BatchBuffer::new();
        for i in 0..20 {
            let report = if i < 5 { Some("REPORT".to_string()) } else { None };
            buffer.add_to_batch(format!("req-{i}"), vec![1], report, "arn:bench".to_string());
        }
        b.iter(|| black_box(buffer.should_send_batch_by_threshold()));
    });

    // Large buffer (100 items)
    group.bench_function("large_buffer_100_items", |b| {
        let buffer = BatchBuffer::new();
        for i in 0..100 {
            let report = if i % 3 == 0 { Some("REPORT".to_string()) } else { None };
            buffer.add_to_batch(format!("req-{i}"), vec![1], report, "arn:bench".to_string());
        }
        b.iter(|| black_box(buffer.should_send_batch_by_threshold()));
    });

    group.finish();
}

// ============================================================================
// Benchmark: split_into_chunks — runs on shutdown
// ============================================================================

fn bench_split_chunks(c: &mut Criterion) {
    let config = Arc::new(make_config());
    let mut group = c.benchmark_group("split_into_chunks");

    for &count in &[10, 50, 100, 500] {
        let items: Vec<BatchedAgentPayload> = (0..count)
            .map(|i| make_payload(&format!("req-{i}"), Some("REPORT"), 1024))
            .collect();

        group.bench_with_input(
            BenchmarkId::new("items", count),
            &items,
            |b, items| {
                b.iter(|| {
                    // split_into_chunks takes ownership, so we clone for each iteration
                    let cloned = items.clone();
                    newrelic_lambda_extension::agent::batch::split_into_chunks(
                        cloned,
                        black_box(1_000_000),
                        black_box(&config),
                    )
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark: get_backoff_delay — trivial but called in retry loops
// ============================================================================

fn bench_backoff_delay(c: &mut Criterion) {
    c.bench_function("get_backoff_delay", |b| {
        let mut attempt = 0usize;
        b.iter(|| {
            attempt = (attempt + 1) % 4;
            black_box(get_backoff_delay(black_box(attempt)))
        });
    });
}

// ============================================================================
// Benchmark: Full invocation simulation — add + threshold + build
// ============================================================================

fn bench_full_invocation_cycle(c: &mut Criterion) {
    let config = make_config();

    c.bench_function("full_invocation_cycle", |b| {
        b.iter(|| {
            let buffer = BatchBuffer::new();

            // Simulate 3 warm invocations adding payloads
            for i in 0..3 {
                buffer.add_to_batch(
                    format!("req-{i}"),
                    vec![0u8; 512],
                    Some(format!("REPORT Duration: {}.{} ms", 100 + i, i * 10)),
                    "arn:aws:lambda:us-east-1:123:function:bench-fn".to_string(),
                );
            }

            // Check threshold
            assert!(buffer.should_send_batch_by_threshold());

            // Build payload
            let reports = buffer.get_batch_with_reports_only();
            let payload = build_newrelic_payload(black_box(&reports), black_box(&config), None);

            // Validate (simulates what the caller does)
            assert!(!payload.is_empty());

            // Clear after "send"
            buffer.clear_batch_with_reports(&reports);
            assert!(buffer.buffer.is_empty());
        });
    });
}

// ============================================================================
// Benchmark: APM payload parsing — runs every invocation in APM mode
// ============================================================================

fn bench_parse_agent_payload(c: &mut Criterion) {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;
    use base64::{Engine as _, engine::general_purpose};

    let mut group = c.benchmark_group("parse_agent_payload");

    // Build realistic test payloads of different sizes
    for &num_spans in &[1, 10, 50] {
        let telemetry = serde_json::json!({
            "metric_data": [[1, 2, 3]],
            "span_event_data": (0..num_spans).map(|i| serde_json::json!([i])).collect::<Vec<_>>(),
            "analytic_event_data": [[null, null, []]],
        });

        let json_bytes = serde_json::to_vec(&telemetry).expect("serialize");
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&json_bytes).expect("compress");
        let compressed = encoder.finish().expect("finish");
        let encoded = general_purpose::STANDARD.encode(&compressed);
        let payload = format!("[\"2\", \"NR_LAMBDA_MONITORING\", \"{encoded}\"]");
        let payload_bytes = payload.into_bytes();

        group.bench_with_input(
            criterion::BenchmarkId::new("spans", num_spans),
            &payload_bytes,
            |b, data| {
                b.iter(|| {
                    newrelic_lambda_extension::apm::payload_parser::parse_agent_payload(
                        black_box(data),
                    ).expect("parse should succeed")
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark: DashMap operations — simulates request state lifecycle
// ============================================================================

fn bench_dashmap_request_lifecycle(c: &mut Criterion) {
    use dashmap::DashMap;

    c.bench_function("dashmap_insert_get_remove", |b| {
        let map: DashMap<String, String> = DashMap::new();
        let mut counter = 0u64;
        b.iter(|| {
            counter += 1;
            let key = format!("req-{counter}");
            map.insert(key.clone(), "arn:test".to_string());
            let _ = map.get(&key);
            map.remove(&key);
        });
    });
}

// ============================================================================
// Group and run
// ============================================================================

criterion_group!(
    benches,
    bench_build_payload,
    bench_build_payload_data_size,
    bench_add_to_batch,
    bench_threshold_check,
    bench_parse_agent_payload,
    bench_dashmap_request_lifecycle,
    bench_split_chunks,
    bench_backoff_delay,
    bench_full_invocation_cycle,
);
criterion_main!(benches);
