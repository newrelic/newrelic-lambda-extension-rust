# Test Coverage — newrelic-lambda-extension

## Quick Reference

| Layer | Count | Command | Status |
|---|---|---|---|
| Rust unit tests | **392** | `cargo test` | ✅ All pass |
| RIE integration scenarios | **11** | `bash tests/rie/run_test.sh` | ⚠️ 9 pass, 2 known findings — see below |

Per-module unit test counts below reflect the harness's original 286-test baseline and
are directional, not exact, against the current 392.

### RIE integration scenario status (last verified 2026-07-23)

All 11 scenarios are implemented in `tests/rie/run_test.sh` (see file header for the
full list). Current status:

- **9/11 pass**, including buffer-cap FIFO eviction (scenario 7) and extension-log
  priority eviction (scenario 11) — both updated to assert against the extension's
  documented `MAX_RETRIES = 3` bound (`src/logs/processor.rs`) instead of a stale
  near-total-recovery expectation from before that policy existed.
- **2 known open findings, left failing intentionally** (scenarios 8 and 9): both show
  *duplicate* log delivery (not loss) during multi-batch HTTP-timeout/retry sequences —
  e.g. scenario 8 delivers 108 total logs for only 58 unique messages. This differs from
  scenario 3's single-batch hang/retry, which delivers cleanly with no duplicates. Not
  yet root-caused — could be inherent at-least-once ambiguity (server completes a write
  the client already gave up on and re-buffered) or a genuine double-send race in the
  auto-flush/retry interaction. The assertions are left as-is (not loosened) so this
  keeps surfacing until someone investigates.

---

## 1. Unit Tests (286 total)

Run with: `cargo test`

### Summary by module

| Module | Tests | Source file(s) | What it covers |
|---|---|---|---|
| `config` | 86 | `src/config/mod.rs` | Env-var parsing, ARN construction, account-ID extraction, license-key sources, log-level validation, tag/log-type parsing, proxy URL handling |
| `logs` | 50 | `src/logs/processor_tests.rs` | Log-level extraction (structured/unstructured), retry buffer push/eviction, `LogType` enum, `start_invocation_retry`, flush handle lifecycle |
| `agent` | 49 | `src/agent/batch_tests.rs`, `src/agent/payload_tests.rs` | APM batch accumulation, payload chunking, ARN extraction, shutdown send paths |
| `request` | 32 | `src/request/mod_tests.rs` | Active/telemetry request-ID tracking, orphan-payload draining, coordination channel, cleanup |
| `telemetry` | 30 | `src/telemetry/listener_tests.rs` | Telemetry record deserialisation, HTTP handler for all event types, concurrent requests, platform-report routing |
| `apm` | 16 | `src/apm/` | APM app creation, error-event generation, metric conversion, ID generation, payload parsing |
| `newrelic` | 7 | `src/newrelic/client.rs` | Proxy URL building, credential masking |
| `credentials` | 7 | `src/credentials/credentials_test.rs` | SSL-cert-file merging, idempotency, non-PEM skip |
| `trace` | 5 | `src/trace/mod_tests.rs` | Trace-ID extraction from compressed/uncompressed payloads (v1, v2) |
| `version` | 3 | `src/version/` | Layer ARN parsing, version-info creation |
| `platform` | 1 | `src/platform/processor_tests.rs` | Runtime-version string normalisation |

---

### 1.1 `config` module (86 tests)

| Test name | What it verifies |
|---|---|
| `test_from_env_defaults_when_not_set` | All fields get correct defaults when no env vars present |
| `test_from_env_license_key` | Plain `NEW_RELIC_LICENSE_KEY` is read |
| `test_from_env_license_key_secret` / `_ssm` | Secret-ARN and SSM-path forms are accepted |
| `test_from_env_proxy_url_*` (5 tests) | Proxy URL absent / set / HTTPS / empty string / with auth |
| `test_from_env_proxy_startup_log_never_leaks_credentials` | Proxy password never appears in log output |
| `test_parse_bool_*` (3 tests) | `true/false/1/0/yes/no` all parse correctly, case-insensitive |
| `test_parse_send_logs_*` (10 tests) | `NEW_RELIC_EXTENSION_SEND_LOGS` comma-list, `all` keyword, individual-flag fallback, precedence |
| `test_parse_nr_tags_*` (9 tests) | `NEW_RELIC_METADATA_TAGS` parsing: empty, single, custom delimiter, whitespace |
| `test_construct_function_arn_*` (7 tests) | ARN built from region env vars, defaults, empty parts |
| `test_extract_account_id_from_arn_*` (6 tests) | Account-ID extracted from valid/invalid/malformed ARNs |
| `test_validate_log_level_*` (3 tests) | Invalid levels fall back to `info` |

### 1.2 `logs` module (50 tests)

| Test name | What it verifies |
|---|---|
| `test_structured_level_*` (10 tests) | JSON log bodies: `level` field, `WARNING`→`warn`, `FATAL`→`error`, `INFORMATION`, `Verbose`→`trace` |
| `test_unstructured_*` (8 tests) | Plain-text prefix detection: `ERROR`, `WARN`, `DEBUG`, `TRACE`, `CRITICAL`→`error`, `FATAL`→`error` |
| `test_level_prefix_beats_body_keyword` | Log-level prefix (`ERROR:`) overrides a different keyword inside the body |
| `test_multiple_keywords_earliest_position_wins` | When multiple level words appear, leftmost wins |
| `test_word_boundary_*` (3 tests) | `errorfoo` is not a match; `[error]` is |
| `test_case_insensitive_matching` | `Error`, `ERROR`, `error` all match |
| `test_serilog_format` / `test_powertools_structured_info` | Framework-specific log formats parsed correctly |
| `test_json_payload_in_message` | JSON string embedded in message field still extracts level |
| `test_aws_lambda_timeout` / `test_http_status_codes` / `test_stack_trace_with_error` | Lambda-specific log patterns |
| `test_log_type_from_record_type` | `"platform"` → `Platform`, `"extension"` → `Extension`, unknown → `Function` |
| `test_log_type_from_message_roundtrip` | `_nr.logType` attribute round-trips through `LogMessage` |
| `test_log_type_missing_defaults_to_function` | Missing attribute → `Function` (safe default) |
| `test_push_below_cap_adds_entry` | Entry pushed when buffer has space |
| `test_overflow_evicts_extension_first` | At cap, an Extension log is evicted before Function/Platform |
| `test_overflow_drops_incoming_extension_when_no_extension_in_buf` | Incoming Extension dropped when buffer is all Function |
| `test_overflow_drops_incoming_platform_when_buf_all_function` | Incoming Platform dropped when buffer is all Function |
| `test_overflow_function_fifo_evicts_oldest` | All-Function buffer: oldest entry is evicted (FIFO) |
| `test_failed_log_entry_clone_preserves_log_type` | `FailedLogEntry::clone()` keeps `log_type` intact |
| `test_start_invocation_retry_empty_buffer_does_not_set_handle` | No task spawned when buffer is empty |
| `test_start_invocation_retry_sets_handle_when_buffer_has_entries` | Task handle stored when retry starts |
| `test_start_invocation_retry_drains_buffer` | Buffer is empty immediately after `start_invocation_retry` |
| `test_exhausted_entries_not_sent` | Entries with `retry_count >= MAX_RETRIES` are dropped silently |
| `test_flush_clears_invocation_retry_handle` | `flush()` takes and awaits the retry handle; slot is `None` after |

### 1.3 `agent` module (49 tests)

| Test name | What it verifies |
|---|---|
| `test_add_to_batch_*` (5 tests) | Payload stored, same request-ID replaced, empty ARN/request-ID do not block |
| `test_split_chunks_*` (5 tests) | Chunking at 1 MB: empty input, all-fit, multi-chunk, single oversized item |
| `test_estimate_*` (4 tests) | Payload size estimation with and without REPORT line |
| `test_threshold_*` (5 tests) | Batch-send threshold: 0/2/3 reports, items without reports ignored |
| `test_cleanup_old_entries_*` (3 tests) | Stale entries removed, recent kept |
| `test_send_batched_payloads_*` (3 tests) | Empty returns early; APM mode skips version line; sends and clears |
| `test_shutdown_send_*` (3 tests) | Shutdown drains batch buffer and per-request buffers |
| `test_arn_*` / `test_valid_arn_*` (5 tests) | ARN segment extraction, fallback, malformed inputs |

### 1.4 `request` module (32 tests)

| Test name | What it verifies |
|---|---|
| `test_current_active_request_id_*` (2 tests) | Set/read/overwrite active request ID |
| `test_telemetry_request_id_*` (3 tests) | Telemetry ID starts None, set by `platform.start`, overwritten on rapid starts |
| `test_orphaned_payloads_*` (2 tests) | Orphan store starts empty; payloads stored and drained |
| `test_route_payload_to_*` (4 tests) | Payload routed to active buffer, any buffer, or orphan when no buffers |
| `test_coordination_*` (3 tests) | Channel signalled per payload, payload-before-poll, timeout no payload |
| `test_cleanup_*` (10 tests) | Removes request state, preserves others, skip-buffer flag, stale removal |
| `test_multiple_requests_get_independent_buffers` | Separate per-request-ID isolation |
| `test_create_request_processing_state_drains_orphans` | Orphans drain into new state on creation |
| `test_late_payload_after_active_request_cleared` | Payload arriving after active ID cleared goes to orphan |

### 1.5 `telemetry` module (30 tests)

| Test name | What it verifies |
|---|---|
| `test_deserialize_*` (6 tests) | All telemetry record types deserialise correctly from JSON |
| `test_handle_*_via_http` (8 tests) | Real HTTP POST to listener: function logs, extension logs, mixed batch, platform events, invalid JSON |
| `test_handle_platform_report_*` (5 tests) | Report matched to batch buffer, request buffer, empty buffer (pending), APM mode, standard mode |
| `test_platform_start_*` (5 tests) | Updates telemetry request ID, overwrites on rapid starts, handles missing ID |
| `test_concurrent_telemetry_requests` | Two parallel HTTP requests handled correctly |
| `test_setup_telemetry_listener_returns_addr` | Listener binds and returns a valid address |

---

## 2. Integration Tests

### 2.1 Infrastructure

The integration test harness lives in `tests/rie/`. It replaces the AWS Lambda Runtime Interface Emulator with a custom Python implementation that fully supports the Telemetry API.

| Component | File | Purpose |
|---|---|---|
| Orchestration script | `tests/rie/run_test.sh` | Builds extension, starts Docker stack, runs all scenarios, tears down |
| Lambda runtime mock | `tests/rie/mock_lambda_runtime.py` | Full Python replacement for AWS RIE — implements Extensions API + Telemetry API with actual forwarding |
| New Relic endpoint mock | `tests/rie/mock_nr_server.py` | Records every POST to `/log/v1`; configurable 500-failure injection; `/stats`, `/messages`, `/reset`, `/config` endpoints |
| Extension container | `tests/rie/Dockerfile.lambda` | arm64 musl binary + mock runtime + startup script |
| Mock NR container | `tests/rie/Dockerfile.mock` | amd64 Python image running mock NR server |
| Compose file | `tests/rie/docker-compose.yml` | Wires both containers, `sandbox→127.0.0.1` extra-host mapping |
| Startup script | `tests/rie/start.sh` | Starts mock runtime first, waits for port 9001, then starts extension |

**Why a custom runtime instead of AWS RIE:**
The open-source AWS RIE returns `HTTP 202` with body `{"errorMessage":"Telemetry API is not supported"}` for every `PUT /2022-07-01/telemetry` call. The extension only checks the status code (202 = accepted), so telemetry subscription silently succeeds but no events are ever forwarded. The custom mock correctly registers subscribers and POSTs real telemetry events to them.

### 2.2 Mock runtime API surface

| Method | Path | What it does |
|---|---|---|
| `POST` | `/2020-01-01/extension/register` | Registers extension, returns UUID identifier |
| `GET` | `/2020-01-01/extension/event/next` | Long-polls until INVOKE or SHUTDOWN event queued; blocks per-request |
| `PUT` | `/2022-07-01/telemetry` | Stores subscriber URI; **actually forwards events** |
| `POST` | `/2015-03-31/functions/function/invocations` | Test trigger: generates synthetic logs, queues INVOKE event, forwards telemetry to subscriber |

### 2.3 Integration scenarios

| # | Name | Setup | What happens | Assertions |
|---|---|---|---|---|
| 1 | **Happy path** | No failures | 3 invocations (5 + 1 + 0 logs); each flush triggers the previous | Total logs ≥ 6; zero duplicates |
| 2 | **Retry without duplication** | `fail_first_n=2` | 4 invocations (5+3+1+0 logs); first 2 HTTP sends return 500; buffered logs retried next invocation | Total logs ≥ 9; zero duplicates; all 3 batches land correctly |
| 3 | **Buffer overflow** | `always_fail=true` | 10 invocations × 30 logs (= 300 logs; exactly at cap); then re-enable sends | Extension still alive after overflow; 0 logs received during fail window; buffer drains after recovery |
| 4 | **Message-level diagnostic** | No failures | 2 invocations (3 + 2 logs); inspect every received message | Total ≥ 5; zero duplicates; log-type breakdown printed per HTTP request |
| 5 | **Retry exhaustion** | `always_fail=true`, 5 invocations, then enable | Rapid invocations exhaust `MAX_RETRIES`; some logs dropped; sends re-enabled | Extension alive; fresh logs delivered normally after exhaustion (≥ 5); exhausted logs silently dropped |
| 6 | **Rapid-fire** | No failures | 5 invocations in quick succession, each with 2 logs, plus flush | Total ≥ 10 function logs; zero duplicates; all 5 prefixes confirmed present (2 msgs each) |

### 2.4 Per-scenario assertion counts

| Scenario | Pass assertions | What is measured |
|---|---|---|
| 1 — Happy path | 2 | Log count ≥ 6, unique count = total |
| 2 — Retry no-dup | 2 + info | Log count ≥ 9, unique = total (+ HTTP request count printed) |
| 3 — Overflow | 4 | Extension alive, 0 logs during fail, drain produces ≥ 1 log |
| 4 — Diagnostic | 2 + info | Log count ≥ 5, unique = total (+ full message list + type breakdown printed) |
| 5 — Exhaustion | 2 + info | Extension alive after exhaustion, fresh logs ≥ 5 after recovery |
| 6 — Rapid-fire | 7 | Total ≥ 10, unique = total, each of 5 prefixes has ≥ 2 messages |

---

## 3. How to Run

### Unit tests only
```bash
cargo test
```

### Full integration suite (builds extension + Docker)
```bash
bash tests/rie/run_test.sh
```

### Integration only — skip Rust build
```bash
bash tests/rie/run_test.sh --no-build
```

### Build extension and Docker images but don't run tests
```bash
bash tests/rie/run_test.sh --build-only
```

### One-off manual invoke (stack must be up)
```bash
# Start stack
DOCKER_BUILDKIT=0 COMPOSE_DOCKER_CLI_BUILD=0 \
  docker compose -f tests/rie/docker-compose.yml up -d

# Send an invocation
curl -X POST http://localhost:9000/2015-03-31/functions/function/invocations \
  -H "Content-Type: application/json" \
  -d '{"log_prefix":"manual","log_count":5}'

# Check what arrived at mock NR
curl http://localhost:9999/stats
curl http://localhost:9999/messages

# Tear down
DOCKER_BUILDKIT=0 COMPOSE_DOCKER_CLI_BUILD=0 \
  docker compose -f tests/rie/docker-compose.yml down -v
```

### Inject failures manually
```bash
# Fail the next 3 sends
curl -X POST http://localhost:9999/config \
  -H "Content-Type: application/json" -d '{"fail_first_n":3}'

# Fail all sends indefinitely
curl -X POST http://localhost:9999/config \
  -H "Content-Type: application/json" -d '{"always_fail":true}'

# Re-enable sends
curl -X POST http://localhost:9999/config \
  -H "Content-Type: application/json" -d '{"always_fail":false}'

# Reset stats
curl -X POST http://localhost:9999/reset
```

---

## 4. Known Gaps

| Gap | Why it matters | Status |
|---|---|---|
| Platform log type classification not yet in NR payload | `_nr.logType` attribute not yet stripped before send (Phase 3 of retry refactor plan) | Pending — plan exists |
| `start_invocation_retry` not wired into `event_loop.rs` | Extension logs `"No failed telemetry to retry"` at every INVOKE instead of draining buffer | Pending — Phase 5 of plan |
| No test for `NEW_RELIC_EXTENSION_SEND_PLATFORM_LOGS=false` filtering | Config flag exists but integration test does not verify platform events are absent when disabled | Not yet written |
| No APM-mode integration test | APM mode (`NEW_RELIC_LAMBDA_EXTENSION_APM_LAMBDA_MODE=true`) path not exercised in Docker harness | Not yet written |
| No SHUTDOWN lifecycle test | Extension SHUTDOWN event handling (flush before exit) not covered by integration tests | Not yet written |
| Real AWS environment | Integration tests run locally against mock; no CI job fires against real Lambda | Out of scope for local harness |
