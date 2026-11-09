# Send-Path Redesign — Shared Checklist

> Ultimate aim: ship two customer-facing send-tier modes behind one
> `NEW_RELIC_SEND_TIER` knob —
> **`high`** (high-invocation-frequency / ultra-fast-function customers,
> non-blocking deferred sends) and **`default`** (today's medium/low-frequency
> behavior, synchronous sends with strong delivery guarantees). One pipeline,
> shared internals, no duplicated code between the two modes.
>
> Full design: `REDESIGN_PLAN.md`. Collaboration/branch contract:
> `TEAM_COLLAB_PLAN.md`. Execution tracker for Phase 1's groundwork:
> `PROJECT_PLAN.md`. This file is the flat, printable checklist for both of
> us to tick off together. Last fully re-verified: 2026-11-09, after wiring
> all three send kinds (`ApmCollector`/`Logs`/`MetricApi`) into the live
> invoke loop — 1102 tests passing, zero new clippy warnings.

---

## DONE — real, wired into the live invoke loop, not just built in isolation

- [x] **`NEW_RELIC_SEND_TIER=high|default` knob** — parsed once at startup
      into `config.extension.send_tier`, logged at startup. Invalid value
      warns + falls back to `default`, never panics. (`src/send/tier.rs`,
      `config/mod.rs`)
- [x] **`SendScheduler`** — the shared primitive, with batch/cursor support
      (`DispatchOutcome { result, cursor }`) and a non-aborting resolve path
      (`resolve_pending_no_abort` + `has_pending`) for sends that can't
      safely be killed mid-flight. (`src/send/scheduler.rs`)
- [x] **Agent payload (`SendKind::ApmCollector`)** — spawned the instant the
      payload is ready via `SEND_SCHEDULER.spawn`, resolved at *this*
      invocation's boundary under `default` tier, or at the *next*
      invocation's start under `high` tier. Uses the aborting
      `resolve_pending` + cursor, since each payload in the batch already
      self-buffers on failure (`send_agent_payload_or_buffer`) — only the
      truly-unresolved tail needs re-buffering on a timeout.
- [x] **Logs (`SendKind::Logs`)** — spawned via `spawn_log_flush`, resolved
      via `resolve_pending_logs`, **never tier-gated** (was already
      unconditionally detached before `SendTier` existed, stays that way for
      both tiers), **never aborted** (`LogProcessor::flush`'s chunk list is
      a local variable with no externally visible cursor — aborting it would
      lose data with nothing to buffer). `has_pending` guard prevents two
      concurrent flushes from ever overlapping.
- [x] **Platform-report-metrics (`SendKind::MetricApi`)** — same pattern as
      logs exactly: `spawn_platform_metrics_flush` / `resolve_pending_platform_metrics`,
      never tier-gated, never aborted (`send_platform_report_metrics`
      already self-buffers via `metric_api_buffer::buffer_failed_metric_api`
      and always returns `Ok`), `has_pending`-guarded against overlap.
- [x] **Removed, not left alongside:** `APM_SEND_RUNTIME_DONE_GRACE`,
      `PendingAgentSend`/`PENDING_AGENT_SEND`, `stash_pending_agent_send`,
      `resolve_previous_pending_agent_send`, `resolve_agent_send_at_boundary`,
      `log_agent_send_join_result` — all deleted when superseded by
      `SendScheduler`, not kept as dead duplicates.
- [x] **Removed a genuine no-op:** the `PlatformProcessor::flush()` spawn in
      APM mode did nothing (hardcoded `Ok(())` — platform.* lifecycle logs
      already go into `LogProcessor`'s own batch at `process_record` time,
      flushed by `spawn_log_flush`). Deleted, not wired — there was nothing
      to wire.
- [x] **SHUTDOWN** resolves all three kinds (agent/logs/metrics) before the
      final durable-buffer sweep, using the same non-destructive resolve
      functions as the normal invoke path.
- [x] **Dead-code audit, done honestly, twice** — every `#[allow(dead_code)]`
      in `src/send/` was verified by physically removing it and rebuilding
      clean, not assumed. Current genuinely-unused list (all **new** code
      kept intentionally for Sashwat's work, not old leftovers):
      `BreakerState`, `CircuitBreaker::allow_send`/`state`,
      `DispatchResult::Dropped`/`Deferred`, `SendScheduler.mode` +
      `mode()`/`dispatch()`/`send_now()`/`drain_all()`, `from_client_result`,
      `from_metric_api_result`, `SendTier::blocking_connect`.
- [x] **Full regression**: 1102 tests passing, stable across repeated clean
      runs (one unrelated pre-existing flaky test in
      `apm/connection_tests.rs`, untouched this session, reproduces as
      passing in isolation — not a regression). Clippy warning count in
      touched files: 46, same as the 48-pre-session baseline minus deletions
      — zero new warnings at every single commit.

## NOT DONE — explicitly, not forgotten

- [ ] **`CircuitBreaker` real implementation** (NR-643533, Sashwat) — `SEND_SCHEDULER`
      still runs on the `AlwaysAllowBreaker` stub. Left aside deliberately,
      per explicit instruction.
- [ ] **`SendTimeoutProvider` real p99-driven implementation** (NR-643534,
      Sashwat) — still `FixedTimeoutProvider(100ms)`. Same placeholder the
      old shipped code already used (behavior-preserving), swap-ready.
- [ ] **Integration session** — swapping the two stubs above for real
      implementations once they land. Blocked on Sashwat's work landing, not
      on anything here.
- [ ] **`blocking_connect`** (generalizing `NEW_RELIC_APM_BLOCKING_HANDSHAKE`
      to logs/metric-API cold-start connect warm-up) — built, tested, not
      wired to anything. Separate piece of work, not started.
- [ ] **Deadline-vs-timeout clamp** — `SendTimeoutProvider`'s bound is never
      clamped against the current invocation's remaining `deadlineMs`. Noted
      as the caller's responsibility in `SendScheduler`'s docs; not yet
      implemented anywhere. Real gap — a misconfigured/very-high timeout
      could in theory run past the function's own deadline today.
- [ ] **Degraded-endpoint test harness** (Sashwat) — slow/5xx/dropped
      connections. Doesn't exist yet; the n/n+1/n+2 integration tests prove
      the scheduler's own behavior against *simulated* sends, not a real
      degraded endpoint.
- [ ] **Fresh-connection-on-retry mechanism** (Sashwat) — not started.
- [ ] **Multi-runtime validation** (Phase G) — not started, blocked on
      nothing except time; the wiring is runtime-agnostic by construction.
- [ ] **`README.md`** still documents the removed
      `NEW_RELIC_EXTENSION_PIPELINE_FLUSH` flag — stale, not yet corrected.
      Needs a docs pass documenting `NEW_RELIC_SEND_TIER` instead.
- [ ] **Kickoff message to Sashwat** — both plan docs and the real trait
      files have existed since early in this work; message never actually
      sent. He's unblocked but may not know it.

## Explicitly out of scope this whole session (not oversights)

- **LMI mode** (`event_loop_lmi.rs`) — no `/next` cycle, never touched.
- **Serverless mode** (`process_request_concurrently`, the other event-loop
  function) — has its own, separate, untouched send paths (including its
  own unrelated `platform_processor.flush()` call) — out of scope throughout,
  not audited or changed.
- **`client.rs`/`collector.rs` internal unification** — deliberately not
  done; `SendTarget`'s interface-only adapter wraps them without rewriting
  retry/backoff/error-type internals.

---

## Division of work (unchanged from original plan)

### Avinash — all items above marked DONE, plus:
- [ ] Send Sashwat the kickoff message + both plan docs
- [ ] `README.md` correction (Phase H)

### Sashwat
- [ ] Circuit breaker (NR-643533)
- [ ] p99 latency numbers + real `SendTimeoutProvider` (NR-643534)
- [ ] Fresh-connection-on-retry mechanism
- [ ] Degraded-endpoint test harness

### Joint (needs both of you, or sign-off from manager)
- [ ] Integration session: swap stubs for real implementations once ready
- [ ] Confirm with manager: is the low-frequency + ultra-fast-function gap
      (customers who need `high`-tier behavior but wouldn't get it under a
      frequency-only `high`/`default` split) real/present today, or genuinely
      rare enough to leave for a future `low` tier
- [ ] Confirm LMI stays explicitly out of scope
- [ ] Agree on one shared failure-counting model between the scheduler's
      buffer-on-timeout path and the circuit breaker's 5-failure cap
- [ ] Decide on the deadline-vs-timeout clamp — who implements it and where
- [ ] Re-measure Go's unexplained `PostRuntimeExtensionsDuration` anomaly
      under the new scheduler before recommending `high` tier for Go
      customers
