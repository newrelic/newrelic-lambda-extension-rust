# Dual-Path Send-Tier Architecture

> Branch: `apm-dual-path-send-tier` (pushed to origin, from a clean `origin/main`
> at `f05e0ce`). This document explains the architecture — what the two modes
> are, how they're selected, and exactly where each one's code lives. Paired
> with `HANDOFF_SASHWAT.md` for what you specifically need to build and where
> to plug it in.

---

## 1. The two modes

One customer-facing knob: `NEW_RELIC_SEND_TIER=default|high`.

| | `default` (the default) | `high` |
|---|---|---|
| Who it's for | Low/medium-frequency customers, and anyone who hasn't opted in | High-invocation-frequency / ultra-fast-function customers |
| What runs | `main`'s existing `pipeline_flush`-era code, **completely unmodified** | New code: spawn-then-resolve-at-next-invocation, via `SendScheduler` |
| Code location | `process_apm_request_default` (`event_loop.rs`) | `process_apm_request_high` (`event_loop.rs`) |
| Shares code with the other mode? | **No** | **No** |

This is a deliberate architectural choice, not an oversight: the two modes are **two separate functions with zero shared internals**, selected by one `match` at the top of `process_apm_request`. There is no unified pipeline configured by a tier parameter — `default` genuinely never touches any of the new code, and `high` genuinely never touches the old `pipeline_flush` logic.

**Why this shape, specifically:** an earlier design (on a different branch, `apm-unified-telemetry-flush`) built one shared `SendScheduler` that both tiers ran through, with `default` just configured to behave synchronously. That was rejected — a reviewer (correctly) pointed out that if `default` mode's *code* changes at all, even if its *behavior* is proven identical, the testing surface and regression risk for your lowest-risk customers grows for no benefit. This branch's design fixes that: `default`'s code is byte-for-byte what shipped before, verified by direct diff, not just by passing the same tests.

---

## 2. What `process_apm_request_default` does (untouched)

Exactly what `main` does today for APM mode, per invocation:

1. Agent payload (if present): spawned via `tokio::spawn` the instant it's ready, fire-and-forget (joined much later, loosely, just to log success/failure — never bounded, never buffered-on-timeout).
2. Platform-report-metrics (if a `platform.report` arrived): sent **synchronously**, blocking, awaited inline before `/next`.
3. `wait_for_runtime_done_with_grace` (pre-existing, unrelated to this work — up to `NEW_RELIC_RUNTIME_DONE_GRACE_MS`, default 25ms).
4. Logs + platform-processor flush: both awaited via `tokio::join!`, blocking, no bound beyond whatever `flush()` itself takes.

Nothing here changed. If you're debugging a `default`-tier issue, you're debugging the exact same code that existed before this effort started.

## 3. What `process_apm_request_high` does (new)

Same three concerns, routed through `SEND_SCHEDULER` (a `crate::send::SendScheduler` instance, `SendMode::Deferred`, built in `event_loop.rs`):

1. **At the very top of the function** (before any of this invocation's own work): resolve whatever the *previous* `high`-tier invocation deferred —
   - `resolve_pending_agent_batch()` — agent payload, **can abort** a straggler (bounded by `SendTimeoutProvider`, currently `FixedTimeoutProvider(100ms)`), buffering the unresolved tail via a progress cursor.
   - `resolve_pending_logs(request_id)` — **never aborts** (a log flush's chunk list has no externally visible cursor; killing it mid-flight would lose data with nothing to buffer).
   - `resolve_pending_platform_metrics(request_id)` — **never aborts**, same reasoning as logs.
2. Agent payload (if present): spawned into `SEND_SCHEDULER` (`SendKind::ApmCollector`) the instant it's ready. Never waited on in this invocation — resolved at the *next* invocation's step 1, or at `SHUTDOWN`.
3. Platform-report-metrics (if present): spawned into `SEND_SCHEDULER` (`SendKind::MetricApi`) via `spawn_platform_metrics_flush`. Guarded against overlap with a previous still-running send via `has_pending`.
4. `wait_for_runtime_done_with_grace` — same pre-existing function, shared with `default` (it's generic, not tier-specific).
5. Logs: spawned into `SEND_SCHEDULER` (`SendKind::Logs`) via `spawn_log_flush`, same overlap guard as metrics. `PlatformProcessor::flush()` isn't called at all here — it's a hardcoded no-op on `main` (platform.* lines already live in `LogProcessor`'s own batch), so there's nothing to call.

**`SHUTDOWN`** (only for `high` tier, guarded by `send_tier == High`): drains all three — `resolve_pending_agent_batch`, `resolve_pending_logs("shutdown")`, `resolve_pending_platform_metrics("shutdown")` — since there's no next invocation to defer to.

## 4. The shared primitive: `SendScheduler`

Lives in `src/send/scheduler.rs`, ported wholesale (already built and tested on the other branch). One instance (`SEND_SCHEDULER`, a `once_cell::sync::Lazy` in `event_loop.rs`) covers all three `SendKind`s (`ApmCollector`, `Logs`, `MetricApi`) via an internal map — "one per kind" at the data level, not three separate struct instances.

Two resolution styles, picked per kind based on whether aborting is safe:

- **`resolve_pending`** (aborting): races the pending send against a timeout; on timeout, aborts the task and reports how far a progress cursor got, so the caller buffers only the genuinely-unresolved tail. Used for agent payload, where each item in the batch already self-buffers on its own failure (`send_agent_payload_or_buffer`), so losing *only* the not-yet-attempted items on abort is safe and recoverable.
- **`resolve_pending_no_abort`** (never kills the task): checks if it's finished; if not, just reports `StillRunning` and leaves it running. Used for logs and platform-metrics, where the underlying implementation (`LogProcessor::flush`, `send_platform_report_metrics`) holds its data in a way that isn't externally recoverable if killed mid-flight — so the design never gives itself the option to kill it.

## 5. What's deliberately still a stub (see `HANDOFF_SASHWAT.md`)

`SendScheduler` is built against two trait interfaces, both currently backed by stubs:

- `CircuitBreaker` (`src/send/circuit_breaker.rs`) — stub is `AlwaysAllowBreaker`, which never opens.
- `SendTimeoutProvider` (`src/send/timeout_provider.rs`) — stub is `FixedTimeoutProvider(100ms)`.

Both are deliberately left this way per the team's "circuit breaker and other things we can leave aside for now" decision. Swapping in real implementations is a one-line change at `SEND_SCHEDULER`'s construction site in `event_loop.rs`.

## 6. Current verification status

- **Build/test**: 1110 tests passing, stable across repeated runs, zero new clippy warnings vs. `main`'s baseline (confirmed by diffing against a clean `git stash` at every commit, not assumed).
- **Live**: a test layer (`NRTestDualPathDotnetX86`/`NRTestDualPathExtensionX86`, us-west-1 + ap-south-1) has been built and published from this branch's current `HEAD`, but not yet exercised live/invoked — unlike the other branch, which was benchmarked with 500-1000 real invocations. That's the next step if you want to validate this branch's `high` tier the same way.
