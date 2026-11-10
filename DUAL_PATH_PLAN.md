# Dual-Path Send Tier — Plan

> Supersedes the "one unified pipeline" approach from `apm-unified-telemetry-flush`
> for the purpose of `default`/`medium` mode. Manager directive (2026-11-10): do
> **not** merge/optimize `default` onto the new shared scheduler. `default`/`medium`
> = the literal existing code on `main`, untouched. `high` = the new code we've
> already built and benchmarked. This directly resolves the regression-scope
> concern raised earlier: if `default` never changes a single line, there is zero
> new testing surface for it, full stop.
>
> Branch: `apm-dual-path-send-tier`, created fresh from `origin/main` at `f05e0ce`
> (confirmed clean — no uncommitted carryover from the other branch).

---

## 1. What "default/medium" actually is today (verified, not assumed)

`main`'s current code still runs on the **pre-existing `NEW_RELIC_EXTENSION_PIPELINE_FLUSH`** mechanism (`event_loop.rs`, `pipeline_flush` field, `should_defer_via_pipeline_flush`) — this predates even this session's Phase 1 work, which only existed on the other branch and is **not** what `main` ships today. This is the exact code `default`/`medium` tier must stay pinned to: no `SendScheduler`, no `AGENT_SEND_SCHEDULER`/`SEND_SCHEDULER`, no `resolve_pending_*` functions, nothing. If `NEW_RELIC_SEND_TIER` is unset or `default`, the extension must behave **exactly** as `main` does right now — bit-for-bit, not "equivalent behavior through new code."

## 2. Design: fork at the top, not shared internals

The previous approach (one `SendScheduler`, `default` configured to behave synchronously) is explicitly **not** what we're building now. Instead:

```
execute_apm_mode_event_loop (or wherever the invoke dispatch lives)
│
├─ tier == Default/unset  →  existing pipeline_flush-based code path,
│                             completely unmodified, zero new functions
│                             called, zero new statics touched
│
└─ tier == High           →  new code path: SendScheduler-based,
                              ported from apm-unified-telemetry-flush,
                              entirely separate functions/statics
```

The fork happens at **one** decision point per send concern (agent payload, logs, platform-metrics) — not threaded through shared internals. Two genuinely separate implementations, selected once per concern, not one implementation parameterized by tier.

## 3. What gets ported from `apm-unified-telemetry-flush` (additive only)

These are self-contained and were already fully tested in isolation — port as-is, no changes needed to the primitives themselves:

- `src/send/` — the whole module: `SendScheduler`, `CircuitBreaker`/`SendTimeoutProvider` traits + stubs, `SendTier`, `BoxedSend`, outcome-mapping helpers. Purely additive; nothing in `main` currently references any of it, so copying it in changes nothing for `default`.
- The `high`-tier implementations of the three send concerns (`spawn_log_flush`/`resolve_pending_logs`, `spawn_platform_metrics_flush`/`resolve_pending_platform_metrics`, the agent-payload `SEND_SCHEDULER`-based spawn/resolve) — ported in, but called **only** when `tier == High`.

## 4. What does NOT get ported / must NOT be touched

- `APM_SEND_RUNTIME_DONE_GRACE`, `resolve_agent_send_at_boundary`, `PendingAgentSend`/`PENDING_AGENT_SEND`, `pipeline_flush`, `should_defer_via_pipeline_flush` — these don't exist as a "replace" target here, because **`main` never had them removed in the first place**. `main`'s own pipeline_flush-based code is the thing staying as `default`, as-is.
- No shared helper functions between the two paths "for consistency" — if `default` and `high` happen to want similar logic, each implements its own, even if that means some duplication. Duplication is the accepted cost of zero regression risk for `default`, per the manager's explicit instruction.

## 5. The one shared thing: the tier knob itself

`NEW_RELIC_SEND_TIER` parsing (`SendTier::from_env()`, `config.extension.send_tier`) is the only piece touching shared ground (`config/mod.rs`) — and it's inert for `default` (reading an env var and storing an enum value changes nothing about behavior until something branches on it). This is safe to add unconditionally.

## 6. Testing requirement (the whole point of this pivot)

- [ ] **Diff-verify `default` tier's code paths are byte-identical to `main`'s current behavior** — not just "tests pass," but confirm the functions `default` tier calls are the *same* functions `main` has today, untouched.
- [ ] Full existing suite green, same pass count as `main`'s current baseline, before adding anything new.
- [ ] New tests only for the `high`-tier path (ported from the other branch, already written).
- [ ] No test added for `default` tier beyond what `main` already has — if `default` needed new tests, that would mean its behavior changed, which it must not.

## 7. Phases

1. **Port `src/send/` module wholesale** (Phase A equivalent) — copy files, wire into `main.rs`'s module list, confirm `cargo build`/`cargo test` still green with zero behavior change (nothing calls it yet).
2. **Add `NEW_RELIC_SEND_TIER` knob** to `config/mod.rs` — inert until step 3.
3. **Add the fork point(s)** in `event_loop.rs` — one per send concern (agent payload, logs, platform-metrics), each branching `if tier == High { new path } else { existing pipeline_flush code, unmodified }`.
4. **Port the `high`-tier implementations** behind each fork, reusing the already-tested logic from `apm-unified-telemetry-flush`.
5. **Full regression + diff audit** confirming `default` is untouched.
6. **Benchmark `high` tier again on this branch** to confirm parity with the numbers already gathered (extension overhead ≤~1ms at every percentile).

## 8. Open question to confirm before coding

Logs and platform-metrics were **never tier-gated** in the other branch's design (always detached, regardless of tier) — they only became "tracked" (not aborted, checked at next invocation) as part of the unified-scheduler work. Under this dual-path model: should logs/metrics get a `high`-tier-only tracked version too, or does "dual path" apply strictly to agent payload only (the one concern that's actually tier-sensitive), leaving logs/metrics as `main`'s existing unconditional-detached behavior for **both** tiers? Recommend the latter (simpler, smaller diff) unless told otherwise — confirm before building.
