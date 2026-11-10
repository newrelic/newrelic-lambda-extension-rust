# Handoff — Circuit Breaker + p99 Timeout Provider

> Branch: `apm-dual-path-send-tier` (pushed to origin). Read
> `ARCHITECTURE_DUAL_PATH.md` first for the overall shape — this file is
> specifically "what you build and where it plugs in."

---

## 1. What's already built, and where

Everything in `src/send/` is ported and ready — you're implementing against
real, committed trait definitions, not a hypothetical interface:

- `src/send/circuit_breaker.rs` — the `CircuitBreaker` trait + the stub
  (`AlwaysAllowBreaker`) `SendScheduler` currently runs on.
- `src/send/timeout_provider.rs` — the `SendTimeoutProvider` trait + the
  stub (`FixedTimeoutProvider(100ms)`) currently in use.
- `src/send/scheduler.rs` — `SendScheduler` itself, the thing that calls
  into both traits. You don't need to touch this file.

The traits themselves (copy for reference):

```rust
pub trait CircuitBreaker: Send + Sync {
    /// Called before every send attempt. false => skip the send entirely,
    /// drop the data (deliberately lossy — "drop data rather than delay
    /// invokes" per the team decision). No buffering happens in this case.
    fn allow_send(&self, kind: SendKind) -> bool;

    /// Called after every send attempt completes, success or failure.
    fn record_result(&self, kind: SendKind, outcome: SendOutcome);

    /// For observability — current state per kind, for logging transitions.
    fn state(&self, kind: SendKind) -> BreakerState; // Closed | Open | HalfOpen
}

pub trait SendTimeoutProvider: Send + Sync {
    /// The bound used both for the aborting resolve (agent payload) and —
    /// indirectly — informs how long a non-aborting resolve (logs/metrics)
    /// might report StillRunning before the next invocation checks again.
    /// Does NOT know about the current invocation's deadlineMs — see §4.
    fn current_timeout(&self, kind: SendKind) -> Duration;
}
```

`SendKind` is `{ ApmCollector, Logs, MetricApi }` — your breaker/timeout
provider should track state **per kind**, not globally, since the three
endpoints fail/recover independently.

## 2. What you're building

### 2.1 Circuit breaker (NR-643533)

- Cap at **5 consecutive failures** per kind before opening (team-agreed
  number).
- While open: `allow_send` returns `false` — data is dropped, not buffered.
  Confirm this is really what's wanted before building it exactly that way;
  it's a real data-loss decision, not just a performance one.
- Half-open probe after a cooldown: let exactly one real send through;
  success closes the breaker, failure reopens it (reset the cooldown).
- Cooldown duration: not yet specified by the team plan — needs a number
  from you/the team before this is fully spec'd.
- Emit on every state transition (closed→open, open→half-open,
  half-open→closed/reopen) — `tracing::warn!`/`info!` is fine, matching the
  style already used elsewhere in `src/send/`.

### 2.2 p99-driven timeout provider (NR-643534)

- Blocked on: real latency numbers for the logs endpoint and the APM
  collector endpoint from `#help-tdp`. Once you have those, the actual
  `current_timeout()` value should reflect measured p99 healthy-send
  latency per kind, not a guess.
- The plumbing (the struct, the trait impl, wiring p99 tracking) can be
  built now against a placeholder constant — you don't need the real
  numbers to start.
- Whatever population/window you measure p99 over, document it in the
  impl's doc comment — this was flagged as an open question (population
  window, storage across freeze/thaw, cold-start bootstrap value) and
  hasn't been resolved yet.

### 2.3 Fresh connection on slow retry

- Concrete `reqwest`/hyper decision needed: does a slow second attempt get
  a connection-pool-bypassing fresh client, or do you disable keep-alive
  reuse for just that attempt? Needs a short spike — no existing code
  commits to an approach yet.

### 2.4 Degraded-endpoint test harness (NR-643534)

- Slow responses / 5xx / dropped or hung connections, pointed at instead of
  the real collector/log endpoints.
- This should become a **standing pre-release gate**, not a one-off test
  run — the circuit breaker's failure/cooldown policy needs to be validated
  against this, not just against a fixed-outcome mock.

## 3. How to plug your real implementations in

One spot, `event_loop.rs`, where `SEND_SCHEDULER` is constructed:

```rust
static SEND_SCHEDULER: once_cell::sync::Lazy<crate::send::SendScheduler> =
    once_cell::sync::Lazy::new(|| crate::send::SendScheduler::new(
        crate::send::SendMode::Deferred,
        std::sync::Arc::new(crate::send::AlwaysAllowBreaker),           // <- swap this
        std::sync::Arc::new(crate::send::FixedTimeoutProvider(          // <- and this
            std::time::Duration::from_millis(crate::send::tier::DEFAULT_SEND_TIMEOUT_MS)
        )),
    ));
```

Swap the two `Arc::new(...)` arguments for your real types. Nothing else in
`event_loop.rs` needs to change — `SendScheduler`'s public API
(`spawn`/`resolve_pending`/`resolve_pending_no_abort`/`has_pending`) stays
identical regardless of what's behind the trait.

**Do this as an explicit joint session**, not solo — per the original team
plan, swapping stubs for real implementations is the one point where two
independently-tested pieces meet for the first time, and it's worth doing
together in case something surfaces that neither side's tests exercised.

## 4. One open gap neither of us has closed yet

`SendTimeoutProvider::current_timeout()` has no visibility into the current
invocation's `deadlineMs` — it only returns an endpoint-latency-based bound.
Nobody has implemented clamping that bound against the invocation's actual
remaining deadline (the same way `wait_for_runtime_done_with_grace` already
clamps itself, `event_loop.rs`). This is a real correctness gap in the
current code, not assigned to either of us specifically — worth deciding
who owns it before it ships.

## 5. Testing expectations

- Full existing suite (currently 1110 tests on this branch) must stay green
  after your changes.
- Add dedicated unit tests for the breaker's state machine (all
  transitions, including half-open probe success/failure) and the p99
  tracker, mirroring the style already in `src/send/scheduler.rs`'s test
  module (`ToggleBreaker`, a hand-rolled test double, is a reasonable
  pattern to copy from).
- Once the degraded-endpoint harness exists, the integration test proving
  "zero added latency healthy, bounded latency degraded" (already written
  for the scheduler itself in `src/send/scheduler_integration_tests.rs`)
  should be re-run against it, not just against simulated sends.

## 6. Reference

- Tickets: NR-643533 (circuit breaker), NR-643534 (p99 latency / degraded
  testing).
- `#help-tdp` — where to ask for the real latency numbers.
- `ARCHITECTURE_DUAL_PATH.md` — the overall shape this plugs into.
- `DUAL_PATH_PLAN.md` — the original design note for why this branch exists
  as two separate functions instead of one shared pipeline.
