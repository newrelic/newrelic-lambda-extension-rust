# CLAUDE.md

Guidance for Claude Code (and other AI coding assistants) working in this
repository. **The Response Discipline and Non-Negotiable Rules below apply
to every response and every change. No exceptions.**

This is the public, open-source `newrelic/newrelic-lambda-extension-rust`
repo — these rules apply to New Relic maintainers and external contributors
alike. A few specific steps below (marked **NR-internal**) require New
Relic AWS access and aren't needed to build, test, or send a PR: `cargo
test`, `cargo build`/`cargo zigbuild`, and the RIE harness under
[tests/rie/](tests/rie/) all run with no New Relic credentials at all, and
CI runs the rest on every PR.

---

## Response Discipline

**Never agree by default. First instinct: stress-test, not validate.**

This applies to every response, not just code reviews. Before affirming
anything — an idea, a strategy, an opinion, a plan — find the weakest
point first.

- **Start from the counter-position.** Ask: what am I not seeing? What's
  the counter-argument? What would someone who disagrees say, and are
  they right? Work back from there.
- **Don't echo framing.** If the user says "I think X is the move," do
  not open with "X is definitely the move," "That makes sense," or any
  variant that parrots the framing. Name the tension or tradeoff first.
- **No glazing.** Don't call something "great," "brilliant," "really
  smart," or "a solid call" unless you can point to specific, concrete
  reasons — and even then, lead with what's wrong, risky, or missing
  before the praise. Compliments without substance are noise.
- **Earn agreement.** When a proposal survives the stress test, say so
  precisely: which specific reasons hold it up, and which alternatives
  were considered and rejected. Agreement without reasoning doesn't count.
- **If you can't find a real weakness, say that explicitly** — "I looked
  for the counter-argument and here's the strongest version; it still
  fails because…". Don't manufacture doubt, but don't skip the step.

This discipline shapes how Rule 1 plans are presented: the plan itself
must already have been stress-tested before the user sees it.

---

## Non-Negotiable Rules

### 1. Plan first, edit after approval

Before touching any file:

1. Present a plan — scope, files to touch, proposed approach, test strategy,
   and expected impact on cold/warm start, memory, and billed duration.
2. **Wait for explicit user approval.** Do not edit code or run mutating
   tools until the plan is approved.
3. If the request is ambiguous or the blast radius is unclear, ask before
   planning — don't assume.

One-line typo fixes and comment-only edits can skip the plan step; anything
that changes behavior, dependencies, or the init path cannot.

### 2. Never regress performance or cost

This extension runs in every Lambda invocation's init path. A tiny
regression multiplies across millions of invocations. Do **not** increase:

- **Cold-start time** (extension INIT duration)
- **Warm-start time** (per-invoke overhead)
- **Memory footprint** (RSS and peak during INVOKE)
- **Billed duration** (Lambda-reported billed ms)

If a change might affect any of these (new dependency, new allocation in
the hot path, new async task, new IO in INIT), flag it explicitly in the
plan and justify it.

### 3. Every change must be tested

- **Unit + integration tests** for all logic changes (`cargo test`) — no
  AWS access needed, runs anywhere.
- **RIE harness** ([tests/rie/](tests/rie/), `bash tests/rie/run_test.sh`)
  for anything touching log buffering, retries, or delivery timing — a
  local Docker Compose setup, no AWS access needed either. See "RIE
  integration test harness" below.
- **NR-internal: real Lambda deployment test** for anything that touches
  the init path, event loop, Extensions/Logs/Telemetry API handlers,
  outbound HTTP, agent IPC, APM collector, or platform log parsing.
  Requires New Relic AWS access — use
  [scripts/testlayer.sh](scripts/testlayer.sh) (publishes a
  `NRTestRustExtension`-prefixed test layer to `us-west-1`). **Do not use
  `createlayer.sh` for testing** — that script is for production/PR-style
  publishes and uses the real layer name prefix. External contributors:
  say so in the PR and a maintainer will run this step.
- **Capture before/after numbers**: cold start, warm start, billed duration,
  max memory. Use a consistent runtime and memory size across runs.

**If Claude cannot run the Lambda deployment test itself (no NR AWS
access), STOP and ask the user to run it and share the numbers, or say so
explicitly in the PR for a maintainer to pick up.** Record the results in
the PR description so we build a proper performance history over time. No
PR should be raised without this evidence (or an explicit waiver).

### 4. Think AWS Lambda lifecycle before writing code

Before proposing an implementation, reason about:

- **INIT phase** — will this block? what runs before the first invoke?
- **INVOKE phase** — what happens per-invocation? what state is reused?
- **Freeze/thaw** — what happens when the sandbox is paused between invokes?
- **SHUTDOWN** — cleanup on `SHUTDOWN` event; is flush guaranteed?
- **Warm vs cold** — which work is one-time vs per-invoke?
- **Cost model** — billed duration, memory-MB-seconds, provisioned concurrency.

If your Claude Code session has AWS/serverless skills installed
(`aws-serverless-eda`, `aws-cost-operations`, `aws-cdk-development`,
`aws-agentic-ai`, via the Skill tool), consult them before, not after,
writing code. These are optional, session-specific tooling — not required
to contribute, and unavailable in a default Claude Code setup.

### 5. If the code-review-graph MCP is available, use it first

Before any Grep/Glob/Read, use the knowledge graph to explore, trace
relationships, and scope impact. This is optional, session-specific
tooling, not a project requirement — see the MCP section at the bottom of
this file for details and what to do if it isn't configured.

### 6. Never log secrets — especially the license key in URLs

The license key is passed to the New Relic collector as a `license_key`
**query parameter** (PreConnect/Connect and the collector send URLs). It also
travels in the `Api-Key` header on the Metric API. **The license key, account
id, or any other secret must never reach a log line** — at any level, including
`debug!`/`trace!`.

- **Never log a full request URL.** Log only `redact_url(&url)`
  ([src/newrelic/client.rs](src/newrelic/client.rs)), which strips the query
  string and fragment, leaving `scheme://host/path`.
- **Never log a raw `reqwest::Error`.** Its `Display` embeds the request URL
  (including `license_key`). Call `e.without_url()` before logging *or*
  propagating it, so downstream log sites stay clean too.
- Never log request headers. When adding any new outbound request or error
  path, verify no secret can appear in the emitted log, then add/extend a test
  asserting the redaction (see `redact_url` tests).

---

## Project Overview

**newrelic-lambda-extension** (Rust) — AWS Lambda Extension that collects
and forwards telemetry from Lambda functions to New Relic without requiring
CloudWatch Logs or Kinesis. Rust port of the original Go extension, tuned
for smaller binary size and faster cold starts.

- Package: `newrelic-lambda-extension` (see [Cargo.toml](Cargo.toml))
- Edition: 2021
- Release profile: `opt-level="z"`, fat LTO, `codegen-units=1`, stripped
- Targets: `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-gnu`

## Source Layout ([src/](src/))

| Module | Responsibility |
|--------|---------------|
| [main.rs](src/main.rs) / [event_loop.rs](src/event_loop.rs) / [runtime.rs](src/runtime.rs) | Extension entrypoint, Extensions API event loop |
| [context.rs](src/context.rs) | Per-invocation context plumbing |
| [error_synthesis.rs](src/error_synthesis.rs) | Synthesize platform timeouts/faults into error events |
| [agent/](src/agent/) | New Relic agent IPC, payload batching, processing |
| [apm/](src/apm/) | APM mode: collector, connection, metric conversion, IDs, telemetry buffer |
| [config/](src/config/) | Environment-variable driven configuration |
| [credentials/](src/credentials/) | License key resolution (env, Secrets Manager, SSM) |
| [logs/](src/logs/) | Logs API subscriber and processor |
| [newrelic/](src/newrelic/) | New Relic HTTP client, payload, flush logic |
| [platform/](src/platform/) | Parses platform START/END/REPORT log lines |
| [request/](src/request/) | Outbound HTTP (proxy-aware) |
| [telemetry/](src/telemetry/) | Local telemetry listener (agent payload ingest) |
| [trace/](src/trace/) | Trace ID / distributed tracing utilities |
| [version/](src/version/) | Build-time version info |

Tests live next to the code they cover (`*_tests.rs`, `mod_test.rs`).

## Build and Test

```sh
# Unit + integration tests
cargo test

# Release build (Linux targets — required for Lambda)
cargo zigbuild --release --target x86_64-unknown-linux-musl
cargo zigbuild --release --target aarch64-unknown-linux-musl

# Build + publish a TEST layer (use this for local testing and perf runs)
#   prefix:  NRTestRustExtension
#   region:  us-west-1 (default)
./scripts/testlayer.sh

# Build + publish the PRODUCTION layer (NOT for ad-hoc testing)
./scripts/createlayer.sh
```

- **NR-internal:** use `testlayer.sh` for every test/perf iteration. It's
  isolated from the real layer namespace so you can publish freely without
  affecting users.
- **NR-internal:** `createlayer.sh` is reserved for production/PR-style
  releases — don't reach for it during development.
- **NR-internal:** for AWS operations in this project, always use
  `AWS_PROFILE=dev-account`. External contributors won't have this
  profile and don't need it — `cargo test`/`cargo build` and the RIE
  harness need no AWS access at all.

### RIE integration test harness

A local Docker Compose harness under `tests/rie/` exercises log-delivery
reliability end-to-end — buffering, retries, timeouts, and eviction — without
touching real AWS or New Relic. It exists because the actual open-source
Lambda RIE stubs out the Telemetry API, so the harness ships its own
`mock_lambda_runtime.py` that fully implements both the Extensions API and
Telemetry API, paired with `mock_nr_server.py` (a configurable mock NR log
endpoint that can fail/hang/randomly-fail on demand).

```sh
bash tests/rie/run_test.sh              # full run: builds extension, builds
                                          # images, runs all 11 scenarios, tears down
bash tests/rie/run_test.sh --build-only  # just cross-compile, skip Docker
bash tests/rie/run_test.sh --no-build    # reuse the existing binary, skip cargo
```

Requirements: Docker with Compose v2, `cargo-zigbuild` or `cross` (the script
builds `aarch64-unknown-linux-musl` itself), and arm64 emulation if running on
an x86_64 host. First run pulls/tags `public.ecr.aws/lambda/python:3.12` base
images (`nr-rie-base:python312[-arm64]`) locally — see [tests/TESTING.md](tests/TESTING.md)
if `docker compose build` fails on a credential-helper prompt; a scoped
`DOCKER_CONFIG` without `credsStore` avoids it without touching global config.

Covers 11 scenarios (happy path, random failure, hang/timeout, buffer
overflow, mixed chaos, auto-flush storm, buffer-cap FIFO eviction, timeout
boundary, selective hang, rapid burst, priority eviction) — see the script
header for details. Current status and known findings (duplicate delivery
under specific multi-batch timeout/retry sequences — not yet root-caused) are
tracked in [tests/TESTING.md](tests/TESTING.md).

A standalone helper, `scripts/count_duplicate_logs.py`, checks an exported
log JSON file for duplicate records (matching timestamp + message + request
ID) — useful when investigating delivery findings like the one above.

## Key Behaviors and Modes

- **Standard mode** — forwards agent payloads to New Relic's telemetry APIs.
- **APM mode** (`NEW_RELIC_APM_LAMBDA_MODE=true`) — functions appear as APM
  entities; platform REPORT lines are converted to `apm.lambda.transaction.*`
  metrics; timeouts/faults become APM error events with DT context.
- **Logs** — controlled by `NEW_RELIC_EXTENSION_SEND_LOGS`
  (`platform`,`extension`,`function`,`all`) which takes precedence over the
  per-type boolean flags. See [README.md](README.md) for full env-var table.
- **Proxy** — `NEW_RELIC_LAMBDA_EXTENSION_PROXY` scopes proxying to the
  extension's outbound traffic only; falls back to `HTTPS_PROXY`/`HTTP_PROXY`.
  Localhost (Extensions API) is never proxied.

## Conventions

- Cold-start cost is load-bearing. Avoid new dependencies without a clear
  justification in the plan.
- Don't skip hooks (`--no-verify`) on commits.
- Commit style: conventional commits (`feat:`, `fix:`, `chore:`, …). Recent
  history uses PR numbers in parens, e.g. `fix: ... (#41)`.
- Tests co-located with source. Add tests for any behavior change in
  `agent/`, `apm/`, `logs/`, `platform/`, or `config/`.

## Performance Evidence in PRs

Every PR that changes runtime behavior must include a results block in
the description:

```
### Perf impact (deployed to Lambda, runtime=<rt>, mem=<MB>, arch=<x86_64|arm64>)
| Metric         | Before | After | Delta |
|----------------|--------|-------|-------|
| Cold start ms  |        |       |       |
| Warm start ms  |        |       |       |
| Billed ms      |        |       |       |
| Max memory MB  |        |       |       |
```

If the user couldn't test yet, say so explicitly in the PR and link back
to this file. No "should be fine" without numbers.

---

## MCP Tools: code-review-graph (optional)

If you have the `code-review-graph` MCP server configured, use it before
Grep/Glob/Read to explore this codebase — it's faster, cheaper (fewer
tokens), and gives structural context (callers, dependents, test coverage)
that file scanning cannot. This is a local, opt-in tool, not a project
dependency — nothing in this repo requires it, and Grep/Glob/Read work
fine without it. If it isn't configured in your session, skip this section
entirely.

Where configured, the graph auto-updates on Edit/Write via a PostToolUse
hook (see [.claude/settings.json](.claude/settings.json), local-only —
not committed).

### Use graph tools first

- **Exploring code** → `semantic_search_nodes` or `query_graph` (not Grep)
- **Understanding impact** → `get_impact_radius` (not manual import tracing)
- **Code review** → `detect_changes` + `get_review_context` (not reading whole files)
- **Relationships** → `query_graph` with `callers_of` / `callees_of` / `imports_of` / `tests_for`
- **Architecture** → `get_architecture_overview` + `list_communities`

Fall back to Grep/Glob/Read only when the graph doesn't cover the need.

### Key tools

| Tool | Use when |
|------|----------|
| `detect_changes` | Reviewing code changes — risk-scored analysis |
| `get_review_context` | Source snippets for review — token-efficient |
| `get_impact_radius` | Blast radius of a change |
| `get_affected_flows` | Which execution paths are impacted |
| `query_graph` | Tracing callers, callees, imports, tests, dependencies |
| `semantic_search_nodes` | Finding functions/classes by name or keyword |
| `get_architecture_overview` | High-level codebase structure |
| `refactor_tool` | Planning renames, finding dead code |

### Workflow

1. Graph auto-updates on file edits.
2. `detect_changes` → risk-scored review context.
3. `get_affected_flows` → understand impact.
4. `query_graph` pattern=`tests_for` → check coverage before declaring done.
