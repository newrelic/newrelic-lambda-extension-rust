[![Community Plus header](https://github.com/newrelic/opensource-website/raw/main/src/images/categories/Community_Plus.png)](https://opensource.newrelic.com/oss-category/#community-plus)

# newrelic-lambda-extension (Rust) [![Build Status](https://github.com/newrelic/newrelic-lambda-extension-rust/actions/workflows/test-layers-pr.yml/badge.svg)](https://github.com/newrelic/newrelic-lambda-extension-rust/actions/workflows/test-layers-pr.yml) [![CI](https://github.com/newrelic/newrelic-lambda-extension-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/newrelic/newrelic-lambda-extension-rust/actions/workflows/ci.yml) [![Coverage](https://codecov.io/gh/newrelic/newrelic-lambda-extension-rust/branch/main/graph/badge.svg)](https://codecov.io/gh/newrelic/newrelic-lambda-extension-rust)

A high-performance Rust implementation of the AWS Lambda extension to collect, enhance, and transport telemetry data from your AWS Lambda functions to New Relic without requiring an external transport such as CloudWatch Logs or Kinesis.

This lightweight AWS Lambda Extension runs alongside your AWS Lambda functions and automatically handles the collection and transport of telemetry data from
supported New Relic serverless agents. The extension requires a telemetry payload from a New Relic agent. Conditions that delay or prevent that payload from being written may result in longer-than-expected invocation durations.

## Installation

To install the extension, simply include the layer with your instrumented
Lambda function. The current layer ARN can be found [here][3].

[3]: https://layers.newrelic-external.com

**Note:** This extension is included with all New Relic AWS Lambda layers going forward.

You'll also need to make the New Relic license key available to the extension. Use the [New Relic Lambda CLI][4]
to install the managed secret, and then add the permission for the secret to your Lambda execution role.

[4]: https://github.com/newrelic/newrelic-lambda-cli

```sh
newrelic-lambda integrations install \
    --nr-account-id <account id> \
    --nr-api-key <api key> \
    --linked-account-name <linked account name>
```

Each of the example functions in the `examples` directory has the appropriate license key secret permission. 

After deploying your AWS Lambda function with one of the layer ARNs from the
link above you should begin seeing telemetry data in New Relic.

See below for details on supported New Relic agents.

## Supported Configurations

The New Relic Extension uses the AWS [Lambda Extensions API](https://docs.aws.amazon.com/lambda/latest/dg/runtimes-extensions-api.html), which supports all Lambda runtimes. For Go lambdas, we suggest using "provided.al2023" or "provided.al2". See the [Custom runtime](https://docs.aws.amazon.com/lambda/latest/dg/runtimes-custom.html) docs for further details.

All of the New Relic Lambda Layers include the Extension and the latest Agent version for the Layer's runtime. The latest 
New Relic Lambda Layer ARNs for your runtime and region are available [here](https://layers.newrelic-external.com/). The 
`NewRelicLambdaExtension` layer is suitable for Go runtime Lambda.

## APM Mode

The extension supports an **APM (Application Performance Monitoring) Mode** that enables Lambda functions to report telemetry directly to New Relic's APM platform, providing deep application insights and entity-level correlation with other APM services.

### Quick Start with APM Mode

Enable APM mode by setting the following environment variables in your Lambda function configuration:

- `NEW_RELIC_APM_LAMBDA_MODE`: `true`
- `NEW_RELIC_LICENSE_KEY`: Your New Relic license key

### APM Mode Features

- **Direct APM Integration**: Lambda functions appear as APM application entities
- **Comprehensive Telemetry**: Metrics, spans, errors, events, and transaction traces
- **Platform Metrics**: Lambda platform logs converted to APM metrics (`apm.lambda.transaction.*`)
- **Enhanced Error Events**: Platform timeouts and faults reported as APM error events with distributed tracing context
- **Entity Correlation**: Function logs sent with Entity GUID for unified observability

### When to Use APM Mode

**Use APM Mode if you need:**
- Lambda functions as APM application entities
- Comprehensive transaction traces and error analytics
- Correlation with other APM-instrumented services
- APM-specific features (Service Maps, distributed tracing)

**Use Standard Mode if you need:**
- Basic serverless monitoring
- Serverless-specific UI views

## Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (1.70 or later)
- AWS CLI configured with appropriate credentials
- For cross-compilation: [cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild) or [cross](https://github.com/cross-rs/cross)

## Building

The extension can be built using cargo-zigbuild (recommended), cross, or standard cargo toolchain.

### Prerequisites for Building

Install one of the following for cross-compilation:

```sh
# Option 1: cargo-zigbuild (recommended)
cargo install cargo-zigbuild

# Option 2: cross
cargo install cross
```

### Build Commands

To build the extension for production:

```sh
# Build for x86_64 architecture
cargo zigbuild --release --target x86_64-unknown-linux-musl

# Build for arm64 architecture
cargo zigbuild --release --target aarch64-unknown-linux-gnu
```

If using `cross` instead of `cargo-zigbuild`:

```sh
# Build for x86_64 architecture
cross build --release --target x86_64-unknown-linux-musl

# Build for arm64 architecture
cross build --release --target aarch64-unknown-linux-gnu
```

The compiled binary will be available at `target/<target>/release/newrelic-lambda-extension`.

### Using the Build Script

For automated builds and layer creation, use the provided script:

```sh
# From the project root
./scripts/createlayer.sh
```

This script automatically handles target installation, builds for multiple architectures, and packages the extension into Lambda layer zip files.

## Deploying

The `scripts/createlayer.sh` script handles building and publishing Lambda layers to multiple AWS regions.

### Basic Usage

To build and publish layers:

```sh
# Build and publish to default regions
./scripts/createlayer.sh

# Customize layer prefix and regions
LAYER_NAME_PREFIX="MyCustomPrefix" \
REGIONS_X86_64="us-east-1 us-west-2" \
REGIONS_ARM64="us-east-1 us-west-2" \
./scripts/createlayer.sh
```

### Manual Layer Publishing

To manually publish a layer to AWS:

1. Build the extension for your target architecture
2. Package it into a zip file with the correct structure:

```sh
# Create layer structure
mkdir -p layer/extensions
cp target/x86_64-unknown-linux-musl/release/newrelic-lambda-extension layer/extensions/
cd layer && zip -r9 ../extension-layer.zip .
cd ..

# Publish to AWS
aws lambda publish-layer-version \
    --layer-name NewRelicLambdaExtension \
    --zip-file fileb://extension-layer.zip \
    --compatible-runtimes provided provided.al2 provided.al2023 \
    --region us-east-1
```

Be sure that the AWS CLI is configured correctly. You can use the usual [AWS CLI environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html) to control the account and region.

## Logging

The New Relic Lambda Extension can send your function's logs to New Relic. If you use the Lambda Extension, you can avoid the CloudWatch Logs ingest charge for telemetry gathered by New Relic.

| Environment variable | Default value | Options | Description |
|--------|-----------|-------------|-------------|
| `NEW_RELIC_EXTENSION_SEND_LOGS` | | `platform`, `extension`, `function`, `all` | **Unified log configuration** - Send specific log types to New Relic using comma-separated values. Use `all` to send all log types. Examples: `function`, `function,platform`, `all`. This takes precedence over individual log flags below. |
| `NEW_RELIC_EXTENSION_SEND_FUNCTION_LOGS` | `false` | `true`, `false`, `1`, `0` | Send function logs to New Relic. Used only if `NEW_RELIC_EXTENSION_SEND_LOGS` is not set. |
| `NEW_RELIC_EXTENSION_SEND_EXTENSION_LOGS` | `false` | `true`, `false`, `1`, `0` | Send extension logs in addition to the function logs to New Relic. Used only if `NEW_RELIC_EXTENSION_SEND_LOGS` is not set. |
| `NEW_RELIC_EXTENSION_SEND_PLATFORM_LOGS` | `false` | `true`, `false`, `1`, `0` | Send platform logs (START, END, REPORT, etc.) to New Relic. Used only if `NEW_RELIC_EXTENSION_SEND_LOGS` is not set. |
| `NEW_RELIC_EXTENSION_LOG_LEVEL` | `info` | `error`, `warn`, `info`, `debug`, `trace` | Set the log level for extension logging. |
| `NEW_RELIC_EXTENSION_LOGS_ENABLED` | `true` | `true`, `false`, `1`, `0` | Enable or disable [NR_EXT] extension log output in CloudWatch. When false, suppresses all extension logs while keeping functionality intact. |
| `NR_TAGS` |  | | Specify tags to be added to all log events. **Optional**. Format: `env:prod;team:myTeam` (colon-delimited key/value, semicolon-delimited pairs).<br><br>**Notes:**<br>- Only affects log events and APM connect Entity Tags.<br>- Does **not** feed Transaction/Span custom attributes — only `NEW_RELIC_LABELS` does (see below). |
| `NR_ENV_DELIMITER` | `;` | Any string | Custom delimiter for `NR_TAGS`. Some users in UTF-8 environments might face difficulty with the default semicolon `;` delimiter. |
| `NEW_RELIC_LABELS` |  | | Specify labels in the standard New Relic cross-agent format. **Optional**. Format: `type1:value1;type2:value2` (fixed `:`/`;` delimiters, not affected by `NR_ENV_DELIMITER`).<br><br>**Where it is used:**<br>- Forwarded log events → `tags.<type>` attributes.<br>- In APM mode (`NEW_RELIC_APM_LAMBDA_MODE=true`), every Transaction and Span event → `tags.<type>` custom attributes (equivalent to calling `newrelic.addCustomAttributes()` in every function). Existing attributes already set by the agent or your code are not overwritten.<br><br>**Validation:**<br>- Duplicate type in `NEW_RELIC_LABELS`: last value wins.<br>- Type/value over 255 chars: truncated, with a warning.<br>- Over 64 entries: list capped, with a warning.<br>- Malformed pair (bad delimiter count, empty type/value): the **entire** list is discarded, with a warning (never partial).<br><br>**With `NR_TAGS`:**<br>- Independent variables: if both are set, both are sent.<br>- `NR_TAGS` does **not** feed Transaction/Span custom attributes; only `NEW_RELIC_LABELS` does.<br>- No cross-variable deduplication.<br>- Entity-level APM connect labels are unprefixed for both variables, so duplicate keys can override each other.<br>- Forwarded logs can also override when keys resolve to the same final attribute name.<br>  Example: `NR_TAGS=tags.team:dev` and `NEW_RELIC_LABELS=team:prod` both become `tags.team` in logs, so `NEW_RELIC_LABELS` wins (`tags.team=prod`). |

## Extension Environment Variables

The New Relic Lambda Extension offers various features, which can be configured using Lambda environment variables:

### Core Configuration

| Environment variable | Default value | Options | Description |
|--------|-----------|-------------|-------------|
| `NEW_RELIC_LICENSE_KEY` | | String | Your New Relic license key. **Required** unless using Secrets Manager or SSM Parameter Store. |
| `NEW_RELIC_LICENSE_KEY_SECRET` | | Secret Name or ARN | Specify the name or ARN of the secret from **AWS Secrets Manager** that contains your New Relic license key.<br><br>**Notes:**<br>- This is only used if `NEW_RELIC_LICENSE_KEY` is not set.<br>- The secret must be in the same AWS region as your Lambda function.<br>- Your Lambda function's execution role needs the `secretsmanager:GetSecretValue` permission. |
| `NEW_RELIC_LICENSE_KEY_SSM_PARAMETER_NAME` | | Parameter Name or ARN | Specify the name or ARN of the parameter from **AWS Systems Manager Parameter Store** that contains your New Relic license key.<br><br>**Notes:**<br>- This is only used if `NEW_RELIC_LICENSE_KEY` is not set.<br>- The SSM parameter must be in the same AWS region as your Lambda function.<br>- Your Lambda function's execution role needs the `ssm:GetParameter` permission. |
| `NEW_RELIC_LAMBDA_EXTENSION_ENABLED` | `true` | `true`, `false` | Enable or disable the extension. |

### APM and Telemetry Configuration

| Environment variable | Default value | Options | Description |
|--------|-----------|-------------|-------------|
| `NEW_RELIC_APP_NAME` | Lambda function name | String | Sets the APM entity name for this Lambda function in New Relic. When set, the function reports under a named entity instead of using the Lambda function name. Also used to group multiple Lambda functions (across regions or deployments) into a single APM entity. Each language runtime creates its own entity — same name across different runtimes results in separate entities. |
| `NEW_RELIC_APM_LAMBDA_MODE` | `false` | `true`, `false`, `1`, `0` | Enable APM mode for deep application monitoring and entity correlation. |
| `NEW_RELIC_APM_BLOCKING_HANDSHAKE` | `false` | `true`, `false`, `1`, `0` | When `true`, the extension holds `/next` after `platform.runtimeDone` until the APM PreConnect+Connect handshake finishes (or the remaining invoke deadline is exhausted). Improves the likelihood that APM is connected before the sandbox is frozen — useful for sparse-traffic functions (infrequent invocations) or very short function timeouts where the background handshake may not complete in time. When `false` (default), the handshake runs in the background and APM connects within a few invocations for high-frequency functions. |
| `NEW_RELIC_APM_HANDSHAKE_TIMEOUT_SECS` | `5` | Number (min: 1) | Maximum seconds to wait for each individual APM PreConnect or Connect request to the New Relic collector. Increase if your function runs in a high-latency network (e.g., cross-region VPC). The total handshake (PreConnect + Connect) can take up to `2 × timeout`. |
| `NEW_RELIC_APM_DISABLE_TELEMETRY` | _(empty)_ | Comma-separated list of: `metric_data`, `custom_event_data`, `log_event_data`, `analytic_event_data`, `error_event_data`, `error_data`, `span_event_data`, `sql_trace_data`, `transaction_sample_data`, `platform_metrics` | APM mode only. Telemetry types listed here are **not sent** (and not buffered/retried). Unknown tokens are ignored with a warning. Example: `NEW_RELIC_APM_DISABLE_TELEMETRY=platform_metrics,sql_trace_data` drops the per-invocation `apm.lambda.*` platform metrics and SQL traces. `platform_metrics` also skips REPORT→metric conversion entirely. Does not affect the APM handshake or error synthesis memory capture. |
| `NEW_RELIC_RUNTIME_DONE_GRACE_MS` | `25` | Number (0–2000) | Grace period in milliseconds added after the `platform.runtimeDone` signal before the end-of-invocation log flush. Only active when the log batch is not already fully drained. Increasing this gives trailing telemetry (emitted by the agent just before the function returns) more time to arrive. Clamped to `[0, 2000]`. |
| `NEW_RELIC_COLLECT_TRACE_ID` | `false` | `true`, `false`, `1`, `0` |Add `trace.id` attribute to Lambda logs for distributed tracing correlation. |
| `NEW_RELIC_TRACE_ID_LOG_BUFFER_MAX` | `2000` | Number (1–100000) | Only used when `NEW_RELIC_COLLECT_TRACE_ID=true`. Max logs parked per invocation while waiting for the agent payload (the `trace.id` source). On overflow, excess logs are sent without `trace.id` (a trace that isn't known yet can't be stamped). Clamped to `[1, 100000]`; invalid values fall back to the default. |
| `NEW_RELIC_ADD_VERSION_DETAIL_TAGS` | `false` | `true`, `false`, `1`, `0` | Add version detail tags to telemetry. |
| `NEW_RELIC_LAYER_VERSION` | | String | Specify the layer version for tracking purposes. |
| `NEW_RELIC_LAMBDA_HANDLER` | | String | Override the Lambda handler value (for agent initialization). |
| `NEW_RELIC_HARVEST_INTERVAL_SECONDS` | `5` | Number | Interval in seconds for periodically flushing logs to reduce memory usage. Does not affect telemetry, which is sent when the Lambda REPORT line is detected. |

### Performance Optimization

| Environment variable | Default value | Options | Description |
|--------|-----------|-------------|-------------|
| `NEW_RELIC_EXTENSION_PIPELINE_FLUSH` | `false` | `true`, `false`, `1`, `0` | **Pipeline flush mode** — when enabled, the extension calls GET /next immediately after `runtimeDone` and flushes telemetry in the background. This removes flush latency from **billed duration** (typically saving 50–200ms per invocation). The in-flight flush is always awaited at the start of the next invocation or during shutdown, so no data is lost. If the TCP connection is broken during a Lambda freeze, the extension retries with exponential backoff on thaw. Recommended for high-frequency functions where per-invocation cost savings matter. |

### Network / Proxy Configuration

| Environment variable | Default value | Options | Description |
|--------|-----------|-------------|-------------|
| `NEW_RELIC_LAMBDA_EXTENSION_PROXY` | | URL | HTTP proxy for the extension's outbound traffic to New Relic. Only affects the extension — does not interfere with your Lambda function's own traffic. Supports `http://`, `https://`, and `socks5://` schemes. Credentials are supported via `http://user:pass@proxy:port` format and are masked in all log output. Localhost traffic (Lambda Extensions API) is never proxied. When not set, the extension respects standard `HTTPS_PROXY`/`HTTP_PROXY` environment variables as a fallback. |
| `NEW_RELIC_DATA_COLLECTION_TIMEOUT` | _(unset)_ | Duration string (e.g. `500ms`, `30s`, `2m`, `1h`) | **(Serverless mode only)** Opt-in total retry budget for sending telemetry and logs. When unset, the extension keeps its original fixed 3-attempt retry behavior unchanged. When set, retries continue with a growing backoff (200ms, doubling every 3 attempts, capped at 3s) until this much wall-clock time has elapsed, with a 20-attempt safety cap. Invalid values (e.g. a bare number with no unit) fall back to a 10s budget. |
| `NEW_RELIC_HTTP_TIMEOUT` | `2400ms` | Duration string (e.g. `500ms`, `30s`, `2m`, `1h`) | **(Serverless mode only)** Opt-in per-request timeout for telemetry and log sends to New Relic, overriding the default 2.4s. Invalid values fall back to the 2400ms default.<br><br>**Cold-start race condition:** Lambda cold starts typically add 200ms–1s+ of latency before the first response arrives. If this timeout is set too close to your function's average response time, it can fire just before a legitimate response completes — causing a failure even though the Lambda succeeded. To avoid this, set `NEW_RELIC_HTTP_TIMEOUT` at least 1–1.5s above the average cold-start duration of your function. Requests that finish faster close immediately; the extra headroom is only used when needed. |

**When to use `NEW_RELIC_LAMBDA_EXTENSION_PROXY`:**

If your Lambda runs in a VPC with no direct internet access and routes outbound traffic through an HTTP proxy, set this variable to route only the extension's traffic through the proxy. This avoids using the process-wide `HTTPS_PROXY` variable, which would also affect your application's own HTTP traffic.

```sh
# Example: route extension traffic through a VPC proxy
NEW_RELIC_LAMBDA_EXTENSION_PROXY=http://proxy.internal:3128

# Example: with authentication
NEW_RELIC_LAMBDA_EXTENSION_PROXY=http://user:pass@proxy.internal:3128
```

## Testing

### Unit and Integration Tests

Run the Rust test suite:

```sh
# Run all tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run specific test
cargo test test_name
```

### Testing in AWS Lambda

The most reliable way to test the extension is to deploy it to an actual AWS Lambda function:

1. Build and package the extension using the [build script](#using-the-build-script)
2. Deploy the extension layer to your AWS account
3. Attach the layer to a test Lambda function
4. Configure the required environment variables (see [Extension Environment Variables](#extension-environment-variables))
5. Invoke your Lambda function and verify telemetry in New Relic

### Local Testing Limitations

**Note:** Local testing of Lambda extensions is complex and has significant limitations:
- AWS Lambda extensions rely on the Lambda runtime environment and Extensions API

For development and testing purposes, we recommend:
- Using the Rust test suite for unit and integration tests
- Deploying to a development AWS Lambda function for end-to-end testing
- Using AWS SAM or Terraform for infrastructure-as-code deployments

## Why Rust?

This Rust implementation provides several advantages over the original Go implementation:

- **Smaller Binary Size**: Optimized for size with aggressive LTO and stripping
- **Faster Cold Starts**: Reduced initialization time and memory footprint
- **Lower Memory Usage**: More efficient memory management
- **Enhanced Safety**: Rust's type system and ownership model prevent common bugs

## Support

New Relic hosts and moderates an online forum where customers can interact with New Relic employees as well as other customers to get help and share best practices. Like all official New Relic open source projects, there's a related Community topic in the New Relic Explorers Hub. You can find this project's topic/threads [in the Explorers Hub](https://discuss.newrelic.com/t/new-relic-lambda-extension/111715).

## Contributing

We encourage your contributions to improve `newrelic-lambda-extension-rust`! Keep in mind when you submit your pull request, you'll need to sign the CLA via the click-through using CLA-Assistant. You only have to sign the CLA one time per project.

If you have any questions, or to execute our corporate CLA, required if your contribution is on behalf of a company, please drop us an email at opensource@newrelic.com.

## License

`newrelic-lambda-extension-rust` is licensed under the [Apache 2.0](http://apache.org/licenses/LICENSE-2.0.txt) License. The `newrelic-lambda-extension-rust` also uses source code from third-party libraries. You can find full details on which libraries are used and the terms under which they are licensed in the [third-party notices document](THIRD_PARTY_NOTICES.md).
