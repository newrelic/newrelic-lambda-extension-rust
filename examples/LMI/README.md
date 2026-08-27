# LMI Examples

This directory contains Lambda Managed Instances (LMI) examples split by runtime and framework.

## Structure

- `sam/<runtime>/template.yaml`
- `serverless/<runtime>/serverless.yml`

Runtimes included:
- python
- nodejs
- java
- dotnet
- go
- ruby

## Notes

- No personal account IDs, profiles, or local machine paths are included.
- Region is dynamic:
  - SAM uses deployment region.
  - Serverless uses `${opt:region, env:AWS_REGION}`.
- New Relic configuration uses environment variables/placeholders.
- Provide your own layer ARNs and credentials at deploy time.
- Customer-provided values:
  - `NEW_RELIC_ACCOUNT_ID`
  - `NEW_RELIC_LICENSE_KEY`
- Capacity provider handling:
  - SAM examples create one `AWS::Lambda::CapacityProvider` and bind one function using `CapacityProviderConfig`.
  - Serverless examples create one `AWS::Lambda::CapacityProvider` under `resources` and bind the generated function via `resources.extensions.AppLambdaFunction`.

## Runtime Wrapper Behavior

- SAM templates set wrapper handlers explicitly where required:
  - Python: `newrelic_lambda_wrapper.handler` with `NEW_RELIC_LAMBDA_HANDLER`
  - Node CommonJS: `newrelic-lambda-wrapper.handler`
  - Node ESM: `/opt/nodejs/node_modules/newrelic-esm-lambda-wrapper/index.handler`
  - Ruby: `newrelic_lambda_wrapper.handler` with `NEW_RELIC_LAMBDA_HANDLER`
  - Java legacy: `com.newrelic.java.HandlerWrapper::handleRequest` (or stream mode)
  - .NET: native handler + required `CORECLR_*` profiler variables
- Serverless templates keep original handlers by default.
  The `serverless-newrelic-lambda-layers` plugin auto-wraps handlers unless `manualWrapping: true` is enabled.

## LMI Support Requirement

LMI is supported with New Relic Lambda Extension version `2.7.0` and above.

When choosing runtime layers, ensure the layer description indicates extension version `2.7.0+` for your target runtime and architecture.
