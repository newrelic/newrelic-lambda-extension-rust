# LMI Examples

Deployable examples showing how to run a New Relic-instrumented Lambda function on
**Lambda Managed Instances (LMI)** — AWS's Lambda compute type that runs your function
on EC2 instances you control (instance type, VPC placement, EC2 Savings Plans/Reserved
Instances), while AWS still manages scaling, patching, and routing for you.

Two things make LMI different from standard Lambda, and both matter for how these
examples are wired up:

- **No standalone "Lambda function" resource.** A function only becomes invocable once
  it's attached to a **capacity provider** — the EC2 instances your function actually
  runs on. Every example here creates its own capacity provider alongside the function.
- **Multi-concurrency.** One execution environment can run several invocations at the
  same time (standard Lambda runs exactly one invocation per environment). This is why
  New Relic's Node.js Lambda wrapper enables the agent's `worker_threads` instrumentation
  automatically when it detects LMI — without it, concurrent invocations on the same
  environment can hang. No action needed on your part; just make sure your Node.js layer
  is recent enough to include the fix (anything published after
  [newrelic/newrelic-lambda-layers#540](https://github.com/newrelic/newrelic-lambda-layers/pull/540)).

Five runtimes are covered — `python`, `nodejs`, `java`, `dotnet`, `go` — each as both a
SAM template and a Serverless Framework config, so you can pick whichever tooling you
already use.

> **Ruby is not covered here.** AWS rejects Lambda Managed Instances deploys for the
> Ruby runtime with an explicit runtime-not-supported error — this is an AWS platform
> limitation, not something these examples can work around. A `ruby` example will be
> added once AWS adds support.

## Before you deploy — prerequisites

You'll need, once, regardless of which runtime/tool you pick:

1. **A VPC with subnets and a security group** the capacity provider's EC2 instances
   will launch into. These need outbound internet access (a NAT gateway, or a public
   subnet) — the instances must reach both the AWS Lambda control plane and New Relic's
   collector endpoints. If they can't, the extension will connect to the Telemetry API
   fine (it's local) but nothing ever reaches New Relic.

2. **An IAM role for the capacity provider** (`CapacityProviderOperatorRoleArn` /
   `CAPACITY_PROVIDER_OPERATOR_ROLE_ARN`) that AWS Lambda assumes to manage the EC2
   instances on your behalf. Create it once:

   ```bash
   aws iam create-role \
     --role-name LambdaCapacityProviderOperatorRole \
     --assume-role-policy-document '{
       "Version": "2012-10-17",
       "Statement": [{
         "Effect": "Allow",
         "Principal": { "Service": "lambda.amazonaws.com" },
         "Action": "sts:AssumeRole"
       }]
     }'

   aws iam attach-role-policy \
     --role-name LambdaCapacityProviderOperatorRole \
     --policy-arn arn:aws:iam::aws:policy/AWSLambdaManagedEC2ResourceOperator
   ```

   That single AWS-managed policy (`AWSLambdaManagedEC2ResourceOperator`) is all the
   role needs — no inline policies required. Grab the resulting role ARN for the
   deploy commands below.

3. **A New Relic account ID and license key** (the ingest license key, not a User key).

4. **The New Relic layer ARN for your runtime and region** (SAM only — see below), at
   extension version `2.7.0` or later. `2.7.0` is the first extension version with LMI
   support at all; check the layer's description for the extension version before using
   it. Look up ARNs at <https://layers.newrelic-external.com>.

## Repository layout

```
sam/<runtime>/template.yaml     SAM template + src/ handler code
serverless/<runtime>/serverless.yml   Serverless Framework config + src/ handler code
```

## Two ways to deploy: SAM vs. Serverless

| | SAM (`sam/`) | Serverless (`serverless/`) |
|---|---|---|
| New Relic layer | You pass `NewRelicLayerArn` explicitly | Attached automatically by the `serverless-newrelic-lambda-layers` plugin |
| Handler wrapping | You set the wrapper handler yourself (see table below) | Plugin rewrites the handler automatically (keep your original handler in `serverless.yml`), unless `manualWrapping: true` |
| Capacity provider config | CloudFormation parameters | Environment variables (see below) |
| Best for | Full control, no plugin dependency | Least setup — plugin handles layer + wrapping |

Pick one per runtime; you don't need both.

### Deploying with SAM

Every SAM template takes the same parameter set. Runtime-specific extras are called
out per-runtime below.

| Parameter | Required | Notes |
|---|---|---|
| `NewRelicLayerArn` | Yes | Full layer ARN, extension `2.7.0+` |
| `NewRelicAccountId` | Yes | |
| `NewRelicLicenseKey` | Yes | `NoEcho` — won't show in console/CLI output |
| `CapacityProviderOperatorRoleArn` | Yes | From the prerequisites step above |
| `SubnetIds` | Yes | Comma-separated |
| `SecurityGroupIds` | Yes | Comma-separated |
| `CapacityProviderName` | No | Default `lmi-example-cp` |
| `MaxVCpuCount` | No | Default `48` — see note below |
| `PerExecutionEnvironmentMaxConcurrency` | No | Default `8` |

```bash
cd sam/nodejs
sam build
sam deploy \
  --stack-name lmi-nodejs-example \
  --resolve-s3 \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides \
    NewRelicLayerArn=<your-layer-arn> \
    NewRelicAccountId=<your-account-id> \
    NewRelicLicenseKey=<your-license-key> \
    CapacityProviderOperatorRoleArn=<role-arn-from-prereqs> \
    SubnetIds=<subnet-1>,<subnet-2> \
    SecurityGroupIds=<sg-1>
```

Runtime-specific parameters:
- **nodejs**: `NodeUseEsm` (`true`/`false`, default `false`) — set `true` if your handler
  is an ES module. This switches the `Handler` to the dedicated
  `newrelic-esm-lambda-wrapper`; the `NEW_RELIC_USE_ESM` env var it also sets is a no-op
  in that case (that flag only matters for the *legacy* path — using the CommonJS
  wrapper's dynamic `import()` — not when you point `Handler` at the ESM wrapper
  directly, which is what this template does).

### Deploying with Serverless Framework

Requires the [`serverless-newrelic-lambda-layers`](https://github.com/newrelic/serverless-newrelic-lambda-layers)
plugin (already listed in each `serverless.yml`) and the framework's own AWS
credentials setup. Configuration comes from environment variables instead of CLI flags:

| Env var | Required | Notes |
|---|---|---|
| `NEW_RELIC_ACCOUNT_ID` | Yes | |
| `NEW_RELIC_LICENSE_KEY` | Yes | |
| `CAPACITY_PROVIDER_OPERATOR_ROLE_ARN` | Yes | |
| `CAPACITY_PROVIDER_SUBNET_IDS` | Yes | Comma-separated |
| `CAPACITY_PROVIDER_SECURITY_GROUP_IDS` | Yes | Comma-separated |
| `CAPACITY_PROVIDER_MAX_VCPU_COUNT` | No | Default `48` |
| `CAPACITY_PROVIDER_PER_ENV_MAX_CONCURRENCY` | No | Default `8` |

```bash
cd serverless/nodejs
npm install

export NEW_RELIC_ACCOUNT_ID=<your-account-id>
export NEW_RELIC_LICENSE_KEY=<your-license-key>
export CAPACITY_PROVIDER_OPERATOR_ROLE_ARN=<role-arn-from-prereqs>
export CAPACITY_PROVIDER_SUBNET_IDS=<subnet-1>,<subnet-2>
export CAPACITY_PROVIDER_SECURITY_GROUP_IDS=<sg-1>

sls deploy --region <region> --capacityProviderName <name>
```

`--region`/`AWS_REGION` and `--capacityProviderName` are optional (default `dev` stage,
region from your CLI config, capacity provider name `lmi-example-cp`).

## Verifying the deployment (read this before you invoke)

**Editing a function's configuration doesn't reach its running capacity — publishing a
version does.** LMI capacity is bound to a specific *published* function version. If you
change environment variables, layers, or anything else on `$LATEST` after the initial
deploy, those changes sit as unpublished drift until you explicitly publish:

```bash
aws lambda publish-version --function-name <function-name>
aws lambda invoke --function-name <function-name> --qualifier <version-number> \
  --payload '{}' out.json && cat out.json
```

Invoking without `--qualifier` (i.e. `$LATEST`) after any post-deploy edit will silently
run the *old* published version, not your latest change — this looks identical to your
change simply "not working."

A couple of other things you'll observe that are expected, not errors:
- `aws lambda get-function` may report `State: ActiveNonInvocable` right after deploy or
  a config update — this clears once the capacity provider finishes provisioning
  instances; it isn't itself an invocation failure.
- The first invocation after publishing a version can trigger AWS launching up to three
  fresh execution environments in parallel (AZ resiliency) before marking the version
  active — expect a short delay on the very first call.

## Runtime wrapper behavior reference

| Runtime | Handler set by the template | Notes |
|---|---|---|
| Python | `newrelic_lambda_wrapper.handler`, real handler via `NEW_RELIC_LAMBDA_HANDLER` | |
| Node.js (CommonJS) | `newrelic-lambda-wrapper.handler` | |
| Node.js (ESM) | `/opt/nodejs/node_modules/newrelic-esm-lambda-wrapper/index.handler` | Dedicated ESM wrapper — `NEW_RELIC_USE_ESM` is not needed here (see note above) |
| Java | Your own handler class directly (e.g. `com.newrelic.lambda.example.App::handleRequest`) | **Not** `com.newrelic.java.HandlerWrapper` — that class is for standard/non-LMI Lambda only. LMI runs in APM mode, and the agent attaches via `AWS_LAMBDA_EXEC_WRAPPER=/opt/newrelic-java-handler` instead of replacing the handler |
| .NET | Your native handler directly | No wrapper — instrumented via the `CORECLR_*` profiler env vars, already set in the template |
| Go | Your binary directly (`bootstrap`) | No wrapper — instrumented via the `newrelic/go-agent/v3` SDK in code (see `src/main.go`) |

Serverless configs keep your original handler as-is; the
`serverless-newrelic-lambda-layers` plugin rewrites it at deploy time unless you set
`manualWrapping: true` in `custom.newRelic`.

## Cleanup

Capacity provider EC2 instances are billable for as long as they exist, independent of
whether you're invoking the function. Tear down when you're done:

```bash
# SAM
cd sam/nodejs
sam delete --stack-name lmi-nodejs-example

# Serverless
cd serverless/nodejs
sls remove
```

`sam delete` also cleans up the artifacts SAM uploaded to its managed S3 bucket, not just
the CloudFormation stack — prefer it over a raw `aws cloudformation delete-stack`.

Deleting the stack/service also deletes the capacity provider, which terminates its
underlying EC2 instances — there's no separate manual EC2 cleanup step.
