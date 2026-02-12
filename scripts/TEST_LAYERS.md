# Test Layer Creation

This document explains how to use the unified test layer creation system.

## Overview

We've consolidated all language-specific test layer scripts into a single unified script: `scripts/createlayer.sh`. This script can build and publish Lambda layers for all supported languages:

- Python
- Node.js  
- Java
- .NET
- Ruby

## Manual Usage

### Build All Test Layers

To build test layers for all languages in `us-west-1`:

```bash
export TEST_MODE="true"
export LANGUAGES="python nodejs java dotnet ruby"
./scripts/createlayer.sh
```

### Build Specific Languages

To build only specific languages:

```bash
export TEST_MODE="true"
export LANGUAGES="python nodejs"  # Only Python and Node.js
./scripts/createlayer.sh
```

### Environment Variables

- `TEST_MODE`: Set to `"true"` for test layers, `"false"` for production (default: `"true"`)
- `LAYER_NAME_PREFIX`: Prefix for layer names (default: `"NRTestRustExtension"` in test mode)
- `BUCKET_PREFIX`: S3 bucket prefix (default: `"nr-extension-test-layers"`)
- `REGIONS_X86_64`: Space-separated list of regions for x86_64 (default: `"us-west-1"`)
- `REGIONS_ARM64`: Space-separated list of regions for arm64 (default: `"us-west-1"`)
- `LANGUAGES`: Space-separated list of languages to build (default: `"python nodejs java dotnet ruby"`)

## Automated PR Testing

The GitHub Actions workflow `.github/workflows/test-layers-pr.yml` automatically builds and publishes test layers when:

1. A pull request is opened, synchronized, or reopened
2. The PR includes changes to:
   - Rust source code (`src/**`)
   - `Cargo.toml` or `Cargo.lock`
   - Scripts directory (`scripts/**`)
3. The PR does NOT only change documentation (`*.md`, `docs/**`)

### Workflow Features

- **Automatic Layer Creation**: Builds layers for all languages in both x86_64 and arm64
- **PR Comments**: Posts layer ARNs as a comment on the PR with "TEST LAYERS" notice
- **Artifacts**: Uploads layer ARNs as GitHub artifacts (30-day retention)
- **Build Summary**: Creates a job summary with all layer ARNs
- **Job Outputs**: Exports layer ARNs for potential downstream jobs

### Required Secrets

Configure these secrets in your GitHub repository:

- `AWS_ACCESS_KEY_ID`: AWS access key with permissions to publish Lambda layers
- `AWS_SECRET_ACCESS_KEY`: AWS secret access key

### AWS Permissions

The AWS credentials need these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::nr-extension-test-layers-*/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "lambda:PublishLayerVersion",
        "lambda:AddLayerVersionPermission"
      ],
      "Resource": "arn:aws:lambda:*:*:layer:NRTestRustExtension*"
    }
  ]
}
```

## Layer Naming Convention

Test layers are named with the following pattern:

- `NRTestRustExtension{Language}{Architecture}`

Examples:
- `NRTestRustExtensionPythonX86`
- `NRTestRustExtensionPythonARM64`
- `NRTestRustExtensionNodejsX86`
- `NRTestRustExtensionJava17X86`
- `NRTestRustExtensionDotnetARM64`
- `NRTestRustExtensionRubyX86`

## Output

The script generates a file `dist/nr_tmp_env.sh` containing environment variables with all layer ARNs:

```bash
export LAYER_ARN_PYTHON_X86_64='arn:aws:lambda:us-west-1:...:layer:NRTestRustExtensionPythonX86:1'
export LAYER_ARN_PYTHON_ARM64='arn:aws:lambda:us-west-1:...:layer:NRTestRustExtensionPythonARM64:1'
export LAYER_ARN_NODEJS_X86_64='arn:aws:lambda:us-west-1:...:layer:NRTestRustExtensionNodejsX86:1'
# ... and more
```

Load these into your environment:

```bash
source dist/nr_tmp_env.sh
```

## Dependencies

The script requires:

- Rust toolchain with `cargo-zigbuild` or `cross`
- Python 3.12+
- Node.js 20+
- Java 17+
- Ruby 3.3+
- Bundler (for Ruby)
- AWS CLI configured with appropriate credentials
- `jq` for JSON parsing

## Troubleshooting

### Build Failures

If a specific language fails to build, you can exclude it:

```bash
export LANGUAGES="python nodejs"  # Skip java, dotnet, ruby
./scripts/createlayer.sh
```

### AWS Authentication

Ensure your AWS credentials are configured:

```bash
aws sts get-caller-identity
```

For the GitHub workflow, verify the secrets are set:
- Go to repository Settings → Secrets and variables → Actions
- Ensure `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are configured

### Cross-compilation Issues

On macOS, you need `cargo-zigbuild` or `cross` for Linux targets:

```bash
pip3 install ziglang
cargo install cargo-zigbuild
```

## Migration from Old Scripts

The following scripts are now consolidated into `createlayer.sh`:

- ❌ `testlayer.sh` - Use `createlayer.sh` with `TEST_MODE=true`
- ❌ `javaTestLayer.sh` - Use `createlayer.sh` with `LANGUAGES=java`
- ❌ `dotnetTestLayer.sh` - Use `createlayer.sh` with `LANGUAGES=dotnet`
- ❌ `rubyTestLayer.sh` - Use `createlayer.sh` with `LANGUAGES=ruby`

These old scripts can be removed or kept for backward compatibility.

## Workflow Structure

The PR workflow follows this structure:

```
build-test-layers job:
├─ Setup environment (Rust, Python, Node.js, Java, Ruby)
├─ Build and publish test layers
├─ Capture layer ARNs as job outputs
├─ Comment on PR with layer ARNs
├─ Upload artifacts
└─ Create build summary
```

This structure allows for:
- **Job outputs**: Layer ARNs available for downstream jobs if needed
- **Clear separation**: Single job focused on building and commenting
- **Extensibility**: Easy to add testing jobs that depend on this one
