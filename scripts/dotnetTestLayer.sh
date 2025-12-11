#!/usr/bin/env bash
set -euo pipefail

# Local testing script for building and publishing .NET Lambda layers with Rust extension
# Adapted from testlayer.sh for .NET runtime

# Ensure we run from repo root so paths resolve
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

# --- Configuration ---
LAYER_NAME_PREFIX=${LAYER_NAME_PREFIX:-"NRTestDotnetRustExtension"}

BUCKET_PREFIX=${BUCKET_PREFIX:-"nr-extension-test-layers"}
REGIONS_X86_64=${REGIONS_X86_64:-"us-west-1"}
REGIONS_ARM64=${REGIONS_ARM64:-"us-west-1"}

# .NET agent configuration
# Auto-fetch latest version from GitHub if not specified
if [ -z "${NEWRELIC_DOTNET_AGENT_VERSION:-}" ]; then
  echo "Fetching latest .NET agent version from GitHub..." >&2
  NEWRELIC_DOTNET_AGENT_VERSION=$(curl -fsSL https://api.github.com/repos/newrelic/newrelic-dotnet-agent/releases/latest | jq -r '.tag_name' | sed 's/^v//')
  if [ -z "$NEWRELIC_DOTNET_AGENT_VERSION" ] || [ "$NEWRELIC_DOTNET_AGENT_VERSION" = "null" ]; then
    echo "Error: Failed to fetch latest .NET agent version from GitHub" >&2
    exit 1
  fi
  echo "Latest .NET agent version: ${NEWRELIC_DOTNET_AGENT_VERSION}" >&2
fi

AGENT_DOWNLOAD_BASE_URL="https://download.newrelic.com/dot_net_agent/latest_release"

# Extension configuration
BIN_NAME="newrelic-lambda-extension"
BUILD_DIR="lib"  # AWS Lambda .NET layers use lib/ directory
DIST_DIR="$ROOT_DIR/dist"
LAYER_DIR="$ROOT_DIR/.layer"
TMP_ENV_FILE_NAME="$DIST_DIR/nr_tmp_env_dotnet.sh"

# --- Build and Package Functions ---

# Builds the Rust extension for a given target
build_extension() {
  local target="$1"
  echo "Building extension for target $target" >&2

  if ! rustup target list --installed | grep -q "$target"; then
    echo "Rust target $target not installed. Installing..." >&2
    rustup target add "$target"
  fi

  if command -v cargo-zigbuild >/dev/null 2>&1; then
    echo "Building with cargo-zigbuild for $target" >&2
    cargo zigbuild --release --target "$target" --target-dir "$ROOT_DIR/target"
  elif command -v cross >/dev/null 2>&1; then
    echo "Building with cross for $target" >&2
    cross build --release --target "$target" --target-dir "$ROOT_DIR/target"
  else
    if [[ "$(uname -s)" == "Darwin" && "$target" == *"unknown-linux"* ]]; then
      echo "Error: Cross-compiling to Linux on macOS requires 'cross' or 'cargo-zigbuild'." >&2
      exit 1
    fi
    echo "Building with cargo for $target (native toolchain)" >&2
    cargo build --release --target "$target" --target-dir "$ROOT_DIR/target"
  fi
}

# Downloads the .NET agent for the specified architecture
download_dotnet_agent() {
  local arch="$1"  # amd64 or arm64
  local agent_file="newrelic-dotnet-agent_${NEWRELIC_DOTNET_AGENT_VERSION}_${arch}.tar.gz"
  local download_url="${AGENT_DOWNLOAD_BASE_URL}/${agent_file}"
  
  echo "Downloading .NET agent version ${NEWRELIC_DOTNET_AGENT_VERSION} for ${arch}" >&2
  
  rm -rf "$LAYER_DIR/$BUILD_DIR"
  mkdir -p "$LAYER_DIR/$BUILD_DIR"
  
  local tmp_agent="/tmp/${agent_file}"
  curl -fsSL "$download_url" -o "$tmp_agent"
  
  echo "Extracting .NET agent to $LAYER_DIR/$BUILD_DIR" >&2
  tar -xzf "$tmp_agent" -C "$LAYER_DIR/$BUILD_DIR"
  
  # Create version.txt for tracking
  echo "$NEWRELIC_DOTNET_AGENT_VERSION" > "$LAYER_DIR/$BUILD_DIR/newrelic-dotnet-agent/version.txt"
  
  rm -f "$tmp_agent"
  echo ".NET agent downloaded and extracted successfully" >&2
}

# Builds a complete .NET layer with agent + extension
build_dotnet_layer() {
  local target="$1"
  local arch_linux="$2"  # x86_64 or arm64 (Linux arch naming)
  local arch_dotnet="$3" # amd64 or arm64 (.NET agent naming)
  local zip_name="$DIST_DIR/dotnet-${arch_linux}.zip"

  echo "Building New Relic layer for .NET 6, 7, 8 (${arch_linux})" >&2

  rm -rf "$LAYER_DIR"
  mkdir -p "$LAYER_DIR"
  
  # Download and extract .NET agent
  download_dotnet_agent "$arch_dotnet"
  
  # Add Rust extension
  mkdir -p "$LAYER_DIR/extensions"
  cp "$ROOT_DIR/target/$target/release/$BIN_NAME" "$LAYER_DIR/extensions/$BIN_NAME"
  
  # Create the layer zip
  (cd "$LAYER_DIR" && zip -r9 "$zip_name" . >/dev/null)
  
  echo "Build complete: $zip_name" >&2
  echo "$zip_name"
}

# --- AWS Publish Functions ---

hash_file() {
  if command -v md5sum &>/dev/null; then
    md5sum "$1" | awk '{ print $1 }'
  else
    md5 -q "$1"
  fi
}

publish_layer() {
  local layer_archive="$1"
  local region="$2"
  local runtime_name="$3"
  local arch="$4"
  local layer_name="$5"

  local hash
  hash=$(hash_file "$layer_archive")
  local bucket_name="${BUCKET_PREFIX}-${region}"
  local s3_key="${runtime_name}/${hash}.${arch}.zip"

  echo "Uploading ${layer_archive} to s3://${bucket_name}/${s3_key}"
  aws --region "$region" s3 cp "$layer_archive" "s3://${bucket_name}/${s3_key}"

  echo "Publishing ${runtime_name} layer to ${region} as ${layer_name}"
  local layer_output
  layer_output=$(aws lambda publish-layer-version \
    --layer-name "${layer_name}" \
    --content "S3Bucket=${bucket_name},S3Key=${s3_key}" \
    --description "New Relic Layer for .NET 6/7/8 (${arch}) - Agent ${NEWRELIC_DOTNET_AGENT_VERSION}" \
    --license-info "Apache-2.0" \
    --compatible-runtimes dotnet6 dotnet8 \
    --compatible-architectures "$arch" \
    --region "$region" \
    --output json)

  local layer_version
  layer_version=$(echo "$layer_output" | jq -r '.Version')
  local layer_arn
  layer_arn=$(echo "$layer_output" | jq -r '.LayerArn')
  local full_layer_arn="${layer_arn}:${layer_version}"

  echo "Published ${runtime_name} layer version ${layer_version} to ${region}"
  echo "Full Layer ARN: ${full_layer_arn}"

  local arch_upper
  arch_upper=$(echo "$arch" | tr '[:lower:]' '[:upper:]')
  local runtime_nodots
  runtime_nodots=$(echo "${runtime_name//./}" | tr '[:lower:]' '[:upper:]')
  local env_var_name="LAYER_ARN_${runtime_nodots}_${arch_upper}"
  
  echo "export $env_var_name='$full_layer_arn'" >> "$TMP_ENV_FILE_NAME"
}

# --- Cleanup Function ---

cleanup_build_artifacts() {
  echo ""
  echo "=== Cleaning up build artifacts ==="

  if [ -d "$LAYER_DIR" ]; then
    echo "Removing temporary layer directory: $LAYER_DIR"
    rm -rf "$LAYER_DIR"
  fi

  if [ -d "$DIST_DIR" ]; then
    echo "Removing zip files from: $DIST_DIR"
    find "$DIST_DIR" -name "*.zip" -type f -delete
  fi

  echo "Cleanup complete!"
  echo ""
}

# --- Main Execution Logic ---

main() {
  mkdir -p "$DIST_DIR"
  rm -f "$TMP_ENV_FILE_NAME"
  touch "$TMP_ENV_FILE_NAME"

  echo ""
  echo "=========================================="
  echo "  Building .NET Lambda Layers            "
  echo "=========================================="
  echo "  .NET Agent Version: ${NEWRELIC_DOTNET_AGENT_VERSION}"
  echo "  Layer Name Prefix: ${LAYER_NAME_PREFIX}"
  echo "=========================================="
  echo ""

  # --- Build for x86_64 ---
  echo "=== Building x86_64 architecture ==="
  local target_x86="x86_64-unknown-linux-musl"
  build_extension "$target_x86"
  
  local zip_x86
  zip_x86=$(build_dotnet_layer "$target_x86" "x86_64" "amd64")
  
  for region in $REGIONS_X86_64; do
    publish_layer "$zip_x86" "$region" "dotnet" "x86_64" "${LAYER_NAME_PREFIX}X86"
  done

  # --- Build for arm64 ---
  echo ""
  echo "=== Building arm64 architecture ==="
  local target_arm="aarch64-unknown-linux-musl"
  build_extension "$target_arm"
  
  local zip_arm
  zip_arm=$(build_dotnet_layer "$target_arm" "arm64" "arm64")
  
  for region in $REGIONS_ARM64; do
    publish_layer "$zip_arm" "$region" "dotnet" "arm64" "${LAYER_NAME_PREFIX}ARM64"
  done

  echo ""
  echo "=========================================="
  echo "  All .NET layers published successfully!"
  echo "=========================================="
  echo ""
  echo "Environment variables saved to $TMP_ENV_FILE_NAME"
  cat "$TMP_ENV_FILE_NAME"

  cleanup_build_artifacts

  echo ""
  echo "To load the layer ARNs into your environment, run:"
  echo "  source $TMP_ENV_FILE_NAME"
  echo ""
  echo "Note: .NET layers are compatible with dotnet6 and dotnet8 runtimes"
}

main "$@"
