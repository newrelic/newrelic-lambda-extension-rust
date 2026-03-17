#!/usr/bin/env bash
set -euo pipefail

# Local testing script for building and publishing Lambda layers
# For production CI/CD, use scripts/ci/publish-layers.sh

# Ensure we run from repo root so paths resolve
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(pwd)"
cd "$ROOT_DIR"

# --- Configuration ---
# Layer naming prefix - change this to customize all layer names
LAYER_NAME_PREFIX=${LAYER_NAME_PREFIX:-"NRTestRustExtension"}

BUCKET_PREFIX=${BUCKET_PREFIX:-"nr-extension-test-layers"}
REGIONS_X86_64=${REGIONS_X86_64:-"us-east-1"}
REGIONS_ARM64=${REGIONS_ARM64:-"us-east-1"}

BIN_NAME="newrelic-lambda-extension"
DIST_DIR="$ROOT_DIR/dist"
LAYER_DIR="$ROOT_DIR/.layer"
TMP_ENV_FILE_NAME="$DIST_DIR/nr_tmp_env.sh"

# --- Build and Package Functions ---

# Builds the Rust extension for a given target
build_extension() {
  local target="$1"
  echo "Building extension for target $target" >&2

  if ! rustup target list --installed | grep -q "$target"; then
    echo "Rust target $target not installed. Installing..." >&2
    rustup target add "$target"
  fi

  # Prefer cargo-zigbuild over cross for musl targets
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

# Packages the built extension into a standalone layer zip
package_extension_layer() {
  local target="$1"
  local arch="${target%%-*}"
  local zip_name="$DIST_DIR/${BIN_NAME}-${arch}.zip"

  echo "Packaging standalone extension layer for $arch" >&2

  rm -rf "$LAYER_DIR"
  mkdir -p "$LAYER_DIR/extensions"
  cp "$ROOT_DIR/target/$target/release/$BIN_NAME" "$LAYER_DIR/extensions/$BIN_NAME"

  (cd "$LAYER_DIR" && zip -r9 "$zip_name" . >/dev/null)
  rm -rf "$LAYER_DIR"

  echo "Created $zip_name"
}

# Builds a layer for a specific Python version
build_python_layer_all() {
  local target="$1"
  local arch="${target%%-*}"
  local zip_name="$DIST_DIR/python-all-${arch}.zip"

  echo "Building single New Relic layer for all python ($arch)" >&2

  rm -rf "$LAYER_DIR"
  mkdir -p "$LAYER_DIR/python/"

  pip3 install --no-cache-dir -qU newrelic newrelic-lambda -t "$LAYER_DIR/python/"
  cp "$SCRIPT_DIR/newrelic_lambda_wrapper.py" "$LAYER_DIR/python/newrelic_lambda_wrapper.py"

  mkdir -p "$LAYER_DIR/extensions"
  cp "$ROOT_DIR/target/$target/release/$BIN_NAME" "$LAYER_DIR/extensions/$BIN_NAME"

  (cd "$LAYER_DIR" && zip -r9 "$zip_name" . >/dev/null)
  rm -rf "$LAYER_DIR"

  echo "Build complete: $zip_name"
}

# Builds a layer for a specific Node.js version
build_nodejs_layer_all() {
  local target="$1"
  local arch="${target%%-*}"
  local zip_name="$DIST_DIR/nodejs-all-${arch}.zip"

  echo "Building single New Relic layer for all nodejs ($arch)" >&2

  rm -rf "$LAYER_DIR"
  mkdir -p "$LAYER_DIR/nodejs/node_modules"

  npm install --install-strategy=nested --prefix "$LAYER_DIR/nodejs" newrelic@latest
  rm -rf "$LAYER_DIR/nodejs/node_modules/newrelic/node_modules/@opentelemetry"

  # CommonJS wrapper
  echo "Adding CommonJS wrapper..." >&2
  mkdir -p "$LAYER_DIR/nodejs/node_modules/newrelic-lambda-wrapper"
  cp "$SCRIPT_DIR/index.js" "$LAYER_DIR/nodejs/node_modules/newrelic-lambda-wrapper/index.js"

  cat > "$LAYER_DIR/nodejs/node_modules/newrelic-lambda-wrapper/package.json" << 'EOF'
{
  "name": "newrelic-lambda-wrapper",
  "version": "1.0.0",
  "main": "index.js",
  "type": "commonjs"
}
EOF

  # ESM wrapper
  echo "Adding ESM wrapper..." >&2
  mkdir -p "$LAYER_DIR/nodejs/node_modules/newrelic-esm-lambda-wrapper"
  cp "$SCRIPT_DIR/esm.mjs" "$LAYER_DIR/nodejs/node_modules/newrelic-esm-lambda-wrapper/index.js"

  cat > "$LAYER_DIR/nodejs/node_modules/newrelic-esm-lambda-wrapper/package.json" << 'EOF'
{
  "name": "newrelic-esm-lambda-wrapper",
  "version": "1.0.0",
  "main": "index.js",
  "type": "module"
}
EOF

  # Add extension binary
  mkdir -p "$LAYER_DIR/extensions"
  cp "$ROOT_DIR/target/$target/release/$BIN_NAME" "$LAYER_DIR/extensions/$BIN_NAME"

  (cd "$LAYER_DIR" && zip -r9 "$zip_name" . >/dev/null)
  rm -rf "$LAYER_DIR"

  echo "Build complete: $zip_name"
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
  local runtime_name="$3" # e.g., python3.11, nodejs20.x, extension
  local arch="$4" # e.g., x86_64, arm64
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
    --description "New Relic Test Layer for ${runtime_name} (${arch})" \
    --license-info "Apache-2.0" \
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

  # Remove temporary layer directory
  if [ -d "$LAYER_DIR" ]; then
    echo "Removing temporary layer directory: $LAYER_DIR"
    rm -rf "$LAYER_DIR"
  fi

  # Remove dist zip files (keep the environment file for reference)
  if [ -d "$DIST_DIR" ]; then
    echo "Removing zip files from: $DIST_DIR"
    find "$DIST_DIR" -name "*.zip" -type f -delete
  fi

  # Remove Cargo build artifacts (target directory) - optional for local testing
  # Uncomment the lines below if you want to clean build artifacts after publishing
  # if [ -d "$ROOT_DIR/target" ]; then
  #   echo "Removing Cargo build directory: $ROOT_DIR/target"
  #   rm -rf "$ROOT_DIR/target"
  # fi

  echo "Cleanup complete!"
  echo ""
}

# --- Main Execution Logic ---

main() {
  mkdir -p "$DIST_DIR"
  rm -f "$TMP_ENV_FILE_NAME"
  touch "$TMP_ENV_FILE_NAME"

  # --- Build for x86_64 ---
  local target_x86="x86_64-unknown-linux-musl"
  build_extension "$target_x86"

  # Package and publish standalone extension
  package_extension_layer "$target_x86"
  for region in $REGIONS_X86_64; do
    publish_layer "$DIST_DIR/${BIN_NAME}-x86_64.zip" "$region" "extension" "x86_64" "${LAYER_NAME_PREFIX}X86"
  done

  # Package and publish single Python layer
  build_python_layer_all "$target_x86"
  for region in $REGIONS_X86_64; do
    publish_layer "$DIST_DIR/python-all-x86_64.zip" "$region" "python" "x86_64" "${LAYER_NAME_PREFIX}PythonX86"
  done

  # Package and publish single Node.js layer
  build_nodejs_layer_all "$target_x86"
  for region in $REGIONS_X86_64; do
    publish_layer "$DIST_DIR/nodejs-all-x86_64.zip" "$region" "nodejs" "x86_64" "${LAYER_NAME_PREFIX}NodejsX86"
  done

  #--- Build for arm64 ---
  local target_arm="aarch64-unknown-linux-musl"
  build_extension "$target_arm"

  # Package and publish standalone extension
  package_extension_layer "$target_arm"
  for region in $REGIONS_ARM64; do
    publish_layer "$DIST_DIR/${BIN_NAME}-aarch64.zip" "$region" "extension" "arm64" "${LAYER_NAME_PREFIX}ARM64"
  done

  # Package and publish single Python layer
  build_python_layer_all "$target_arm"
  for region in $REGIONS_ARM64; do
    publish_layer "$DIST_DIR/python-all-aarch64.zip" "$region" "python" "arm64" "${LAYER_NAME_PREFIX}PythonARM64"
  done

  # Package and publish single Node.js layer
  build_nodejs_layer_all "$target_arm"
  for region in $REGIONS_ARM64; do
    publish_layer "$DIST_DIR/nodejs-all-aarch64.zip" "$region" "nodejs" "arm64" "${LAYER_NAME_PREFIX}NodejsARM64"
  done

  echo ""
  echo "=========================================="
  echo "  All layers published successfully!     "
  echo "=========================================="
  echo ""
  echo "Environment variables saved to $TMP_ENV_FILE_NAME"
  cat "$TMP_ENV_FILE_NAME"

  # Cleanup after successful publish
  cleanup_build_artifacts

  echo "To load the layer ARNs into your environment, run:"
  echo "  source $TMP_ENV_FILE_NAME"
}

main "$@"