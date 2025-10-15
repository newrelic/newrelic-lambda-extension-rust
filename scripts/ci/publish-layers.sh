#!/usr/bin/env bash
set -euo pipefail

# This script is designed to run in GitHub Actions for publishing Lambda layers to AWS
# It builds Lambda layers for Amazon Linux 2, packages them, and publishes to multiple regions

# --- Configuration ---
# Layer naming prefix - change this to customize all layer names
LAYER_NAME_PREFIX=${LAYER_NAME_PREFIX:-"NRLambdaTestRustExtension"}

# S3 and region configuration
BUCKET_PREFIX=${BUCKET_PREFIX:-"nr-extension-test-layers"}
# Convert comma-separated regions to space-separated
REGIONS_X86_64=${REGIONS_X86_64:-"us-west-2,us-east-1"}
REGIONS_ARM64=${REGIONS_ARM64:-"us-west-2,us-east-1"}
REGIONS_X86_64="${REGIONS_X86_64//,/ }"
REGIONS_ARM64="${REGIONS_ARM64//,/ }"

BIN_NAME="newrelic-lambda-extension"
ROOT_DIR="$(pwd)"
DIST_DIR="$ROOT_DIR/dist"
LAYER_DIR="$ROOT_DIR/.layer"
LAYER_ARNS_FILE="$DIST_DIR/layer_arns.txt"

echo "=== Configuration ==="
echo "Layer name prefix: $LAYER_NAME_PREFIX"
echo "Bucket prefix: $BUCKET_PREFIX"
echo "x86_64 regions: $REGIONS_X86_64"
echo "ARM64 regions: $REGIONS_ARM64"
echo "Root directory: $ROOT_DIR"
echo ""

# --- Build Functions ---

# Builds the Rust extension for a given target (optimized for Amazon Linux 2)
build_extension() {
  local target="$1"
  echo "=== Building extension for target $target ===" >&2

  if ! rustup target list --installed | grep -q "$target"; then
    echo "Installing Rust target $target..." >&2
    rustup target add "$target"
  fi

  # Use cargo-zigbuild for cross-compilation (already installed in CI)
  echo "Building with cargo-zigbuild for $target" >&2
  cargo zigbuild --release --target "$target" --target-dir "$ROOT_DIR/target"

  echo "✓ Build complete for $target" >&2
  echo ""
}

# Packages the built extension into a standalone layer zip
package_extension_layer() {
  local target="$1"
  local arch="${target%%-*}"
  local zip_name="$DIST_DIR/${BIN_NAME}-${arch}.zip"

  echo "=== Packaging standalone extension layer for $arch ===" >&2

  rm -rf "$LAYER_DIR"
  mkdir -p "$LAYER_DIR/extensions"
  cp "$ROOT_DIR/target/$target/release/$BIN_NAME" "$LAYER_DIR/extensions/$BIN_NAME"

  (cd "$LAYER_DIR" && zip -r9 "$zip_name" . >/dev/null)

  echo "✓ Created $zip_name" >&2
  ls -lh "$zip_name" >&2
  echo ""
}

# Builds a single Python layer for all Python versions
build_python_layer_all() {
  local target="$1"
  local arch="${target%%-*}"
  local zip_name="$DIST_DIR/python-all-${arch}.zip"

  echo "=== Building Python layer for all versions ($arch) ===" >&2

  rm -rf "$LAYER_DIR"
  mkdir -p "$LAYER_DIR/python/"

  echo "Installing Python dependencies..." >&2
  pip3 install --no-cache-dir -qU newrelic newrelic-lambda -t "$LAYER_DIR/python/"
  cp "$ROOT_DIR/scripts/newrelic_lambda_wrapper.py" "$LAYER_DIR/python/newrelic_lambda_wrapper.py"

  mkdir -p "$LAYER_DIR/extensions"
  cp "$ROOT_DIR/target/$target/release/$BIN_NAME" "$LAYER_DIR/extensions/$BIN_NAME"

  (cd "$LAYER_DIR" && zip -r9 "$zip_name" . >/dev/null)

  echo "✓ Build complete: $zip_name" >&2
  ls -lh "$zip_name" >&2
  echo ""
}

# Builds a single Node.js layer for all Node.js versions
build_nodejs_layer_all() {
  local target="$1"
  local arch="${target%%-*}"
  local zip_name="$DIST_DIR/nodejs-all-${arch}.zip"

  echo "=== Building Node.js layer for all versions ($arch) ===" >&2

  rm -rf "$LAYER_DIR"
  mkdir -p "$LAYER_DIR/nodejs/node_modules"

  echo "Installing Node.js dependencies..." >&2
  npm install --install-strategy=nested --prefix "$LAYER_DIR/nodejs" newrelic@latest
  rm -rf "$LAYER_DIR/nodejs/node_modules/newrelic/node_modules/@opentelemetry"

  # CommonJS wrapper
  echo "Adding CommonJS wrapper..." >&2
  mkdir -p "$LAYER_DIR/nodejs/node_modules/newrelic-lambda-wrapper"
  cp "$ROOT_DIR/scripts/index.js" "$LAYER_DIR/nodejs/node_modules/newrelic-lambda-wrapper/index.js"

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
  cp "$ROOT_DIR/scripts/esm.mjs" "$LAYER_DIR/nodejs/node_modules/newrelic-esm-lambda-wrapper/index.js"

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

  echo "✓ Build complete: $zip_name" >&2
  ls -lh "$zip_name" >&2
  echo ""
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
  local runtime_name="$3" # e.g., python, nodejs, extension
  local arch="$4" # e.g., x86_64, arm64
  local layer_name="$5"

  local hash
  hash=$(hash_file "$layer_archive")
  local bucket_name="${BUCKET_PREFIX}-${region}"
  local s3_key="${runtime_name}/${hash}.${arch}.zip"

  echo "→ Uploading to s3://${bucket_name}/${s3_key}" >&2
  aws --region "$region" s3 cp "$layer_archive" "s3://${bucket_name}/${s3_key}"

  echo "→ Publishing layer to ${region} as ${layer_name}" >&2
  local layer_output
  layer_output=$(aws lambda publish-layer-version \
    --layer-name "${layer_name}" \
    --content "S3Bucket=${bucket_name},S3Key=${s3_key}" \
    --description "New Relic Layer for ${runtime_name} (${arch}) - Built for Amazon Linux 2" \
    --license-info "Apache-2.0" \
    --compatible-architectures "$arch" \
    --region "$region" \
    --output json)

  local layer_version
  layer_version=$(echo "$layer_output" | jq -r '.Version')
  local layer_arn
  layer_arn=$(echo "$layer_output" | jq -r '.LayerArn')
  local full_layer_arn="${layer_arn}:${layer_version}"

  echo "✓ Published ${runtime_name} layer version ${layer_version} to ${region}" >&2
  echo "  ARN: ${full_layer_arn}" >&2

  # Save to output file
  echo "${layer_name} (${region}): ${full_layer_arn}" >> "$LAYER_ARNS_FILE"
  echo ""
}

# --- Cleanup Function ---

cleanup_build_artifacts() {
  echo "=== Cleaning up build artifacts ===" >&2

  # Remove temporary layer directory
  if [ -d "$LAYER_DIR" ]; then
    echo "→ Removing temporary layer directory: $LAYER_DIR" >&2
    rm -rf "$LAYER_DIR"
  fi

  # Remove dist zip files (keep layer_arns.txt)
  if [ -d "$DIST_DIR" ]; then
    echo "→ Removing zip files from: $DIST_DIR" >&2
    find "$DIST_DIR" -name "*.zip" -type f -delete
  fi

  # Remove Cargo build artifacts (target directory)
  if [ -d "$ROOT_DIR/target" ]; then
    echo "→ Removing Cargo build directory: $ROOT_DIR/target" >&2
    rm -rf "$ROOT_DIR/target"
  fi

  echo "✓ Cleanup complete" >&2
  echo ""
}

# --- Main Execution Logic ---

main() {
  mkdir -p "$DIST_DIR"
  rm -f "$LAYER_ARNS_FILE"
  touch "$LAYER_ARNS_FILE"

  echo "============================================"
  echo "  New Relic Lambda Layer Publisher (CI)    "
  echo "============================================"
  echo ""

  # --- Build for x86_64 ---
  echo "### Building x86_64 artifacts ###"
  echo ""
  local target_x86="x86_64-unknown-linux-musl"
  build_extension "$target_x86"

  # Package and publish standalone extension
  echo "## Publishing x86_64 Extension Layers ##"
  package_extension_layer "$target_x86"
  for region in $REGIONS_X86_64; do
    publish_layer "$DIST_DIR/${BIN_NAME}-x86_64.zip" "$region" "extension" "x86_64" "${LAYER_NAME_PREFIX}X86"
  done

  # Package and publish Python layer
  echo "## Publishing x86_64 Python Layers ##"
  build_python_layer_all "$target_x86"
  for region in $REGIONS_X86_64; do
    publish_layer "$DIST_DIR/python-all-x86_64.zip" "$region" "python" "x86_64" "${LAYER_NAME_PREFIX}PythonX86"
  done

  # Package and publish Node.js layer
  echo "## Publishing x86_64 Node.js Layers ##"
  build_nodejs_layer_all "$target_x86"
  for region in $REGIONS_X86_64; do
    publish_layer "$DIST_DIR/nodejs-all-x86_64.zip" "$region" "nodejs" "x86_64" "${LAYER_NAME_PREFIX}NodejsX86"
  done

  # --- Build for arm64 ---
  echo "### Building ARM64 artifacts ###"
  echo ""
  local target_arm="aarch64-unknown-linux-musl"
  build_extension "$target_arm"

  # Package and publish standalone extension
  echo "## Publishing ARM64 Extension Layers ##"
  package_extension_layer "$target_arm"
  for region in $REGIONS_ARM64; do
    publish_layer "$DIST_DIR/${BIN_NAME}-aarch64.zip" "$region" "extension" "arm64" "${LAYER_NAME_PREFIX}ARM64"
  done

  # Package and publish Python layer
  echo "## Publishing ARM64 Python Layers ##"
  build_python_layer_all "$target_arm"
  for region in $REGIONS_ARM64; do
    publish_layer "$DIST_DIR/python-all-aarch64.zip" "$region" "python" "arm64" "${LAYER_NAME_PREFIX}PythonARM64"
  done

  # Package and publish Node.js layer
  echo "## Publishing ARM64 Node.js Layers ##"
  build_nodejs_layer_all "$target_arm"
  for region in $REGIONS_ARM64; do
    publish_layer "$DIST_DIR/nodejs-all-aarch64.zip" "$region" "nodejs" "arm64" "${LAYER_NAME_PREFIX}NodejsARM64"
  done

  echo "============================================"
  echo "  ✓ All layers published successfully!     "
  echo "============================================"
  echo ""
  echo "Published Layer ARNs:"
  cat "$LAYER_ARNS_FILE"
  echo ""

  # Cleanup after successful publish
  cleanup_build_artifacts
}

main "$@"
