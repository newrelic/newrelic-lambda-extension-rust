#!/usr/bin/env bash
set -euo pipefail

# Unified script for building and publishing Lambda test layers
# Supports all language runtimes: Python, Node.js, Java, .NET, Ruby
# Can be used for PR testing or production deployments
# For production CI/CD, use scripts/ci/publish-layers.sh

# Ensure we run from repo root so paths resolve
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(pwd)"
cd "$ROOT_DIR"

# --- Configuration ---
# Layer naming prefix - change this to customize all layer names
LAYER_NAME_PREFIX=${LAYER_NAME_PREFIX:-"NewRelicLambdaRustExtension"}
# Test mode (defaults to true for PR testing, set to false for production)
TEST_MODE=${TEST_MODE:-"true"}

# If TEST_MODE is true, use test configuration
if [ "$TEST_MODE" = "true" ]; then
  LAYER_NAME_PREFIX="NRTestRustExtension"
  BUCKET_PREFIX=${BUCKET_PREFIX:-"nr-extension-test-layers"}
  REGIONS_X86_64=${REGIONS_X86_64:-"us-west-1"}
  REGIONS_ARM64=${REGIONS_ARM64:-"us-west-1"}
else
  # Production configuration
  BUCKET_PREFIX=${BUCKET_PREFIX:-"nr-extension-test-layers"}
  REGIONS_X86_64=${REGIONS_X86_64:-"sa-east-1 eu-north-1 eu-west-3 eu-west-2 eu-west-1 eu-central-1 ca-central-1 ap-northeast-1 ap-southeast-2 ap-southeast-1 ap-northeast-2 ap-northeast-3 ap-south-1 us-east-1 us-east-2 us-west-1 us-west-2"}
  REGIONS_ARM64=${REGIONS_ARM64:-"sa-east-1 eu-north-1 eu-west-3 eu-west-2 eu-west-1 eu-central-1 ca-central-1 ap-northeast-1 ap-southeast-2 ap-southeast-1 ap-northeast-2 ap-northeast-3 ap-south-1 us-east-1 us-east-2 us-west-1 us-west-2"}
fi

# Languages to build (can be overridden: "python nodejs java dotnet ruby")
LANGUAGES=${LANGUAGES:-"python nodejs java dotnet ruby"}

BIN_NAME="newrelic-lambda-extension"
DIST_DIR="$ROOT_DIR/dist"
LAYER_DIR="$ROOT_DIR/.layer"
TMP_ENV_FILE_NAME="$DIST_DIR/nr_tmp_env.sh"

# Java configuration
BUILD_DIR="$SCRIPT_DIR/java/build"
GRADLE_ARCHIVE="$BUILD_DIR/distributions/NewRelicJavaLayer.zip"
JAVA_DIST_DIR="$SCRIPT_DIR/java/dist"

# .NET configuration
DOTNET_BUILD_DIR="lib"  # AWS Lambda .NET layers use lib/ directory
if [ -z "${NEWRELIC_DOTNET_AGENT_VERSION:-}" ]; then
  echo "Fetching latest .NET agent version from GitHub..." >&2
  NEWRELIC_DOTNET_AGENT_VERSION=$(curl -fsSL https://api.github.com/repos/newrelic/newrelic-dotnet-agent/releases/latest | jq -r '.tag_name' | sed 's/^v//')
  if [ -z "$NEWRELIC_DOTNET_AGENT_VERSION" ] || [ "$NEWRELIC_DOTNET_AGENT_VERSION" = "null" ]; then
    echo "Warning: Failed to fetch latest .NET agent version, using default" >&2
    NEWRELIC_DOTNET_AGENT_VERSION="10.0.0"
  fi
  echo "Using .NET agent version: ${NEWRELIC_DOTNET_AGENT_VERSION}" >&2
fi
AGENT_DOWNLOAD_BASE_URL="https://download.newrelic.com/dot_net_agent/latest_release"

# Ruby configuration
RUBY_VERSION=${RUBY_VERSION:-"3.3"}
RUBY_ASSETS_DIR="$SCRIPT_DIR/ruby"
WRAPPER_FILE="newrelic_lambda_wrapper.rb"
GEMFILE="Gemfile"

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

# --- Java Layer Functions ---

build_java_layer() {
  local java_version="$1"
  local target_zip="$2"
  local arch="$3"
  local target="$4"
  
  echo "Building New Relic Java layer (Java $java_version, $arch)" >&2
  
  # Ensure extension is built and placed in correct location
  local extension_dir="java/extensions/$arch"
  mkdir -p "$SCRIPT_DIR/$extension_dir"
  cp "$ROOT_DIR/target/$target/release/$BIN_NAME" "$SCRIPT_DIR/$extension_dir/"
  
  cd "$SCRIPT_DIR/java"
  rm -rf "$BUILD_DIR" "$target_zip"
  ./gradlew packageLayer -P javaVersion="$java_version" -P arch="$arch"
  
  mkdir -p "$JAVA_DIST_DIR"
  cp "$GRADLE_ARCHIVE" "$target_zip"
  
  echo "Build complete: $target_zip" >&2
  cd "$ROOT_DIR"
}

# --- .NET Layer Functions ---

download_dotnet_agent() {
  local arch="$1"  # amd64 or arm64
  local agent_file="newrelic-dotnet-agent_${NEWRELIC_DOTNET_AGENT_VERSION}_${arch}.tar.gz"
  local download_url="${AGENT_DOWNLOAD_BASE_URL}/${agent_file}"
  
  echo "Downloading .NET agent version ${NEWRELIC_DOTNET_AGENT_VERSION} for ${arch}" >&2
  
  rm -rf "$LAYER_DIR/$DOTNET_BUILD_DIR"
  mkdir -p "$LAYER_DIR/$DOTNET_BUILD_DIR"
  
  local tmp_agent="/tmp/${agent_file}"
  curl -fsSL "$download_url" -o "$tmp_agent"
  
  echo "Extracting .NET agent to $LAYER_DIR/$DOTNET_BUILD_DIR" >&2
  tar -xzf "$tmp_agent" -C "$LAYER_DIR/$DOTNET_BUILD_DIR"
  
  echo "$NEWRELIC_DOTNET_AGENT_VERSION" > "$LAYER_DIR/$DOTNET_BUILD_DIR/newrelic-dotnet-agent/version.txt"
  
  rm -f "$tmp_agent"
  echo ".NET agent downloaded and extracted successfully" >&2
}

build_dotnet_layer() {
  local target="$1"
  local arch_linux="$2"  # x86_64 or arm64 (Linux arch naming)
  local arch_dotnet="$3" # amd64 or arm64 (.NET agent naming)
  local zip_name="$DIST_DIR/dotnet-${arch_linux}.zip"

  echo "Building New Relic layer for .NET (${arch_linux})" >&2

  rm -rf "$LAYER_DIR"
  mkdir -p "$LAYER_DIR"

  download_dotnet_agent "$arch_dotnet"
  
  mkdir -p "$LAYER_DIR/extensions"
  cp "$ROOT_DIR/target/$target/release/$BIN_NAME" "$LAYER_DIR/extensions/$BIN_NAME"

  (cd "$LAYER_DIR" && zip -r9 "$zip_name" . >/dev/null)
  rm -rf "$LAYER_DIR"

  echo "Build complete: $zip_name" >&2
}

# --- Ruby Layer Functions ---

build_ruby_layer() {
  local target="$1"
  local arch="${target%%-*}"
  local zip_name="$DIST_DIR/ruby-${arch}.zip"

  echo "Building New Relic layer for Ruby ${RUBY_VERSION} (${arch})" >&2

  rm -rf "$LAYER_DIR"
  mkdir -p "$LAYER_DIR/ruby/gems/${RUBY_VERSION}.0"

  cd "$RUBY_ASSETS_DIR"
  bundle config set --local path "$LAYER_DIR/ruby/gems/${RUBY_VERSION}.0"
  bundle install
  
  cp "$RUBY_ASSETS_DIR/$WRAPPER_FILE" "$LAYER_DIR/ruby/gems/${RUBY_VERSION}.0/newrelic_lambda_wrapper.rb"

  mkdir -p "$LAYER_DIR/extensions"
  cp "$ROOT_DIR/target/$target/release/$BIN_NAME" "$LAYER_DIR/extensions/$BIN_NAME"

  cd "$ROOT_DIR"
  (cd "$LAYER_DIR" && zip -r9 "$zip_name" . >/dev/null)
  rm -rf "$LAYER_DIR"

  echo "Build complete: $zip_name" >&2
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

  echo "→ Setting public permissions for layer version ${layer_version}" >&2
  aws lambda add-layer-version-permission \
    --layer-name "${layer_name}" \
    --version-number "$layer_version" \
    --statement-id public \
    --action lambda:GetLayerVersion \
    --principal "*" \
    --region "$region" \
    --output json >/dev/null

  echo "✓ Layer is now publicly accessible" >&2


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
  mkdir -p "$DIST_DIR" "$JAVA_DIST_DIR"
  rm -f "$TMP_ENV_FILE_NAME"
  touch "$TMP_ENV_FILE_NAME"

  echo "=========================================="
  echo "  Building Test Layers                   "
  echo "  Languages: $LANGUAGES"
  echo "  Regions: $REGIONS_X86_64"
  echo "=========================================="

  # --- Build for x86_64 ---
  local target_x86="x86_64-unknown-linux-musl"
  echo ""
  echo "Building Rust extension for x86_64..."
  build_extension "$target_x86"

  # Package and publish standalone extension
  if echo "$LANGUAGES" | grep -q "extension"; then
    package_extension_layer "$target_x86"
    for region in $REGIONS_X86_64; do
      publish_layer "$DIST_DIR/${BIN_NAME}-x86_64.zip" "$region" "extension" "x86_64" "${LAYER_NAME_PREFIX}X86"
    done
  fi

  # Package and publish Python layer
  if echo "$LANGUAGES" | grep -q "python"; then
    echo ""
    echo "Building Python layer for x86_64..."
    build_python_layer_all "$target_x86"
    for region in $REGIONS_X86_64; do
      publish_layer "$DIST_DIR/python-all-x86_64.zip" "$region" "python" "x86_64" "${LAYER_NAME_PREFIX}PythonX86"
    done
  fi

  # Package and publish Node.js layer
  if echo "$LANGUAGES" | grep -q "nodejs"; then
    echo ""
    echo "Building Node.js layer for x86_64..."
    build_nodejs_layer_all "$target_x86"
    for region in $REGIONS_X86_64; do
      publish_layer "$DIST_DIR/nodejs-all-x86_64.zip" "$region" "nodejs" "x86_64" "${LAYER_NAME_PREFIX}NodejsX86"
    done
  fi

  # Package and publish Java layers
  if echo "$LANGUAGES" | grep -q "java"; then
    echo ""
    echo "Building Java layers for x86_64..."
    # Java 17 (default)
    build_java_layer "17" "$JAVA_DIST_DIR/java17.x86_64.zip" "x86_64" "$target_x86"
    for region in $REGIONS_X86_64; do
      publish_layer "$JAVA_DIST_DIR/java17.x86_64.zip" "$region" "java17" "x86_64" "${LAYER_NAME_PREFIX}Java17X86"
    done
  fi

  # Package and publish .NET layer
  if echo "$LANGUAGES" | grep -q "dotnet"; then
    echo ""
    echo "Building .NET layer for x86_64..."
    build_dotnet_layer "$target_x86" "x86_64" "amd64"
    for region in $REGIONS_X86_64; do
      publish_layer "$DIST_DIR/dotnet-x86_64.zip" "$region" "dotnet" "x86_64" "${LAYER_NAME_PREFIX}DotnetX86"
    done
  fi

  # Package and publish Ruby layer
  if echo "$LANGUAGES" | grep -q "ruby"; then
    echo ""
    echo "Building Ruby layer for x86_64..."
    build_ruby_layer "$target_x86"
    for region in $REGIONS_X86_64; do
      publish_layer "$DIST_DIR/ruby-x86_64.zip" "$region" "ruby" "x86_64" "${LAYER_NAME_PREFIX}RubyX86"
    done
  fi

  #--- Build for arm64 ---
  local target_arm="aarch64-unknown-linux-musl"
  echo ""
  echo "Building Rust extension for arm64..."
  build_extension "$target_arm"

  # Package and publish standalone extension
  if echo "$LANGUAGES" | grep -q "extension"; then
    package_extension_layer "$target_arm"
    for region in $REGIONS_ARM64; do
      publish_layer "$DIST_DIR/${BIN_NAME}-aarch64.zip" "$region" "extension" "arm64" "${LAYER_NAME_PREFIX}ARM64"
    done
  fi

  # Package and publish Python layer
  if echo "$LANGUAGES" | grep -q "python"; then
    echo ""
    echo "Building Python layer for arm64..."
    build_python_layer_all "$target_arm"
    for region in $REGIONS_ARM64; do
      publish_layer "$DIST_DIR/python-all-aarch64.zip" "$region" "python" "arm64" "${LAYER_NAME_PREFIX}PythonARM64"
    done
  fi

  # Package and publish Node.js layer
  if echo "$LANGUAGES" | grep -q "nodejs"; then
    echo ""
    echo "Building Node.js layer for arm64..."
    build_nodejs_layer_all "$target_arm"
    for region in $REGIONS_ARM64; do
      publish_layer "$DIST_DIR/nodejs-all-aarch64.zip" "$region" "nodejs" "arm64" "${LAYER_NAME_PREFIX}NodejsARM64"
    done
  fi

  # Package and publish Java layers
  if echo "$LANGUAGES" | grep -q "java"; then
    echo ""
    echo "Building Java layers for arm64..."
    # Java 17 (default)
    build_java_layer "17" "$JAVA_DIST_DIR/java17.arm64.zip" "arm64" "$target_arm"
    for region in $REGIONS_ARM64; do
      publish_layer "$JAVA_DIST_DIR/java17.arm64.zip" "$region" "java17" "arm64" "${LAYER_NAME_PREFIX}Java17ARM"
    done
  fi

  # Package and publish .NET layer
  if echo "$LANGUAGES" | grep -q "dotnet"; then
    echo ""
    echo "Building .NET layer for arm64..."
    build_dotnet_layer "$target_arm" "arm64" "arm64"
    for region in $REGIONS_ARM64; do
      publish_layer "$DIST_DIR/dotnet-arm64.zip" "$region" "dotnet" "arm64" "${LAYER_NAME_PREFIX}DotnetARM64"
    done
  fi

  # Package and publish Ruby layer
  if echo "$LANGUAGES" | grep -q "ruby"; then
    echo ""
    echo "Building Ruby layer for arm64..."
    build_ruby_layer "$target_arm"
    for region in $REGIONS_ARM64; do
      publish_layer "$DIST_DIR/ruby-aarch64.zip" "$region" "ruby" "arm64" "${LAYER_NAME_PREFIX}RubyARM64"
    done
  fi

  echo ""
  echo "=========================================="
  echo "  All layers published successfully!     "
  echo "=========================================="
  echo ""
  echo "Environment variables saved to $TMP_ENV_FILE_NAME"
  cat "$TMP_ENV_FILE_NAME"

  # Cleanup after successful publish
  cleanup_build_artifacts

  echo ""
  echo "To load the layer ARNs into your environment, run:"
  echo "  source $TMP_ENV_FILE_NAME"
}

main "$@"