#!/usr/bin/env bash
set -euo pipefail

# Local testing script for building and publishing Java Lambda layers
# Usage: ./javaTestLayer.sh [java11 java17 java21]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# --- Configuration ---
LAYER_NAME_PREFIX=${LAYER_NAME_PREFIX:-"NRTestRustExtension"}
BUCKET_PREFIX=${BUCKET_PREFIX:-"nr-extension-test-layers"}
REGIONS=${REGIONS:-"us-west-1"}

BUILD_DIR="$SCRIPT_DIR/java/build"
GRADLE_ARCHIVE="$BUILD_DIR/distributions/NewRelicJavaLayer.zip"
DIST_DIR="$SCRIPT_DIR/java/dist"

JAVA11_DIST_X86_64="$DIST_DIR/java11.x86_64.zip"
JAVA17_DIST_X86_64="$DIST_DIR/java17.x86_64.zip"
JAVA21_DIST_X86_64="$DIST_DIR/java21.x86_64.zip"

JAVA11_DIST_ARM64="$DIST_DIR/java11.arm64.zip"
JAVA17_DIST_ARM64="$DIST_DIR/java17.arm64.zip"
JAVA21_DIST_ARM64="$DIST_DIR/java21.arm64.zip"

# --- Build Extension Locally ---
build_extension() {
  local target="$1"
  local extension_dir="java/extensions/$2"
  
  echo "Building Rust extension for $target"
  cd "$ROOT_DIR"
  
  if ! rustup target list --installed | grep -q "$target"; then
    echo "Installing Rust target $target..."
    rustup target add "$target"
  fi

  if command -v cargo-zigbuild >/dev/null 2>&1; then
    cargo zigbuild --release --target "$target"
  elif command -v cross >/dev/null 2>&1; then
    cross build --release --target "$target"
  else
    cargo build --release --target "$target"
  fi
  
  # Copy extension to java/extensions/{arch} for Gradle packaging
  mkdir -p "$SCRIPT_DIR/$extension_dir"
  cp "$ROOT_DIR/target/$target/release/newrelic-lambda-extension" "$SCRIPT_DIR/$extension_dir/"
  echo "Extension binary ready at $extension_dir/"
}

# --- Hash Function ---
hash_file() {
  if command -v md5sum &>/dev/null; then
    md5sum "$1" | awk '{ print $1 }'
  else
    md5 -q "$1"
  fi
}

# --- Build Java Layer with Gradle ---
build_java_layer() {
  local java_version="$1"
  local target_zip="$2"
  local arch="$3"
  
  echo "Building New Relic Java layer (Java $java_version, $arch)"
  cd "$SCRIPT_DIR/java"
  
  rm -rf "$BUILD_DIR" "$target_zip"
  ./gradlew packageLayer -P javaVersion="$java_version" -P arch="$arch"
  
  mkdir -p "$DIST_DIR"
  cp "$GRADLE_ARCHIVE" "$target_zip"
  
  echo "Build complete: $target_zip"
}

# --- Publish Layer ---
publish_layer() {
  local layer_archive="$1"
  local region="$2"
  local runtime_names="$3"  # Space-separated list of runtimes
  local layer_name="$4"
  local arch="$5"  # x86_64 or arm64

  local hash
  hash=$(hash_file "$layer_archive")
  local bucket_name="${BUCKET_PREFIX}-${region}"
  local primary_runtime=$(echo "$runtime_names" | awk '{print $1}')
  local s3_key="${primary_runtime}/${hash}.${arch}.zip"

  echo "Uploading ${layer_archive} to s3://${bucket_name}/${s3_key}"
  aws --region "$region" s3 cp "$layer_archive" "s3://${bucket_name}/${s3_key}"

  echo "Publishing layer to ${region} as ${layer_name} (compatible with: ${runtime_names}, arch: ${arch})"
  local layer_output
  layer_output=$(aws lambda publish-layer-version \
    --layer-name "${layer_name}" \
    --content "S3Bucket=${bucket_name},S3Key=${s3_key}" \
    --description "New Relic Test Layer for ${runtime_names} (${arch})" \
    --license-info "Apache-2.0" \
    --compatible-architectures "${arch}" \
    --compatible-runtimes $runtime_names \
    --region "$region" \
    --output json)

  local layer_version
  layer_version=$(echo "$layer_output" | jq -r '.Version')
  local layer_arn
  layer_arn=$(echo "$layer_output" | jq -r '.LayerArn')
  local full_layer_arn="${layer_arn}:${layer_version}"

  echo "Published layer version ${layer_version}"
  echo "Layer ARN: ${full_layer_arn}"

  aws lambda add-layer-version-permission \
    --layer-name "${layer_name}" \
    --version-number "$layer_version" \
    --statement-id public \
    --action lambda:GetLayerVersion \
    --principal "*" \
    --region "$region" \
    --output json >/dev/null

  local runtime_upper
  runtime_upper=$(echo "$primary_runtime" | tr '[:lower:]' '[:upper:]')
  local arch_upper=$(echo "$arch" | tr '[:lower:]' '[:upper:]' | tr '-' '_')
  echo "export LAYER_ARN_${runtime_upper}_${arch_upper}='$full_layer_arn'"
  
  # Clean up zip file after successful publish
  echo "Cleaning up ${layer_archive}"
  rm -f "$layer_archive"
}

# --- Main ---
main() {
  local java_versions="${1:-java17}"
  
  echo "=========================================="
  echo "  Building Extension Binaries            "
  echo "=========================================="
  build_extension "x86_64-unknown-linux-musl" "extensions/x86_64"
  build_extension "aarch64-unknown-linux-musl" "extensions/arm64"
  
  for java_ver in $java_versions; do
    echo ""
    echo "=========================================="
    echo "  Building Java Layers ($java_ver)      "
    echo "=========================================="
    
    case $java_ver in
      java11)
        # x86_64
        build_java_layer "11" "$JAVA11_DIST_X86_64" "x86_64"
        for region in $REGIONS; do
          publish_layer "$JAVA11_DIST_X86_64" "$region" "java11 java17 java21" "${LAYER_NAME_PREFIX}Java11X86" "x86_64"
        done
        
        # arm64
        build_java_layer "11" "$JAVA11_DIST_ARM64" "arm64"
        for region in $REGIONS; do
          publish_layer "$JAVA11_DIST_ARM64" "$region" "java11 java17 java21" "${LAYER_NAME_PREFIX}Java11ARM" "arm64"
        done
        ;;
      java17)
        # x86_64
        build_java_layer "17" "$JAVA17_DIST_X86_64" "x86_64"
        for region in $REGIONS; do
          publish_layer "$JAVA17_DIST_X86_64" "$region" "java11 java17 java21" "${LAYER_NAME_PREFIX}Java17X86" "x86_64"
        done
        
        # arm64
        build_java_layer "17" "$JAVA17_DIST_ARM64" "arm64"
        for region in $REGIONS; do
          publish_layer "$JAVA17_DIST_ARM64" "$region" "java11 java17 java21" "${LAYER_NAME_PREFIX}Java17ARM" "arm64"
        done
        ;;
      java21)
        # x86_64
        build_java_layer "21" "$JAVA21_DIST_X86_64" "x86_64"
        for region in $REGIONS; do
          publish_layer "$JAVA21_DIST_X86_64" "$region" "java11 java17 java21" "${LAYER_NAME_PREFIX}Java21X86" "x86_64"
        done
        
        # arm64
        build_java_layer "21" "$JAVA21_DIST_ARM64" "arm64"
        for region in $REGIONS; do
          publish_layer "$JAVA21_DIST_ARM64" "$region" "java11 java17 java21" "${LAYER_NAME_PREFIX}Java21ARM" "arm64"
        done
        ;;
      *)
        echo "Unknown Java version: $java_ver"
        echo "Usage: $0 [java11 | java17 | java21]"
        exit 1
        ;;
    esac
  done
  
  echo ""
  echo "=========================================="
  echo "  ✓ Java layers published successfully   "
  echo "=========================================="
}

main "$@"
