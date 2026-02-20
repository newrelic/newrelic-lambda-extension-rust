#!/usr/bin/env bash
set -euo pipefail

# Local testing script for building and publishing Java Lambda layers
# Usage: ./javaTestLayer.sh [java11 java17 java21]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Prioritize rustup's Rust over Homebrew's Rust
export PATH="$HOME/.cargo/bin:$PATH"

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

# --- ECR Configuration ---
# Change this to "x6n7b2o2" for production
ECR_REPOSITORY=${ECR_REPOSITORY:-"q6k3q1g1"}
ECR_REGION="us-west-1"

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

  # Prepare a clean extensions directory with only the correct architecture binary
  # Lambda expects the extension at /opt/extensions/newrelic-lambda-extension
  echo "Preparing extensions directory for $arch architecture"

  # Create a temporary clean extensions directory
  rm -rf extensions_temp
  mkdir -p extensions_temp

  if [ "$arch" = "arm64" ]; then
    cp extensions/arm64/newrelic-lambda-extension extensions_temp/newrelic-lambda-extension
  else
    cp extensions/x86_64/newrelic-lambda-extension extensions_temp/newrelic-lambda-extension
  fi

  # Backup original extensions directory and use the clean one
  mv extensions extensions_backup
  mv extensions_temp extensions

  echo "Using extension for $arch architecture"

  ./gradlew packageLayer -P javaVersion="$java_version"

  mkdir -p "$DIST_DIR"
  cp "$GRADLE_ARCHIVE" "$target_zip"

  # Restore the original extensions directory
  rm -rf extensions
  mv extensions_backup extensions

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

# --- Create Dockerfile for ECR ---
create_ecr_dockerfile() {
  local dockerfile_path="$1"

  cat > "$dockerfile_path" << 'EOF'
FROM alpine:latest

ARG layer_zip
ARG file_without_dist

RUN apk update && apk add --no-cache curl unzip

WORKDIR /

COPY ${layer_zip} .

RUN unzip ${file_without_dist} -d ./opt
RUN rm ${file_without_dist}
EOF

  echo "Created Dockerfile at $dockerfile_path"
}

# --- Publish Docker Image to ECR ---
publish_docker_ecr() {
  local layer_archive="$1"
  local runtime_name="$2"
  local arch="$3"

  local arch_flag=""
  local platform=""

  if [[ ${arch} =~ 'arm64' ]]; then
    arch_flag="-arm64"
    platform="linux/arm64"
  else
    arch_flag=""
    platform="linux/amd64"
  fi

  local version_flag=$(echo "$runtime_name" | sed 's/[^0-9]//g')
  local language_flag=$(echo "$runtime_name" | sed 's/[0-9].*//')

  # Get relative path for Docker build context (we'll be in java/ directory)
  # layer_archive is like: /path/to/scripts/java/dist/java17.x86_64.zip
  # We need: dist/java17.x86_64.zip (relative to java/ directory)
  local layer_archive_relative
  if [[ $layer_archive == *"/java/dist/"* ]]; then
    layer_archive_relative="dist/$(basename "$layer_archive")"
  elif [[ $layer_archive == dist/* ]]; then
    layer_archive_relative="$layer_archive"
  else
    layer_archive_relative="dist/$(basename "$layer_archive")"
  fi

  # Remove 'dist/' prefix for the final unzipped filename
  local file_without_dist=$(basename "$layer_archive")

  echo "Layer archive: $layer_archive"
  echo "Relative path for Docker: $layer_archive_relative"
  echo "File name: $file_without_dist"

  echo "=========================================="
  echo "Building and pushing Docker image to ECR"
  echo "Repository: public.ecr.aws/${ECR_REPOSITORY}"
  echo "Image tag: newrelic-lambda-layers-${language_flag}:${version_flag}${arch_flag}"
  echo "Platform: ${platform}"
  echo "=========================================="

  # Create Dockerfile in the java directory
  local dockerfile_path="$SCRIPT_DIR/java/Dockerfile.ecrImage"
  create_ecr_dockerfile "$dockerfile_path"

  cd "$SCRIPT_DIR/java"

  # Temporarily fix credential helper for Rancher Desktop
  local docker_config="$HOME/.docker/config.json"
  local docker_config_backup="$HOME/.docker/config.json.javatestlayer.bak"
  if grep -q '"credsStore"' "$docker_config" 2>/dev/null; then
    echo "Temporarily disabling credsStore for build..."
    cp "$docker_config" "$docker_config_backup"
    # Remove credsStore line
    sed -i.tmp 's/"credsStore".*,//g; s/"credsStore".*//' "$docker_config"
    rm -f "$docker_config.tmp"
  fi

  # Login to ECR (Public ECR always uses us-east-1 for authentication)
  echo "Logging in to ECR..."
  aws ecr-public get-login-password --region us-east-1 | \
    docker login --username AWS --password-stdin public.ecr.aws/${ECR_REPOSITORY}

  # Build Docker image (using regular docker build for Rancher Desktop compatibility)
  echo "Building Docker image..."
  if command -v docker buildx >/dev/null 2>&1; then
    # Try buildx first, but use --load to avoid credential issues
    docker buildx build --platform ${platform} --load \
      -t layer-nr-image-${language_flag}-${version_flag}${arch_flag}:latest \
      -f Dockerfile.ecrImage \
      --build-arg layer_zip=${layer_archive_relative} \
      --build-arg file_without_dist=${file_without_dist} \
      . 2>/dev/null || \
    # Fallback to regular docker build if buildx fails
    docker build \
      -t layer-nr-image-${language_flag}-${version_flag}${arch_flag}:latest \
      -f Dockerfile.ecrImage \
      --build-arg layer_zip=${layer_archive_relative} \
      --build-arg file_without_dist=${file_without_dist} \
      .
  else
    # Use regular docker build
    docker build \
      -t layer-nr-image-${language_flag}-${version_flag}${arch_flag}:latest \
      -f Dockerfile.ecrImage \
      --build-arg layer_zip=${layer_archive_relative} \
      --build-arg file_without_dist=${file_without_dist} \
      .
  fi

  # Tag for ECR
  echo "Tagging image for ECR..."
  docker tag layer-nr-image-${language_flag}-${version_flag}${arch_flag}:latest \
    public.ecr.aws/${ECR_REPOSITORY}/newrelic-lambda-layers-${language_flag}:${version_flag}${arch_flag}

  # Push to ECR
  echo "Pushing to ECR..."
  docker push public.ecr.aws/${ECR_REPOSITORY}/newrelic-lambda-layers-${language_flag}:${version_flag}${arch_flag}

  # Cleanup
  rm -rf "$dockerfile_path"

  # Restore original Docker config if we backed it up
  if [ -f "$docker_config_backup" ]; then
    echo "Restoring Docker config..."
    mv "$docker_config_backup" "$docker_config"
  fi

  echo "Successfully pushed to public.ecr.aws/${ECR_REPOSITORY}/newrelic-lambda-layers-${language_flag}:${version_flag}${arch_flag}"
  echo ""
}

# --- Main ---
main() {
  local java_versions="${1:-java17}"
  
  echo "=========================================="
  echo "  Building Extension Binaries            "
  echo "=========================================="
  build_extension "x86_64-unknown-linux-musl" "x86_64"
  build_extension "aarch64-unknown-linux-musl" "arm64"
  
  for java_ver in $java_versions; do
    echo ""
    echo "=========================================="
    echo "  Building Java Layers ($java_ver)      "
    echo "=========================================="
    
    case $java_ver in
      java11)
        # x86_64
        build_java_layer "11" "$JAVA11_DIST_X86_64" "x86_64"

        # Push to ECR for x86_64
        echo "Pushing Java 11 x86_64 layer to ECR..."
        publish_docker_ecr "$JAVA11_DIST_X86_64" "java11" "x86_64"

        # Publish to Lambda Layer (optional - only if REGIONS is set)
        if [ -n "$REGIONS" ] && [ "$REGIONS" != "" ]; then
          for region in $REGIONS; do
            publish_layer "$JAVA11_DIST_X86_64" "$region" "java11 java17 java21" "${LAYER_NAME_PREFIX}Java11X86" "x86_64"
          done
        fi

        # arm64
        build_java_layer "11" "$JAVA11_DIST_ARM64" "arm64"

        # Push to ECR for arm64
        echo "Pushing Java 11 arm64 layer to ECR..."
        publish_docker_ecr "$JAVA11_DIST_ARM64" "java11" "arm64"

        # Publish to Lambda Layer (optional - only if REGIONS is set)
        if [ -n "$REGIONS" ] && [ "$REGIONS" != "" ]; then
          for region in $REGIONS; do
            publish_layer "$JAVA11_DIST_ARM64" "$region" "java11 java17 java21" "${LAYER_NAME_PREFIX}Java11ARM" "arm64"
          done
        fi
        ;;
      java17)
        # x86_64
        build_java_layer "17" "$JAVA17_DIST_X86_64" "x86_64"

        # Push to ECR for x86_64
        echo "Pushing Java 17 x86_64 layer to ECR..."
        publish_docker_ecr "$JAVA17_DIST_X86_64" "java17" "x86_64"

        # Publish to Lambda Layer (optional - only if REGIONS is set)
        if [ -n "$REGIONS" ] && [ "$REGIONS" != "" ]; then
          for region in $REGIONS; do
            publish_layer "$JAVA17_DIST_X86_64" "$region" "java11 java17 java21" "${LAYER_NAME_PREFIX}Java17X86" "x86_64"
          done
        fi

        # arm64
        build_java_layer "17" "$JAVA17_DIST_ARM64" "arm64"

        # Push to ECR for arm64
        echo "Pushing Java 17 arm64 layer to ECR..."
        publish_docker_ecr "$JAVA17_DIST_ARM64" "java17" "arm64"

        # Publish to Lambda Layer (optional - only if REGIONS is set)
        if [ -n "$REGIONS" ] && [ "$REGIONS" != "" ]; then
          for region in $REGIONS; do
            publish_layer "$JAVA17_DIST_ARM64" "$region" "java11 java17 java21" "${LAYER_NAME_PREFIX}Java17ARM" "arm64"
          done
        fi
        ;;
      java21)
        # x86_64
        build_java_layer "21" "$JAVA21_DIST_X86_64" "x86_64"

        # Push to ECR for x86_64
        echo "Pushing Java 21 x86_64 layer to ECR..."
        publish_docker_ecr "$JAVA21_DIST_X86_64" "java21" "x86_64"

        # Publish to Lambda Layer (optional - only if REGIONS is set)
        if [ -n "$REGIONS" ] && [ "$REGIONS" != "" ]; then
          for region in $REGIONS; do
            publish_layer "$JAVA21_DIST_X86_64" "$region" "java11 java17 java21" "${LAYER_NAME_PREFIX}Java21X86" "x86_64"
          done
        fi

        # arm64
        build_java_layer "21" "$JAVA21_DIST_ARM64" "arm64"

        # Push to ECR for arm64
        echo "Pushing Java 21 arm64 layer to ECR..."
        publish_docker_ecr "$JAVA21_DIST_ARM64" "java21" "arm64"

        # Publish to Lambda Layer (optional - only if REGIONS is set)
        if [ -n "$REGIONS" ] && [ "$REGIONS" != "" ]; then
          for region in $REGIONS; do
            publish_layer "$JAVA21_DIST_ARM64" "$region" "java11 java17 java21" "${LAYER_NAME_PREFIX}Java21ARM" "arm64"
          done
        fi
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
  echo ""
  echo "Docker images pushed to ECR:"
  echo "  Repository: public.ecr.aws/${ECR_REPOSITORY}/newrelic-lambda-layers-java"
  echo ""

  for java_ver in $java_versions; do
    case $java_ver in
      java11)
        echo "  - newrelic-lambda-layers-java:11 (x86_64)"
        echo "  - newrelic-lambda-layers-java:11-arm64 (arm64)"
        ;;
      java17)
        echo "  - newrelic-lambda-layers-java:17 (x86_64)"
        echo "  - newrelic-lambda-layers-java:17-arm64 (arm64)"
        ;;
      java21)
        echo "  - newrelic-lambda-layers-java:21 (x86_64)"
        echo "  - newrelic-lambda-layers-java:21-arm64 (arm64)"
        ;;
    esac
  done

  echo ""
  echo "To pull and use these images:"
  echo "  docker pull public.ecr.aws/${ECR_REPOSITORY}/newrelic-lambda-layers-java:<tag>"
  echo ""
}

main "$@"
