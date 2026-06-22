#!/usr/bin/env bash
set -euo pipefail

# Local testing script for building and publishing New Relic Java APM Agent layers.
#
# Unlike javaTestLayer.sh (which builds the OpenTracing serverless layer via Gradle),
# this script builds the FULL New Relic Java APM agent layer: the real newrelic.jar
# attached via -javaagent, bundled with the locally-built Rust extension. It mirrors
# the canonical newrelic-lambda-layers/java-agent build, but compiles the extension
# from this repo's source instead of downloading a published binary.
#
# Usage:
#   ./javaApmAgentTestLayer.sh                  # download latest agent, build+publish all
#   ./javaApmAgentTestLayer.sh /path/agent.jar  # use a locally-built agent jar instead
#
# Env overrides:
#   NEWRELIC_AGENT_VERSION   pin a specific agent version (default: latest "current")
#   VARIANTS                 "full slim" (default) — which variants to build
#   ARCHES                   "x86_64 arm64" (default) — which architectures to build
#   REGIONS                  publish regions (default "us-west-1"); empty = skip publish
#   LAYER_NAME_PREFIX        test layer name prefix (default "NRTestRustExtension")
#   BUCKET_PREFIX            S3 bucket prefix (default "nr-extension-test-layers")

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ASSET_DIR="$SCRIPT_DIR/java-agent"

# --- Configuration ---
LAYER_NAME_PREFIX=${LAYER_NAME_PREFIX:-"NRTestRustExtension"}
BUCKET_PREFIX=${BUCKET_PREFIX:-"nr-extension-test-layers"}
REGIONS=${REGIONS:-"us-east-1"}
VARIANTS=${VARIANTS:-"full slim"}
ARCHES=${ARCHES:-"x86_64"} #x86_64 arm64

# Compatible runtimes for the agent layer. The agent only attaches on JVM >= 17
# (see java-agent/lib-handler.sh), so we advertise 17/21 for the test layer.
COMPAT_RUNTIMES=${COMPAT_RUNTIMES:-"java17 java21"}

BUILD_DIR="$SCRIPT_DIR/java-agent/build"
DIST_DIR="$SCRIPT_DIR/java-agent/dist"

# Layer layout constants (must match the canonical java-agent layer).
AGENT_JAR="newrelic.jar"
AGENT_DIR="newrelic"
AGENT_VERSION_FILE="java-agent-version.txt"
EXEC_WRAPPER="newrelic-java-handler"
LIB_HANDLER="lib-handler.sh"
PREVIEW_FILE="preview-extensions-ggqizro707"
BIN_NAME="newrelic-lambda-extension"

# Resolve the default agent version (a pinned fallback lives in versions.sh).
# shellcheck source=/dev/null
NEWRELIC_AGENT_VERSION_DEFAULT=""
if [ -f "$ASSET_DIR/versions.sh" ]; then
  # versions.sh sets NEWRELIC_AGENT_VERSION; capture it without clobbering an env override.
  NEWRELIC_AGENT_VERSION_DEFAULT="$(. "$ASSET_DIR/versions.sh" >/dev/null 2>&1; echo "${NEWRELIC_AGENT_VERSION:-}")"
fi
# If the caller exported NEWRELIC_AGENT_VERSION, honor it; otherwise leave unset = "latest".
NEWRELIC_AGENT_VERSION="${NEWRELIC_AGENT_VERSION:-}"

# Map architecture -> Rust target triple (musl, matching javaTestLayer.sh).
target_for_arch() {
  case "$1" in
    x86_64) echo "x86_64-unknown-linux-musl" ;;
    arm64)  echo "aarch64-unknown-linux-musl" ;;
    *) echo "Unknown arch: $1" >&2; return 1 ;;
  esac
}

# --- Hash Function ---
hash_file() {
  if command -v md5sum &>/dev/null; then
    md5sum "$1" | awk '{ print $1 }'
  else
    md5 -q "$1"
  fi
}

# --- Build Rust extension locally for a given architecture ---
# Produces the binary at $BUILD_DIR/extensions/<arch>/newrelic-lambda-extension
build_extension() {
  local arch="$1"
  local target
  target="$(target_for_arch "$arch")"

  echo "Building Rust extension for $arch ($target)"
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

  mkdir -p "$BUILD_DIR/extensions/$arch"
  cp "$ROOT_DIR/target/$target/release/$BIN_NAME" "$BUILD_DIR/extensions/$arch/$BIN_NAME"
  echo "Extension binary ready for $arch"
}

# --- Obtain the New Relic Java agent jar ---
# Downloads the latest (or pinned) agent, or copies a locally-supplied jar.
# Leaves the jar at $BUILD_DIR/$AGENT_JAR and prints the resolved version.
get_agent() {
  local agent_path="${1:-}"
  local dest="$BUILD_DIR/$AGENT_JAR"
  mkdir -p "$BUILD_DIR"
  rm -f "$dest"

  if [[ -n "$agent_path" ]]; then
    echo "Using local agent jar: $agent_path" >&2
    cp "$agent_path" "$dest"
  elif [[ -n "$NEWRELIC_AGENT_VERSION" ]]; then
    local url="https://download.newrelic.com/newrelic/java-agent/newrelic-agent/${NEWRELIC_AGENT_VERSION}/newrelic-agent-${NEWRELIC_AGENT_VERSION}.jar"
    echo "Downloading pinned agent v${NEWRELIC_AGENT_VERSION} from $url" >&2
    curl -fL "$url" -o "$dest"
  else
    local url="https://download.newrelic.com/newrelic/java-agent/newrelic-agent/current/newrelic-agent.jar"
    echo "Downloading latest agent from $url" >&2
    curl -fL "$url" -o "$dest"
  fi

  # Resolve the concrete version from the jar manifest for the version file.
  local resolved
  resolved="$(unzip -p "$dest" META-INF/MANIFEST.MF 2>/dev/null \
    | awk -F': ' '/Implementation-Version/ { gsub(/\r/,"",$2); print $2; exit }')"
  if [[ -z "$resolved" ]]; then
    resolved="${NEWRELIC_AGENT_VERSION:-${NEWRELIC_AGENT_VERSION_DEFAULT:-unknown}}"
  fi
  echo "$resolved"
}

# --- Assemble a single layer zip (variant x arch) ---
# Layout matches the canonical java-agent layer:
#   extensions/newrelic-lambda-extension
#   preview-extensions-ggqizro707
#   newrelic/newrelic.jar
#   newrelic/java-agent-version.txt
#   newrelic-java-handler            (exec wrapper; full or slim)
#   lib-handler.sh
assemble_layer() {
  local variant="$1"   # full | slim
  local arch="$2"      # x86_64 | arm64
  local agent_version="$3"
  local target_zip="$4"

  local handler_src
  case "$variant" in
    full) handler_src="$ASSET_DIR/java-handler-full" ;;
    slim) handler_src="$ASSET_DIR/java-handler-slim" ;;
    *) echo "Unknown variant: $variant" >&2; return 1 ;;
  esac

  echo "Assembling Java agent layer (variant=$variant, arch=$arch, agent=v${agent_version})"

  local stage="$BUILD_DIR/stage-${variant}-${arch}"
  rm -rf "$stage"
  mkdir -p "$stage/extensions" "$stage/$AGENT_DIR"

  # Extension binary for this architecture.
  cp "$BUILD_DIR/extensions/$arch/$BIN_NAME" "$stage/extensions/$BIN_NAME"
  chmod +x "$stage/extensions/$BIN_NAME"

  # Legacy extensions-API opt-in marker (empty file), as in the published layer.
  : > "$stage/$PREVIEW_FILE"

  # Agent jar + version file.
  cp "$BUILD_DIR/$AGENT_JAR" "$stage/$AGENT_DIR/$AGENT_JAR"
  echo "$agent_version" > "$stage/$AGENT_DIR/$AGENT_VERSION_FILE"

  # Exec wrapper + shared handler library.
  cp "$handler_src" "$stage/$EXEC_WRAPPER"
  chmod +x "$stage/$EXEC_WRAPPER"
  cp "$ASSET_DIR/$LIB_HANDLER" "$stage/$LIB_HANDLER"
  chmod +x "$stage/$LIB_HANDLER"

  mkdir -p "$DIST_DIR"
  rm -f "$target_zip"
  ( cd "$stage" && zip -rq9 "$target_zip" \
      extensions "$PREVIEW_FILE" "$AGENT_DIR" "$EXEC_WRAPPER" "$LIB_HANDLER" )

  rm -rf "$stage"
  echo "Build complete: $target_zip"
}

# --- Publish Layer (test layer; no ECR) ---
publish_layer() {
  local layer_archive="$1"
  local region="$2"
  local arch="$3"        # x86_64 | arm64
  local variant="$4"     # full | slim
  local agent_version="$5"

  local hash
  hash=$(hash_file "$layer_archive")
  local bucket_name="${BUCKET_PREFIX}-${region}"
  local s3_key="nr-java-agent/${variant}/${hash}.${arch}.zip"

  # Layer name: <prefix>AgentJava[ARM64][-slim]
  local arch_part=""
  [ "$arch" = "arm64" ] && arch_part="ARM64"
  local layer_name="${LAYER_NAME_PREFIX}AgentJava${arch_part}"
  [ "$variant" = "slim" ] && layer_name="${layer_name}-slim"

  echo "Uploading ${layer_archive} to s3://${bucket_name}/${s3_key}"
  aws --region "$region" s3 cp "$layer_archive" "s3://${bucket_name}/${s3_key}"

  echo "Publishing ${layer_name} to ${region} (arch: ${arch}, agent: v${agent_version})"
  local layer_output
  layer_output=$(aws lambda publish-layer-version \
    --layer-name "${layer_name}" \
    --content "S3Bucket=${bucket_name},S3Key=${s3_key}" \
    --description "New Relic Test Java APM Agent Layer v${agent_version} (${variant}, ${arch})" \
    --license-info "Apache-2.0" \
    --compatible-architectures "${arch}" \
    --compatible-runtimes $COMPAT_RUNTIMES \
    --region "$region" \
    --output json)

  local layer_version layer_arn full_layer_arn
  layer_version=$(echo "$layer_output" | jq -r '.Version')
  layer_arn=$(echo "$layer_output" | jq -r '.LayerArn')
  full_layer_arn="${layer_arn}:${layer_version}"

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

  local arch_upper variant_upper
  arch_upper=$(echo "$arch" | tr '[:lower:]' '[:upper:]' | tr '-' '_')
  variant_upper=$(echo "$variant" | tr '[:lower:]' '[:upper:]')
  echo "export LAYER_ARN_AGENTJAVA_${variant_upper}_${arch_upper}='$full_layer_arn'"

  echo "Cleaning up ${layer_archive}"
  rm -f "$layer_archive"
}

# --- Main ---
main() {
  local agent_path="${1:-}"

  echo "=========================================="
  echo "  Building Rust extension binaries        "
  echo "=========================================="
  for arch in $ARCHES; do
    build_extension "$arch"
  done

  echo ""
  echo "=========================================="
  echo "  Fetching New Relic Java APM agent       "
  echo "=========================================="
  local agent_version
  agent_version="$(get_agent "$agent_path")"
  echo "Resolved Java agent version: v${agent_version}"

  for variant in $VARIANTS; do
    for arch in $ARCHES; do
      echo ""
      echo "=========================================="
      echo "  Java APM Agent layer: ${variant} / ${arch}"
      echo "=========================================="
      local target_zip="$DIST_DIR/java-agent-${variant}.${arch}.zip"
      assemble_layer "$variant" "$arch" "$agent_version" "$target_zip"

      if [ -n "$REGIONS" ]; then
        for region in $REGIONS; do
          publish_layer "$target_zip" "$region" "$arch" "$variant" "$agent_version"
        done
      else
        echo "REGIONS empty — skipping publish. Layer zip left at: $target_zip"
      fi
    done
  done

  # Clean up the downloaded agent jar.
  rm -f "$BUILD_DIR/$AGENT_JAR"

  echo ""
  echo "=========================================="
  echo "  ✓ Java APM Agent layers done (v${agent_version})"
  echo "=========================================="
}

main "$@"
