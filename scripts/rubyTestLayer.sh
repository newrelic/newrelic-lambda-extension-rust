#!/usr/bin/env bash
set -euo pipefail

# Local testing script for building and publishing Ruby Lambda layers with Rust extension
# Adapted from testlayer.sh for Ruby 3.3 runtime

# Ensure we run from repo root so paths resolve
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

# --- Configuration ---
LAYER_NAME_PREFIX=${LAYER_NAME_PREFIX:-"NRTestRubyRustExtension"}

BUCKET_PREFIX=${BUCKET_PREFIX:-"nr-extension-test-layers"}
REGIONS_X86_64=${REGIONS_X86_64:-"us-west-1"}
REGIONS_ARM64=${REGIONS_ARM64:-"us-west-1"}

# Ruby configuration
RUBY_VERSION=${RUBY_VERSION:-"3.3"}
RUBY_ASSETS_DIR="$SCRIPT_DIR/ruby"
WRAPPER_FILE="newrelic_lambda_wrapper.rb"
GEMFILE="Gemfile"

# Extension configuration
BIN_NAME="newrelic-lambda-extension"
DIST_DIR="$ROOT_DIR/dist"
LAYER_DIR="$ROOT_DIR/.layer"
TMP_ENV_FILE_NAME="$DIST_DIR/nr_tmp_env_ruby.sh"

# Agent version tracking
NEWRELIC_RUBY_AGENT_VERSION=""

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

# Creates the Ruby wrapper file
create_ruby_wrapper() {
  local wrapper_path="$1"
  cat > "$wrapper_path" << 'EOF'
# frozen_string_literal: true

ENV['NEW_RELIC_DISTRIBUTED_TRACING_ENABLED'] ||= 'true'
ENV['AWS_LAMBDA_FUNCTION_NAME'] ||= 'lambda_function'
ENV['NEW_RELIC_APP_NAME'] ||= ENV.fetch('AWS_LAMBDA_FUNCTION_NAME', nil)
ENV['NEW_RELIC_TRUSTED_ACCOUNT_KEY'] = ENV.fetch('NEW_RELIC_ACCOUNT_ID', '')

class NewRelicLambdaWrapper
  HANDLER_VAR = 'NEW_RELIC_LAMBDA_HANDLER'
  NR_LAYER_GEM_PATH = "/opt/ruby/gems/#{RUBY_VERSION.rpartition('.').first}.0".freeze

  def self.adjust_load_path
    return unless Dir.exist?(NR_LAYER_GEM_PATH)

    # Add the gems directory to load path
    gem_dirs = Dir.glob(File.join(NR_LAYER_GEM_PATH, 'gems', '*'))
    gem_dirs.each do |gem_dir|
      lib_dir = File.join(gem_dir, 'lib')
      $LOAD_PATH.unshift(lib_dir) if Dir.exist?(lib_dir) && !$LOAD_PATH.include?(lib_dir)
    end
    
    # Also check specifications directory exists
    specs_dir = File.join(NR_LAYER_GEM_PATH, 'specifications')
    if Dir.exist?(specs_dir)
      # Add to GEM_PATH if not already there
      gem_path = ENV['GEM_PATH'] || ''
      unless gem_path.split(':').include?(NR_LAYER_GEM_PATH)
        ENV['GEM_PATH'] = [NR_LAYER_GEM_PATH, gem_path].reject(&:empty?).join(':')
      end
    end
  end

  def self.require_ruby_agent
    adjust_load_path
    require 'newrelic_rpm'
  rescue StandardError => e
    raise "#{self.class.name}: failed to require New Relic layer provided gem(s) - #{e}"
  end

  def self.method_name_and_namespace
    @method_name_and_namespace ||= parse_customer_handler_string
  rescue StandardError => e
    raise "#{self.class.name}: failed to prep the Lambda function to be wrapped - #{e}"
  end

  def self.parse_customer_handler_string
    handler_string = ENV.fetch(HANDLER_VAR, nil)
    raise "Environment variable '#{HANDLER_VAR}' is not set!" unless handler_string

    elements = handler_string.split('.')
    ridx = determine_ridx(elements)
    file = elements[0..ridx].join('.')
    method_string = elements[(ridx + 1)..].join('.')

    require_source_file(file)

    method_string.split('.').reverse
  end
  private_class_method :parse_customer_handler_string

  def self.determine_ridx(elements)
    if elements.size == 1
      raise "Failed to parse the '#{HANDLER_VAR}' env var which is expected to be in '<path>.<method>' format!"
    end

    elements.size > 2 ? -3 : -2
  end
  private_class_method :determine_ridx

  def self.require_source_file(path)
    path = "#{path}.rb" unless path.end_with?('.rb')
    path = "#{Dir.pwd}/#{path}" unless path.start_with?('/')
    raise "Path '#{path}' does not exist or is not readable" unless File.exist?(path) && File.readable?(path)

    require_relative path
  end
  private_class_method :require_source_file
end

NewRelicLambdaWrapper.method_name_and_namespace
NewRelicLambdaWrapper.require_ruby_agent

def handler(event:, context:)
  method_name, namespace = NewRelicLambdaWrapper.method_name_and_namespace
  NewRelic::Agent.agent.serverless_handler.invoke_lambda_function_with_new_relic(event:,
                                                                                 context:,
                                                                                 method_name:,
                                                                                 namespace:)
end
EOF
}

# Bundles Ruby gems
bundle_ruby_gems() {
  local ruby_version="$1"
  local layer_ruby_dir="$2"  # Where to put the final ruby/ directory
  
  echo "Bundling Ruby gems for version $ruby_version" >&2
  
  # Create temporary directory for bundling
  local temp_dir=$(mktemp -d)
  cd "$temp_dir"
  
  # Copy Gemfile
  cp "$RUBY_ASSETS_DIR/$GEMFILE" .
  
  # Configure bundler to install in current directory
  bundle config set --local without development >/dev/null 2>&1
  bundle config set --local path . >/dev/null 2>&1
  
  # Install gems
  echo "Installing newrelic_rpm gem..." >&2
  bundle install --quiet
  
  # Find the bundled version directory (e.g., ./ruby/3.3.0)
  local bundled_dir=$(find ruby -maxdepth 1 -type d -name "[0-9]*" 2>/dev/null | head -n 1)
  if [ -z "$bundled_dir" ]; then
    echo "Error: Could not find bundled Ruby version directory" >&2
    cd "$ROOT_DIR"
    rm -rf "$temp_dir"
    return 1
  fi
  
  # Create the target structure: ruby/gems/3.3.0/
  mkdir -p "$layer_ruby_dir/gems"
  mv "$bundled_dir" "$layer_ruby_dir/gems/$ruby_version.0"
  
  # Clean up unnecessary directories
  local target_dir="$layer_ruby_dir/gems/$ruby_version.0"
  for sub_dir in 'bin' 'build_info' 'cache' 'doc' 'extensions' 'plugins'; do
    rm -rf "$target_dir/$sub_dir" 2>/dev/null || true
  done
  
  # Extract and save agent version
  local agent_dir=$(find "$target_dir/gems" -type d -name "newrelic_rpm-*" 2>/dev/null | head -n 1)
  if [ -n "$agent_dir" ]; then
    NEWRELIC_RUBY_AGENT_VERSION=$(basename "$agent_dir" | cut -d'-' -f2-)
    echo "$NEWRELIC_RUBY_AGENT_VERSION" > "$layer_ruby_dir/version.txt"
    echo "New Relic Ruby Agent version: $NEWRELIC_RUBY_AGENT_VERSION" >&2
  else
    echo "Warning: Could not determine newrelic_rpm version" >&2
    NEWRELIC_RUBY_AGENT_VERSION="unknown"
  fi
  
  cd "$ROOT_DIR"
  rm -rf "$temp_dir"
}

# Builds a complete Ruby layer with agent + extension
build_ruby_layer() {
  local target="$1"
  local arch_linux="$2"  # x86_64 or arm64
  local ruby_version="$3"
  local zip_name="$DIST_DIR/ruby${ruby_version//./}-${arch_linux}.zip"

  echo "Building New Relic layer for Ruby $ruby_version ($arch_linux)" >&2

  rm -rf "$LAYER_DIR"
  mkdir -p "$LAYER_DIR/ruby"
  
  # Bundle Ruby gems
  bundle_ruby_gems "$ruby_version" "$LAYER_DIR/ruby"
  
  # Copy wrapper to lib directory
  mkdir -p "$LAYER_DIR/ruby/lib"
  cp "$RUBY_ASSETS_DIR/$WRAPPER_FILE" "$LAYER_DIR/ruby/lib/"
  
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
    --description "New Relic Layer for Ruby ${RUBY_VERSION} (${arch}) - Agent ${NEWRELIC_RUBY_AGENT_VERSION}" \
    --license-info "Apache-2.0" \
    --compatible-runtimes "ruby${RUBY_VERSION}" \
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
  # Check for required tools
  if ! command -v bundle &> /dev/null; then
    echo "Error: 'bundle' command not found. Please install Bundler: gem install bundler" >&2
    exit 1
  fi

  mkdir -p "$DIST_DIR"
  rm -f "$TMP_ENV_FILE_NAME"
  touch "$TMP_ENV_FILE_NAME"

  echo ""
  echo "=========================================="
  echo "  Building Ruby Lambda Layers            "
  echo "=========================================="
  echo "  Ruby Version: ${RUBY_VERSION}"
  echo "  Layer Name Prefix: ${LAYER_NAME_PREFIX}"
  echo "=========================================="
  echo ""

  # --- Build for x86_64 ---
  echo "=== Building x86_64 architecture ==="
  local target_x86="x86_64-unknown-linux-musl"
  build_extension "$target_x86"
  
  local zip_x86
  zip_x86=$(build_ruby_layer "$target_x86" "x86_64" "$RUBY_VERSION")
  
  for region in $REGIONS_X86_64; do
    publish_layer "$zip_x86" "$region" "ruby${RUBY_VERSION}" "x86_64" "${LAYER_NAME_PREFIX}Ruby${RUBY_VERSION//./}X86"
  done

  # --- Build for arm64 ---
  echo ""
  echo "=== Building arm64 architecture ==="
  local target_arm="aarch64-unknown-linux-musl"
  build_extension "$target_arm"
  
  local zip_arm
  zip_arm=$(build_ruby_layer "$target_arm" "arm64" "$RUBY_VERSION")
  
  for region in $REGIONS_ARM64; do
    publish_layer "$zip_arm" "$region" "ruby${RUBY_VERSION}" "arm64" "${LAYER_NAME_PREFIX}Ruby${RUBY_VERSION//./}ARM64"
  done

  echo ""
  echo "=========================================="
  echo "  All Ruby layers published successfully!"
  echo "=========================================="
  echo ""
  echo "Environment variables saved to $TMP_ENV_FILE_NAME"
  cat "$TMP_ENV_FILE_NAME"

  cleanup_build_artifacts

  echo ""
  echo "To load the layer ARNs into your environment, run:"
  echo "  source $TMP_ENV_FILE_NAME"
  echo ""
  echo "Note: Ruby layers are compatible with ruby${RUBY_VERSION} runtime"
  echo "      New Relic Ruby Agent version: ${NEWRELIC_RUBY_AGENT_VERSION}"
}

main "$@"
