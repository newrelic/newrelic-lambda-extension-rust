#!/usr/bin/env bash
set -euo pipefail

# Script to prepare release artifacts for New Relic Lambda Extension
# Creates standalone extension zip files for both architectures

# Ensure we run from repo root so paths resolve
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

BIN_NAME="newrelic-lambda-extension"
RELEASE_DIR="$ROOT_DIR/release"
LAYER_DIR="$ROOT_DIR/.layer"

echo "=========================================="
echo "  New Relic Lambda Extension Release"
echo "=========================================="
echo ""

# Create release directory
mkdir -p "$RELEASE_DIR"

# --- Build Functions ---

build_extension() {
  local target="$1"
  echo "→ Building extension for target $target"

  if ! rustup target list --installed | grep -q "$target"; then
    echo "  Installing Rust target $target..."
    rustup target add "$target"
  fi

  # Prefer cargo-zigbuild over cross for musl targets
  if command -v cargo-zigbuild >/dev/null 2>&1; then
    echo "  Using cargo-zigbuild..."
    cargo zigbuild --release --target "$target" --target-dir "$ROOT_DIR/target"
  elif command -v cross >/dev/null 2>&1; then
    echo "  Using cross..."
    cross build --release --target "$target" --target-dir "$ROOT_DIR/target"
  else
    if [[ "$(uname -s)" == "Darwin" && "$target" == *"unknown-linux"* ]]; then
      echo "Error: Cross-compiling to Linux on macOS requires 'cross' or 'cargo-zigbuild'." >&2
      echo "Install with: cargo install cross" >&2
      exit 1
    fi
    echo "  Using cargo (native toolchain)..."
    cargo build --release --target "$target" --target-dir "$ROOT_DIR/target"
  fi

  echo "✓ Build complete for $target"
  echo ""
}

package_extension() {
  local target="$1"
  local arch_name="$2"
  local zip_name="$RELEASE_DIR/${BIN_NAME}.${arch_name}.zip"

  echo "→ Packaging extension for $arch_name"

  rm -rf "$LAYER_DIR"
  mkdir -p "$LAYER_DIR/extensions"
  
  cp "$ROOT_DIR/target/$target/release/$BIN_NAME" "$LAYER_DIR/extensions/$BIN_NAME"
  
  echo "  Creating zip archive..."
  (cd "$LAYER_DIR" && zip -r9 "$zip_name" . >/dev/null)
  
  rm -rf "$LAYER_DIR"

  local size
  size=$(du -h "$zip_name" | cut -f1)
  echo "✓ Created: $zip_name ($size)"
  echo ""
}

# --- Main Execution ---

echo "Step 1: Building x86_64 extension"
echo "-----------------------------------"
build_extension "x86_64-unknown-linux-musl"
package_extension "x86_64-unknown-linux-musl" "x86_64"

echo "Step 2: Building ARM64 extension"
echo "-----------------------------------"
build_extension "aarch64-unknown-linux-musl"
package_extension "aarch64-unknown-linux-musl" "arm64"

echo "=========================================="
echo "  ✓ Release artifacts ready!"
echo "=========================================="
echo ""
echo "Files created in: $RELEASE_DIR"
ls -lh "$RELEASE_DIR"/*.zip 2>/dev/null || echo "No zip files found"
echo ""