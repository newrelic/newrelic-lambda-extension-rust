#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/package_layer.sh <target-triple>
# Example (recommended): ./scripts/package_layer.sh x86_64-unknown-linux-musl

TARGET=${1:-x86_64-unknown-linux-musl}
if ! rustup target list --installed | grep -q "$TARGET"; then
  echo "Target $TARGET not installed. Installing..." >&2
  rustup target add "$TARGET"
fi
BIN_NAME=newrelic-lambda-extension

# Ensure we run from repo root so paths resolve
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$ROOT_DIR"

OUT_DIR="$ROOT_DIR/dist"
LAYER_ROOT="$ROOT_DIR/.layer"

mkdir -p "$OUT_DIR"
rm -rf "$LAYER_ROOT"

echo "Packaging $BIN_NAME for target $TARGET" >&2

build_with_cross() {
  echo "Building with cross for $TARGET" >&2
  cross build --release --target "$TARGET"
}

build_with_zig() {
  echo "Building with cargo-zigbuild for $TARGET" >&2
  cargo zigbuild --release --target "$TARGET"
}

build_with_cargo() {
  echo "Building with cargo for $TARGET (native toolchain)" >&2
  cargo build --release --target "$TARGET"
}

# Prefer cargo-zigbuild over cross for musl targets on macOS/ARM
if command -v cargo-zigbuild >/dev/null 2>&1; then
  build_with_zig
elif command -v cross >/dev/null 2>&1; then
  build_with_cross
else
  # If on macOS and targeting linux, using plain cargo will likely fail to link
  if [[ "$(uname -s)" == "Darwin" && "$TARGET" == *"unknown-linux"* ]]; then
    echo "Error: Cross-linking to $TARGET on macOS requires 'cross' or 'cargo-zigbuild'." >&2
    echo "Install one of these and retry:" >&2
    echo "  cargo install cross --git https://github.com/cross-rs/cross" >&2
    echo "  # or" >&2
    echo "  cargo install cargo-zigbuild && brew install zig" >&2
    exit 1
  fi
  build_with_cargo
fi

# Assemble layer structure
mkdir -p "$LAYER_ROOT/extensions"
cp "$ROOT_DIR/target/$TARGET/release/$BIN_NAME" "$LAYER_ROOT/extensions/$BIN_NAME"

# Zip
ARCH="${TARGET%%-*}"
ZIP_NAME="$OUT_DIR/$BIN_NAME-${ARCH}.zip"
(cd "$LAYER_ROOT" && zip -r9 "../$(basename "$ZIP_NAME")" . >/dev/null)

# Cleanup
rm -rf "$LAYER_ROOT"

echo "Created $ZIP_NAME"