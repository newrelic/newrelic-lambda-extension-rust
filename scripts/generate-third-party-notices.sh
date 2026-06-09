#!/usr/bin/env bash
set -euo pipefail

# Regenerates THIRD_PARTY_NOTICES.md from Cargo.lock using cargo-about.
#
# The generated file pins every direct AND transitive dependency to the exact
# version in Cargo.lock, so the attribution stays accurate for CVE / license
# audits. It is refreshed automatically on every release (see
# .github/workflows/prepare-release.yml) — run this locally if you change
# dependencies and want to preview/commit the updated notices.
#
# Config:   about.toml   (accepted SPDX licenses — generation FAILS on an
#                          unlisted license, forcing a compliance decision)
# Template: about.hbs     (Markdown layout: header, versioned dependency list,
#                          deduplicated license texts, footer)
#
# Usage:
#   scripts/generate-third-party-notices.sh           # write THIRD_PARTY_NOTICES.md
#   scripts/generate-third-party-notices.sh --check    # fail if it would change (CI drift guard)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

OUTPUT="THIRD_PARTY_NOTICES.md"
TEMPLATE="about.hbs"
CONFIG="about.toml"

if ! command -v cargo-about >/dev/null 2>&1; then
  echo "Error: cargo-about is not installed." >&2
  echo "Install it with: cargo install --locked cargo-about --features cli" >&2
  exit 1
fi

echo "→ Generating ${OUTPUT} from Cargo.lock via cargo-about..."

mode="${1:-write}"
case "$mode" in
  --check)
    tmp="$(mktemp)"
    trap 'rm -f "$tmp"' EXIT
    cargo about generate --config "$CONFIG" "$TEMPLATE" > "$tmp"
    if ! diff -u "$OUTPUT" "$tmp"; then
      echo "" >&2
      echo "Error: ${OUTPUT} is out of date with Cargo.lock." >&2
      echo "Run 'scripts/generate-third-party-notices.sh' and commit the result." >&2
      exit 1
    fi
    echo "✓ ${OUTPUT} is in sync with Cargo.lock."
    ;;
  write|"")
    cargo about generate --config "$CONFIG" "$TEMPLATE" > "$OUTPUT"
    echo "✓ Wrote ${OUTPUT}."
    ;;
  *)
    echo "Unknown argument: $mode (expected nothing or --check)" >&2
    exit 2
    ;;
esac
