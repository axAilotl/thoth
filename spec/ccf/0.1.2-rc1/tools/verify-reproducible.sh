#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
reproduction_root="$(mktemp -d)"
reproduction_package="$reproduction_root/ccf-0.1.2-rc1"
published_metadata="$reproduction_root/published-metadata"

cleanup() {
  rm -rf -- "$reproduction_root"
}
trap cleanup EXIT

cp -a "$ROOT" "$reproduction_package"
mkdir "$published_metadata"
cp "$ROOT/SHA256SUMS" "$published_metadata/SHA256SUMS"
cp "$ROOT/PACKAGE-INVENTORY.md" "$published_metadata/PACKAGE-INVENTORY.md"
cd "$reproduction_package"
node tools/build-catalog.mjs >/dev/null
node tools/build-example.mjs >/dev/null
node tools/build-vectors.mjs >/dev/null
node tools/build-package-metadata.mjs >/dev/null
cmp --silent "$published_metadata/PACKAGE-INVENTORY.md" PACKAGE-INVENTORY.md || {
  echo "FAIL: rebuilt package inventory differs from the published inventory" >&2
  if ! diff --unified "$published_metadata/PACKAGE-INVENTORY.md" PACKAGE-INVENTORY.md >&2; then
    echo "Inventory difference printed above." >&2
  fi
  exit 1
}
cmp --silent "$published_metadata/SHA256SUMS" SHA256SUMS || {
  echo "FAIL: rebuilt checksum manifest differs from the published manifest" >&2
  exit 1
}
cp "$published_metadata/SHA256SUMS" SHA256SUMS
sha256sum --check --quiet SHA256SUMS

echo "CCF 0.1.2-rc1 artifacts reproduce byte-for-byte in a clean copy."
