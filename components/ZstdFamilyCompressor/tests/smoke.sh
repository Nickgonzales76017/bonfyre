#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
echo "=== ZstdFamilyCompressor smoke test ==="
for sub in train-dict compress decompress pack-family; do
  echo "--- $sub --dry-run ---"
  bash "$here/../bin/zstd_compress.sh" "$sub" /tmp/test --dry-run
done
echo "=== PASS ==="
