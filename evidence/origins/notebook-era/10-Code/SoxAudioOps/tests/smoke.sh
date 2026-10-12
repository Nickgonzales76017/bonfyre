#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
echo "=== SoxAudioOps smoke test ==="
for sub in silence-detect fingerprint normalize-peak trim-silence; do
  echo "--- $sub --dry-run ---"
  bash "$here/../bin/sox_ops.sh" "$sub" /tmp/test.wav --dry-run
done
echo "=== PASS ==="
