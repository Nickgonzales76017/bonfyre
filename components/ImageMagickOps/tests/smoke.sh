#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
echo "=== ImageMagickOps smoke test ==="
for sub in thumbnail waveform resize convert strip-meta; do
  echo "--- $sub --dry-run ---"
  bash "$here/../bin/imagemagick_ops.sh" "$sub" /tmp/test.png --dry-run
done
echo "=== PASS ==="
