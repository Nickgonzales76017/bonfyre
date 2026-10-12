#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
echo "=== PandocFormatter smoke test ==="
for sub in convert brief-to-html brief-to-pdf brief-to-epub vtt-render; do
  echo "--- $sub --dry-run ---"
  bash "$here/../bin/pandoc_format.sh" "$sub" /tmp/brief.md --dry-run
done
echo "=== PASS ==="
