#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
echo "=== YtdlpIngest smoke test ==="
for sub in extract-audio extract-meta download list-formats; do
  echo "--- $sub --dry-run ---"
  bash "$here/../bin/ytdlp_ingest.sh" "$sub" "https://example.com/video" --dry-run
done
echo "=== PASS ==="
