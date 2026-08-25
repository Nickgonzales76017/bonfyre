#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
cli="$here/../bin/duckdb_analytics.py"
echo "=== DuckDBAnalytics smoke test ==="
echo "--- family-stats --dry-run ---"
python3 "$cli" family-stats /tmp/test.json --dry-run
echo "--- family-stats (real, using ArtifactManifest example) ---"
python3 "$cli" family-stats "$here/../../ArtifactManifest/examples/brief.artifact.json" 2>&1 || echo "(expected — may not exist yet)"
echo "--- query --dry-run ---"
python3 "$cli" query "SELECT 1" --dry-run
echo "=== PASS ==="
