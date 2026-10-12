#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
cli="$here/../bin/sqlite_graph.py"
db="/tmp/test-family.db"
rm -f "$db"
echo "=== SQLiteGraphStore smoke test ==="
echo "--- init ---"
python3 "$cli" init "$db"
echo "--- add-atom ---"
python3 "$cli" add-atom "$db" --id src-transcript --hash abc123 --media-type text/plain --path source/transcript.txt
echo "--- add-op ---"
python3 "$cli" add-op "$db" --id op-clean --op Clean --inputs src-transcript --output cleaned --version 1.0.0
echo "--- lineage ---"
python3 "$cli" lineage "$db" --id op-clean
echo "--- export ---"
python3 "$cli" export "$db" --out /tmp/test-family.artifact.json
cat /tmp/test-family.artifact.json
echo "--- dry-run ---"
python3 "$cli" --dry-run init "$db"
rm -f "$db" /tmp/test-family.artifact.json
echo "=== PASS ==="
