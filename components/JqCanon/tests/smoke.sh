#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
echo "=== JqCanon smoke test ==="
tmp=$(mktemp).json
echo '{"b":2,"a":1,"c":3}' > "$tmp"
echo "--- canonicalize --dry-run ---"
bash "$here/../bin/jq_canon.sh" canonicalize "$tmp" --dry-run
echo "--- query --dry-run ---"
bash "$here/../bin/jq_canon.sh" query "$tmp" --filter '.a' --dry-run
echo "--- canonicalize (real) ---"
bash "$here/../bin/jq_canon.sh" canonicalize "$tmp"
echo "--- query (real) ---"
bash "$here/../bin/jq_canon.sh" query "$tmp" --filter '.a'
rm -f "$tmp"
echo "=== PASS ==="
