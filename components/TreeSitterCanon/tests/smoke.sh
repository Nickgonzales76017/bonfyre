#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
cli="$here/../bin/treesitter_canon.py"
echo "=== TreeSitterCanon smoke test ==="
echo "--- parse --dry-run ---"
python3 "$cli" parse /tmp/test.py --dry-run
echo "--- parse (real, self-parse) ---"
python3 "$cli" parse "$cli" --out /tmp/ts-self.ast.json
head -10 /tmp/ts-self.ast.json
echo "--- canon --dry-run ---"
python3 "$cli" canon /tmp/test.py --dry-run
rm -f /tmp/ts-self.ast.json
echo "=== PASS ==="
