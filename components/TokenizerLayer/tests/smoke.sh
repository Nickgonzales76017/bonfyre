#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
echo "=== TokenizerLayer smoke test ==="
tmp=$(mktemp)
echo "Hello world. This is a test transcript for tokenization." > "$tmp"
echo "--- segment --dry-run ---"
python3 "$here/../bin/tokenizer_layer.py" segment "$tmp" --dry-run
echo "--- count --dry-run ---"
python3 "$here/../bin/tokenizer_layer.py" count "$tmp" --dry-run
echo "--- segment (real, naive fallback) ---"
python3 "$here/../bin/tokenizer_layer.py" segment "$tmp" --out /tmp/tokens-test.json
cat /tmp/tokens-test.json | head -5
rm -f "$tmp" /tmp/tokens-test.json
echo "=== PASS ==="
