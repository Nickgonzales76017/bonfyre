#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IN="$ROOT/tmp-raw.txt"
OUT="$ROOT/tmp-out/cleaned.txt"
mkdir -p "$(dirname "$OUT")"
cat > "$IN" <<'TXT'
## Chunk 000 um Thank you Thank you Thank you this is  a   test you you you you you
TXT
python3 "$ROOT/bin/transcript_qa_cleaner.py" --transcript "$IN" --out "$OUT"
cat "$OUT"
cat "$ROOT/tmp-out/status.json"
