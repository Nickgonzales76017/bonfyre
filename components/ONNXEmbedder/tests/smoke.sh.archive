#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT="$ROOT/bin/embed_onnx.py"
TEXT="$ROOT/../LocalAITranscriptionService/tmp/silence.txt"
mkdir -p "$ROOT/tmp-out"

if [[ ! -f "$SCRIPT" ]]; then
  echo "Missing script: $SCRIPT" >&2
  exit 2
fi

echo "Running ONNX embedder dry-run smoke test..."
python3 "$SCRIPT" --text "$TEXT" --out "$ROOT/tmp-out/silence.npy" --dry-run
echo "ONNX embedder smoke done."
