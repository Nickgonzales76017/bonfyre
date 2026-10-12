#!/usr/bin/env bash
set -euo pipefail

# Dry-run smoke test for acoustic_classify.sh
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT="$ROOT/bin/acoustic_classify.sh"
SILENCE="$(cd "$(dirname "$0")/../../LocalAITranscriptionService/tmp" && pwd)/silence.wav"

if [[ ! -f "$SCRIPT" ]]; then
  echo "Missing script: $SCRIPT" >&2
  exit 2
fi
if [[ ! -f "$SILENCE" ]]; then
  echo "Missing test audio: $SILENCE" >&2
  exit 2
fi

echo "Running dry-run smoke test for AcousticClassifier..."
"$SCRIPT" --audio "$SILENCE" --out "$ROOT/tmp-out" --model "$ROOT/acoustic_classifier.onnx" --dry-run

echo "AcousticClassifier smoke test completed (dry-run)."

exit 0
