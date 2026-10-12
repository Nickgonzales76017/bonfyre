#!/usr/bin/env bash
set -euo pipefail

# Smoke test (dry-run) for align_whisperx_mfa.sh
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT="$ROOT/bin/align_whisperx_mfa.sh"
SILENCE="$(cd "$(dirname "$0")/../../LocalAITranscriptionService/tmp" && pwd)/silence.wav"

if [[ ! -f "$SCRIPT" ]]; then
  echo "Missing script: $SCRIPT" >&2
  exit 2
fi
if [[ ! -f "$SILENCE" ]]; then
  echo "Missing test audio: $SILENCE" >&2
  exit 2
fi

echo "Running dry-run smoke test for WhisperAlignment..."
"$SCRIPT" --audio "$SILENCE" --out "$ROOT/tmp-out" --dry-run

echo "Smoke test completed (dry-run)."

exit 0
