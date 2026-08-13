#!/usr/bin/env bash
set -euo pipefail

# Dry-run smoke test for diarize_pyannote_resemblyzer.sh
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT="$ROOT/bin/diarize_pyannote_resemblyzer.sh"
SILENCE="$(cd "$(dirname "$0")/../../LocalAITranscriptionService/tmp" && pwd)/silence.wav"

if [[ ! -f "$SCRIPT" ]]; then
  echo "Missing script: $SCRIPT" >&2
  exit 2
fi
if [[ ! -f "$SILENCE" ]]; then
  echo "Missing test audio: $SILENCE" >&2
  exit 2
fi

echo "Running dry-run smoke test for SpeakerDiarization..."
"$SCRIPT" --audio "$SILENCE" --out "$ROOT/tmp-out" --dry-run

echo "Diarization smoke test completed (dry-run)."

exit 0
