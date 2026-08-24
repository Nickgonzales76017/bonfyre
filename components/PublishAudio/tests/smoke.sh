#!/usr/bin/env bash
set -euo pipefail

# Dry-run smoke test for publish_audio.sh
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT="$ROOT/bin/publish_audio.sh"
SILENCE="$(cd "$(dirname "$0")/../../LocalAITranscriptionService/tmp" && pwd)/silence.wav"

if [[ ! -f "$SCRIPT" ]]; then
  echo "Missing script: $SCRIPT" >&2
  exit 2
fi
if [[ ! -f "$SILENCE" ]]; then
  echo "Missing test audio: $SILENCE" >&2
  exit 2
fi

echo "Running dry-run smoke test for PublishAudio..."
"$SCRIPT" --audio "$SILENCE" --out "$ROOT/tmp-out" --tts piper --dry-run

echo "PublishAudio smoke test completed (dry-run)."

exit 0
