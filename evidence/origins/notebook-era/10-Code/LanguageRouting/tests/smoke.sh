#!/usr/bin/env bash
set -euo pipefail

# Dry-run smoke test for lang_route_vosk_pocketsphinx.sh
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT="$ROOT/bin/lang_route_vosk_pocketsphinx.sh"
SILENCE="$(cd "$(dirname "$0")/../../LocalAITranscriptionService/tmp" && pwd)/silence.wav"

if [[ ! -f "$SCRIPT" ]]; then
  echo "Missing script: $SCRIPT" >&2
  exit 2
fi
if [[ ! -f "$SILENCE" ]]; then
  echo "Missing test audio: $SILENCE" >&2
  exit 2
fi

echo "Running dry-run smoke test for LanguageRouting..."
"$SCRIPT" --audio "$SILENCE" --out "$ROOT/tmp-out" --vosk-langs en --dry-run

echo "LanguageRouting smoke test completed (dry-run)."

exit 0
