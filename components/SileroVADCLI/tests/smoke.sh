#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
AUDIO="$ROOT/../LocalAITranscriptionService/samples/incoming-audio/founder-01-pickfu-assumptions.mp3"
OUT="$ROOT/tmp-out"
rm -rf "$OUT"
mkdir -p "$OUT"
python3 "$ROOT/bin/silero_vad_cli.py" --audio "$AUDIO" --out "$OUT" --min-speech 20.0
cat "$OUT/status.json"
