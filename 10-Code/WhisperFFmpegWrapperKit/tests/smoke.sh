#!/usr/bin/env bash
set -euo pipefail

ROOT=$(dirname "$0")/..
SCRIPT="$ROOT/bin/vad_rnnoise_whisper.sh"

echo "Running smoke test: --help"
"$SCRIPT" --help >/dev/null
echo "Smoke test passed: $SCRIPT --help returned 0"

exit 0
