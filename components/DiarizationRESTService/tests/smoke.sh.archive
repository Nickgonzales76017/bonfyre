#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
AUDIO="$ROOT/../LocalAITranscriptionService/tmp/silence.wav"
PORT=8877
python3 "$ROOT/server.py" --host 127.0.0.1 --port "$PORT" >/tmp/diar-rest.log 2>&1 &
PID=$!
trap 'kill $PID >/dev/null 2>&1 || true' EXIT
sleep 1
curl -s "http://127.0.0.1:$PORT/health"
curl -s -X POST "http://127.0.0.1:$PORT/jobs" -H 'Content-Type: application/json' \
  -d "{\"audioPath\":\"$AUDIO\",\"outputDir\":\"$ROOT/tmp-job\",\"dryRun\":true}"
