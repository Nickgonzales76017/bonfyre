#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
python3 "$ROOT/server.py" --dry-run
test -f "$ROOT/proto/rnnt_streaming.proto"
