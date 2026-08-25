#!/bin/zsh
set -euo pipefail
# ─────────────────────────────────────────────────────────────
# DEPRECATED: Use .bonfyre-runtime/drain_all_queues.sh instead
# This script still works for standalone use, but the unified
# dispatcher coordinates both queues to prevent conflicts.
# ─────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

export PATH="/Users/nickgonzales/Library/Python/3.9/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

PYTHONPATH=src python3 -m local_ai_transcription_service.cli \
  --process-queued \
  --max-queued-jobs 1 \
  --output-root outputs
