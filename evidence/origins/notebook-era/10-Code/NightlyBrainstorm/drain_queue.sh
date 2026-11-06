#!/bin/zsh
set -euo pipefail
# ─────────────────────────────────────────────────────────────
# DEPRECATED: Use .bonfyre-runtime/drain_all_queues.sh instead
# This script still works for standalone use, but the unified
# dispatcher coordinates both queues to prevent conflicts.
# ─────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

perl nightly.pl \
  --process-queued \
  --max-queued-jobs 3 \
  --no-audio
