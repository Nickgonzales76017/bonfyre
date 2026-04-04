#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

export PATH="/Users/nickgonzales/Library/Python/3.9/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

perl drain_all_queues.pl "$@"
