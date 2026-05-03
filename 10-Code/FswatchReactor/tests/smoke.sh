#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
echo "=== FswatchReactor smoke test ==="
for sub in watch once list-events; do
  echo "--- $sub --dry-run ---"
  bash "$here/../bin/fswatch_reactor.sh" "$sub" /tmp --cmd "echo TRIGGERED:" --dry-run
done
echo "=== PASS ==="
