#!/usr/bin/env bash
set -euo pipefail
# FswatchReactor — file-watcher reactive pipeline trigger via fswatch
# Usage: fswatch_reactor.sh <subcommand> <path> [--cmd COMMAND] [--pattern GLOB] [--dry-run]
#   subcommands: watch | once | list-events

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DRY_RUN=false
for arg in "$@"; do [[ "$arg" == "--dry-run" ]] && DRY_RUN=true; done

subcmd="${1:-}"
watchpath="${2:-}"

if [[ -z "$subcmd" || -z "$watchpath" ]]; then
  echo "Usage: fswatch_reactor.sh <watch|once|list-events> <path> [--cmd COMMAND] [--pattern '*.wav'] [--dry-run]"
  exit 1
fi

CMD="echo" PATTERN=""
prev=""
for i in "${@:3}"; do
  case "$prev" in --cmd) CMD="$i";; --pattern) PATTERN="$i";; esac
  prev="$i"
done

case "$subcmd" in
  watch)
    if [[ -n "$PATTERN" ]]; then
      filter="--include=\"$PATTERN\" --exclude='.*'"
    else
      filter=""
    fi
    cmd="fswatch -0 $filter \"$watchpath\" | while IFS= read -r -d '' file; do $CMD \"\$file\"; done"
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v fswatch &>/dev/null; then echo "fswatch not found. Install: brew install fswatch"; exit 2; fi
    echo "Watching $watchpath (Ctrl+C to stop)..."
    eval "$cmd"
    ;;
  once)
    cmd="fswatch -1 \"$watchpath\""
    if $DRY_RUN; then echo "Would run: $cmd && $CMD <changed-file>"; exit 0; fi
    if ! command -v fswatch &>/dev/null; then echo "fswatch not found."; exit 2; fi
    changed=$(eval "$cmd")
    echo "Changed: $changed"
    $CMD "$changed"
    ;;
  list-events)
    cmd="fswatch --event-flags -1 \"$watchpath\""
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v fswatch &>/dev/null; then echo "fswatch not found."; exit 2; fi
    eval "$cmd"
    ;;
  *)
    echo "Unknown subcommand: $subcmd"
    exit 1
    ;;
esac
