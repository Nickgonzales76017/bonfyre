#!/usr/bin/env bash
set -euo pipefail
# JqCanon — jq-based JSON canonicalization and manifest transforms
# Usage: jq_canon.sh <subcommand> <input.json> [--filter EXPR] [--out FILE] [--dry-run]
#   subcommands: canonicalize | query | transform | diff

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DRY_RUN=false
for arg in "$@"; do [[ "$arg" == "--dry-run" ]] && DRY_RUN=true; done

subcmd="${1:-}"
input="${2:-}"

if [[ -z "$subcmd" || -z "$input" ]]; then
  echo "Usage: jq_canon.sh <canonicalize|query|transform|diff> <input.json> [--filter EXPR] [--out FILE] [--dry-run]"
  exit 1
fi

FILTER="."
OUT=""
prev=""
for i in "${@:3}"; do
  case "$prev" in --filter) FILTER="$i";; --out) OUT="$i";; esac
  prev="$i"
done

case "$subcmd" in
  canonicalize)
    cmd="jq -S '.' \"$input\""
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v jq &>/dev/null; then echo "jq not found. Install: brew install jq"; exit 2; fi
    if [[ -n "$OUT" ]]; then
      eval "$cmd" > "$OUT"
      echo "Canonicalized: $OUT"
    else
      eval "$cmd"
    fi
    ;;
  query)
    cmd="jq '$FILTER' \"$input\""
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v jq &>/dev/null; then echo "jq not found."; exit 2; fi
    eval "$cmd"
    ;;
  transform)
    cmd="jq '$FILTER' \"$input\""
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v jq &>/dev/null; then echo "jq not found."; exit 2; fi
    if [[ -n "$OUT" ]]; then
      eval "$cmd" > "$OUT"
      echo "Transformed: $OUT"
    else
      eval "$cmd"
    fi
    ;;
  diff)
    # input is file1, next positional is file2
    file2="${3:-}"
    if [[ -z "$file2" ]]; then echo "diff requires two files"; exit 1; fi
    cmd="diff <(jq -S '.' \"$input\") <(jq -S '.' \"$file2\")"
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v jq &>/dev/null; then echo "jq not found."; exit 2; fi
    eval "$cmd" || true
    ;;
  *)
    echo "Unknown subcommand: $subcmd"
    exit 1
    ;;
esac
