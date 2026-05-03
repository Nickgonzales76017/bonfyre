#!/usr/bin/env bash
set -euo pipefail
# PandocFormatter — universal format transformer via pandoc
# Usage: pandoc_format.sh <subcommand> <input> [--out FILE] [--to FORMAT] [--dry-run]
#   subcommands: convert | brief-to-html | brief-to-pdf | brief-to-epub | vtt-render

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DRY_RUN=false
for arg in "$@"; do [[ "$arg" == "--dry-run" ]] && DRY_RUN=true; done

subcmd="${1:-}"
input="${2:-}"

if [[ -z "$subcmd" || -z "$input" ]]; then
  echo "Usage: pandoc_format.sh <convert|brief-to-html|brief-to-pdf|brief-to-epub|vtt-render> <input> [--out FILE] [--to FORMAT] [--dry-run]"
  exit 1
fi

OUT="" TO=""
prev=""
for i in "${@:3}"; do
  case "$prev" in --out) OUT="$i";; --to) TO="$i";; esac
  prev="$i"
done

case "$subcmd" in
  convert)
    TO="${TO:-html}"
    out="${OUT:-${input%.*}.$TO}"
    cmd="pandoc \"$input\" -o \"$out\" -t $TO --standalone"
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v pandoc &>/dev/null; then echo "pandoc not found. Install: brew install pandoc"; exit 2; fi
    eval "$cmd"
    echo "Output: $out"
    ;;
  brief-to-html)
    out="${OUT:-${input%.md}.html}"
    cmd="pandoc \"$input\" -o \"$out\" -t html5 --standalone --metadata title=\"Bonfyre Brief\""
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v pandoc &>/dev/null; then echo "pandoc not found."; exit 2; fi
    eval "$cmd"
    echo "Output: $out"
    ;;
  brief-to-pdf)
    out="${OUT:-${input%.md}.pdf}"
    cmd="pandoc \"$input\" -o \"$out\" --pdf-engine=wkhtmltopdf"
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v pandoc &>/dev/null; then echo "pandoc not found."; exit 2; fi
    eval "$cmd"
    echo "Output: $out"
    ;;
  brief-to-epub)
    out="${OUT:-${input%.md}.epub}"
    cmd="pandoc \"$input\" -o \"$out\" -t epub3 --metadata title=\"Bonfyre Brief\""
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v pandoc &>/dev/null; then echo "pandoc not found."; exit 2; fi
    eval "$cmd"
    echo "Output: $out"
    ;;
  vtt-render)
    out="${OUT:-${input%.vtt}.html}"
    # Convert VTT to simple HTML with timestamps
    cmd="pandoc \"$input\" -o \"$out\" -t html5 --standalone"
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v pandoc &>/dev/null; then echo "pandoc not found."; exit 2; fi
    eval "$cmd"
    echo "Output: $out"
    ;;
  *)
    echo "Unknown subcommand: $subcmd"
    exit 1
    ;;
esac
