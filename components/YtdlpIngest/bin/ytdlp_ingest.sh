#!/usr/bin/env bash
set -euo pipefail
# YtdlpIngest — media extraction and metadata mining via yt-dlp
# Usage: ytdlp_ingest.sh <subcommand> <url> [--out DIR] [--dry-run]
#   subcommands: extract-audio | extract-meta | download | list-formats

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DRY_RUN=false
for arg in "$@"; do [[ "$arg" == "--dry-run" ]] && DRY_RUN=true; done

subcmd="${1:-}"
url="${2:-}"

if [[ -z "$subcmd" || -z "$url" ]]; then
  echo "Usage: ytdlp_ingest.sh <extract-audio|extract-meta|download|list-formats> <url> [--out DIR] [--dry-run]"
  exit 1
fi

OUT="."
prev=""
for i in "${@:3}"; do
  case "$prev" in --out) OUT="$i";; esac
  prev="$i"
done

case "$subcmd" in
  extract-audio)
    cmd="yt-dlp -x --audio-format wav --audio-quality 0 -o \"$OUT/%(title)s.%(ext)s\" \"$url\""
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v yt-dlp &>/dev/null; then echo "yt-dlp not found. Install: brew install yt-dlp"; exit 2; fi
    eval "$cmd"
    ;;
  extract-meta)
    cmd="yt-dlp --dump-json --no-download \"$url\""
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v yt-dlp &>/dev/null; then echo "yt-dlp not found."; exit 2; fi
    eval "$cmd" | python3 -c "
import sys, json
d = json.load(sys.stdin)
out = {k: d.get(k) for k in ['title','duration','upload_date','uploader','description','view_count','like_count','categories','tags']}
json.dump(out, sys.stdout, indent=2, ensure_ascii=False)
print()
"
    ;;
  download)
    cmd="yt-dlp -o \"$OUT/%(title)s.%(ext)s\" \"$url\""
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v yt-dlp &>/dev/null; then echo "yt-dlp not found."; exit 2; fi
    eval "$cmd"
    ;;
  list-formats)
    cmd="yt-dlp -F \"$url\""
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v yt-dlp &>/dev/null; then echo "yt-dlp not found."; exit 2; fi
    eval "$cmd"
    ;;
  *)
    echo "Unknown subcommand: $subcmd"
    exit 1
    ;;
esac
