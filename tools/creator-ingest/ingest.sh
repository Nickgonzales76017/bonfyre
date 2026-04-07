#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# Bonfyre Creator Ingest — uses bonfyre-ingest (native binary)
# to pull latest video audio + metadata from YouTube channels.
#
# bonfyre-ingest already handles:
#   - yt-dlp download (--type url)
#   - ffmpeg normalization (mono 16 kHz s16 + loudnorm)
#   - SHA-256 content-addressed dedup
#   - intake-manifest.json, artifact.json, source-metadata.json
#
# This script adds:
#   - Batch iteration over creators.json
#   - Puppeteer-based URL discovery (scrape-channels.js)
#   - Per-creator output directories
#   - Filtering by group/creator name
#
# Usage:
#   ./ingest.sh                          # 1 video per creator (latest)
#   ./ingest.sh --count 3                # 3 videos per creator
#   ./ingest.sh --group "Group 2"        # only matching group
#   ./ingest.sh --creator "Ali Abdaal"   # single creator
#   ./ingest.sh --dry-run                # print commands only
#   ./ingest.sh --skip-scrape            # use cached URLs only
#
# Output:  ./output/<creator-slug>/<video-id>/
#            ├── normalized.wav    (16 kHz mono — Whisper-ready)
#            ├── intake-manifest.json
#            ├── artifact.json
#            ├── source-metadata.json
#            └── url-intake.json
# ──────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CREATORS_JSON="$SCRIPT_DIR/creators.json"
OUTPUT_DIR="$SCRIPT_DIR/output"
COUNT=1
GROUP_FILTER=""
CREATOR_FILTER=""
DRY_RUN=false
SKIP_SCRAPE=false
BONFYRE_INGEST="${BONFYRE_INGEST:-bonfyre-ingest}"

# Prefer Homebrew yt-dlp (old Python 3.9 version gets 403s)
export BONFYRE_YTDLP="${BONFYRE_YTDLP:-/opt/homebrew/bin/yt-dlp}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --count)       COUNT="$2"; shift 2 ;;
    --group)       GROUP_FILTER="$2"; shift 2 ;;
    --creator)     CREATOR_FILTER="$2"; shift 2 ;;
    --dry-run)     DRY_RUN=true; shift ;;
    --skip-scrape) SKIP_SCRAPE=true; shift ;;
    --output)      OUTPUT_DIR="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

# Verify bonfyre-ingest is available
command -v "$BONFYRE_INGEST" >/dev/null 2>&1 || {
  # Try ~/.local/bin fallback
  if [[ -x "$HOME/.local/bin/bonfyre-ingest" ]]; then
    BONFYRE_INGEST="$HOME/.local/bin/bonfyre-ingest"
  else
    echo "ERROR: bonfyre-ingest not found. Run: cd bonfyre-oss && make install"
    exit 1
  fi
}
command -v jq >/dev/null || { echo "ERROR: jq not found. brew install jq"; exit 1; }

mkdir -p "$OUTPUT_DIR"

slugify() { echo "$1" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/-/g' | sed 's/--*/-/g' | sed 's/^-//;s/-$//'; }

# ── Step 1: Discover video URLs via Puppeteer (or use cache) ──
get_video_urls() {
  local handle="$1" slug="$2" count="$3"
  local cache_file="$OUTPUT_DIR/$slug/.video_urls"

  # Use cache if it exists and --skip-scrape
  if $SKIP_SCRAPE && [[ -f "$cache_file" ]]; then
    head -n "$count" "$cache_file"
    return
  fi

  # Try Puppeteer scraper first
  if [[ -f "$SCRIPT_DIR/scrape-channels.js" ]] && command -v node >/dev/null 2>&1; then
    mkdir -p "$OUTPUT_DIR/$slug"
    node "$SCRIPT_DIR/scrape-channels.js" \
      --handle "$handle" \
      --count "$count" \
      --output "$cache_file" 2>/dev/null && {
      head -n "$count" "$cache_file"
      return
    }
  fi

  # Fallback: construct channel /videos URL (bonfyre-ingest + yt-dlp
  # will resolve individual videos from the channel page)
  echo "https://www.youtube.com/$handle/videos"
}

# ── Step 2: Parse creators.json ──
creators=$(jq -r '
  .groups[]
  | .name as $g
  | .creators[]
  | [$g, .name, .yt] | @tsv
' "$CREATORS_JSON")

total=0
skipped=0
ingested=0

while IFS=$'\t' read -r group name handle; do
  # Apply filters
  if [[ -n "$GROUP_FILTER" ]] && [[ "$group" != *"$GROUP_FILTER"* ]]; then continue; fi
  if [[ -n "$CREATOR_FILTER" ]] && [[ "$name" != *"$CREATOR_FILTER"* ]]; then continue; fi

  slug=$(slugify "$name")
  total=$((total + 1))

  echo "━━━ [$total] $name ($handle) — ingesting $COUNT video(s) ━━━"

  if $DRY_RUN; then
    echo "  [dry-run] $BONFYRE_INGEST <url> $OUTPUT_DIR/$slug/<id> --type url"
    continue
  fi

  # Get video URLs (from Puppeteer scraper or fallback)
  urls=$(get_video_urls "$handle" "$slug" "$COUNT")

  vid_num=0
  while IFS= read -r url; do
    [[ -z "$url" ]] && continue
    vid_num=$((vid_num + 1))

    # Extract video ID for directory naming
    vid_id=$(echo "$url" | grep -oP 'v=\K[^&]+' 2>/dev/null || echo "vid-$vid_num")
    out_dir="$OUTPUT_DIR/$slug/$vid_id"

    # Skip if already ingested (SHA-256 dedup in bonfyre-ingest)
    if [[ -f "$out_dir/intake-manifest.json" ]]; then
      echo "  [$vid_num] $vid_id — already ingested, skipping"
      continue
    fi

    mkdir -p "$out_dir"
    echo "  [$vid_num] ingesting $url → $out_dir/"

    # bonfyre-ingest handles: yt-dlp download, ffmpeg normalize,
    # SHA-256 hash, intake-manifest.json, artifact.json, metadata
    "$BONFYRE_INGEST" "$url" "$out_dir" --type url 2>&1 | while IFS= read -r line; do
      echo "    $line"
    done || {
      echo "  WARN: bonfyre-ingest failed for $url"
    }

  done <<< "$urls"

  ingested=$((ingested + 1))
  echo "  OK ($slug)"

done <<< "$creators"

echo ""
echo "━━━ Ingest complete ━━━"
echo "  Creators processed: $total"
echo "  Successfully ingested: $ingested"
echo "  Skipped/failed: $skipped"
echo "  Output: $OUTPUT_DIR/"
echo ""
echo "Next: run-demo-pipeline.sh to process ingested audio through Bonfyre"
