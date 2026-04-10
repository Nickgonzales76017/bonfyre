#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/collect_youtube_seeds.sh [results_per_query] [max_per_app]
# Requires: yt-dlp, jq

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
QUERIES_FILE="$ROOT_DIR/site/demos/seed_queries.json"
OUT_DIR="$ROOT_DIR/site/demos"
RESULTS_PER_QUERY=${1:-50}
MAX_PER_APP=${2:-200}

command -v yt-dlp >/dev/null 2>&1 || { echo "yt-dlp not found; install it (pip install yt-dlp or brew install yt-dlp)"; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "jq not found; install it (brew install jq)"; exit 1; }

echo "Reading queries from $QUERIES_FILE"
apps=$(jq -r 'keys[]' "$QUERIES_FILE")

for app in $apps; do
  echo "\nCollecting seeds for: $app"
  mkdir -p "$OUT_DIR/$app"
  OUT_FILE="$OUT_DIR/$app/seeds.json"
  tmpfile=$(mktemp)
  >"$tmpfile"

  jq -r --arg app "$app" '.[$app][]' "$QUERIES_FILE" | while IFS= read -r query; do
    echo " Searching: $query (up to $RESULTS_PER_QUERY)"
    # Use yt-dlp's ytsearch to get IDs without downloading
    yt-dlp "ytsearch${RESULTS_PER_QUERY}:${query}" --get-id --flat-playlist >> "$tmpfile" || true
    sleep 1
  done

  # Deduplicate and trim to max
  awk '!seen[$0]++' "$tmpfile" | head -n "$MAX_PER_APP" | jq -R -s -c 'split("\n")[:-1]' > "$OUT_FILE"
  rm -f "$tmpfile"
  echo " Wrote $OUT_FILE"
done

echo "Done. Review $OUT_DIR/<app>/seeds.json files."
