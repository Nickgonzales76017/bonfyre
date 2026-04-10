#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SEED="$SCRIPT_DIR/seed_app_from_youtube.sh"

run_seed() {
  local url="$1" slug="$2" item_id="$3"
  local proof_dir="$HOME/Projects/pages-$slug/site/demos/$slug/proofs/$item_id"
  echo ">>> Processing: $slug / $item_id"
  bash "$SEED" "$url" "$slug" "$item_id" "$proof_dir" || echo "  !! FAILED: $slug / $item_id"
}

# memory-atlas: personal/family recordings
run_seed "https://www.youtube.com/watch?v=uKbmDjJWHnA" "memory-atlas" "oral-history-recording-tips"
run_seed "https://www.youtube.com/watch?v=hw_GhqtKfqw" "memory-atlas" "recording-family-history"

# customer-voice: customer experience
run_seed "https://www.youtube.com/watch?v=7dQYLzW-7yU" "customer-voice" "secret-to-great-service"
run_seed "https://www.youtube.com/watch?v=ACn7H3yfA2o" "customer-voice" "elevate-customer-experience"

# town-box: city council / civic meetings
run_seed "https://www.youtube.com/watch?v=DjqL_7Lk0wI" "town-box" "city-council-recap-hendersonville"
run_seed "https://www.youtube.com/watch?v=8MSiGYGJ0kM" "town-box" "city-council-recap-march-2020"

# podcast-plant: podcast/content repurposing
run_seed "https://www.youtube.com/watch?v=qmh-bfEjIGw" "podcast-plant" "maximize-content-repurposing"

# oss-cockpit: open source maintainer
run_seed "https://www.youtube.com/watch?v=3_9LGSex1JY" "oss-cockpit" "interview-oss-maintainer"
run_seed "https://www.youtube.com/watch?v=yO96yqpcycY" "oss-cockpit" "become-oss-maintainer"

# release-radio: release notes / changelogs
run_seed "https://www.youtube.com/watch?v=ysVRqIgoWsI" "release-radio" "smart-release-notes-manager"
run_seed "https://www.youtube.com/watch?v=pQ6-TlhsFMQ" "release-radio" "changelogs-vs-release-notes"

echo ""
echo "=== All 6 apps seeded ==="
