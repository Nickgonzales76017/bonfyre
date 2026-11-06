#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms=${1:-"$root/cmd/BonfyreCMS/bonfyre-cms"}
[ -x "$cms" ] || { echo "CMS FactDelta: SKIP (bonfyre-cms not built)"; exit 0; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
schemas="$tmp/schemas"
mkdir -p "$schemas" "$tmp/deltas"
cp "$root/cmd/BonfyreCMS/content-types/publication_occurrence.json" "$schemas/"

db="$tmp/cms.db"
"$cms" schema migrate --db "$db" --schemas "$schemas" >/dev/null

published='{"occurrence_id":"pub-delta-proof","source_twin":"proof-twin","source_ref":"proof-rss","canonical_href":"https://example.test/proof","title":"A retained foreign assertion","observed_at":"2026-09-11T12:00:00Z","receipt_ref":"proof-twin","rights_basis":"retained-public","retention":"retained","visibility":"public","assertion_state":"OBSERVED","cannot_infer":"foreign publication is not an Aurekai claim","status":"published"}'
result=$(BONFYRE_FACT_DELTAS="$tmp/deltas" "$cms" entry create publication_occurrence "$published" --ns root --db "$db" --schemas "$schemas")
printf '%s' "$result" | grep -q '"fact_delta":"DELIVERED"'
[ "$(find "$tmp/deltas" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
delta=$(find "$tmp/deltas" -name '*.json' | head -n 1)
grep -q '"schema":"bonfyre-fact-delta.v1"' "$delta"
grep -q '"cms:publication_occurrence"' "$delta"
grep -q '"cms:publication_occurrence:pub-delta-proof"' "$delta"
grep -q '"cms:publication_source:proof-rss"' "$delta"
grep -q '"cms:internet_source:proof-rss"' "$delta"

# Publishing is not create-only. A post-commit edit and deletion retain the
# same exact owner/source coordinates, so existing derived closures can retire
# even though the canonical row has changed or no longer exists.
update=$(BONFYRE_FACT_DELTAS="$tmp/deltas" "$cms" entry update publication_occurrence 1 '{"title":"A changed retained foreign assertion"}' --ns root --db "$db" --schemas "$schemas")
printf '%s' "$update" | grep -q '"fact_delta":"DELIVERED"'
[ "$(find "$tmp/deltas" -name '*.json' | wc -l | tr -d ' ')" = 2 ]
grep -rl '"cms:publication_occurrence:pub-delta-proof"' "$tmp/deltas" | grep -q .

# Draft authoring is canonical CMS state but has no published projection cut.
draft=$(printf '%s' "$published" | sed 's/"published"}/"draft"}/')
draft_result=$(BONFYRE_FACT_DELTAS="$tmp/deltas" "$cms" entry create publication_occurrence "$draft" --ns other --db "$db" --schemas "$schemas")
printf '%s' "$draft_result" | grep -q '"fact_delta":"NOT_PUBLISHED"'
[ "$(find "$tmp/deltas" -name '*.json' | wc -l | tr -d ' ')" = 2 ]

# A configured but unwritable target never pretends delivery succeeded. The
# owner write stays visible and the caller gets an explicit repair signal.
if BONFYRE_FACT_DELTAS="$tmp/missing" "$cms" entry create publication_occurrence \
  '{"occurrence_id":"pub-delta-failure","source_twin":"proof-twin","source_ref":"proof-rss","title":"Delta delivery failure","observed_at":"2026-09-11T12:01:00Z","receipt_ref":"proof-twin","rights_basis":"retained-public","retention":"retained","visibility":"public","assertion_state":"OBSERVED","cannot_infer":"foreign publication is not an Aurekai claim","status":"published"}' \
  --ns failure --db "$db" --schemas "$schemas" >"$tmp/failure.json" 2>/dev/null; then
  echo 'CMS FactDelta: configured delivery failure unexpectedly succeeded' >&2
  exit 1
fi
grep -q '"created":true' "$tmp/failure.json"
grep -q '"fact_delta":"DELIVERY_FAILED"' "$tmp/failure.json"
[ "$(sqlite3 "$db" "SELECT count(*) FROM publication_occurrence WHERE occurrence_id='pub-delta-failure';")" = 1 ]

# Retiring a published row is itself a public projection change. The old
# coordinates are retained long enough to invalidate derived closures; deleting
# the already-unpublished row adds no second public cut.
retire=$(BONFYRE_FACT_DELTAS="$tmp/deltas" "$cms" entry update publication_occurrence 1 '{"status":"draft"}' --ns root --db "$db" --schemas "$schemas")
printf '%s' "$retire" | grep -q '"fact_delta":"RETIRED"'
[ "$(find "$tmp/deltas" -name '*.json' | wc -l | tr -d ' ')" = 3 ]
delete=$(BONFYRE_FACT_DELTAS="$tmp/deltas" "$cms" entry delete publication_occurrence 1 --db "$db" --schemas "$schemas")
printf '%s' "$delete" | grep -q '"fact_delta":"NOT_PUBLISHED"'
[ "$(find "$tmp/deltas" -name '*.json' | wc -l | tr -d ' ')" = 3 ]
grep -rl '"cms:publication_source:proof-rss"' "$tmp/deltas" | grep -q .

# The native CMS index result must be understood by the Bun consumer, and the
# recorded deltas only advance after that actual rebuild succeeds.
consumer="$root/scripts/apply_fact_deltas.mjs"
applied=$(bun "$consumer" --deltas "$tmp/deltas" --cms "$cms" --db "$db" --json)
printf '%s' "$applied" | grep -q '"applied": 3'
[ "$(find "$tmp/deltas/.applied" -name '*.json' | wc -l | tr -d ' ')" = 3 ]
# Acknowledgement is content-bound and separate; producer event bytes remain.
[ "$(find "$tmp/deltas" -maxdepth 1 -name '*.json' | wc -l | tr -d ' ')" = 3 ]
pending=$(bun "$consumer" --deltas "$tmp/deltas" --cms "$cms" --db "$db" --dry-run --json)
printf '%s' "$pending" | grep -q '"pending_deltas": 0'

echo "CMS FactDelta: PASS"
