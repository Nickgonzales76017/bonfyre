#!/bin/sh
# The evidence contract, and what it refuses.
#
# Before this existed the participation reader ended in `return raw`: any
# document carrying an axis, an owner and a state was counted at face value.
# An owner could have written itself to `settled` with no money involved. The
# rows below are the ones that must be refused, and they are the point of the
# witness -- a schema that only accepts good input proves nothing.
set -eu
bin=${1:?binary required}
bin=$(CDPATH= cd -- "$(dirname -- "$bin")" && pwd -P)/$(basename -- "$bin")
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
cd "$root"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

cue export ./schemas/control -e surfacePresentationCellSet --out json > "$tmp/cells.json"
"$bin" import --db "$tmp/estate.db" \
  --occurrences "$root/estate/generated/external-estate-v4/observed-external-estate.jsonl" \
  --cells "$tmp/cells.json" >/dev/null

"$bin" participation --db "$tmp/estate.db" --out "$tmp/plane.jsonl" 2>"$tmp/summary.json"

# Every row carries both digests and a declared rung. A row without them is not
# evidence, it is an assertion.
bun cmd/BonfyreExternalEstate/tests/participation_contract.mjs --plane "$tmp/plane.jsonl"

# The reader refuses what it cannot check.
refused=0
for bad in \
  '{"schema":"bonfyre-owner-participation.v1","occurrence_id":"external:ext-1","axis":"economic","state":"settled","owner":"X","from_owner":"A","to_owner":"B","basis_sha256":"00","evidence_sha256":"00","produced_by":"x","recorded_at":"t"}' \
  '{"schema":"bonfyre-owner-participation.v1","occurrence_id":"external:ext-1","axis":"capability","state":"not_a_rung","owner":"X","from_owner":"A","to_owner":"B","basis_sha256":"'"$(printf '0%.0s' $(seq 64))"'","evidence_sha256":"'"$(printf '0%.0s' $(seq 64))"'","produced_by":"x","recorded_at":"t"}' \
  '{"axis":"economic","owner":"X","state":"settled","occurrence_id":"external:ext-1"}' ; do
  printf '%s\n' "$bad" > "$tmp/bad.jsonl"
  # The row is read but cannot become a measured economic/capability rung.
  bun cmd/BonfyreExperienceMap/tools/participation_report.mjs --evidence "$tmp/bad.jsonl" > "$tmp/bad-report.json"
  bun cmd/BonfyreExternalEstate/tests/participation_contract.mjs --reject "$tmp/bad-report.json" \
    || { echo "LAUNDERED: $bad" >&2; exit 1; }
  refused=$((refused + 1))
done

echo "participation contract: PASS ($refused unverifiable rows refused)"
