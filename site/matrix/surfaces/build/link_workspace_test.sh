#!/bin/sh
# The authored page is a product carrier, not a mock dashboard. Keep the
# live publication path and every non-effect boundary visible in one read.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
page="$root/surfaces/link.html"
test -s "$page"
# Structural contract: the live carrier and publication path stay present, the
# retired browser plan and two-write publisher stay gone, and every inline
# script still parses.
while IFS= read -r token; do
  grep -qF -- "$token" "$page" || { echo "Missing source-workbench contract: $token" >&2; exit 1; }
done <<'TOKENS'
Find · Understand · Distribute · Connect
/foreign/twins
K.foreignEdges()
K.observeForeign
openSourcePublication
/apps/source-publication.mjs
requestId:recovering[1]
source:B(selectedSource).source
subject:selectedSource.subject_id
Review collection + World
source-comparison
source-destinations
catalogue_generation
snapshot_mismatch
Retry next page
Inspect known authorization endpoints
TOKENS
while IFS= read -r old; do
  if grep -qF -- "$old" "$page"; then echo "Retired browser plan or two-write publisher returned: $old" >&2; exit 1; fi
done <<'RETIRED'
localStorage.setItem(key,JSON.stringify(x))
foreign-twin-plan.json
async function cmsDraft(
RETIRED
scripts=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-link-scripts.XXXXXX")
awk -v dir="$scripts" '
  { line = $0
    while (length(line)) {
      if (!inside) { i = match(line, /<script[^>]*>/); if (!i) break; n++; inside = 1; line = substr(line, RSTART + RLENGTH) }
      j = index(line, "</script>")
      if (j) { printf "%s\n", substr(line, 1, j - 1) >> (dir "/" n ".js"); inside = 0; line = substr(line, j + 9) }
      else { print line >> (dir "/" n ".js"); line = "" }
    } }' "$page"
for script in "$scripts"/*.js; do
  [ -e "$script" ] || continue
  grep -q '[^[:space:]]' "$script" || continue
  node --check "$script" >/dev/null 2>&1 || { echo "link.html inline script does not parse: $script" >&2; exit 1; }
done
rm -rf "$scripts"
echo 'ForeignTwin carrier and native publication contract pass.'

# The UI writes existing source-selection and World owners, not a browser
# object. The public World can only compose already-published material from its
# exact named collection; this fixture proves the declared owner shape.
cms="$root/../../cmd/BonfyreCMS/bonfyre-cms"
collection_schema="$root/../../cmd/BonfyreCMS/content-types/source_collection.json"
world_schema="$root/../../cmd/BonfyreCMS/content-types/world.json"
project_schemas="$root/../../cmd/BonfyreCMS/content-types"
if [ -x "$cms" ]; then
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-foreign-world-publication.XXXXXX")
  trap 'rm -rf "$tmp"' EXIT INT TERM
  db="$tmp/cms.db"
  "$cms" schema apply "$collection_schema" --db "$db" --schemas "$project_schemas" >/dev/null
  "$cms" schema apply "$world_schema" --db "$db" --schemas "$project_schemas" >/dev/null
  collection=$("$cms" entry create source_collection --ns foreign-workspace '{"collection_id":"foreign-collection-proof","subject_address":"foreign_twin:proof","source_selector":{"source_refs":["proof-source"]},"material_kind":["PublicationOccurrence"],"refresh_policy":"manual","status":"published"}' --db "$db" --schemas "$project_schemas")
  world=$("$cms" entry create world --ns foreign-workspace '{"world":"foreign-world-proof","title":"Proof World","character":"zine","lede":"A human-authored publication over a source collection.","subject":"foreign_twin:proof","sources":"source_collection:foreign-collection-proof","grammars":"discover","cannot_infer":"public material remains bounded to this named collection","status":"published"}' --db "$db" --schemas "$project_schemas")
  printf '%s' "$collection$world" | grep -q '"created":true'
  [ "$(sqlite3 "$db" "select json_extract(source_selector,'$.source_refs[0]') from source_collection where collection_id='foreign-collection-proof';")" = proof-source ]
  [ "$(sqlite3 "$db" "select sources from world where world='foreign-world-proof';")" = source_collection:foreign-collection-proof ]
  [ "$(sqlite3 "$db" "select count(*) from source_collection where status='published';")" = 1 ]
  [ "$(sqlite3 "$db" "select count(*) from world where status='published';")" = 1 ]
fi

printf '%s\n' 'foreign twin workspace: pass'
