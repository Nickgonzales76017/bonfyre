#!/bin/sh
# Promote the 90 canonical journal posts (person-nick-work) and archive the
# 77 duplicate copies (aurekai-journal) in the live CMS store, through the CMS
# owner so every change is receipted. Backs the store up first.
#   sh scripts/release/journal-promote-20261112.sh
set -eu
DB=${BONFYRE_CMS_DB:-$HOME/.bonfyre/estate-local/cms.db}
CMS=${BONFYRE_CMS_BIN:-$(command -v bonfyre-cms)}
BACKUP=$HOME/.bonfyre/rollback/cms-journal-20261112
mkdir -p "$BACKUP"
sqlite3 "$DB" ".backup '$BACKUP/cms.db'"
echo "backup: $BACKUP/cms.db"
export BONFYRE_CMS_DB="$DB"
export BONFYRE_CMS_SCHEMAS="${BONFYRE_CMS_SCHEMAS:-$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)/cmd/BonfyreCMS/content-types}"
# Correct the four source-pinned provenance mismatches before any publication.
# Each Git blob and SHA-256 was independently verified against the canonical
# GitHub editorial source; 11 fragments must still have these exact identities.
# Two passes prevent touching even one fragment when another source drifted.
PINS='the-spreadsheet-import-that-was-not-just-data|d873f1ab3d271464f9a4f039cbd5106a78a15618|bd742865ef5e2b7513d776f3b0a8cb690000ba0bd00fce5c4d00949099235e30|7d4c652d94ff8b35c64b1940a557e64475975ad093f4b7349d4c9e7d690bbf78|3
the-spreadsheet-that-became-a-business|f2706115a0f903584821154a9dc2cd311c021ee2|9c53d53c6f2a903ac1de935588449cc98249540e77905bc75de5036042f8f636|f27fe033f6aa5d63b8f9780d0998044bc79065253c78c9a1390c56b85a6fa3db|2
the-train-arrived-after-midnight-yesterday|9e09330296d277da9973dbb2872f8db168703d0b|2a6b423b3931ce6b49c6ff62f1a819ffe7bf93b9eb48fc43ad10c7de376e5d7d|6ed817dd2efa686526816d0b7062536a10e47e3f2f1f91ff94d98f7522892e87|3
the-world-is-not-one-embedding|35a3dac7ce86a6cfd34d12747845b463bb6a60d4|557b63c35abdb4e921892488fd23badef26f45323a86d9ecc75182b498c78afd|dabc2faa021e0d4df579dbb67064b068537e332552229bb618ec064ae8d0fa97|3'
source_rows(){
 sqlite3 "$DB" "SELECT id FROM site_fragment WHERE namespace='person-nick-work' AND
 page='/blog/$1/' AND source_ref='github:Nickgonzales76017/bonfyre:editorial/posts/$1.md@$2' AND
 json_extract(semantic_tags,'$.authoring_v1.metadata.source_blob_sha')='$2' AND
 source_digest IN ('$3','$4') ORDER BY id;"
}
printf '%s\n' "$PINS" | while IFS='|' read -r slug blob old current expected; do
 [ -n "$slug" ] || continue
 actual=$(source_rows "$slug" "$blob" "$old" "$current" | awk 'NF{n++}END{print n+0}')
 [ "$actual" -eq "$expected" ] || { echo "Source drift: $slug ($actual of $expected)" >&2; exit 1; }
done
printf '%s\n' "$PINS" | while IFS='|' read -r slug blob old current expected; do
 [ -n "$slug" ] || continue
 ids=$(sqlite3 "$DB" "SELECT id FROM site_fragment WHERE namespace='person-nick-work' AND
 page='/blog/$slug/' AND source_ref='github:Nickgonzales76017/bonfyre:editorial/posts/$slug.md@$blob' AND
 json_extract(semantic_tags,'$.authoring_v1.metadata.source_blob_sha')='$blob' AND source_digest='$old';")
 for id in $ids; do
   "$CMS" entry update site_fragment "$id" "{\"source_digest\":\"$current\"}" --ns person-nick-work --db "$DB" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null
 done
 now=$(sqlite3 "$DB" "SELECT count(*) FROM site_fragment WHERE namespace='person-nick-work' AND page='/blog/$slug/' AND source_digest='$current' AND source_ref='github:Nickgonzales76017/bonfyre:editorial/posts/$slug.md@$blob';")
 [ "$now" -eq "$expected" ] || { echo "Provenance reconciliation refused: $slug" >&2; exit 1; }
done
echo 'source-pinned journal provenance reconciled: 11 fragments'
n=0
for id in $(sqlite3 "$DB" "select id from site_page where namespace='aurekai-journal' and route like '/blog/%' and status<>'archived'"); do
  "$CMS" entry update site_page "$id" '{"status":"archived"}' --ns aurekai-journal >/dev/null; n=$((n+1)); done
echo "archived duplicate pages: $n"
n=0
for id in $(sqlite3 "$DB" "select id from site_fragment where namespace='person-nick-work' and page like '/blog/%' and status='draft'"); do
  "$CMS" entry update site_fragment "$id" '{"status":"published"}' --ns person-nick-work >/dev/null; n=$((n+1)); done
echo "published sections: $n"
n=0
for id in $(sqlite3 "$DB" "select id from site_page where namespace='person-nick-work' and route like '/blog/%' and status='draft'"); do
  "$CMS" entry update site_page "$id" '{"status":"published"}' --ns person-nick-work >/dev/null; n=$((n+1)); done
echo "published posts: $n"
