#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms=${1:-"$root/cmd/BonfyreCMS/bonfyre-cms"}
schema="$root/cmd/BonfyreCMS/content-types/site_fragment_migration.json"
schemas="$root/cmd/BonfyreCMS/content-types"
[ -x "$cms" ] || { echo "jsonl import: SKIP (bonfyre-cms not built)"; exit 0; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
db="$tmp/cms.db"
good="$tmp/good.jsonl"
bad="$tmp/bad.jsonl"

cat >"$good" <<'JSONL'
{"candidate_key":"candidate-1","source_ref":"fixture.c:10:4","source_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_class":"c_literal","extracted_text":"First authored candidate","stage":"discovered","compiled_fallback":true,"stale_source":false}
{"candidate_key":"candidate-2","source_ref":"fixture.c:20:4","source_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_class":"c_literal","extracted_text":"Second authored candidate","stage":"discovered","compiled_fallback":true,"stale_source":false}
{"candidate_key":"candidate-3","source_ref":"fixture.c:30:4","source_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_class":"c_literal","extracted_text":"Third authored candidate","stage":"discovered","compiled_fallback":true,"stale_source":false}
JSONL

"$cms" schema apply "$schema" --db "$db" --schemas "$schemas" >/dev/null
result=$("$cms" entry import-jsonl site_fragment_migration "$good" --ns aurekai --db "$db" --schemas "$schemas")
printf '%s\n' "$result" | grep -q '"imported":3'
printf '%s\n' "$result" | grep -q '"families":1'
printf '%s\n' "$result" | grep -q '"ann_indexed":3'

[ "$(sqlite3 "$db" "select count(*) from site_fragment_migration where namespace='aurekai';")" = 3 ]
[ "$(sqlite3 "$db" "select count(*) from _family_members where target_type='site_fragment_migration';")" = 3 ]
[ "$(sqlite3 "$db" "select count(distinct target_id) from _tensor_cells where target_type='site_fragment_migration';")" = 3 ]

stats=$("$cms" ann stats site_fragment_migration --db "$db" --schemas "$schemas")
printf '%s\n' "$stats" | grep -q '"indexed_members":3'
printf '%s\n' "$stats" | grep -q '"quantized_members":3'
bench=$("$cms" ann qbench site_fragment_migration 3 3 --db "$db" --schemas "$schemas")
printf '%s\n' "$bench" | grep -q '"exact_self_recall_pct":100.0'
printf '%s\n' "$bench" | grep -q '"quant_self_recall_pct":100.0'

if "$cms" entry import-jsonl site_fragment_migration "$good" --ns aurekai --db "$db" --schemas "$schemas" >/dev/null 2>&1; then
    echo "jsonl import: duplicate generation unexpectedly succeeded" >&2
    exit 1
fi
[ "$(sqlite3 "$db" "select count(*) from site_fragment_migration;")" = 3 ]
[ "$(sqlite3 "$db" "select count(distinct target_id) from _tensor_cells where target_type='site_fragment_migration';")" = 3 ]

cat >"$bad" <<'JSONL'
{"candidate_key":"candidate-4","source_ref":"fixture.c:40:4","source_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","source_class":"c_literal","extracted_text":"Would be valid alone","stage":"discovered","compiled_fallback":true}
{"candidate_key":"candidate-5","source_ref":"fixture.c:50:4","source_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","source_class":"c_literal","stage":"discovered","compiled_fallback":true}
JSONL

if "$cms" entry import-jsonl site_fragment_migration "$bad" --ns aurekai --db "$db" --schemas "$schemas" >/dev/null 2>&1; then
    echo "jsonl import: malformed generation unexpectedly succeeded" >&2
    exit 1
fi
[ "$(sqlite3 "$db" "select count(*) from site_fragment_migration;")" = 3 ]
[ "$(sqlite3 "$db" "select count(*) from site_fragment_migration where candidate_key='candidate-4';")" = 0 ]

echo "jsonl import: PASS"
