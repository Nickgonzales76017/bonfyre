#!/bin/sh
# Native witness for authored SiteFragment -> tensor -> ANN/qANN.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms=${1:-"$root/cmd/BonfyreCMS/bonfyre-cms"}
schema="$root/cmd/BonfyreCMS/content-types/site_fragment.json"
schemas="$root/cmd/BonfyreCMS/content-types"
[ -x "$cms" ] || { echo "site fragment pipeline: SKIP (bonfyre-cms not built)"; exit 0; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
db="$tmp/cms.db"
"$cms" schema apply "$schema" --db "$db" --schemas "$schemas" >/dev/null

a='{"coordinate":"life:protocols:hero:public:default","page":"protocols","slot":"hero","audience":"public","kind":"copy","position":10,"heading":"One state, many carriers","body":"Protocol surfaces project the same owned state.","status":"published","variant_key":"default","semantic_tags":["protocols","carrier"],"projection_contract":{"html":true,"htmx":true,"mcp":true},"source_ref":"cmd/BonfyreWebAddress/src/main.c:1","source_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","evidence_ref":"receipt:test:published"}'
b='{"coordinate":"life:protocols:hero:public:alt","page":"protocols","slot":"hero","audience":"public","kind":"copy","position":20,"heading":"Draft variant","body":"Drafts are authored but not public.","status":"draft","variant_key":"alt","semantic_tags":["protocols","draft"],"projection_contract":{"html":true,"htmx":true},"source_ref":"cmd/BonfyreWebAddress/src/main.c:2","source_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","evidence_ref":"receipt:test:draft"}'
c='{"coordinate":"shop:home:hero:market:default","page":"home","slot":"hero","audience":"market","kind":"hero","position":10,"heading":"Market from one service book","body":"Offers project from canonical owners.","status":"published","variant_key":"default","semantic_tags":["market","finance"],"projection_contract":{"html":true,"htmx":true,"a2a":true,"mcp":true},"source_ref":"site/matrix/index.html:3","source_digest":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","evidence_ref":"receipt:test:shop"}'

"$cms" entry create site_fragment "$a" --ns aurekai --db "$db" --schemas "$schemas" >/dev/null
"$cms" entry create site_fragment "$b" --ns aurekai --db "$db" --schemas "$schemas" >/dev/null
"$cms" entry create site_fragment "$c" --ns aurekai --db "$db" --schemas "$schemas" >/dev/null

if "$cms" entry create site_fragment "$a" --ns aurekai --db "$db" --schemas "$schemas" >/dev/null 2>&1; then
  echo "site fragment pipeline: duplicate coordinate admitted" >&2; exit 1
fi

[ "$(sqlite3 "$db" "select count(*) from site_fragment where page='protocols' and slot='hero' and audience='public' and status='published';")" = 1 ]
[ "$(sqlite3 "$db" "select count(*) from site_fragment where page='protocols' and slot='hero' and audience='public' and status='draft';")" = 1 ]
[ "$(sqlite3 "$db" "select count(*) from _tensor_cells where target_type='site_fragment';")" -gt 0 ]

build=$("$cms" ann build site_fragment --db "$db" --schemas "$schemas")
printf '%s\n' "$build" | grep -q '"indexed":3'
stats=$("$cms" ann stats site_fragment --db "$db" --schemas "$schemas")
printf '%s\n' "$stats" | grep -q '"indexed_members":3'
printf '%s\n' "$stats" | grep -q '"quantized_members":3'
bench=$("$cms" ann qbench site_fragment 16 3 --db "$db" --schemas "$schemas")
printf '%s\n' "$bench" | grep -q '"exact_self_recall_pct":100.0'
printf '%s\n' "$bench" | grep -q '"quant_self_recall_pct":100.0'

before=$(sqlite3 "$db" "select group_concat(coalesce(val_text,''),'|') from _tensor_cells where target_type='site_fragment' and target_id=1 order by position;")
"$cms" entry update site_fragment 1 '{"body":"One canonical state can project through many lawful carriers."}' --ns aurekai --db "$db" --schemas "$schemas" >/dev/null
after=$(sqlite3 "$db" "select group_concat(coalesce(val_text,''),'|') from _tensor_cells where target_type='site_fragment' and target_id=1 order by position;")
[ "$before" != "$after" ]

"$cms" ann build site_fragment --db "$db" --schemas "$schemas" >/dev/null
bench=$("$cms" ann qbench site_fragment 16 3 --db "$db" --schemas "$schemas")
printf '%s\n' "$bench" | grep -q '"quant_self_recall_pct":100.0'
echo "site fragment pipeline: PASS"
