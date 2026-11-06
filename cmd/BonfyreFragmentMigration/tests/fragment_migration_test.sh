#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
bin=${1:-"$root/cmd/BonfyreFragmentMigration/bonfyre-fragment-migration"}
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
schemas="$root/cmd/BonfyreCMS/content-types"
[ -x "$bin" ] || { echo "fragment migration: SKIP (binary not built)"; exit 0; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
cat >"$tmp/literals.json" <<'JSON'
{"schema_version":"bonfyre.surface.literal-candidates.v2","candidates":[
{"candidate_id":"a","function":"route_content","role":"copy","line":10,"source_ref":"web.c:10","source_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","text_nodes":[" Welcome   home ","Same Copy"]},
{"candidate_id":"b","function":"route_content","role":"copy","line":20,"source_ref":"web.c:20","source_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","text_nodes":["Same Copy"]},
{"candidate_id":"c","function":"stage_fragment_api","role":"copy","line":30,"source_ref":"web.c:30","source_digest":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","text_nodes":["Needs review"]},
{"candidate_id":"d","function":"unknown_function","role":"copy","line":40,"source_ref":"web.c:40","source_digest":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","text_nodes":["Unmapped"]}
]}
JSON
cat >"$tmp/map.json" <<'JSON'
{"schema_version":"bonfyre.surface.source-map.v1","mappings":{"route_content":{"page":"home","audience":"public","slot":"content"},"stage_fragment_api":{"page":"stage","audience":"public","slot":"content"}},"review_required":["stage_fragment_api"]}
JSON

"$bin" compile "$tmp/literals.json" --source-map "$tmp/map.json" --out-dir "$tmp/out" >"$tmp/compile.out"
grep -q '"drafts":3' "$tmp/compile.out"
grep -q '"review":2' "$tmp/compile.out"
grep -q '"equivalence_groups":2' "$tmp/compile.out"
grep -q '"duplicate_groups":1' "$tmp/compile.out"
[ "$(wc -l <"$tmp/out/drafts.jsonl" | tr -d ' ')" = 3 ]
[ "$(wc -l <"$tmp/out/review.jsonl" | tr -d ' ')" = 2 ]
grep -q '"body":"Welcome home"' "$tmp/out/drafts.jsonl"
grep -q '"status":"draft"' "$tmp/out/drafts.jsonl"
grep -q '"publication_effect": false' "$tmp/out/manifest.json"

db="$tmp/cms.db"
"$bin" admit "$tmp/out/drafts.jsonl" --db "$db" --schemas "$schemas" --namespace migration-test --cms-bin "$cms" >"$tmp/preview.out"
grep -q '"create":3' "$tmp/preview.out"
grep -q '"skip_existing":0' "$tmp/preview.out"

if [ -x "$cms" ]; then
  "$cms" schema apply "$schemas/site_fragment.json" --db "$db" --schemas "$schemas" >/dev/null
  first=$(sed -n '1p' "$tmp/out/drafts.jsonl")
  existing=$(printf '%s\n' "$first" | sed 's/"body":"Welcome home"/"body":"Authored existing"/')
  "$cms" entry create site_fragment "$existing" --ns migration-test --db "$db" --schemas "$schemas" >/dev/null
  "$bin" admit "$tmp/out/drafts.jsonl" --db "$db" --schemas "$schemas" --namespace migration-test --cms-bin "$cms" --apply >"$tmp/apply.out"
  grep -q '"created":2' "$tmp/apply.out"
  grep -q '"skipped_existing":1' "$tmp/apply.out"
  "$bin" admit "$tmp/out/drafts.jsonl" --db "$db" --schemas "$schemas" --namespace migration-test --cms-bin "$cms" --apply >"$tmp/reapply.out"
  grep -q '"created":0' "$tmp/reapply.out"
  grep -q '"skipped_existing":3' "$tmp/reapply.out"
  "$cms" entry list site_fragment --ns migration-test --status draft --limit 10 --db "$db" --schemas "$schemas" >"$tmp/list.out"
  [ "$(grep -o '"coordinate":"' "$tmp/list.out" | wc -l | tr -d ' ')" = 3 ]
  grep -q '"body":"Authored existing"' "$tmp/list.out"
  ! grep -q '"status":"published"' "$tmp/list.out"
fi

sed 's/"status":"draft"/"status":"published"/' "$tmp/out/drafts.jsonl" >"$tmp/published.jsonl"
if "$bin" admit "$tmp/published.jsonl" --db "$db" --schemas "$schemas" --namespace migration-test --cms-bin "$cms" >/dev/null 2>&1; then
  echo "fragment migration: published candidate unexpectedly admitted" >&2
  exit 1
fi
echo "fragment migration: PASS"
