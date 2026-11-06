#!/bin/sh
set -eu

resolver=${1:?ScopedDemand resolver required}
reheat=${2:?ScopedDemand reheat worker required}
estate=${3:?ExternalEstate binary required}
workers=${4:?LoweringWorkers binary required}

root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
mkdir -p "$root/reheat-inbox" "$root/source-out" "$root/demands"

topic='https://example.test/shared-nightlife.rss'
other='https://example.test/unrelated.rss'

cat > "$root/ads.jsonl" <<JSONL
{"identity":"external:shared","candidate_kind":"external_occurrence","owner_ref":"ExternalEstate:external:shared","exact_verifier":"ForeignTwin:admission","lowerer_ref":"MaterialCompiler:external","validity_frontier":"fixture","terms":["source:nightlife-shared"],"dependencies":["ForeignTwin:$topic"]}
{"identity":"external:other","candidate_kind":"external_occurrence","owner_ref":"ExternalEstate:external:other","exact_verifier":"ForeignTwin:admission","lowerer_ref":"MaterialCompiler:external","validity_frontier":"fixture","terms":["source:other-feed"],"dependencies":["ForeignTwin:$other"]}
{"identity":"cms:pack_activation:nightlife:deep","candidate_kind":"pack","owner_ref":"CMS:pack_activation:nightlife","exact_verifier":"CMS:publication","lowerer_ref":"PackGraph:activation","validity_frontier":"fixture","terms":["pack:nightlife","subject:world-deep-ellum","scope:world-deep-ellum"],"dependencies":["cms:pack_head:nightlife"]}
{"identity":"cms:pack_activation:nightlife:bishop","candidate_kind":"pack","owner_ref":"CMS:pack_activation:nightlife","exact_verifier":"CMS:publication","lowerer_ref":"PackGraph:activation","validity_frontier":"fixture","terms":["pack:nightlife","subject:world-bishop-arts","scope:world-bishop-arts"],"dependencies":["cms:pack_head:nightlife"]}
{"identity":"cms:pack_demand:nightlife:internet:events","candidate_kind":"pack_demand","owner_ref":"CMS:pack_demand:nightlife","exact_verifier":"CMS:publication","lowerer_ref":"MaterialCompiler:pack_demand","validity_frontier":"fixture","terms":["pack:nightlife","demand:internet","requirement:events","source:nightlife-shared","regime:remote"],"dependencies":["cms:pack_head:nightlife"]}
{"identity":"cms:pack_activation:other","candidate_kind":"pack","owner_ref":"CMS:pack_activation:other","exact_verifier":"CMS:publication","lowerer_ref":"PackGraph:activation","validity_frontier":"fixture","terms":["pack:other-pack","subject:world-other","scope:world-other"],"dependencies":["cms:pack_head:other-pack"]}
{"identity":"cms:pack_demand:other:internet:events","candidate_kind":"pack_demand","owner_ref":"CMS:pack_demand:other","exact_verifier":"CMS:publication","lowerer_ref":"MaterialCompiler:pack_demand","validity_frontier":"fixture","terms":["pack:other-pack","demand:internet","requirement:events","source:other-feed","regime:remote"],"dependencies":["cms:pack_head:other-pack"]}
JSONL

"$estate" import-advertisements --db "$root/address.db" --advertisements "$root/ads.jsonl" > "$root/imported.json"
sqlite3 "$root/address.db" "INSERT INTO occurrence(id,observed_name,source_ref,primary_url,source_kind,section,raw_json) VALUES
('external:shared','Shared nightlife feed','nightlife-shared','$topic','rss','nightlife','{}'),
('external:other','Other feed','other-feed','$other','rss','other','{}');"

printf '%s\n' '{"delta_id":"delta:pack-head:nightlife","writes":["cms:pack_head:nightlife"]}' > "$root/pack-delta.json"
"$estate" fact-delta --db "$root/address.db" --delta "$root/pack-delta.json" > "$root/cut.json"
grep -q '"affected_count":3' "$root/cut.json"
grep -q 'cms:pack_activation:nightlife:deep' "$root/cut.json"
grep -q 'cms:pack_activation:nightlife:bishop' "$root/cut.json"
grep -q 'cms:pack_demand:nightlife:internet:events' "$root/cut.json"
! grep -q 'other-pack' "$root/cut.json"

"$estate" drain-lowering-queue --db "$root/address.db" --dir "$root/reheat-inbox" --limit 16 > "$root/queued.json"
grep -q '"lowering_handoffs_dispatched":3' "$root/queued.json"
[ "$(find "$root/reheat-inbox/PackGraph-activation" -name '*.json' | wc -l | tr -d ' ')" = 2 ]
[ "$(find "$root/reheat-inbox/MaterialCompiler-pack_demand" -name '*.json' | wc -l | tr -d ' ')" = 1 ]

"$reheat" --db "$root/address.db" --lowerer-dir "$root/reheat-inbox" --dir "$root/source-out" \
  --resolver "$resolver" --limit 16 > "$root/reheated.json"
grep -q '"handoffs_handled":3' "$root/reheated.json"
grep -q '"closures_recomputed":2' "$root/reheated.json"
[ "$(find "$root/reheat-inbox" -name '*.json.handled' | wc -l | tr -d ' ')" = 3 ]

# Two semantic World closures share one source, so physical work stays one.
[ "$(find "$root/source-out/MaterialCompiler-external" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
grep -q '"identity":"external:shared"' "$root/source-out/MaterialCompiler-external"/*.json
! grep -q 'external:other' "$root/source-out/MaterialCompiler-external"/*.json

"$workers" drain --db "$root/derived.db" --lowerer-dir "$root/source-out" \
  --external-estate "$root/address.db" --demand-dir "$root/demands" --limit 8 > "$root/lowered.json"
grep -q '"handled":1' "$root/lowered.json"
[ "$(sqlite3 "$root/derived.db" "SELECT count(*) FROM material_observation_demand WHERE identity='external:shared' AND topic='$topic';")" = 1 ]
[ "$(sqlite3 "$root/derived.db" "SELECT count(*) FROM material_observation_demand WHERE identity='external:other';")" = 0 ]

"$estate" fact-delta --db "$root/address.db" --delta "$root/pack-delta.json" > "$root/replay.json"
grep -q '"state":"already_processed"' "$root/replay.json"
[ "$(sqlite3 "$root/address.db" "SELECT count(*) FROM candidate_lowering_queue;")" = 3 ]

echo 'Pack head delta -> 2 World closures -> 1 shared source demand: PASS'
