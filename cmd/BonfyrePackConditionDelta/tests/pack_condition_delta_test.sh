#!/bin/sh
set -eu
bridge=${1:?condition delta bridge required}
condition=${2:?PackCondition binary required}
demand=${3:?ScopedDemand binary required}
frontier=${4:?ScopedFrontier binary required}
reheat=${5:?ScopedReheat binary required}
outbox=../BonfyrePackCondition/bonfyre-pack-condition-outbox
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
address="$root/address.db"
state="$root/frontier.db"
conditions="$root/conditions.db"
sqlite3 "$address" <<'SQL'
CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT,validity_frontier TEXT,canonical_json TEXT);
CREATE TABLE candidate_advertisement_term(identity TEXT,axis TEXT,term TEXT);
CREATE TABLE candidate_advertisement_dependency(identity TEXT,dependency_ref TEXT);
CREATE TABLE occurrence(id TEXT PRIMARY KEY,source_ref TEXT,primary_url TEXT);
INSERT INTO candidate_advertisement VALUES('cms:pack_activation:deep','pack','CMS:pack_activation:deep','CMS:publication','PackGraph:activation','','{}'),('cms:pack_demand:deep','pack_demand','CMS:pack_demand:deep','CMS:publication','MaterialCompiler:pack_demand','','{}'),('external:deep','external_occurrence','ExternalEstate:external:deep','ForeignTwin:admission','MaterialCompiler:external','','{}');
INSERT INTO candidate_advertisement_term VALUES('cms:pack_activation:deep','pack','deep-ellum-nightlife'),('cms:pack_activation:deep','subject','world-deep-ellum'),('cms:pack_activation:deep','scope','world-deep-ellum'),('cms:pack_activation:deep','condition','venue-open'),('cms:pack_demand:deep','pack','deep-ellum-nightlife'),('cms:pack_demand:deep','demand','internet'),('cms:pack_demand:deep','requirement','stage-times'),('cms:pack_demand:deep','source','deep-feed'),('cms:pack_demand:deep','regime','remote');
INSERT INTO candidate_advertisement_dependency VALUES('cms:pack_activation:deep','cms:pack_head:deep-ellum-nightlife'),('cms:pack_demand:deep','cms:pack_head:deep-ellum-nightlife'),('external:deep','ExternalEstate:external:deep'),('external:deep','ForeignTwin:https://example.test/deep.rss');
INSERT INTO occurrence VALUES('external:deep','deep-feed','https://example.test/deep.rss');
SQL

"$condition" init --db "$conditions" >/dev/null
"$condition" assert --db "$conditions" --condition venue-open --subject world:deep-ellum --scope world:deep-ellum --state satisfied --generation 1 --basis evidence:door:1 >/dev/null
mkdir "$root/events1" "$root/facts1"
"$outbox" --db "$conditions" --dir "$root/events1" >/dev/null
"$bridge" translate --input "$root/events1" --out "$root/facts1" > "$root/bridge1.json"
grep -q '"fact_deltas_projected":1' "$root/bridge1.json"
fact1=$(find "$root/facts1" -name '*.json' | head -1)
grep -q '"schema":"bonfyre-fact-delta.v1"' "$fact1"
grep -q 'PackCondition:venue-open:world-deep-ellum:world-deep-ellum' "$fact1"

"$demand" resolve --db "$address" --condition-db "$conditions" --subject world:deep-ellum --scope world:deep-ellum > "$root/initial.json"
grep -q '"active_packs":\[{"pack":"deep-ellum-nightlife"' "$root/initial.json"
"$frontier" track --db "$state" --address-db "$address" --closure "$root/initial.json" >/dev/null
"$frontier" affected --db "$state" --write 'PackCondition:venue-open:world-deep-ellum:world-deep-ellum' | grep -q '"affected_count":1'

"$condition" assert --db "$conditions" --condition venue-open --subject world:deep-ellum --scope world:deep-ellum --state unsatisfied --generation 2 --basis evidence:door:2 >/dev/null
mkdir "$root/events2" "$root/facts2"
"$outbox" --db "$conditions" --dir "$root/events2" >/dev/null
"$bridge" translate --input "$root/events2" --out "$root/facts2" >/dev/null
delta2=$(find "$root/facts2" -name '*.json' | head -1)
grep -q '"generation":2' "$delta2"
"$reheat" reheat --frontier-db "$state" --address-db "$address" --condition-db "$conditions" --delta "$delta2" --scoped-demand "$demand" --scoped-frontier "$frontier" --handoff-dir "$root/handoffs" --closure-dir "$root/closures" > "$root/off.json"
grep -q '"affected_closures":1' "$root/off.json"
grep -q '"processed":1' "$root/off.json"
closure2=$(sqlite3 "$state" "SELECT closure_path FROM scoped_reheat_receipt WHERE delta_id='pack-condition:venue-open:world-deep-ellum:world-deep-ellum:2';")
grep -q '"active_packs":\[\]' "$closure2"
grep -q '"blocked_packs":1' "$closure2"
"$frontier" affected --db "$state" --write 'PackCondition:venue-open:world-deep-ellum:world-deep-ellum' | grep -q '"affected_count":1'
[ "$(find "$root/handoffs" -type f | wc -l | tr -d ' ')" = 0 ]

"$condition" assert --db "$conditions" --condition venue-open --subject world:deep-ellum --scope world:deep-ellum --state satisfied --generation 3 --basis evidence:door:3 >/dev/null
mkdir "$root/events3" "$root/facts3"
"$outbox" --db "$conditions" --dir "$root/events3" >/dev/null
"$bridge" translate --input "$root/events3" --out "$root/facts3" >/dev/null
delta3=$(find "$root/facts3" -name '*.json' | head -1)
"$reheat" reheat --frontier-db "$state" --address-db "$address" --condition-db "$conditions" --delta "$delta3" --scoped-demand "$demand" --scoped-frontier "$frontier" --handoff-dir "$root/handoffs" --closure-dir "$root/closures" > "$root/on.json"
grep -q '"processed":1' "$root/on.json"
closure3=$(sqlite3 "$state" "SELECT closure_path FROM scoped_reheat_receipt WHERE delta_id='pack-condition:venue-open:world-deep-ellum:world-deep-ellum:3';")
grep -q '"pack":"deep-ellum-nightlife"' "$closure3"
grep -q '"blocked_packs":0' "$closure3"
[ "$(find "$root/handoffs/MaterialCompiler-external" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
grep -q '"identity":"external:deep"' "$root/handoffs/MaterialCompiler-external"/*.json

"$reheat" reheat --frontier-db "$state" --address-db "$address" --condition-db "$conditions" --delta "$delta3" --scoped-demand "$demand" --scoped-frontier "$frontier" --handoff-dir "$root/handoffs" --closure-dir "$root/closures" > "$root/retry.json"
grep -q '"already_processed":1' "$root/retry.json"
grep -q '"processed":0' "$root/retry.json"
[ "$(sqlite3 "$state" 'SELECT count(*) FROM scoped_reheat_receipt;')" = 2 ]
"$bridge" translate --input "$root/events3" --out "$root/facts3" >/dev/null
[ "$(find "$root/facts3" -name '*.json' | wc -l | tr -d ' ')" = 1 ]

echo 'PackCondition outbox -> FactDelta -> freeze/thaw scoped reheat: PASS'
