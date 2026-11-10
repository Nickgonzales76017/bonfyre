#!/bin/sh
set -eu
condition=${1:?PackCondition binary required}
outbox=${2:?PackCondition outbox required}
bridge=${3:?PackCondition delta bridge required}
demand=${4:?ScopedDemand binary required}
frontier=${5:?ScopedFrontier binary required}
lease=${6:?MaterialLease binary required}
reheat=${7:?ScopedLeaseReheat binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
address="$root/address.db"
state="$root/frontier.db"
conditions="$root/conditions.db"
leases="$root/leases.db"
mkdir "$root/events1" "$root/events2" "$root/events3" "$root/facts2" "$root/facts3" \
      "$root/initial-acquire" "$root/initial-cancel" "$root/off-acquire" "$root/off-cancel" \
      "$root/on-acquire" "$root/on-cancel" "$root/closures"
sqlite3 "$address" <<'SQL'
CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT,validity_frontier TEXT,canonical_json TEXT);
CREATE TABLE candidate_advertisement_term(identity TEXT,axis TEXT,term TEXT);
CREATE TABLE candidate_advertisement_dependency(identity TEXT,dependency_ref TEXT);
CREATE TABLE occurrence(id TEXT PRIMARY KEY,source_ref TEXT,primary_url TEXT);
INSERT INTO candidate_advertisement VALUES
('cms:pack_activation:deep','pack','CMS:pack_activation:deep','CMS:publication','PackGraph:activation','','{}'),
('cms:pack_demand:deep','pack_demand','CMS:pack_demand:deep','CMS:publication','MaterialCompiler:pack_demand','','{}'),
('external:deep','external_occurrence','ExternalEstate:external:deep','ForeignTwin:admission','MaterialCompiler:external','','{}');
INSERT INTO candidate_advertisement_term VALUES
('cms:pack_activation:deep','pack','deep-ellum-nightlife'),
('cms:pack_activation:deep','subject','world-deep-ellum'),
('cms:pack_activation:deep','scope','world-deep-ellum'),
('cms:pack_activation:deep','condition','venue-open'),
('cms:pack_demand:deep','pack','deep-ellum-nightlife'),
('cms:pack_demand:deep','demand','internet'),
('cms:pack_demand:deep','requirement','stage-times'),
('cms:pack_demand:deep','source','deep-feed'),
('cms:pack_demand:deep','regime','remote');
INSERT INTO candidate_advertisement_dependency VALUES
('cms:pack_activation:deep','cms:pack_head:deep-ellum-nightlife'),
('cms:pack_demand:deep','cms:pack_head:deep-ellum-nightlife'),
('external:deep','ExternalEstate:external:deep'),
('external:deep','ForeignTwin:https://example.test/deep.rss');
INSERT INTO occurrence VALUES('external:deep','deep-feed','https://example.test/deep.rss');
SQL

"$condition" init --db "$conditions" >/dev/null
"$condition" assert --db "$conditions" --condition venue-open --subject world:deep-ellum --scope world:deep-ellum --state satisfied --generation 1 --basis evidence:door:1 >/dev/null
"$outbox" --db "$conditions" --dir "$root/events1" >/dev/null
"$demand" resolve --db "$address" --condition-db "$conditions" --subject world:deep-ellum --scope world:deep-ellum > "$root/initial.json"
grep -q '"pack":"deep-ellum-nightlife"' "$root/initial.json"
"$frontier" track --db "$state" --address-db "$address" --closure "$root/initial.json" >/dev/null
"$lease" sync --db "$leases" --address-db "$address" --closure "$root/initial.json" > "$root/initial-lease.json"
grep -q '"physical_acquires":1' "$root/initial-lease.json"
"$lease" drain --db "$leases" --acquire-dir "$root/initial-acquire" --cancel-dir "$root/initial-cancel" > "$root/initial-drain.json"
grep -q '"transitions_emitted":1' "$root/initial-drain.json"
[ "$(sqlite3 "$leases" "SELECT count(*) FROM material_lease WHERE identity='external:deep';")" = 1 ]

"$condition" assert --db "$conditions" --condition venue-open --subject world:deep-ellum --scope world:deep-ellum --state unsatisfied --generation 2 --basis evidence:door:2 >/dev/null
"$outbox" --db "$conditions" --dir "$root/events2" >/dev/null
"$bridge" translate --input "$root/events2" --out "$root/facts2" >/dev/null
delta2=$(find "$root/facts2" -name '*.json' | head -1)
grep -q 'PackCondition:venue-open:world-deep-ellum:world-deep-ellum' "$delta2"
"$reheat" reheat --frontier-db "$state" --address-db "$address" --lease-db "$leases" --condition-db "$conditions" --delta "$delta2" --scoped-demand "$demand" --scoped-frontier "$frontier" --material-lease "$lease" --acquire-dir "$root/off-acquire" --cancel-dir "$root/off-cancel" --closure-dir "$root/closures" > "$root/off.json"
grep -q '"affected_closures":1' "$root/off.json"
grep -q '"physical_acquires":0' "$root/off.json"
grep -q '"physical_releases":1' "$root/off.json"
grep -q '"transitions_emitted":1' "$root/off.json"
[ "$(sqlite3 "$leases" "SELECT count(*) FROM material_lease WHERE identity='external:deep';")" = 0 ]
[ "$(find "$root/off-acquire" -name '*.json' | wc -l | tr -d ' ')" = 0 ]
[ "$(find "$root/off-cancel" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
closure2=$(sqlite3 "$state" "SELECT closure_path FROM scoped_lease_reheat_receipt WHERE delta_id='pack-condition:venue-open:world-deep-ellum:world-deep-ellum:2';")
grep -q '"active_packs":\[\]' "$closure2"
grep -q '"blocked_packs":1' "$closure2"
"$frontier" affected --db "$state" --write 'PackCondition:venue-open:world-deep-ellum:world-deep-ellum' | grep -q '"affected_count":1'

"$condition" assert --db "$conditions" --condition venue-open --subject world:deep-ellum --scope world:deep-ellum --state satisfied --generation 3 --basis evidence:door:3 >/dev/null
"$outbox" --db "$conditions" --dir "$root/events3" >/dev/null
"$bridge" translate --input "$root/events3" --out "$root/facts3" >/dev/null
delta3=$(find "$root/facts3" -name '*.json' | head -1)
"$reheat" reheat --frontier-db "$state" --address-db "$address" --lease-db "$leases" --condition-db "$conditions" --delta "$delta3" --scoped-demand "$demand" --scoped-frontier "$frontier" --material-lease "$lease" --acquire-dir "$root/on-acquire" --cancel-dir "$root/on-cancel" --closure-dir "$root/closures" > "$root/on.json"
grep -q '"physical_acquires":1' "$root/on.json"
grep -q '"physical_releases":0' "$root/on.json"
grep -q '"transitions_emitted":1' "$root/on.json"
[ "$(sqlite3 "$leases" "SELECT count(*) FROM material_lease WHERE identity='external:deep';")" = 1 ]
[ "$(find "$root/on-acquire" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
[ "$(find "$root/on-cancel" -name '*.json' | wc -l | tr -d ' ')" = 0 ]
closure3=$(sqlite3 "$state" "SELECT closure_path FROM scoped_lease_reheat_receipt WHERE delta_id='pack-condition:venue-open:world-deep-ellum:world-deep-ellum:3';")
grep -q '"pack":"deep-ellum-nightlife"' "$closure3"

"$reheat" reheat --frontier-db "$state" --address-db "$address" --lease-db "$leases" --condition-db "$conditions" --delta "$delta3" --scoped-demand "$demand" --scoped-frontier "$frontier" --material-lease "$lease" --acquire-dir "$root/on-acquire" --cancel-dir "$root/on-cancel" --closure-dir "$root/closures" > "$root/retry.json"
grep -q '"processed":0' "$root/retry.json"
grep -q '"already_processed":1' "$root/retry.json"
grep -q '"transitions_emitted":0' "$root/retry.json"

echo 'PackCondition freeze/thaw -> reference-counted material lease convergence: PASS'
