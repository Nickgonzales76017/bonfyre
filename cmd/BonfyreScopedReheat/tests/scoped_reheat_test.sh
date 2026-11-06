#!/bin/sh
set -eu
reheat=${1:?reheat binary required}
demand=${2:?scoped-demand binary required}
frontier=${3:?scoped-frontier binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
address="$root/address.db"
state="$root/frontier.db"
sqlite3 "$address" <<'SQL'
CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT,validity_frontier TEXT,canonical_json TEXT);
CREATE TABLE candidate_advertisement_term(identity TEXT,axis TEXT,term TEXT);
CREATE TABLE candidate_advertisement_dependency(identity TEXT,dependency_ref TEXT);
CREATE TABLE occurrence(id TEXT PRIMARY KEY,source_ref TEXT,primary_url TEXT);
INSERT INTO candidate_advertisement VALUES('cms:pack_activation:deep','pack','CMS:pack_activation:deep','CMS:publication','PackGraph:activation','','{}'),('cms:pack_demand:deep','pack_demand','CMS:pack_demand:deep','CMS:publication','MaterialCompiler:pack_demand','','{}'),('cms:world:deep','world_publication','CMS:world:deep','CMS:publication','SurfaceIR:world','','{}'),('external:old','external_occurrence','ExternalEstate:external:old','ForeignTwin:admission','MaterialCompiler:external','','{}'),('external:new','external_occurrence','ExternalEstate:external:new','ForeignTwin:admission','MaterialCompiler:external','','{}');
INSERT INTO candidate_advertisement_term VALUES('cms:pack_activation:deep','pack','deep-ellum-nightlife'),('cms:pack_activation:deep','subject','world-deep-ellum'),('cms:pack_activation:deep','scope','world-deep-ellum'),('cms:pack_demand:deep','pack','deep-ellum-nightlife'),('cms:pack_demand:deep','demand','internet'),('cms:pack_demand:deep','requirement','stage-times'),('cms:pack_demand:deep','source','old-feed'),('cms:pack_demand:deep','regime','remote'),('cms:world:deep','subject','world-deep-ellum');
INSERT INTO candidate_advertisement_dependency VALUES('cms:pack_activation:deep','cms:pack_head:deep-ellum-nightlife'),('cms:pack_demand:deep','cms:pack_head:deep-ellum-nightlife'),('cms:world:deep','cms:internet_source:old-feed'),('cms:world:deep','ForeignTwin:https://example.test/old.rss'),('external:old','ExternalEstate:external:old'),('external:old','ForeignTwin:https://example.test/old.rss'),('external:new','ExternalEstate:external:new'),('external:new','ForeignTwin:https://example.test/new.rss');
INSERT INTO occurrence VALUES('external:old','old-feed','https://example.test/old.rss'),('external:new','new-feed','https://example.test/new.rss');
SQL

"$demand" resolve --db "$address" --subject world:deep-ellum --scope world:deep-ellum > "$root/initial.json"
"$frontier" track --db "$state" --address-db "$address" --closure "$root/initial.json" > "$root/track.json"
grep -q '"identity":"external:old"' "$root/initial.json"
[ "$(sqlite3 "$state" "SELECT count(*) FROM closure_dependency WHERE dependency_ref='ForeignTwin:https://example.test/old.rss';")" -gt 0 ]

sqlite3 "$address" <<'SQL'
UPDATE candidate_advertisement_term SET term='new-feed' WHERE identity='cms:pack_demand:deep' AND axis='source';
DELETE FROM candidate_advertisement_dependency WHERE identity='cms:world:deep' AND dependency_ref IN ('cms:internet_source:old-feed','ForeignTwin:https://example.test/old.rss');
INSERT INTO candidate_advertisement_dependency VALUES('cms:world:deep','cms:internet_source:new-feed'),('cms:world:deep','ForeignTwin:https://example.test/new.rss');
SQL

cat > "$root/source-delta.json" <<'JSON'
{"schema":"bonfyre-fact-delta.v1","delta_id":"old-source-changed","writes":["ForeignTwin:https://example.test/old.rss"]}
JSON
"$reheat" reheat --frontier-db "$state" --address-db "$address" --delta "$root/source-delta.json" --scoped-demand "$demand" --scoped-frontier "$frontier" --handoff-dir "$root/handoffs" --closure-dir "$root/closures" > "$root/reheat-source.json"
grep -q '"affected_closures":1' "$root/reheat-source.json"
grep -q '"processed":1' "$root/reheat-source.json"
closure=$(find "$root/closures" -name 'closure-*.json' | head -1)
grep -q '"identity":"external:new"' "$closure"
! grep -q '"identity":"external:old"' "$closure"
[ "$(find "$root/handoffs/MaterialCompiler-external" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
handoff=$(find "$root/handoffs/MaterialCompiler-external" -name '*.json' | head -1)
grep -q '"identity":"external:new"' "$handoff"
"$frontier" affected --db "$state" --write 'ForeignTwin:https://example.test/old.rss' | grep -q '"affected_count":0'
"$frontier" affected --db "$state" --write 'ForeignTwin:https://example.test/new.rss' | grep -q '"affected_count":1'
[ "$(sqlite3 "$state" "SELECT count(*) FROM scoped_reheat_receipt WHERE delta_id='old-source-changed';")" = 1 ]

cat > "$root/head-delta.json" <<'JSON'
{"schema":"bonfyre-fact-delta.v1","delta_id":"pack-head-changed","writes":["cms:pack_head:deep-ellum-nightlife"]}
JSON
"$reheat" reheat --frontier-db "$state" --address-db "$address" --delta "$root/head-delta.json" --scoped-demand "$demand" --scoped-frontier "$frontier" --handoff-dir "$root/handoffs" --closure-dir "$root/closures" > "$root/reheat-head.json"
grep -q '"processed":1' "$root/reheat-head.json"
"$reheat" reheat --frontier-db "$state" --address-db "$address" --delta "$root/head-delta.json" --scoped-demand "$demand" --scoped-frontier "$frontier" --handoff-dir "$root/handoffs" --closure-dir "$root/closures" > "$root/reheat-head-retry.json"
grep -q '"affected_closures":1' "$root/reheat-head-retry.json"
grep -q '"processed":0' "$root/reheat-head-retry.json"
grep -q '"already_processed":1' "$root/reheat-head-retry.json"
[ "$(sqlite3 "$state" 'SELECT count(*) FROM scoped_reheat_receipt;')" = 2 ]

cat > "$root/miss-delta.json" <<'JSON'
{"schema":"bonfyre-fact-delta.v1","delta_id":"unrelated","writes":["ForeignTwin:https://example.test/unrelated"]}
JSON
"$reheat" reheat --frontier-db "$state" --address-db "$address" --delta "$root/miss-delta.json" --scoped-demand "$demand" --scoped-frontier "$frontier" --handoff-dir "$root/handoffs" --closure-dir "$root/closures" > "$root/reheat-miss.json"
grep -q '"affected_closures":0' "$root/reheat-miss.json"
grep -q '"processed":0' "$root/reheat-miss.json"
echo 'FactDelta -> scoped reheat -> current declaration -> replacement frontier: PASS'
