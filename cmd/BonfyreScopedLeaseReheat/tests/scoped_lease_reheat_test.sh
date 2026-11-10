#!/bin/sh
set -eu
reheat=${1:?lease reheat binary required}
demand=${2:?scoped demand binary required}
frontier=${3:?scoped frontier binary required}
lease=${4:?material lease binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
address="$root/address.db"
state="$root/frontier.db"
leases="$root/leases.db"
mkdir "$root/initial-acquire" "$root/initial-cancel" "$root/acquire" "$root/cancel" "$root/closures"
sqlite3 "$address" <<'SQL'
CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT,validity_frontier TEXT,canonical_json TEXT);
CREATE TABLE candidate_advertisement_term(identity TEXT,axis TEXT,term TEXT);
CREATE TABLE candidate_advertisement_dependency(identity TEXT,dependency_ref TEXT);
CREATE TABLE occurrence(id TEXT PRIMARY KEY,source_ref TEXT,primary_url TEXT);
INSERT INTO candidate_advertisement VALUES
('cms:pack_demand:shared','pack_demand','CMS:pack_demand:shared','CMS:publication','MaterialCompiler:pack_demand','','{}'),
('external:old','external_occurrence','ExternalEstate:external:old','ForeignTwin:admission','MaterialCompiler:external','','{}'),
('external:new','external_occurrence','ExternalEstate:external:new','ForeignTwin:admission','MaterialCompiler:external','','{}');
INSERT INTO candidate_advertisement_term VALUES
('cms:pack_demand:shared','pack','shared-pack'),
('cms:pack_demand:shared','demand','internet'),
('cms:pack_demand:shared','requirement','live-state'),
('cms:pack_demand:shared','source','old-feed'),
('cms:pack_demand:shared','regime','remote');
INSERT INTO candidate_advertisement_dependency VALUES
('cms:pack_demand:shared','cms:pack_head:shared-pack'),
('external:old','ExternalEstate:external:old'),
('external:old','ForeignTwin:https://example.test/old.rss'),
('external:new','ExternalEstate:external:new'),
('external:new','ForeignTwin:https://example.test/new.rss');
INSERT INTO occurrence VALUES
('external:old','old-feed','https://example.test/old.rss'),
('external:new','new-feed','https://example.test/new.rss');
SQL

i=1
while [ "$i" -le 4 ]; do
  sqlite3 "$address" "INSERT INTO candidate_advertisement VALUES('cms:pack_activation:$i','pack','CMS:pack_activation:shared','CMS:publication','PackGraph:activation','','{}'); INSERT INTO candidate_advertisement_term VALUES('cms:pack_activation:$i','pack','shared-pack'),('cms:pack_activation:$i','subject','world-$i'),('cms:pack_activation:$i','scope','world-$i'); INSERT INTO candidate_advertisement_dependency VALUES('cms:pack_activation:$i','cms:pack_head:shared-pack'); INSERT INTO candidate_advertisement VALUES('cms:world:$i','world_publication','CMS:world:$i','CMS:publication','SurfaceIR:world','','{}'); INSERT INTO candidate_advertisement_term VALUES('cms:world:$i','subject','world-$i'); INSERT INTO candidate_advertisement_dependency VALUES('cms:world:$i','ForeignTwin:https://example.test/old.rss');"
  "$demand" resolve --db "$address" --subject "world:$i" --scope "world:$i" > "$root/initial-$i.json"
  "$frontier" track --db "$state" --address-db "$address" --closure "$root/initial-$i.json" > /dev/null
  "$lease" sync --db "$leases" --address-db "$address" --closure "$root/initial-$i.json" > "$root/lease-$i.json"
  i=$((i + 1))
done
[ "$(sqlite3 "$leases" "SELECT count(*) FROM material_lease WHERE identity='external:old';")" = 4 ]
[ "$(sqlite3 "$leases" "SELECT count(*) FROM material_transition WHERE action='acquire' AND identity='external:old';")" = 1 ]
"$lease" drain --db "$leases" --acquire-dir "$root/initial-acquire" --cancel-dir "$root/initial-cancel" > "$root/initial-drain.json"
grep -q '"transitions_emitted":1' "$root/initial-drain.json"
[ "$(find "$root/initial-acquire" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
[ "$(find "$root/initial-cancel" -name '*.json' | wc -l | tr -d ' ')" = 0 ]

cat > "$root/same.json" <<'JSON'
{"schema":"bonfyre-fact-delta.v1","delta_id":"same-source-observed","writes":["ForeignTwin:https://example.test/old.rss"]}
JSON
"$reheat" reheat --frontier-db "$state" --address-db "$address" --lease-db "$leases" --delta "$root/same.json" --scoped-demand "$demand" --scoped-frontier "$frontier" --material-lease "$lease" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" --closure-dir "$root/closures" > "$root/same.out"
grep -q '"affected_closures":4' "$root/same.out"
grep -q '"processed":4' "$root/same.out"
grep -q '"physical_acquires":0' "$root/same.out"
grep -q '"physical_releases":0' "$root/same.out"
grep -q '"transitions_emitted":0' "$root/same.out"
[ "$(find "$root/acquire" -name '*.json' | wc -l | tr -d ' ')" = 0 ]
[ "$(find "$root/cancel" -name '*.json' | wc -l | tr -d ' ')" = 0 ]

sqlite3 "$address" "UPDATE candidate_advertisement_term SET term='new-feed' WHERE identity='cms:pack_demand:shared' AND axis='source'; UPDATE candidate_advertisement_dependency SET dependency_ref='ForeignTwin:https://example.test/new.rss' WHERE identity LIKE 'cms:world:%' AND dependency_ref='ForeignTwin:https://example.test/old.rss';"
cat > "$root/move.json" <<'JSON'
{"schema":"bonfyre-fact-delta.v1","delta_id":"shared-pack-moved","writes":["cms:pack_head:shared-pack"]}
JSON
"$reheat" reheat --frontier-db "$state" --address-db "$address" --lease-db "$leases" --delta "$root/move.json" --scoped-demand "$demand" --scoped-frontier "$frontier" --material-lease "$lease" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" --closure-dir "$root/closures" > "$root/move.out"
grep -q '"affected_closures":4' "$root/move.out"
grep -q '"physical_acquires":1' "$root/move.out"
grep -q '"physical_releases":1' "$root/move.out"
grep -q '"transitions_emitted":2' "$root/move.out"
[ "$(sqlite3 "$leases" "SELECT count(*) FROM material_lease WHERE identity='external:old';")" = 0 ]
[ "$(sqlite3 "$leases" "SELECT count(*) FROM material_lease WHERE identity='external:new';")" = 4 ]
[ "$(find "$root/acquire" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
[ "$(find "$root/cancel" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
"$frontier" affected --db "$state" --write 'ForeignTwin:https://example.test/old.rss' | grep -q '"affected_count":0'
"$frontier" affected --db "$state" --write 'ForeignTwin:https://example.test/new.rss' | grep -q '"affected_count":4'

"$reheat" reheat --frontier-db "$state" --address-db "$address" --lease-db "$leases" --delta "$root/move.json" --scoped-demand "$demand" --scoped-frontier "$frontier" --material-lease "$lease" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" --closure-dir "$root/closures" > "$root/retry.out"
grep -q '"processed":0' "$root/retry.out"
grep -q '"already_processed":4' "$root/retry.out"
grep -q '"transitions_emitted":0' "$root/retry.out"

echo 'shared semantic leases -> one acquire/release physical boundary: PASS'
