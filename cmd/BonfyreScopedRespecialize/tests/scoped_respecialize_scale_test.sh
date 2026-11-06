#!/bin/sh
set -eu
respecialize=${1:?respecialize binary required}
demand=${2:?scoped-demand binary required}
frontier=${3:?scoped-frontier binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
address="$root/address.db"
state="$root/frontier.db"
mkdir "$root/handoffs" "$root/closures"
cat > "$root/schema.sql" <<'SQL'
BEGIN;
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
while [ "$i" -le 128 ]; do
  cat >> "$root/schema.sql" <<SQL
INSERT INTO candidate_advertisement VALUES('cms:pack_activation:$i','pack','CMS:pack_activation:shared','CMS:publication','PackGraph:activation','','{}');
INSERT INTO candidate_advertisement_term VALUES
('cms:pack_activation:$i','pack','shared-pack'),
('cms:pack_activation:$i','subject','world-$i'),
('cms:pack_activation:$i','scope','world-$i');
INSERT INTO candidate_advertisement_dependency VALUES
('cms:pack_activation:$i','cms:pack_head:shared-pack');
INSERT INTO candidate_advertisement VALUES('cms:world:$i','world_publication','CMS:world:$i','CMS:publication','SurfaceIR:world','','{}');
INSERT INTO candidate_advertisement_term VALUES('cms:world:$i','subject','world-$i');
INSERT INTO candidate_advertisement_dependency VALUES
('cms:world:$i','ForeignTwin:https://example.test/old.rss');
SQL
  i=$((i + 1))
done
echo 'COMMIT;' >> "$root/schema.sql"
sqlite3 "$address" < "$root/schema.sql"

i=1
while [ "$i" -le 128 ]; do
  "$demand" resolve --db "$address" --subject "world:$i" --scope "world:$i" > "$root/initial-$i.json"
  "$frontier" track --db "$state" --address-db "$address" --closure "$root/initial-$i.json" > /dev/null
  i=$((i + 1))
done
[ "$(sqlite3 "$state" 'SELECT count(*) FROM closure_frontier;')" = 128 ]
[ "$(sqlite3 "$state" "SELECT count(DISTINCT subject||'|'||scope) FROM closure_dependency WHERE dependency_ref='ForeignTwin:https://example.test/old.rss';")" = 128 ]

sqlite3 "$address" "UPDATE candidate_advertisement_term SET term='new-feed' WHERE identity='cms:pack_demand:shared' AND axis='source';"
cat > "$root/delta.json" <<'JSON'
{"schema":"bonfyre-fact-delta.v1","delta_id":"shared-pack-new-source","writes":["cms:pack_head:shared-pack"]}
JSON
"$respecialize" apply --frontier-db "$state" --address-db "$address" --delta "$root/delta.json" --scoped-demand "$demand" --scoped-frontier "$frontier" --handoff-dir "$root/handoffs" --closure-dir "$root/closures" --limit 256 > "$root/out.json"
grep -q '"affected_closures":128' "$root/out.json"
grep -q '"processed":128' "$root/out.json"
grep -q '"new_external_candidates":128' "$root/out.json"
grep -q '"material_handoffs_dispatched":1' "$root/out.json"
[ "$(find "$root/handoffs/MaterialCompiler-external" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
handoff=$(find "$root/handoffs/MaterialCompiler-external" -name '*.json' | head -1)
grep -q '"identity":"external:new"' "$handoff"
[ "$(sqlite3 "$state" "SELECT count(DISTINCT subject||'|'||scope) FROM closure_dependency WHERE dependency_ref='ForeignTwin:https://example.test/new.rss';")" = 128 ]

"$respecialize" apply --frontier-db "$state" --address-db "$address" --delta "$root/delta.json" --scoped-demand "$demand" --scoped-frontier "$frontier" --handoff-dir "$root/handoffs" --closure-dir "$root/closures" --limit 256 > "$root/retry.json"
grep -q '"processed":0' "$root/retry.json"
grep -q '"already_processed":128' "$root/retry.json"
[ "$(find "$root/handoffs/MaterialCompiler-external" -name '*.json' | wc -l | tr -d ' ')" = 1 ]

echo '128 semantic closures -> one shared physical material demand: PASS'
