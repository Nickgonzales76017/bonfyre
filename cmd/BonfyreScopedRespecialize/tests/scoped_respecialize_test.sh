#!/bin/sh
set -eu
respecialize=${1:?respecialize binary required}
demand=${2:?scoped-demand binary required}
frontier=${3:?scoped-frontier binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
# A bounded closure is heap-owned data-plane state. The usage boundary must not
# reserve it on the default thread stack before rejecting incomplete input.
if "$respecialize" --help >"$root/help.out" 2>&1; then
  echo "expected --help to refuse incomplete apply arguments" >&2
  exit 1
fi
grep -q 'usage: bonfyre-scoped-respecialize apply' "$root/help.out"
address="$root/address.db"
state="$root/frontier.db"
mkdir "$root/handoffs" "$root/closures"
sqlite3 "$address" <<'SQL'
CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT,validity_frontier TEXT,canonical_json TEXT);
CREATE TABLE candidate_advertisement_term(identity TEXT,axis TEXT,term TEXT);
CREATE TABLE candidate_advertisement_dependency(identity TEXT,dependency_ref TEXT);
CREATE TABLE occurrence(id TEXT PRIMARY KEY,source_ref TEXT,primary_url TEXT);
INSERT INTO candidate_advertisement VALUES
('cms:pack_activation:deep','pack','CMS:pack_activation:deep','CMS:publication','PackGraph:activation','','{}'),
('cms:pack_demand:deep','pack_demand','CMS:pack_demand:deep','CMS:publication','MaterialCompiler:pack_demand','','{}'),
('cms:world:deep','world_publication','CMS:world:deep','CMS:publication','SurfaceIR:world','','{}'),
('external:old','external_occurrence','ExternalEstate:external:old','ForeignTwin:admission','MaterialCompiler:external','','{}'),
('external:new','external_occurrence','ExternalEstate:external:new','ForeignTwin:admission','MaterialCompiler:external','','{}');
INSERT INTO candidate_advertisement_term VALUES
('cms:pack_activation:deep','pack','deep-ellum-nightlife'),
('cms:pack_activation:deep','subject','world-deep-ellum'),
('cms:pack_activation:deep','scope','world-deep-ellum'),
('cms:pack_demand:deep','pack','deep-ellum-nightlife'),
('cms:pack_demand:deep','demand','internet'),
('cms:pack_demand:deep','requirement','stage-times'),
('cms:pack_demand:deep','source','old-feed'),
('cms:pack_demand:deep','regime','remote'),
('cms:world:deep','subject','world-deep-ellum');
INSERT INTO candidate_advertisement_dependency VALUES
('cms:pack_activation:deep','cms:pack_head:deep-ellum-nightlife'),
('cms:pack_demand:deep','cms:pack_head:deep-ellum-nightlife'),
('cms:world:deep','cms:internet_source:old-feed'),
('cms:world:deep','ForeignTwin:https://example.test/old.rss'),
('external:old','ExternalEstate:external:old'),
('external:old','ForeignTwin:https://example.test/old.rss'),
('external:new','ExternalEstate:external:new'),
('external:new','ForeignTwin:https://example.test/new.rss');
INSERT INTO occurrence VALUES
('external:old','old-feed','https://example.test/old.rss'),
('external:new','new-feed','https://example.test/new.rss');
SQL

"$demand" resolve --db "$address" --subject world:deep-ellum --scope world:deep-ellum > "$root/initial.json"
"$frontier" track --db "$state" --address-db "$address" --closure "$root/initial.json" > "$root/track.json"
grep -q '"identity":"external:old"' "$root/initial.json"
[ "$(sqlite3 "$state" "SELECT count(*) FROM closure_dependency WHERE basis='external_candidate' AND dependency_ref='external:old';")" = 1 ]

# A new observation of the same exact source reheats the semantic closure but must
# not manufacture a second request to observe what just caused the FactDelta.
cat > "$root/same-source.json" <<'JSON'
{"schema":"bonfyre-fact-delta.v1","delta_id":"same-source-observed","writes":["ForeignTwin:https://example.test/old.rss"]}
JSON
"$respecialize" apply --frontier-db "$state" --address-db "$address" --delta "$root/same-source.json" --scoped-demand "$demand" --scoped-frontier "$frontier" --handoff-dir "$root/handoffs" --closure-dir "$root/closures" > "$root/same.out"
grep -q '"affected_closures":1' "$root/same.out"
grep -q '"new_external_candidates":0' "$root/same.out"
grep -q '"material_handoffs_dispatched":0' "$root/same.out"
[ "$(find "$root/handoffs" -name '*.json' | wc -l | tr -d ' ')" = 0 ]
"$frontier" affected --db "$state" --write 'ForeignTwin:https://example.test/old.rss' | grep -q '"affected_count":1'

# A structural declaration change selects a genuinely new source. Only that new
# source earns a MaterialCompiler handoff; the old source is not re-requested.
sqlite3 "$address" <<'SQL'
UPDATE candidate_advertisement_term SET term='new-feed'
 WHERE identity='cms:pack_demand:deep' AND axis='source';
DELETE FROM candidate_advertisement_dependency
 WHERE identity='cms:world:deep'
   AND dependency_ref IN ('cms:internet_source:old-feed','ForeignTwin:https://example.test/old.rss');
INSERT INTO candidate_advertisement_dependency VALUES
('cms:world:deep','cms:internet_source:new-feed'),
('cms:world:deep','ForeignTwin:https://example.test/new.rss');
SQL
cat > "$root/head-change.json" <<'JSON'
{"schema":"bonfyre-fact-delta.v1","delta_id":"pack-head-new-source","writes":["cms:pack_head:deep-ellum-nightlife"]}
JSON
"$respecialize" apply --frontier-db "$state" --address-db "$address" --delta "$root/head-change.json" --scoped-demand "$demand" --scoped-frontier "$frontier" --handoff-dir "$root/handoffs" --closure-dir "$root/closures" > "$root/change.out"
grep -q '"affected_closures":1' "$root/change.out"
grep -q '"new_external_candidates":1' "$root/change.out"
grep -q '"material_handoffs_dispatched":1' "$root/change.out"
[ "$(find "$root/handoffs/MaterialCompiler-external" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
handoff=$(find "$root/handoffs/MaterialCompiler-external" -name '*.json' | head -1)
grep -q '"identity":"external:new"' "$handoff"
! grep -q '"identity":"external:old"' "$handoff"
"$frontier" affected --db "$state" --write 'ForeignTwin:https://example.test/old.rss' | grep -q '"affected_count":0'
"$frontier" affected --db "$state" --write 'ForeignTwin:https://example.test/new.rss' | grep -q '"affected_count":1'

# Delta replay is idempotent at the semantic-closure level.
"$respecialize" apply --frontier-db "$state" --address-db "$address" --delta "$root/head-change.json" --scoped-demand "$demand" --scoped-frontier "$frontier" --handoff-dir "$root/handoffs" --closure-dir "$root/closures" > "$root/retry.out"
grep -q '"processed":0' "$root/retry.out"
grep -q '"already_processed":1' "$root/retry.out"
[ "$(find "$root/handoffs/MaterialCompiler-external" -name '*.json' | wc -l | tr -d ' ')" = 1 ]

echo 'FactDelta -> semantic respecialization -> only-new physical demand: PASS'
