#!/bin/sh
set -eu
respecialize=${1:?respecialize binary required}
demand=${2:?scoped-demand binary required}
frontier=${3:?scoped-frontier binary required}
workers=${4:?lowering workers binary required}
websub=${5:?websub binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
address="$root/address.db"
state="$root/frontier.db"
mkdir "$root/initial-handoffs" "$root/reheat-handoffs" "$root/demands" "$root/closures" "$root/bin" "$root/foreign"
sqlite3 "$address" <<'SQL'
CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT,validity_frontier TEXT,canonical_json TEXT);
CREATE TABLE candidate_advertisement_term(identity TEXT,axis TEXT,term TEXT);
CREATE TABLE candidate_advertisement_dependency(identity TEXT,dependency_ref TEXT);
CREATE TABLE occurrence(id TEXT PRIMARY KEY,source_ref TEXT,primary_url TEXT);
INSERT INTO candidate_advertisement VALUES
('cms:pack_activation:deep','pack','CMS:pack_activation:deep','CMS:publication','PackGraph:activation','','{}'),
('cms:pack_demand:deep','pack_demand','CMS:pack_demand:deep','CMS:publication','MaterialCompiler:pack_demand','','{}'),
('cms:world:deep','world_publication','CMS:world:deep','CMS:publication','SurfaceIR:world','','{}'),
('external:deep','external_occurrence','ExternalEstate:external:deep','ForeignTwin:admission','MaterialCompiler:external','','{}');
INSERT INTO candidate_advertisement_term VALUES
('cms:pack_activation:deep','pack','deep-ellum-nightlife'),
('cms:pack_activation:deep','subject','world-deep-ellum'),
('cms:pack_activation:deep','scope','world-deep-ellum'),
('cms:pack_demand:deep','pack','deep-ellum-nightlife'),
('cms:pack_demand:deep','demand','internet'),
('cms:pack_demand:deep','requirement','stage-times'),
('cms:pack_demand:deep','source','deep-feed'),
('cms:pack_demand:deep','regime','remote'),
('cms:world:deep','subject','world-deep-ellum');
INSERT INTO candidate_advertisement_dependency VALUES
('cms:pack_activation:deep','cms:pack_head:deep-ellum-nightlife'),
('cms:pack_demand:deep','cms:pack_head:deep-ellum-nightlife'),
('cms:world:deep','ForeignTwin:http://127.0.0.1:19876/deep.rss'),
('external:deep','ExternalEstate:external:deep'),
('external:deep','ForeignTwin:http://127.0.0.1:19876/deep.rss');
INSERT INTO occurrence VALUES('external:deep','deep-feed','http://127.0.0.1:19876/deep.rss');
SQL

"$demand" resolve --db "$address" --subject world:deep-ellum --scope world:deep-ellum > "$root/initial.json"
"$frontier" track --db "$state" --address-db "$address" --closure "$root/initial.json" > /dev/null
"$demand" dispatch --db "$address" --subject world:deep-ellum --scope world:deep-ellum --dir "$root/initial-handoffs" > "$root/dispatched.json"
grep -q '"handoffs_dispatched":1' "$root/dispatched.json"
"$workers" drain --db "$root/derived.db" --lowerer-dir "$root/initial-handoffs" --external-estate "$address" --demand-dir "$root/demands" --limit 8 > "$root/lowered.json"
grep -q '"handled":1' "$root/lowered.json"
[ "$(sqlite3 "$root/derived.db" "SELECT count(*) FROM material_observation_demand WHERE identity='external:deep' AND state='queued';")" = 1 ]

BONFYRE_WEBSUB_DB="$root/websub.db" BONFYRE_WEBSUB_ALLOW_INSECURE_TEST=1 \
  "$websub" accept-demands --dir "$root/demands" > "$root/accepted.json"
grep -q '"accepted_demands":1' "$root/accepted.json"
cat > "$root/bin/bonfyre-project" <<'SH'
#!/bin/sh
topic=''
receipt=''
while [ "$#" -gt 0 ]; do
  case "$1" in
    observe) shift; shift; topic=$1 ;;
    --receipt) shift; receipt=$1 ;;
  esac
  shift
done
[ -n "$topic" ] && [ -n "$receipt" ]
printf '{"schema":"bonfyre-foreign-observation.v1","source_url":"%s","observed_at":"2026-09-15T17:00:00Z","observation_kind":"boundary_response"}\n' "$topic" > "$receipt"
SH
chmod +x "$root/bin/bonfyre-project"
BONFYRE_WEBSUB_DB="$root/websub.db" \
BONFYRE_WEBSUB_ALLOW_INSECURE_TEST=1 \
BONFYRE_BIN_DIR="$root/bin" \
BONFYRE_FOREIGN_ROOT="$root/foreign" \
BONFYRE_FACT_DELTAS="$root/fact-deltas" \
  "$websub" drain --limit 8 > "$root/drained.json"
grep -q '"observed":1' "$root/drained.json"
grep -q '"foreign_twin_fact_deltas":1' "$root/drained.json"
delta=$(find "$root/fact-deltas" -name '*.json' | head -1)
grep -q '"writes":\["ForeignTwin:http://127.0.0.1:19876/deep.rss"\]' "$delta"

"$respecialize" apply --frontier-db "$state" --address-db "$address" --delta "$delta" --scoped-demand "$demand" --scoped-frontier "$frontier" --handoff-dir "$root/reheat-handoffs" --closure-dir "$root/closures" > "$root/respecialized.json"
grep -q '"affected_closures":1' "$root/respecialized.json"
grep -q '"processed":1' "$root/respecialized.json"
grep -q '"new_external_candidates":0' "$root/respecialized.json"
grep -q '"material_handoffs_dispatched":0' "$root/respecialized.json"
[ "$(find "$root/reheat-handoffs" -name '*.json' | wc -l | tr -d ' ')" = 0 ]
[ "$(find "$root/demands" -name '*.json' ! -name '*.handled' | wc -l | tr -d ' ')" = 0 ]

echo 'observation -> FactDelta -> semantic re-specialization -> no self-exciting observation: PASS'
