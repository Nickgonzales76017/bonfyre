#!/bin/sh
set -eu
refresh=${1:?refresh binary required}; project=${2:?project binary required}; transition=${3:?transition binary required}; lease=${4:?lease binary required}; gate=${5:?gate binary required}; delta=${6:?delta binary required}; reheat=${7:?reheat binary required}
root=$(mktemp -d); trap 'rm -rf "$root"' EXIT
mkdir "$root/acquire" "$root/cancel" "$root/admitted" "$root/deltas" "$root/receipts" "$root/bodies"
sqlite3 "$root/cms.db" <<'SQL'
CREATE TABLE internet_source(id INTEGER PRIMARY KEY,source TEXT,family TEXT,url TEXT,twin TEXT,tier TEXT,cadence_seconds INTEGER,label TEXT,status TEXT);
CREATE TABLE pack_demand(pack TEXT,needs TEXT,demand_kind TEXT,status TEXT);
CREATE TABLE pack_activation(pack TEXT,status TEXT);
INSERT INTO internet_source VALUES(1,'weather-feed','weather','https://example.test/weather','foreign-weather','FROZEN',1,'Fixture weather','published');
SQL
sqlite3 "$root/address.db" <<'SQL'
CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT,validity_frontier TEXT,canonical_json TEXT);
CREATE TABLE occurrence(id TEXT PRIMARY KEY,source_ref TEXT,primary_url TEXT);
INSERT INTO candidate_advertisement VALUES('external:weather','external_occurrence','ExternalEstate:external:weather','ForeignTwin:admission','MaterialCompiler:external','','{}');
INSERT INTO occurrence VALUES('external:weather','weather-feed','https://example.test/weather');
SQL
printf '%s\n' '{"schema":"bonfyre-scoped-demand-closure.v3","subject":"world:weather","scope":"world:weather","pack_demands":[{"candidate_groups":[{"candidates":[{"identity":"external:weather"}]}]}],"world_demands":[],"truth":"scoped_candidate_cut_exact_owner_recheck_required"}' > "$root/closure.json"
"$lease" sync --db "$root/lease.db" --address-db "$root/address.db" --closure "$root/closure.json" >/dev/null
"$lease" drain --db "$root/lease.db" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" >/dev/null
printf '%s\n' '{"schema":"bonfyre-resource-weather.v1","sources":[{"source":"weather-feed","tier":"FROZEN"}]}' > "$root/frozen-gate.json"
"$gate" drain --db "$root/gate.db" --address-db "$root/address.db" --lease-db "$root/lease.db" --weather "$root/frozen-gate.json" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" --out-dir "$root/admitted" >/dev/null
sqlite3 "$root/gate.db" "SELECT source_ref FROM weather_pending WHERE identity='external:weather';" | grep -q 'weather-feed'
common="--project $project --transition $transition --weather $root/weather.json --db $root/transition.db --delta-bin $delta --reheat-bin $reheat --weather-gate $gate --gate-db $root/gate.db --address-db $root/address.db --lease-db $root/lease.db --reheat-db $root/reheat.db --acquire-dir $root/acquire --cancel-dir $root/cancel --out-dir $root/admitted --delta-dir $root/deltas"
BONFYRE_CMS_DB="$root/cms.db" BONFYRE_FOREIGN_RECEIPTS="$root/receipts" BONFYRE_FOREIGN_BODIES="$root/bodies" "$refresh" refresh $common > "$root/baseline.out"
grep -q '"project_dry_run":true' "$root/baseline.out"
grep -q '"baseline":true' "$root/baseline.out"
grep -q '"tier":"FROZEN"' "$root/weather.json"
sqlite3 "$root/cms.db" "UPDATE internet_source SET tier='HOT' WHERE source='weather-feed';"
BONFYRE_CMS_DB="$root/cms.db" BONFYRE_FOREIGN_RECEIPTS="$root/receipts" BONFYRE_FOREIGN_BODIES="$root/bodies" "$refresh" refresh $common > "$root/hot.out"
grep -q '"delta_files":1' "$root/hot.out"
grep -q '"reheat_deltas_processed":1' "$root/hot.out"
[ "$(find "$root/admitted" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
[ "$(find "$root/receipts" -type f | wc -l | tr -d ' ')" = 0 ]
[ "$(find "$root/bodies" -type f | wc -l | tr -d ' ')" = 0 ]
echo 'native whole-census weather refresh -> source-scoped material transition: PASS'
