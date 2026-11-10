#!/bin/sh
set -eu
transition=${1:?transition binary required}; lease=${2:?lease binary required}; gate=${3:?gate binary required}; delta=${4:?delta binary required}; reheat=${5:?reheat binary required}
root=$(mktemp -d); trap 'rm -rf "$root"' EXIT
mkdir "$root/acquire" "$root/cancel" "$root/admitted" "$root/deltas"
sqlite3 "$root/address.db" <<'SQL'
CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT,validity_frontier TEXT,canonical_json TEXT);
CREATE TABLE occurrence(id TEXT PRIMARY KEY,source_ref TEXT,primary_url TEXT);
INSERT INTO candidate_advertisement VALUES('external:weather','external_occurrence','ExternalEstate:external:weather','ForeignTwin:admission','MaterialCompiler:external','','{}');
INSERT INTO occurrence VALUES('external:weather','weather-feed','https://example.test/weather');
SQL
cat > "$root/closure.json" <<'JSON'
{"schema":"bonfyre-scoped-demand-closure.v3","subject":"world:weather","scope":"world:weather","pack_demands":[{"candidate_groups":[{"candidates":[{"identity":"external:weather"}]}]}],"world_demands":[],"truth":"scoped_candidate_cut_exact_owner_recheck_required"}
JSON
printf '%s\n' '{"schema":"bonfyre-resource-weather.v1","sources":[{"source":"weather-feed","tier":"FROZEN"}]}' > "$root/frozen.json"
printf '%s\n' '{"schema":"bonfyre-resource-weather.v1","sources":[{"source":"weather-feed","tier":"HOT"}]}' > "$root/hot.json"
"$lease" sync --db "$root/leases.db" --address-db "$root/address.db" --closure "$root/closure.json" >/dev/null
"$lease" drain --db "$root/leases.db" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" >/dev/null
"$gate" drain --db "$root/gate.db" --address-db "$root/address.db" --lease-db "$root/leases.db" --weather "$root/frozen.json" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" --out-dir "$root/admitted" >/dev/null
common="--db $root/transition.db --delta-bin $delta --reheat-bin $reheat --weather-gate $gate --gate-db $root/gate.db --address-db $root/address.db --lease-db $root/leases.db --reheat-db $root/reheat.db --acquire-dir $root/acquire --cancel-dir $root/cancel --out-dir $root/admitted --delta-dir $root/deltas"
"$transition" advance $common --snapshot "$root/frozen.json" > "$root/baseline.out"
grep -q '"baseline":true' "$root/baseline.out"; grep -q '"delta_files":0' "$root/baseline.out"
"$transition" advance $common --snapshot "$root/hot.json" > "$root/hot.out"
grep -q '"baseline":false' "$root/hot.out"; grep -q '"delta_files":1' "$root/hot.out"; grep -q '"reheat_deltas_processed":1' "$root/hot.out"
[ "$(find "$root/admitted" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
"$transition" advance $common --snapshot "$root/hot.json" > "$root/retry.out"
grep -q '"unchanged":true' "$root/retry.out"; grep -q '"reheat_deltas_processed":0' "$root/retry.out"
[ "$(find "$root/admitted" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
echo 'ResourceWeather snapshot frontier -> targeted reheat worker: PASS'
