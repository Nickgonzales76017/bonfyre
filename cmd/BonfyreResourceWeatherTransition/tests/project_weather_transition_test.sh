#!/bin/sh
set -eu

transition=${1:?transition binary required}
lease=${2:?lease binary required}
gate=${3:?gate binary required}
delta=${4:?delta binary required}
reheat=${5:?reheat binary required}
project=${6:?BonfyreProject binary required}

root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
mkdir "$root/acquire" "$root/cancel" "$root/admitted" "$root/deltas" \
      "$root/receipts" "$root/bodies"

# This is an explicitly local CMS census fixture. BonfyreProject is invoked
# only with --dry-run, so it publishes its actual whole-census weather shape
# without fetching the example URL or producing a ForeignTwin receipt/body.
sqlite3 "$root/cms.db" <<'SQL'
CREATE TABLE internet_source(
  id INTEGER PRIMARY KEY,
  source TEXT,
  family TEXT,
  url TEXT,
  twin TEXT,
  tier TEXT,
  cadence_seconds INTEGER,
  label TEXT,
  status TEXT
);
CREATE TABLE pack_demand(pack TEXT, needs TEXT, demand_kind TEXT, status TEXT);
CREATE TABLE pack_activation(pack TEXT, status TEXT);
INSERT INTO internet_source VALUES(
  1, 'weather-feed', 'weather', 'https://example.test/weather',
  'foreign-weather', 'FROZEN', 1, 'Fixture weather', 'published'
);
SQL
sqlite3 "$root/address.db" <<'SQL'
CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT,validity_frontier TEXT,canonical_json TEXT);
CREATE TABLE occurrence(id TEXT PRIMARY KEY,source_ref TEXT,primary_url TEXT);
INSERT INTO candidate_advertisement VALUES('external:weather','external_occurrence','ExternalEstate:external:weather','ForeignTwin:admission','MaterialCompiler:external','','{}');
INSERT INTO occurrence VALUES('external:weather','weather-feed','https://example.test/weather');
SQL
printf '%s\n' '{"schema":"bonfyre-scoped-demand-closure.v3","subject":"world:weather","scope":"world:weather","pack_demands":[{"candidate_groups":[{"candidates":[{"identity":"external:weather"}]}]}],"world_demands":[],"truth":"scoped_candidate_cut_exact_owner_recheck_required"}' > "$root/closure.json"

BONFYRE_CMS_DB="$root/cms.db" \
BONFYRE_FOREIGN_RECEIPTS="$root/receipts" \
BONFYRE_FOREIGN_BODIES="$root/bodies" \
BONFYRE_WEATHER="$root/weather.json" \
  "$project" foreign reheat --dry-run > "$root/frozen-project.out"
grep -q '"schema":"bonfyre-resource-weather.v1"' "$root/weather.json"
grep -q '"source":"weather-feed"' "$root/weather.json"
grep -q '"tier":"FROZEN"' "$root/weather.json"

"$lease" sync --db "$root/leases.db" --address-db "$root/address.db" --closure "$root/closure.json" >/dev/null
"$lease" drain --db "$root/leases.db" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" >/dev/null
"$gate" drain --db "$root/gate.db" --address-db "$root/address.db" --lease-db "$root/leases.db" --weather "$root/weather.json" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" --out-dir "$root/admitted" >/dev/null

common="--db $root/transition.db --delta-bin $delta --reheat-bin $reheat --weather-gate $gate --gate-db $root/gate.db --address-db $root/address.db --lease-db $root/leases.db --reheat-db $root/reheat.db --acquire-dir $root/acquire --cancel-dir $root/cancel --out-dir $root/admitted --delta-dir $root/deltas"
"$transition" advance $common --snapshot "$root/weather.json" > "$root/baseline.out"
grep -q '"baseline":true' "$root/baseline.out"

sqlite3 "$root/cms.db" "UPDATE internet_source SET tier='HOT' WHERE source='weather-feed';"
BONFYRE_CMS_DB="$root/cms.db" \
BONFYRE_FOREIGN_RECEIPTS="$root/receipts" \
BONFYRE_FOREIGN_BODIES="$root/bodies" \
BONFYRE_WEATHER="$root/weather.json" \
  "$project" foreign reheat --dry-run > "$root/hot-project.out"
grep -q '"tier":"HOT"' "$root/weather.json"
grep -q '"decision":"due"' "$root/weather.json"

"$transition" advance $common --snapshot "$root/weather.json" > "$root/hot.out"
grep -q '"delta_files":1' "$root/hot.out"
grep -q '"reheat_deltas_processed":1' "$root/hot.out"
[ "$(find "$root/admitted" -name '*.json' | wc -l | tr -d ' ')" = 1 ]

# A dry-run describes intended physical work but cannot itself observe, retain,
# or mint a receipt. The later WebSub worker remains the only observation edge.
[ "$(find "$root/receipts" -type f | wc -l | tr -d ' ')" = 0 ]
[ "$(find "$root/bodies" -type f | wc -l | tr -d ' ')" = 0 ]
echo 'BonfyreProject weather census -> durable targeted material transition: PASS'
