#!/bin/sh
set -eu
reheat=${1:?reheat binary required}; lease=${2:?lease binary required}; gate=${3:?gate binary required}; delta=${4:?delta binary required}
root=$(mktemp -d); trap 'rm -rf "$root"' EXIT
address="$root/address.db"; leases="$root/leases.db"; gate_db="$root/gate.db"; reheat_db="$root/reheat.db"; mkdir "$root/acquire" "$root/cancel" "$root/admitted" "$root/deltas"
sqlite3 "$address" <<'SQL'
CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT,validity_frontier TEXT,canonical_json TEXT);
CREATE TABLE occurrence(id TEXT PRIMARY KEY,source_ref TEXT,primary_url TEXT);
INSERT INTO candidate_advertisement VALUES('external:a','external_occurrence','ExternalEstate:external:a','ForeignTwin:admission','MaterialCompiler:external','','{}');
INSERT INTO candidate_advertisement VALUES('external:b','external_occurrence','ExternalEstate:external:b','ForeignTwin:admission','MaterialCompiler:external','','{}');
INSERT INTO occurrence VALUES('external:a','feed-a','https://example.test/a.rss'); INSERT INTO occurrence VALUES('external:b','feed-b','https://example.test/b.rss');
SQL
for x in a b; do cat > "$root/on-$x.json" <<JSON
{"schema":"bonfyre-scoped-demand-closure.v3","subject":"world:$x","scope":"world:$x","pack_demands":[{"candidate_groups":[{"candidates":[{"identity":"external:$x"}]}]}],"world_demands":[],"truth":"scoped_candidate_cut_exact_owner_recheck_required"}
JSON
done
cat > "$root/off-b.json" <<'JSON'
{"schema":"bonfyre-scoped-demand-closure.v3","subject":"world:b","scope":"world:b","pack_demands":[],"world_demands":[],"truth":"scoped_candidate_cut_exact_owner_recheck_required"}
JSON
printf '%s\n' '{"schema":"bonfyre-resource-weather.v1","sources":[{"source":"feed-a","tier":"FROZEN"},{"source":"feed-b","tier":"FROZEN"}]}' > "$root/old.json"
printf '%s\n' '{"schema":"bonfyre-resource-weather.v1","sources":[{"source":"feed-a","tier":"HOT"},{"source":"feed-b","tier":"FROZEN"}]}' > "$root/new-a.json"
printf '%s\n' '{"schema":"bonfyre-resource-weather.v1","sources":[{"source":"feed-a","tier":"HOT"},{"source":"feed-b","tier":"HOT"}]}' > "$root/new-b.json"
"$lease" sync --db "$leases" --address-db "$address" --closure "$root/on-a.json" >/dev/null; "$lease" sync --db "$leases" --address-db "$address" --closure "$root/on-b.json" >/dev/null
"$lease" drain --db "$leases" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" >/dev/null
"$gate" drain --db "$gate_db" --address-db "$address" --lease-db "$leases" --weather "$root/old.json" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" --out-dir "$root/admitted" >/dev/null
[ "$(sqlite3 "$gate_db" "SELECT count(*) FROM weather_pending WHERE state='pending' AND tier='FROZEN';")" = 2 ]
"$delta" diff --old "$root/old.json" --new "$root/new-a.json" --out "$root/deltas" >/dev/null
a_delta=$(find "$root/deltas" -type f -exec grep -l 'ResourceWeather:feed-a' {} +)
"$reheat" reheat --db "$reheat_db" --weather-gate "$gate" --gate-db "$gate_db" --address-db "$address" --lease-db "$leases" --weather "$root/new-a.json" --delta "$a_delta" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" --out-dir "$root/admitted" > "$root/a.out"
grep -q '"targeted_sources":1' "$root/a.out"; grep -q '"admitted":1' "$root/a.out"; [ "$(find "$root/admitted" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
grep -q '"pending_intents_examined":1' "$root/a.out"
[ "$(sqlite3 "$gate_db" "SELECT count(*) FROM weather_pending WHERE identity='external:a' AND state='admitted';")" = 1 ]; [ "$(sqlite3 "$gate_db" "SELECT count(*) FROM weather_pending WHERE identity='external:b' AND state='pending';")" = 1 ]
"$reheat" reheat --db "$reheat_db" --weather-gate "$gate" --gate-db "$gate_db" --address-db "$address" --lease-db "$leases" --weather "$root/new-a.json" --delta "$a_delta" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" --out-dir "$root/admitted" > "$root/a-replay.out"
grep -q '"processed":0' "$root/a-replay.out"; [ "$(find "$root/admitted" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
"$lease" sync --db "$leases" --address-db "$address" --closure "$root/off-b.json" >/dev/null; "$lease" drain --db "$leases" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" >/dev/null
"$delta" diff --old "$root/new-a.json" --new "$root/new-b.json" --out "$root/deltas" >/dev/null
b_delta=$(find "$root/deltas" -type f -exec grep -l 'ResourceWeather:feed-b' {} +)
"$reheat" reheat --db "$reheat_db" --weather-gate "$gate" --gate-db "$gate_db" --address-db "$address" --lease-db "$leases" --weather "$root/new-b.json" --delta "$b_delta" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" --out-dir "$root/admitted" > "$root/b.out"
grep -q '"admitted":0' "$root/b.out"; grep -q '"pending_cancelled":1' "$root/b.out"; [ "$(sqlite3 "$gate_db" "SELECT count(*) FROM weather_pending WHERE identity='external:b' AND state='cancelled';")" = 1 ]; [ "$(find "$root/admitted" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
echo 'ResourceWeather FactDelta -> only matching held lease reheat: PASS'
