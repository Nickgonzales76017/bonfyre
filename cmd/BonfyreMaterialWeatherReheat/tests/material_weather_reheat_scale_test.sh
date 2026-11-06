#!/bin/sh
set -eu
reheat=${1:?reheat binary required}; lease=${2:?lease binary required}; gate=${3:?gate binary required}; delta=${4:?weather delta binary required}
root=$(mktemp -d); trap 'rm -rf "$root"' EXIT
mkdir "$root/acquire" "$root/cancel" "$root/admitted" "$root/deltas"
sqlite3 "$root/address.db" 'CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT,validity_frontier TEXT,canonical_json TEXT); CREATE TABLE occurrence(id TEXT PRIMARY KEY,source_ref TEXT,primary_url TEXT);'
printf '%s' '{"schema":"bonfyre-scoped-demand-closure.v3","subject":"world:scale","scope":"world:scale","pack_demands":[{"candidate_groups":[{"candidates":[' > "$root/closure.json"
printf '%s' '{"schema":"bonfyre-resource-weather.v1","sources":[' > "$root/old.json"
printf '%s' '{"schema":"bonfyre-resource-weather.v1","sources":[' > "$root/new.json"
i=1
while [ "$i" -le 128 ]; do
  sep=''; [ "$i" -gt 1 ] && sep=,
  sqlite3 "$root/address.db" "INSERT INTO candidate_advertisement VALUES('external:$i','external_occurrence','ExternalEstate:external:$i','ForeignTwin:admission','MaterialCompiler:external','','{}'); INSERT INTO occurrence VALUES('external:$i','feed-$i','https://example.test/$i');"
  printf '%s{"identity":"external:%s"}' "$sep" "$i" >> "$root/closure.json"
  printf '%s{"source":"feed-%s","tier":"FROZEN"}' "$sep" "$i" >> "$root/old.json"
  tier=FROZEN; [ "$i" -eq 77 ] && tier=HOT
  printf '%s{"source":"feed-%s","tier":"%s"}' "$sep" "$i" "$tier" >> "$root/new.json"
  i=$((i + 1))
done
printf '%s\n' ']}]}],"world_demands":[],"truth":"scoped_candidate_cut_exact_owner_recheck_required"}' >> "$root/closure.json"
printf '%s\n' ']}' >> "$root/old.json"; printf '%s\n' ']}' >> "$root/new.json"
"$lease" sync --db "$root/leases.db" --address-db "$root/address.db" --closure "$root/closure.json" >/dev/null
"$lease" drain --db "$root/leases.db" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" >/dev/null
"$gate" drain --db "$root/gate.db" --address-db "$root/address.db" --lease-db "$root/leases.db" --weather "$root/old.json" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" --out-dir "$root/admitted" >/dev/null
[ "$(sqlite3 "$root/gate.db" "SELECT count(*) FROM weather_pending WHERE state='pending';")" = 128 ]
"$delta" diff --old "$root/old.json" --new "$root/new.json" --out "$root/deltas" >/dev/null
weather_delta=$(find "$root/deltas" -type f -name '*.json')
"$reheat" reheat --db "$root/reheat.db" --weather-gate "$gate" --gate-db "$root/gate.db" --address-db "$root/address.db" --lease-db "$root/leases.db" --weather "$root/new.json" --delta "$weather_delta" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" --out-dir "$root/admitted" > "$root/out.json"
grep -q '"targeted_sources":1' "$root/out.json"; grep -q '"pending_intents_examined":1' "$root/out.json"; grep -q '"admitted":1' "$root/out.json"
[ "$(sqlite3 "$root/gate.db" "SELECT count(*) FROM weather_pending WHERE state='pending';")" = 127 ]
[ "$(find "$root/admitted" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
echo '128 held leases -> one ResourceWeather source reheat: PASS'
