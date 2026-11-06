#!/bin/sh
set -eu
gate=${1:?weather gate binary required}
lease=${2:?material lease binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
address="$root/address.db"
leases="$root/leases.db"
gate_db="$root/gate.db"
mkdir "$root/acquire1" "$root/cancel1" "$root/admitted" "$root/acquire2" "$root/cancel2"

sqlite3 "$address" <<'SQL'
CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT,validity_frontier TEXT,canonical_json TEXT);
CREATE TABLE occurrence(id TEXT PRIMARY KEY,source_ref TEXT,primary_url TEXT);
INSERT INTO candidate_advertisement VALUES('external:deep','external_occurrence','ExternalEstate:external:deep','ForeignTwin:admission','MaterialCompiler:external','','{}');
INSERT INTO occurrence VALUES('external:deep','deep-feed','https://example.test/deep.rss');
SQL

cat > "$root/on.json" <<'JSON'
{"schema":"bonfyre-scoped-demand-closure.v3","subject":"world:deep-ellum","scope":"world:deep-ellum","pack_demands":[{"candidate_groups":[{"candidates":[{"identity":"external:deep"}]}]}],"world_demands":[],"truth":"scoped_candidate_cut_exact_owner_recheck_required"}
JSON
cat > "$root/off.json" <<'JSON'
{"schema":"bonfyre-scoped-demand-closure.v3","subject":"world:deep-ellum","scope":"world:deep-ellum","pack_demands":[],"world_demands":[],"truth":"scoped_candidate_cut_exact_owner_recheck_required"}
JSON
cat > "$root/frozen.json" <<'JSON'
{"schema":"bonfyre-resource-weather.v1","sources":[{"source":"deep-feed","tier":"FROZEN","decision":"frozen; not asked again until something changes there"}]}
JSON
cat > "$root/hot.json" <<'JSON'
{"schema":"bonfyre-resource-weather.v1","sources":[{"source":"deep-feed","tier":"HOT","decision":"due"}]}
JSON

# First semantic lease creates one physical acquire intent, but FROZEN weather
# holds it before MaterialCompiler can see it.
"$lease" sync --db "$leases" --address-db "$address" --closure "$root/on.json" > "$root/sync1.json"
grep -q '"physical_acquires":1' "$root/sync1.json"
"$lease" drain --db "$leases" --acquire-dir "$root/acquire1" --cancel-dir "$root/cancel1" > "$root/drain1.json"
grep -q '"transitions_emitted":1' "$root/drain1.json"
"$gate" drain --db "$gate_db" --address-db "$address" --lease-db "$leases" --weather "$root/frozen.json" --acquire-dir "$root/acquire1" --cancel-dir "$root/cancel1" --out-dir "$root/admitted" > "$root/frozen.out"
grep -q '"admitted":0' "$root/frozen.out"
grep -q '"held":1' "$root/frozen.out"
[ "$(find "$root/admitted" -name '*.json' | wc -l | tr -d ' ')" = 0 ]
[ "$(sqlite3 "$gate_db" "SELECT count(*) FROM weather_pending WHERE identity='external:deep' AND state='pending' AND tier='FROZEN';")" = 1 ]

# The final semantic lease disappears before thaw. Its release cancels the held
# acquire, and later HOT weather must not resurrect that stale physical work.
"$lease" sync --db "$leases" --address-db "$address" --closure "$root/off.json" > "$root/sync-off.json"
grep -q '"physical_releases":1' "$root/sync-off.json"
"$lease" drain --db "$leases" --acquire-dir "$root/acquire1" --cancel-dir "$root/cancel1" > "$root/drain-off.json"
grep -q '"transitions_emitted":1' "$root/drain-off.json"
"$gate" drain --db "$gate_db" --address-db "$address" --lease-db "$leases" --weather "$root/frozen.json" --acquire-dir "$root/acquire1" --cancel-dir "$root/cancel1" --out-dir "$root/admitted" > "$root/cancel.out"
grep -q '"pending_cancelled":1' "$root/cancel.out"
[ "$(sqlite3 "$gate_db" "SELECT count(*) FROM weather_pending WHERE identity='external:deep' AND state='cancelled';")" = 1 ]
"$gate" drain --db "$gate_db" --address-db "$address" --lease-db "$leases" --weather "$root/hot.json" --acquire-dir "$root/acquire1" --cancel-dir "$root/cancel1" --out-dir "$root/admitted" > "$root/stale-hot.out"
grep -q '"admitted":0' "$root/stale-hot.out"
[ "$(find "$root/admitted" -name '*.json' | wc -l | tr -d ' ')" = 0 ]

# Reacquisition creates a new transition id. HOT weather admits that new intent
# exactly once; rescanning immutable outboxes is idempotent by acquire id.
"$lease" sync --db "$leases" --address-db "$address" --closure "$root/on.json" > "$root/sync2.json"
grep -q '"physical_acquires":1' "$root/sync2.json"
"$lease" drain --db "$leases" --acquire-dir "$root/acquire2" --cancel-dir "$root/cancel2" > "$root/drain2.json"
grep -q '"transitions_emitted":1' "$root/drain2.json"
"$gate" drain --db "$gate_db" --address-db "$address" --lease-db "$leases" --weather "$root/hot.json" --acquire-dir "$root/acquire2" --cancel-dir "$root/cancel2" --out-dir "$root/admitted" > "$root/hot.out"
grep -q '"admitted":1' "$root/hot.out"
grep -q '"held":0' "$root/hot.out"
[ "$(find "$root/admitted/MaterialCompiler-external" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
"$gate" drain --db "$gate_db" --address-db "$address" --lease-db "$leases" --weather "$root/hot.json" --acquire-dir "$root/acquire2" --cancel-dir "$root/cancel2" --out-dir "$root/admitted" > "$root/hot-retry.out"
grep -q '"admitted":0' "$root/hot-retry.out"
[ "$(find "$root/admitted/MaterialCompiler-external" -name '*.json' | wc -l | tr -d ' ')" = 1 ]

echo 'MaterialLease -> ResourceWeather hold/thaw/cancel gate: PASS'
