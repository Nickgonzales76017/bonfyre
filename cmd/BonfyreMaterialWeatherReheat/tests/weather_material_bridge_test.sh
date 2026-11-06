#!/bin/sh
set -eu
reheat=${1:?reheat binary required}; lease=${2:?lease binary required}; gate=${3:?gate binary required}; delta=${4:?weather delta binary required}; workers=${5:?lowering workers required}; websub=${6:?websub required}; estate=${7:?external estate required}
root=$(mktemp -d); trap 'rm -rf "$root"' EXIT
mkdir -p "$root/acquire" "$root/cancel" "$root/admitted" "$root/deltas" "$root/demands" "$root/foreign" "$root/bin" "$root/plane-in/MaterialCompiler-external" "$root/plane-out"
# The first database is an exact local address/lease fixture.  The separate
# AddressPlane database below proves that the resulting ForeignTwin delta is
# forwarded back into the same MaterialCompiler family rather than looping.
sqlite3 "$root/lease-address.db" <<'SQL'
CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT,validity_frontier TEXT,canonical_json TEXT);
CREATE TABLE occurrence(id TEXT PRIMARY KEY,source_ref TEXT,primary_url TEXT);
INSERT INTO candidate_advertisement VALUES('external:weather','external_occurrence','ExternalEstate:external:weather','ForeignTwin:admission','MaterialCompiler:external','','{}');
INSERT INTO occurrence VALUES('external:weather','weather-feed','http://127.0.0.1:19876/weather');
SQL
cat > "$root/closure.json" <<'JSON'
{"schema":"bonfyre-scoped-demand-closure.v3","subject":"world:weather","scope":"world:weather","pack_demands":[{"candidate_groups":[{"candidates":[{"identity":"external:weather"}]}]}],"world_demands":[],"truth":"scoped_candidate_cut_exact_owner_recheck_required"}
JSON
printf '%s\n' '{"schema":"bonfyre-resource-weather.v1","sources":[{"source":"weather-feed","tier":"FROZEN"}]}' > "$root/old-weather.json"
printf '%s\n' '{"schema":"bonfyre-resource-weather.v1","sources":[{"source":"weather-feed","tier":"HOT"}]}' > "$root/new-weather.json"
"$lease" sync --db "$root/leases.db" --address-db "$root/lease-address.db" --closure "$root/closure.json" >/dev/null
"$lease" drain --db "$root/leases.db" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" >/dev/null
"$gate" drain --db "$root/gate.db" --address-db "$root/lease-address.db" --lease-db "$root/leases.db" --weather "$root/old-weather.json" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" --out-dir "$root/admitted" >/dev/null
"$delta" diff --old "$root/old-weather.json" --new "$root/new-weather.json" --out "$root/deltas" >/dev/null
weather_delta=$(find "$root/deltas" -type f -name '*.json')
"$reheat" reheat --db "$root/reheat.db" --weather-gate "$gate" --gate-db "$root/gate.db" --address-db "$root/lease-address.db" --lease-db "$root/leases.db" --weather "$root/new-weather.json" --delta "$weather_delta" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" --out-dir "$root/admitted" > "$root/reheat.out"
grep -q '"admitted":1' "$root/reheat.out"

# Now use the real native MaterialCompiler consumer.  It emits an observation
# demand only; the subsequent labelled ForeignTwin owner is the only writer of
# an observation receipt and FactDelta.
"$workers" drain --db "$root/derived.db" --lowerer-dir "$root/admitted" --external-estate "$root/lease-address.db" --demand-dir "$root/demands" --limit 8 > "$root/material.out"
grep -q '"handled":1' "$root/material.out"
grep -q '"truth":"observation_requested_not_performed"' "$root/demands/external-weather.json"
printf '%s\n' '{"identity":"external:weather","candidate_kind":"external_occurrence","owner_ref":"ExternalEstate:external:weather","exact_verifier":"ForeignTwin:admission","lowerer_ref":"MaterialCompiler:external","validity_frontier":"fixture","terms":["domain:weather"],"dependencies":["ForeignTwin:http://127.0.0.1:19876/weather"]}' > "$root/advertisement.jsonl"
"$estate" import-advertisements --db "$root/plane.db" --advertisements "$root/advertisement.jsonl" >/dev/null
BONFYRE_WEBSUB_DB="$root/websub.db" BONFYRE_WEBSUB_ALLOW_INSECURE_TEST=1 "$websub" accept-demands --dir "$root/demands" > "$root/accepted.out"
grep -q '"accepted_demands":1' "$root/accepted.out"
cat > "$root/bin/bonfyre-project" <<'SH'
#!/bin/sh
topic=''; receipt=''
while [ "$#" -gt 0 ]; do case "$1" in observe) shift; shift; topic=$1 ;; --receipt) shift; receipt=$1 ;; esac; shift; done
[ -n "$topic" ] && [ -n "$receipt" ]
printf '{"schema":"bonfyre-foreign-observation.v1","source_url":"%s","observed_at":"2026-09-15T00:00:00Z","observation_kind":"boundary_response"}\n' "$topic" > "$receipt"
SH
chmod +x "$root/bin/bonfyre-project"
BONFYRE_WEBSUB_DB="$root/websub.db" BONFYRE_WEBSUB_ALLOW_INSECURE_TEST=1 BONFYRE_BIN_DIR="$root/bin" BONFYRE_FOREIGN_ROOT="$root/foreign" BONFYRE_FACT_DELTAS="$root/fact-deltas" "$websub" drain --limit 8 > "$root/observed.out"
grep -q '"foreign_twin_fact_deltas":1' "$root/observed.out"
"$estate" fact-delta --db "$root/plane.db" --delta "$root/fact-deltas"/*.json > "$root/cut.out"
grep -q '"affected_count":1' "$root/cut.out"
"$estate" drain-lowering-queue --db "$root/plane.db" --dir "$root/plane-out" --limit 8 >/dev/null
grep -q '"causal_writes":\["ForeignTwin:http://127.0.0.1:19876/weather"\]' "$root/plane-out"/MaterialCompiler-external/*.json
"$workers" drain --db "$root/derived.db" --lowerer-dir "$root/plane-out" --external-estate "$root/lease-address.db" --foreign-receipts "$root/foreign/receipts" --demand-dir "$root/demands" --limit 8 > "$root/materialized.out"
[ "$(sqlite3 "$root/derived.db" "SELECT count(*) FROM material_foreign_receipt_artifact WHERE identity='external:weather' AND observation_kind='boundary_response';")" = 1 ]
[ "$(find "$root/demands" -name '*.json' ! -name '*.handled' | wc -l | tr -d ' ')" = 0 ]

# A later scheduler transition for the same HOT lease is not a replay of the
# old acquire. It creates one fresh MaterialCompiler request, then stops at
# WebSub's coalesced SourceDirty boundary. The existing receipt remains owner
# history; no scheduler component treats this as a fact/evidence rewrite.
printf '%s\n' '{"schema":"bonfyre-resource-weather.v1","sources":[{"source":"weather-feed","tier":"HOT","decision":"due"}]}' > "$root/due-weather.json"
"$delta" diff --old "$root/new-weather.json" --new "$root/due-weather.json" --out "$root/due-deltas" >/dev/null
due_delta=$(find "$root/due-deltas" -type f -name '*.json')
"$reheat" reheat --db "$root/reheat.db" --weather-gate "$gate" --gate-db "$root/gate.db" --address-db "$root/lease-address.db" --lease-db "$root/leases.db" --weather "$root/due-weather.json" --delta "$due_delta" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" --out-dir "$root/admitted" > "$root/due-reheat.out"
grep -q '"admitted":0' "$root/due-reheat.out"
grep -q '"refreshes_emitted":1' "$root/due-reheat.out"
"$workers" drain --db "$root/derived.db" --lowerer-dir "$root/admitted" --external-estate "$root/lease-address.db" --demand-dir "$root/demands" --limit 8 > "$root/refresh-material.out"
grep -q '"handled":1' "$root/refresh-material.out"
BONFYRE_WEBSUB_DB="$root/websub.db" BONFYRE_WEBSUB_ALLOW_INSECURE_TEST=1 "$websub" accept-demands --dir "$root/demands" > "$root/refresh-accepted.out"
grep -Eq '"accepted_demands":1|"coalesced_topics":1' "$root/refresh-accepted.out"
[ "$(sqlite3 "$root/websub.db" "SELECT count(*) FROM source_dirty WHERE topic='http://127.0.0.1:19876/weather' AND state IN ('pending','running');")" = 1 ]
"$reheat" reheat --db "$root/reheat.db" --weather-gate "$gate" --gate-db "$root/gate.db" --address-db "$root/lease-address.db" --lease-db "$root/leases.db" --weather "$root/due-weather.json" --delta "$due_delta" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" --out-dir "$root/admitted" > "$root/due-replay.out"
grep -q '"processed":0' "$root/due-replay.out"
echo 'ResourceWeather FactDelta -> gated material -> ForeignTwin -> receipt materialization: PASS'
