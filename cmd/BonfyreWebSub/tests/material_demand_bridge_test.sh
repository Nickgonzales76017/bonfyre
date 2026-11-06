#!/bin/sh
set -eu

websub=${1:?WebSub binary required}
workers=${2:?LoweringWorkers binary required}
estate=${3:?ExternalEstate binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT

# This is a deliberately local, labelled fixture. The first command is the
# real MaterialCompiler consumer: it must emit a request, never an observation.
mkdir -p "$root/inbox/MaterialCompiler-external" "$root/demands" "$root/bin" "$root/foreign"
sqlite3 "$root/external.db" "CREATE TABLE occurrence(id TEXT PRIMARY KEY,primary_url TEXT); INSERT INTO occurrence VALUES('external:fixture:weather','http://127.0.0.1:19876/weather');"
printf '%s\n' '{"schema":"bonfyre-lowering-handoff.v1","delta_id":"fixture-material","identity":"external:fixture:weather","candidate_kind":"external_occurrence","owner_ref":"ExternalEstate:external:fixture:weather","lowerer_ref":"MaterialCompiler:external","truth":"dependency_cut_only"}' > "$root/inbox/MaterialCompiler-external/material.json"
"$workers" drain --db "$root/derived.db" --lowerer-dir "$root/inbox" --external-estate "$root/external.db" --demand-dir "$root/demands" --limit 8 > "$root/material.json"
grep -q '"handled":1' "$root/material.json"
grep -q '"truth":"observation_requested_not_performed"' "$root/demands/external-fixture-weather.json"
[ "$(sqlite3 "$root/derived.db" 'SELECT count(*) FROM material_observation_demand;')" = 1 ]

# AddressPlane has the same external candidate registered against the exact
# ForeignTwin topic. This is an index fixture only; it carries no source body.
printf '%s\n' '{"identity":"external:fixture:weather","candidate_kind":"external_occurrence","owner_ref":"ExternalEstate:external:fixture:weather","exact_verifier":"ForeignTwin:admission","lowerer_ref":"MaterialCompiler:external","validity_frontier":"fixture","terms":["domain:weather"],"dependencies":["ForeignTwin:http://127.0.0.1:19876/weather"]}' > "$root/advertisement.jsonl"
"$estate" import-advertisements --db "$root/address.db" --advertisements "$root/advertisement.jsonl" > "$root/imported.json"

# The WebSub owner accepts the exact demand envelope, coalesces it into its
# own SourceDirty state, and does not yet invoke the fixture observer.
BONFYRE_WEBSUB_DB="$root/websub.db" BONFYRE_WEBSUB_ALLOW_INSECURE_TEST=1 \
  "$websub" accept-demands --dir "$root/demands" > "$root/accepted.json"
grep -q '"accepted_demands":1' "$root/accepted.json"
grep -q '"truth":"SourceDirty_only_no_foreign_read_or_fact"' "$root/accepted.json"
[ "$(sqlite3 "$root/websub.db" "SELECT count(*) FROM source_dirty WHERE topic='http://127.0.0.1:19876/weather' AND last_demand_identity='external:fixture:weather';")" = 1 ]

# This fixture stands in for the native ForeignTwin owner and writes its
# required receipt. The downstream FactDelta is legal only after that receipt.
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
printf '{"schema":"bonfyre-foreign-observation.v1","source_url":"%s","observed_at":"2026-09-15T00:00:00Z","observation_kind":"boundary_response"}\n' "$topic" > "$receipt"
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
grep -q '"writes":\["ForeignTwin:http://127.0.0.1:19876/weather"\]' "$root/fact-deltas"/*.json

# The independently receipted change returns through AddressPlane. Its handoff
# retains the causal write set, so MaterialCompiler consumes the receipt into
# a derived material artifact rather than re-enqueueing the same observation.
"$estate" fact-delta --db "$root/address.db" --delta "$root/fact-deltas"/*.json > "$root/cut.json"
grep -q '"affected_count":1' "$root/cut.json"
mkdir "$root/forwarded"
"$estate" drain-lowering-queue --db "$root/address.db" --dir "$root/forwarded" --limit 8 > "$root/forwarded.json"
grep -q '"lowering_handoffs_dispatched":1' "$root/forwarded.json"
grep -q '"causal_writes":\["ForeignTwin:http://127.0.0.1:19876/weather"\]' "$root/forwarded"/MaterialCompiler-external/*.json
"$workers" drain --db "$root/derived.db" --lowerer-dir "$root/forwarded" --external-estate "$root/external.db" --foreign-receipts "$root/foreign/receipts" --demand-dir "$root/demands" --limit 8 > "$root/materialized.json"
grep -q '"handled":1' "$root/materialized.json"
[ "$(sqlite3 "$root/derived.db" "SELECT count(*) FROM material_foreign_receipt_artifact WHERE identity='external:fixture:weather' AND observation_kind='boundary_response';")" = 1 ]
[ "$(find "$root/demands" -name '*.json' ! -name '*.handled' | wc -l | tr -d ' ')" = 0 ]

echo 'MaterialCompiler -> SourceDirty -> ForeignTwin -> AddressPlane -> material bridge: PASS'
