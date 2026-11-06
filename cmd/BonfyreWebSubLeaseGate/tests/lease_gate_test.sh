#!/bin/sh
set -eu
bin=${1:?WebSub lease gate required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
lease="$root/lease.db"
web="$root/websub.db"
handoff="$root/handoffs"
demands="$root/demands"
cancels="$root/cancels"
mkdir -p "$handoff/MaterialCompiler-external" "$demands" "$cancels/MaterialCompiler-external"
sqlite3 "$lease" 'CREATE TABLE material_lease(subject TEXT,scope TEXT,identity TEXT,owner_ref TEXT,updated_at INTEGER,PRIMARY KEY(subject,scope,identity));'
sqlite3 "$web" "CREATE TABLE source_dirty(token TEXT PRIMARY KEY,topic TEXT,frontier TEXT,state TEXT,dirty_at INTEGER,claimed_at INTEGER DEFAULT 0,attempts INTEGER DEFAULT 0,last_error TEXT DEFAULT '',demand_count INTEGER DEFAULT 0,last_demand_identity TEXT DEFAULT ''); CREATE TABLE source_dirty_demand(topic TEXT,identity TEXT,first_requested_at INTEGER,last_requested_at INTEGER,PRIMARY KEY(topic,identity));"

cancel() {
  name=$1 id=$2
  printf '%s\n' "{\"schema\":\"bonfyre-material-observation-cancel.v1\",\"transition_id\":1,\"identity\":\"$id\",\"refcount_before\":1,\"refcount_after\":0,\"released_by\":{\"subject\":\"test\",\"scope\":\"test\"},\"truth\":\"lease_count_zero_not_observation_retraction\"}" > "$cancels/MaterialCompiler-external/$name.json"
}
handoff() {
  name=$1 id=$2
  printf '%s\n' "{\"schema\":\"bonfyre-lowering-handoff.v1\",\"delta_id\":\"lease\",\"identity\":\"$id\",\"candidate_kind\":\"external_occurrence\",\"owner_ref\":\"ExternalEstate:$id\",\"lowerer_ref\":\"MaterialCompiler:external\",\"truth\":\"dependency_cut_only\"}" > "$handoff/MaterialCompiler-external/$name.json"
}
demand() {
  name=$1 id=$2 topic=$3
  printf '%s\n' "{\"schema\":\"bonfyre-foreign-observation-demand.v1\",\"identity\":\"$id\",\"topic\":\"$topic\",\"truth\":\"observation_requested_not_performed\"}" > "$demands/$name.json"
}

# A stale cancellation delivered after reacquisition is superseded.
sqlite3 "$lease" "INSERT INTO material_lease VALUES('world:a','world:a','external:race','ExternalEstate:external:race',1);"
handoff race external:race
cancel race external:race
"$bin" apply --cancel-dir "$cancels" --handoff-dir "$handoff" --demand-dir "$demands" --websub-db "$web" --lease-db "$lease" > "$root/race.json"
grep -q '"superseded":1' "$root/race.json"
[ -f "$handoff/MaterialCompiler-external/race.json" ]
[ -f "$cancels/MaterialCompiler-external/race.json.handled" ]

# Once the last lease is actually gone, pending work at both pre-WebSub stages is retired.
sqlite3 "$lease" "DELETE FROM material_lease WHERE identity='external:race';"
demand race-demand external:race https://example.test/race
cancel race-zero external:race
"$bin" apply --cancel-dir "$cancels" --handoff-dir "$handoff" --demand-dir "$demands" --websub-db "$web" --lease-db "$lease" > "$root/prequeue.json"
grep -q '"suppressed":1' "$root/prequeue.json"
grep -q '"handoffs_retired":1' "$root/prequeue.json"
grep -q '"demands_retired":1' "$root/prequeue.json"
[ -f "$handoff/MaterialCompiler-external/race.json.cancelled" ]
[ -f "$demands/race-demand.json.cancelled" ]

# One identity can release while another still keeps the same topic dirty.
sqlite3 "$web" "INSERT INTO source_dirty(token,topic,frontier,state,dirty_at,demand_count,last_demand_identity) VALUES('t-shared','https://example.test/shared','MaterialCompiler:observation-demand','pending',1,2,'external:b'); INSERT INTO source_dirty_demand VALUES('https://example.test/shared','external:a',1,1),('https://example.test/shared','external:b',1,1);"
cancel shared-a external:a
"$bin" apply --cancel-dir "$cancels" --handoff-dir "$handoff" --demand-dir "$demands" --websub-db "$web" --lease-db "$lease" > "$root/shared-a.json"
[ "$(sqlite3 "$web" "SELECT count(*) FROM source_dirty WHERE token='t-shared';")" = 1 ]
[ "$(sqlite3 "$web" "SELECT count(*) FROM source_dirty_demand WHERE topic='https://example.test/shared';")" = 1 ]

# Last demand for a pending topic removes SourceDirty entirely.
cancel shared-b external:b
"$bin" apply --cancel-dir "$cancels" --handoff-dir "$handoff" --demand-dir "$demands" --websub-db "$web" --lease-db "$lease" > "$root/shared-b.json"
grep -q '"pending_topics_removed":1' "$root/shared-b.json"
[ "$(sqlite3 "$web" "SELECT count(*) FROM source_dirty WHERE token='t-shared';")" = 0 ]
[ "$(sqlite3 "$web" "SELECT count(*) FROM source_dirty_demand WHERE topic='https://example.test/shared';")" = 0 ]

# Running observation is too late to cancel: demand intent is retired but the read continues.
sqlite3 "$web" "INSERT INTO source_dirty(token,topic,frontier,state,dirty_at,demand_count,last_demand_identity) VALUES('t-run','https://example.test/run','MaterialCompiler:observation-demand','running',1,1,'external:run'); INSERT INTO source_dirty_demand VALUES('https://example.test/run','external:run',1,1);"
cancel running external:run
"$bin" apply --cancel-dir "$cancels" --handoff-dir "$handoff" --demand-dir "$demands" --websub-db "$web" --lease-db "$lease" > "$root/running.json"
grep -q '"running_too_late":1' "$root/running.json"
[ "$(sqlite3 "$web" "SELECT state FROM source_dirty WHERE token='t-run';")" = running ]
[ "$(sqlite3 "$web" "SELECT count(*) FROM source_dirty_demand WHERE identity='external:run';")" = 0 ]

echo 'Material lease cancellation -> pending physical work suppression with stale-cancel guard: PASS'
