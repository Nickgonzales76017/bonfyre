#!/bin/sh
# Real Restate <-> WorkGraph durable progression through the native Rust SDK.
# Restate owns journaled run/sleep/resume; the C fabric owns lifecycle, claims,
# effects, receipts, and completion semantics.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
adapter="$root/services/bonfyre-restate-native"
run_root=${BONFYRE_STATE_DIR:-/tmp/bonfyre-restate-integration}
service_pid=''

fail() { echo "FAIL: $*" >&2; exit 1; }
cleanup() {
    [ -n "$service_pid" ] && kill "$service_pid" 2>/dev/null || true
    rm -rf "$run_root"
}
trap cleanup EXIT INT TERM

command -v docker >/dev/null 2>&1 || fail "docker is required for the real Restate integration"
docker info >/dev/null 2>&1 || fail "Docker daemon is required for the real Restate integration"

rm -rf "$run_root"
mkdir -p "$run_root/state"
export BONFYRE_STATE_DIR="$run_root/state"

fabric="$root/programs/bonfyre/bonfyre"
make -C "$root/engine/core" >/dev/null
make -C "$root/programs/bonfyre" >/dev/null
make -C "$root/cmd/BonfyreHash" >/dev/null
make -C "$adapter" release >/dev/null
"$fabric" fabric compile "$root/bonfyre.workspace.yaff" "$root/bonfyre.lock.yaff" \
    "$root/estate/catalog.yaff" "$root/estate/compositions.yaff" \
    "$root/estate/profiles.yaff" "$root/estate/legacy-operators.tsv" >/dev/null

mission="m-restate-integration-$$"
worker_run="worker-run-restate-$$"
protocol_instance="protocol-restate-$$"
"$fabric" mission create "$mission" >/dev/null
"$fabric" work add "$mission" node-a fs.restate.node >/dev/null
"$fabric" work add "$mission" node-b fs.restate.node >/dev/null

BONFYRE_ROOT="$root" BONFYRE_STATE_DIR="$run_root/state" \
    "$adapter/target/release/bonfyre-restate-native" >"$run_root/service.log" 2>&1 &
service_pid=$!
sleep 2

register_out=$(curl -s -X POST http://localhost:9070/deployments -H 'content-type: application/json' \
    -d '{"uri":"http://host.docker.internal:9080","force":true}')
echo "$register_out" | grep -q '"name":"BonfyreWorkGraph"' || fail "native service discovery failed: $register_out"
echo "$register_out" | grep -q '"name":"run_two_nodes"' || fail "native handler discovery failed: $register_out"

send_out=$(curl -s -X POST "http://localhost:8081/BonfyreWorkGraph/run_two_nodes/send" \
    -H 'content-type: application/json' \
    -d "{\"mission_id\":\"$mission\",\"node_a\":\"node-a\",\"node_b\":\"node-b\",\"worker_id\":\"worker-profile-restate\",\"worker_run_id\":\"$worker_run\",\"protocol_instance_id\":\"$protocol_instance\",\"wait_seconds\":15}")
echo "$send_out" | grep -q '"status":"Accepted"' || fail "native invocation was not accepted: $send_out"

sleep 3
state_a=$(sqlite3 "$run_root/state/fabric.db" "SELECT status FROM workgraph_nodes WHERE mission_id='$mission' AND node_id='node-a';")
state_b=$(sqlite3 "$run_root/state/fabric.db" "SELECT status FROM workgraph_nodes WHERE mission_id='$mission' AND node_id='node-b';")
[ "$state_a" = "complete" ] || fail "node-a should complete before the durable wait, got: $state_a"
[ "$state_b" != "complete" ] || fail "node-b completed before Restate's durable wait elapsed"

kill -9 "$service_pid" 2>/dev/null || true
service_pid=''
sleep 2
BONFYRE_ROOT="$root" BONFYRE_STATE_DIR="$run_root/state" \
    "$adapter/target/release/bonfyre-restate-native" >>"$run_root/service.log" 2>&1 &
service_pid=$!

attempts=0
while [ "$(sqlite3 "$run_root/state/fabric.db" "SELECT status FROM workgraph_nodes WHERE mission_id='$mission' AND node_id='node-b';")" != "complete" ]; do
    attempts=$((attempts + 1))
    [ "$attempts" -lt 60 ] || fail "node-b never completed after durable resume"
    sleep 1
done

worker_rows=$(sqlite3 "$run_root/state/fabric.db" "SELECT count(*) FROM worker_runs WHERE run_id='$worker_run' AND mission_id='$mission' AND state='completed';")
protocol_rows=$(sqlite3 "$run_root/state/fabric.db" "SELECT count(*) FROM protocol_instances WHERE instance_id='$protocol_instance' AND worker_run_id='$worker_run' AND state='completed';")
worker_moves=$(sqlite3 "$run_root/state/fabric.db" "SELECT count(*) FROM worker_run_transitions WHERE run_id='$worker_run';")
protocol_moves=$(sqlite3 "$run_root/state/fabric.db" "SELECT count(*) FROM protocol_instance_transitions WHERE instance_id='$protocol_instance';")
[ "$worker_rows" = "1" ] && [ "$protocol_rows" = "1" ] || fail "stable lifecycle did not recover"
[ "$worker_moves" = "3" ] && [ "$protocol_moves" = "4" ] || fail "replay created duplicate lifecycle transitions"
echo "restate native integration: durable two-node recovery passed"
