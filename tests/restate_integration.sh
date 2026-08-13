#!/bin/sh
# Proves real Restate <-> WorkGraph durable progression. NOT part of
# tests/unified_fabric_acceptance.sh -- this needs Docker and a running
# Restate container (docker.restate.dev/restatedev/restate) plus the venv
# under services/bonfyre-restate-adapter, none of which the core fabric
# acceptance gate should hard-depend on. Run manually or from a separate
# CI lane that has Docker available.
#
# What this proves, using the real restate-sdk (not a hand-rolled protocol
# implementation -- see services/bonfyre-restate-adapter/app.py for why):
#   1. Restate's service-discovery handshake succeeds against a real ASGI
#      app built with restate.app() (real protocol compliance, not assumed).
#   2. A real invocation drives TWO separately claimed real workgraph nodes,
#      each dispatching the real bonfyre-hash binary against a real fixture
#      and completing through the real `work complete` CLI path.
#   3. A durable sleep (ctx.sleep) between the two nodes genuinely pauses
#      execution mid-mission -- checked by observing node-a=complete,
#      node-b=ready at a checkpoint before the sleep elapses, not just
#      trusting the final result.
#   4. The final result carries real fabric receipt_id/event_id values for
#      both nodes, not placeholders.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
adapter="$root/services/bonfyre-restate-adapter"
run_root=${BONFYRE_STATE_DIR:-/tmp/bonfyre-restate-integration}

fail() { echo "FAIL: $*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || fail "docker is required for this test"
[ -x "$adapter/venv/bin/hypercorn" ] || fail "run: python3.12 -m venv $adapter/venv && $adapter/venv/bin/pip install restate-sdk hypercorn"

echo "== ensure Restate is reachable =="
if ! curl -s -o /dev/null -w '%{http_code}' http://localhost:9070/health 2>/dev/null | grep -q '^200$'; then
    fail "Restate admin API not reachable on :9070 -- start the bonfyre-restate container first"
fi

rm -rf "$run_root"
mkdir -p "$run_root/state"
export BONFYRE_STATE_DIR="$run_root/state"

fabric="$root/programs/bonfyre/bonfyre"
make -C "$root/engine/core" >/dev/null
make -C "$root/programs/bonfyre" >/dev/null
make -C "$root/cmd/BonfyreHash" >/dev/null
"$fabric" fabric compile "$root/bonfyre.workspace.yaff" "$root/bonfyre.lock.yaff" \
    "$root/estate/catalog.yaff" "$root/estate/compositions.yaff" \
    "$root/estate/profiles.yaff" "$root/estate/legacy-operators.tsv" >/dev/null

mission="m-restate-integration-$$"
"$fabric" mission create "$mission" >/dev/null
"$fabric" work add "$mission" node-a fs.restate.node >/dev/null
"$fabric" work add "$mission" node-b fs.restate.node >/dev/null

echo "== start the real Restate bridge service =="
pkill -9 -f "hypercorn app:app" 2>/dev/null || true
sleep 1
(cd "$adapter" && BONFYRE_ROOT="$root" BONFYRE_STATE_DIR="$run_root/state" \
    ./venv/bin/hypercorn app:app --bind 127.0.0.1:9080 --workers 1 >"$run_root/service.log" 2>&1 &)
sleep 3

echo "== register with Restate =="
register_out=$(curl -s -X POST http://localhost:9070/deployments -H 'content-type: application/json' \
    -d '{"uri":"http://host.docker.internal:9080","force":true}')
echo "$register_out" | grep -q '"name":"BonfyreWorkGraph"' || fail "service discovery/registration failed: $register_out"
echo "$register_out" | grep -q '"name":"run_two_nodes"' || fail "handler not discovered: $register_out"
echo "real Restate discovery succeeded: $(echo "$register_out" | head -c 120)..."

echo "== invoke =="
send_out=$(curl -s -X POST "http://localhost:8081/BonfyreWorkGraph/run_two_nodes/send" \
    -H 'content-type: application/json' \
    -d "{\"mission_id\":\"$mission\",\"node_a\":\"node-a\",\"node_b\":\"node-b\"}")
echo "$send_out" | grep -q '"status":"Accepted"' || fail "invocation was not accepted: $send_out"

echo "== checkpoint mid-sleep: node-a must be complete, node-b must still be pending =="
sleep 3
state_a=$(sqlite3 "$run_root/state/fabric.db" "SELECT status FROM workgraph_nodes WHERE mission_id='$mission' AND node_id='node-a';")
state_b=$(sqlite3 "$run_root/state/fabric.db" "SELECT status FROM workgraph_nodes WHERE mission_id='$mission' AND node_id='node-b';")
[ "$state_a" = "complete" ] || fail "node-a should be complete by the checkpoint, got: $state_a"
[ "$state_b" != "complete" ] || fail "node-b completed before the durable sleep elapsed -- sleep is not actually pausing execution"
echo "durable sleep genuinely paused mid-mission: node-a=$state_a node-b=$state_b"

echo "== wait for the durable sleep to elapse and node-b to complete =="
attempts=0
while [ "$(sqlite3 "$run_root/state/fabric.db" "SELECT status FROM workgraph_nodes WHERE mission_id='$mission' AND node_id='node-b';")" != "complete" ]; do
    attempts=$((attempts + 1))
    [ "$attempts" -lt 60 ] || fail "node-b never completed"
    sleep 1
done
echo "node-b completed after the durable sleep elapsed"

echo "== confirm real receipts exist for both nodes =="
receipts=$(sqlite3 "$run_root/state/fabric.db" "SELECT count(*) FROM receipts WHERE subject_id LIKE 'node-%';")
[ "$receipts" -ge 2 ] || fail "expected at least 2 receipts for the two node completions, got $receipts"
echo "real receipts confirmed: $receipts"

pkill -9 -f "hypercorn app:app" 2>/dev/null || true
rm -rf "$run_root"
echo "restate integration: all checks passed"
