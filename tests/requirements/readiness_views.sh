#!/bin/sh
# Deterministic Feldera adapter boundary: proves the live readiness views
# (schema version 8) and the `readiness` CLI verb reflect real state
# changes without any external materialization engine. When real Feldera
# is available, only the computation engine behind these view names
# changes -- this is the output contract downstream readers rely on.
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
fabric_test_bootstrap

mission="m-readiness-views"
"$requirement_fabric" mission create "$mission" >/dev/null
"$requirement_fabric" work add "$mission" node-a command.hash >/dev/null

echo "== schema migrated to version 8, views present =="
version=$(sqlite3 "$requirement_db" "SELECT COALESCE(MAX(version),0) FROM schema_migrations;")
[ "$version" -ge 8 ] || { echo "FAIL: workgraph schema not migrated to version 8 (got $version)" >&2; exit 1; }

for view in bf_readiness_mission bf_readiness_lease_pressure bf_readiness_effect_backlog \
            bf_readiness_compensation_backlog bf_readiness_capability; do
  sqlite3 "$requirement_db" "SELECT 1 FROM sqlite_master WHERE type='view' AND name='$view';" \
    | grep -q '^1$' || { echo "FAIL: view $view was not created" >&2; exit 1; }
done

echo "== readiness mission via CLI reflects real node state =="
before=$("$requirement_fabric" readiness mission "$mission")
echo "$before" | grep -q '"total_nodes":1' || { echo "FAIL: unexpected mission readiness: $before" >&2; exit 1; }
echo "$before" | grep -q '"complete_nodes":0' || { echo "FAIL: unexpected mission readiness: $before" >&2; exit 1; }

echo "== induce a real state change: claim the node, confirm running_nodes updates =="
"$requirement_fabric" work claim "$mission" node-a worker-readiness-test --lease-ms 60000 >/dev/null
after=$("$requirement_fabric" readiness mission "$mission")
echo "$after" | grep -q '"running_nodes":1' || { echo "FAIL: readiness view did not reflect claim: $after" >&2; exit 1; }

echo "== induce a real effect, confirm effect-backlog view reflects it =="
"$requirement_fabric" effect request "$mission" derive-file "bonfyre://local/readiness-test.txt" >/dev/null
backlog=$("$requirement_fabric" readiness effect-backlog "$mission")
echo "$backlog" | grep -q '"backlog_count":1' || { echo "FAIL: effect backlog view did not reflect planned effect: $backlog" >&2; exit 1; }

echo "== capability view reflects real compiled catalog (93 command.* bindings expected) =="
capability=$("$requirement_fabric" readiness capability)
bound_count=$(echo "$capability" | python3 -c "import json,sys; d=json.load(sys.stdin); print(sum(1 for r in d['rows'] if r['binding_state']=='bound'))")
[ "$bound_count" -ge 90 ] || { echo "FAIL: expected roughly 93 bound command.* operators, got $bound_count" >&2; exit 1; }

echo "== unknown view and missing args fail cleanly =="
"$requirement_fabric" readiness bogus >/dev/null 2>&1 && { echo "FAIL: readiness accepted an unknown view" >&2; exit 1; }
"$requirement_fabric" readiness >/dev/null 2>&1 && { echo "FAIL: readiness accepted no args" >&2; exit 1; }

echo 'readiness views: passed'
