#!/bin/sh
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
make -C "$root/cmd/BonfyreGraph" >/dev/null
fabric_test_bootstrap

input="$requirement_runtime/graph-source.txt"
printf 'source artifact becomes a durable typed graph operation\n' >"$input"
input_uri=$(fabric_test_ingest "$input" text/plain)
graph_run=$(fabric_test_run_node graph-persist mutate-query command.graph "$input_uri")
graph_uri=$(printf '%s\n' "$graph_run" | sed -n 's/.*output=\([^ ]*\).*/\1/p')
[ -n "$graph_uri" ]
graph_path=$("$requirement_fabric" namespace show "$graph_uri" | sed -n 's/^locator=//p')

python3 - "$graph_path" <<'PY'
import json
import sys

result = None
for line in open(sys.argv[1]):
    if line.startswith('{"atom_exists"'):
        result = json.loads(line)
assert result == {
    "atom_exists": True,
    "operation_exists": True,
    "lineage_correct": True,
    "no_dangling_relation": True,
    "query_identity": True,
    "restart_persisted": True,
}
PY

receipt_id=$(sqlite3 "$requirement_db" "SELECT receipt_id FROM events WHERE mission_id='graph-persist' AND operator_id='command.graph' AND status='complete';")
"$requirement_fabric" receipt show "$receipt_id" | grep -q '"family":"graph_store_query"'
[ "$(sqlite3 "$requirement_db" "SELECT count(*) FROM execution_metrics m JOIN events e ON e.id=m.event_id WHERE e.mission_id='graph-persist' AND m.quality_result='passed';")" -eq 1 ]
echo 'graph persistence: passed'
