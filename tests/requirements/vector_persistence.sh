#!/bin/sh
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
make -C "$root/cmd/BonfyreMoE" >/dev/null
make -C "$root/cmd/BonfyreVec" >/dev/null
fabric_test_bootstrap

input="$requirement_runtime/vector-source.txt"
printf 'durable semantic vectors preserve nearest-neighbor identity\n' >"$input"
input_uri=$(fabric_test_ingest "$input" text/plain)
embed_run=$(fabric_test_run_node vector-embed embed command.embed "$input_uri")
embedding_uri=$(printf '%s\n' "$embed_run" | sed -n 's/.*output=\([^ ]*\).*/\1/p')
[ -n "$embedding_uri" ]
vector_run=$(fabric_test_run_node vector-store persist-query command.vec "$embedding_uri")
vector_uri=$(printf '%s\n' "$vector_run" | sed -n 's/.*output=\([^ ]*\).*/\1/p')
[ -n "$vector_uri" ]
vector_path=$("$requirement_fabric" namespace show "$vector_uri" | sed -n 's/^locator=//p')

python3 - "$vector_path" <<'PY'
import json
import sys

result = json.load(open(sys.argv[1]))
assert result["backend"] == "native-moe-token-embedding"
assert result["dimensions"] == 2048
assert result["distance_metric"] == "cosine"
assert result["inserted"] and result["updated"] and result["deleted"]
assert result["metadata_filtered"] and result["reference_neighbor"]
assert result["nearest_neighbor"] == "native-reference"
assert result["restart_persisted"]
PY

[ "$(sqlite3 "$requirement_db" "SELECT count(*) FROM events e JOIN receipts r ON r.id=e.receipt_id WHERE e.mission_id IN ('vector-embed','vector-store') AND e.status='complete' AND json_extract(r.payload,'$.quality_result')='passed';")" -eq 2 ]
echo 'vector persistence: passed'
