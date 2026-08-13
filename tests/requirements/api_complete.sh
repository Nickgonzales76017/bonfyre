#!/bin/sh
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
fabric_test_bootstrap

mission=api-workgraph
node=durable-node
"$requirement_fabric" mission create "$mission" >/dev/null
"$requirement_fabric" work add "$mission" "$node" core.intake --family api >/dev/null
claim=$("$requirement_fabric" work claim-next api-worker --family api --lease-ms 30000)
token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
"$requirement_fabric" work complete "$mission" "$node" api-worker "$token" >/dev/null

trap 'fabric_test_stop_daemon' EXIT HUP INT TERM
fabric_test_start_daemon
base="http://127.0.0.1:$requirement_port"
curl --silent --fail "$base/health" >"$requirement_runtime/api-health.json"
curl --silent --fail "$base/meta" >"$requirement_runtime/api-meta.json"
curl --silent --fail "$base/catalog" >"$requirement_runtime/api-catalog.json"
curl --silent --fail "$base/mission/$mission" >"$requirement_runtime/api-mission.json"
missing_status=$(curl --silent --output "$requirement_runtime/api-missing.json" --write-out '%{http_code}' "$base/mission/missing")
[ "$missing_status" = 404 ]

python3 - "$requirement_runtime" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
health = json.loads((root / "api-health.json").read_text())
meta = json.loads((root / "api-meta.json").read_text())
catalog = json.loads((root / "api-catalog.json").read_text())["catalog"]
mission = json.loads((root / "api-mission.json").read_text())
missing = json.loads((root / "api-missing.json").read_text())
assert health == {"service": "bonfyred", "state": "ready"}
assert meta["api_version"] == "v1"
assert len(meta["catalog_generation"]) == 64
assert meta["public_commands"] == 93
assert meta["typed_command_contracts"] == 93
assert catalog["operators"] == 98 and catalog["public_commands"] == 93
assert mission["mission"] == "api-workgraph"
assert mission["status"] == "complete"
assert mission["nodes"] == 1 and mission["completed_nodes"] == 1
assert missing == {"error": "not_found"}
PY

fabric_test_stop_daemon
trap - EXIT HUP INT TERM
echo 'API contract: passed'
