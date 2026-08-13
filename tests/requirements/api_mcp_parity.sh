#!/bin/sh
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
fabric_test_bootstrap
trap 'fabric_test_stop_daemon' EXIT HUP INT TERM
fabric_test_start_daemon

curl --silent --fail "http://127.0.0.1:$requirement_port/meta" >"$requirement_runtime/api-meta.json"
"$requirement_fabric" mcp meta-abi >"$requirement_runtime/mcp-meta.json"
python3 - "$requirement_runtime/api-meta.json" "$requirement_runtime/mcp-meta.json" <<'PY'
import json
import sys

api = json.load(open(sys.argv[1]))
mcp = json.load(open(sys.argv[2]))
assert api["catalog_generation"] == mcp["catalog_generation"]
assert api["public_commands"] == mcp["tools"] == 93
assert api["typed_command_contracts"] == mcp["typed_contracts"] == 93
PY

fabric_test_stop_daemon
trap - EXIT HUP INT TERM
echo 'API/MCP generation parity: passed'
