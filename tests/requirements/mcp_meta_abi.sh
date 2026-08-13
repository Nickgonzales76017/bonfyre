#!/bin/sh
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
fabric_test_bootstrap

"$requirement_fabric" mcp meta-abi >"$requirement_runtime/mcp-meta.json"
"$requirement_fabric" mcp tools-list >"$requirement_runtime/mcp-tools.json"
python3 - "$requirement_runtime/mcp-meta.json" "$requirement_runtime/mcp-tools.json" <<'PY'
import json
import sys

meta = json.load(open(sys.argv[1]))
listed = json.load(open(sys.argv[2]))
assert meta["protocolVersion"] == "2025-06-18"
assert meta["serverInfo"]["name"] == "bonfyre"
assert meta["capabilities"] == {"tools": True}
assert meta["tools"] == 93 and meta["typed_contracts"] == 93
assert len(meta["catalog_generation"]) == 64
tools = listed["tools"]
assert len(tools) == 93
assert len({tool["name"] for tool in tools}) == 93
assert all(tool["name"].startswith("command.") for tool in tools)
assert all(tool["inputSchema"] and tool["outputSchema"] for tool in tools)
assert listed["catalog_generation"] == meta["catalog_generation"]
PY

first_generation=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["catalog_generation"])' "$requirement_runtime/mcp-meta.json")
"$requirement_fabric" fabric compile "$requirement_root/bonfyre.workspace.yaff" >/dev/null
second_generation=$("$requirement_fabric" mcp meta-abi | python3 -c 'import json,sys; print(json.load(sys.stdin)["catalog_generation"])')
[ "$first_generation" = "$second_generation" ]
echo 'MCP meta ABI: passed'
