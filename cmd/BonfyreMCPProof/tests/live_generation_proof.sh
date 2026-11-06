#!/bin/sh
# Read-only verification of a real, manifest-persisted MCP generation receipt.
# This is intentionally not a fixture: callers may set
# BONFYRE_MCP_PROOF_RECEIPT to another completed local generation receipt.
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
receipt=${BONFYRE_MCP_PROOF_RECEIPT:-"$HOME/.bonfyre/moe/receipts/24337a43-fc9f-4112-a50a-a7485370f8af.json"}
make -C "$repo_root/cmd/BonfyreMCPProof" all >/dev/null
result=$("$repo_root/cmd/BonfyreMCPProof/bonfyre-mcp-proof" generation "$receipt")

printf '%s' "$result" | bun "$repo_root/cmd/BonfyreMCPProof/tests/proof-assert.mjs"
