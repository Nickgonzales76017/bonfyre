#!/bin/sh
set -eu

# Proves the existing project MCP transport, not a surface descriptor.  The
# caller supplies an authenticated target so this can exercise a local owner
# or the public TLS edge without committing a bearer token to the repository.
# Gated, and a gate is not a failure: without BONFYRE_MCP_URL there is nothing to test.
if [ -z "${BONFYRE_MCP_URL:-}" ]; then echo 'mcp tunnel: SKIP (set BONFYRE_MCP_URL to the exact MCP URL)'; exit 0; fi
: "${BONFYRE_MCP_TOKEN_FILE:?set a readable file containing the bearer token}"

token=$(tr -d '\r\n' < "$BONFYRE_MCP_TOKEN_FILE")
body='{"jsonrpc":"2.0","id":"mcp-handshake","method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"bonfyre-mcp-tunnel-test","version":"1"}}}'
reply=$(curl -fsS --max-time 20 \
  -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  --data "$body" "$BONFYRE_MCP_URL")

printf '%s' "$reply" | grep -q '"protocolVersion":"2025-06-18"'
printf '%s' "$reply" | grep -q '"name":"bonfyre-project-node"'
printf '%s\n' 'mcp streamable-http handshake: PASS'
