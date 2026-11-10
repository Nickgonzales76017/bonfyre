#!/bin/sh
# Stateless MCP 2026 carrier witness.  This uses tiny executable doubles only
# for its three external owners; the assertion is the adapter's routing law:
# verify native transport admission, compile a native aperture, expose it as a
# resource, and forward only explicitly advertised owner calls to the existing
# exact owner lane.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
adapter="$root/integrations/mcp/bonfyre_mcp_2026_adapter.mjs"
tmp=$(mktemp -d)
adapter_pid=''
upstream_pid=''
cleanup() { [ -z "$adapter_pid" ] || kill "$adapter_pid" 2>/dev/null || true; [ -z "$upstream_pid" ] || kill "$upstream_pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

cat > "$tmp/auth" <<'SH'
#!/bin/sh
case "$3" in
  verify) printf '%s\n' '{"valid":true,"user":{"id":71}}' ;;
  actor-closure-stdin) cat >/dev/null; printf '%s\n' '{"schema":"bonfyre-actor-closure.v1","authenticated":true,"actor_subject":"actor:mcp-fixture","validity_frontier":"Auth:session:71"}' ;;
  authorize) printf '%s\n' '{"authorized":true}' ;;
  *) exit 1 ;;
esac
SH
chmod +x "$tmp/auth"
cat > "$tmp/address" <<'SH'
#!/bin/sh
[ "$1" = mcp-aperture ] || exit 1
printf '%s\n' '{"schema":"bonfyre-mcp-aperture.v1","mcp_protocol_version":"2026-07-28","aperture_digest":"sha256:fixture","resources":[],"resource_templates":[{"identity":"bonfyre://media/library/{library}/catalog","exact_verifier":"MediaLibrary:access"}],"operations":[{"identity":"media_library_catalog","exact_verifier":"MediaLibrary:access"},{"identity":"media_playback_plan","exact_verifier":"MediaLibrary:access"},{"identity":"mcp:tool:owner_exact_read","exact_verifier":"MCPClosure:scope_recheck"}],"interaction_programs":[],"dependency_frontier":["ForeignTwin:https://example.test"],"truth":"advertisement_only_exact_owner_recheck_required"}'
SH
chmod +x "$tmp/address"
cat > "$tmp/media" <<'SH'
#!/bin/sh
case "$1" in
  authenticated-catalog) cat >/dev/null; printf '%s\n' '{"schema":"bonfyre-media-library-catalog.v1","library_id":"fixture-library","items":[{"media_program_ref":"fixture:listen"}],"truth":"library_memberships_reference_media_not_media_identity_or_rights"}' ;;
  authenticated-playback-plan) cat >/dev/null; printf '%s\n' '{"schema":"bonfyre-playback-plan.v1","state":"DIRECT_PLAYABLE","representation_id":"fixture-opus","truth":"access_and_representation_plan_only_no_transport_or_watch_claim"}' ;;
  *) exit 1 ;;
esac
SH
chmod +x "$tmp/media"
cat > "$tmp/upstream.mjs" <<'JS'
import http from 'node:http';
const port = Number(process.argv[2]);
http.createServer((req, res) => {
  let body=''; req.on('data', v => { body += v; }); req.on('end', () => {
    const call=JSON.parse(body);
    const result=call.method === 'tools/list'
      ? {tools:[{name:'owner_exact_read',description:'exact owner read',inputSchema:{type:'object'}},{name:'global_not_advertised',description:'must be hidden',inputSchema:{type:'object'}}]}
      : {owner:'exact-upstream',method:call.method,name:call.params?.name || ''};
    res.writeHead(200, {'content-type':'application/json'});
    res.end(JSON.stringify({jsonrpc:'2.0',id:call.id,result}));
  });
}).listen(port, '127.0.0.1');
JS
set -- $(python3 -c 'import socket; a=socket.socket(); b=socket.socket(); a.bind(("127.0.0.1",0)); b.bind(("127.0.0.1",0)); print(a.getsockname()[1],b.getsockname()[1]); a.close(); b.close()')
upstream_port=$1
port=$2
node "$tmp/upstream.mjs" "$upstream_port" >"$tmp/upstream.log" 2>&1 &
upstream_pid=$!
BONFYRE_MCP_2026_HOST=127.0.0.1 BONFYRE_MCP_2026_PORT="$port" \
BONFYRE_AUTH_BINARY="$tmp/auth" BONFYRE_AUTH_DB="$tmp/auth.db" \
BONFYRE_ADDRESSPLANE_BINARY="$tmp/address" BONFYRE_ADDRESSPLANE_DB="$tmp/address.db" \
BONFYRE_MEDIA_LIBRARY_BINARY="$tmp/media" BONFYRE_MEDIA_LIBRARY_DB="$tmp/media.db" BONFYRE_MEDIA_LIBRARY_CMS_DB="$tmp/cms.db" \
BONFYRE_MCP_2026_UPSTREAM="http://127.0.0.1:$upstream_port/mcp" \
node "$adapter" >"$tmp/adapter.log" 2>&1 &
adapter_pid=$!
for _ in $(seq 1 60); do grep -q 'MCP 2026 aperture' "$tmp/adapter.log" 2>/dev/null && break; sleep 0.05; done
grep -q 'MCP 2026 aperture' "$tmp/adapter.log"
base="http://127.0.0.1:$port/mcp"
meta='"_meta":{"io.modelcontextprotocol/clientInfo":{"name":"bonfyre-test","version":"1"}}'
headers='-H Authorization: Bearer fixture -H Content-Type: application/json -H MCP-Protocol-Version: 2026-07-28'

# Stateless requests are self describing. A client cannot choose a method in
# the body and route it to another owner with a mismatched header.
bad=$(curl -sS -o "$tmp/bad" -w '%{http_code}' -X POST -H 'Authorization: Bearer fixture' -H 'Content-Type: application/json' --data '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' "$base")
[ "$bad" = 400 ]

tools=$(curl -fsS -X POST -H 'Authorization: Bearer fixture' -H 'Content-Type: application/json' -H 'MCP-Protocol-Version: 2026-07-28' -H 'Mcp-Method: tools/list' -H 'Mcp-Name: tools/list' --data "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{$meta}}" "$base")
printf '%s' "$tools" | grep -q 'bonfyre_aperture_refresh'
printf '%s' "$tools" | grep -q 'owner_exact_read'
! printf '%s' "$tools" | grep -q 'global_not_advertised'
printf '%s' "$tools" | grep -q 'media_library_catalog'
printf '%s' "$tools" | grep -q 'media_playback_plan'
printf '%s' "$tools" | grep -q 'sha256:fixture'

resources=$(curl -fsS -X POST -H 'Authorization: Bearer fixture' -H 'Content-Type: application/json' -H 'MCP-Protocol-Version: 2026-07-28' -H 'Mcp-Method: resources/list' -H 'Mcp-Name: resources/list' --data "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"resources/list\",\"params\":{$meta}}" "$base")
printf '%s' "$resources" | grep -q 'bonfyre://closure/current'

templates=$(curl -fsS -X POST -H 'Authorization: Bearer fixture' -H 'Content-Type: application/json' -H 'MCP-Protocol-Version: 2026-07-28' -H 'Mcp-Method: resources/templates/list' -H 'Mcp-Name: resources/templates/list' --data "{\"jsonrpc\":\"2.0\",\"id\":31,\"method\":\"resources/templates/list\",\"params\":{$meta}}" "$base")
printf '%s' "$templates" | grep -q 'bonfyre://media/library/{library}/catalog'

closure=$(curl -fsS -X POST -H 'Authorization: Bearer fixture' -H 'Content-Type: application/json' -H 'MCP-Protocol-Version: 2026-07-28' -H 'Mcp-Method: resources/read' -H 'Mcp-Name: bonfyre://closure/current' --data "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"resources/read\",\"params\":{\"uri\":\"bonfyre://closure/current\",$meta}}" "$base")
printf '%s' "$closure" | grep -q 'advertisement_only_exact_owner_recheck_required'
printf '%s' "$closure" | grep -q 'ForeignTwin:https://example.test'

catalog=$(curl -fsS -X POST -H 'Authorization: Bearer fixture' -H 'Content-Type: application/json' -H 'MCP-Protocol-Version: 2026-07-28' -H 'Mcp-Method: resources/read' -H 'Mcp-Name: bonfyre://media/library/fixture-library/catalog' --data "{\"jsonrpc\":\"2.0\",\"id\":41,\"method\":\"resources/read\",\"params\":{\"uri\":\"bonfyre://media/library/fixture-library/catalog\",$meta}}" "$base")
printf '%s' "$catalog" | grep -q 'fixture:listen'

# The new read-only aperture tool does not need a write scope. A different
# tool is only relayed to the exact upstream owner after header/body binding.
refresh=$(curl -fsS -X POST -H 'Authorization: Bearer fixture' -H 'Content-Type: application/json' -H 'MCP-Protocol-Version: 2026-07-28' -H 'Mcp-Method: tools/call' -H 'Mcp-Name: bonfyre_aperture_refresh' --data "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"tools/call\",\"params\":{\"name\":\"bonfyre_aperture_refresh\",\"arguments\":{},$meta}}" "$base")
printf '%s' "$refresh" | grep -q 'sha256:fixture'
media_plan=$(curl -fsS -X POST -H 'Authorization: Bearer fixture' -H 'Content-Type: application/json' -H 'MCP-Protocol-Version: 2026-07-28' -H 'Mcp-Method: tools/call' -H 'Mcp-Name: media_playback_plan' --data "{\"jsonrpc\":\"2.0\",\"id\":51,\"method\":\"tools/call\",\"params\":{\"name\":\"media_playback_plan\",\"arguments\":{\"library\":\"fixture-library\",\"subject\":\"recording:fixture\",\"capabilities\":{\"codecs\":[\"opus\"]}},$meta}}" "$base")
printf '%s' "$media_plan" | grep -q 'DIRECT_PLAYABLE'
printf '%s' "$media_plan" | grep -q 'no_transport_or_watch_claim'
forwarded=$(curl -fsS -X POST -H 'Authorization: Bearer fixture' -H 'Content-Type: application/json' -H 'MCP-Protocol-Version: 2026-07-28' -H 'Mcp-Method: tools/call' -H 'Mcp-Name: owner_exact_read' --data "{\"jsonrpc\":\"2.0\",\"id\":6,\"method\":\"tools/call\",\"params\":{\"name\":\"owner_exact_read\",\"arguments\":{},$meta}}" "$base")
printf '%s' "$forwarded" | grep -q 'exact-upstream'
refused=$(curl -fsS -X POST -H 'Authorization: Bearer fixture' -H 'Content-Type: application/json' -H 'MCP-Protocol-Version: 2026-07-28' -H 'Mcp-Method: tools/call' -H 'Mcp-Name: global_not_advertised' --data "{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"tools/call\",\"params\":{\"name\":\"global_not_advertised\",\"arguments\":{},$meta}}" "$base")
printf '%s' "$refused" | grep -q 'tool_not_in_current_aperture'

echo 'MCP 2026 aperture: PASS'
