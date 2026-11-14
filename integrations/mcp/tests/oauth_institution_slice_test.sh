#!/bin/sh
# What this does not prove:
# - a script written in this test stands where a program would run, so it shows how the caller handles that program's interface and nothing about the program.
# - a server started in this test answers in place of the outside service, so nothing here is evidence about that service.
# OAuth MCP carrier witness: generic write scope is not a project-MCP grant.
# A delegated session gets only the closure-compiled institutional tools, while
# a live Bonfyre admin remains eligible for the full upstream owner.
set -eu

root=$(CDPATH='' cd -- "$(dirname -- "$0")/../../.." && pwd)
gateway="$root/integrations/mcp/bonfyre_oauth_gateway.mjs"
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
upper90="$root/cmd/BonfyreUpper90/bonfyre-upper90"
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
provider="$root/cmd/BonfyreProvider/bonfyre-provider"
[ -x "$auth" ] || { echo 'mcp institution slice: SKIP (auth not built)'; exit 0; }
[ -x "$upper90" ] || { echo 'mcp institution slice: SKIP (upper90 not built)'; exit 0; }
[ -x "$cms" ] || make -C "$root/cmd/BonfyreCMS" all >/dev/null
[ -x "$provider" ] || make -C "$root/cmd/BonfyreProvider" all >/dev/null

tmp=$(mktemp -d)
pid=''
upstream_pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; [ -n "$upstream_pid" ] && kill "$upstream_pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

authdb="$tmp/auth.db"
u90db="$tmp/upper90.db"
cmsdb="$tmp/cms.db"
providerdb="$tmp/provider.db"
cms_schemas="$root/cmd/BonfyreCMS/content-types"
foreign_root="$tmp/foreign"
tokenfile="$tmp/project.token"
port=18917
printf '%s\n' 'fixture-project-token' > "$tokenfile"
"$cms" schema migrate --db "$cmsdb" --schemas "$cms_schemas" >/dev/null

"$auth" --db "$authdb" signup --email slice@t.test --name Slice --password slice-fixture-passphrase --tier pro >/dev/null
uid=$(sqlite3 "$authdb" "select id from users where email='slice@t.test';")
"$auth" --db "$authdb" institution-grant --user-id "$uid" --institution jrss --role write >/dev/null
"$upper90" --db "$u90db" grant --actor "auth-user:$uid" --estate jrss --role jrss-stable-owner --scope stable:cedar --evidence receipt:fixture >/dev/null
token=$("$auth" --db "$authdb" login --email slice@t.test --password slice-fixture-passphrase | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
[ -n "$token" ] || { echo 'mcp institution slice: no fixture session'; exit 1; }

# A minimal upstream lets the owner lane prove it is forwarded rather than
# accidentally receiving the delegated tool list. The gateway is the subject
# of this test; the full project owner is represented only by this receipt.
node -e 'require("http").createServer((q,s)=>{let b="";q.on("data",c=>b+=c);q.on("end",()=>{const request=JSON.parse(b||"{}");const receipt={owner_upstream:true,path:q.url,operation:request.params?.name,arguments:request.params?.arguments,credential_matches:q.headers.authorization==="Bearer fixture-project-token"};console.log(JSON.stringify({id:request.id,...receipt}));s.writeHead(200,{"content-type":"application/json"});s.end(JSON.stringify({jsonrpc:"2.0",id:request.id,result:{...receipt,...(request.method==="tools/list"?{tools:[]}:request.method==="resources/list"?{resources:[{uri:"fixture://upstream",name:"Upstream",mimeType:"application/json"}]}:request.method==="initialize"?{capabilities:{tools:{}}}:{})}}));});}).listen(19999,"127.0.0.1")' >"$tmp/upstream.log" 2>&1 &
upstream_pid=$!

bun "$root/components/BonfyreServiceSite/workbook-mcp-build.mjs" "$tmp/review.html" >/dev/null
bun "$root/components/BonfyreServiceSite/connected-measurement-mcp-build.mjs" "$tmp/connected.html" >/dev/null
BONFYRE_WORKBOOK_MCP_APP_FILE="$tmp/review.html" \
BONFYRE_CONNECTED_MCP_APP_FILE="$tmp/connected.html" \
BONFYRE_AUTH_BINARY="$auth" BONFYRE_AUTH_DB="$authdb" \
  BONFYRE_UPPER90_BINARY="$upper90" BONFYRE_UPPER90_DB="$u90db" \
  BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$cms_schemas" BONFYRE_FOREIGN_ROOT="$foreign_root" \
  BONFYRE_PROVIDER_BINARY="$provider" BONFYRE_PROVIDER_DB="$providerdb" \
  BONFYRE_PROJECT_MCP_TOKEN_FILE="$tokenfile" BONFYRE_OAUTH_MCP_PORT="$port" \
  BONFYRE_MCP_PUBLIC_ORIGIN='https://mcp.fixture.test' BONFYRE_AUTH_PUBLIC_ORIGIN='https://auth.fixture.test' \
  BONFYRE_PROJECT_MCP_URL='http://127.0.0.1:19999/mcp' \
  node "$gateway" >"$tmp/gateway.log" 2>&1 &
pid=$!
sleep 1
base="http://127.0.0.1:$port/mcp"
auth_md=$(curl -fsS "http://127.0.0.1:$port/auth.md")
printf '%s' "$auth_md" | grep -q 'Aurekai Bonfyre agent authentication'
printf '%s' "$auth_md" | grep -q 'protected resource'
printf '%s' "$auth_md" | grep -q 'https://mcp.fixture.test/mcp'
printf '%s' "$auth_md" | grep -q 'https://auth.fixture.test/.well-known/oauth-authorization-server'
metadata=$(curl -fsS "http://127.0.0.1:$port/.well-known/oauth-protected-resource/mcp")
printf '%s' "$metadata" | grep -q '"resource":"https://mcp.fixture.test/mcp"'
printf '%s' "$metadata" | grep -q '"authorization_servers":\["https://auth.fixture.test"\]'
issuer=$(curl -fsS "http://127.0.0.1:$port/.well-known/oauth-authorization-server")
printf '%s' "$issuer" | grep -q '"issuer":"https://auth.fixture.test"'
if printf '%s' "$auth_md" | grep -Eqi 'password[[:space:]]*='; then
  echo 'mcp institution slice: auth.md exposed credential material'; exit 1
fi
openid=$(curl -fsS "http://127.0.0.1:$port/.well-known/openid-configuration")
printf '%s' "$openid" | grep -q 'authorization_endpoint'
if printf '%s' "$openid" | grep -q '"openid"'; then
  echo 'mcp institution slice: OAuth discovery alias falsely advertised OpenID'; exit 1
fi

# A free/self account receives a real actor-local MCP slice instead of an
# empty institutional tool set or the global project owner. Transport OAuth
# may say read/write, but the native self scope remains the hard boundary.
"$auth" --db "$authdb" signup --email self@t.test --name Self --password self-fixture-passphrase >/dev/null
self_uid=$(sqlite3 "$authdb" "select id from users where email='self@t.test';")
self_token=$("$auth" --db "$authdb" login --email self@t.test --password self-fixture-passphrase | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
[ -n "$self_token" ] || { echo 'mcp institution slice: no self fixture session'; exit 1; }
self_init=$(curl -fsS -X POST -H "Authorization: Bearer $self_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":90,"method":"initialize","params":{}}' "$base")
printf '%s' "$self_init" | grep -q 'bonfyre-self-slice'
self_tools=$(curl -fsS -X POST -H "Authorization: Bearer $self_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":91,"method":"tools/list","params":{}}' "$base")
printf '%s' "$self_tools" | grep -q 'self_closure'
printf '%s' "$self_tools" | grep -q 'self_connected_measurements'
printf '%s' "$self_tools" | grep -q 'self_foreign_observe'
if printf '%s' "$self_tools" | grep -q 'institution_cms_create\|project_status\|native_tool_invoke\|bonfyre_admin_'; then
  echo 'mcp institution slice: self account received institutional/global tools'; exit 1
fi
self_closure=$(curl -fsS -X POST -H "Authorization: Bearer $self_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":92,"method":"tools/call","params":{"name":"self_closure","arguments":{}}}' "$base")
printf '%s' "$self_closure" | grep -q "auth-user:$self_uid"
printf '%s' "$self_closure" | grep -q 'actor-'

# Composing Spines into Project must not export its proof-writing execution
# operations into the self aperture. Record upstream calls as well as checking
# the refusal: a denial response after forwarding would already be too late.
self_spines=$(curl -fsS -X POST -H "Authorization: Bearer $self_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":921,"method":"tools/call","params":{"name":"r6_helix_reduce","arguments":{"helix_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}' "$base")
printf '%s' "$self_spines" | grep -q 'tool_not_in_current_self_slice'
printf '%s' "$self_spines" | grep -q '"isError":true'
if printf '%s' "$self_tools" | grep -q 'r6_helix_reduce' || grep -q 'r6_helix_reduce' "$tmp/upstream.log"; then
  echo 'mcp institution slice: self Spines call reached the global owner'; exit 1
fi

# Modern MCP is stateless and header-routed. The closure is projected from the
# current auth slice rather than copied into a second MCP state store.
modern_discover=$(curl -fsS -X POST -H "Authorization: Bearer $self_token" -H 'Content-Type: application/json' \
  -H 'MCP-Protocol-Version: 2026-07-28' -H 'Mcp-Method: server/discover' \
  --data '{"jsonrpc":"2.0","id":93,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}' "$base")
printf '%s' "$modern_discover" | grep -q '2026-07-28'
printf '%s' "$modern_discover" | grep -q 'io.modelcontextprotocol/serverInfo'
modern_resources=$(curl -fsS -X POST -H "Authorization: Bearer $self_token" -H 'Content-Type: application/json' \
  -H 'MCP-Protocol-Version: 2026-07-28' -H 'Mcp-Method: resources/list' \
  --data '{"jsonrpc":"2.0","id":94,"method":"resources/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}}' "$base")
printf '%s' "$modern_resources" | grep -q 'bonfyre://closure/actor/'
printf '%s' "$modern_resources" | grep -q 'ui://aurekai/connected-performance/v1.html'
printf '%s' "$modern_resources" | grep -q 'aurekai/addressApertureDigest'
printf '%s' "$modern_resources" | grep -q '"cacheScope":"private"'

# A paid write session may initialize/read tools. It must not fail merely
# because BonfyreAuth stores the highest session scope as write rather than a
# redundant separate read scope.
init=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' "$base")
printf '%s' "$init" | grep -q 'bonfyre-institution-slice'
tools=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' "$base")
printf '%s' "$tools" | grep -q 'institution_closures'
printf '%s' "$tools" | grep -q 'institution_cms_create'
printf '%s' "$tools" | grep -q 'institution_connected_measurements'
if printf '%s' "$tools" | grep -q 'project_status\|native_tool_invoke\|bonfyre_admin_'; then
  echo 'mcp institution slice: delegated user received global project tools'; exit 1
fi
closure=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"institution_closures","arguments":{}}}' "$base")
printf '%s' "$closure" | grep -q 'stable:cedar'
cross_tenant=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":417,"method":"tools/call","params":{"name":"institution_connected_measurements","arguments":{"estate":"not-in-closure"}}}' "$base")
printf '%s' "$cross_tenant" | grep -q 'estate_not_in_current_closure'
printf '%s' "$closure" | grep -q 'jrss'
if printf '%s' "$closure" | grep -q 'ai-loss\|leads'; then
  echo 'mcp institution slice: sibling estate leaked into JRSS closure'; exit 1
fi
institution_spines=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":301,"method":"tools/call","params":{"name":"r6_helix_reduce","arguments":{"helix_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}' "$base")
printf '%s' "$institution_spines" | grep -q 'tool_not_in_current_institution_slice'
printf '%s' "$institution_spines" | grep -q '"isError":true'
if printf '%s' "$tools" | grep -q 'r6_helix_reduce' || grep -q 'r6_helix_reduce' "$tmp/upstream.log"; then
  echo 'mcp institution slice: institutional Spines call reached the global owner'; exit 1
fi

# Exercise an actual native owner through the carrier. This is not a claimed
# tool catalog: the MCP write creates a declared CMS row, and the MCP read can
# see it only through the same freshly compiled JRSS namespace.
created=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":31,"method":"tools/call","params":{"name":"institution_cms_create","arguments":{"estate":"jrss","type":"wiki_page","document":{"title":"MCP private brief","route":"mcp-private-brief","content":"Scoped owner content","published":false,"allow_guest":false}}}}' "$base")
printf '%s' "$created" | grep -q '"created":true'
sqlite3 "$cmsdb" "select namespace from wiki_page where title='MCP private brief';" | grep -q '^private:jrss:stable:cedar$'
listed=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":32,"method":"tools/call","params":{"name":"institution_cms_list","arguments":{"estate":"jrss","type":"wiki_page"}}}' "$base")
printf '%s' "$listed" | grep -q 'MCP private brief'
if printf '%s' "$listed" | grep -q 'actor-\|ai-loss\|leads'; then
  echo 'mcp institution slice: CMS tool exposed a generic or sibling namespace'; exit 1
fi

# Upper90 leadership is a live parent delegation. It exposes each child as an
# inherited aperture with that child's institution scope—not the direct JRSS
# stable:cedar namespace and not a global all-client tool set.
"$auth" --db "$authdb" signup --email leadership@t.test --name Leadership --password leadership-fixture-passphrase --tier enterprise >/dev/null
lead_uid=$(sqlite3 "$authdb" "select id from users where email='leadership@t.test';")
"$auth" --db "$authdb" institution-grant --user-id "$lead_uid" --institution upper90 --role write >/dev/null
"$upper90" --db "$u90db" grant --actor "auth-user:$lead_uid" --estate upper90 --role upper90-leadership --scope institution:upper90 --evidence receipt:leadership >/dev/null
lead_token=$("$auth" --db "$authdb" login --email leadership@t.test --password leadership-fixture-passphrase | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
lead_closures=$(curl -fsS -X POST -H "Authorization: Bearer $lead_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":41,"method":"tools/call","params":{"name":"institution_closures","arguments":{}}}' "$base")
for inherited in jrss ai-loss leads; do printf '%s' "$lead_closures" | grep -q "\"estate\":\"$inherited\""; done
printf '%s' "$lead_closures" | grep -q '"scope":"institution:jrss"'
lead_tools=$(curl -fsS -X POST -H "Authorization: Bearer $lead_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":401,"method":"tools/list","params":{}}' "$base")
if printf '%s' "$lead_tools" | grep -q 'bonfyre_admin_provider_history'; then
  echo 'institutional account discovered administrator provider history' >&2; exit 1
fi
lead_created=$(curl -fsS -X POST -H "Authorization: Bearer $lead_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":42,"method":"tools/call","params":{"name":"institution_cms_create","arguments":{"estate":"jrss","type":"wiki_page","document":{"title":"Leadership MCP brief","route":"leadership-mcp-brief","content":"Inherited parent content","published":false,"allow_guest":false}}}}' "$base")
printf '%s' "$lead_created" | grep -q '"created":true'
sqlite3 "$cmsdb" "select namespace from wiki_page where title='Leadership MCP brief';" | grep -q '^private:jrss:institution:jrss$'
if sqlite3 "$cmsdb" "select namespace from wiki_page where title='Leadership MCP brief';" | grep -q 'stable:cedar'; then
  echo 'mcp institution slice: leadership write merged into child direct scope'; exit 1
fi

# The Bonfyre superuser is intentionally different: their live bonfyre/admin
# authority retains the complete project MCP rather than being reduced to a
# child or client slice.
"$auth" --db "$authdb" signup --email owner@t.test --name Owner --password owner-fixture-passphrase --tier enterprise >/dev/null
owner_uid=$(sqlite3 "$authdb" "select id from users where email='owner@t.test';")
"$auth" --db "$authdb" institution-grant --user-id "$owner_uid" --institution bonfyre --role admin >/dev/null
owner_token=$("$auth" --db "$authdb" login --email owner@t.test --password owner-fixture-passphrase | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
owner=$(curl -fsS -X POST -H "Authorization: Bearer $owner_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":4,"method":"tools/list","params":{}}' "$base")
printf '%s' "$owner" | grep -q 'owner_upstream'
printf '%s' "$owner" | grep -q 'bonfyre_admin_users'
printf '%s' "$owner" | grep -q 'bonfyre_admin_cms_create'
printf '%s' "$owner" | grep -q 'bonfyre_admin_external_descriptors'
printf '%s' "$owner" | grep -q 'bonfyre_admin_provider_materializations'
printf '%s' "$owner" | grep -q 'bonfyre_admin_provider_history'
printf '%s' "$owner" | grep -q 'bonfyre_admin_connected_measurements'

# Embedded resources share the personal/admin boundary, while upstream
# resources survive the merge. The HTML contains no account data.
owner_resources=$(curl -fsS -X POST -H "Authorization: Bearer $owner_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":410,"method":"resources/list","params":{}}' "$base")
printf '%s' "$owner_resources" | grep -q 'fixture://upstream'
printf '%s' "$owner_resources" | grep -q 'ui://aurekai/financial-review/v1.html'
printf '%s' "$owner_resources" | grep -q 'ui://aurekai/connected-performance/v1.html'
owner_ui=$(curl -fsS -X POST -H "Authorization: Bearer $owner_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":411,"method":"resources/read","params":{"uri":"ui://aurekai/financial-review/v1.html"}}' "$base")
printf '%s' "$owner_ui" | grep -q 'text/html;profile=mcp-app'
owner_connected=$(curl -fsS -X POST -H "Authorization: Bearer $owner_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":415,"method":"resources/read","params":{"uri":"ui://aurekai/connected-performance/v1.html"}}' "$base")
printf '%s' "$owner_connected" | grep -q 'text/html;profile=mcp-app'
printf '%s' "$owner_connected" | grep -q 'Connected Performance'
owner_init=$(curl -fsS -X POST -H "Authorization: Bearer $owner_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":412,"method":"initialize","params":{}}' "$base")
printf '%s' "$owner_init" | bun -e 'const r=(await Bun.stdin.json()).result;if(!r.owner_upstream||!r.capabilities.tools||!r.capabilities.resources)throw Error("admin capabilities were not preserved")'
private_resources=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":413,"method":"resources/list","params":{}}' "$base")
printf '%s' "$private_resources" | grep -q 'ui://aurekai/connected-performance/v1.html'
if printf '%s' "$private_resources" | grep -q 'ui://aurekai/financial-review'; then
  echo 'mcp institution slice: delegated account received financial UI'; exit 1
fi
private_ui=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":414,"method":"resources/read","params":{"uri":"ui://aurekai/financial-review/v1.html"}}' "$base")
printf '%s' "$private_ui" | grep -q '"isError":true'
private_connected=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":416,"method":"resources/read","params":{"uri":"ui://aurekai/connected-performance/v1.html"}}' "$base")
printf '%s' "$private_connected" | grep -q 'text/html;profile=mcp-app'


# The admin closure uses the same existing Project upstream and exact Spines
# operation, not an alternate anonymous endpoint. This witnesses transport
# admission only: the mocked owner does not claim to execute the kernel.
admin_spines=$(curl -fsS -X POST -H "Authorization: Bearer $owner_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":401,"method":"tools/call","params":{"name":"r6_helix_reduce","arguments":{"helix_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}' "$base")
printf '%s' "$admin_spines" | bun -e 'const reply=await Bun.stdin.json();const r=reply.result;if(reply.id!==401||r?.owner_upstream!==true||r.path!=="/mcp"||r.operation!=="r6_helix_reduce"||r.credential_matches!==true||JSON.stringify(r.arguments)!==JSON.stringify({helix_id:"a".repeat(64)}))throw Error("admin Spines forwarding contract failed")'
[ "$(grep -c 'r6_helix_reduce' "$tmp/upstream.log")" -eq 1 ] || { echo 'mcp institution slice: unexpected Spines upstream call count'; exit 1; }

# The same catalog-bound V6 bridge remains administrative, even for reads.
for private_token in "$self_token" "$token"; do
  private_v6=$(curl -fsS -X POST -H "Authorization: Bearer $private_token" -H 'Content-Type: application/json' \
    --data '{"jsonrpc":"2.0","id":402,"method":"tools/call","params":{"name":"v6_status","arguments":{}}}' "$base")
  printf '%s' "$private_v6" | grep -q '"isError":true'
done
if grep -q 'v6_status' "$tmp/upstream.log"; then
  echo 'mcp institution slice: V6 escaped private closure'; exit 1
fi
admin_v6=$(curl -fsS -X POST -H "Authorization: Bearer $owner_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":403,"method":"tools/call","params":{"name":"v6_status","arguments":{}}}' "$base")
printf '%s' "$admin_v6" | bun -e 'const r=(await Bun.stdin.json()).result;if(r?.owner_upstream!==true||r.operation!=="v6_status"||r.credential_matches!==true)throw Error("admin V6 forwarding failed")'
[ "$(grep -c 'v6_status' "$tmp/upstream.log")" -eq 1 ] || { echo 'mcp institution slice: V6 forwarding count'; exit 1; }

# Delivery status and submissions use the same administrative upstream. A
# personal or institution session cannot read the developer task history.
for private_token in "$self_token" "$token"; do
  for operation in fabric_status implementation_relay task_status task_cancel recent_receipts; do
    denied_delivery=$(curl -fsS -X POST -H "Authorization: Bearer $private_token" -H 'Content-Type: application/json' \
      --data "{\"jsonrpc\":\"2.0\",\"id\":404,\"method\":\"tools/call\",\"params\":{\"name\":\"$operation\",\"arguments\":{}}}" "$base")
    printf '%s' "$denied_delivery" | grep -q '"isError":true'
  done
done
if grep -Eq 'fabric_status|implementation_relay|task_status|task_cancel|recent_receipts' "$tmp/upstream.log"; then
  echo 'mcp institution slice: delivery escaped private closure'; exit 1
fi
for operation in fabric_status task_status recent_receipts task_cancel implementation_relay; do
  admin_delivery=$(curl -fsS -X POST -H "Authorization: Bearer $owner_token" -H 'Content-Type: application/json' \
    --data "{\"jsonrpc\":\"2.0\",\"id\":405,\"method\":\"tools/call\",\"params\":{\"name\":\"$operation\",\"arguments\":{\"fixture\":true}}}" "$base")
  printf '%s' "$admin_delivery" | EXPECTED_DELIVERY_OPERATION="$operation" bun -e 'const r=(await Bun.stdin.json()).result;if(r?.owner_upstream!==true||r.operation!==process.env.EXPECTED_DELIVERY_OPERATION||r.credential_matches!==true||r.arguments?.fixture!==true)throw Error("admin delivery forwarding failed")'
  [ "$(grep -c "\"operation\":\"$operation\"" "$tmp/upstream.log")" -eq 1 ] || { echo 'mcp institution slice: delivery forwarding count'; exit 1; }
done

# A delegated product aperture does not become the account workspace aperture.
for operation in workspace_list workspace_records workspace_views workspace_preview workspace_update; do
  denied_workspace=$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
    --data "{\"jsonrpc\":\"2.0\",\"id\":406,\"method\":\"tools/call\",\"params\":{\"name\":\"$operation\",\"arguments\":{}}}" "$base")
  printf '%s' "$denied_workspace" | grep -q 'tool_not_in_current_institution_slice'
done

# The full Bonfyre admin closure has first-class control of canonical users,
# levels, institution admission, and CMS copy. These calls are intercepted by
# the carrier and delegated to their native owners; the fixture upstream never
# sees or owns this administrative state.
admin_users=$(curl -fsS -X POST -H "Authorization: Bearer $owner_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":51,"method":"tools/call","params":{"name":"bonfyre_admin_users","arguments":{}}}' "$base")
printf '%s' "$admin_users" | grep -q 'self@t.test'
if printf '%s' "$admin_users" | grep -Eq 'password_hash|password_salt|"token"'; then
  echo 'mcp institution slice: admin user control leaked credential material'; exit 1
fi
admin_update=$(curl -fsS -X POST -H "Authorization: Bearer $owner_token" -H 'Content-Type: application/json' \
  --data "{\"jsonrpc\":\"2.0\",\"id\":52,\"method\":\"tools/call\",\"params\":{\"name\":\"bonfyre_admin_user_update\",\"arguments\":{\"user_id\":$self_uid,\"tier\":\"pro\"}}}" "$base")
printf '%s' "$admin_update" | grep -q 'BonfyreAuth'
sqlite3 "$authdb" "select tier from users where id=$self_uid;" | grep -q '^pro$'
# Tier changes burn the old session because its scope was compiled from the
# prior tier. Re-authenticate before checking a fresh delegated closure.
self_token=$($auth --db "$authdb" login --email self@t.test --password self-fixture-passphrase | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
[ -n "$self_token" ] || { echo 'mcp institution slice: no fresh self session after tier change'; exit 1; }
admin_grant=$(curl -fsS -X POST -H "Authorization: Bearer $owner_token" -H 'Content-Type: application/json' \
  --data "{\"jsonrpc\":\"2.0\",\"id\":53,\"method\":\"tools/call\",\"params\":{\"name\":\"bonfyre_admin_institution_grant\",\"arguments\":{\"user_id\":$self_uid,\"institution\":\"leads\",\"role\":\"read\"}}}" "$base")
printf '%s' "$admin_grant" | grep -q 'BonfyreAuth'
"$auth" --db "$authdb" institution-authorize --token "$self_token" --institution leads --role read | grep -q '"authorized":true'
admin_revoke=$(curl -fsS -X POST -H "Authorization: Bearer $owner_token" -H 'Content-Type: application/json' \
  --data "{\"jsonrpc\":\"2.0\",\"id\":54,\"method\":\"tools/call\",\"params\":{\"name\":\"bonfyre_admin_institution_revoke\",\"arguments\":{\"user_id\":$self_uid,\"institution\":\"leads\"}}}" "$base")
printf '%s' "$admin_revoke" | grep -q 'BonfyreAuth'
"$auth" --db "$authdb" institution-authorize --token "$self_token" --institution leads --role read | grep -q '"authorized":false'
admin_sessions=$(curl -fsS -X POST -H "Authorization: Bearer $owner_token" -H 'Content-Type: application/json' \
  --data "{\"jsonrpc\":\"2.0\",\"id\":541,\"method\":\"tools/call\",\"params\":{\"name\":\"bonfyre_admin_sessions_revoke\",\"arguments\":{\"user_id\":$self_uid}}}" "$base")
printf '%s' "$admin_sessions" | grep -q 'BonfyreAuth'
if "$auth" --db "$authdb" verify --token "$self_token" >/dev/null 2>&1; then
  echo 'mcp institution slice: admin session revoke retained a live session'; exit 1
fi
admin_copy=$(curl -fsS -X POST -H "Authorization: Bearer $owner_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":55,"method":"tools/call","params":{"name":"bonfyre_admin_cms_create","arguments":{"type":"wiki_page","document":{"title":"Superuser repair copy","route":"superuser-repair-copy","content":"Canonical admin copy","published":false,"allow_guest":false}}}}' "$base")
printf '%s' "$admin_copy" | grep -q 'BonfyreCMS'
sqlite3 "$cmsdb" "select title from wiki_page where title='Superuser repair copy';" | grep -q '^Superuser repair copy$'

# Update and delete address the same namespace that list and create do.
# Before, they omitted --ns, so a row in any namespace other than root could be
# read and created through the gateway but never updated ("no row in root").
admin_call() { # id, tool, arguments-json
  curl -fsS -X POST -H "Authorization: Bearer $owner_token" -H 'Content-Type: application/json' \
    --data "{\"jsonrpc\":\"2.0\",\"id\":$1,\"method\":\"tools/call\",\"params\":{\"name\":\"$2\",\"arguments\":$3}}" "$base"
}
admin_call 551 bonfyre_admin_cms_create '{"type":"wiki_page","namespace":"draft-fixture","document":{"title":"Draft copy","route":"draft-copy","content":"v1","published":false,"allow_guest":false}}' | grep -q 'draft-fixture'
draft_id=$(sqlite3 "$cmsdb" "select id from wiki_page where namespace='draft-fixture' and title='Draft copy';")
root_id=$(sqlite3 "$cmsdb" "select id from wiki_page where namespace='root' and title='Superuser repair copy';")
[ -n "$draft_id" ] && [ -n "$root_id" ] || { echo 'mcp institution slice: namespace fixture rows missing'; exit 1; }
admin_call 552 bonfyre_admin_cms_update "{\"type\":\"wiki_page\",\"namespace\":\"draft-fixture\",\"id\":$draft_id,\"document\":{\"content\":\"v2\"}}" | grep -q 'updated'
sqlite3 "$cmsdb" "select content from wiki_page where id=$draft_id;" | grep -q '^v2$' || { echo 'mcp institution slice: non-root update did not land'; exit 1; }
# Root stays the default, so existing callers that omit the namespace keep working.
admin_call 553 bonfyre_admin_cms_update "{\"type\":\"wiki_page\",\"id\":$root_id,\"document\":{\"content\":\"Canonical admin copy v2\"}}" | grep -q 'updated'
sqlite3 "$cmsdb" "select content from wiki_page where id=$root_id;" | grep -q '^Canonical admin copy v2$' || { echo 'mcp institution slice: root-default update did not land'; exit 1; }
# A namespace is a scope, not a hint: the wrong one refuses and changes nothing.
admin_call 554 bonfyre_admin_cms_update "{\"type\":\"wiki_page\",\"id\":$draft_id,\"document\":{\"content\":\"leak\"}}" | grep -Eq 'updated[^a-z]{1,4}true' && { echo 'mcp institution slice: root update reached a draft row'; exit 1; }
sqlite3 "$cmsdb" "select content from wiki_page where id=$draft_id;" | grep -q '^v2$' || { echo 'mcp institution slice: refused update changed the row'; exit 1; }
admin_call 555 bonfyre_admin_cms_delete "{\"type\":\"wiki_page\",\"id\":$draft_id}" | grep -Eq 'deleted[^a-z]{1,4}true' && { echo 'mcp institution slice: root delete removed a draft row'; exit 1; }
[ "$(sqlite3 "$cmsdb" "select count(*) from wiki_page where id=$draft_id;")" = 1 ] || { echo 'mcp institution slice: refused delete removed the row'; exit 1; }
admin_call 556 bonfyre_admin_cms_update '{"type":"wiki_page","namespace":"bad ns!","id":1,"document":{}}' | grep -Eq 'updated[^a-z]{1,4}true' && { echo 'mcp institution slice: invalid namespace accepted'; exit 1; }
# The same owner supports older tool schema bindings with a reserved nested
# namespace, while still refusing cross-namespace mutation and invalid input.
admin_call 558 bonfyre_admin_cms_update "{\"type\":\"wiki_page\",\"id\":$draft_id,\"document\":{\"__namespace\":\"draft-fixture\",\"content\":\"v3\"}}" | grep -q 'updated'
sqlite3 "$cmsdb" "select content from wiki_page where id=$draft_id;" | grep -q '^v3$' || { echo 'mcp institution slice: cached-schema update did not land'; exit 1; }
admin_call 559 bonfyre_admin_cms_update "{\"type\":\"wiki_page\",\"namespace\":\"root\",\"id\":$draft_id,\"document\":{\"__namespace\":\"draft-fixture\",\"content\":\"leak\"}}" | grep -Eq 'updated[^a-z]{1,4}true' && { echo 'mcp institution slice: conflicting scopes reached native CMS'; exit 1; }
admin_call 560 bonfyre_admin_cms_update "{\"type\":\"wiki_page\",\"id\":$draft_id,\"document\":{\"__namespace\":\"bad ns!\",\"content\":\"leak\"}}" | grep -Eq 'updated[^a-z]{1,4}true' && { echo 'mcp institution slice: invalid nested scope accepted'; exit 1; }
sqlite3 "$cmsdb" "select content from wiki_page where id=$draft_id;" | grep -q '^v3$' || { echo 'mcp institution slice: refused nested update changed row'; exit 1; }
admin_call 557 bonfyre_admin_cms_delete "{\"type\":\"wiki_page\",\"namespace\":\"draft-fixture\",\"id\":$draft_id}" | grep -q 'deleted'
[ "$(sqlite3 "$cmsdb" "select count(*) from wiki_page where id=$draft_id;")" = 0 ] || { echo 'mcp institution slice: non-root delete did not land'; exit 1; }
[ "$(sqlite3 "$cmsdb" "select count(*) from wiki_page where id=$root_id;")" = 1 ] || { echo 'mcp institution slice: non-root delete touched root'; exit 1; }

# Provider file/index/container handles are full-superuser observable and
# releasable, but only as replaceable physical residency. They never enter a
# child estate or ordinary self slice.
"$provider" materialize --db "$providerdb" --provider openai --artifact-sid artifact-fixture --representation-digest aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa --external-ref file_fixture --kind file --purpose model-input --scope project-fixture --authority-ref authority-fixture >/dev/null
admin_materializations=$(curl -fsS -X POST -H "Authorization: Bearer $owner_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":56,"method":"tools/call","params":{"name":"bonfyre_admin_provider_materializations","arguments":{"provider":"openai"}}}' "$base")
printf '%s' "$admin_materializations" | grep -q 'artifact-fixture'
if printf '%s' "$admin_materializations" | grep -q 'authority-fixture'; then
  echo 'mcp institution slice: provider materialization control leaked forbidden state'; exit 1
fi
admin_release=$(curl -fsS -X POST -H "Authorization: Bearer $owner_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":57,"method":"tools/call","params":{"name":"bonfyre_admin_provider_materialization_release","arguments":{"receipt_id":1,"reason":"fixture-cooling"}}}' "$base")
printf '%s' "$admin_release" | grep -q '"released":true'
if "$provider" materializations --db "$providerdb" --provider openai | grep -q 'artifact-fixture'; then
  echo 'mcp institution slice: released provider residency remained active'; exit 1
fi

# An MCP call may carry a user's file: a 200 KB argument reaches the owner,
# while an oversized sign-in form is still refused.
big=$(head -c 150000 /dev/zero | tr '\0' 'a')
printf '{"jsonrpc":"2.0","id":90,"method":"tools/call","params":{"name":"project_status","arguments":{"pad":"%s"}}}' "$big" > "$tmp/big.json"
big_answer=$(curl -sS -X POST -H "Authorization: Bearer $owner_token" -H 'Content-Type: application/json' --data-binary @"$tmp/big.json" "$base")
printf '%s' "$big_answer" | grep -q 'owner_upstream' || { echo "mcp institution slice: large MCP body did not reach the owner"; exit 1; }
head -c 40000 /dev/zero | tr '\0' 'a' > "$tmp/big-form"
[ "$(curl -s -o /dev/null -w '%{http_code}' -X POST --data-binary @"$tmp/big-form" "http://127.0.0.1:$port/oauth/token")" = 400 ] || { echo 'mcp institution slice: oversized token form accepted'; exit 1; }

# OAuth code flow with refresh: the token carries the session's real lifetime
# and a refresh token that works only while BonfyreAuth still verifies it.
verifier='fixture-pkce-verifier-0123456789-abcdefghijklmnopqrstuv'
challenge=$(printf '%s' "$verifier" | openssl dgst -sha256 -binary | openssl base64 -A | tr '+/' '-_' | tr -d '=')
location=$(curl -sS -o /dev/null -w '%{redirect_url}' -X POST "http://127.0.0.1:$port/oauth/authorize" \
  --data-urlencode 'client_id=fixture-client' --data-urlencode 'redirect_uri=https://client.fixture.test/cb' \
  --data-urlencode 'state=s1' --data-urlencode 'scope=write' --data-urlencode "code_challenge=$challenge" \
  --data-urlencode 'code_challenge_method=S256' --data-urlencode 'email=slice@t.test' --data-urlencode 'password=slice-fixture-passphrase')
code=$(printf '%s' "$location" | sed -n 's/.*[?&]code=\([^&]*\).*/\1/p')
[ -n "$code" ] || { echo "mcp institution slice: authorize returned no code ($location)"; exit 1; }
grant=$(curl -fsS -X POST "http://127.0.0.1:$port/oauth/token" --data-urlencode 'grant_type=authorization_code' \
  --data-urlencode "code=$code" --data-urlencode 'client_id=fixture-client' \
  --data-urlencode 'redirect_uri=https://client.fixture.test/cb' --data-urlencode "code_verifier=$verifier")
access=$(printf '%s' "$grant" | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
refresh=$(printf '%s' "$grant" | sed -n 's/.*"refresh_token":"\([^"]*\)".*/\1/p')
lifetime=$(printf '%s' "$grant" | sed -n 's/.*"expires_in":\([0-9]*\).*/\1/p')
[ -n "$access" ] && [ -n "$refresh" ] || { echo 'mcp institution slice: code grant lacks access or refresh token'; exit 1; }
[ "${lifetime:-0}" -gt 86400 ] || { echo "mcp institution slice: expires_in $lifetime is not the session lifetime"; exit 1; }
refreshed=$(curl -fsS -X POST "http://127.0.0.1:$port/oauth/token" --data-urlencode 'grant_type=refresh_token' \
  --data-urlencode "refresh_token=$refresh" --data-urlencode 'client_id=fixture-client')
printf '%s' "$refreshed" | grep -q "\"access_token\":\"$access\"" || { echo 'mcp institution slice: refresh did not return the live session'; exit 1; }
curl -fsS "http://127.0.0.1:$port/.well-known/oauth-authorization-server" | grep -q '"refresh_token"'
status() { curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$port/oauth/token" --data-urlencode 'grant_type=refresh_token' "$@"; }
[ "$(status --data-urlencode "refresh_token=${refresh}x" --data-urlencode 'client_id=fixture-client')" = 400 ] || { echo 'mcp institution slice: tampered refresh accepted'; exit 1; }
[ "$(status --data-urlencode "refresh_token=$refresh" --data-urlencode 'client_id=other-client')" = 400 ] || { echo 'mcp institution slice: refresh accepted for another client'; exit 1; }
"$auth" --db "$authdb" logout --token "$access" >/dev/null
[ "$(status --data-urlencode "refresh_token=$refresh" --data-urlencode 'client_id=fixture-client')" = 400 ] || { echo 'mcp institution slice: refresh outlived logout'; exit 1; }

printf '%s\n' 'mcp institution slice: PASS'
