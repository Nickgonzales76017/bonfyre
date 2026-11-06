#!/bin/sh
# Actual-owner MCP media ingress witness.  The HTTP carrier is the additive
# stateless adapter, but Auth, AddressPlane, MediaLibrary and the CMS
# representation row are real native owners—not doubles.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
media="$root/cmd/BonfyreMediaLibrary/bonfyre-media-library"
delivery="$root/cmd/BonfyreMediaDelivery/bonfyre-media-delivery"
channel="$root/cmd/BonfyreChannel/bonfyre-channel"
estate="$root/cmd/BonfyreExternalEstate/bonfyre-external-estate"
adapter="$root/integrations/mcp/bonfyre_mcp_2026_adapter.mjs"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-mcp-media.XXXXXX")
pid=''
cleanup() { [ -z "$pid" ] || kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

make -C "$root/cmd/BonfyreAuth" all >/dev/null
make -C "$root/cmd/BonfyreMediaLibrary" all >/dev/null
make -C "$root/cmd/BonfyreMediaDelivery" all >/dev/null
make -C "$root/cmd/BonfyreChannel" all >/dev/null
make -C "$root/cmd/BonfyreExternalEstate" all >/dev/null

auth_db="$tmp/auth.db"
media_db="$tmp/media.db"
cms_db="$tmp/cms.db"
address_db="$tmp/address.db"
delivery_db="$tmp/delivery.db"
channel_db="$tmp/channel.db"
sqlite3 "$cms_db" "CREATE TABLE media_program(id INTEGER PRIMARY KEY,program_id TEXT,subject_ref TEXT,media_kind TEXT,asset_ref TEXT,representations TEXT,publication_state TEXT,status TEXT); INSERT INTO media_program VALUES(1,'actual:listen','recording:actual','audio','/media/actual.flac','[{\"id\":\"actual-opus\",\"asset_ref\":\"/media/actual.opus\",\"codec\":\"opus\",\"bitrate_kbps\":192,\"delivery\":\"direct\"}]','published','published'); INSERT INTO media_program VALUES(2,'actual:watch','video:actual','video','/media/actual.mp4','[]','published','published');"
"$media" library-put --db "$media_db" --id actual-library --mode personal --owner actor:actual --actor actor:actual --title 'Actual Media' --visibility private >/dev/null
"$media" bind --db "$media_db" --library actual-library --subject recording:actual --program actual:listen --state active --by actor:actual >/dev/null
"$auth" --db "$auth_db" signup --email actual@example.test --name Actual --password fixture-secret --tier pro >/dev/null
token=$("$auth" --db "$auth_db" login --email actual@example.test --password fixture-secret | python3 -c 'import json,sys; print(json.load(sys.stdin)["session"]["token"])')
"$auth" --db "$auth_db" actor-bind --user-id 1 --actor actor:actual >/dev/null
"$channel" put --db "$channel_db" --id actual-channel --owner actor:actual --name 'Actual Channel' --handle actual-channel --visibility public --by actor:actual >/dev/null
"$channel" publish --db "$channel_db" --cms-db "$cms_db" --channel actual-channel --subject video:actual --program actual:watch --kind video --by actor:actual >/dev/null
now=$(date +%s); end=$((now+600))
"$delivery" right-put --db "$delivery_db" --id actual-hls --library actual-library --subject recording:actual --representation actual-opus --actor actor:actual --protocol hls --starts-at "$now" --ends-at "$end" --evidence evidence:actual-hls --by institution:fixture >/dev/null
"$estate" compile-advertisements --db "$address_db" --media-library-db "$media_db" --creator-channel-db "$channel_db" >/dev/null

set -- $(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
port=$1
BONFYRE_MCP_2026_HOST=127.0.0.1 BONFYRE_MCP_2026_PORT="$port" \
BONFYRE_AUTH_BINARY="$auth" BONFYRE_AUTH_DB="$auth_db" \
BONFYRE_ADDRESSPLANE_BINARY="$estate" BONFYRE_ADDRESSPLANE_DB="$address_db" \
BONFYRE_MEDIA_LIBRARY_BINARY="$media" BONFYRE_MEDIA_LIBRARY_DB="$media_db" BONFYRE_MEDIA_LIBRARY_CMS_DB="$cms_db" \
BONFYRE_MEDIA_DELIVERY_BINARY="$delivery" BONFYRE_MEDIA_DELIVERY_DB="$delivery_db" \
BONFYRE_CHANNEL_BINARY="$channel" BONFYRE_CHANNEL_DB="$channel_db" BONFYRE_CHANNEL_CMS_DB="$cms_db" \
node "$adapter" >"$tmp/adapter.log" 2>&1 &
pid=$!
for _ in $(seq 1 60); do grep -q 'MCP 2026 aperture' "$tmp/adapter.log" 2>/dev/null && break; sleep 0.05; done
grep -q 'MCP 2026 aperture' "$tmp/adapter.log"

base="http://127.0.0.1:$port/mcp"
meta='"_meta":{"io.modelcontextprotocol/clientInfo":{"name":"actual-media-test","version":"1"}}'
request() {
  method=$1 name=$2 body=$3
  curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' -H 'MCP-Protocol-Version: 2026-07-28' -H "Mcp-Method: $method" -H "Mcp-Name: $name" --data "$body" "$base"
}

templates=$(request resources/templates/list resources/templates/list "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"resources/templates/list\",\"params\":{$meta}}")
printf '%s' "$templates" | grep -q 'bonfyre://media/library/{library}/catalog'
printf '%s' "$templates" | grep -q 'bonfyre://channel/{channel}/surface'
catalog=$(request resources/read bonfyre://media/library/actual-library/catalog "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"resources/read\",\"params\":{\"uri\":\"bonfyre://media/library/actual-library/catalog\",$meta}}")
printf '%s' "$catalog" | grep -q 'actual:listen'
channel_surface=$(request resources/read bonfyre://channel/actual-channel/surface "{\"jsonrpc\":\"2.0\",\"id\":21,\"method\":\"resources/read\",\"params\":{\"uri\":\"bonfyre://channel/actual-channel/surface\",$meta}}")
printf '%s' "$channel_surface" | grep -q 'actual:watch'
plan=$(request tools/call media_playback_plan "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"media_playback_plan\",\"arguments\":{\"library\":\"actual-library\",\"subject\":\"recording:actual\",\"capabilities\":{\"codecs\":[\"opus\"],\"max_bitrate_kbps\":256}},$meta}}")
printf '%s' "$plan" | grep -q '"state":"DIRECT_PLAYABLE"'
printf '%s' "$plan" | grep -q 'no_transport_or_watch_claim'
channel_tool=$(request tools/call creator_channel_surface "{\"jsonrpc\":\"2.0\",\"id\":22,\"method\":\"tools/call\",\"params\":{\"name\":\"creator_channel_surface\",\"arguments\":{\"channel\":\"actual-channel\"},$meta}}")
printf '%s' "$channel_tool" | grep -q 'actual:watch'
lease=$(request tools/call media_transport_lease "{\"jsonrpc\":\"2.0\",\"id\":31,\"method\":\"tools/call\",\"params\":{\"name\":\"media_transport_lease\",\"arguments\":{\"library\":\"actual-library\",\"subject\":\"recording:actual\",\"capabilities\":{\"codecs\":[\"opus\"],\"max_bitrate_kbps\":256},\"protocol\":\"hls\",\"ttl_seconds\":60},$meta}}")
printf '%s' "$lease" | grep -q 'LEASED_NOT_DELIVERED'
printf '%s' "$lease" | grep -q 'no_bytes_transcode_session_or_watch_proof'

# Revoking the principal->Actor projection invalidates the same bearer at the
# adapter before it can expose another catalog or a representation plan.
"$auth" --db "$auth_db" actor-revoke --user-id 1 >/dev/null
status=$(curl -sS -o "$tmp/revoked.json" -w '%{http_code}' -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/json' -H 'MCP-Protocol-Version: 2026-07-28' -H 'Mcp-Method: resources/read' -H 'Mcp-Name: bonfyre://media/library/actual-library/catalog' --data "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"resources/read\",\"params\":{\"uri\":\"bonfyre://media/library/actual-library/catalog\",$meta}}" "$base")
[ "$status" = 401 ]
grep -q 'oauth_required' "$tmp/revoked.json"

echo 'MCP media ActorClosure: PASS'
