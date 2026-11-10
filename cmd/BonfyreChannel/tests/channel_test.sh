#!/bin/sh
set -eu
bin=${1:?binary}; root=$(mktemp -d); trap 'rm -rf "$root"' EXIT
repo=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
auth="$repo/cmd/BonfyreAuth/bonfyre-auth"
estate="$repo/cmd/BonfyreExternalEstate/bonfyre-external-estate"
make -C "$repo/cmd/BonfyreAuth" all >/dev/null
make -C "$repo/cmd/BonfyreExternalEstate" all >/dev/null
db="$root/channel.db"; cms="$root/cms.db"; auth_db="$root/auth.db"
sqlite3 "$cms" "CREATE TABLE media_program(id INTEGER PRIMARY KEY,program_id TEXT,subject_ref TEXT,media_kind TEXT,asset_ref TEXT,publication_state TEXT,status TEXT); INSERT INTO media_program VALUES(1,'video:opening','video:opening','video','/assets/opening.mp4','published','published'); INSERT INTO media_program VALUES(2,'live:tonight','stream:tonight','video','/assets/tonight.mp4','published','published');"
"$auth" --db "$auth_db" signup --email viewer@example.test --name Viewer --password fixture-secret --tier free >/dev/null
token=$("$auth" --db "$auth_db" login --email viewer@example.test --password fixture-secret | python3 -c 'import json,sys;print(json.load(sys.stdin)["session"]["token"])')
"$auth" --db "$auth_db" actor-bind --user-id 1 --actor actor:viewer >/dev/null
"$bin" put --db "$db" --id channel:deep-ellum --owner actor:creator --name 'Deep Ellum Tonight' --handle deepellum --visibility public --community community:deep-ellum --by actor:creator >/dev/null
"$bin" publish --db "$db" --cms-db "$cms" --channel channel:deep-ellum --subject video:opening --program video:opening --kind video --by actor:creator >/dev/null
"$bin" publish --db "$db" --cms-db "$cms" --channel channel:deep-ellum --subject stream:tonight --program live:tonight --kind live --by actor:creator >/dev/null
"$bin" surface --db "$db" --cms-db "$cms" --channel channel:deep-ellum >"$root/public.json"
grep -q '"navigation"' "$root/public.json"; grep -q 'video:opening' "$root/public.json"; grep -q 'stream:tonight' "$root/public.json"
printf '%s\n' "$token" | BONFYRE_AUTH_BIN="$auth" "$bin" authenticated-subscribe --db "$db" --auth-db "$auth_db" --channel channel:deep-ellum >"$root/subscribe.json"
grep -q '"actor_subject":"actor:viewer"' "$root/subscribe.json"; grep -q '"state":"active"' "$root/subscribe.json"
printf '%s\n' "$token" | BONFYRE_AUTH_BIN="$auth" "$bin" authenticated-unsubscribe --db "$db" --auth-db "$auth_db" --channel channel:deep-ellum >"$root/unsubscribe.json"
grep -q '"state":"unsubscribed"' "$root/unsubscribe.json"
"$bin" put --db "$db" --id channel:private --owner actor:creator --name Private --handle private --visibility private --by actor:creator >/dev/null
! printf '%s\n' "$token" | BONFYRE_AUTH_BIN="$auth" "$bin" authenticated-subscribe --db "$db" --auth-db "$auth_db" --channel channel:private >/dev/null 2>&1
"$bin" emit-advertisements --db "$db" >"$root/ads.json" 2>"$root/ads.err"
grep -q 'CreatorChannel:visibility' "$root/ads.json"; grep -q 'forwarding_descriptors_only' "$root/ads.err"
"$estate" compile-advertisements --db "$root/address.db" --cms-db "$cms" --creator-channel-db "$db" >"$root/address-compile.json"
grep -q '"compiled_owner_populations":2' "$root/address-compile.json"
"$estate" forward --db "$root/address.db" --kind creator_channel --need operation:watch >"$root/address-forward.json"
grep -q '"owner_ref":"CreatorChannel:channel:deep-ellum"' "$root/address-forward.json"
grep -q 'eligible_for_further_consideration' "$root/address-forward.json"
# The same addressed population compiles a scoped machine aperture.  It is an
# advertisement only: Channel still receives an authenticated actor closure
# before exposing a private catalog or mutating subscriptions.
"$estate" mcp-aperture --db "$root/address.db" --actor-closure sha256:actor-fixture --address-closure sha256:address-fixture --target-frontier sha256:target-fixture --policy-generation sha256:policy-fixture --need operation:watch >"$root/channel-aperture.json"
grep -q 'bonfyre://channel/{channel}/surface' "$root/channel-aperture.json"
grep -q '"identity":"creator_channel_surface"' "$root/channel-aperture.json"
grep -q 'CreatorChannel:channel:deep-ellum' "$root/channel-aperture.json"
echo 'BonfyreChannel: PASS'
