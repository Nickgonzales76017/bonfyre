#!/bin/sh
# End-to-end native authority projection: a MOQT authorization token carries
# only an opaque MediaDelivery lease id.  The listener asks the delivery owner
# at request time; neither MoQ nor the lease issue media bytes or watch proof.
set -eu
bin=${1:?binary}; root=$(mktemp -d); pid=''
cleanup() { [ -z "$pid" ] || kill "$pid" 2>/dev/null || true; [ -z "$pid" ] || wait "$pid" 2>/dev/null || true; rm -rf "$root"; }
trap cleanup EXIT INT TERM
repo=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
auth="$repo/cmd/BonfyreAuth/bonfyre-auth"; media="$repo/cmd/BonfyreMediaLibrary/bonfyre-media-library"; delivery="$repo/cmd/BonfyreMediaDelivery/bonfyre-media-delivery"
make -C "$repo/cmd/BonfyreAuth" all >/dev/null
make -C "$repo/cmd/BonfyreMediaLibrary" all >/dev/null
make -C "$repo/cmd/BonfyreMediaDelivery" all >/dev/null
cms="$root/cms.db"; media_db="$root/media.db"; auth_db="$root/auth.db"; delivery_db="$root/delivery.db"
sqlite3 "$cms" "CREATE TABLE media_program(id INTEGER PRIMARY KEY,program_id TEXT,subject_ref TEXT,media_kind TEXT,asset_ref TEXT,representations TEXT,publication_state TEXT,status TEXT); INSERT INTO media_program VALUES(1,'live:listen','recording:live','audio','/media/live.flac','[{\"id\":\"opus\",\"asset_ref\":\"/media/live.opus\",\"codec\":\"opus\",\"bitrate_kbps\":192,\"delivery\":\"direct\"}]','published','published');"
"$media" library-put --db "$media_db" --id live-library --mode community --owner community:live --actor community:live --title Live --visibility community >/dev/null
"$media" bind --db "$media_db" --library live-library --subject recording:live --program live:listen --collection now --state active --by community:live >/dev/null
"$media" access-put --db "$media_db" --library live-library --actor actor:nick --role viewer --state active --by community:live >/dev/null
"$auth" --db "$auth_db" signup --email moqt@example.test --name Moqt --password fixture-secret --tier free >/dev/null
token=$("$auth" --db "$auth_db" login --email moqt@example.test --password fixture-secret | python3 -c 'import json,sys;print(json.load(sys.stdin)["session"]["token"])')
"$auth" --db "$auth_db" actor-bind --user-id 1 --actor actor:nick >/dev/null
now=$(date +%s); end=$((now+600))
"$delivery" right-put --db "$delivery_db" --id moqt-live --library live-library --subject recording:live --representation opus --actor actor:nick --protocol moqt --starts-at "$now" --ends-at "$end" --evidence evidence:moqt-live --by institution:fixture >/dev/null
printf '%s\n' "$token" | BONFYRE_AUTH_BIN="$auth" BONFYRE_MEDIA_LIBRARY_BIN="$media" "$delivery" authenticated-lease --db "$delivery_db" --media-library-db "$media_db" --cms-db "$cms" --auth-db "$auth_db" --library live-library --subject recording:live --capabilities '{"codecs":["opus"],"max_bitrate_kbps":256}' --protocol moqt >"$root/lease.json"
lease=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["lease_id"])' "$root/lease.json")
openssl req -x509 -newkey rsa:2048 -keyout "$root/key.pem" -out "$root/cert.pem" -days 1 -nodes -subj /CN=127.0.0.1 >/dev/null 2>&1
port=19445
"$bin" moqt-native-server --bind 127.0.0.1 --port "$port" --cert "$root/cert.pem" --key "$root/key.pem" --delivery-db "$delivery_db" --delivery-bin "$delivery" >"$root/server.out" 2>"$root/server.err" & pid=$!
for _ in $(seq 1 60); do grep -q 'delivery_lease_verifier":true' "$root/server.out" 2>/dev/null && break; sleep 0.05; done
grep -q 'delivery_lease_verifier":true' "$root/server.out"
if ! "$bin" moqt-native-subscribe --host 127.0.0.1 --port "$port" --authority "127.0.0.1:$port" --path /live --namespace bonfyre,live --track opus --lease "$lease" >"$root/client.out" 2>"$root/client.err"; then
  cat "$root/server.err" >&2; cat "$root/client.err" >&2; cat "$root/client.out" >&2; exit 1
fi
grep -q 'SUBSCRIBED_CONTROL_ONLY_NO_OBJECTS' "$root/client.out"
grep -q 'lease_verified_subscription_not_media_delivery_or_watch_proof' "$root/client.out"
grep -q 'accepted leased subscribe' "$root/server.err"
grep -q 'received SUBSCRIBE_OK' "$root/client.err"
"$delivery" right-revoke --db "$delivery_db" --id moqt-live --by institution:fixture >"$root/right-revoked.json"
grep -q '"state":"REVOKED"' "$root/right-revoked.json"
if "$bin" moqt-native-subscribe --host 127.0.0.1 --port "$port" --authority "127.0.0.1:$port" --path /live --namespace bonfyre,live --track opus --lease "$lease" >"$root/revoked-client.out" 2>"$root/revoked-client.err"; then
  cat "$root/revoked-client.out" >&2; cat "$root/revoked-client.err" >&2; exit 1
fi
grep -q 'request_submitted_not_authorized_not_subscribed' "$root/revoked-client.out"
grep -q 'received REQUEST_ERROR' "$root/revoked-client.err"
grep -q 'rejected subscribe' "$root/server.err"
echo 'BonfyreMoQ leased native QUIC session: PASS'
