#!/bin/sh
set -eu
bin=${1:?binary}; root=$(mktemp -d); trap 'rm -rf "$root"' EXIT
repo=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
auth="$repo/cmd/BonfyreAuth/bonfyre-auth"; media="$repo/cmd/BonfyreMediaLibrary/bonfyre-media-library"
db="$root/delivery.db"; media_db="$root/media.db"; cms="$root/cms.db"; auth_db="$root/auth.db"
make -C "$repo/cmd/BonfyreAuth" all >/dev/null
make -C "$repo/cmd/BonfyreMediaLibrary" all >/dev/null
sqlite3 "$cms" "CREATE TABLE media_program(id INTEGER PRIMARY KEY,program_id TEXT,subject_ref TEXT,media_kind TEXT,asset_ref TEXT,representations TEXT,publication_state TEXT,status TEXT); INSERT INTO media_program VALUES(1,'late-set:listen','recording:late-set','audio','/media/late.flac','[{\"id\":\"opus\",\"asset_ref\":\"/media/late.opus\",\"codec\":\"opus\",\"bitrate_kbps\":192,\"delivery\":\"direct\"}]','published','published');"
"$media" library-put --db "$media_db" --id night --mode personal --owner actor:nick --actor actor:nick --title Night --visibility private >/dev/null
"$media" bind --db "$media_db" --library night --subject recording:late-set --program late-set:listen --collection live --state active --by actor:nick >/dev/null
"$auth" --db "$auth_db" signup --email nick-delivery@example.test --name Nick --password fixture-secret --tier free >/dev/null
token=$("$auth" --db "$auth_db" login --email nick-delivery@example.test --password fixture-secret | python3 -c 'import json,sys;print(json.load(sys.stdin)["session"]["token"])')
"$auth" --db "$auth_db" actor-bind --user-id 1 --actor actor:nick >/dev/null
now=$(date +%s); end=$((now+600))
"$bin" right-put --db "$db" --id right-late --library night --subject recording:late-set --representation opus --actor actor:nick --protocol hls --starts-at "$now" --ends-at "$end" --evidence evidence:right-late --by institution:fixture >/dev/null
! printf '%s\n' "$token" | BONFYRE_AUTH_BIN="$auth" BONFYRE_MEDIA_LIBRARY_BIN="$media" "$bin" authenticated-lease --db "$db" --media-library-db "$media_db" --cms-db "$cms" --auth-db "$auth_db" --library night --subject recording:late-set --capabilities '{"codecs":["opus"],"max_bitrate_kbps":256}' --protocol dash >"$root/no-right.json" 2>&1
grep -q 'REFUSED_RIGHT_REQUIRED' "$root/no-right.json"
printf '%s\n' "$token" | BONFYRE_AUTH_BIN="$auth" BONFYRE_MEDIA_LIBRARY_BIN="$media" "$bin" authenticated-lease --db "$db" --media-library-db "$media_db" --cms-db "$cms" --auth-db "$auth_db" --library night --subject recording:late-set --capabilities '{"codecs":["opus"],"max_bitrate_kbps":256}' --protocol hls >"$root/lease.json"
grep -q 'LEASED_NOT_DELIVERED' "$root/lease.json"
grep -q 'no_bytes_transcode_session_or_watch_proof' "$root/lease.json"
lease=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["lease_id"])' "$root/lease.json")
"$bin" lease-status --db "$db" --lease "$lease" >"$root/status.json"
grep -q '"state":"active"' "$root/status.json"
"$bin" lease-revoke --db "$db" --lease "$lease" --by institution:fixture >"$root/revoked-lease.json"
grep -q '"state":"REVOKED"' "$root/revoked-lease.json"
"$bin" lease-status --db "$db" --lease "$lease" >"$root/revoked-status.json"
grep -q '"state":"revoked"' "$root/revoked-status.json"
printf '%s\n' "$token" | BONFYRE_AUTH_BIN="$auth" BONFYRE_MEDIA_LIBRARY_BIN="$media" "$bin" authenticated-lease --db "$db" --media-library-db "$media_db" --cms-db "$cms" --auth-db "$auth_db" --library night --subject recording:late-set --capabilities '{"codecs":["opus"],"max_bitrate_kbps":256}' --protocol hls >"$root/second-lease.json"
second_lease=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["lease_id"])' "$root/second-lease.json")
"$bin" right-revoke --db "$db" --id right-late --by institution:fixture >"$root/revoked-right.json"
grep -q '"state":"REVOKED"' "$root/revoked-right.json"
"$bin" lease-status --db "$db" --lease "$second_lease" >"$root/right-revoked-status.json"
grep -q '"state":"revoked"' "$root/right-revoked-status.json"
! printf '%s\n' "$token" | BONFYRE_AUTH_BIN="$auth" BONFYRE_MEDIA_LIBRARY_BIN="$media" "$bin" authenticated-lease --db "$db" --media-library-db "$media_db" --cms-db "$cms" --auth-db "$auth_db" --library night --subject recording:late-set --capabilities '{"codecs":["opus"],"max_bitrate_kbps":256}' --protocol hls >"$root/right-revoked-refusal.json" 2>&1
grep -q 'REFUSED_RIGHT_REQUIRED' "$root/right-revoked-refusal.json"
"$auth" --db "$auth_db" actor-revoke --user-id 1 >/dev/null
! printf '%s\n' "$token" | BONFYRE_AUTH_BIN="$auth" BONFYRE_MEDIA_LIBRARY_BIN="$media" "$bin" authenticated-lease --db "$db" --media-library-db "$media_db" --cms-db "$cms" --auth-db "$auth_db" --library night --subject recording:late-set --capabilities '{"codecs":["opus"],"max_bitrate_kbps":256}' --protocol hls >/dev/null 2>&1
echo 'BonfyreMediaDelivery: PASS'
