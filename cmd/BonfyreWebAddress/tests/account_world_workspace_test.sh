#!/bin/sh
set -eu
root=$(CDPATH='' cd -- "$(dirname "$0")/../../.." && pwd)
web=${BONFYRE_TEST_WEB_BINARY:-"$root/cmd/BonfyreWebAddress/bonfyre-web-address"}
cms=${BONFYRE_TEST_CMS_BINARY:-"$root/cmd/BonfyreCMS/bonfyre-cms"}
auth=${BONFYRE_TEST_AUTH_BINARY:-"$root/cmd/BonfyreAuth/bonfyre-auth"}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/world-workspace.XXXXXX")
pid=''
cleanup(){ [ -z "$pid" ] || kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT HUP INT TERM
mkdir "$tmp/schemas"
for type in world site_presentation site_page; do cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$tmp/schemas/$type.json"; done
export BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_CMS_SCHEMAS="$tmp/schemas" BONFYRE_MATRIX_DB="$tmp/matrix.db" BONFYRE_AUTH_BINARY="$auth" BONFYRE_CMS_BINARY="$cms" BONFYRE_FACT_DELTAS=''
"$cms" schema migrate --db "$BONFYRE_CMS_DB" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null
"$auth" --db "$tmp/auth.db" signup --email owner@example.test --name Owner --password 'fixture-ownership-pass' >/dev/null
"$auth" --db "$tmp/auth.db" signup --email other@example.test --name Other --password 'fixture-ownership-pass' >/dev/null
owner=$("$auth" --db "$tmp/auth.db" login --email owner@example.test --password 'fixture-ownership-pass' | jq -r .session.token)
other=$("$auth" --db "$tmp/auth.db" login --email other@example.test --password 'fixture-ownership-pass' | jq -r .session.token)
"$auth" --db "$tmp/auth.db" actor-bind --user-id 1 --actor person:avery >/dev/null
"$auth" --db "$tmp/auth.db" actor-bind --user-id 2 --actor person:other >/dev/null
"$web" bootstrap-owned --db "$BONFYRE_MATRIX_DB" >/dev/null
"$web" subject --db "$BONFYRE_MATRIX_DB" --id person:avery --kind Person --digest aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa --evidence fixture:profile >/dev/null
"$web" institution --db "$BONFYRE_MATRIX_DB" --id inst:person-avery --kind Person --world live --evidence fixture:profile >/dev/null
for territory in work life; do
 "$web" domain --db "$BONFYRE_MATRIX_DB" --host "avery.aurekai.$territory" --institution inst:person-avery --territory "$territory" --scope public --world live --repr HTML --evidence fixture:profile >/dev/null
 "$web" presence --db "$BONFYRE_MATRIX_DB" --id "presence:avery:$territory" --subject person:avery --territory "$territory" --scope public --world live --repr HTML --evidence fixture:profile >/dev/null
 "$cms" entry create site_presentation "{\"site_id\":\"profile-avery-$territory\",\"host\":\"avery.aurekai.$territory\",\"subject_address\":\"person:avery\",\"territory\":\"$territory\",\"profile\":\"member-profile-v1\",\"navigation\":[],\"status\":\"published\"}" --ns root --db "$BONFYRE_CMS_DB" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null
 "$cms" entry create world "{\"world\":\"private-$territory\",\"title\":\"Private $territory\",\"character\":\"atlas\",\"lede\":\"Draft only\",\"subject\":\"user:1\",\"sources\":\"none\",\"cannot_infer\":\"No publication\",\"status\":\"draft\"}" --ns "profile-avery-$territory" --db "$BONFYRE_CMS_DB" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null
done
port=$((20000 + $$ % 20000))
"$web" serve --db "$BONFYRE_MATRIX_DB" --auth-db "$tmp/auth.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 & pid=$!
base="http://127.0.0.1:$port"
i=0; until curl -fsS "$base/healthz" >/dev/null 2>&1; do i=$((i+1)); [ "$i" -lt 100 ] || { cat "$tmp/server.log"; exit 1; }; sleep .1; done
request(){ token=$1; method=$2; data=$3; curl -sS -o "$tmp/response" -w '%{http_code}' -X "$method" -H 'Host: aurekai.work' -H 'Origin: https://aurekai.work' -H "Authorization: Bearer $token" -H 'Content-Type: application/x-www-form-urlencoded' --data "$data" "$base/workspace/world"; }
get(){ token=$1; data=$2; curl -sS -o "$tmp/response" -w '%{http_code}' -H 'Host: aurekai.work' -H "Authorization: Bearer $token" "$base/workspace/world?$data"; }
[ "$(request "$owner" POST 'operation=bind&site=profile-avery-work&world=1')" = 200 ]
for space in gallery room archive; do
 [ "$(request "$owner" POST "operation=put&site=profile-avery-work&world=1&space=$space&title=$space&renderer=2d&template=template%3Aa&layout=layout%3Aa&revision=0")" = 200 ]
done
[ "$(get "$owner" 'operation=list&site=profile-avery-work&world=1')" = 200 ]
[ "$(jq -r '.spaces|length' "$tmp/response")" = 3 ]
[ "$(jq -r '[.spaces[].space]|join(",")' "$tmp/response")" = archive,gallery,room ]
[ "$(jq -r '.spaces[0].revision' "$tmp/response")" = 1 ]
[ "$(get "$owner" 'operation=list&site=profile-avery-work&world=1&space=room')" = 400 ]
[ "$(get "" 'operation=list&site=profile-avery-work&world=1')" = 404 ]
[ "$(get "$other" 'operation=list&site=profile-avery-work&world=1')" = 404 ]
[ "$(get "$owner" 'operation=list&site=profile-avery-life&world=1')" = 404 ]
[ "$(get "$owner" 'operation=read&site=profile-avery-work&world=1&space=room')" = 200 ]
[ "$(jq -r .state "$tmp/response")" = draft ]
curl -sS -D "$tmp/headers" -o "$tmp/response" -H 'Host: aurekai.work' -H "Authorization: Bearer $owner" "$base/workspace/world?operation=read&site=profile-avery-work&world=1&space=room"
grep -qi '^Cache-Control: private, no-store' "$tmp/headers"
curl -sS -o "$tmp/public" -H 'Host: aurekai.work' "$base/_surface/worlds"
! grep -q 'private-work' "$tmp/public"
[ "$(curl -sS -o "$tmp/response" -w '%{http_code}' -H 'Host: aurekai.work' -H 'Origin: https://attacker.test' -H "Authorization: Bearer $owner" -H 'Content-Type: application/x-www-form-urlencoded' --data 'operation=bind&site=profile-avery-work&world=1' "$base/workspace/world")" = 403 ]
[ "$(request "$owner" POST 'operation=bind&site=profile-avery-work&site=profile-avery-life&world=1')" = 400 ]
[ "$(get '' 'operation=read&site=profile-avery-work&world=1&space=room')" = 404 ]
sqlite3 "$BONFYRE_CMS_DB" "UPDATE world SET status='published' WHERE id=1"
[ "$(get "$owner" 'operation=list&site=profile-avery-work&world=1')" = 404 ]
[ "$(get "$owner" 'operation=read&site=profile-avery-work&world=1&space=room')" = 404 ]
[ "$(request "$owner" POST 'operation=put&site=profile-avery-work&world=1&space=room&title=No&renderer=2d&template=t&layout=l&revision=1')" = 409 ]
sqlite3 "$BONFYRE_CMS_DB" "UPDATE world SET status='draft' WHERE id=1"
[ "$(get "$other" 'operation=read&site=profile-avery-work&world=1&space=room')" = 404 ]
[ "$(get "$owner" 'operation=read&site=profile-avery-life&world=1&space=room')" = 404 ]
[ "$(request "$owner" POST 'operation=put&site=profile-avery-work&world=1&space=room&title=New&renderer=3d&template=template%3Ab&layout=layout%3Ab&revision=1')" = 200 ]
[ "$(request "$owner" POST 'operation=put&site=profile-avery-work&world=1&space=room&title=Stale&renderer=2d&template=t&layout=l&revision=1')" = 409 ]
sqlite3 "$BONFYRE_MATRIX_DB" "UPDATE matrix_presences SET active=0 WHERE presence_id='presence:avery:work'"
[ "$(get "$owner" 'operation=read&site=profile-avery-work&world=1&space=room')" = 404 ]
[ "$(request "$owner" POST 'operation=put&site=profile-avery-work&world=1&space=room&title=No&renderer=2d&template=t&layout=l&revision=2')" = 404 ]
sqlite3 "$BONFYRE_MATRIX_DB" "UPDATE matrix_presences SET active=1 WHERE presence_id='presence:avery:work'"
"$auth" --db "$tmp/auth.db" sessions-revoke --user-id 1 >/dev/null
[ "$(get "$owner" 'operation=read&site=profile-avery-work&world=1&space=room')" = 404 ]
[ "$(request "$owner" POST 'operation=bind&site=profile-avery-life&world=2')" = 404 ]
owner=$("$auth" --db "$tmp/auth.db" login --email owner@example.test --password 'fixture-ownership-pass' | jq -r .session.token)
[ "$(get "$owner" 'operation=read&site=profile-avery-work&world=1&space=room')" = 200 ]
"$auth" --db "$tmp/auth.db" actor-revoke --user-id 1 >/dev/null
[ "$(get "$owner" 'operation=read&site=profile-avery-work&world=1&space=room')" = 404 ]
[ "$(request "$owner" POST 'operation=bind&site=profile-avery-life&world=2')" = 404 ]
[ "$(sqlite3 "$BONFYRE_CMS_DB" "SELECT status FROM world WHERE id=1")" = draft ]
printf 'account World workspace: ok\n'
