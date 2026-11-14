#!/bin/sh
# Account continuity and field visits over native HTTP: a person enrolls a
# device key (recorded as typed, never attested) and revokes it; offers a
# one-use move code that the same account redeems once on another session
# and a stranger cannot; asks for a Bluesky sync that stays "not run"; sets
# a feed rule for an agent on a feed they keep; and records a field request
# that only the named member may accept. A stranger can write none of them,
# and a cross-site submit is refused.
set -eu
root=$(CDPATH='' cd -- "$(dirname "$0")/../../.." && pwd)
web=${BONFYRE_TEST_WEB_BINARY:-"$root/cmd/BonfyreWebAddress/bonfyre-web-address"}
cms=${BONFYRE_TEST_CMS_BINARY:-"$root/cmd/BonfyreCMS/bonfyre-cms"}
auth=${BONFYRE_TEST_AUTH_BINARY:-"$root/cmd/BonfyreAuth/bonfyre-auth"}
[ -x "$cms" ] || { echo 'account continuity: SKIP (cms not built)'; exit 0; }
[ -x "$auth" ] || { echo 'account continuity: SKIP (auth not built)'; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/account-continuity.XXXXXX")
pid=''
cleanup(){ [ -z "$pid" ] || kill "$pid" 2>/dev/null || true; [ -n "${KEEP_ACCOUNT_CONTINUITY:-}" ] || rm -rf "$tmp"; }
trap cleanup EXIT HUP INT TERM
fail(){ printf 'account continuity: FAIL %s\n' "$1" >&2; exit 1; }
mkdir -p "$tmp/schemas"
for type in site_presentation site_page saved_item device_key device_pairing carrier_sync_request feed_agent_policy field_dispatch; do
  cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$tmp/schemas/$type.json"
done
export BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_CMS_SCHEMAS="$tmp/schemas" BONFYRE_MATRIX_DB="$tmp/matrix.db" BONFYRE_AUTH_BINARY="$auth" BONFYRE_CMS_BINARY="$cms" BONFYRE_FACT_DELTAS=''
"$cms" schema migrate --db "$BONFYRE_CMS_DB" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null
entry(){ "$cms" entry create "$1" "$2" --ns root --db "$BONFYRE_CMS_DB" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null; }
q(){ sqlite3 "$BONFYRE_CMS_DB" "$1"; }

for who in owner:Avery other:Robin; do
  "$auth" --db "$tmp/auth.db" signup --email "${who%%:*}@example.test" --name "${who##*:}" --password 'fixture-continuity-pass' >/dev/null
done
login(){ "$auth" --db "$tmp/auth.db" login --email "$1@example.test" --password 'fixture-continuity-pass' | jq -r .session.token; }
owner=$(login owner); owner2=$(login owner); other=$(login other)
"$auth" --db "$tmp/auth.db" actor-bind --user-id 2 --actor person:robin >/dev/null
entry site_presentation '{"site_id":"profile-robin-work","host":"robin.aurekai.work","subject_address":"person:robin","territory":"work","profile":"member-profile-v1","navigation":[],"status":"published"}'
entry saved_item '{"saved":"s1","actor":"user:1","item_ref":"news/aaaaaaaaaaaaaaaaaaaaaaaa","title":"A kept story","saved_at":"2026-11-01T00:00:00Z","collection":"Morning reading","cannot_infer":"x","status":"published"}'
"$web" bootstrap-owned --db "$BONFYRE_MATRIX_DB" >/dev/null
"$web" subject --db "$BONFYRE_MATRIX_DB" --id person:robin --kind Person --digest aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa --evidence fixture:profile >/dev/null
"$web" institution --db "$BONFYRE_MATRIX_DB" --id inst:person-robin --kind Person --world live --evidence fixture:profile >/dev/null
"$web" domain --db "$BONFYRE_MATRIX_DB" --host robin.aurekai.work --institution inst:person-robin --territory work --scope public --world live --repr HTML --evidence fixture:profile >/dev/null
"$web" presence --db "$BONFYRE_MATRIX_DB" --id presence:robin:work --subject person:robin --territory work --scope public --world live --repr HTML --evidence fixture:profile >/dev/null
port=$((20000 + ($$ + 29) % 20000))
"$web" serve --db "$BONFYRE_MATRIX_DB" --auth-db "$tmp/auth.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 & pid=$!
base="http://127.0.0.1:$port"
i=0; until curl -fsS "$base/healthz" >/dev/null 2>&1; do i=$((i+1)); [ "$i" -lt 100 ] || { cat "$tmp/server.log"; exit 1; }; sleep .1; done
send(){ curl -sS -o "$tmp/response" -D "$tmp/headers" -w '%{http_code}' -X POST -H 'Host: aurekai.work' -H "Origin: ${ORIGIN:-https://aurekai.work}" -H "Authorization: Bearer $1" -H 'Content-Type: application/x-www-form-urlencoded' --data "$3" "$base$2"; }
get(){ curl -sS -o "$tmp/response" -D "$tmp/headers" -w '%{http_code}' -H 'Host: aurekai.work' -H 'Accept: text/html' ${1:+-H "Authorization: Bearer $1"} "$base$2"; }

# --- signed out: sent to sign in, nothing recorded -------------------------
for route in /account/devices /account/move /account/move/receive /connections/bluesky/sync /account/agents /dispatch; do
  [ "$(get '' "$route")" = 303 ] || fail "signed-out $route not sent to sign in"
done

# --- devices ---------------------------------------------------------------
[ "$(get "$owner" /account/devices)" = 200 ] || fail 'devices page'
grep -q 'class="ak-app' "$tmp/response" && grep -q 'No devices yet' "$tmp/response" || fail 'devices page is not the shell with an empty state'
[ "$(ORIGIN=https://evil.test send "$owner" /account/devices 'device_label=Phone&key_fingerprint=SHA256%3Aabcdefabcdef0123456789&key_algorithm=ed25519')" = 403 ] || fail 'cross-site enroll accepted'
[ "$(q 'select count(*) from device_key')" = 0 ] || fail 'cross-site enroll wrote'
[ "$(send "$owner" /account/devices 'device_label=Phone&key_fingerprint=SHA256%3Aabcdefabcdef0123456789&key_algorithm=ed25519')" = 303 ] || fail 'enroll'
[ "$(q "select account_subject||'|'||attestation||'|'||status from device_key")" = 'user:1|not verified|published' ] || fail 'enrolled row'
get "$owner" /account/devices >/dev/null; grep -q 'SHA256:abcdefabcdef0123456789' "$tmp/response" || fail 'device not listed'
get "$other" /account/devices >/dev/null; if grep -q 'SHA256:abcdef' "$tmp/response"; then fail 'stranger sees device'; fi
dev=$(q 'select id from device_key')
[ "$(send "$other" /account/devices/revoke "device=$dev")" = 404 ] || fail 'stranger revoked'
[ "$(q 'select status from device_key')" = published ] || fail 'stranger revoke wrote'
[ "$(send "$owner" /account/devices/revoke "device=$dev")" = 303 ] || fail 'revoke'
[ "$(q 'select status from device_key')" = archived ] || fail 'revoke not recorded'

# --- move: offer, stranger fails, same account receives once ---------------
[ "$(send "$owner" /account/move 'operation=offer')" = 200 ] || fail 'offer'
code=$(sed -n 's/.*data-pairing-code>\([0-9]\{3\}\)<span>&ndash;<\/span>\([0-9]\{3\}\).*/\1\2/p' "$tmp/response")
[ ${#code} = 6 ] || fail 'no code shown'
if q 'select pairing_id||sender_session from device_pairing' | grep -q "$code"; then fail 'code stored in clear'; fi
[ "$(send "$other" /account/move/receive "code=$code")" = 400 ] || fail 'stranger redeemed the code'
[ "$(q 'select state from device_pairing')" = offered ] || fail 'stranger changed the pairing'
[ "$(send "$owner2" /account/move/receive "code=$code")" = 200 ] || fail 'receive'
grep -q 'Code accepted on this device' "$tmp/response" || fail 'receive page'
[ "$(q 'select state from device_pairing')" = received ] || fail 'pairing not received'
[ "$(send "$owner2" /account/move/receive "code=$code")" = 400 ] || fail 'code reused'

# --- bluesky: requested, not run --------------------------------------------
[ "$(get "$owner" /connections/bluesky/sync)" = 200 ] || fail 'bluesky page'
grep -q 'not declared a Bluesky handle' "$tmp/response" || fail 'bluesky empty state'
"$auth" --db "$tmp/auth.db" external-link-claim --token "$owner" --provider bluesky --remote-account avery.bsky.social --profile-url https://bsky.app/profile/avery.bsky.social --display-name Avery >/dev/null
link=$("$auth" --db "$tmp/auth.db" external-link-list --token "$owner" | jq -r '.links[0].id')
[ "$(send "$other" /connections/bluesky/sync "link=$link")" = 404 ] || fail 'stranger requested sync of another link'
[ "$(send "$owner" /connections/bluesky/sync "link=$link")" = 303 ] || fail 'sync request'
[ "$(q "select remote_account||'|'||run_state from carrier_sync_request")" = 'avery.bsky.social|requested' ] || fail 'sync row'
get "$owner" /connections/bluesky/sync >/dev/null; grep -q 'requested · not run' "$tmp/response" || fail 'sync state not shown honestly'

# --- agents: a rule on a kept feed; strangers cannot ------------------------
[ "$(send "$other" /account/agents 'feed=Morning+reading&agent=Claude+Desktop&access=read')" = 404 ] || fail 'stranger set rule on another feed'
[ "$(send "$owner" /account/agents 'feed=Morning+reading&agent=Claude+Desktop&access=summary')" = 303 ] || fail 'rule'
[ "$(send "$owner" /account/agents 'feed=Morning+reading&agent=Claude+Desktop&access=none')" = 303 ] || fail 'rule change'
[ "$(q "select count(*)||'|'||max(access) from feed_agent_policy")" = '1|none' ] || fail 'rule not updated in place'
get "$owner" /account/agents >/dev/null; grep -q 'No access' "$tmp/response" && grep -q 'Not yet' "$tmp/response" || fail 'agents page'

# --- dispatch: only the named member answers --------------------------------
[ "$(send "$owner" /dispatch 'operation=request&place=Community+garden&needed_at=Saturday+9am&need=Fix+the+gate+latch')" = 303 ] || fail 'dispatch request'
d=$(q 'select id from field_dispatch')
[ "$(send "$other" /dispatch "operation=offer&dispatch=$d&address=robin.aurekai.work")" = 403 ] || fail 'stranger offered'
[ "$(send "$owner" /dispatch "operation=offer&dispatch=$d&address=robin.aurekai.work")" = 303 ] || fail 'offer to member'
[ "$(q "select state||'|'||assignee_subject from field_dispatch")" = 'offered|person:robin' ] || fail 'offer row'
[ "$(send "$owner" /dispatch "operation=accept&dispatch=$d")" = 403 ] || fail 'requester accepted own request'
get "$other" /dispatch >/dev/null; grep -q 'Fix the gate latch' "$tmp/response" || fail 'assignee does not see it'
[ "$(send "$other" /dispatch "operation=accept&dispatch=$d")" = 303 ] || fail 'accept'
[ "$(q 'select state from field_dispatch')" = accepted ] || fail 'accept not recorded'
[ "$(send "$other" /dispatch "operation=withdraw&dispatch=$d")" = 403 ] || fail 'assignee withdrew'
echo 'account continuity: PASS'
