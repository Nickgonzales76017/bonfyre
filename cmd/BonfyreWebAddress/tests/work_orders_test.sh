#!/bin/sh
# Work orders, release sign-offs, disputes and vouches (src/work_orders.h):
# records are written through the CMS owner only by the person whose decision
# they are, and a stranger can neither read nor write them.
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
[ -x "$cms" ] || { echo 'work orders: SKIP (cms not built)'; exit 0; }
[ -x "$auth" ] || { echo 'work orders: SKIP (auth not built)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-work-orders.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM
fail() { echo "work orders: FAIL: $*" >&2; [ -f "$tmp/server.log" ] && tail -20 "$tmp/server.log" >&2; exit 1; }

schemas="$tmp/schemas"
mkdir -p "$schemas"
for t in work_order work_order_party work_order_milestone work_order_signoff work_dispute dispute_statement role_vouch role_program; do
  cp "$root/cmd/BonfyreCMS/content-types/$t.json" "$schemas/"
done
cmsdb="$tmp/cms.db"
"$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null 2>&1
"$cms" entry create role_program '{"role_id":"fixture-steward","label":"Fixture Steward","subject_kinds":[],"relationship_kinds":[],"search_scopes":[],"lenses":[],"work_queues":[],"capability_programs":[],"authority_requirements":[],"exact_operations":[],"unresolved_operations":[],"metric_views":[],"handoff_contracts":[],"surface_scene":{},"cannot_infer":"fixture","status":"published"}' \
  --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null 2>&1

matrix="$tmp/matrix.db"
"$web" bootstrap-owned --db "$matrix" >/dev/null
port=$((20000 + $$ % 30000))
BONFYRE_AUTH_BINARY="$auth" BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" \
  "$web" serve --db "$matrix" --auth-db "$tmp/auth.db" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
base="http://127.0.0.1:$port"
host='Host: aurekai.work'
origin='Origin: http://aurekai.work'
attempt=0
until curl -sS -o /dev/null -H "$host" "$base/healthz" 2>/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 100 ] || ! kill -0 "$pid" 2>/dev/null; then fail 'server did not start'; fi
  sleep 0.05
done

account() {
  curl -fsS -X POST -H "$host" -d "{\"email\":\"$1@work.test\",\"name\":\"$2\",\"password\":\"a-long-enough-passphrase\"}" "$base/signup" >/dev/null
  curl -fsS -X POST -H "$host" -d "{\"email\":\"$1@work.test\",\"password\":\"a-long-enough-passphrase\"}" "$base/login" |
    sed -n 's/.*"token":"\([^"]*\)".*/\1/p'
}
opener=$(account opener 'Opal Opener'); worker=$(account worker 'Wren Worker')
arbiter=$(account arbiter 'Ari Arbiter'); stranger=$(account stranger 'Sam Stranger')
[ -n "$opener" ] && [ -n "$worker" ] && [ -n "$arbiter" ] && [ -n "$stranger" ] || fail 'sessions'
code() { curl -sS -o "$tmp/last" -w '%{http_code}' "$@"; }
post() { who=$1; path=$2; shift 2; code -X POST -H "$host" -H "$origin" -H "Cookie: bonfyre_session=$who" "$@" "$base$path"; }
q() { sqlite3 "$cmsdb" "$1"; }

# Signed out: the pages ask for sign-in, writes are refused.
[ "$(code -H "$host" "$base/work-orders")" = 303 ] || fail 'signed-out index should redirect to sign-in'
[ "$(code -X POST -H "$host" -H "$origin" -d 'title=x' "$base/work-orders/open")" = 401 ] || fail 'anonymous open'
# Wrong origin and wrong host are refused before anything is written.
[ "$(code -X POST -H "$host" -H 'Origin: https://elsewhere.test' -H "Cookie: bonfyre_session=$opener" -d 'title=x&scope=y&terms=z&currency=USD' "$base/work-orders/open")" = 403 ] || fail 'foreign origin'
[ "$(code -X POST -H 'Host: aurekai.shop' -H 'Origin: http://aurekai.shop' -H "Cookie: bonfyre_session=$opener" -d 'title=x&scope=y&terms=z&currency=USD' "$base/work-orders/open")" = 404 ] || fail 'other host'
[ "$(q 'SELECT count(*) FROM work_order')" = 0 ] || fail 'refused submits wrote'

# Empty state.
[ "$(code -H "$host" -H "Cookie: bonfyre_session=$opener" "$base/work-orders")" = 200 ] || fail 'index'
grep -q 'You are not a party to any work order yet' "$tmp/last" || fail 'empty state'
grep -q 'action="/work-orders/open"' "$tmp/last" || fail 'compose form'

# Opening a work order names the opener as its first party.
[ "$(post "$opener" /work-orders/open --data-urlencode 'title=Hull inspection' --data-urlencode 'scope=Inspect the hull and write it up' --data-urlencode 'terms=Each milestone needs every party' -d 'currency=USD&context=self')" = 303 ] || fail 'open'
wo=$(q "SELECT work_order_id FROM work_order WHERE title='Hull inspection'")
[ -n "$wo" ] || fail 'work order row'
[ "$(q "SELECT count(*) FROM work_order_party WHERE work_order_id='$wo' AND party_subject='user:1'")" = 1 ] || fail 'opener party'
# Opening for an organisation you hold no grant in is refused.
[ "$(post "$opener" /work-orders/open -d 'title=x&scope=y&terms=z&currency=USD&context=institution:nobody')" = 403 ] || fail 'foreign institution'

# The opener names a party (existing account only) and stages a milestone.
[ "$(post "$opener" /work-orders/party -d "wo=$wo&email=nobody%40work.test&part=Worker")" = 404 ] || fail 'unknown account'
[ "$(post "$opener" /work-orders/party -d "wo=$wo&email=worker%40work.test&part=Worker&compensation=2400&basis=Per+milestone&role=fixture-steward")" = 303 ] || fail 'name party'
[ "$(post "$opener" /work-orders/milestone -d "wo=$wo&title=Bore+inspection&scope=Photos+and+readings&amount=2400&due=2026-12-01")" = 303 ] || fail 'milestone'
ms=$(q "SELECT milestone_id FROM work_order_milestone WHERE work_order_id='$wo'")
[ -n "$ms" ] || fail 'milestone row'
# Only the opener composes.
[ "$(post "$worker" /work-orders/milestone -d "wo=$wo&title=Extra&scope=More")" = 403 ] || fail 'worker composed'

# A stranger can neither see nor write.
[ "$(code -H "$host" -H "Cookie: bonfyre_session=$stranger" "$base/work-orders/$wo")" = 404 ] || fail 'stranger read'
[ "$(post "$stranger" /work-orders/signoff -d "wo=$wo&milestone=$ms&decision=approve")" = 403 ] || fail 'stranger signoff'
[ "$(post "$stranger" /work-orders/party -d "wo=$wo&email=stranger%40work.test&part=Me")" = 403 ] || fail 'stranger added self'
[ "$(post "$stranger" /disputes/open -d "wo=$wo&milestone=terms&arbiter=arbiter%40work.test&claim=x")" = 403 ] || fail 'stranger dispute'
[ "$(q 'SELECT count(*) FROM work_order_signoff')" = 0 ] || fail 'stranger wrote a signoff'

# Each party records their own decisions; N of M is a count of them.
[ "$(post "$worker" /work-orders/signoff -d "wo=$wo&milestone=terms&decision=approve")" = 303 ] || fail 'accept terms'
[ "$(post "$opener" /work-orders/milestone -d "wo=$wo&title=Late&scope=After+acceptance")" = 409 ] || fail 'terms not fixed after acceptance'
[ "$(post "$worker" /work-orders/signoff -d "wo=$wo&milestone=$ms&decision=withhold&note=Readings+missing")" = 303 ] || fail 'withhold'
[ "$(post "$worker" /work-orders/signoff -d "wo=$wo&milestone=$ms&decision=approve")" = 303 ] || fail 'approve'
[ "$(q "SELECT count(*) FROM work_order_signoff WHERE milestone_id='$ms'")" = 1 ] || fail 'decision replaced, not added'
[ "$(code -H "$host" -H "Cookie: bonfyre_session=$worker" "$base/work-orders/$wo")" = 200 ] || fail 'party read'
grep -q 'Approved by 1 of 2' "$tmp/last" || fail 'N of M'
grep -q 'Wren Worker' "$tmp/last" || fail 'party names'
grep -q 'No funds are held' "$tmp/last" || fail 'authority line'
[ "$(post "$opener" /work-orders/signoff -d "wo=$wo&milestone=$ms&decision=approve")" = 303 ] || fail 'opener approve'
curl -fsS -H "$host" -H "Cookie: bonfyre_session=$opener" "$base/work-orders/$wo" | grep -q 'Approved by 2 of 2' || fail '2 of 2'
curl -fsS -H "$host" -H "Cookie: bonfyre_session=$opener" "$base/work-orders" >"$tmp/index"
grep -q 'data-compensation-term' "$tmp/index" || fail 'compensation ledger'
grep -q 'Hull inspection' "$tmp/index" || fail 'ledger row'

# Disputes: a party names an arbiter who is not a party; only the arbiter decides.
[ "$(post "$worker" /disputes/open -d "wo=$wo&milestone=$ms&arbiter=opener%40work.test&claim=x")" = 409 ] || fail 'party as arbiter'
[ "$(post "$worker" /disputes/open -d "wo=$wo&milestone=$ms&arbiter=arbiter%40work.test" --data-urlencode 'claim=The readings were late')" = 303 ] || fail 'raise dispute'
dp=$(q "SELECT dispute_id FROM work_dispute WHERE work_order_id='$wo'")
[ -n "$dp" ] || fail 'dispute row'
[ "$(code -H "$host" -H "Cookie: bonfyre_session=$stranger" "$base/disputes/$dp")" = 404 ] || fail 'stranger hearing'
[ "$(post "$stranger" /disputes/statement -d "dispute=$dp&body=hi")" = 403 ] || fail 'stranger statement'
[ "$(post "$opener" /disputes/statement -d "dispute=$dp&body=They+were+on+time")" = 303 ] || fail 'response'
[ "$(post "$opener" /disputes/decide -d "dispute=$dp&decision=Mine&confirm=yes")" = 403 ] || fail 'party decided'
[ "$(post "$arbiter" /disputes/decide -d "dispute=$dp&decision=Unconfirmed")" = 400 ] || fail 'unconfirmed decision'
[ "$(code -H "$host" -H "Cookie: bonfyre_session=$arbiter" "$base/disputes/$dp")" = 200 ] || fail 'arbiter hearing'
grep -q 'action="/disputes/decide"' "$tmp/last" || fail 'decision form'
[ "$(post "$arbiter" /disputes/decide -d "dispute=$dp&confirm=yes" --data-urlencode 'decision=Approve the milestone; readings were on time')" = 303 ] || fail 'decide'
[ "$(q "SELECT state FROM work_dispute WHERE dispute_id='$dp'")" = decided ] || fail 'decided state'
[ "$(post "$arbiter" /disputes/decide -d "dispute=$dp&decision=Again&confirm=yes")" = 409 ] || fail 'decided twice'
curl -fsS -H "$host" -H "Cookie: bonfyre_session=$worker" "$base/disputes/$dp" | grep -q 'readings were on time' || fail 'decision shown'
[ "$(q "SELECT count(*) FROM work_order_signoff WHERE milestone_id='$ms' AND decision='approve'")" = 2 ] || fail 'decision changed a signoff'

# Vouches: for another existing account in a published role; only the voucher revokes.
[ "$(code -H "$host" -H "Cookie: bonfyre_session=$stranger" "$base/vouches")" = 200 ] || fail 'vouch page'
grep -q 'Nobody has vouched for anyone yet' "$tmp/last" || fail 'vouch empty state'
[ "$(post "$opener" /vouches/give -d 'email=opener%40work.test&role=fixture-steward&scope=global&statement=me&confirm=yes')" = 409 ] || fail 'self vouch'
[ "$(post "$opener" /vouches/give -d 'email=worker%40work.test&role=not-a-role&scope=global&statement=x&confirm=yes')" = 400 ] || fail 'unknown role'
[ "$(post "$opener" /vouches/give -d 'email=worker%40work.test&role=fixture-steward&scope=global&statement=Careful+work')" = 400 ] || fail 'unconfirmed vouch'
[ "$(post "$opener" /vouches/give -d 'email=worker%40work.test&role=fixture-steward&scope=global&statement=Careful+work&confirm=yes')" = 303 ] || fail 'vouch'
vo=$(q "SELECT vouch_id FROM role_vouch WHERE voucher='user:1'")
[ -n "$vo" ] || fail 'vouch row'
[ "$(post "$stranger" /vouches/revoke -d "vouch=$vo")" = 404 ] || fail 'stranger revoked'
[ "$(q "SELECT state FROM role_vouch WHERE vouch_id='$vo'")" = standing ] || fail 'stranger changed vouch'
curl -fsS -H "$host" -H "Cookie: bonfyre_session=$stranger" "$base/vouches" | grep -q 'Wren Worker' || fail 'vouch listed'
[ "$(post "$opener" /vouches/revoke -d "vouch=$vo")" = 303 ] || fail 'revoke'
[ "$(q "SELECT state FROM role_vouch WHERE vouch_id='$vo'")" = revoked ] || fail 'revoked state'
[ "$(post "$opener" /vouches/give -d 'email=worker%40work.test&role=fixture-steward&scope=global&statement=Again&confirm=yes')" = 303 ] || fail 're-vouch'
[ "$(q "SELECT count(*)||state FROM role_vouch WHERE vouch_id='$vo'")" = 1standing ] || fail 're-vouch is the same record'

echo 'work orders: PASS'
