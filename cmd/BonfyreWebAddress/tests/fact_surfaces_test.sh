#!/bin/sh
# Fact surfaces over native HTTP: stores built from nothing; a public row
# (prices) answers anyone with the recorded rows; an admin row (roles) sends a
# signed-out visitor to sign in, refuses a signed-in member, and answers the
# bonfyre admin; a store absent on this host shows an honest empty state; /x
# lists only rows the viewer may see; nothing under /x accepts a write.
set -eu
root=$(CDPATH='' cd -- "$(dirname "$0")/../../.." && pwd)
web=${1:-${BONFYRE_TEST_WEB_BINARY:-"$root/cmd/BonfyreWebAddress/bonfyre-web-address"}}
case $web in /*) ;; *) web="$PWD/$web" ;; esac
cms=${BONFYRE_TEST_CMS_BINARY:-"$root/cmd/BonfyreCMS/bonfyre-cms"}
auth=${BONFYRE_TEST_AUTH_BINARY:-"$root/cmd/BonfyreAuth/bonfyre-auth"}
[ -x "$cms" ] || { echo 'fact surfaces: SKIP (cms not built)'; exit 0; }
[ -x "$auth" ] || { echo 'fact surfaces: SKIP (auth not built)'; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/fact-surfaces.XXXXXX")
pid=''
cleanup(){ [ -z "$pid" ] || kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT HUP INT TERM
fail(){ printf 'fact surfaces: FAIL %s\n' "$1" >&2; exit 1; }
mkdir -p "$tmp/schemas" "$tmp/home"
for type in price_observation product_identity role_program site_presentation; do
  cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$tmp/schemas/$type.json"
done
# Every other store the registry names is absent: HOME points at an empty dir.
for v in BONFYRE_SOURCE_FRONTIER_DB BONFYRE_DETAIL_DEPTH_DB BONFYRE_LATENT_ATLAS_DB BONFYRE_COMMERCE_DB BONFYRE_ADMISSION_DB; do unset "$v" 2>/dev/null || true; done
export HOME="$tmp/home" BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_CMS_SCHEMAS="$tmp/schemas" BONFYRE_MATRIX_DB="$tmp/matrix.db" BONFYRE_AUTH_BINARY="$auth" BONFYRE_CMS_BINARY="$cms" BONFYRE_FACT_DELTAS=''
"$cms" schema migrate --db "$BONFYRE_CMS_DB" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null
entry(){ "$cms" entry create "$1" "$2" --ns root --db "$BONFYRE_CMS_DB" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null; }
entry product_identity '{"product_id":"p1","title":"Oat milk 1 l","brand":"Fixture","gtin":"0001","source_ref":"fixture:shop","receipt_ref":"fixture:r1","observed_at":"2026-11-01T10:00:00Z","cannot_infer":"x","status":"published"}'
entry price_observation '{"observation_id":"o1","product_id":"p1","amount":"2.49","currency":"USD","kind":"listed","source_ref":"fixture:shop","receipt_ref":"fixture:r1","observed_at":"2026-11-01T10:00:00Z","cannot_infer":"x","status":"published"}'
entry price_observation '{"observation_id":"o2","product_id":"p1","amount":"9.99","currency":"USD","kind":"listed","source_ref":"fixture:shop","receipt_ref":"fixture:r2","observed_at":"2026-11-02T10:00:00Z","cannot_infer":"x","status":"draft"}'
entry role_program '{"role_id":"steward","label":"Steward","subject_kinds":[],"relationship_kinds":[],"search_scopes":[],"lenses":[],"work_queues":[],"capability_programs":[],"authority_requirements":[],"exact_operations":[],"unresolved_operations":[],"metric_views":[],"handoff_contracts":[],"surface_scene":[],"cannot_infer":"x","status":"published"}'
for who in admin:Avery member:Robin; do
  "$auth" --db "$tmp/auth.db" signup --email "${who%%:*}@example.test" --name "${who##*:}" --password 'fixture-facts-pass' >/dev/null
done
"$auth" --db "$tmp/auth.db" institution-grant --user-id 1 --institution bonfyre --role admin >/dev/null
login(){ "$auth" --db "$tmp/auth.db" login --email "$1@example.test" --password 'fixture-facts-pass' | jq -r .session.token; }
admin=$(login admin); member=$(login member)
"$web" bootstrap-owned --db "$BONFYRE_MATRIX_DB" >/dev/null
port=$((20000 + ($$ + 41) % 20000))
"$web" serve --db "$BONFYRE_MATRIX_DB" --auth-db "$tmp/auth.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 & pid=$!
base="http://127.0.0.1:$port"
i=0; until curl -fsS "$base/healthz" >/dev/null 2>&1; do i=$((i+1)); [ "$i" -lt 100 ] || { cat "$tmp/server.log"; exit 1; }; sleep .1; done
get(){ curl -sS -o "$tmp/response" -w '%{http_code}' -H 'Host: aurekai.work' -H 'Accept: text/html' ${1:+-H "Authorization: Bearer $1"} "$base$2"; }

# public row: anyone, published rows only, inside the one shell
[ "$(get '' /x/prices)" = 200 ] || fail 'public row not served'
grep -q 'class="ak-app' "$tmp/response" || fail 'public row outside the shell'
grep -q 'Oat milk 1 l' "$tmp/response" && grep -q '2.49' "$tmp/response" || fail 'public row missing its record'
grep -q '9.99' "$tmp/response" && fail 'a draft record was shown'
grep -q '<table class="ak-pattern ak-table' "$tmp/response" || fail 'public row not drawn as a table'
# admin row: signed out -> sign in; member -> refused; admin -> rows
[ "$(get '' /x/roles)" = 303 ] || fail 'signed-out admin row not sent to sign in'
[ "$(get "$member" /x/roles)" = 403 ] || fail 'member saw an admin row'
grep -q 'Steward' "$tmp/response" && fail 'refusal leaked a record'
[ "$(get "$admin" /x/roles)" = 200 ] && grep -q 'Steward' "$tmp/response" || fail 'admin row missing for admin'
# absent store: honest empty state, not an error
[ "$(get "$admin" /x/source-channels)" = 200 ] && grep -q 'cannot be read on this server' "$tmp/response" || fail 'absent store not stated'
# index: only rows the viewer may see
[ "$(get '' /x)" = 200 ] || fail 'index'
grep -q '/x/prices' "$tmp/response" || fail 'index lacks a public row'
grep -q '/x/roles' "$tmp/response" && fail 'index offered an admin row to a visitor'
grep -q 'Visits you asked for' "$tmp/response" && fail 'index offered a member row to a visitor'
[ "$(get "$member" /x)" = 200 ] && grep -q 'Visits you asked for' "$tmp/response" || fail 'index lacks member rows for a member'
grep -q '/x/roles' "$tmp/response" && fail 'index offered an admin row to a member'
[ "$(get "$admin" /x)" = 200 ] && grep -q '/x/roles' "$tmp/response" || fail 'index lacks admin rows for admin'
# no write path
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'Host: aurekai.work' -H "Authorization: Bearer $admin" --data 'x=1' "$base/x/prices")
[ "$code" != 200 ] && [ "$code" != 303 ] || fail "a POST under /x answered $code"
[ "$(get '' /x/no-such-fact)" = 404 ] || fail 'unknown slug did not fall to not-found'
echo 'fact surfaces: PASS'
