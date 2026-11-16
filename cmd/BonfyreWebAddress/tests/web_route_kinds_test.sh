#!/bin/sh
# Route kinds past a fixed list (schemas/control/web-routes.cue,
# src/web_route_kinds.h): a records row answers a path that became a row of
# its declared store after the server started, with no rebuild or restart;
# a path nobody answers returns 404 with up to five ranked suggestions that
# are links, never a dispatch, and machines keep the resolver's JSON verdict.
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'route kinds: SKIP (cms not built)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-route-kinds.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM
fail() { echo "route kinds: FAIL: $*" >&2; [ -f "$tmp/server.log" ] && tail -20 "$tmp/server.log" >&2; exit 1; }

schemas="$tmp/schemas"
mkdir -p "$schemas"
cp "$root/cmd/BonfyreCMS/content-types/wiki_page.json" "$schemas/"
cmsdb="$tmp/cms.db"
"$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null 2>&1
q() { sqlite3 "$cmsdb" "$1"; }
note() { # route title guest
  q "INSERT INTO wiki_page(namespace,created_at,updated_at,published_at,status,title,route,content,published,allow_guest)
     VALUES('root','2026-11-15','2026-11-15','2026-11-15','published','$2','$1','First paragraph.',1,$3);"
}

matrix="$tmp/matrix.db"
"$web" bootstrap-owned --db "$matrix" >/dev/null
port=$((20000 + $$ % 30000))
# The ranker reads the distilled static model when it is present; without it
# the ranking is trigram overlap alone and the semantic case is skipped.
model=${BONFYRE_STATIC_MODEL:-$HOME/BonfyreModels/static/nomic-static-128.bfse}
BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" BONFYRE_STATIC_MODEL="$model" \
  "$web" serve --db "$matrix" --auth-db "$tmp/auth.db" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
base="http://127.0.0.1:$port"
host='Host: aurekai.work'
attempt=0
until curl -sS -o /dev/null -H "$host" "$base/healthz" 2>/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 100 ] || ! kill -0 "$pid" 2>/dev/null; then fail 'server did not start'; fi
  sleep 0.05
done
page() { curl -sS -o "$tmp/last" -w '%{http_code}' -H "$host" -H 'Accept: text/html' "$base$1"; }
suggested() { grep -o '<section class="ak-pattern ak-section not-found-suggestions">.*</section>' "$tmp/last" | grep -o 'href="[^"]*"' | sed 's/href="//;s/"$//'; }

# 1. records: the path is not a record yet, so the owner is not asked and
# the tail answers 404.
[ "$(page /notes/river-crossing)" = 404 ] || fail 'a note that is not a record answered'
grep -q 'Nothing is at this address' "$tmp/last" || fail 'not-found page missing'

# Writing the row is the whole act: the same running server now answers it.
note notes/river-crossing 'River crossing' 1
[ "$(page /notes/river-crossing)" = 200 ] || fail 'a new record did not answer without a rebuild'
grep -q 'River crossing' "$tmp/last" || fail 'the note owner did not render the record'

# The owner's own authority checks still decide: a published note that is
# not guest-readable is asked for, and the owner refuses a stranger.
note notes/steward-only 'Steward only' 0
[ "$(page /notes/steward-only)" = 401 ] || fail 'owner authority changed under records match'
# A draft is not a record of the set.
q "INSERT INTO wiki_page(namespace,created_at,updated_at,status,title,route,content,allow_guest) VALUES('root','x','x','draft','Draft','notes/draft-one','x',1);"
[ "$(page /notes/draft-one)" = 404 ] || fail 'a draft answered'
# The index row is unchanged.
[ "$(page /notes)" = 200 ] && grep -q 'River crossing' "$tmp/last" || fail 'notes index'

# 2. semantic: a misspelt record path returns 404 and offers the record,
# ranked first, within five suggestions; the private note and the draft are
# never offered.
[ "$(page /notes/river-crosing)" = 404 ] || fail 'misspelt path did not keep 404'
first=$(suggested | head -1)
[ "$first" = /notes/river-crossing ] || fail "record not ranked first (got '$first')"
[ "$(suggested | wc -l | tr -d ' ')" -le 5 ] || fail 'more than five suggestions'
suggested | grep -q 'steward-only\|draft-one' && fail 'a record nobody may read was offered'
# A misspelt declared page is offered by its declared title.
[ "$(page /comunities)" = 404 ] || fail 'misspelt page status'
suggested | grep -qx /communities || fail 'declared page not suggested'
grep -q '>Communities<' "$tmp/last" || fail 'declared title not shown'
# The same meaning under other words, when the model is present.
if [ -f "$model" ]; then
  [ "$(page /prices)" = 404 ] || fail 'semantic status'
  suggested | head -1 | grep -qx /what-things-cost || fail "semantic neighbour not first ($(suggested | head -1))"
fi
# Nothing near: the empty state, still 404.
[ "$(page /zqxjv)" = 404 ] || fail 'unrelated path status'
[ -z "$(suggested)" ] || fail 'suggestions for an unrelated path'
grep -q 'Nothing here has a similar name' "$tmp/last" || fail 'empty state missing'
# Suggestions are never dispatch: the suggestion page is the resolver's reply
# and a machine asking for JSON keeps the resolver's verdict.
code=$(curl -sS -o "$tmp/json" -w '%{http_code}' -H "$host" -H 'Accept: application/json' "$base/notes/river-crosing")
[ "$code" = 404 ] && grep -q '"verdict":"not_found"' "$tmp/json" || fail 'machine 404 verdict changed'
grep -q 'suggest' "$tmp/json" && fail 'machine reply carries suggestions'
echo 'route kinds: PASS'
