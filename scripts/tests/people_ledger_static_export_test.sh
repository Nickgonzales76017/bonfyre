#!/bin/sh
# What this does not prove:
# - a script written in this test stands where a program would run, so it shows how the caller handles that program's interface and nothing about the program.
# What this proves: the people-ledger export writes the pages the estate's own
# web server gives it. A script named curl used to answer in the server's place
# with one fixed line of HTML.
# The real web server and its archive are isolated from the live estate.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d)
pid=''
cleanup() { [ -z "$pid" ] || kill "$pid" 2>/dev/null || :; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM
sqlite3 "$tmp/items.db" "
CREATE TABLE item(source TEXT, key TEXT, title TEXT, kind TEXT, by TEXT, family TEXT DEFAULT 'news', publisher TEXT DEFAULT '', at INTEGER DEFAULT 0);
INSERT INTO item(source,key,title,kind,by) VALUES ('feed-a','a','First story','article','Alex & Co'),('feed-b','b','Second story','article','Alex & Co');
"
web="$root/cmd/BonfyreWebAddress/bonfyre-web-address"
"$web" bootstrap-owned --db "$tmp/matrix.db" >/dev/null
sqlite3 "$tmp/cms.db" 'CREATE TABLE fixture(id INTEGER);'
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_ITEM_ARCHIVE_DB="$tmp/items.db" \
  "$web" serve --db "$tmp/matrix.db" --auth-db "$tmp/auth.db" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
origin="http://127.0.0.1:$port"
attempt=0
until curl -fsS -o /dev/null -H 'Host: aurekai.life' "$origin/people" 2>/dev/null; do
    attempt=$((attempt + 1))
    [ "$attempt" -lt 100 ] && kill -0 "$pid" 2>/dev/null || { cat "$tmp/server.log" >&2; echo 'fixture web server did not start' >&2; exit 1; }
    sleep 0.05
done
sh "$root/scripts/export_static_estate.sh" \
    --people-ledger-only --archive "$tmp/items.db" --out "$tmp/out" \
    --from "$origin" --parallel 1 > "$tmp/export.log"
test -s "$tmp/out/people.html"
# what was written is the server's page, not a stand-in for it
grep -q '<title>People' "$tmp/out/people.html"
test "$(find "$tmp/out/people/__q/h" -type f -name '*.html' | wc -l | tr -d ' ')" = 3
for page in $(find "$tmp/out/people/__q/h" -type f -name '*.html'); do grep -q '<!doctype html>' "$page"; done
test ! -e "$tmp/out/.people-ledger.list"
test ! -d "$tmp/out/hosts"
rg -q 'wrote 4 people-ledger pages \(0 refused\)' "$tmp/export.log"

# An origin that does not answer refuses every page and writes none.
if sh "$root/scripts/export_static_estate.sh" --people-ledger-only --archive "$tmp/items.db" \
    --out "$tmp/dead" --from http://127.0.0.1:9 --parallel 1 > "$tmp/dead.log" 2>&1; then
    ! rg -q 'wrote 4 people-ledger pages \(0 refused\)' "$tmp/dead.log"
fi
test ! -s "$tmp/dead/people.html"
echo 'people ledger static export: PASS'
