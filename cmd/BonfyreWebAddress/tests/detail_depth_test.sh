#!/usr/bin/env bash
# detail_depth_test.sh -- from any detail page a visitor keeps going ten hops
# through distinct pages that cross feeds, tools, apps and profiles.
#
# Runs against real stores, never a mock: the estate's archive, ledger, CMS
# and compiled application shapes are copied into a temporary directory (the
# originals are only read), facets are derived from the retained bodies with
# `bonfyre-web-address detail-depth derive`, the server is started on that
# copy, and tests/detail_depth_witness.c walks it from real detail pages.
#
#   BONFYRE_TEST_WEB_BINARY   the bonfyre-web-address to test (required)
#   BONFYRE_TEST_ESTATE       stores to copy (default ~/.bonfyre/estate-local)
#   BONFYRE_TEST_HOPS         hops required (default 10)
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
bin="${BONFYRE_TEST_WEB_BINARY:?set BONFYRE_TEST_WEB_BINARY}"
estate="${BONFYRE_TEST_ESTATE:-$HOME/.bonfyre/estate-local}"
hops="${BONFYRE_TEST_HOPS:-10}"
for f in items.db frontier.db cms.db frappe-surfaces.db matrix.db; do
  [ -s "$estate/$f" ] || { echo "SKIP: $estate/$f missing"; exit 0; }
done
tmp="$(mktemp -d "${TMPDIR:-/tmp}/detail-depth.XXXXXX")"
pid=""
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; [ -n "${BONFYRE_TEST_KEEP:-}" ] || rm -rf "$tmp"; }
trap cleanup EXIT
# Copies leave WAL mode: a read-only reader cannot open a WAL file whose
# -shm has not been created yet.
for f in items.db frontier.db cms.db frappe-surfaces.db matrix.db; do sqlite3 -readonly "$estate/$f" ".backup $tmp/$f"; sqlite3 "$tmp/$f" 'PRAGMA journal_mode=DELETE' >/dev/null; done
# Add only to the copied CMS. A draft, a different namespace, an unsafe URL,
# and an unrelated topic must never appear on a public news detail page.
sqlite3 "$tmp/cms.db" <<'SQL'
INSERT INTO internet_source(namespace,family,source,label,says,url,state,note,status,kind,carries) VALUES
 ('aurekai-journalism-review','journalism-toolkit','fixture:news-review','Journalism reviewed fixture','How to check the underlying source','https://example.org/research-guide','reviewed','Check rights before reuse','published','guide','general'),
 ('aurekai-journalism-review','journalism-toolkit','fixture:news-draft','Journalism draft fixture','Not admitted','https://example.org/draft','review-pending','','draft','guide','general'),
 ('outside-review','journalism-toolkit','fixture:news-other','Journalism wrong namespace fixture','Not admitted','https://example.org/other','reviewed','','published','guide','general'),
 ('aurekai-journalism-review','journalism-toolkit','fixture:news-unsafe','Journalism unsafe URL fixture','Not admitted','http://example.org/untrusted','reviewed','','published','guide','general'),
 ('aurekai-journalism-review','journalism-toolkit','fixture:news-unrelated','Journalism unrelated topic fixture','Not admitted','https://example.org/unrelated','reviewed','','published','guide','nonexistentkeyword');
SQL
mkdir -p "$tmp/foreign"; ln -s "$estate/foreign/bodies" "$tmp/foreign/bodies"

BONFYRE_SOURCE_FRONTIER_DB="$tmp/frontier.db" "$bin" detail-depth derive "$tmp/items.db" "$tmp/foreign/bodies" "$tmp/detail-depth.db" > "$tmp/derive.json"
grep -q '"facets_written":[1-9]' "$tmp/derive.json" || { echo "FAIL: derivation wrote no facets"; cat "$tmp/derive.json"; exit 1; }

cc -O2 -Wall -Wextra -Werror -std=c11 -D_DARWIN_C_SOURCE -o "$tmp/witness" "$here/detail_depth_witness.c"
# A port nothing answers on: another server there would be walked instead.
for _ in $(seq 40); do port=$(( 29100 + RANDOM % 600 )); curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$port/" || break; done
# Only the stores named here: an operator shell's own BONFYRE_* settings
# (a project root, a surface) would otherwise steer routes to other stores.
env -i HOME="$HOME" PATH="$PATH" \
BONFYRE_ITEM_ARCHIVE_DB="$tmp/items.db" BONFYRE_SOURCE_FRONTIER_DB="$tmp/frontier.db" BONFYRE_CMS_DB="$tmp/cms.db" \
BONFYRE_FRAPPE_SURFACE_DB="$tmp/frappe-surfaces.db" BONFYRE_DETAIL_DEPTH_DB="$tmp/detail-depth.db" BONFYRE_FOREIGN_ROOT="$tmp/foreign" \
BONFYRE_CMS_SCHEMAS="$repo/cmd/BonfyreCMS/content-types" \
  "$bin" serve --db "$tmp/matrix.db" --port "$port" --workers 2 --site-root "$repo/site/matrix" > "$tmp/serve.log" 2>&1 &
pid=$!
for _ in $(seq 50); do curl -fs -o /dev/null -H 'Host: aurekai.live' "http://127.0.0.1:$port/channels" && break; sleep 0.2; done
kill -0 "$pid" 2>/dev/null || { cat "$tmp/serve.log"; echo "FAIL: server did not start on $port"; exit 1; }

# Starts: the newest piece of each kind, a credited maker, a feed, a
# publisher, a reader, an application and one of its record shapes.
starts=()
for kind in story audio photo video event; do
  p=$(sqlite3 "$tmp/items.db" "SELECT '/o/'||source||'/'||key FROM item WHERE kind='$kind' AND at < strftime('%s','now') ORDER BY at DESC LIMIT 1")
  [ -n "$p" ] && starts+=("$p")
done
maker=$(sqlite3 "$tmp/detail-depth.db" "SELECT replace(value,' ','%20')||'&source='||source FROM item_facet WHERE facet='maker' GROUP BY value,source ORDER BY COUNT(*) DESC LIMIT 1")
[ -n "$maker" ] && starts+=("/people?who=$maker")
urlenc() { local s="$1" o="" c i; for ((i=0;i<${#s};i++)); do c="${s:i:1}"; case "$c" in [a-zA-Z0-9._~-]) o+="$c";; *) o+=$(printf '%%%02X' "'$c");; esac; done; printf %s "$o"; }
feed=$(sqlite3 "$tmp/frontier.db" "SELECT exact_address FROM channel WHERE census_source<>'' AND protocol='rss' ORDER BY last_contributed DESC LIMIT 1")
if [ -n "$feed" ]; then
  starts+=("/channels?channel=$(urlenc "$feed")")
  starts+=("/channels?root=$(sqlite3 "$tmp/frontier.db" "SELECT r.host FROM channel c JOIN root r ON r.root_id=c.root_id WHERE c.exact_address='$feed' LIMIT 1")")
fi
starts+=("/channels?way=rss" "/s/apps/frappe" "/s/record-kind/frappe-blog-post")

out="$tmp/witness.txt"
"$tmp/witness" --port "$port" --host aurekai.live --hops "$hops" --budget 300 --require "${starts[@]}" > "$out" || { cat "$out"; echo "FAIL: a start could not reach $hops hops across feeds, tools, apps and profiles"; exit 1; }
grep '^summary' "$out"

# Every continuation names its kind in markup and draws its icon.
page=$(curl -fs -H 'Accept: text/html' -H 'Host: aurekai.live' "http://127.0.0.1:$port${starts[0]}")
grep -q 'data-detail-depth' <<<"$page" || { echo "FAIL: item page has no continuation region"; exit 1; }
grep -q 'data-depth-kind="feed"><a href="/channels?channel=[^"]*"><svg class="glyph' <<<"$page" || { echo "FAIL: feed continuation lacks its link or icon"; exit 1; }
# The boundary is said in words beside the material it qualifies.
grep -q 'None of it is a recommendation, an identity or a record kept anywhere else' <<<"$page" || { echo "FAIL: boundary missing"; exit 1; }
story=$(sqlite3 "$tmp/items.db" "SELECT '/o/'||source||'/'||key FROM item WHERE kind='story' AND at < strftime('%s','now') ORDER BY at DESC LIMIT 1")
if [ -n "$story" ]; then
  story_html=$(curl -fs -H 'Accept: text/html' -H 'Host: aurekai.live' "http://127.0.0.1:$port$story")
  grep -q 'data-journalism-toolkits' <<<"$story_html" || { echo "FAIL: reviewed research section missing"; exit 1; }
  grep -q 'Journalism reviewed fixture' <<<"$story_html" || { echo "FAIL: reviewed research link missing"; exit 1; }
  for withheld in 'Journalism draft fixture' 'Journalism wrong namespace fixture' 'Journalism unsafe URL fixture' 'Journalism unrelated topic fixture'; do
    if grep -Fq "$withheld" <<<"$story_html"; then echo "FAIL: withheld research resource leaked: $withheld"; exit 1; fi
  done
  grep -q 'does not imply open-source licensing' <<<"$story_html" || { echo "FAIL: toolkit rights boundary missing"; exit 1; }
  echo "PASS: editorially gated research resources on item detail"
else
  echo "SKIP: no story item available for journalism toolkit route"
fi
echo "PASS: detail depth"
