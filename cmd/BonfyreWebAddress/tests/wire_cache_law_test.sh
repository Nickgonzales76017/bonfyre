#!/bin/sh
# The cache law every answer leaves under, and the feed every page links.
#
# Origin 404s once carried no Cache-Control; the edge kept them four hours and
# aurekai.work drew its front unstyled from a cached refusal. Sheets were
# linked by bare name, so a deploy was a stale hit, not a new URL. And
# /feed.xml probed route items per member row: an apex feed took over a minute
# and pinned one of the two workers every front page waited behind.
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
here=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
root=$(CDPATH= cd -- "$here/../.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-wire-cache.XXXXXX")
pid=''
trap '[ -n "$pid" ] && kill "$pid" 2>/dev/null; [ -n "${KEEP:-}" ] || rm -rf "$tmp"' EXIT INT TERM
fail() { echo "wire cache law: $*" >&2; exit 1; }

# A projection shaped like the compiled one: members, bodies, the view over
# them, and route items keyed route-first, at a size the old plan could not
# answer inside the budget.
python3 - "$tmp/subject_fabric.db" <<'EOF'
import sqlite3, sys
db = sqlite3.connect(sys.argv[1])
db.executescript("""
CREATE TABLE subject_projection_build(build_id INTEGER PRIMARY KEY, compiled_at TEXT);
CREATE TABLE subject_route(subject TEXT,scope TEXT,route TEXT,territory TEXT,title TEXT,lede TEXT,character TEXT,cannot_infer TEXT);
CREATE TABLE subject_route_item(subject TEXT NOT NULL,scope TEXT NOT NULL,route TEXT NOT NULL,ordinal INTEGER NOT NULL,
  source TEXT NOT NULL,item_key TEXT NOT NULL,material TEXT NOT NULL,PRIMARY KEY(subject,scope,route,source,item_key,material));
CREATE TABLE subject_material_member(subject TEXT NOT NULL,scope TEXT NOT NULL,source TEXT NOT NULL,item_key TEXT NOT NULL,
  material TEXT NOT NULL,PRIMARY KEY(subject,scope,source,item_key,material));
CREATE TABLE subject_material_body(source TEXT NOT NULL,item_key TEXT NOT NULL,material TEXT NOT NULL,family TEXT,title TEXT,
  byline TEXT,publisher TEXT,href TEXT,group_name TEXT,image TEXT,asset TEXT,poster TEXT,body TEXT,ident TEXT,about TEXT,
  rights TEXT,rights_href TEXT,origin TEXT,origin_href TEXT,people TEXT,where_text TEXT,transcript_href TEXT,chapters_href TEXT,
  talk_href TEXT,at INTEGER,duration_ms INTEGER,placed INTEGER,lon REAL,lat REAL,PRIMARY KEY(source,item_key,material));
CREATE VIEW subject_material AS SELECT x.subject,x.scope,b.source,b.item_key,b.material,b.family,b.title,b.byline,b.publisher,
  b.href,b.group_name,b.image,b.asset,b.poster,b.body,b.ident,b.about,b.rights,b.rights_href,b.origin,b.origin_href,b.people,
  b.where_text,b.transcript_href,b.chapters_href,b.talk_href,b.at,b.duration_ms,b.placed,b.lon,b.lat
  FROM subject_material_member x JOIN subject_material_body b ON b.source=x.source AND b.item_key=x.item_key AND b.material=x.material;
INSERT INTO subject_projection_build VALUES(1,'2026-10-24T00:00:00Z');
INSERT INTO subject_route VALUES('institution:aurekai:life','public','/','life','Tonight','','','');
""")
subject = 'institution:aurekai:life'
rows = [(f'feed-{i % 40}', f'{i:024x}') for i in range(12000)]
db.executemany("INSERT INTO subject_material_member VALUES(?,?,?,?,'publication')", [(subject, 'public', s, k) for s, k in rows])
db.executemany("INSERT INTO subject_material_body(source,item_key,material,title,body,at) VALUES(?,?,'publication',?,?,?)",
               [(s, k, f'Story {int(k, 16)}', 'Body', 1760000000 + n) for n, (s, k) in enumerate(rows)])
db.executemany("INSERT INTO subject_route_item VALUES(?,?,?,?,?,?,'publication')",
               [(subject, 'public', f'/r{n % 12}', n, s, k) for n, (s, k) in enumerate(rows)])
db.commit()
EOF

"$web" bootstrap-owned --db "$tmp/matrix.db" >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_SUBJECT_FABRIC_DB="$tmp/subject_fabric.db" BONFYRE_WORKSPACE_CLIENT="$root/integrations/browser" \
  "$web" serve --db "$tmp/matrix.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
attempt=0
until curl -sS -o /dev/null "http://127.0.0.1:$port/healthz" 2>/dev/null; do
    attempt=$((attempt + 1))
    [ "$attempt" -lt 80 ] && kill -0 "$pid" 2>/dev/null || fail 'server did not start'
    sleep 0.05
done
get() { curl -sS --max-time 10 -D "$tmp/head" -o "$tmp/body" -w '%{http_code} %{time_total}' -H "Host: $1" "http://127.0.0.1:$port$2"; }
header() { tr -d '\r' < "$tmp/head" | grep -i "^$1:" | head -1 | sed 's/^[^:]*: *//'; }

# The feed answers inside its budget and carries the newest compiled items.
answer=$(get aurekai.life /feed.xml) || fail '/feed.xml did not answer inside ten seconds'
case "$answer" in 200\ *) ;; *) fail "/feed.xml answered $answer" ;; esac
awk -v t="${answer#* }" 'BEGIN { exit !(t < 1.5) }' || fail "/feed.xml took ${answer#* }s (budget 1.5s)"
grep -q 'Story 11999<' "$tmp/body" || fail 'the feed lost its newest item'
[ "$(grep -c '<entry>' "$tmp/body")" -eq 50 ] || fail 'the feed is not the fifty newest items'

# A refusal is never kept, whatever the handler wrote.
for path in /assets/no-such-sheet-zz.css /apps/no-such-code-zz.mjs /no-such-page-zz; do
    answer=$(get aurekai.work "$path")
    case "$answer" in 4*|5*) ;; *) fail "$path answered $answer" ;; esac
    [ "$(header Cache-Control)" = no-store ] || fail "$path refusal leaves as '$(header Cache-Control)'"
done

# Each front names its sheets by their bytes, and those URLs are kept forever.
for host in aurekai.life aurekai.work aurekai.shop aurekai.live; do
    answer=$(get "$host" /)
    case "$answer" in 200\ *) ;; *) fail "$host / answered $answer" ;; esac
    if grep -oE '"/assets/[A-Za-z0-9._-]+"' "$tmp/body" | grep -q .; then
        fail "$host links $(grep -oE '"/assets/[A-Za-z0-9._-]+"' "$tmp/body" | head -1) by bare name"
    fi
    refs=$(grep -oE '/assets/[A-Za-z0-9._-]+\?v=[0-9a-f]{16}"' "$tmp/body" | tr -d '"' | sort -u)
    [ -n "$refs" ] || fail "$host / names no versioned sheet"
    for ref in $refs; do
        name=${ref#/assets/}; name=${name%%\?*}; version=${ref##*v=}
        [ "$(shasum -a 256 "$root/site/matrix/$name" | cut -c1-16)" = "$version" ] ||
            fail "$host names $name by a version that is not its digest"
        answer=$(get "$host" "$ref")
        case "$answer" in 200\ *) ;; *) fail "$ref answered $answer" ;; esac
        [ "$(header Cache-Control)" = 'public, max-age=31536000, immutable' ] ||
            fail "$ref is kept as '$(header Cache-Control)'"
    done
    answer=$(get "$host" /)
    awk -v t="${answer#* }" 'BEGIN { exit !(t < 0.2) }' || fail "$host / warm took ${answer#* }s (budget 0.2s)"
done

# Editor code is revalidated, not resent: its digest answers 304.
get aurekai.work /apps/aurekai-activity.mjs >/dev/null
tag=$(header ETag)
case "$(header Cache-Control)" in 'public, no-cache') ;; *) fail "editor code is kept as '$(header Cache-Control)'" ;; esac
code=$(curl -sS -o /dev/null -w '%{http_code}' -H 'Host: aurekai.work' -H "If-None-Match: $tag" "http://127.0.0.1:$port/apps/aurekai-activity.mjs")
[ "$code" = 304 ] || fail "editor code revalidation answered $code"
echo 'wire cache law: PASS'
