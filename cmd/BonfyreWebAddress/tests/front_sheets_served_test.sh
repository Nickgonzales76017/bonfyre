#!/bin/sh
# Every sheet a territory front links is one the server answers. The four
# front sheets were linked but never declared, so each apex answered 404 for
# its own styling and was drawn by world.css alone.
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
here=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
root=$(CDPATH= cd -- "$here/../.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-front-sheets.XXXXXX")
pid=''
trap '[ -n "$pid" ] && kill "$pid" 2>/dev/null; rm -rf "$tmp"' EXIT INT TERM

sheets=$(grep -ohE 'href=\\"/assets/[A-Za-z0-9._-]+\.css' "$here"/src/app_shell.h "$here"/src/front_*.h | sed 's#.*"/assets/##' | sort -u)
[ -n "$sheets" ] || { echo 'front sheets served: no front links a sheet' >&2; exit 1; }

"$web" bootstrap-owned --db "$tmp/matrix.db" >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
"$web" serve --db "$tmp/matrix.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
attempt=0
until curl -sS -o /dev/null "http://127.0.0.1:$port/healthz" 2>/dev/null; do
    attempt=$((attempt + 1))
    [ "$attempt" -lt 80 ] && kill -0 "$pid" 2>/dev/null || { echo 'front sheets served: server did not start' >&2; exit 1; }
    sleep 0.05
done
for sheet in $sheets; do
    answer=$(curl -sS -o "$tmp/sheet" -w '%{http_code} %{content_type}' -H 'Host: aurekai.life' "http://127.0.0.1:$port/assets/$sheet")
    case "$answer" in
        "200 text/css"*) cmp -s "$tmp/sheet" "$root/site/matrix/$sheet" || { echo "front sheets served: $sheet differs from the site root" >&2; exit 1; } ;;
        *) echo "front sheets served: /assets/$sheet answered $answer" >&2; exit 1 ;;
    esac
done
echo "front sheets served: PASS ($(echo $sheets | wc -w | tr -d ' ') sheets)"
