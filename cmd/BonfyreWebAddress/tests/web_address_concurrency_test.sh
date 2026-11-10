#!/bin/sh
# A process-pool witness, not a synthetic route benchmark.  Each request runs
# the ordinary native Matrix root compiler; the only special condition is that
# four independent workers must accept one shared edge socket without changing
# the resolved public subject or leaking a process-local closure.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
bin=${1:-"$root/cmd/BonfyreWebAddress/bonfyre-web-address"}
tmp=$(mktemp -d)
parent=''
cleanup() {
    if [ -n "$parent" ]; then
        kill "$parent" 2>/dev/null || true
        # The supervisor normally terminates the whole pool.  This is a
        # defensive cleanup for interrupted shell-test runs.
        ps -ax -o pid= -o ppid= 2>/dev/null | awk -v parent="$parent" '$2 == parent { print $1 }' |
            xargs -n 1 kill 2>/dev/null || true
    fi
    rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

db="$tmp/matrix.db"
port=18878
"$bin" bootstrap-owned --db "$db" >/dev/null
"$bin" serve --db "$db" --port "$port" --workers 4 --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
parent=$!

ready=0
for _ in 1 2 3 4 5 6 7 8 9 10; do
    if curl -fsS --max-time 1 "http://127.0.0.1:$port/healthz" | grep -q '"verdict":"healthy"'; then
        ready=1
        break
    fi
    sleep 1
done
[ "$ready" = 1 ] || { cat "$tmp/server.log" >&2; exit 1; }

children=$(ps -ax -o pid= -o ppid= 2>/dev/null | awk -v parent="$parent" '$2 == parent { n++ } END { print n + 0 }')
[ "$children" -ge 4 ] || { echo "expected four fresh native edge workers, found $children" >&2; exit 1; }

pids=''
i=0
while [ "$i" -lt 128 ]; do
    curl -fsS --max-time 5 -H 'Host: aurekai.live' "http://127.0.0.1:$port/" >"$tmp/$i.html" &
    pids="$pids $!"
    i=$((i + 1))
done
for request in $pids; do wait "$request"; done

for page in "$tmp"/*.html; do
    grep -q '"subject":"institution:aurekai:live"' "$page"
    grep -q '<body class="live">' "$page"
done

echo "web address prefork concurrency: PASS"
