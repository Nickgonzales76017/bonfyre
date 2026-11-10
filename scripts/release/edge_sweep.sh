#!/bin/sh
# Every published subdomain, not a sample. The local pass checks that each of
# the edge pages names only sheets and scripts the upload carries; the live pass
# asks each host once, slowly, and each linked house asset once. A 429 is the
# Worker's daily cap and is counted, never read as a broken page.
# Findings go to stdout as tab-separated lines: FAIL|NOTE host kind detail.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
assets="$root/deploy/worker/edge-assets"
live=1 jobs=${BONFYRE_SWEEP_JOBS:-4}
[ "${1:-}" = --local ] && live=0
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-edge-sweep.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

ls "$assets/h" > "$tmp/hosts"
total=$(wc -l < "$tmp/hosts" | tr -d ' ')
while read -r host; do
    page="$assets/h/$host/index.html"
    [ -s "$page" ] || { printf 'FAIL\t%s\tlocal-page\tno index.html\n' "$host"; continue; }
    grep -c 'rel="stylesheet" href="/assets/' "$page" >/dev/null || printf 'FAIL\t%s\tlocal-unstyled\tno house sheet linked\n' "$host"
    grep -oE '(href|src)="/(assets|apps)/[^"?#]+' "$page" | sed 's/.*="//' | sort -u | while read -r ref; do
        [ -e "$assets$ref" ] || [ -e "$assets/h/$host$ref" ] || printf 'FAIL\t%s\tlocal-missing\t%s\n' "$host" "$ref"
        echo "$ref" >> "$tmp/refs"
    done
done < "$tmp/hosts"

if [ "$live" = 1 ]; then
    # One request per host; the -w line carries the answer so xargs can run a few at once.
    xargs -P "$jobs" -I{} sh -c 'curl -sS -o /dev/null --max-time 20 -w "{} %{http_code} %{time_total}\n" "https://{}/" 2>/dev/null || echo "{} 000 0"; sleep 0.2' < "$tmp/hosts" > "$tmp/live"
    awk '$2==429{t++} $2!=200&&$2!=429{printf "FAIL\t%s\tlive-status\t%s\n",$1,$2} $2==200&&$3>3{printf "NOTE\t%s\tlive-slow\t%ss\n",$1,$3} END{printf "NOTE\t*\tlive-throttled\t%d\n",t+0}' "$tmp/live" | grep -v 'live-throttled	0$' || true
    first=$(head -1 "$tmp/hosts")
    sort -u "$tmp/refs" 2>/dev/null | grep '\.css$' | while read -r ref; do
        answer=$(curl -sS -o /dev/null --max-time 20 -w '%{http_code} %{content_type}' "https://$first$ref" 2>/dev/null || echo 000)
        case "$answer" in "200 text/css"*|429*) ;; *) printf 'FAIL\t%s\tlive-sheet\t%s %s\n' "$first" "$ref" "$answer" ;; esac
    done
fi
echo "EDGE_SWEEP hosts=$total live=$live" >&2
