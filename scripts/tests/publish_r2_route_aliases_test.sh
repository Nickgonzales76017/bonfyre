#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d)
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT INT TERM
mkdir -p "$tmp/dist/discover"
printf '<h1>home</h1>' > "$tmp/dist/index.html"
printf '<h1>discover</h1>' > "$tmp/dist/discover/13.html"
printf 'not an alias' > "$tmp/dist/robots.txt"
count=$("$root/scripts/publish_r2_route_aliases.sh" --source "$tmp/dist" --prepare-only --out "$tmp/aliases")
[ "$count" = 2 ]
cmp "$tmp/dist/index.html" "$tmp/aliases/index"
cmp "$tmp/dist/discover/13.html" "$tmp/aliases/discover/13"
[ ! -e "$tmp/aliases/robots.txt" ]
if "$root/scripts/publish_r2_route_aliases.sh" --source "$tmp/dist" --prepare-only --verify --out "$tmp/aliases" >/dev/null 2>&1; then
  echo 'prepare-only and verify must be exclusive' >&2
  exit 1
fi
echo 'r2 route aliases: PASS'
