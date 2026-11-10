#!/bin/sh
# What this does not prove:
# - a script written in this test stands where a program would run, so it shows how the caller handles that program's interface and nothing about the program.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
sqlite3 "$tmp/frontier.db" "
CREATE TABLE channel(state TEXT, census_source TEXT, exact_address TEXT);
INSERT INTO channel VALUES ('admitted','source-a','https://a.example/feed'),
  ('active','source-b','https://b.example/feed'),
  ('candidate','not-public','https://candidate.example/feed');
"
# The pages come from the estate's own web server. A script named curl used to
# answer in its place with one fixed line of HTML.
origin=${BONFYRE_TEST_ORIGIN:-https://aurekai.life}
[ "$(curl -s -o /dev/null -m 15 -w '%{http_code}' "$origin/channels")" = 200 ] || { echo "no estate web server answers at $origin" >&2; exit 1; }
sh "$root/scripts/export_static_estate.sh" \
    --source-ledger-only --frontier "$tmp/frontier.db" --out "$tmp/out" \
    --from "$origin" --parallel 1 > "$tmp/export.log"
test "$(find "$tmp/out/channels/__q" -type f -name '*.html' | wc -l | tr -d ' ')" = 3
for page in $(find "$tmp/out/channels/__q" -type f -name '*.html'); do grep -q '<!doctype html>' "$page"; done
test ! -e "$tmp/out/.source-ledger.list"
rg -q 'wrote 3 source-ledger pages \(0 refused\)' "$tmp/export.log"
echo 'source ledger static export: PASS'
