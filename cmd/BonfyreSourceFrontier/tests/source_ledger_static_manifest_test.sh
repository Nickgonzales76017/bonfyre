#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
sqlite3 "$tmp/frontier.db" "
CREATE TABLE channel(state TEXT, census_source TEXT, exact_address TEXT);
INSERT INTO channel VALUES
  ('admitted','first','https://a.example/feed'),
  ('active','repeat','https://a.example/feed'),
  ('admitted','second','https://é.example/feed'),
  ('admitted','long','https://b.example/'||replace(hex(zeroblob(200)),'00','x')),
  ('admitted','null',NULL),('admitted','empty',''),
  ('candidate','excluded','https://candidate.example/'),
  ('admitted','','https://without-census.example/');
"
native="$root/cmd/BonfyreSourceFrontier/tools/source-ledger-static-manifest"
before=$(shasum -a 256 "$tmp/frontier.db")
"$native" --frontier "$tmp/frontier.db" --page-size 2 > "$tmp/manifest"
test "$(shasum -a 256 "$tmp/frontier.db")" = "$before"
test "$(wc -l < "$tmp/manifest" | tr -d ' ')" = 7
tab=$(printf '\t')
sed -n '1p' "$tmp/manifest" | grep -Fx "/channels?standing=admitted${tab}channels/__q/standing=admitted.html"
sed -n '2p' "$tmp/manifest" | grep -Fx "/channels?standing=admitted&from=2${tab}channels/__q/standing=admitted&from=2.html"
sed -n '3p' "$tmp/manifest" | grep -Fx "/channels?standing=admitted&from=4${tab}channels/__q/standing=admitted&from=4.html"
grep -Fx "/channels?${tab}channels/__q/.html" "$tmp/manifest"
grep -F '/channels?channel=https%3A%2F%2F%C3%A9.example%2Ffeed' "$tmp/manifest"
long_query="channel=https%3A%2F%2Fb.example%2F$(printf '%0200d' 0 | tr '0' 'x')"
digest=$(printf '%s' "$long_query" | shasum -a 256)
digest=${digest%% *}
grep -F "/channels?${long_query}${tab}channels/__q/h/${digest}.html" "$tmp/manifest"
if "$native" --frontier "$tmp/frontier.db" --page-size 0 > "$tmp/invalid" 2>/dev/null; then
    echo 'zero page size was accepted' >&2
    exit 1
fi
test ! -s "$tmp/invalid"
echo 'source ledger static manifest: PASS'
