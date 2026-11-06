#!/bin/sh
# The static estate must retain all source-bound credited-name addresses, not
# only the directory page. A long coordinate remains semantic in the URL and
# lowers only its carrier leaf to a digest.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
sqlite3 "$tmp/items.db" "
CREATE TABLE item(source TEXT, by TEXT);
INSERT INTO item VALUES ('source-a','Alex & Co'),('source-b','Alex & Co'),
  ('source-a','Robin'),('source-'||replace(hex(zeroblob(500)),'00','x'),'Long Name'),
  ('source-α','Zoë'),('source-z','Zero'||char(0)||'Byte');
"
native="$root/cmd/BonfyreWebAddress/tools/people-static-manifest"
before=$(shasum -a 256 "$tmp/items.db")
"$native" --archive "$tmp/items.db" > "$tmp/manifest"
[ "$(wc -l < "$tmp/manifest" | tr -d ' ')" = 11 ]
tab=$(printf '\t')
digest=$(printf '%s' 'who=Alex%20%26%20Co' | shasum -a 256)
digest=${digest%% *}
grep -F "/people?who=Alex%20%26%20Co${tab}people/__q/h/${digest}.html" "$tmp/manifest"
grep -Eq "^/people\\?who=Alex%20%26%20Co${tab}people/__q/h/[0-9a-f]{64}\\.html$" "$tmp/manifest"
grep -Eq "^/people\\?who=Alex%20%26%20Co&source=source-a${tab}people/__q/h/[0-9a-f]{64}\\.html$" "$tmp/manifest"
grep -Eq "^/people\\?who=Alex%20%26%20Co&source=source-b${tab}people/__q/h/[0-9a-f]{64}\\.html$" "$tmp/manifest"
grep -Eq '^/people\?who=Long%20Name&source=source-x+.*people/__q/h/[0-9a-f]{64}\.html$' "$tmp/manifest"
grep -Eq '^/people\?who=Zo%C3%AB&source=source-%CE%B1' "$tmp/manifest"
grep -Eq '^/people\?who=Zero%00Byte&source=source-z' "$tmp/manifest"
"$native" --archive "$tmp/items.db" --null > "$tmp/manifest.nul"
test "$(shasum -a 256 "$tmp/items.db")" = "$before"
[ "$(tr -cd '\000' < "$tmp/manifest.nul" | wc -c | tr -d ' ')" = 22 ]
tr '\000' '\n' < "$tmp/manifest.nul" | grep -Fx '/people?who=Alex%20%26%20Co'
sqlite3 "$tmp/items.db" "ALTER TABLE item ADD COLUMN family TEXT NOT NULL DEFAULT 'news';"
sqlite3 "$tmp/items.db" "ALTER TABLE item ADD COLUMN publisher TEXT NOT NULL DEFAULT '';"
sqlite3 "$tmp/items.db" "ALTER TABLE item ADD COLUMN at INTEGER NOT NULL DEFAULT 0;"
"$native" --archive "$tmp/items.db" --profile-hosts --null > "$tmp/profiles.nul"
[ "$(tr -cd '\000' < "$tmp/profiles.nul" | wc -c | tr -d ' ')" = 6 ]
tr '\000' '\n' < "$tmp/profiles.nul" > "$tmp/profiles.lines"
grep -Eq '^/people\?who=Alex%20%26%20Co&source=source-a&profile=1$' "$tmp/profiles.lines"
grep -Eq '^/people\?who=Alex%20%26%20Co&source=source-b&profile=1$' "$tmp/profiles.lines"
[ "$(grep -Ec '^hosts/credited-[a-z0-9-]+-[0-9a-f]{20}\.aurekai\.work/index\.html$' "$tmp/profiles.lines")" = 3 ]
[ "$(grep '^hosts/' "$tmp/profiles.lines" | sort -u | wc -l | tr -d ' ')" = 3 ]
! grep -q 'who=Robin&source=' "$tmp/profiles.lines"
! grep -q 'who=Zo' "$tmp/profiles.lines"
sqlite3 "$tmp/cms.db" "CREATE TABLE internet_source(source TEXT,status TEXT,label TEXT,publisher_name TEXT);
INSERT INTO internet_source VALUES
('source-a','published','Alex & Co','Source A'),
('source-b','published','Independent B','Independent B');"
"$native" --archive "$tmp/items.db" --cms "$tmp/cms.db" --profile-hosts --null > "$tmp/admitted.nul"
[ "$(tr -cd '\000' < "$tmp/admitted.nul" | wc -c | tr -d ' ')" = 4 ]
tr '\000' '\n' < "$tmp/admitted.nul" > "$tmp/admitted.lines"
! grep -q '^/people?who=Alex%20%26%20Co&source=source-a' "$tmp/admitted.lines"
grep -q '^/people?who=Alex%20%26%20Co&source=source-b&profile=1$' "$tmp/admitted.lines"


echo 'people static manifest: PASS'
