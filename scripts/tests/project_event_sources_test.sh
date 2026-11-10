#!/bin/sh
# What this does not prove:
# - a script written in this test stands where a program would run, so it shows how the caller handles that program's interface and nothing about the program.
# - some checks run in embedded Python, which this estate has retired; they say nothing about a native path.
# What this proves: the refiling script changes the record, through the content
# store itself. A script that wrote down its arguments used to stand in for the
# store, and the test confirmed the record was left as it was.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
schemas="$root/cmd/BonfyreCMS/content-types"
[ -x "$cms" ] || { echo "build cmd/BonfyreCMS first" >&2; exit 1; }
tmp=$(mktemp -d)
trap 'chmod -R u+w "$tmp" 2>/dev/null; rm -rf "$tmp"' EXIT INT TERM
mkdir -p "$tmp/estate"
db="$tmp/estate/cms.db"

"$cms" schema apply "$schemas/internet_source.json" --db "$db" --schemas "$schemas" >/dev/null 2>&1
add() { "$cms" entry create internet_source --ns root "$1" --db "$db" --schemas "$schemas" >/dev/null 2>&1; }
add '{"source":"event-a","kind":"event","family":"news","label":"Event A","says":"what is on","state":"admitted","status":"published"}'
add '{"source":"event-b","kind":"event","family":"events","label":"Event B","says":"what is on","state":"admitted","status":"published"}'
add '{"source":"event-c","kind":"event","family":"news","label":"Event C","says":"what is on","state":"admitted","status":"draft"}'
family() { sqlite3 "$db" "SELECT family FROM internet_source WHERE source='$1'"; }
[ "$(family event-a)" = news ]

# A preview names what would change and changes nothing.
bun "$root/scripts/project_event_sources.mjs" --estate "$tmp/estate" --repo "$root" > "$tmp/dry.json"
rg -q '"event_sources_to_refile": 1' "$tmp/dry.json"
rg -q '"event-a"' "$tmp/dry.json"
[ "$(family event-a)" = news ]

# Applied, the published event source is refiled in the store; the draft and
# the one already filed are left alone.
bun "$root/scripts/project_event_sources.mjs" --estate "$tmp/estate" --repo "$root" --apply > "$tmp/apply.json"
rg -q '"event_sources_refiled": 1' "$tmp/apply.json"
rg -q '"refused": 0' "$tmp/apply.json"
[ "$(family event-a)" = events ]
[ "$(family event-b)" = events ]
[ "$(family event-c)" = news ]

# A second run finds nothing left to do.
bun "$root/scripts/project_event_sources.mjs" --estate "$tmp/estate" --repo "$root" > "$tmp/again.json"
rg -q '"event_sources_to_refile": 0' "$tmp/again.json"

# A store that cannot be written refuses, and the refusal is reported.
add '{"source":"event-d","kind":"event","family":"news","label":"Event D","says":"what is on","state":"admitted","status":"published"}'
chmod a-w "$db" "$tmp/estate"
if bun "$root/scripts/project_event_sources.mjs" --estate "$tmp/estate" --repo "$root" --apply > "$tmp/refused.json"; then
    echo 'a store that refused the write was reported as written' >&2
    exit 1
fi
rg -q '"refused": 1' "$tmp/refused.json"
chmod u+w "$tmp/estate" "$db"
[ "$(family event-d)" = news ]
echo 'project event sources: PASS'
