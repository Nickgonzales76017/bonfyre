#!/bin/sh
set -eu
bin=${1:?binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT

"$bin" audit > "$root/audit.json"
grep -q '"worlds":5' "$root/audit.json"
grep -q '"entry_missing":0' "$root/audit.json"
grep -q '"visitor_path_missing":0' "$root/audit.json"
grep -q '"unknown_projection_surfaces":0' "$root/audit.json"
grep -q '"unknown_source_impact_surfaces":0' "$root/audit.json"
grep -q '"featured_projection_count":22' "$root/audit.json"

"$bin" project dallas-tonight > "$root/dallas.json"
grep -q '"schema":"bonfyre-public-world.v1"' "$root/dallas.json"
grep -q '"entry_surface":"tonight"' "$root/dallas.json"
grep -q '"surface":"habitat"' "$root/dallas.json"
grep -q '"surface":"market"' "$root/dallas.json"
grep -q '"surface":"console"' "$root/dallas.json"
grep -q '"surface":"link"' "$root/dallas.json"
grep -q 'dart-gtfs-realtime' "$root/dallas.json"
grep -q 'tabc-license-public-inquiry' "$root/dallas.json"
grep -q 'owner_projection_plus_unobserved_source_candidates' "$root/dallas.json"
[ "$(sqlite3 :memory: "SELECT json_array_length(json_extract(CAST(readfile('$root/dallas.json') AS TEXT),'$.projections')); ")" = 22 ]

"$bin" project storm-cell-dallas > "$root/storm.json"
grep -q 'drivetexas-conditions' "$root/storm.json"
grep -q 'ercot-system-conditions' "$root/storm.json"
grep -q '"truth":"world_projection_plan_not_owner_state"' "$root/storm.json"

echo 'surface maturity + showcase estate -> consumable PublicWorld projection: PASS'
