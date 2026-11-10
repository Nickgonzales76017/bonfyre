#!/bin/sh
set -eu
bin=${1:?binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT

"$bin" audit > "$root/audit.json"
grep -q '"worlds":5' "$root/audit.json"
grep -q '"featured_worlds":1' "$root/audit.json"
grep -q '"source_occurrences":25' "$root/audit.json"
grep -q '"unique_sources":17' "$root/audit.json"
grep -q '"bad_truth_rows":0' "$root/audit.json"
grep -q '"entry_surface_projection":22' "$root/audit.json"

"$bin" show dallas-tonight > "$root/dallas.json"
grep -q '"entry_world":"dallas-tonight"' "$root/dallas.json"
grep -q '"entry_surface":"tonight"' "$root/dallas.json"
grep -q 'DART GTFS Realtime' "$root/dallas.json"
grep -q 'TXC113' "$root/dallas.json"
grep -q 'candidate_unobserved_external' "$root/dallas.json"

"$bin" impact market > "$root/market.json"
grep -q 'tabc-license-public-inquiry' "$root/market.json"
grep -q 'ercot-market-information' "$root/market.json"
grep -q 'candidate_projection_impact_only' "$root/market.json"

"$bin" shared > "$root/shared.tsv"
grep -q "$(printf 'nws-dallas-alerts\t3')" "$root/shared.tsv"
grep -q "$(printf 'drivetexas-conditions\t3')" "$root/shared.tsv"
grep -q "$(printf 'dart-gtfs-realtime\t2')" "$root/shared.tsv"
grep -q "$(printf 'ercot-system-conditions\t2')" "$root/shared.tsv"

echo 'showcase source constellations -> world/surface candidate impacts + shared-source reuse: PASS'
