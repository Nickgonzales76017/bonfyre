#!/bin/sh
set -eu
bin=${1:?binary required}
estate=${2:?estate jsonl required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT

"$bin" audit --estate "$estate" > "$root/audit.json"
grep -q '"showcase_sources":17' "$root/audit.json"
grep -q '"kr_dart_false_collisions":0' "$root/audit.json"
grep -q '"truth":"identity_and_semantic_crosswalk_only"' "$root/audit.json"

"$bin" emit --estate "$estate" --out "$root/crosswalk.jsonl" > "$root/emit.json"
grep -q '"rows":17' "$root/emit.json"
[ "$(wc -l < "$root/crosswalk.jsonl" | tr -d ' ')" = 17 ]
grep -q '"source":"dart-gtfs-schedule"' "$root/crosswalk.jsonl"
grep -q '"source":"aviationweather"' "$root/crosswalk.jsonl"
grep -q '"truth":"crosswalk_only_not_observation_or_admission"' "$root/crosswalk.jsonl"
if grep -q 'KR-DART Events' "$root/crosswalk.jsonl"; then
  echo 'Dallas transit was falsely crosswalked to KR-DART Events' >&2
  exit 1
fi

echo 'showcase source candidates -> exact source/host/protocol ExternalEstate crosswalk: PASS'
