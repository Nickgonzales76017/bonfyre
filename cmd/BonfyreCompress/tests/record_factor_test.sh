#!/bin/sh
set -eu
binary=${1:?compress binary required}
case "$binary" in /*) ;; *) binary="$(pwd)/$binary";; esac
scratch=$(mktemp -d "${TMPDIR:-/tmp}/bf-record-factor.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
printf '%s\r\n' '{"id":"first","name":"Oats base","shared":{"phase":"prepare"},"amount":1.2300}' > "$scratch/source"
printf '%s' '{"id":"second","name":"Oats variation","shared":{"phase":"prepare"},"amount":1.2300}' >> "$scratch/source"
"$binary" factor-records "$scratch/source" "$scratch/pack" > "$scratch/report"
"$binary" factor-export "$scratch/pack" > "$scratch/export"
cmp "$scratch/source" "$scratch/export"
"$binary" factor-verify "$scratch/pack" "$scratch/source" > "$scratch/verify"
grep -q 'identical_records' "$scratch/verify"
"$binary" factor-search "$scratch/pack" Oats > "$scratch/search"
[ "$(wc -l < "$scratch/search" | tr -d ' ')" = 3 ]
"$binary" factor-read "$scratch/pack" 1 > "$scratch/first"
printf '%s\r\n' '{"id":"first","name":"Oats base","shared":{"phase":"prepare"},"amount":1.2300}' > "$scratch/expected"
cmp "$scratch/first" "$scratch/expected"
if "$binary" factor-records "$scratch/source" "$scratch/pack" >/dev/null 2>&1; then exit 1; fi
if "$binary" factor-read "$scratch/pack" '1oops' >/dev/null 2>&1; then exit 1; fi
if "$binary" factor-search "$scratch/pack" '"' >/dev/null 2>&1; then exit 1; fi
printf '\n{"id":"third","name":"Missing from pack"}\n' >> "$scratch/export"
if "$binary" factor-verify "$scratch/pack" "$scratch/export" >/dev/null 2>&1; then exit 1; fi
"$binary" factor-export "$scratch/pack" > "$scratch/unchanged"
cmp "$scratch/source" "$scratch/unchanged"
printf '%s\n' 'record_factor_test: PASS (normal Compress commands, exact bytes, search, refused incomplete verification)'
