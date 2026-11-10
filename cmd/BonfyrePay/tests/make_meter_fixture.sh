#!/bin/sh
# Seed a meter store holding one key and one month of priced events, for
# tests that need a billable fixture without reaching any live store.
set -eu
meter=${1:?bonfyre-meter required}
out=${2:?output db required}
case "$meter" in /*) ;; *) meter="$(pwd)/$meter";; esac
"$meter" admit-rate --db "$out" --provider local --op transcribe --unit call --amount 0.5 \
  --evidence receipt:test:rate --effective-at 2026-10-01T00:00:00Z >/dev/null 2>&1
"$meter" record --db "$out" --key k1 --op transcribe --provider local --receipt "r:1" \
  --attempt-ref a1 --realization-ref z1 --at 2026-10-15T00:00:00Z >/dev/null 2>&1
"$meter" record --db "$out" --key k1 --op transcribe --provider local --receipt "r:2" \
  --attempt-ref a2 --realization-ref z2 --at 2026-10-16T00:00:00Z >/dev/null 2>&1
