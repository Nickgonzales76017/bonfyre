#!/bin/bash
# Golden vectors for bonfyre-atlas over tests/fixture (a small atlas that
# breaks every rule once). Each case runs in a fresh copy; stdout, exit status
# and the files it writes (index, organ catalogue, WIRING.md) are recorded,
# with generated_at timestamps normalised. The vectors were recorded from the
# retired architecture/*.mjs atlas, so they are its behaviour.
#   tests/parity.sh                 check the C build
#   tests/parity.sh --record JSDIR  record from atlas.mjs and friends in JSDIR
set -u
here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd); organ=$(dirname "$here")
mode=check; js=""
if [ "${1:-}" = --record ]; then mode=record; js=$(CDPATH='' cd -- "$2" && pwd); fi
make -s -C "$organ" >/dev/null || exit 1
mkdir -p "$here/golden"; n=0; fail=0
while IFS= read -r line; do
  n=$((n + 1)); tmp=$(mktemp -d); cp -R "$here/fixture/architecture" "$tmp/"
  read -ra args <<< "$line"
  case "${args[0]}" in   # plant an outdated or unstamped index first
    STALE) cp "$here/fixture/stale-index.json" "$tmp/architecture/atlas.index.json"; args=("${args[@]:1}") ;;
    UNSTAMPED) cp "$here/fixture/unstamped-index.json" "$tmp/architecture/atlas.index.json"; args=("${args[@]:1}") ;;
  esac
  if [ $mode = record ]; then
    cp "$js"/atlas.mjs "$js"/atlas-core.mjs "$js"/atlas-index.mjs "$js"/atlas-query.mjs "$js"/wiring.mjs "$tmp/architecture/"
    if [ "${args[0]}" = query ]; then (cd "$tmp" && bun architecture/atlas-query.mjs "${args[@]:1}" > "$tmp/out" 2>&1; echo "exit $?" >> "$tmp/out")
    else (cd "$tmp" && bun architecture/atlas.mjs "${args[@]}" > "$tmp/out" 2>&1; echo "exit $?" >> "$tmp/out"); fi
  else (cd "$tmp" && "$organ/bonfyre-atlas" "${args[@]}" > "$tmp/out" 2>&1; echo "exit $?" >> "$tmp/out"); fi
  for f in atlas.index.json atlas.organs.json WIRING.md; do
    [ -f "$tmp/architecture/$f" ] && { printf '== %s\n' "$f"; cat "$tmp/architecture/$f"; echo; } >> "$tmp/out"
  done
  sed -E 's/"generated_at": "[^"]*"/"generated_at": "T"/' "$tmp/out" > "$tmp/got"
  if [ $mode = record ]; then cp "$tmp/got" "$here/golden/$n"
  elif ! cmp -s "$tmp/got" "$here/golden/$n"; then fail=$((fail + 1)); echo "FAIL $n: $line"; diff "$here/golden/$n" "$tmp/got" | head -10; fi
  rm -rf "$tmp"
done < "$here/cases.txt"
[ $mode = record ] && { echo "recorded $n vectors"; exit 0; }
outside=$(mktemp -d)
outside_summary=$(cd "$outside" && env -u BONFYRE_ATLAS_DIR -u BONFYRE_REPO_ROOT -u BONFYRE_ROOT "$organ/bonfyre-atlas" summary 2>/dev/null)
if ! printf '%s\n' "$outside_summary" | grep -Eq '^atlas: [1-9][0-9]* architectures'; then
  fail=$((fail + 1)); echo "FAIL checkout-relative atlas discovery from unrelated cwd"
fi
if BONFYRE_ATLAS_DIR="$outside/missing" "$organ/bonfyre-atlas" summary >/dev/null 2>&1; then
  fail=$((fail + 1)); echo "FAIL invalid BONFYRE_ATLAS_DIR did not refuse"
fi
rm -rf "$outside"
[ $fail = 0 ] && echo "bonfyre-atlas parity: $n vectors PASS"
exit $fail
