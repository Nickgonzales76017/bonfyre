#!/bin/sh
set -eu

bin=${1:?native Stitch binary required}
repo=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

out=$("$bin" cache-stats --cache "$tmp/missing")
case "$out" in
  *"Cache: $tmp/missing"*) ;;
  *) echo "Stitch did not dispatch cache-stats" >&2; exit 1 ;;
esac

for verb in plan materialize prune compile layer-plan layer-composite resolve-bridges validate-layer-dag; do
  if "$bin" "$verb" >"$tmp/output" 2>&1; then
    echo "Stitch accepted incomplete $verb" >&2
    exit 1
  fi
done
if "$bin" unknown >"$tmp/output" 2>&1; then
  echo "Stitch accepted unknown verb" >&2
  exit 1
fi
"$bin" compile "$repo/recipes/TESTRUN1.json" --output "$tmp/compiled binary" --recipe-dir "$tmp" >"$tmp/output" 2>&1
[ -x "$tmp/compiled binary" ]
if "$bin" compile "$repo/cmd/BonfyreStitch/tests/invalid_recipe_id.json" --output "$tmp/outside" --recipe-dir "$tmp" >"$tmp/output" 2>&1; then
  echo "Stitch accepted a recipe_id outside its source directory" >&2
  exit 1
fi
[ ! -e "$tmp/outside_compiled.c" ]
echo "Stitch native dispatch: PASS"
