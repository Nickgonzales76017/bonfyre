#!/bin/sh
# The physics field must never consume a state built from another vector width.
set -eu
bin=${1:-./bonfyre-physics}
pack=${BONFYRE_EMBED_PACK:-"$HOME/.local/share/bonfyre/embeds/pack.bfpack"}
if [ ! -f "$pack" ]; then
  echo "physics geometry admission: SKIP (no installed pack)"
  exit 0
fi
tmp=$(mktemp -d "${TMPDIR:-/tmp}/physics-dim.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
printf "VECF\020\000\000\000" > "$tmp/wrong.vecf"
dd if=/dev/zero bs=64 count=1 2>/dev/null >> "$tmp/wrong.vecf"
printf "VECF\040\000\000\000" > "$tmp/valid.vecf"
dd if=/dev/zero bs=128 count=1 2>/dev/null >> "$tmp/valid.vecf"
if "$bin" init "$tmp/wrong.vecf" --pack "$pack" --state "$tmp/rejected.bfps" >"$tmp/invalid-out" 2>"$tmp/invalid-err"; then
  echo "physics geometry admission: mismatched pack init succeeded" >&2
  exit 1
fi
test ! -e "$tmp/rejected.bfps"
grep -F "input dim 16 != pack dim 32" "$tmp/invalid-err" >/dev/null
# Create the state only in an isolated non-pack coordinate; it may not execute against the real pack.
"$bin" init "$tmp/wrong.vecf" --pack "$tmp/absent.bfpack" --state "$tmp/wrong.bfps" >/dev/null
"$bin" bvh --pack "$pack" --out "$tmp/index.bfvh" >/dev/null
for op in step run collide energy; do
  case "$op" in
    step) set -- step --n 1 ;;
    run) set -- run --steps 1 ;;
    collide) set -- collide --k 1 ;;
    energy) set -- energy ;;
  esac
  if "$bin" "$@" --state "$tmp/wrong.bfps" --pack "$pack" --bvh "$tmp/index.bfvh" >"$tmp/$op.out" 2>"$tmp/$op.err"; then
    echo "physics geometry admission: mismatched state admitted to $op" >&2
    exit 1
  fi
  grep -F "state dim 16 != pack dim 32" "$tmp/$op.err" >/dev/null
done
"$bin" init "$tmp/valid.vecf" --pack "$pack" --state "$tmp/valid.bfps" >/dev/null
test -s "$tmp/valid.bfps"
"$bin" show --state "$tmp/valid.bfps" | grep -E "dim[[:space:]]*=[[:space:]]*32" >/dev/null
echo "physics geometry admission: PASS (bad init refused without state; four execution gates refuse; valid persisted)"
