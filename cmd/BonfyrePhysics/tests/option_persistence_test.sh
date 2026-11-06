#!/bin/sh
# Invalid parameters, unwritable states and malformed traces are refused, and
# a refusal leaves the last good state byte-identical. The pack is a real
# static-embedding pack built in an isolated HOME; nothing outside $tmp moves.
set -eu
bin=${1:-./bonfyre-physics}
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
embed=${BONFYRE_EMBED_BIN:-$root/cmd/BonfyreEmbed/bonfyre-embed}
model=${BONFYRE_STATIC_MODEL:-$HOME/BonfyreModels/static/nomic-static-128.bfse}
if [ ! -x "$embed" ] || [ ! -f "$model" ]; then
  echo "physics option/persistence: SKIP (no bonfyre-embed build or static model)"
  exit 0
fi
case "$bin" in /*) ;; *) bin=$(pwd)/$bin ;; esac
tmp=$(mktemp -d "${TMPDIR:-/tmp}/physics-opts.XXXXXX")
trap 'chmod -R u+w "$tmp" 2>/dev/null; rm -rf "$tmp"' EXIT HUP INT TERM
fail() { echo "physics option/persistence: $*" >&2; exit 1; }
refuse() { # refuse NAME MESSAGE -- argv...
  name=$1; msg=$2; shift 3
  if HOME="$tmp/home" "$bin" "$@" >"$tmp/$name.out" 2>"$tmp/$name.err"; then fail "$name unexpectedly succeeded"; fi
  grep -F -- "$msg" "$tmp/$name.err" >/dev/null || fail "$name: expected '$msg', got: $(cat "$tmp/$name.err")"
}
mkdir -p "$tmp/home"
i=0
for t in "checkout pricing on Friday" "parcel owner and assessed value" "transit schedule downtown" \
         "historic tax credit authority" "mixed beverage gross receipts" "zoning overlay and deed restrictions"; do
  i=$((i + 1)); printf '{"id":"p%d","text":"%s"}\n' "$i" "$t"
done >"$tmp/corpus.jsonl"
HOME="$tmp/home" "$embed" static --model "$model" --input-jsonl "$tmp/corpus.jsonl" --store --out "$tmp/vecs.jsonl"
HOME="$tmp/home" "$embed" pack >/dev/null
pack="$tmp/home/.local/share/bonfyre/embeds/pack.bfpack"
test -s "$pack" || fail "static store produced no pack"
P="--pack $pack --bvh $tmp/i.bfvh"
HOME="$tmp/home" "$bin" bvh --pack "$pack" --out "$tmp/i.bfvh" | grep -F "6 vecs  dim=128" >/dev/null || fail "bvh over static pack"

for v in abc 0 -1 nan inf 1e999 1.0x; do
  refuse "sigma$v" "--sigma needs a positive finite number" -- init --pack-row 0 --sigma "$v" --pack "$pack" --state "$tmp/bad.bfps"
  refuse "dt$v" "--dt needs a positive finite number" -- init --pack-row 0 --dt "$v" --pack "$pack" --state "$tmp/bad.bfps"
done
refuse row-range "pack row 6 >= 6 objects" -- init --pack-row 6 --pack "$pack" --state "$tmp/bad.bfps"
refuse row-neg "--pack-row needs" -- init --pack-row -2 --pack "$pack" --state "$tmp/bad.bfps"
refuse row-and-file "exactly one of" -- init x.vecf --pack-row 0 --pack "$pack" --state "$tmp/bad.bfps"
refuse no-input "exactly one of" -- init --pack "$pack" --state "$tmp/bad.bfps"
test ! -e "$tmp/bad.bfps" || fail "a refused init persisted a state"

HOME="$tmp/home" "$bin" init --pack-row 0 --sigma 0.5 --dt 0.01 --pack "$pack" --state "$tmp/s.bfps" >"$tmp/init.out"
grep -F "dim    = 128" "$tmp/init.out" >/dev/null || fail "pack-row init dim"
good=$(shasum -a 256 "$tmp/s.bfps" | awk '{print $1}')
refuse n0 "--n needs" -- step --n 0 $P --state "$tmp/s.bfps"
refuse nabc "--n needs" -- step --n abc $P --state "$tmp/s.bfps"
refuse steps "--steps needs" -- run --steps -3 $P --state "$tmp/s.bfps"
refuse k "--k needs" -- collide --k 0 $P --state "$tmp/s.bfps"
refuse scale "--scale needs a finite number" -- kick x.vecf --scale nan --state "$tmp/s.bfps"
refuse trace-dir "cannot open trace" -- run --steps 2 --trace "$tmp/absent/dir/t.jsonl" $P --state "$tmp/s.bfps"
[ "$(shasum -a 256 "$tmp/s.bfps" | awk '{print $1}')" = "$good" ] || fail "a refused command changed the state"

# Unwritable state directory: the step happens in memory, the save is refused,
# the exit code says so, and the old bytes stay.
mkdir "$tmp/ro"; cp "$tmp/s.bfps" "$tmp/ro/s.bfps"; chmod 555 "$tmp/ro"
refuse persist "cannot persist state" -- run --steps 1 $P --state "$tmp/ro/s.bfps"
chmod 755 "$tmp/ro"
[ "$(shasum -a 256 "$tmp/ro/s.bfps" | awk '{print $1}')" = "$good" ] || fail "failed persist altered the state"

HOME="$tmp/home" "$bin" run --steps 3 --trace "$tmp/t.jsonl" $P --state "$tmp/s.bfps" >/dev/null
HOME="$tmp/home" "$bin" trace "$tmp/t.jsonl" --replay | grep -F "3 events replayed" >/dev/null || fail "valid trace replay"
cp "$tmp/t.jsonl" "$tmp/bad.jsonl"; echo 'not json' >>"$tmp/bad.jsonl"
refuse trace-garbage "malformed event 4" -- trace "$tmp/bad.jsonl" --replay
printf '{"step":1,"gap":0}\n' >"$tmp/noh.jsonl"
refuse trace-noh "malformed event 1" -- trace "$tmp/noh.jsonl" --summary
: >"$tmp/empty.jsonl"
refuse trace-empty "no events" -- trace "$tmp/empty.jsonl"
refuse trace-missing "cannot open" -- trace "$tmp/missing.jsonl"

printf "VECF\020\000\000\000" >"$tmp/w.vecf"; dd if=/dev/zero bs=64 count=1 2>/dev/null >>"$tmp/w.vecf"
HOME="$tmp/home" "$bin" init "$tmp/w.vecf" --pack "$tmp/absent.bfpack" --state "$tmp/w16.bfps" >/dev/null
cp "$tmp/s.bfps" "$tmp/s2.bfps"
refuse compare-dim "state dim 16 != pack dim 128" -- compare "$tmp/s.bfps" "$tmp/w16.bfps" $P
HOME="$tmp/home" "$bin" compare "$tmp/s.bfps" "$tmp/s2.bfps" $P >/dev/null || fail "compare of two admitted states"
echo "physics option/persistence: PASS (bad sigma/dt/n/steps/k/scale/row refused without state change; unwritable state and trace refused; malformed traces refused; compare gated)"
