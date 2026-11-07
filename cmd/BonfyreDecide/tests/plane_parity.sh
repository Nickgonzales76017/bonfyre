#!/bin/sh
# bonfyre-decide plane against golden vectors recorded from the JS DecisionPlane
# (components/BonfyreDecision/decision-plane.mjs). Outputs are compared by value:
# the C digest of each output must equal the C digest of the recorded JS output,
# and the C digest itself is checked against digests the JS computed.
#   plane_parity.sh [BONFYRE_DECIDE]
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
bin=${1:-$here/../bonfyre-decide}
v=$here/plane
pass=0 fail=0
ok() { pass=$((pass + 1)); }
bad() { fail=$((fail + 1)); echo "FAIL $*" >&2; }
for f in "$v"/digest-*.json; do
  want=$(cat "${f%.json}.hex"); got=$("$bin" plane digest < "$f")
  [ "$got" = "$want" ] && ok || bad "digest $(basename "$f")"
done
for c in "$v"/*.case.json; do
  n=${c%.case.json}; name=$(basename "$n")
  if [ -f "$n.compile.error" ]; then
    if "$bin" plane compile < "$c" > /dev/null 2> "$n.stderr.tmp"; then bad "compile $name should refuse"
    elif grep -qF -- "$(cat "$n.compile.error")" "$n.stderr.tmp"; then ok
    else bad "compile $name refused with a different reason: $(cat "$n.stderr.tmp")"; fi
    rm -f "$n.stderr.tmp"; continue
  fi
  got=$("$bin" plane compile < "$c" | "$bin" plane digest)
  want=$("$bin" plane digest < "$n.compile.json")
  [ "$got" = "$want" ] && ok || bad "compile $name"
  if [ -f "$n.receipt.json" ]; then
    got=$("$bin" plane proposal < "$n.receipt.json" | "$bin" plane digest)
    want=$("$bin" plane digest < "$n.proposal.json")
    [ "$got" = "$want" ] && ok || bad "proposal $name"
  fi
done
echo "bonfyre-decide plane parity: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
