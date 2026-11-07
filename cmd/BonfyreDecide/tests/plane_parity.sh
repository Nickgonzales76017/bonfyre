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
# assess: hosted providers, planned and executed through the recorded transport
export TYPESAFE_API_KEY=fixture-key CLOUDFLARE_API_TOKEN=fixture-key BONFYRE_DECISION_CURL="$here/fake_transport.sh"
cap=$(mktemp -d "${TMPDIR:-/tmp}/plane-assess.XXXXXX"); trap 'rm -rf "$cap"' 0
nolat() { sed -E 's/"latency_ms":[0-9]+,?//'; }
for i in "$v"/*.assess-*-planned.in.json; do
  got=$("$bin" plane assess < "$i" | "$bin" plane digest)
  want=$("$bin" plane digest < "${i%.in.json}.json")
  [ "$got" = "$want" ] && ok || bad "assess $(basename "$i")"
done
for i in "$v"/*.assess-*-executed.in.json; do
  stem=${i%.in.json}
  out=$(BONFYRE_FAKE_TRANSPORT="$stem.transport.json" BONFYRE_FAKE_CAPTURE="$cap/x" "$bin" plane assess < "$i")
  got=$(printf '%s' "$out" | nolat | "$bin" plane digest)
  want=$(nolat < "$stem.json" | "$bin" plane digest)
  [ "$got" = "$want" ] && ok || bad "assess $(basename "$i")"
  if grep -q fixture-key "$cap/x.body" || ! grep -q 'Bearer fixture-key' "$cap/x.cfg"; then bad "credential placement $(basename "$i")"; else ok; fi
  ps -o args= -p $$ | grep -q fixture-key && bad "credential in argv" || :
done
for i in "$v"/*.assess-*-refused.in.json; do
  if "$bin" plane assess < "$i" > /dev/null 2> "$cap/err"; then bad "assess $(basename "$i") should refuse"
  elif grep -qF -- "$(cat "${i%.in.json}.error")" "$cap/err"; then ok
  else bad "assess $(basename "$i") refused differently: $(cat "$cap/err")"; fi
done
unset TYPESAFE_API_KEY
i=$(ls "$v"/*.assess-jev-executed.in.json | head -1)
if "$bin" plane assess < "$i" > /dev/null 2> "$cap/err" || ! grep -q TYPESAFE_API_KEY "$cap/err"; then bad "missing credential is refused"; else ok; fi
echo "bonfyre-decide plane parity: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
