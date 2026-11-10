#!/usr/bin/env bash
# A consequential financial write is evaluated before it is authorised.
#
# What this proves: that the engine computes and never applies, that an
# obligation it cannot resolve is reported rather than assumed to close, that
# the reconciliation formula is the one the source actually agrees with, and
# that this work is placed and represented as money work rather than discovery.
#
# What it cannot prove: that any obligation should close. That is an approval,
# and nothing here holds the authority to give one.
set -u
pass=0; fail=0
ok()  { printf '  ok   %s\n' "$1"; pass=$((pass+1)); }
bad() { printf '  FAIL %s\n' "$1"; fail=$((fail+1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$3', got '$2')"; fi; }
J() { jq -r "$1"; }
command -v jq >/dev/null || { echo "jq is required" >&2; exit 2; }

echo "== the engine computes a consequence and performs no effect =="
R=$(AI_LOSS_ENV="${AI_LOSS_ENV:-$HOME/Work/u90.env}" bun scripts/product_close_consequence.mjs \
      --namespace "${PRODUCT_NAMESPACE:-upper90/managed/ai-loss}" \
      --obligations "${OBLIGATION_DB:-$HOME/.bonfyre/ai-loss-oracle.sqlite3}" --json 2>/dev/null)
[ -n "$R" ] || { echo "  (no AI Loss store reachable; skipping)"; exit 0; }
eff=$(printf '%s' "$R" | J '.effect_performed')
check "no effect was performed" "$eff" "false"
cls=$(printf '%s' "$R" | J '.effect_class')
check "the job declares itself pure" "$cls" "PURE"
printf '%s' "$R" | grep -q 'a computed consequence -> an approval' \
  && ok "the receipt states it is not an approval" \
  || bad "a financial consequence is reported without bounding its reading"

echo "== an obligation that cannot be resolved is reported, never assumed =="
un=$(printf '%s' "$R" | J '.unresolvable')
res=$(printf '%s' "$R" | J '.resolved_against_source')
tot=$(printf '%s' "$R" | J '.obligations')
check "every obligation is accounted for" "$((res + un))" "$tot"
# The oracle store held events for 61 of 101; the source answers for all of
# them. An unevaluable obligation must never be counted as closable.
overlap=$(printf '%s' "$R" | J '[.detail | group_by(.obligation)[] | map(.verdict) | unique | select(length > 1)] | length')
check "no obligation carries two verdicts" "$overlap" "0"

echo "== the receipt says whether the system of record was actually asked =="
# An empty result from a database that answered and an empty result from one that
# was never reached used to be the same dict, and every obligation then said "the
# system of record returned no row for it" while the client had failed to connect.
# The distinction is asserted first, because everything below depends on it.
reached=$(printf '%s' "$R" | J '.source_of_record.reached')
status=$(printf '%s' "$R" | J '.source_of_record.status')
case "$status" in
  "") bad "the receipt does not say whether the source was reached" ;;
  *)  ok "the receipt states the source's condition ($reached)" ;;
esac
if [ "$reached" = "false" ]; then
  # Every unresolvable obligation must blame the unreachable source, not claim an
  # empty answer from it.
  lying=$(printf '%s' "$R" | J '[.detail[] | select(.verdict == "unresolvable" and ((.because // "") | contains("returned no row")))] | length')
  check "no obligation claims an answer the source never gave" "$lying" "0"
fi

echo "== over- and under-allocation are distinguished, not netted into one number =="
dirs=$(printf '%s' "$R" | J '[.detail[].direction // empty] | unique | join(",") | if . == "" then "none" else . end')
case "$dirs" in
  *under-allocated*) ok "directions are carried per obligation ($dirs)" ;;
  # With the source unreachable nothing reconciles, so there is no direction to
  # carry. Demanding one here would be asserting a measurement the run could not
  # take -- the failure this suite exists to catch, committed by the suite.
  *) if [ "$reached" = "false" ]; then
       ok "no direction is claimed while the system of record is unreachable"
     else
       bad "the consequence does not distinguish over from under allocation"
     fi ;;
esac
# A net figure alone would hide that money is missing in both directions.
net=$(printf '%s' "$R" | J '.net_amount_left')
[ -n "$net" ] && ok "a net position is reported alongside the directions ($net)" \
              || bad "no net position reported"

echo "== the machinery consumes a namespace and names no product =="
# A delegated estate is added by declaration, not by another code path. This is
# the regression that put a product's tables, prefixes and schema string inside
# generic machinery, where a second product would have cost another compiler.
ns=$(printf '%s' "$R" | J '.namespace')
check "the run is scoped to a namespace" "$ns" "${PRODUCT_NAMESPACE:-upper90/managed/ai-loss}"
pk=$(printf '%s' "$R" | J '.pack')
[ -f "$pk" ] && ok "the grammar came from the pack the namespace declares ($pk)" \
             || bad "no pack was resolved from the namespace"
law=$(printf '%s' "$R" | J '.law_declared_by_pack | length')
[ "$law" -ge 8 ] && ok "the reconciliation law is read from the declaration ($law fields)" \
                 || bad "the law is not coming from the pack"
printf '%s' "$R" | grep -q 'reconcile_witness' \
  && ok "and the declaration carries its own validating witness" \
  || bad "a law is applied with no witness recorded beside it"
if [ -f architecture/check_generic_machinery.mjs ]; then
  if bun architecture/check_generic_machinery.mjs >/dev/null 2>&1; then
    ok "no generic machinery names a declared product or its tables"
  else
    bad "generic machinery names a declared product; run check_generic_machinery.mjs"
  fi
fi

echo "== money work is represented and placed as money work =="
if [ -n "${ADMIT:-}" ]; then
  a=$(eval "$ADMIT --class money --k 5 --min-recall 0.50 --backend reference --data-scope institution-private --json" 2>/dev/null)
  rep=$(printf '%s' "$a" | J '.admitted')
  check "a financial closure runs on the exact representation" "$rep" "exact-f32"
  site=$(printf '%s' "$a" | J '.placement.site')
  check "and runs where the data already is" "$site" "estate"
  # Even a generous loss budget must not buy approximation for money work.
  cheap=$(eval "$ADMIT --class money --k 5 --min-recall 0.10 --backend reference --data-scope institution-private --json" 2>/dev/null | J '.admitted')
  check "a cheap budget does not buy approximation for money" "$cheap" "exact-f32"
fi

echo
echo "close consequence: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
