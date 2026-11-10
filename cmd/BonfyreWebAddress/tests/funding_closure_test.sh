#!/usr/bin/env bash
# A funding path is a set of separable conditions, not a verdict.
#
# What this proves: that the answer is never DECLINED, that each unmet condition
# is classified by what would actually resolve it, that capacity is scoped to the
# institution that holds it, and that an unasked market is unmeasured rather than
# zero.
#
# What it cannot prove: that any path would close. Satisfying a requirement is
# not receiving capital.
set -u
pass=0; fail=0
ok()  { printf '  ok   %s\n' "$1"; pass=$((pass+1)); }
bad() { printf '  FAIL %s\n' "$1"; fail=$((fail+1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$3', got '$2')"; fi; }
J() { python3 -c "import sys,json;d=json.load(sys.stdin);$1"; }

# Gated, and a gate is not a failure. This exercises an institution the
# estate does not declare in a plain checkout, so without FUNDING there is
# nothing to test rather than something broken. Saying SKIP keeps the
# suite honest about which red is real.
if [ -z "${FUNDING:-}" ]; then echo 'funding closure: SKIP (set FUNDING to the funding_closure invocation)'; exit 0; fi
R=$(eval "$FUNDING --json" 2>/dev/null)
[ -n "$R" ] || { echo "  (no funding requirements declared; skipping)"; exit 0; }

echo "== the answer is a classified remainder, not a verdict =="
printf '%s' "$R" | grep -qi '"verdict": *"declined' \
  && bad "the closure returned a verdict rather than a remainder" \
  || ok "no path is reported as simply declined"
cls=$(printf '%s' "$R" | J 'print(",".join(sorted(d["residual"])))')
case "$cls" in
  *CapitalResidual*) ok "unmet conditions are classified ($cls)" ;;
  *) bad "the residual is not classified by what would resolve it" ;;
esac
# Only one class is answered with money. Collapsing them would make every
# shortfall look like a financing problem.
non=$(printf '%s' "$R" | J '
print(sum(v["count"] for k,v in d["residual"].items() if k!="CapitalResidual"))')
[ "$non" -ge 1 ] && ok "$non unmet condition(s) are not capital problems" \
                 || bad "every residual was classified as capital"
why=$(printf '%s' "$R" | J '
print(sum(1 for v in d["residual"].values() for r in v["requirements"] if not r["because"].strip()))')
check "every unmet condition says why it is unmet" "$why" "0"

echo "== capacity is scoped to the institution that holds it =="
inst=$(printf '%s' "$R" | J 'print(d["institution"])')
[ -n "$inst" ] && ok "the closure is scoped to $inst" || bad "no institution scope"
printf '%s' "$R" | grep -q 'capital observed for one institution -> capital available to another' \
  && ok "and it states that another institution's capital is not its own" \
  || bad "the receipt does not bound cross-institution capacity"
# Demand counted at every status, with the states kept apart: an expression of
# interest is not a commitment.
dem=$(printf '%s' "$R" | J 'print(d["declared_demand"])')
com=$(printf '%s' "$R" | J 'print(d["committed_demand"])')
awk -v a="$dem" 'BEGIN{exit !(a>0)}' && ok "declared demand is counted (\$$dem)" \
                                     || bad "declared demand reported as zero"
states=$(printf '%s' "$R" | J 'print(len(d["demand_by_state"]))')
[ "$states" -ge 1 ] && ok "demand is broken out by state ($states)" \
                    || bad "demand states are collapsed into one figure"
awk -v c="$com" -v t="$dem" 'BEGIN{exit !(c<=t)}' \
  && ok "committed (\$$com) never exceeds declared (\$$dem)" \
  || bad "more is committed than was ever declared"

echo "== an unasked market is unmeasured, not zero =="
mm=$(printf '%s' "$R" | J 'print(d["ceilings"]["MarketCeiling"]["measured"])')
check "the capital market is not claimed as measured" "$mm" "False"
rung=$(printf '%s' "$R" | J 'print(d["ceilings"]["MarketCeiling"].get("rung",""))')
[ -n "$rung" ] && ok "and it names the rung it stopped at ($rung)" \
               || bad "an unmeasured market gives no rung"
amt=$(printf '%s' "$R" | J 'print(d["ceilings"]["MarketCeiling"]["amount"])')
check "an unmeasured market contributes no amount" "$amt" "None"
# The internalized ceiling is measured, and must not silently absorb the market.
im=$(printf '%s' "$R" | J 'print(d["ceilings"]["InternalizedCeiling"]["measured"])')
check "what the institution itself satisfies is measured" "$im" "True"

echo

echo "== the requirement set this closure read is pinned =="
# Mutation testing changed a requirement's name, kind, state and resolution class
# to nonsense and all thirteen checks stayed green. The arithmetic over the set
# was witnessed; the set was not.
check "eight requirements are declared" \
      "$(printf '%s' "$R" | J 'print(len(d["declared"]["requirement_names"]))')" "8"
check "three are satisfied, four missing, one unknown" \
      "$(printf '%s' "$R" | J 's=d["declared"]["by_state"];print(",".join(str(s.get(k,0)) for k in ("SATISFIED","MISSING","UNKNOWN")))')" \
      "3,4,1"
check "only one unmet condition is answered with money" \
      "$(printf '%s' "$R" | J 'print(d["declared"]["by_resolvable_by"].get("capital",0))')" "1"
check "the requirement kinds are the five this institution declares" \
      "$(printf '%s' "$R" | J 'print(",".join(sorted(d["declared"]["by_kind"])))')" \
      "capital,evidence,institutional,market,partner"
check "every requirement is named, none blank" \
      "$(printf '%s' "$R" | J 'print(sum(1 for n in d["declared"]["requirement_names"] if n.strip()))')" "8"


echo "== the demand this closure computed is asserted, not the rows it read =="
# Mutation changed a budget request's amount from 4,200 to 12,601 and its
# lifecycle_state and institution to nonsense; the declared demand moved and
# every check stayed green. These pin what the arithmetic produced.
check "declared demand across every request" \
      "$(printf '%s' "$R" | J 'print(int(d["declared_demand"]))')" "6074"
check "and none of it is committed" \
      "$(printf '%s' "$R" | J 'print(int(d["committed_demand"]))')" "0"
check "the internalized ceiling is what the institution can already satisfy" \
      "$(printf '%s' "$R" | J 'print(int(d["ceilings"]["InternalizedCeiling"]["amount"]))')" "6063"
check "demand is scoped to one institution" \
      "$(printf '%s' "$R" | J 'print(d["institution"])')" "institution:nightgrid"
echo "funding closure: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
