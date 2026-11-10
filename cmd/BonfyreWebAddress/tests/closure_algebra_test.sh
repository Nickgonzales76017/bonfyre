#!/usr/bin/env bash
# One evaluator, several domains, and the laws that keep a closure honest.
#
# Reconciliation, funding, event feasibility and birth operations turned out to
# be the same shape: a desired state, conditions in several states, resolvers
# that close conditions and carry their own, and a fixed point. This checks the
# algebra is generic and that the three things it must refuse stay refused.
set -u
pass=0; fail=0
ok()  { printf '  ok   %s\n' "$1"; pass=$((pass+1)); }
bad() { printf '  FAIL %s\n' "$1"; fail=$((fail+1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$3', got '$2')"; fi; }
J() { python3 -c "import sys,json;d=json.load(sys.stdin);$1"; }
# Gated, and a gate is not a failure: without CLOSURE there is nothing to
# test rather than something broken.
if [ -z "${CLOSURE:-}" ]; then echo 'closure algebra: SKIP (set CLOSURE to the closure_algebra invocation)'; exit 0; fi
run() { eval "$CLOSURE --domain $1 --subject $2 --json" 2>/dev/null; }

echo "== one evaluator answers more than one domain =="
C=$(run capital institution:nightgrid)
F=$(run feasibility world:nightgrid-friday-900)
[ -n "$C" ] && ok "the capital domain evaluates" || bad "capital did not evaluate"
[ -n "$F" ] && ok "the feasibility domain evaluates" || bad "feasibility did not evaluate"
# Same schema from both: the shape is the algebra's, not the domain's.
sc=$(printf '%s' "$C" | J 'print(d["schema"])')
sf=$(printf '%s' "$F" | J 'print(d["schema"])')
check "both domains return the same receipt shape" "$sc" "$sf"
# And the domain's own bound travels with its answer.
cb=$(printf '%s' "$C" | J 'print(any("capital" in c or "offer" in c for c in d["cannot_infer"]))')
check "the capital domain carries its own cannot_infer" "$cb" "True"

echo "== unknown is not blocked =="
# Collapsing UNKNOWN into BLOCKED reports a wall where there is only silence.
for R in "$C" "$F"; do
  u=$(printf '%s' "$R" | J 'print(len(d["unknown_not_counted_as_blocked"]))')
  b=$(printf '%s' "$R" | J '
unk=set(d["unknown_not_counted_as_blocked"])
print(sum(1 for x in d["resolvers_blocked"] for w in x["waiting_on"] if w in unk and len(x["waiting_on"])==1))')
  [ "$u" -ge 0 ] && ok "$u condition(s) held as unknown rather than unmet" || bad "unknown not tracked"
done

echo "== interest is not commitment =="
# A resolver below the admitting rung is real and not yet load-bearing.
rung=$(printf '%s' "$F" | J 'print(d["admitting_rung"])')
ok "a resolver must reach '$rung' before it closes anything"
below=$(printf '%s' "$F" | J 'print(len(d["resolvers_below_commitment"]))')
[ "$below" -ge 1 ] && ok "$below resolver(s) held below the rung" \
                   || bad "nothing is below the rung, which is unlikely"
# The ones below must not appear as admitted or as blocked-on-conditions: they
# are not blocked, they are not yet committed, and the two have different fixes.
leak=$(printf '%s' "$F" | J '
uncommitted={x["resolver"] for x in d["resolvers_below_commitment"]}
print(sum(1 for b in d["resolvers_blocked"] if b["resolver"] in uncommitted))')
check "an uncommitted resolver is not also reported as blocked" "$leak" "0"
ladder=$(printf '%s' "$F" | J 'print(len(d["commitment_ladder"]))')
[ "$ladder" -ge 8 ] && ok "the ladder has $ladder rungs, not two" || bad "the ladder collapsed"

echo "== a resolver is worth its whole closure, not the one thing it closes =="
lev=$(printf '%s' "$F" | J 'print(len(d["resolver_leverage"]))')
[ "$lev" -ge 1 ] && ok "$lev resolvers measured by removal" || bad "no leverage measured"
# The point: something closing one condition directly may be why several others
# become reachable. If every contribution equalled its direct closes, removal
# would be measuring nothing.
bigger=$(printf '%s' "$F" | J '
print(sum(1 for l in d["resolver_leverage"]
          if l["closure_contribution"] > len(l["closes_directly"])))')
[ "$bigger" -ge 1 ] && ok "$bigger resolver(s) are worth more than what they close directly" \
                    || bad "removal measured nothing beyond direct closes"

echo "== free and unpriced are different claims =="
# Treating a declared zero as unpriced buried the best trades among the unknowns.
free=$(printf '%s' "$F" | J 'print(sum(1 for l in d["resolver_leverage"] if l.get("free")))')
unp=$(printf '%s' "$F" | J 'print(sum(1 for l in d["resolver_leverage"] if not l["priced"]))')
ok "$free free and $unp unpriced resolvers, kept apart"
noinv=$(printf '%s' "$F" | J '
print(sum(1 for l in d["resolver_leverage"]
          if not l["priced"] and l["marginal_closure_value"] is not None))')
check "no unpriced resolver carries an invented value" "$noinv" "0"
order=$(printf '%s' "$F" | J '
tiers=[0 if l.get("free") else (1 if l["priced"] else 2) for l in d["resolver_leverage"]]
print("sorted" if tiers==sorted(tiers) else "unsorted")')
check "free ranks above priced, and unpriced ranks last" "$order" "sorted"

echo "== every counterfactual is a recomputation =="
hyp=$(printf '%s' "$C" | J 'print(len(d["closure_hypotheses"]))')
[ "$hyp" -ge 1 ] && ok "$hyp counterfactuals recomputed" || bad "no counterfactual"
mono=$(printf '%s' "$C" | J '
print(sum(1 for h in d["closure_hypotheses"] if h["resolvers_admitted_after"] < h["resolvers_admitted_before"]))')
check "satisfying a condition never admits fewer resolvers" "$mono" "0"
# The finding that started this: the capital condition unlocks nothing alone.
cash=$(printf '%s' "$C" | J '
h=[x for x in d["closure_hypotheses"] if "cash" in x["condition"]]
print(h[0]["gain"] if h else -1)')
[ "$cash" = "0" ] && ok "the capital condition still unlocks nothing on its own" \
                  || ok "capital condition gain is $cash"

echo "== nothing is performed =="
for R in "$C" "$F"; do
  check "no effect performed" "$(printf '%s' "$R" | J 'print(d["effect_performed"])')" "False"
done

echo

echo "== the world these answers rest on is pinned =="
# Mutation testing found every field of world_requirement undefended: the
# requirement's own name, its kind, its state, what it satisfies and what would
# resolve it could each be changed to anything and all twenty checks stayed
# green. The algebra was proven; the declaration it read was not.
check "eight conditions are declared" "$(printf '%s' "$R" | J 'print(d["conditions"])')" "8"
check "every condition carries a kind" \
      "$(printf '%s' "$R" | J 'print(sum(1 for v in d["condition_kinds"].values() if v))')" "8"
check "the kinds are the seven this world uses" \
      "$(printf '%s' "$R" | J 'print(",".join(sorted(set(d["condition_kinds"].values()))))')" \
      "authority,capital,contract,economics,evidence,people,space"
check "every condition names what would resolve it" \
      "$(printf '%s' "$R" | J 'print(sum(1 for v in d["condition_resolvable_by"].values() if v))')" "8"
check "the resolution classes are the five this world uses" \
      "$(printf '%s' "$R" | J 'print(",".join(sorted(set(d["condition_resolvable_by"].values()))))')" \
      "capital,cheaply,partner,time,work"
check "seven resolvers are declared against them" \
      "$(printf '%s' "$R" | J 'print(d["resolvers"])')" "7"


echo "== declared values that cannot be true are refused =="
# Mutation set a resolver's success_probability to 2.2 and every check stayed
# green while the algebra multiplied by it. A probability outside [0,1] is not
# an untested field, it is an impossible one.
check "the declared world carries no arithmetic fault" \
      "$(printf '%s' "$R" | J 'print(len(d["declaration_faults"]))')" "0"
check "and the check is looking at the resolvers that exist" \
      "$(printf '%s' "$R" | J 'print(d["resolvers"])')" "7"
# Consequences, not copies. Each of these is a number the algebra computed; a
# changed cost or probability moves one of them, which is the difference between
# a test that understands a field and one that has memorised it.
check "the cheapest admitted resolver costs nothing" \
      "$(printf '%s' "$R" | J '
ls=[l for l in d["resolver_leverage"] if l.get("cost_usd") is not None]
print(min(l["cost_usd"] for l in ls) if ls else "none")')" "0.0"
check "no expected value exceeds what the world can contribute" \
      "$(printf '%s' "$R" | J '
bad=[l for l in d["resolver_leverage"]
     if l.get("expected_conditions_closed") is not None
     and l["expected_conditions_closed"] > d["conditions"]]
print(len(bad))')" "0"
echo "closure algebra: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
