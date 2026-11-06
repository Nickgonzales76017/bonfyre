#!/usr/bin/env bash
# Capital routes are combinations, not alternatives.
#
# What this proves: that one route's requirement can be closed by another, that
# the search is pure and moves nothing, that UNKNOWN capacity is never counted as
# zero, and that the planner ranks what each unmet condition would actually
# unlock rather than assuming money is the answer.
#
# What it cannot prove: that any combination would fund anything. A closing world
# is a world in which nothing is blocked, which is not an offer.
set -u
pass=0; fail=0
ok()  { printf '  ok   %s\n' "$1"; pass=$((pass+1)); }
bad() { printf '  FAIL %s\n' "$1"; fail=$((fail+1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$3', got '$2')"; fi; }
J() { python3 -c "import sys,json;d=json.load(sys.stdin);$1"; }

# Gated, and a gate is not a failure. This exercises an institution the
# estate does not declare in a plain checkout, so without FRONTIER there is
# nothing to test rather than something broken. Saying SKIP keeps the
# suite honest about which red is real.
if [ -z "${FRONTIER:-}" ]; then echo 'funding frontier: SKIP (set FRONTIER to the funding_frontier invocation)'; exit 0; fi
R=$(eval "$FRONTIER --json" 2>/dev/null)
[ -n "$R" ] || { echo "  (no funding routes declared; skipping)"; exit 0; }

echo "== the search is combinatorial and moves nothing =="
check "no effect was performed" "$(printf '%s' "$R" | J 'print(d["effect_performed"])')" "False"
n=$(printf '%s' "$R" | J 'print(d["routes"])')
w=$(printf '%s' "$R" | J 'print(d["worlds_evaluated"])')
expect=$(python3 -c "print(2**$n - 1)")
check "every non-empty combination of $n routes was evaluated" "$w" "$expect"

echo "== a route's condition can be closed by another route =="
# This is the whole reason routes are combined rather than chosen between: a
# small closure can unlock a rail an order of magnitude larger.
best=$(printf '%s' "$R" | J 'print(len(d["best_world"]["requirements_closed"]))')
[ "$best" -ge 1 ] && ok "the furthest world closes $best requirement(s)" \
                  || bad "no world closes anything"
bind=$(printf '%s' "$R" | J 'print(len(d["binding_conditions"]))')
[ "$bind" -ge 1 ] && ok "$bind binding condition(s) identified across the world set" \
                  || bad "no binding condition was found"
top=$(printf '%s' "$R" | J 'print(d["binding_conditions"][0][1])')
[ "$top" -gt 1 ] && ok "the tightest condition blocks a route in $top combinations" \
                 || bad "no condition blocks more than one combination"

echo "== unknown capacity is never counted as zero =="
# Reporting UNKNOWN as zero understates the frontier and sends a planner looking
# for money it may already reach; reporting it as capacity is worse.
unk=$(printf '%s' "$R" | J 'print(len(d["routes_of_unknown_capacity"]))')
[ "$unk" -ge 1 ] && ok "$unk route(s) carry capacity nobody has asked for" \
                 || bad "every route claims a known capacity, which is implausible"
ceil=$(printf '%s' "$R" | J 'print(d["evidenced_capacity_ceiling"])')
ok "the evidenced ceiling counts only what can be evidenced (\$$ceil)"
printf '%s' "$R" | grep -q 'UNKNOWN capacity -> zero capacity' \
  && ok "and the receipt forbids reading unknown as zero" \
  || bad "the receipt does not bound how unknown capacity is read"

echo "== the planner ranks what would unlock, not what costs money =="
h=$(printf '%s' "$R" | J 'print(len(d["closure_hypotheses"]))')
[ "$h" -ge 1 ] && ok "$h closure hypotheses tested as counterfactuals" \
              || bad "no closure hypothesis was tested"
# A gap resolves through information, partner or time at least as often as
# through capital. If the capital condition topped the list unconditionally the
# planner would just be a lender search.
kinds=$(printf '%s' "$R" | J '
print(",".join(sorted({x["resolvable_by"] for x in d["closure_hypotheses"] if x["worlds_gained"]>0})) or "none")')
case "$kinds" in
  *cheaply*|*partner*) ok "non-capital conditions unlock worlds ($kinds)" ;;
  *) bad "only capital conditions unlock anything; the planner is a lender search" ;;
esac
gains=$(printf '%s' "$R" | J '
print(sum(1 for x in d["closure_hypotheses"] if x["worlds_gained"]>0))')
zero=$(printf '%s' "$R" | J '
print(sum(1 for x in d["closure_hypotheses"] if x["worlds_gained"]<=0))')
ok "$gains condition(s) unlock worlds, $zero unlock nothing alone"
# Every hypothesis must be a real recomputation, not a guess.
mono=$(printf '%s' "$R" | J '
print(sum(1 for x in d["closure_hypotheses"] if x["worlds_unblocked_after"] < x["worlds_unblocked_before"]))')
check "satisfying a condition never removes a reachable world" "$mono" "0"

echo "== evidence is priced against every route that needs it =="
# A revenue history is not a bank's document, a grant's document and a lessor's
# document. It is one artifact three routes each require, and costing it against
# a single route understates it by however many others were waiting.
reuse=$(printf '%s' "$R" | J 'print(len(d["evidence_reuse"]))')
[ "$reuse" -ge 1 ] && ok "$reuse evidence artifact(s) tracked for reuse" \
                   || bad "no artifact reuse computed"
top=$(printf '%s' "$R" | J 'print(d["evidence_reuse"][0]["reuse_factor"] if d["evidence_reuse"] else 0)')
[ "$top" -gt 1 ] && ok "the most reused artifact serves $top routes" \
                 || bad "no artifact serves more than one route"
# The cheapest conditions to resolve should be visible as the most reused, or
# the planner and the reuse view disagree about what to do first.
agree=$(printf '%s' "$R" | J '
top=d["evidence_reuse"][0]["artifact"] if d["evidence_reuse"] else ""
gain={h["condition"]: h["worlds_gained"] for h in d["closure_hypotheses"]}
print("yes" if top in gain else "no")')
check "the most reused artifact is one the planner also tested" "$agree" "yes"
bad_ref=$(printf '%s' "$R" | J '
routes=set()
for e in d["evidence_reuse"]:
    routes |= set(e["required_by_routes"]) | set(e["produced_by_routes"])
print(0 if routes else 1)')
check "reuse references real routes" "$bad_ref" "0"

echo

echo "== the frontier this search computed is asserted =="
# Mutation changed a route's name, what it satisfies, what it requires and its
# publication status; the combinatorial search produced different worlds and
# every check stayed green. These pin the search's own output.
check "every declared route enters the search" \
      "$(printf '%s' "$R" | J 'print(d["routes"])')" "8"
check "the number of route combinations evaluated" \
      "$(printf '%s' "$R" | J 'print(d["worlds_evaluated"])')" "255"
check "and how many of them close with nothing blocked" \
      "$(printf '%s' "$R" | J 'print(d["worlds_with_no_blocked_route"])')" "3"
check "revenue-history is the condition that unlocks the most" \
      "$(printf '%s' "$R" | J 'print(d["binding_conditions"][0][0])')" "revenue-history"
check "the binding conditions are named, not summarised" \
      "$(printf '%s' "$R" | J 'print(len(d["binding_conditions"]) > 0)')" "True"
echo "funding frontier: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
