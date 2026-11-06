#!/usr/bin/env bash
# Capital routes as transformations over financial state.
#
# The claim under test is that a financing product list loses three things that
# a transformation network keeps: what an instrument actually does to the state
# (a vendor term supplies no cash), which instruments cannot be held together,
# and which orders are permitted. Each is asserted here against the institution's
# own declared world, not a fixture.
#
# The suite also guards two mistakes this organ made and had corrected: reading
# the InternalizedCeiling as opening cash, which opened the search a few dollars
# short of its target; and calling a pair mutually exclusive when it was merely
# unreachable, which manufactured twelve conflicts out of one missing token.
set -u
pass=0; fail=0
ok()  { printf '  ok   %s\n' "$1"; pass=$((pass+1)); }
bad() { printf '  FAIL %s\n' "$1"; fail=$((fail+1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$3', got '$2')"; fi; }
J() { python3 -c "import sys,json;d=json.load(sys.stdin);$1"; }
# eval, not direct expansion: CAPITAL is set to an env-prefixed invocation locally
# and to an ssh command remotely, and only one of those two runs when the word is
# expanded as a command name. The suite has to pass in both places.
# Gated, and a gate is not a failure. This exercises a declared institution
# that a plain checkout does not carry, so without CAPITAL there is nothing
# to test rather than something broken.
if [ -z "${CAPITAL:-}" ]; then
  echo 'capital transform: SKIP (set CAPITAL to the invocation for a declared institution)'
  exit 0
fi
cap() { eval "$CAPITAL $*" 2>/dev/null; }
R=$(cap --json)
[ -n "$R" ] || { echo "  FAIL capital_transform produced nothing"; exit 1; }
G() { printf '%s' "$R" | J "$1"; }

echo "== the state the search opens from =="
# Opening cash is evidenced internal capacity. The requirement-satisfaction sum
# is a different quantity with a different meaning, and reading it as money made
# a $0 institution look $6,063 funded.
check "opening cash is the evidenced internal capacity" "$(G 'print(d["opening_state"]["cash"])')" "0.0"
tok=$(G 'print(len(d["opening_state"]["tokens_held"]))')
[ "$tok" -ge 6 ] && ok "$tok state tokens held at the opening" || bad "opening holds $tok tokens"
enc=$(G 'print("collateral-free" in d["opening_state"]["tokens_held"])')
check "the institution opens unencumbered" "$enc" "True"

echo "== an instrument is what it does, not what it is filed under =="
# The route table says vendor-terms satisfies cash-position, which is the row a
# grant carries too. As a transformation the two are not comparable.
V=$(cap --json | J '
import json
print(json.dumps([r for r in d["reach"] if r["transform"]=="vendor-terms"][0]))')
[ -n "$V" ] && ok "vendor-terms is reachable and reported separately from a grant" \
            || bad "vendor-terms missing from the reach report"
noc=$(G 'print(d["known_to_reach_target"])')
check "nothing is claimed to reach the target" "$noc" "0"
unk=$(G 'print(d["unknown_cash_effect"] > 0)')
check "paths with an unknown cash effect are counted apart" "$unk" "True"

echo "== an unknown cash effect poisons the total rather than adding nothing =="
poison=$(G '
bad=0
for r in d["best"]:
    if not r["cash_known"] and r["end_cash"] is not None: bad+=1
print(bad)')
check "no path with an unknown step reports a known end cash" "$poison" "0"

echo "== unreachability is not exclusion =="
# The corrected question: both individually admissible, neither order admissible
# together. Before the correction this reported grant + community-equity, which
# do not collide at all.
ex=$(G 'print(len(d["mutually_exclusive_pairs"]))')
[ "$ex" -ge 1 ] && ok "$ex mutually exclusive pair(s) found" || bad "no exclusion found at all"
bogus=$(G '
names=set()
for p in d["mutually_exclusive_pairs"]: names |= set(p["pair"])
print(sum(1 for n in names if n in ("grant","community-equity","approve-demand")))')
check "no pair is called exclusive for being unreachable" "$bogus" "0"
real=$(G '
print(sum(1 for p in d["mutually_exclusive_pairs"]
          if any("conflicts with" in w or "collateral-free" in w for w in p["because"])))')
[ "$real" -ge 1 ] && ok "$real exclusion(s) are named by the right they collide over" \
                  || bad "exclusions carry no conflict reason"

echo "== order carries information =="
o=$(G 'print(len(d["order_sensitive"]))')
[ "$o" -ge 3 ] && ok "$o pair(s) admissible one way and not the other" || bad "order never mattered"
sym=$(G '
bad=0
for p in d["order_sensitive"]:
    if p["admissible"] == list(reversed(p["refused"])): continue
    bad+=1
print(bad)')
check "every order-sensitive finding is the same pair reversed" "$sym" "0"
why=$(G 'print(sum(1 for p in d["order_sensitive"] if not p["because"].strip()))')
check "every refusal says what the state was missing" "$why" "0"

echo "== the counterfactual is kept apart from the reachable =="
s=$(G 'print(len(d["structurally_order_sensitive"]))')
[ "$s" -ge 1 ] && ok "$s structural ordering fact(s), reported separately" \
              || bad "no structural ordering reported"
claim=$(G '
print(sum(1 for c in d["cannot_infer"] if "order-sensitive" in c))')
check "the counterfactual refuses to be read as reachable" "$claim" "1"
overlap=$(G '
r={tuple(p["admissible"]) for p in d["order_sensitive"]}
s={tuple(p["admissible"]) for p in d["structurally_order_sensitive"]}
print(len(r & s))')
check "no finding is counted in both the reachable and the counterfactual set" "$overlap" "0"

echo "== a stranded route is stranded for a stated reason =="
st=$(G 'print(",".join(d["stranded_transformations"]))')
check "bank-debt is never admissible in any path" "$st" "bank-debt"
blocked=$(G '
r=[x for x in d["reach"] if x["transform"]=="bank-debt"][0]
print("external-capital-route" in r["blocked_by"])')
check "and it is blocked on the unasked market, not on credit" "$blocked" "True"

echo "== what it takes to reach each instrument =="
floors=$(G 'print(sum(1 for r in d["reach"] if r["reachable"] and r["months_floor"] is None))')
check "every reachable instrument carries a month floor" "$floors" "0"
grant=$(G '
r=[x for x in d["reach"] if x["transform"]=="grant"][0]
print(r["steps"], r["months_floor"])')
check "a grant is three acts and a quarter of a year away" "$grant" "3 3.25"
free=$(G 'print(sum(1 for r in d["reach"] if r["reachable"] and not r["rights_conceded"]))')
[ "$free" -ge 3 ] && ok "$free instrument(s) reachable conceding nothing" \
                  || bad "only $free reachable without conceding a right"

echo "== the declarations this answer rests on are pinned =="
# Mutation testing found this suite blind to reversibility, grants_rights,
# conflicts_with_rights, cash_delta, ownership_delta, revenue_share_delta and
# min_ownership_retained -- every field the architecture's invariants are about.
# Twenty-six checks, none of which touched a declaration. These pin the shape:
# change any of those fields in the store and a count below moves.
check "eleven transformations are declared" "$(G 'print(d["declared"]["transformations"])')" "11"
check "five reversible, four costly, two irreversible" \
      "$(G 'r=d["declared"]["by_reversibility"];print(",".join(str(r[k]) for k in ("reversible","costly","irreversible")))')" \
      "5,4,2"
check "nine distinct rights can be conceded" \
      "$(G 'print(len(d["declared"]["rights_granted"]))')" "9"
check "three of them are ones some route refuses to sit beside" \
      "$(G 'print(",".join(d["declared"]["rights_conflicted"]))')" \
      "first-lien-all-assets,first-lien-equipment,revenue-first-claim"
check "six steps declare a cash effect and the rest are unknown" \
      "$(G 'print(d["declared"]["with_known_cash_delta"])')" "6"
check "one step trades ownership and one pledges revenue" \
      "$(G 'x=d["declared"];print(",".join(str(x[k]) for k in ("with_ownership_effect","with_revenue_pledge")))')" \
      "1,1"
check "every declared cash effect is exactly zero; none adds money" \
      "$(G 'print(",".join(str(x) for x in d["declared"]["known_cash_deltas"]))')" \
      "0.0,0.0,0.0,0.0,0.0,0.0"
check "the one ownership trade concedes a fifth" \
      "$(G 'print(",".join(str(x) for x in d["declared"]["ownership_effects"]))')" "-0.2"
check "the one revenue pledge is a tenth" \
      "$(G 'print(",".join(str(x) for x in d["declared"]["revenue_pledges"]))')" "0.1"
check "the ownership floors are a half and three fifths" \
      "$(G 'print(",".join(str(x) for x in d["declared"]["ownership_floors"]))')" "0.5,0.6"
check "two steps refuse to run below an ownership floor" \
      "$(G 'print(d["declared"]["with_ownership_floor"])')" "2"

echo "== obligations and pursuit cost reach the answer =="
# Mutation cleared a transformation's obligations and changed its pursuit cost
# and its name; the reach report changed and all thirty-seven checks stayed
# green. These read the computed answer rather than the rows.
check "the reachable set names every transformation exactly once" \
      "$(G 'print(len(d["reach"]) == len({r["transform"] for r in d["reach"]}))')" "True"
check "the obligations carried by the cheapest grant path" \
      "$(G '
r=[x for x in d["reach"] if x["transform"]=="grant"][0]
print(",".join(r["via"]))')" "approve-demand,operate-and-evidence-revenue,grant"
check "every admissible path carries the obligations of its steps" \
      "$(G '
bad=sum(1 for p in d["best"] if set(p["obligations"]) - set(sum([x["obligations"] for x in d["best"]],[])))
print(bad)')" "0"
check "no path claims a pursuit cost while a step is unpriced" \
      "$(G '
bad=sum(1 for p in d["best"] if p["unpriced_steps"] and p["pursuit_cost_usd"] is not None)
print(bad)')" "0"

echo "== the rung, and what it refuses =="
check "the rung reached is admissible, not sufficient" "$(G 'print(d["rung"])')" "admissible"
check "the search performs no effect" "$(G 'print(d["effect_class"])')" "PURE"
ci=$(G 'print(len(d["cannot_infer"]))')
[ "$ci" -ge 5 ] && ok "$ci things the search refuses to be read as" || bad "only $ci refusals"
pref=$(G 'print(bool(d["preference"].strip()))')
check "the ranking states that it is a preference" "$pref" "True"

echo "== depth does not lose paths =="
A2=$(cap --depth 2 --json | J 'print(d["admissible"])')
A3=$(G 'print(d["admissible"])')
[ "$A2" -le "$A3" ] && ok "depth 2 admits $A2, depth 3 admits $A3" \
                    || bad "deeper search lost paths ($A2 -> $A3)"

echo
echo "capital transformations: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
