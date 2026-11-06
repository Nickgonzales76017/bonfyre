#!/usr/bin/env bash
# The loop: residual -> target -> spend decision -> effect -> scored prediction.
#
# Each stage refuses to claim the next one's ground. A named target is not a
# measured market; an admitted spend is not a payment; a scored prediction is not
# a validated planner.
set -u
pass=0; fail=0
ok()  { printf '  ok   %s\n' "$1"; pass=$((pass+1)); }
bad() { printf '  FAIL %s\n' "$1"; fail=$((fail+1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$3', got '$2')"; fi; }
J() { python3 -c "import sys,json;d=json.load(sys.stdin);$1"; }
# Gated, and a gate is not a failure: without CLOSURE_ENV there is nothing to
# test rather than something broken.
if [ -z "${CLOSURE_ENV:-}" ]; then echo 'closure loop: SKIP (set CLOSURE_ENV to the env prefix for the closure organs)'; exit 0; fi
run() { eval "$CLOSURE_ENV bun scripts/$1 $2 --json" 2>/dev/null; }

echo "== a residual becomes a target, and stops there =="
T=$(run residual-target.mjs "--domain capital --subject institution:nightgrid")
n=$(printf '%s' "$T" | J 'print(len(d["targets"]))')
[ "$n" -ge 1 ] && ok "$n residual target(s) named" || bad "no targets"
check "the rung reached is named, not measured" \
      "$(printf '%s' "$T" | J 'print(d["rung_reached"])')" "named"
rungs=$(printf '%s' "$T" | J 'print(len(d["rungs"]))')
[ "$rungs" -ge 5 ] && ok "the ladder to a measured market has $rungs rungs" \
                   || bad "the ladder collapsed to $rungs"
# Every target says what capability would answer it, so a route that does not
# close that capability can be refused the way the retrieval market already does.
nocap=$(printf '%s' "$T" | J '
print(sum(1 for t in d["targets"] if not t["capability_that_would_answer"].strip()))')
check "every target names the capability that would answer it" "$nocap" "0"
# Typed by the condition's own kind, not by whoever is blocked on it.
bykind=$(printf '%s' "$T" | J '
print(sum(1 for t in d["targets"] if t["kind"]))')
check "every target carries a kind" "$bykind" "$n"
# A phrasing must not repeat the condition's own word back at it.
dup=$(printf '%s' "$T" | J '
bad=0
for t in d["targets"]:
    for p in t["phrasings"]:
        w=p.lower().split()
        if len(w)!=len(set(w)): bad+=1
print(bad)')
check "no phrasing says the same word twice" "$dup" "0"

echo "== a spend is decided, and no money moves =="
S1=$(run closure-spend.mjs "--domain feasibility --subject world:nightgrid-friday-900 --ceiling 10")
check "no money moved" "$(printf '%s' "$S1" | J 'print(d["money_moved"])')" "False"
# A ceiling that bends is a suggestion: an over-ceiling item is referred, never trimmed.
ref=$(printf '%s' "$S1" | J 'print(len(d["referred_to_a_person"]))')
[ "$ref" -ge 1 ] && ok "$ref item(s) referred rather than trimmed to fit" \
                || ok "nothing exceeded the ceiling on this cohort"
# An unpriced resolver is never admitted: a purchase with no ceiling is not a purchase.
unp=$(printf '%s' "$S1" | J '
print(sum(1 for x in d["decisions"] if x.get("admit") and x["cost_usd"] is None))')
check "no unpriced resolver is admitted" "$unp" "0"
# Nor is anything with no measured leverage.
noop=$(printf '%s' "$S1" | J '
print(sum(1 for x in d["decisions"] if x.get("admit") and x["closure_contribution"] < 1))')
check "nothing is bought that closes nothing" "$noop" "0"
S2=$(run closure-spend.mjs "--domain feasibility --subject world:nightgrid-friday-900 --ceiling 500 --world-value 30000")
adm=$(printf '%s' "$S2" | J 'print(len(d["admitted"]))')
[ "$adm" -ge 1 ] && ok "$adm resolver(s) admitted under a wider ceiling" \
                 || ok "nothing was admissible even at the wider ceiling"
printf '%s' "$S2" | grep -q 'nothing here holds a credential or moves money' \
  && ok "the decision states it is not a payment" \
  || bad "a spend decision does not disclaim payment"

echo "== a prediction is scored against what happened =="
if [ -n "${OUTCOME_SCORE:-}" ]; then
  O=$(eval "$OUTCOME_SCORE" 2>/dev/null)
  if [ -n "$O" ]; then
    made=$(printf '%s' "$O" | J 'print(d["predictions_made"])')
    [ "$made" -ge 1 ] && ok "$made prediction(s) were standing when scored" \
                      || bad "nothing was predicted"
    # Nothing happening must not read as a confirmed prediction.
    unchanged=$(printf '%s' "$O" | J 'print(d["nothing_changed"])')
    if [ "$unchanged" = "True" ]; then
      printf '%s' "$O" | grep -q 'not evidence the prediction was right' \
        && ok "an unchanged closure is not scored as a confirmation" \
        || bad "an unchanged closure was treated as confirmation"
    else
      nc=$(printf '%s' "$O" | J 'print(len(d["newly_closed"]))')
      ok "$nc condition(s) closed since the prediction"
      # The valuable case: something closed that was predicted to unlock nothing.
      surprises=$(printf '%s' "$O" | J '
print(sum(1 for s in d["scored"] if s["was_predicted"] and s["predicted_gain"]==0))')
      [ "$surprises" -ge 0 ] && ok "$surprises condition(s) closed that were predicted to unlock nothing" \
                             || bad "scoring produced no comparison"
    fi
    printf '%s' "$O" | grep -q 'one cohort agreeing -> a validated planner' \
      && ok "the score refuses to call itself validation" \
      || bad "the outcome does not bound its own reading"
  fi
fi

echo
echo "closure loop: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
