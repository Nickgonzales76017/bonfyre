#!/usr/bin/env bash
# The representation market: a compressed form is chosen on measured cost, not
# on reputation, and it earns an envelope only by an executed trial.
#
# What this proves: that a declared representation without a trial shows no
# envelope, that preservation is reported as false prunes and false keeps
# separately, that the reference reproduces itself, and that a representation on
# a different axis cannot borrow a measurement from another axis.
#
# What it cannot prove: that any envelope transfers. It is one cohort, one k, and
# one embedding backend.
set -u
BASE="${BASE:-https://aurekai.life}"
pass=0; fail=0
ok()  { printf '  ok   %s\n' "$1"; pass=$((pass+1)); }
bad() { printf '  FAIL %s\n' "$1"; fail=$((fail+1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$3', got '$2')"; fi; }
J() { python3 -c "import sys,json;d=json.load(sys.stdin);$1"; }

echo "== a declared representation is not a measured one =="
R=$(curl -s --max-time 25 -H 'Accept: application/json' "$BASE/_surface/representations" 2>/dev/null)
if printf '%s' "$R" | grep -q '"records"'; then
  n=$(printf '%s' "$R" | J 'print(d["count"])')
  [ "$n" -ge 4 ] && ok "$n representations declared" || bad "only $n declared"
  # The rung that matters: declared and untrialled must carry no envelope.
  liars=$(printf '%s' "$R" | J '
print(sum(1 for r in d["records"]
          if r["availability"]=="declared_untrialled" and r.get("envelope","").strip()))')
  check "no untrialled representation shows an envelope" "$liars" "0"
  trialled=$(printf '%s' "$R" | J '
print(sum(1 for r in d["records"] if r["availability"]=="available" and r.get("envelope","").strip()))')
  [ "$trialled" -ge 3 ] && ok "$trialled trialled representations carry one" \
                        || bad "only $trialled measured envelopes"
  miss=$(printf '%s' "$R" | J 'print(sum(1 for r in d["records"] if not r["cannot_infer"].strip()))')
  check "every representation states what it cannot infer" "$miss" "0"
  # Axes are not interchangeable: a tensor cosine is not a retrieval envelope.
  axes=$(printf '%s' "$R" | J 'print(",".join(sorted({r["axis"] for r in d["records"]})))')
  printf '%s' "$axes" | grep -q 'numerical_body' \
    && ok "a second axis is declared and kept distinct ($axes)" \
    || bad "every representation sits on one axis; the market cannot compare trades"
fi

echo "== preservation is measured, and prunes are not netted against keeps =="
if [ -n "${REP_TRIAL:-}" ]; then
  T=$($REP_TRIAL 2>/dev/null)
  bodies=$(printf '%s' "$T" | J 'print(d["bodies"])')
  [ "$bodies" -ge 20 ] && ok "the cohort is $bodies real bodies, not a fixture" \
                       || bad "cohort of $bodies is too small to measure recall"
  # The reference must reproduce its own ranking or nothing after it means anything.
  ex=$(printf '%s' "$T" | J '
t=[x for x in d["trials"] if x["representation"]=="exact-f32"][0]
print(t["recall_at_k"], t["false_prunes"])')
  check "the exact reference reproduces itself" "$ex" "1.0 0"
  both=$(printf '%s' "$T" | J '
print(sum(1 for t in d["trials"] if "false_prunes" in t and "false_keeps" in t))')
  tot=$(printf '%s' "$T" | J 'print(len(d["trials"]))')
  check "every trial reports prunes and keeps separately" "$both" "$tot"
  # int8 is the trade the market should currently prefer; if it ever measures
  # badly again, the harness is the first suspect -- comparing raw codes without
  # carrying the per-body scale once reported it at 0.357.
  i8=$(printf '%s' "$T" | J '
t=[x for x in d["trials"] if x["representation"]=="int8"]
print(t[0]["recall_at_k"] if t else 0)')
  awk -v r="$i8" 'BEGIN{exit !(r>0.9)}' \
    && ok "int8 preserves $i8 of the exact top-k at 4x" \
    || bad "int8 measured $i8; suspect the scale is not being carried"
  # A control that measures badly is the point: a tournament containing only
  # candidates believed good confirms rather than measures.
  ctl=$(printf '%s' "$T" | J '
t=[x for x in d["trials"] if x["representation"]=="halved-f32"]
print(t[0]["false_prunes"] if t else 0)')
  [ "$ctl" -gt 0 ] && ok "the deliberate control loses $ctl candidates, as a bad trade should" \
                   || bad "the control lost nothing; it is not controlling anything"
  # Cheaper must not silently mean better.
  order=$(printf '%s' "$T" | J '
best=max(d["trials"], key=lambda t: t["recall_at_k"] if t["representation"]!="exact-f32" else -1)
print(best["representation"])')
  check "the best non-exact envelope is the one the bytes justify" "$order" "int8"
  printf '%s' "$T" | grep -q 'hashed-token' \
    && ok "the receipt names the backend its geometry came from" \
    || bad "an envelope is reported without saying whose geometry it measured"
fi

echo "== admission chooses on the envelope, and refuses when nothing earns it =="
# An envelope nothing acts on is decoration. This is what the measuring was for.
if [ -n "${ADMIT:-}" ]; then
  A() { eval "$ADMIT $1 --json" 2>/dev/null; }
  got=$(A "--class discovery --k 5 --min-recall 0.95" | J 'print(d["admitted"])')
  check "a loose budget admits the cheapest representation that earns it" "$got" "int8"
  # Work carrying an institutional effect has no loss budget to spend.
  for c in authority money identity publication effect; do
    e=$(A "--class $c --k 5 --min-recall 0.10" | J 'print(d["admitted"])')
    check "$c work runs exact whatever the budget says" "$e" "exact-f32"
  done
  # A budget nothing meets falls back to exact, never to the closest miss.
  hard=$(A "--class discovery --k 5 --min-recall 0.999" | J 'print(d["admitted"])')
  check "an unmeetable budget falls back to exact, not the nearest miss" "$hard" "exact-f32"
  # An envelope measured at one k does not satisfy a budget stated at another.
  kk=$(A "--class discovery --k 10 --min-recall 0.5" | J 'print(d["admitted"])')
  check "an envelope at another k does not satisfy this budget" "$kk" "exact-f32"
  # And an envelope from another axis is not a bound on this one.
  ax=$(A "--class discovery --axis numerical_body --k 5 --min-recall 0.5" | J 'print(d["admitted"])')
  check "an envelope from another axis is never admitted here" "$ax" "exact-f32"
  # An untrialled representation must never be named, however cheap it looks.
  never=$(A "--class discovery --k 5 --min-recall 0.10" | J '
print(sum(1 for c in d["considered"]
          if c["representation"] in ("fpq-v8","cms-quantized-ann")
          and "no measured envelope" not in c["verdict"]))')
  check "no untrialled representation is considered admissible" "$never" "0"
fi

echo "== placement asks whether the input may be seen here before asking what is cheap =="
# Choosing a representation decides what a computation costs. Choosing a site
# decides whether it may happen there at all, and that is a disclosure question
# rather than an economic one, so it is answered first and is not tradeable
# against speed.
if [ -n "${ADMIT:-}" ]; then
  P() { eval "$ADMIT --class discovery --k 5 --min-recall 0.95 $1 --json" 2>/dev/null \
        | J 'print(d["placement"]["site"])'; }
  check "a reducer on public input is placed where the reducer is" \
        "$(P '--backend hvm2 --data-scope public')" "workstation"
  # Capability is not permission. The machine with every reducer holds no
  # institutional data, so placing private input there discloses it.
  for scope in institution-private self-private restricted; do
    check "a reducer is refused $scope input it would have to receive" \
          "$(P "--backend hvm2 --data-scope $scope")" "None"
  done
  # Where the data already is, nothing is disclosed by computing on it.
  for scope in public institution-private restricted; do
    check "$scope input runs where it already lives" \
          "$(P "--backend reference --data-scope $scope")" "estate"
  done
  # The verdict per site must say which of the two questions rejected it.
  why=$(eval "$ADMIT --class discovery --k 5 --min-recall 0.95 --backend hvm2 --data-scope restricted --json" 2>/dev/null | J '
v=[c["verdict"] for c in d["placement"]["considered"]]
print("cap" if any("does not run" in x for x in v) else "", "perm" if any("not permission" in x for x in v) else "")')
  check "each refusal names capability or permission" "$why" "cap perm"
fi

echo "== the run is verified against what the envelope promised =="
# An envelope is a fact about the cohort it was measured over. Reporting only the
# envelope lets a number from one cohort stand in for a result on another, which
# is the substitution this whole market exists to stop.
if [ -n "${FRONTIER_MEASURED:-}" ] && [ -n "${FRONTIER_OTHER:-}" ]; then
  M=$(eval "$FRONTIER_MEASURED --json" 2>/dev/null)
  O=$(eval "$FRONTIER_OTHER --json" 2>/dev/null)
  rep=$(printf '%s' "$M" | J 'print(d["admitted_representation"])')
  check "the frontier is computed on the admitted representation" "$rep" "int8"
  mr=$(printf '%s' "$M" | J 'print(d["realised_recall"])')
  pr=$(printf '%s' "$M" | J 'print(d["predicted_recall"])')
  ok "on the measured cohort the run realised $mr against a promise of $pr"
  # The point of verifying: a different cohort can miss the promise.
  orr=$(printf '%s' "$O" | J 'print(d["realised_recall"])')
  opr=$(printf '%s' "$O" | J 'print(d["predicted_recall"])')
  awk -v a="$orr" -v b="$opr" 'BEGIN{exit !(a<b)}'     && ok "on an unmeasured cohort it realised $orr against $opr, and said so"     || bad "the two cohorts agreed exactly; the verification proves nothing here"
  note=$(printf '%s' "$O" | J 'print(d["prediction_note"])')
  printf '%s' "$note" | grep -q 'different cohort'     && ok "and the receipt names why the numbers differ"     || bad "a divergence is reported without saying it is a different cohort"
  # A budget that bends is a suggestion: a missed budget must not exit clean.
  if [ -n "${FRONTIER_STRICT:-}" ]; then
    eval "$FRONTIER_STRICT" >/dev/null 2>&1; rc=$?
    check "a budget missed on the running cohort exits non-zero" "$rc" "1"
  fi
fi

echo
echo "representations: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
