#!/usr/bin/env bash
# Do the estate's capabilities actually feed each other, or merely coexist?
#
# The estate had grown many organs that each measured something correctly and
# ended at a number nobody was obliged to read. 87% of declared architectures
# participated in no bridge at all. Maturity that nothing consumes is inventory.
#
# What this proves: that a declared bridge names architectures that exist, that
# one with a witness has that witness, that the admission bridge actually refuses
# at runtime rather than only on paper, and that residuals from several organs
# reach one queue.
#
# What it cannot prove: that the bridges are the right ones.
set -u
pass=0; fail=0
ok()  { printf '  ok   %s\n' "$1"; pass=$((pass+1)); }
bad() { printf '  FAIL %s\n' "$1"; fail=$((fail+1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$3', got '$2')"; fi; }
J() { python3 -c "import sys,json;d=json.load(sys.stdin);$1"; }

echo "== a declared bridge names real architectures and keeps its witness =="
python3 - <<'PY' > /tmp/conn.$$ 2>/dev/null
import pathlib, json, os
arch=set(); inter=[]; cur=None
for f in pathlib.Path('architecture').rglob('*.yaff'):
    for raw in f.read_text(errors='replace').splitlines():
        if raw.startswith('architecture '): arch.add(raw.split(' ',1)[1].strip())
        elif raw.startswith('interaction '):
            inter.append({"name":raw.split(' ',1)[1].strip(),"source":None,"destination":None,"witness":None})
        elif inter and raw.strip().startswith(('source ','destination ','witness ')):
            k,v = raw.strip().split(' ',1)
            inter[-1][k]=v.strip()
bad_ref=[i for i in inter if (i["source"] and i["source"] not in arch) or (i["destination"] and i["destination"] not in arch)]
# The witness field is overloaded: some are file paths and most are prose
# describing the evidence ("a bounded proof artifact"). Only a path can be
# checked for existence, so only a path is checked -- and the split is reported,
# because a prose witness is a claim no test can verify.
def looks_like_path(w):
    return "/" in w and " " not in w and not w.endswith(".")
paths=[i for i in inter if i["witness"] and looks_like_path(i["witness"])]
prose=[i for i in inter if i["witness"] and not looks_like_path(i["witness"])]
missing_w=[i for i in paths if not os.path.exists(i["witness"])]
named=set()
for i in inter:
    for k in ("source","destination"):
        if i[k] in arch: named.add(i[k])
print(json.dumps({"architectures":len(arch),"interactions":len(inter),
                  "dangling":[i["name"] for i in bad_ref],
                  "missing_witness":[i["name"] for i in missing_w],
                  "witness_paths":len(paths),"witness_prose":len(prose),
                  "bridged":len(named),
                  "bridged_pct":len(named)*100//max(len(arch),1)}))
PY
C=$(cat /tmp/conn.$$); rm -f /tmp/conn.$$
check "no interaction names an architecture that does not exist" \
      "$(printf '%s' "$C" | J 'print(len(d["dangling"]))')" "0"
check "every interaction whose witness is a path has that file" \
      "$(printf '%s' "$C" | J 'print(len(d["missing_witness"]))')" "0"
wp=$(printf '%s' "$C" | J 'print(d["witness_paths"])')
wr=$(printf '%s' "$C" | J 'print(d["witness_prose"])')
ok "$wp witness(es) are checkable paths; $wr are prose no test can verify"
pct=$(printf '%s' "$C" | J 'print(d["bridged_pct"])')
n=$(printf '%s' "$C" | J 'print(d["interactions"])')
ok "$n interactions bridge ${pct}% of declared architectures"
# A ratchet, not a target: this was 12% and must not silently fall back.
[ "$pct" -ge 30 ] && ok "connectivity has not regressed below its recorded floor" \
                  || bad "bridged fraction fell to ${pct}%, below the recorded floor of 30%"

echo "== a claimed measurement is backed by evidence that opens today =="
# The atlas's first non-negotiable rule is no maturity laundering, and it checked
# only that a witness list was non-empty. Any sentence satisfied it.
bun architecture/atlas.mjs validate >/dev/null 2>&1 \
  && ok "the atlas validates with witness enforcement on" \
  || bad "the atlas does not validate"
# The guard must actually refuse. A rule that cannot fail is a comment.
probe=$(mktemp architecture/probe_XXXX.yaff)
cat > "$probe" <<'PROBE'
atlas probe

architecture laundering-probe
  canonical_name LaunderingProbe
  family learning
  domain Learning
  maturity measured
  semantic_question does the guard refuse a fictional witness
  invariant this entry exists to be rejected
  forbidden_inference a probe -> a capability
  computational_form validation
  native_format yaff
  witness cmd/BonfyreWebAddress/tests/deliberately_absent.sh
PROBE
if bun architecture/atlas.mjs validate >/dev/null 2>&1; then
  bad "a measured claim with a fictional witness was accepted"
else
  ok "a measured claim with a fictional witness is refused"
fi
rm -f "$probe"
bun architecture/atlas.mjs validate >/dev/null 2>&1 \
  && ok "and the atlas is clean again once the probe is removed" \
  || bad "removing the probe did not restore validity"
# No measured claim may rest only on archives.
arch_only=$(python3 - <<'PY2'
import pathlib, os
arch={}; cur=None
for f in sorted(pathlib.Path('architecture').rglob('*.yaff')):
    for raw in f.read_text(errors='replace').splitlines():
        s=raw.strip()
        if raw.startswith('architecture '): cur=s.split(' ',1)[1]; arch[cur]={'m':None,'w':[]}
        elif cur and s.startswith('maturity '): arch[cur]['m']=s.split(' ',1)[1]
        elif cur and s.startswith('witness '): arch[cur]['w'].append(s.split(' ',1)[1])
def live(w):
    if w.endswith('.archive'): return False
    if '/' in w and ' ' not in w:
        return os.path.exists(os.path.expanduser(w)) or os.path.exists(w)
    return True
print(sum(1 for v in arch.values() if v['m'] in ('measured','proven') and not any(live(w) for w in v['w'])))
PY2
)
check "no measured claim rests only on archived or missing evidence" "$arch_only" "0"

echo "== the admission bridge refuses at runtime, not only on paper =="
if [ -n "${PURE_PRIVATE:-}" ] && [ -n "${PURE_PUBLIC:-}" ]; then
  P=$(eval "$PURE_PRIVATE --json" 2>/dev/null)
  # This asserted that no reducer may touch private data, which was true only
  # while no reducer lived where the data does. Building one on the estate then
  # failed the suite for closing the gap. The rule is placement: a backend runs
  # only at a site cleared for the input, and the machines cleared only for
  # public input must stay refused.
  ran=$(printf '%s' "$P" | J '
print(",".join(sorted(b["backend"] for b in d["backends"]
                      if b.get("result") is not None or b.get("aggregate") is not None)))')
  offsite=$(printf '%s' "$P" | J '
sites={b["backend"]: b.get("placement") for b in d["backends"]}
ran=[b["backend"] for b in d["backends"]
     if (b.get("result") is not None or b.get("aggregate") is not None)]
print(sum(1 for r in ran if sites.get(r) not in ("estate", None)))')
  check "nothing ran off the site holding the private data" "$offsite" "0"
  ok "backends that ran on private work: ${ran:-none}"
  # And the ones that exist only where the data may not go stay refused.
  refused=$(printf '%s' "$P" | J '
print(sum(1 for b in d["backends"]
          if b["backend"] in ("hvm2","bend") and b.get("refused_by_admission")))')
  [ "$refused" -ge 1 ] && ok "$refused backend(s) living off-site remain refused" \
                       || bad "an off-site backend was not refused private input"
  why=$(printf '%s' "$P" | J '
print(sum(1 for b in d["backends"] if b.get("refused_by_admission")))')
  [ "$why" -ge 1 ] && ok "$why backend(s) name admission as the reason" \
                   || bad "backends were skipped without naming admission"
  ref=$(printf '%s' "$P" | J '
print(sum(1 for b in d["backends"] if b["backend"]=="reference" and b.get("result") is not None))')
  check "the work still ran where the data lives" "$ref" "1"
  # And the bridge must not refuse everything: public work is unchanged.
  Q=$(eval "$PURE_PUBLIC --json" 2>/dev/null)
  qr=$(printf '%s' "$Q" | J '
print(sum(1 for b in d["backends"] if b["backend"] in ("hvm2","hvm4") and b.get("result") is not None))')
  [ "$qr" -ge 2 ] && ok "public work still reaches $qr reducers" \
                  || bad "the bridge refuses public work too; it is a wall not a gate"
  printf '%s' "$P" | grep -q '"admission"' \
    && ok "the receipt carries the admission that produced the placement" \
    || bad "a placement decision is applied without being recorded"
fi

echo "== residuals from several organs reach one queue =="
if [ -n "${RESIDUAL_WORK:-}" ]; then
  W=$(eval "$RESIDUAL_WORK --json" 2>/dev/null)
  src=$(printf '%s' "$W" | J 'print(d["sources_read"])')
  [ "$src" -ge 3 ] && ok "$src measuring organs feed the queue" \
                   || bad "only $src organ(s) reached the queue"
  kinds=$(printf '%s' "$W" | J '
print(len({i["kind"] for i in d["buildable"]}))')
  [ "$kinds" -ge 3 ] && ok "$kinds distinct residual kinds are represented" \
                     || bad "the queue collapses to $kinds kind(s)"
  # The human line again: an item nobody can close must not sit in the build list.
  unc=$(printf '%s' "$W" | J 'print(sum(1 for i in d["buildable"] if not i["closable"]))')
  check "nothing unclosable appears in the buildable list" "$unc" "0"
  waiting=$(printf '%s' "$W" | J 'print(len(d["waiting_on_a_decision"]))')
  [ "$waiting" -ge 1 ] && ok "$waiting item(s) correctly wait on a decision" \
                       || bad "everything was classified as buildable, which is unlikely"
  own=$(printf '%s' "$W" | J 'print(sum(1 for i in d["buildable"] if not i["owner"].strip()))')
  check "every work item names the plane that owns the fix" "$own" "0"
  check "the queue writes nothing by default" \
        "$(printf '%s' "$W" | J 'print(d["written"])')" "False"
fi

echo "== the estate can measure its own structure, and says what it cannot conclude =="
if [ -f scripts/architecture_frontier.py ]; then
  A=$(bun scripts/architecture-frontier.mjs --json 2>/dev/null)
  arch=$(printf '%s' "$A" | J 'print(d["architectures"])')
  [ "$arch" -ge 200 ] && ok "the frontier reads $arch architectures" || bad "read only $arch"
  # The correction that matters: a raw isolation count is dominated by a
  # generated catalogue, and reading it as disconnection overstates the problem.
  orph=$(printf '%s' "$A" | J 'print(d["orphan_count"])')
  iso=$(printf '%s' "$A" | J 'print(d["isolated"])')
  [ "$orph" -le "$iso" ] && ok "$orph of $iso isolated share nothing with anything connected" \
                         || bad "more orphans than isolated, which is impossible"
  # Weighting: a signal a hundred things share must not top the ranking.
  floor=$(printf '%s' "$A" | J 'print(d["signal_floor"])')
  ok "affinity signals below a rarity floor of $floor are discarded as filing"
  weak=$(printf '%s' "$A" | J '
print(sum(1 for h in d["hubs_by_reach"] for c in h["candidates"]
          for w in c["affinity"] if w["weight"] < d["signal_floor"]))')
  check "no candidate rests on a signal below the floor" "$weak" "0"
  # It must rank without declaring.
  printf '%s' "$A" | grep -q 'a high-scoring candidate -> a bridge worth building' \
    && ok "the frontier refuses to call a candidate a decision" \
    || bad "the frontier does not bound its own reading"
fi

echo
echo "connectivity: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
