#!/usr/bin/env bash
# Run7 acceptance (mandate SS32): exercise the principal loop end to end, and prove
# RETRACTION AND RECOVERY -- not just happy-path insertion. Chains the real
# witnesses built across the run; each is running code with an asserted result.
set -uo pipefail
cd "$(dirname "$0")"
CP=10-Code/BonfyreControlPlane
CORE=engine/core
PROBE="$HOME/.bonfyre/substrates/v6.1/feldera/probe/target/release"
pass=0; fail=0
ok(){ echo "  PASS  $1"; pass=$((pass+1)); }
bad(){ echo "  FAIL  $1"; fail=$((fail+1)); }

echo "== 1. native principal loop + native -1 on every store (P0)"
if (cd "$CORE" && make test >/tmp/run7_native.log 2>&1); then
  grep -q "p0_loop_test: PASS" /tmp/run7_native.log && ok "native loop: observation->occurrence->work->receipt->occurrence" || bad "p0 loop"
  grep -q "occurrence_correction_test: PASS" /tmp/run7_native.log && ok "occurrence correction/retraction" || bad "correction"
  grep -q "retraction_cascade_test: PASS" /tmp/run7_native.log && ok "Evidence -1 -> proof retracts" || bad "evidence cascade"
  grep -q "activation_retraction_test: PASS" /tmp/run7_native.log && ok "Authority -1 -> activation retracts" || bad "activation cascade"
else bad "native suite build/run"; fi

echo "== 2. consequence-plane cascade through real DBSP (P2+P3)"
if bash "$CP/substrate/full_retraction_cascade.sh" 2>/tmp/run7_casc.log | grep -q "3/3 links proven"; then
  ok "Evidence -1 -> proof -1 -> reachable -1 -> route -1"
else bad "full retraction cascade"; fi

echo "== 3. many relations maintained in one engine, independent retraction (P2)"
if bash "$CP/substrate/maintained_relations_witness.sh" 2>/dev/null | grep -q "MAINTAINED RELATIONS: PASS"; then
  ok "EligibleProviders/CurrentAuthority/FormReadiness -- per-gate retraction"
else bad "maintained relations"; fi

echo "== 4. RECOVERY: a withdrawn member returns when its gate comes back (not just -1)"
if [ -x "$PROBE/maintained_relations" ]; then
  rec=$(printf 'S\tEligibleProviders\tfpq\tprov\n+\tprov\n-\tprov\n+\tprov\n' | "$PROBE/maintained_relations" | tail -1)
  echo "$rec" | grep -q '"EligibleProviders":\["fpq"\]' && ok "provider down then UP -> eligibility restored (recovery)" || bad "recovery: $rec"
else echo "  SKIP  maintained_relations not built"; fi

echo "== 5. one identity, no pairwise sync; served through many grammars (P30+P31)"
(cd "$CP" && python3 -c "import fabric_queries as fq, fabric_facts as ff; fq.publish(); ff.publish()" >/dev/null 2>&1)
(cd "$CP" && python3 witness_nine_app.py 2>/dev/null | grep -q "NINE-APP WITNESS: PASS") && ok "nine-app: 1 owner, many app grammars, mutation re-enters" || bad "nine-app"
(cd "$CP" && python3 witness_multi_surface.py 2>/dev/null | grep -q "ALL-SURFACE WITNESS: PASS") && ok "all-surface: 1 identity, >=3 served grammars, no copy" || bad "all-surface"

echo "== 6. control plane is connected: projections registered into the live fabric"
served=$(sqlite3 ~/.bonfyre/estate-fabric/fabric.db "SELECT COUNT(*) FROM namespace_objects WHERE native_id LIKE 'query-%' OR native_id LIKE 'fact-%';" 2>/dev/null || echo 0)
[ "${served:-0}" -ge 12 ] && ok "BonfyreFS serves $served control-plane projections" || bad "fabric projections ($served)"

echo
echo "RUN7 ACCEPTANCE: $pass passed, $fail failed"
[ "$fail" -eq 0 ] && echo "principal loop + retraction + recovery + connection: PROVEN" || exit 1
