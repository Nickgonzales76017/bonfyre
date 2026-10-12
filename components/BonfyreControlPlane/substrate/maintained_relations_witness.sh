#!/usr/bin/env bash
# SS2 breadth witness: one DBSP engine maintains MANY named consequence relations
# with independent, incremental retraction -- not a binary per relation.
# EligibleProviders / CurrentAuthority / FormReadiness in one circuit; a gate -1
# withdraws only its relation's member, the others survive.
set -euo pipefail
BIN="$HOME/.bonfyre/substrates/v6.1/feldera/probe/target/release/maintained_relations"
if [ ! -x "$BIN" ]; then echo "maintained_relations not built -- skipping"; exit 0; fi

out=$(printf 'S\tEligibleProviders\tfpq-generate\tprovider-mac
S\tCurrentAuthority\talex@payroll\tgrant-alex
S\tFormReadiness\tacm-form\tslots-filled
+\tprovider-mac
+\tgrant-alex
+\tslots-filled
-\tprovider-mac
-\tgrant-alex
' | "$BIN")

# after all gates up: 3 members across 3 relations
peak=$(echo "$out" | grep '"delta":"gate_up"' | tail -1 | grep -o '"count":[0-9]*' | grep -o '[0-9]*')
# after provider-mac down: EligibleProviders empty, others intact (count 2)
after_prov=$(echo "$out" | grep '"delta":"gate_down"' | head -1)
# after grant-alex down: CurrentAuthority gone too (count 1, only FormReadiness)
final=$(echo "$out" | grep '"delta":"gate_down"' | tail -1 | grep -o '"count":[0-9]*' | grep -o '[0-9]*')

ok=1
[ "$peak" = "3" ] || { echo "FAIL: peak=$peak (want 3)"; ok=0; }
echo "$after_prov" | grep -q '"EligibleProviders":\[\]' || echo "$after_prov" | grep -qv 'EligibleProviders' || { echo "FAIL: EligibleProviders not withdrawn"; ok=0; }
echo "$after_prov" | grep -q 'CurrentAuthority' || { echo "FAIL: CurrentAuthority should survive provider-mac -1"; ok=0; }
[ "$final" = "1" ] || { echo "FAIL: final=$final (want 1)"; ok=0; }

if [ "$ok" = "1" ]; then
  echo "MAINTAINED RELATIONS: PASS (3 relations, one engine; per-gate retraction independent)"
else exit 1; fi
