#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
egglog=${EGGLOG:-"$HOME/.bonfyre/substrates/v6.1/egglog/target/release/egglog"}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

cd "$root/schemas/control"
cue export . -e inverseCompiler --out json > "$tmp/algebra.json"
cd "$root"
zig run cmd/BonfyreSemanticOptimizer/tools/compile_inverse_program.zig -- "$tmp/algebra.json" "$tmp/algebra.egg" "$tmp/algebra.h"
"$egglog" "$tmp/algebra.egg"

# No scenario subjects or fixture program registry can re-enter production.
! grep -Eq 'event:93|field_trial_17|bf_inverse_programs|inverseCompilerPrograms' "$tmp/algebra.h" "$tmp/algebra.egg"

# Lossless IR boundary: negative/non-monotone fields must survive CUE -> C.
for token in retracts creates_commitments grants_rights concedes_rights conflicts_with_rights obligations cash_delta_micros ownership_dilution_bps compensation_cost_micros success_probability_ppm semantic_loss; do
  grep -q "$token" "$tmp/algebra.h"
done

# Semantic fields are strict input, not ignored JSON decoration.
sed 's/"effect_kind"/"unknown_semantic_field":1,"effect_kind"/' "$tmp/algebra.json" > "$tmp/bad.json"
if zig run cmd/BonfyreSemanticOptimizer/tools/compile_inverse_program.zig -- "$tmp/bad.json" "$tmp/bad.egg" "$tmp/bad.h" >/dev/null 2>&1; then
  echo 'inverse compiler accepted an unknown semantic field' >&2; exit 1
fi

# The generated extraction table has eclasses and policies: duration is not an
# implicit runtime selector.
grep -q 'eclass' "$tmp/algebra.h"
grep -q 'BF_COST_FAILURE_DOMAIN' "$tmp/algebra.h"
grep -q 'bf_inverse_policy_resilient' "$tmp/algebra.h"

# CommerceChain is a single owner grammar, not two hand-picked "quote" and
# "payment" cases.  Its full fifteen-stage law must compile to numeric owner
# queries, so new target contracts can address any canonical stage without C.
commerce_grammar=$(awk '/static const BfInverseOwnerGrammar/,/bf_inverse_owner_grammar_count/' "$tmp/algebra.h")
[ "$(printf '%s\n' "$commerce_grammar" | grep -F -c '{"CommerceChain"')" -eq 15 ]
[ "$(printf '%s\n' "$commerce_grammar" | grep -F -c '{"CommitmentGraph"')" -eq 5 ]
grep -Fq '"product","product"' "$tmp/algebra.h"
grep -Fq '"ownership","ownership"' "$tmp/algebra.h"
grep -F '"ownership","ownership",10,' "$tmp/algebra.h" | grep -Fq ',14}'
echo 'inverse compiler: PASS'
