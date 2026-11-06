#!/bin/sh
# The composition engine asks; the pure tier answers; two reducers agree.
#
# Each piece of this existed separately and none of them met.
#
#   DisCIPL proposes chains and scores them with declared priors, and until
#   now nothing else could compute the same thing, so the scores had no
#   second opinion.
#
#   The pure tier can reduce a pure search across several backends and
#   report where they differ, and it had only ever been pointed at closure
#   predicates.
#
#   `propose` ranks chains one at a time. An operator does not run one chain;
#   they run as many as a budget allows, which is a subset-sum over the
#   chains' accumulated cost and a different question with a different shape.
#
# So the engine states the portfolio question in the tier's own job shape and
# the tier decides where it may be computed -- purity checked before dispatch,
# placement asked per backend, and a backend that cannot express the lattice
# refusing rather than answering something easier. What this pins is that the
# interaction net and plain enumeration return the same feasible set, arriving
# by routes with nothing in common.
set -eu

repo=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd -P)
discipl="$repo/cmd/BonfyreDiscipl/bonfyre-discipl"
hvm="$repo/vendor/hvm4/src/hvm4"
[ -x "$discipl" ] || { echo "SKIP: bonfyre-discipl not built"; exit 0; }
[ -x "$hvm" ] || { echo "SKIP: HVM4 not built"; exit 0; }

root=$(mktemp -d); trap 'rm -rf "$root"' EXIT
fail() { echo "FAIL: $1"; exit 1; }

# A lattice of its own, so the test does not depend on the operator's store.
"$discipl" init --root "$root" >/dev/null
"$discipl" contracts import --root "$root" >/dev/null
"$discipl" seed-commands --root "$root" >/dev/null

"$discipl" portfolio-job --from T_INGEST --to T_PROOF --budget-milli 400 \
    --depth 8 --root "$root" > "$root/job.json" 2>/dev/null \
    || fail "the engine could not state the portfolio question"

bun "$repo/architecture/tests/pure_tier_witness.mjs" portfolio-job "$root/job.json"

# The tier needs its declarations or it refuses, which is correct and is not
# what is being tested here.
cms="$root/tier.db"
"$repo/cmd/BonfyreCMS/bonfyre-cms" schema migrate --db "$cms" \
    --schemas "$repo/cmd/BonfyreCMS/content-types" >/dev/null 2>&1
for b in reference hvm2 hvm4 bend; do
  "$repo/cmd/BonfyreCMS/bonfyre-cms" entry create execution_profile --db "$cms" \
    --schemas "$repo/cmd/BonfyreCMS/content-types" \
    "{\"backend\":\"$b\",\"kind\":\"reducer\",\"purity\":\"pure\",\"availability\":\"declared_available\",\"status\":\"published\"}" >/dev/null 2>&1
done
"$repo/cmd/BonfyreCMS/bonfyre-cms" entry create compute_site --db "$cms" \
  --schemas "$repo/cmd/BonfyreCMS/content-types" \
  '{"site":"test-local","cores":4,"backends":"reference,hvm2,hvm4,bend","holds_live_data":true,"max_data_scope":"restricted","status":"published"}' >/dev/null 2>&1

BONFYRE_CMS_DB="$cms" BONFYRE_HVM4="$hvm" \
  bun "$repo/scripts/pure-job.mjs" "$root/job.json" --json > "$root/out.json" 2>/dev/null \
  || fail "the tier could not run the portfolio job"

bun "$repo/architecture/tests/pure_tier_witness.mjs" portfolio-output "$root/out.json" "$root/job.json"

# The wall is real and is refused at the boundary rather than truncated.
bun "$repo/architecture/tests/pure_tier_witness.mjs" portfolio-wide "$root/wide.json"
if BONFYRE_CMS_DB="$cms" BONFYRE_HVM4="$hvm" \
     bun "$repo/scripts/pure-job.mjs" "$root/wide.json" >/dev/null 2>&1; then
    fail "a 2^24 lattice was accepted; the enumeration wall must refuse, not truncate"
fi

echo "PASS portfolio: the engine states it, the tier places it, the net and enumeration agree, and the wide case is refused"
