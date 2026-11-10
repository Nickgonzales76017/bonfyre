#!/bin/sh
# The whole chain, and the property that makes a bad ranker harmless.
#
#   AddressPlane   decides membership -- a deterministic mask cut, 4,891
#                  considered and 4,843 rejected before exact
#   BitNet         decides order -- SHADOW, and measured worse than chance at
#                  choosing a prefix on this estate's own frontier
#   HVM2/HVM4/Bend reduce the predicate over what the plane admitted
#
# The ranker being unfit is a real measurement and it is not a reason to take
# it out of the chain, because of what is asserted here: when the reducer
# computes the whole frontier exactly, the order it arrives in cannot change
# the answer. BitNet is dangerous exactly where it truncates and harmless
# where it only sorts. This runs the same frontier in rank order, reversed
# rank order and plane order, and requires one survivor set.
#
# The second assertion is the trap underneath it. Bend answers with an
# aggregate over indices, and indices are positional: its checksum *does*
# move when the order moves. Comparing a Bend checksum against a reference
# computed in a different order would read as a backend disagreeing about a
# pure computation when nothing disagreed at all.
set -eu

repo=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd -P)
estate="$HOME/.bonfyre/estate/estate.db"
rankdb="$HOME/.bonfyre/estate/rank.db"
plane="$repo/cmd/BonfyreExternalEstate/bonfyre-external-estate"
[ -f "$estate" ] || { echo "SKIP: no estate at $estate"; exit 0; }
[ -x "$plane" ] || { echo "SKIP: external-estate not built"; exit 0; }

root=$(mktemp -d); trap 'rm -rf "$root"' EXIT
fail() { echo "FAIL: $1"; exit 1; }

"$plane" address --db "$estate" --need "domain:weather" --limit 48 > "$root/plane.json" 2>/dev/null \
    || fail "the address cut did not run"

bun "$repo/architecture/tests/pure_tier_witness.mjs" plane-generate \
  "$root" "$rankdb" "$estate" "$HOME/.bonfyre/estate/observation.jsonl"

# The tier needs its own declarations; without them it refuses, which is
# correct and is not what this test is about.
cms="$root/tier.db"
"$repo/cmd/BonfyreCMS/bonfyre-cms" schema migrate --db "$cms" \
    --schemas "$repo/cmd/BonfyreCMS/content-types" >/dev/null 2>&1
add() { "$repo/cmd/BonfyreCMS/bonfyre-cms" entry create "$1" --db "$cms" \
        --schemas "$repo/cmd/BonfyreCMS/content-types" "$2" >/dev/null 2>&1; }
for b in reference hvm2 hvm4 bend; do
  add execution_profile "{\"backend\":\"$b\",\"kind\":\"reducer\",\"purity\":\"pure\",\"availability\":\"declared_available\",\"status\":\"published\"}"
done
add compute_site '{"site":"test-local","cores":4,"backends":"reference,hvm2,hvm4,bend","holds_live_data":true,"max_data_scope":"restricted","status":"published"}'

for o in rank reversed plane; do
  BONFYRE_CMS_DB="$cms" BONFYRE_HVM4="$repo/vendor/hvm4/src/hvm4" \
    bun "$repo/scripts/pure-job.mjs" "$root/chain_$o.json" --json \
    > "$root/out_$o.json" 2>/dev/null || fail "the chain did not run in $o order"
done

bun "$repo/architecture/tests/pure_tier_witness.mjs" plane-check "$root"

echo "chain order witness finished (the live precondition is reported above)"
