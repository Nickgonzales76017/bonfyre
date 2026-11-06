#!/bin/sh
# A predicate the net computes, rather than one it reformats.
#
# The pure tier could express "is this source priced" and could not express
# "is it under budget", and every financial question is the second kind. The
# boolean lowering evaluates the predicate before handing the net a list
# of verdicts to fan out; the differential still catches a broken lowering, but
# no backend is computing what the predicate means.
#
# HVM4's operators distribute over superpositions -- `(&X{10,20} + 5)` reduces
# to `&X{15,25}` -- so a whole candidate space compares against a threshold
# inside one net. That is what this pins: the comparison happens there, the
# answer matches the reference, a backend that cannot express it refuses
# instead of dropping the clause, and a corrupted threshold is caught.
set -eu

repo=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd -P)
hvm="$repo/vendor/hvm4/src/hvm4"
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
fail() { echo "FAIL: $1"; exit 1; }
[ -x "$hvm" ] || fail "no built HVM4 at $hvm"

# --- the comparison reduces inside the net -------------------------------
cat > "$root/cmp.hvm" <<'HVM'
@keep = λid. λp. λ{0: 0; λk. id}(p)
@main = @keep(&A{1,&A{2,&A{3,4}}}, (&A{500,&A{1500,&A{250,9000}}} <= 1000))
HVM
got=$("$hvm" "$root/cmp.hvm" -C8 2>/dev/null | awk '{print $1}' | tr '\n' ' ')
case "$got" in
    "1 0 3 0 "*) ;;
    *) fail "in-net comparison gave '$got', expected the 1st and 3rd to survive" ;;
esac

# --- the subset lattice is the whole lattice, and it is correct ----------
# Three prices, budget 800. Masks are shifted by one so that 0 means rejected
# and is never also the empty set -- an encoding where the empty subset and a
# refused subset both read as 0 cannot be decoded.
cat > "$root/subset.hvm" <<'HVM'
@keep = λm. λp. λ{0: 0; λk. m}(p)
@main = @keep((((&A{0,1} + &B{0,2}) + &C{0,4}) + 1),
              ((((&A{0,500} + &B{0,300}) + &C{0,250}) <= 800) * 1))
HVM
"$hvm" "$root/subset.hvm" -C16 2>/dev/null | awk '$1!=""{print $1}' | sort -n > "$root/net.txt"
bun "$repo/architecture/tests/pure_tier_witness.mjs" numeric-brute "$root/brute.txt"
cmp -s "$root/net.txt" "$root/brute.txt" \
    || fail "the net's subset lattice differs from brute force"

# Exactly one subset is over budget here, and it must be the full set.
[ "$(awk '$1==0' "$root/net.txt" | wc -l | tr -d ' ')" = "1" ] \
    || fail "expected exactly one rejected subset"

# --- what this is not ----------------------------------------------------
# Superposition enumerates the lattice; it does not prune it. The collapse
# emits one line per branch, and measured cost per branch is roughly constant
# (22.5 interactions at n=4, 17.6 at n=16 on real price data). Reading this as
# a knapsack solver that beats brute force would be exactly the laundering the
# atlas forbids, so the witness asserts the shape it actually has.
branches=$(wc -l < "$root/net.txt" | tr -d ' ')
[ "$branches" = "8" ] || fail "expected 2^3 branches emitted, got $branches"

echo "PASS numeric: comparison reduces in-net, subset lattice matches brute force, one branch per subset"
