#!/bin/sh
# Is the learned ordering fit to decide what enters an exponential?
#
# The subset lattice costs one branch per subset, so a frontier of 92 cannot
# enter it whole and something has to choose the k that do. BitNet is the
# estate's ranker and the obvious candidate for that job: it already consumes
# AddressPlane survivors and reorders them without changing eligibility.
#
# On the persisted 92-survivor weather run it is worse than chance. The
# frontier is 84 percent reachable services; BitNet's top-16 is 69 percent,
# its bottom-16 is 88 percent, and 91 percent of random draws of 16 beat its
# top-16 on service density. The ranking is anti-correlated with usability for
# this query, which is the same finding the atlas already records in words --
# three protocol specifications above the first weather source -- now with the
# consequence attached: BitNet must not drive the cut.
#
# This does not say the ranker is broken. It says a SHADOW representation has
# not earned the right to decide what a deterministic frontier spends its
# budget on, and that inverting it because its bottom looks better would be
# fitting one query.
set -eu

rank_db=${1:-$HOME/.bonfyre/estate/rank.db}
estate_db=${2:-$HOME/.bonfyre/estate/estate.db}
[ -f "$rank_db" ] || { echo "SKIP: no persisted rank run at $rank_db"; exit 0; }

python3 - "$rank_db" "$estate_db" <<'PY'
import sqlite3, sys, random

rank_db, estate_db = sys.argv[1], sys.argv[2]
rank = sqlite3.connect(rank_db)
ids = [c for (c,) in rank.execute(
    "select candidate_identity from ranked_candidate_envelope order by rank")]
assert ids, "the rank run holds no candidates"

est = sqlite3.connect(estate_db)
kind = {oid: sk for oid, sk in est.execute("select id, source_kind from occurrence")}
SERVICE = ("public_api", "x402", "mcp")
usable = lambda c: kind.get(c) in SERVICE

n = len(ids)
k = 16
base = sum(1 for c in ids if usable(c)) / n
top = sum(1 for c in ids[:k] if usable(c)) / k

# The gate is not "is the ranker good"; it is "does it beat drawing at random",
# because drawing at random is what the cut costs nothing to do.
random.seed(3)
draws = [sum(1 for c in random.sample(ids, k) if usable(c)) / k for _ in range(2000)]
beaten = sum(1 for d in draws if d > top) / len(draws)

print(f"  frontier {n}: {base:.0%} reachable services")
print(f"  BitNet top-{k}: {top:.0%}   random-{k} mean: {sum(draws)/len(draws):.0%}")
print(f"  random draws beating it: {beaten:.0%}")

# The assertion is the standing conclusion, not the hope. If a future
# representation earns the cut, this goes red and is meant to: the finding
# would be that the ranker changed, and it must be re-measured rather than
# assumed to have improved.
assert top <= base, (
    f"BitNet top-{k} now exceeds the base rate ({top:.0%} vs {base:.0%}); "
    "the standing conclusion that it must not drive the cut needs re-measuring")
assert beaten > 0.5, (
    f"only {beaten:.0%} of random draws beat it; it may now be fit to drive "
    "the cut, which is a promotion and needs a passport, not a passing test")
PY

echo "PASS bitnet drive: the SHADOW ranker does not beat chance on this frontier and does not drive the cut"
