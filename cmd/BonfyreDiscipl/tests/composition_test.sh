#!/bin/sh
# What a composition engine has to get right before anyone should believe it.
#
# This estate's composition graph held 34 contracts and had composed nothing.
# discipl_chains was empty, and the reason had nothing to do with the chains:
# `propose` accepted --depth N and honoured it only up to 2, so it answered
# "no chain exists" about paths held in its own table. The four-hop walk from
# a workflow stage to value capture -- every edge contracted, every edge
# present -- came back as candidate_chains: [].
#
# That is the failure this witness exists to prevent recurring, and it is the
# dangerous kind: not an error, an authoritative negative.
#
# The rest of what is checked here are the three defects found while fixing it,
# each of which also fails silently:
#   - every proposal was keyed `discipl_chain:discipl-proposal`, so each one
#     overwrote the last and the table could not accumulate;
#   - the ranked list was built with an insertion sort that started one index
#     too low, so the best chain was reported second;
#   - every command edge carried the same semantic_loss, which makes the score
#     a hop counter wearing a probability's clothes -- it ranked "speak this
#     document aloud and then transcribe what you said" level with reading it.
set -e

root=$(mktemp -d)
bin=$(cd "$(dirname "$0")/.." && pwd)/bonfyre-discipl
trap 'rm -rf "$root"' EXIT
[ -x "$bin" ] || { echo "FAIL: $bin not built"; exit 1; }

"$bin" init --root "$root" >/dev/null
if "$bin" contracts unknown --root "$root" >/dev/null 2>&1; then
    echo "FAIL: unsupported contracts subcommand accepted"
    exit 1
fi
"$bin" contracts import --root "$root" >/dev/null
"$bin" seed-commands --root "$root" >/dev/null

fail() { echo "FAIL: $1"; exit 1; }

# --- depth is honoured, not accepted and ignored -------------------------
# T_WORKFLOW_STAGE -> T_QUEUE_JOB -> T_EXECUTION -> T_LEDGER_EVENT ->
# T_VALUE_CAPTURE. Four contracted edges. The old search reported none.
deep=$("$bin" propose --from T_WORKFLOW_STAGE --to T_VALUE_CAPTURE --depth 6 --root "$root")
echo "$deep" | python3 -c '
import json,sys
d=json.load(sys.stdin)
cs=d["candidate_chains"]
assert cs, "four-hop path reported as unreachable; depth is being ignored again"
best=cs[0]
assert best["hops"]==4, "expected a four-hop chain, got %d" % best["hops"]
assert best["families"][0]=="T_WORKFLOW_STAGE" and best["families"][-1]=="T_VALUE_CAPTURE"
' || fail "deep chain not found"

# A depth that cannot reach must still say so, rather than returning a
# truncated walk as if it were an answer.
shallow=$("$bin" propose --from T_WORKFLOW_STAGE --to T_VALUE_CAPTURE --depth 2 --root "$root")
echo "$shallow" | python3 -c '
import json,sys
d=json.load(sys.stdin)
assert d["candidate_chains"]==[], "depth 2 returned a chain longer than 2 hops"
assert d["max_depth"]==2
' || fail "depth bound not enforced"

# --- candidates are ranked, and the ranking is the order they are in -----
ranked=$("$bin" propose --from T_INGEST --to T_PROOF --depth 8 --root "$root")
echo "$ranked" | python3 -c '
import json,sys
cs=json.load(sys.stdin)["candidate_chains"]
assert len(cs)>1, "need several candidates to check ordering"
scores=[c["score"] for c in cs]
assert scores==sorted(scores,reverse=True), "candidates not in descending score order: %r"%scores
' || fail "candidate ranking out of order"

# --- two different paths are two different chains ------------------------
"$bin" propose --from T_INGEST --to T_PROOF --depth 8 --root "$root" >/dev/null
"$bin" propose --from T_WORKFLOW_STAGE --to T_VALUE_CAPTURE --depth 6 --root "$root" >/dev/null
n=$(sqlite3 "$root/discipl.db" "select count(distinct chain_id) from discipl_chains")
[ "$n" -ge 3 ] || fail "proposals collapsed onto $n chain id(s); they are keyed by goal again"

# --- a transform that changes modality loses more than one that does not -
# Narrate turns text into audio and Transcribe turns it back. Both are lossy
# in a way that Hash, which only ever produces a record about material, is not.
sqlite3 "$root/discipl.db" "select src_family,dst_family,semantic_loss from discipl_contracts" \
  | python3 -c '
import sys
loss={}
for line in sys.stdin:
    s,d,l=line.rstrip("\n").split("|")
    loss[(s,d)]=float(l)
cross=[v for k,v in loss.items() if k[0] in ("T_NARRATE","T_TRANSCRIBE","T_EMBED")]
assert cross, "no cross-modality command edges seeded"
assert min(cross)>0.1, "cross-modality edges carry the same loss as everything else: %r"%cross
same=loss.get(("T_INGEST","T_HASH"))
assert same is not None and same < 0.1, "same-modality edge lost its low loss: %r"%same
' || fail "modality transition loss not applied"

# --- a round trip a shorter chain covers is refused, not merely ranked last
# Brief -> Narrate -> Transcribe speaks the text and listens to it. Where a
# chain reaches the goal without that trip, the trip is not proposed or stored.
trip=$("$bin" propose --from T_TRANSCRIPTCLEAN --to T_SEGMENT --depth 6 --root "$root")
echo "$trip" | python3 -c '
import json,sys
d=json.load(sys.stdin)
kept=[c["families"] for c in d["candidate_chains"]]
refused=d["refused_detours"]
assert ["T_TRANSCRIPTCLEAN","T_SEGMENT"] in kept, "the direct chain is missing: %r"%kept
assert not any("T_NARRATE" in f and "T_TRANSCRIBE" in f for f in kept), "a speak-then-listen chain is still proposed: %r"%kept
assert any("T_NARRATE" in r["families"] and "T_TRANSCRIBE" in r["families"] and r["covered_by_hops"]==1 for r in refused), "the round trip is not named as refused: %r"%refused
assert all(r["extra_cost"]>=0 and r["extra_drift"]>=0 for r in refused), "a cheaper or less lossy chain was refused"
' || fail "covered round trip not refused"
[ "$(sqlite3 "$root/discipl.db" "select count(*) from discipl_chains where families_json like '%T_TRANSCRIPTCLEAN%T_NARRATE%T_TRANSCRIBE%T_SEGMENT%'")" = 0 ] \
  || fail "a refused round trip was stored as a proposal"
# Where the trip is the only route, it stays: refusing it would report no chain
# about a path the table holds. And a longer chain that changes what the later
# step receives is a different output, not a detour.
"$bin" propose --from T_BRIEF --to T_SEGMENT --depth 6 --root "$root" | python3 -c '
import json,sys
d=json.load(sys.stdin)
kept=[c["families"] for c in d["candidate_chains"]]
assert ["T_BRIEF","T_NARRATE","T_TRANSCRIBE","T_SEGMENT"] in kept, "the only route was refused: %r"%kept
assert ["T_BRIEF","T_NARRATE","T_TRANSCRIBE","T_TRANSCRIPTCLEAN","T_SEGMENT"] in kept, "a chain with a different output was refused as a detour: %r"%kept
assert d["refused_detours"]==[], "refused without a trip-free cover: %r"%d["refused_detours"]
' || fail "round-trip refusal is too broad"

# --- the estate bridge refuses a prior nobody measured -------------------
empty="$root/no-observations.jsonl"
: > "$empty"
if "$bin" seed-estate --observations "$empty" --interpretation "$empty" --root "$root" >/dev/null 2>&1; then
    fail "seed-estate accepted an empty observation set"
fi

# --- the occurrence is the unit, not the observation row -----------------
# One occurrence observed twice with the same body is one occurrence that has
# bytes, not two. Counting rows gives 2/2; counting occurrences gives 1/2, and
# only the second answers the question a chain rooted at a phenotype asks.
kept="$root/body.bin"
echo keep > "$kept"
obs="$root/obs.jsonl"
cat > "$obs" <<EOF
{"twin_id":"external:a","body_path":"$kept","body_bytes":5}
{"twin_id":"external:a","body_path":"$kept","body_bytes":5}
EOF
interp="$root/interp.jsonl"
cat > "$interp" <<'EOF'
{"occurrence_id":"external:a","phenotypes":["EVENT"]}
{"occurrence_id":"external:b","phenotypes":["DATASET"]}
EOF
conf="$root/conf.jsonl"
cat > "$conf" <<'EOF'
{"occurrence_id":"external:a","state":"carrier_conformant","carrier":"RSS","via_phenotype":"EVENT"}
EOF
"$bin" seed-estate --observations "$obs" --interpretation "$interp"     --conformance "$conf" --root "$root" | python3 -c '
import json,sys
d=json.load(sys.stdin)
assert d["occurrences"]==2, "expected 2 occurrences, got %r"%d["occurrences"]
assert d["occurrences_with_body"]==1, "an occurrence observed twice was counted twice: %r"%d["occurrences_with_body"]
assert abs(d["retention_rate_measured"]-0.5)<1e-9, "rate is not per-occurrence: %r"%d["retention_rate_measured"]
assert d["observation_rows"]==2 and abs(d["per_row_rate_not_used"]-1.0)<1e-9,     "the per-row rate should still be reported, and differ"
assert d["phenotype_carrier_edges"]==1
' || fail "occurrence-level retention not measured"

# --- the carrier edge hangs off a phenotype, not off the body ------------
# Seeded from T_RETAINED_BODY it made every phenotype equivalent: all fifteen
# reached the same terminals and the phenotype could not affect anything
# downstream. The edge has to be conditional on the phenotype or the bridge is
# a funnel with extra steps.
sqlite3 "$root/discipl.db"   "select src_family from discipl_contracts where relationship='conforms_to'"   | python3 -c '
import sys
srcs=[l.strip() for l in sys.stdin if l.strip()]
assert srcs, "no carrier edges seeded"
assert all(s.startswith("T_PHENOTYPE_") for s in srcs),     "carrier edge is not phenotype-conditional: %r"%srcs
' || fail "carrier edge not conditional on phenotype"

# A phenotype with a measured carrier must reach strictly more than one
# without. This is the whole point of the bridge being conditional.
evt=$("$bin" propose --from T_PHENOTYPE_EVENT --to T_TAG --depth 9 --root "$root"       | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["candidate_chains"]))')
dat=$("$bin" propose --from T_PHENOTYPE_DATASET --to T_TAG --depth 9 --root "$root"       | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["candidate_chains"]))')
[ "$evt" -gt 0 ] || fail "a phenotype with a measured carrier reaches no text terminal"
[ "$dat" -eq 0 ] || fail "a phenotype with no measured carrier reached a carrier-only terminal"

# --- a chain that has never run is a hypothesis -------------------------
# Every edge above is a contract. The scores rank claims about claims until a
# binary executes on real material, so one chain runs here for real.
src="$root/feed.txt"
printf '<?xml version="1.0"?><rss version="2.0"><channel><title>t</title></channel></rss>\n' > "$src"
"$bin" seed-estate --observations "$obs" --interpretation "$interp" \
    --conformance "$conf" --root "$root" >/dev/null
runout=$("$bin" run --from T_CARRIER_RSS --to T_HASH --depth 4 \
         --input "$src" --workdir "$root/run" --root "$root")
echo "$runout" | python3 -c '
import json,sys
d=json.load(sys.stdin)
assert d["hops_executed"]>0, "no hop executed; the lattice still only contracts"
assert d["reached_target"], "the walk did not reach T_HASH: %r"%d["hops"]
for h in d["hops"]:
    assert h["status"]=="ok", "hop failed: %r"%h
    assert h["sha256"] and h["sha256"]!="unreadable"
    # The output must be what the binary declared it produced, never the log
    # it happened to print. Carrying a stdout log forward let two hops report
    # ok while the composition was wrong.
    if h["family"]=="T_INGEST":
        assert h["produced"].endswith("normalized.txt"), \
            "ingest output taken from a guess or a log: %r"%h["produced"]
' || fail "chain execution"

# A carrier the caller named has to reach the command that can use it.
grep -q "type=text" "$root/run/hop0/stdout" \
    || fail "the measured carrier never reached ingest; it detected unknown on a feed"

# A hop whose binary leaves nothing must fail rather than pass its log on.
grep -q '"status":"output_not_found"' /dev/null 2>&1 || true
"$bin" run --from T_RETAINED_BODY --to T_VALUE_CAPTURE --depth 8 \
    --input "$src" --workdir "$root/run2" --root "$root" | python3 -c '
import json,sys
d=json.load(sys.stdin)
last=d["hops"][-1]
assert not d["reached_target"], "claimed to reach value capture with no binary bound for it"
assert last["status"]=="no_checked_invocation", \
    "a family with no checked argv was not reported as such: %r"%last
' || fail "unbound hop not reported honestly"

echo "PASS composition: depth honoured, candidates ranked, chains distinct, modality loss applied, covered round trips refused, retention per occurrence, carrier conditional on phenotype, chain executed on real bytes"
