#!/bin/sh
# HVCP on real admitted vectors, not command presence.
#
# The vectors are nomic-static-128 embeddings of the site passages whose
# agreement with the ONNX teacher was measured under a memory grant
# (estate/generated/nomic-static-128-teacher-agreement-site-passages-*.json).
# This test refuses to run on any other model or corpus bytes. It takes a
# memory grant on a copy of the admission ledger before loading the model,
# builds a pack in an isolated HOME, and checks what a leapfrog integrator
# must show: energy error that falls as dt^2 at fixed integration time,
# bit-identical replay, trace replay, branch/compare, and refusal of a vector
# of another width. With BONFYRE_PHYSICS_WITNESS_OUT it writes a TSV witness.
set -eu
bin=${1:-./bonfyre-physics}
case "$bin" in /*) ;; *) bin=$(pwd)/$bin ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
embed=${BONFYRE_EMBED_BIN:-$root/cmd/BonfyreEmbed/bonfyre-embed}
admission=${BONFYRE_ADMISSION_BIN:-$HOME/.bonfyre/bin/bonfyre-admission}
meter=${BONFYRE_METER_BIN:-$HOME/.bonfyre/bin/bonfyre-meter}
ledger=${BONFYRE_ADMISSION_DB:-$HOME/.bonfyre/admission.db}
receipt=${BONFYRE_AGREEMENT_RECEIPT:-$(ls "$root"/estate/generated/nomic-static-128-teacher-agreement-site-passages-*.json 2>/dev/null | tail -n 1)}
for need in "$embed" "$admission" "$meter"; do
  [ -x "$need" ] || { echo "physics real-vector witness: SKIP (missing $need)"; exit 0; }
done
[ -n "$receipt" ] && [ -f "$receipt" ] && [ -f "$ledger" ] || { echo "physics real-vector witness: SKIP (no agreement receipt or ledger)"; exit 0; }
field() { sed -n "/\"$1\": {/,/}/s/.*\"$2\": \"\\([^\"]*\\)\".*/\\1/p" "$receipt" | head -n 1; }
model=$(field static path); model_sha=$(field static sha256)
corpus=$(field corpus path); corpus_sha=$(field corpus sha256)
[ -f "$model" ] && [ -f "$corpus" ] || { echo "physics real-vector witness: SKIP (receipt bytes not on this machine)"; exit 0; }
sha() { shasum -a 256 "$1" | awk '{print $1}'; }
fail() { echo "physics real-vector witness: $*" >&2; exit 1; }
[ "$(sha "$model")" = "$model_sha" ] || fail "model bytes differ from the measured receipt"
[ "$(sha "$corpus")" = "$corpus_sha" ] || fail "corpus bytes differ from the measured receipt"

tmp=$(mktemp -d "${TMPDIR:-/tmp}/physics-real.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/home"
H="$tmp/home"

# The representation passport in a copy of the model registry must name these
# exact bytes and this receipt; its gate decision is recorded, not overridden.
model_bin=${BONFYRE_MODEL_BIN:-$HOME/.bonfyre/bin/bonfyre-model}
models_db=${BONFYRE_MODEL_DB:-$HOME/.local/share/bonfyre/models.db}
passport=nomic-static-128/bfse-int8/2026-10-17
[ -x "$model_bin" ] && [ -f "$models_db" ] || fail "no model registry to read the representation passport from"
sqlite3 "$models_db" ".backup '$tmp/models.db'"
pp=$(sqlite3 "$tmp/models.db" "SELECT physical_digest||' '||receipt_digest||' '||measurement_state FROM model_representation_passport WHERE representation_id='$passport'")
[ "$pp" = "$model_sha $(sha "$receipt") measured" ] || fail "passport $passport does not bind these bytes and receipt: $pp"
gate=$(BONFYRE_MODEL_DB="$tmp/models.db" "$model_bin" representation-gate "$passport" --scope embedding)

# Memory grant before the model loads (2 x model bytes), on a ledger copy.
sqlite3 "$ledger" ".backup '$tmp/admission.db'"
model_bytes=$(wc -c <"$model" | tr -d ' ')
grant=$("$admission" request --db "$tmp/admission.db" --plane physics-hvcp-witness --kind model \
        --bytes $((model_bytes * 2)) --volume memory)
echo "$grant" | grep -F '"verdict":"admit"' >/dev/null || fail "memory grant not admitted: $grant"
grant_id=$(echo "$grant" | sed -n 's/.*"grant_id":\([0-9]*\).*/\1/p')
HOME="$H" "$embed" static --model "$model" --input-jsonl "$corpus" --store --out "$tmp/vecs.jsonl"
release=$("$admission" release --db "$tmp/admission.db" --grant "$grant_id")
rows=$(wc -l <"$tmp/vecs.jsonl" | tr -d ' ')
visible=$(grep -c -v '"tokens":0,' "$tmp/vecs.jsonl" || true)
HOME="$H" "$embed" pack >"$tmp/pack.out"
pack="$H/.local/share/bonfyre/embeds/pack.bfpack"
packed=$(sed -n 's/^embed pack: packed \([0-9]*\) vectors.*/\1/p' "$tmp/pack.out")
[ "$packed" -gt 1000 ] || fail "pack holds $packed vectors"
[ "$packed" -le "$visible" ] || fail "pack holds more vectors than visible rows"
P="--pack $pack --bvh $tmp/i.bfvh"
HOME="$H" "$bin" bvh --pack "$pack" --out "$tmp/i.bfvh" >"$tmp/bvh.out"
grep -F "$packed vecs  dim=128" "$tmp/bvh.out" >/dev/null || fail "bvh does not cover the pack"
bvh_nodes=$(sed -n 's/^bvh: \([0-9]*\) nodes.*/\1/p' "$tmp/bvh.out")

# Leapfrog is second order: at fixed time T=0.64 halving dt cuts |dH| ~4x.
dh() { sed -n 's/.*ΔH=\([-0-9.e+]*\).*/\1/p' "$1" | tail -n 1 | awk '{v=$1+0; if(v<0)v=-v; printf "%.6g", v}'; }
h0() { sed -n 's/.*H₀=\([-0-9.]*\).*/\1/p' "$1" | head -n 1; }
conv=""
for row in 0 $((packed / 3)) $((2 * packed / 3)); do
  line="row=$row"
  for pair in "0.01 64" "0.005 128" "0.0025 256"; do
    set -- $pair
    HOME="$H" "$bin" init --pack-row "$row" --dt "$1" $P --state "$tmp/c$row-$1.bfps" >/dev/null
    HOME="$H" "$bin" run --steps "$2" $P --state "$tmp/c$row-$1.bfps" >"$tmp/c$row-$1.out" || fail "run row $row dt $1"
    grep -F "steps=$2 " "$tmp/c$row-$1.out" >/dev/null || fail "row $row dt $1 stopped early"
    line="$line dt$1:$(dh "$tmp/c$row-$1.out")"
  done
  a=$(dh "$tmp/c$row-0.01.out"); b=$(dh "$tmp/c$row-0.005.out"); c=$(dh "$tmp/c$row-0.0025.out")
  h=$(h0 "$tmp/c$row-0.0025.out")
  awk -v a="$a" -v b="$b" -v c="$c" -v h="$h" 'BEGIN{
    r1=a/b; r2=b/c; rel=c/(h<0?-h:h);
    if(!(r1>3 && r1<6 && r2>3 && r2<6)) { printf "dt^2 ratios %.2f %.2f\n", r1, r2; exit 1 }
    if(!(rel<1e-4)) { printf "relative drift %.3g at dt=0.0025\n", rel; exit 1 } }' >"$tmp/awk.err" \
    || fail "row $row: $(cat "$tmp/awk.err")"
  line="$line ratios:$(awk -v a="$a" -v b="$b" -v c="$c" 'BEGIN{printf "%.2f,%.2f", a/b, b/c}') H0:$h"
  conv="$conv$line;"
done

# Determinism and trace replay on one admitted trajectory.
HOME="$H" "$bin" init --pack-row 0 --dt 0.0025 $P --state "$tmp/a.bfps" >/dev/null
cp "$tmp/a.bfps" "$tmp/b.bfps"
init_sha=$(sha "$tmp/a.bfps")
HOME="$H" "$bin" run --steps 256 --trace "$tmp/a.jsonl" $P --state "$tmp/a.bfps" >"$tmp/a.out"
HOME="$H" "$bin" run --steps 256 --trace "$tmp/b.jsonl" $P --state "$tmp/b.bfps" >/dev/null
final_sha=$(sha "$tmp/a.bfps")
[ "$final_sha" = "$(sha "$tmp/b.bfps")" ] || fail "identical start produced different states"
[ "$(sha "$tmp/a.jsonl")" = "$(sha "$tmp/b.jsonl")" ] || fail "identical start produced different traces"
[ "$final_sha" != "$init_sha" ] || fail "trajectory did not move"
HOME="$H" "$bin" trace "$tmp/a.jsonl" --replay | grep -F "256 events replayed" >/dev/null || fail "trace replay"
HOME="$H" "$bin" energy $P --state "$tmp/a.bfps" | grep -F "step=256" >/dev/null || fail "energy"
nearest=$(HOME="$H" "$bin" nearest --k 3 $P --state "$tmp/a.bfps" | sed -n 's/.*\[ 1\] score=\([-0-9.]*\).*/\1/p')
HOME="$H" "$bin" collide --k 16 $P --state "$tmp/a.bfps" >/dev/null || fail "collide"
HOME="$H" "$bin" branch --as left --as right --state "$tmp/a.bfps" >/dev/null
HOME="$H" "$bin" compare "$tmp/branches/left.bfps" "$tmp/branches/right.bfps" $P >"$tmp/cmp.out" || fail "compare"
[ "$(grep -c '^left.bfps\|^right.bfps' "$tmp/cmp.out")" = 2 ] || fail "compare rows"

# A vector of another width never becomes a state against this pack.
perl -e 'print "VECF".pack("V",32).pack("f<*", map { sin($_) } 1..32)' >"$tmp/h32.vecf"
if HOME="$H" "$bin" init "$tmp/h32.vecf" $P --state "$tmp/h32.bfps" 2>"$tmp/h32.err"; then fail "32-d input admitted"; fi
grep -F "input dim 32 != pack dim 128" "$tmp/h32.err" >/dev/null || fail "32-d refusal message"
[ ! -e "$tmp/h32.bfps" ] || fail "refused 32-d init persisted"

# Meter records the run as unpriced work against the trace digest.
at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
"$meter" record --db "$tmp/meter.db" --key physics-hvcp --op physics.hvcp.run \
  --receipt "sha256:$(sha "$tmp/a.jsonl")" --attempt-ref "goal:2f49fc7899e24bd4a52fdd631876bae6" \
  --realization-ref "sha256:$final_sha" --at "$at" --bytes "$(wc -c <"$pack" | tr -d ' ')" --duration 0 >"$tmp/meter.out" 2>&1
"$meter" usage --key physics-hvcp --db "$tmp/meter.db" | grep -F "physics.hvcp.run" >/dev/null || fail "meter readback"

if [ -n "${BONFYRE_PHYSICS_WITNESS_OUT:-}" ]; then
  {
    printf 'key\tvalue\n'
    printf 'schema\tbonfyre.physics.hvcp-real-vector-witness.v1\n'
    printf 'at\t%s\n' "$at"
    printf 'goal\t2f49fc7899e24bd4a52fdd631876bae6\n'
    printf 'physics_binary_sha256\t%s\n' "$(sha "$bin")"
    printf 'physics_source_sha256\t%s\n' "$(sha "$root/cmd/BonfyrePhysics/src/main.c")"
    printf 'physics_lib_source_sha256\t%s\n' "$(sha "$root/lib/libbonfyre/bf_physics.c")"
    printf 'embed_binary_sha256\t%s\n' "$(sha "$embed")"
    printf 'embed_static_source_sha256\t%s\n' "$(sha "$root/cmd/BonfyreEmbed/src/static_embed.h")"
    printf 'representation\tnomic-static-128 (.bfse, 128 dims, distilled from nomic-embed-text-onnx)\n'
    printf 'representation_sha256\t%s\n' "$model_sha"
    printf 'representation_agreement_receipt\t%s\n' "${receipt#"$root"/}"
    printf 'representation_agreement_receipt_sha256\t%s\n' "$(sha "$receipt")"
    printf 'representation_neighbour_overlap_at_10\t%s\n' "$(sed -n 's/.*"neighbour_overlap_at_k": \([0-9.]*\).*/\1/p' "$receipt")"
    printf 'representation_passport\t%s (copy of %s; physical and receipt digests match)\n' "$passport" "$models_db"
    printf 'representation_gate_embedding\t%s\n' "$gate"
    printf 'corpus\t%s\n' "$corpus"
    printf 'corpus_sha256\t%s\n' "$corpus_sha"
    printf 'admission_ledger\tcopy of %s\n' "$ledger"
    printf 'admission_grant\t%s\n' "$grant"
    printf 'admission_release\t%s\n' "$release"
    printf 'rows_embedded\t%s\n' "$rows"
    printf 'pack_vectors\t%s\n' "$packed"
    printf 'pack_sha256\t%s\n' "$(sha "$pack")"
    printf 'bvh_nodes\t%s\n' "$bvh_nodes"
    printf 'energy_convergence_T0.64\t%s\n' "$conv"
    printf 'determinism\tinit %s -> 256 steps dt=0.0025 -> %s twice; traces identical\n' "$init_sha" "$final_sha"
    printf 'trace_sha256\t%s\n' "$(sha "$tmp/a.jsonl")"
    printf 'trace_replay_events\t256\n'
    printf 'nearest_top1_score_after_run\t%s\n' "$nearest"
    printf 'branch_compare\tleft,right compared under the pack: pass\n'
    printf 'negative_width\t32-d vecf refused before state: input dim 32 != pack dim 128\n'
    printf 'meter\t%s\n' "$(tr '\n' ' ' <"$tmp/meter.out")"
    printf 'cannot_infer\tdH ~ dt^2 shows a symplectic second-order integrator over this potential; it says nothing about retrieval, reasoning or meaning\n'
    printf 'cannot_infer\tthe static vectors agree with their teacher at 0.19 top-10 overlap; physics over them inherits that representation, it does not improve it\n'
    printf 'cannot_infer\ttime reversibility is not exercised: no command exports or negates p\n'
    printf 'cannot_infer\tthe passport gate answers DEFER for the embedding scope: these vectors are a measured latent representation, not an admitted substitute for the teacher or a ranker of record\n'
    printf 'cannot_infer\tthe grant was taken on a copy of the admission ledger; the live ledger was not changed\n'
  } >"$BONFYRE_PHYSICS_WITNESS_OUT"
fi
echo "physics real-vector witness: PASS ($packed admitted vectors; |dH| ~ dt^2 on 3 rows; deterministic replay; trace, branch, compare; 32-d refused; metered)"
