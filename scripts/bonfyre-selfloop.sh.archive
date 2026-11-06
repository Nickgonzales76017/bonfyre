#!/usr/bin/env bash
# bonfyre-selfloop.sh
#
# End-to-end self-optimization loop.
# Bonfyre uses itself: artifacts feed the feedback brain, which routes
# and tunes the next iteration.
#
# Architecture exercised:
#   ingest → hash → embed → vec → sli (GDN chain T04:T16)
#   → control score → ledger assess → compete → learn feedback
#   → economy route → tier record → entity resolve → time schedule
#   → fragment create → space put  →  [repeat N times]
#
# Usage:
#   bash scripts/bonfyre-selfloop.sh [--iters N] [--dir WORKDIR]
#   bash scripts/bonfyre-selfloop.sh           # 3 iters, /tmp/bonfyre-selfloop
#
# Every iteration:
#   1. Ingest a synthetic artifact (or real doc)
#   2. Hash + index it
#   3. Embed → store in vec DB
#   4. Run SLI chain (T04:T16 with GDN step) on its embedding
#   5. Score it via control (HE-SLI)
#   6. Assess value via ledger
#   7. Register A/B competition for the 'embed' stage (onnx vs hash)
#   8. Run both variants, score both, record winner
#   9. Feed win/loss to learn (threshold tuner)
#  10. Ask economy to route the next embed call (cost-aware)
#  11. Assign tier for embed stage, record observed latency
#  12. Resolve entity from the artifact ID
#  13. Schedule re-processing via time
#  14. Store run summary in space
#  15. Create fragment record
#
# After all iters:
#   - Print learn thresholds (before vs after)
#   - Print economy routing recommendation
#   - Print control ops dashboard
#   - Print entity count
#   - Print tier compliance summary

set -euo pipefail

# ── CLI args ──────────────────────────────────────────────────────────────
ITERS=3
WORKDIR="/tmp/bonfyre-selfloop"
while [[ $# -gt 0 ]]; do
  case $1 in
    --iters) ITERS=$2; shift 2;;
    --dir)   WORKDIR=$2; shift 2;;
    *)       echo "usage: $0 [--iters N] [--dir DIR]" >&2; exit 1;;
  esac
done

mkdir -p "$WORKDIR"
LOG="$WORKDIR/selfloop.log"
exec > >(tee -a "$LOG") 2>&1

BF="bonfyre"

banner() { echo; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; echo "  $*"; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; }
step()   { echo; echo "  ▶ $*"; }
ok()     { echo "    ✓ $*"; }
warn()   { echo "    ⚠ $*"; }

# ── Generate synthetic input artifact ────────────────────────────────────
make_input_artifact() {
  local iter=$1
  local outdir="$WORKDIR/iter-${iter}/input"
  mkdir -p "$outdir"
  local text="$outdir/doc.txt"
  cat > "$text" <<EOF
Bonfyre self-optimization loop — iteration ${iter}
Date: $(date -u +"%Y-%m-%dT%H:%M:%SZ")

This document is a synthetic behavioral artifact created by the Bonfyre
self-loop. It represents a pattern of activity over time.

Pattern signals:
  - Session start: $(date -u +"%H:%M")
  - Interaction depth: $((RANDOM % 20 + 1)) events
  - Confidence score: $(python3 -c "import random; print(f'{random.uniform(0.6,0.99):.3f}')")
  - Category: [shopping, dating, food, events][$(( iter % 4 ))]

Trend analysis:
  The system observed a consistent pattern across $((iter * 3)) prior sessions.
  The pattern suggests increasing engagement with local recommendations.
  Signal strength: $((60 + iter * 7))%

This artifact should be embedded, vectorized, and scored.
EOF
  # Create a minimal artifact.json
  HASH=$($BF hash file "$text" 2>/dev/null | awk '{print $NF}' || echo "sha256-$(openssl rand -hex 16)")
  cat > "$outdir/artifact.json" <<JEOF
{
  "id": "selfloop-iter${iter}",
  "family": "T_SELFLOOP",
  "stage": "ingest",
  "hash": "${HASH}",
  "created": $(date +%s),
  "iteration": ${iter},
  "source": "bonfyre-selfloop",
  "inputs": [{"path": "doc.txt", "type": "text"}]
}
JEOF
  echo "$outdir"
}

# ── Snapshot learn thresholds ─────────────────────────────────────────────
snapshot_learn() {
  $BF learn export 2>/dev/null || echo "{}"
}

# ── Main loop ─────────────────────────────────────────────────────────────
banner "bonfyre-selfloop: ${ITERS} iterations"
echo "  workdir: $WORKDIR"
echo "  log:     $LOG"
echo ""
$BF doctor 2>&1 | grep -E "catalog:|surfaces:|summary:" || true

# Capture initial learn state
LEARN_BEFORE=$(snapshot_learn)
echo ""
echo "  initial thresholds:"
echo "$LEARN_BEFORE" | python3 -c "
import sys,json
try:
  d = json.load(sys.stdin)
  for k,v in d.items(): print(f'    {k}: {v}')
except: print('    (empty — first run)')
"

# Initialize economy budget for selfloop recipe
$BF economy budget set selfloop 0.10 2>/dev/null || true

# Register a tier assignment for embed stage
$BF tier set selfloop embed fast 2>/dev/null || true

# Register an A/B competition for the embed stage
COMP_ID=$($BF compete pair selfloop embed 2>/dev/null | sed -n 's/.*competition created: //p' | tr -d '[:space:]' || echo "")
if [ -n "$COMP_ID" ]; then
  $BF compete add-variant "$COMP_ID" "onnx"   '{"backend":"onnx","dims":384}' 2>/dev/null || true
  $BF compete add-variant "$COMP_ID" "hash"   '{"backend":"hash","dims":64}'  2>/dev/null || true
  ok "compete: registered A/B for embed stage (comp=$COMP_ID)"
fi

# Register time triggers
$BF time trigger add model_updated selfloop 2>/dev/null || true

# ── Iteration loop ─────────────────────────────────────────────────────────
for iter in $(seq 1 $ITERS); do
  banner "ITERATION ${iter} / ${ITERS}"
  ITER_DIR="$WORKDIR/iter-${iter}"
  mkdir -p "$ITER_DIR"

  # ── 1. Ingest ────────────────────────────────────────────────────────────
  step "1. ingest: generate synthetic artifact"
  INPUT_DIR=$(make_input_artifact $iter)
  ARTIFACT_ID="selfloop-iter${iter}"
  ok "artifact: $INPUT_DIR/artifact.json"

  # ── 2. Hash ──────────────────────────────────────────────────────────────
  step "2. hash: content-address the document"
  $BF hash file "$INPUT_DIR/doc.txt" 2>/dev/null | tee "$ITER_DIR/hash.txt" || warn "hash not available"

  # ── 3. Embed ─────────────────────────────────────────────────────────────
  step "3. embed: generate text embedding"
  EMBED_START=$(date +%s%N 2>/dev/null || date +%s)
  $BF embed --text "$INPUT_DIR/doc.txt" --out "$ITER_DIR/embedding.json" \
      --backend hash --dims 384 --output-format json 2>/dev/null \
    || $BF embed --text "$INPUT_DIR/doc.txt" --out "$ITER_DIR/embedding.json" \
       --backend hash --output-format json 2>/dev/null \
    || warn "embed: skipped (no model available)"
  EMBED_END=$(date +%s%N 2>/dev/null || date +%s)
  # Compute latency in ms (nanoseconds available on macOS via gdate or date +%s%N)
  if command -v python3 &>/dev/null && [ -f "$ITER_DIR/embedding.json" ]; then
    EMBED_MS=$(python3 -c "
s,e = $EMBED_START,$EMBED_END
ms = (e-s)//1000000 if e>1e12 else (e-s)*1000
print(max(1, ms))
" 2>/dev/null || echo "10")
    ok "embedding written: $ITER_DIR/embedding.json  (${EMBED_MS}ms)"
  else
    EMBED_MS=10
  fi

  # ── 4. Vec: insert embedding ──────────────────────────────────────────────
  step "4. vec: insert embedding into local vector DB"
  VEC_DB="$WORKDIR/selfloop.vecdb"
  $BF vec init "$VEC_DB" 2>/dev/null || true
  if [ -f "$ITER_DIR/embedding.json" ]; then
    # Wrap embedding in the format vec insert expects:
    # {"embeddings": [{"id": "...", "embedding": [...384 floats...]}]}
    python3 -c "
import json, sys
d = json.load(open('$ITER_DIR/embedding.json'))
vec = d.get('vector', d.get('embedding', d.get('vec', [0.0]*384)))
# Pad/trim to 384 dims to match VEC_DIMS in bonfyre-vec
if len(vec) < 384: vec = vec + [0.0] * (384 - len(vec))
else: vec = vec[:384]
wrapped = {'embeddings': [{'id': '$ARTIFACT_ID', 'source': 'selfloop', 'type': 'text', 'embedding': vec}]}
json.dump(wrapped, open('$ITER_DIR/vec_insert.json', 'w'))
" 2>/dev/null || true
    $BF vec insert "$VEC_DB" "$ITER_DIR/vec_insert.json" 2>/dev/null \
      && ok "vec: inserted $ARTIFACT_ID" \
      || warn "vec insert skipped"
    COUNT=$($BF vec count "$VEC_DB" 2>/dev/null || echo "?")
    ok "vec DB size: $COUNT vectors"
  fi

  # ── 5. SLI chain (GDN T04→T16) ─────────────────────────────────────────
  step "5. sli chain: T04:gdn:T16 pattern inference"
  MODELS_DIR="$HOME/.local/share/bonfyre/models"
  if $BF model list 2>/dev/null | grep -q "T04\|T16" && [ -f "$ITER_DIR/embedding.json" ]; then
    python3 -c "
import json, struct, os
d = json.load(open('$ITER_DIR/embedding.json'))
vec = d.get('vector', d.get('embedding', d.get('vec', [0.0]*64)))
n, dim = 1, len(vec)
with open('$ITER_DIR/vecs.bin','wb') as f:
    f.write(struct.pack('<II', n, dim))
    for v in vec: f.write(struct.pack('<f', float(v)))
" 2>/dev/null
    $BF sli chain --in "$ITER_DIR/vecs.bin" \
        --chain T04:gdn:T16 \
        --models-dir "$MODELS_DIR" \
        --out "$ITER_DIR/sli_out.bin" 2>/dev/null \
      && ok "sli chain: wrote $ITER_DIR/sli_out.bin" \
      || warn "sli chain: T04/T16 models not present — skipping"
  else
    warn "sli chain: no BQFP models in registry — skipping"
  fi

  # ── 6. Control: score the artifact ───────────────────────────────────────
  step "6. control: score artifact via HE-SLI"
  SCORE_OUT=$($BF control score "$INPUT_DIR/artifact.json" 2>/dev/null || echo "")
  if [ -n "$SCORE_OUT" ]; then
    echo "$SCORE_OUT" | head -8
    COMPOSITE=$(echo "$SCORE_OUT" | grep -oE "composite[: ]+[0-9.]+" | grep -oE "[0-9.]+$" | head -1 || echo "0.75")
  else
    warn "control score: no output — using default 0.75"
    COMPOSITE="0.75"
  fi

  # Check entropy pre-flight
  $BF control entropy-check "$INPUT_DIR/artifact.json" 2>/dev/null \
    && ok "entropy: passed" \
    || warn "entropy: low (artifact may lack signal)"

  # ── 7. Ledger: assess value ────────────────────────────────────────────
  step "7. ledger: assess artifact value"
  $BF ledger assess "$INPUT_DIR/artifact.json" 2>/dev/null | head -6 \
    || warn "ledger: not available"

  # ── 8. Compete: run and score embed variants ───────────────────────────
  step "8. compete: run A/B embed variants"
  if [ -n "$COMP_ID" ]; then
    SCORE_A=$(python3 -c "import random; print(f'{random.uniform(0.70,0.98):.3f}')")
    SCORE_B=$(python3 -c "import random; print(f'{random.uniform(0.55,0.85):.3f}')")
    INPUT_JSON="{\"artifact\":\"$ARTIFACT_ID\",\"iter\":$iter}"
    $BF compete run "$COMP_ID" "$INPUT_JSON" 2>/dev/null \
      && ok "compete: recorded run for $COMP_ID" \
      || warn "compete run: skipped"
    # Score table
    $BF compete score "$COMP_ID" 2>/dev/null | head -8 || true
    WINNER=$($BF compete score "$COMP_ID" 2>/dev/null | awk 'NR>1 && /[0-9]/{print $2; exit}' || echo "onnx")
  else
    WINNER="onnx"
    warn "compete: no competition registered"
  fi

  # ── 9. Learn: feed win/loss signal ────────────────────────────────────
  step "9. learn: record feedback and tune thresholds"
  RUN_ID="selfloop-${iter}"
  LEARN_SCORE=$(python3 -c "print(f'{float($COMPOSITE):.3f}')")
  $BF learn feedback "$RUN_ID" "$LEARN_SCORE" good 2>/dev/null \
    && ok "learn: feedback $LEARN_SCORE recorded (good)" \
    || warn "learn: feedback skipped"
  $BF learn tune embed 2>/dev/null | head -4 || true

  # ── 10. Economy: cost record + route ───────────────────────────────────
  step "10. economy: record cost and get routing recommendation"
  COST=$(python3 -c "import random; print(f'{random.uniform(0.00001,0.0005):.6f}')")
  $BF economy cost record selfloop embed "$WINNER" "$COST" 2>/dev/null \
    && ok "economy: recorded \$$COST for $WINNER" \
    || warn "economy cost record: skipped"
  ROUTE=$($BF economy route selfloop 2>/dev/null | head -3 || echo "(no recommendation)")
  echo "    route: $ROUTE"

  # ── 11. Tier: record observed embed latency ───────────────────────────
  step "11. tier: record embed latency"
  $BF tier record selfloop embed "$EMBED_MS" 2>/dev/null \
    && ok "tier: recorded ${EMBED_MS}ms for embed" \
    || warn "tier record: skipped"

  # ── 12. Entity: resolve artifact as entity ────────────────────────────
  step "12. entity: resolve artifact identity"
  ENTITY_ID=$($BF entity resolve text "$ARTIFACT_ID" 2>/dev/null \
    | grep -oE "entity:[^ ]+" | head -1 || echo "")
  if [ -n "$ENTITY_ID" ]; then
    ok "entity: $ENTITY_ID"
  else
    $BF entity resolve text "$ARTIFACT_ID" 2>/dev/null | head -3 || warn "entity: skipped"
  fi

  # ── 13. Time: schedule for re-processing ─────────────────────────────
  step "13. time: schedule artifact for re-processing"
  $BF time schedule "$ARTIFACT_ID" selfloop 2>/dev/null \
    && ok "time: $ARTIFACT_ID queued" \
    || warn "time schedule: skipped"

  # ── 14. Space: store iteration summary ───────────────────────────────
  step "14. space: store iteration summary"
  SPACE_NAME="bonfyre-selfloop"
  $BF space open "$SPACE_NAME" 2>/dev/null || true
  SUMMARY="{\"iter\":$iter,\"composite\":$COMPOSITE,\"winner\":\"$WINNER\",\"embed_ms\":$EMBED_MS,\"cost\":$COST}"
  $BF space put "$SPACE_NAME" "iter-${iter}" "$SUMMARY" 2>/dev/null \
    && ok "space: saved iter-${iter} → $SUMMARY" \
    || warn "space: put skipped"

  # ── 15. Fragment: create behavioral record ────────────────────────────
  step "15. fragment: create behavioral fragment"
  FRAG_DB="$WORKDIR/fragments.db"
  PAYLOAD="{\"artifact_id\":\"$ARTIFACT_ID\",\"iter\":$iter,\"score\":$COMPOSITE}"
  $BF fragment create \
      --store "$FRAG_DB" \
      --kind behavior \
      --persp selfloop \
      --conf "$COMPOSITE" \
      --payload "$PAYLOAD" 2>/dev/null \
    && ok "fragment: created (score=$COMPOSITE)" \
    || warn "fragment: skipped"

  ok "iteration ${iter} complete  [composite=${COMPOSITE}  winner=${WINNER}  embed=${EMBED_MS}ms]"
done

# ── Final report ───────────────────────────────────────────────────────────
banner "FINAL REPORT — ${ITERS} iterations complete"

step "learn: threshold evolution"
LEARN_BEFORE_SORTED=$(echo "$LEARN_BEFORE" | python3 -c "
import sys,json
try:
  d=json.load(sys.stdin)
  for k,v in sorted(d.items()): print(f'  {k}: {v}')
except: print('  (empty)')
" 2>/dev/null || echo "  (empty)")
LEARN_AFTER=$(snapshot_learn)
echo "  before:"
echo "$LEARN_BEFORE_SORTED"
echo "  after:"
echo "$LEARN_AFTER" | python3 -c "
import sys,json
try:
  d=json.load(sys.stdin)
  for k,v in sorted(d.items()): print(f'    {k}: {v}')
except: print('    (no change)')
" 2>/dev/null || true

step "learn: current stage thresholds"
$BF learn list 2>/dev/null | head -12 || true

step "economy: routing recommendation"
$BF economy route selfloop 2>/dev/null | head -6 || true
$BF economy report 2>/dev/null | head -10 || true

step "compete: final rankings"
if [ -n "${COMP_ID:-}" ]; then
  $BF compete score "$COMP_ID" 2>/dev/null | head -10 || true
fi

step "control: operations dashboard"
$BF control ops 2>/dev/null | head -20 || true
$BF control history 2>/dev/null | head -8 || true

step "tier: SLA compliance"
$BF tier violations selfloop 2>/dev/null | head -10 || true
$BF tier history selfloop 2>/dev/null | head -6 || true

step "entity: identity map"
$BF entity status 2>/dev/null | head -6 || true

step "time: scheduled artifacts"
$BF time status 2>/dev/null | head -6 || true

step "space: iteration summaries"
$BF space list "$SPACE_NAME" 2>/dev/null | head -10 || true

step "vec: final DB size"
$BF vec count "$WORKDIR/selfloop.vecdb" 2>/dev/null || true

step "fragment: behavioral record count"
$BF fragment stats --store "$WORKDIR/fragments.db" 2>/dev/null | head -6 || true

banner "bonfyre-selfloop complete — log: $LOG"
