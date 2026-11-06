#!/usr/bin/env bash
# bonfyre-selfloop-v2.sh
#
# Full self-utilization loop — every active Bonfyre subsystem exercises every
# other.  Artifacts flow through the complete pipeline and each output feeds
# back into the intelligence layers.
#
# Subsystems exercised per iteration (22 steps):
#   1.  ingest      — synthetic behavioral artifact
#   2.  hash        — content-address via SHA-256
#   3.  compress    — zstd pack + savings report
#   4.  embed       — 384-dim hash embedding
#   5.  vec insert  — store in local SIMD vector DB
#   6.  vec search  — find similar past artifacts (recall loop)
#   7.  vec compare — cosine similarity between consecutive iters
#   8.  sli chain   — GDN T04:T16 pattern inference
#   9.  control score  — HE-SLI quality scoring
#  10.  control route  — policy-driven execution routing
#  11.  control entropy-check — Shannon entropy pre-flight
#  12.  ledger assess  — atom/operator/realization value
#  13.  compete run    — A/B variant tournament
#  14.  learn feedback — record outcome into rolling average
#  15.  learn tune     — adjust thresholds if ≥50 samples
#  16.  economy cost record — log actual spend
#  17.  economy route  — recommend cheapest valid model (DB-aware)
#  18.  tier record    — log latency vs SLA
#  19.  queue enqueue  — submit artifact for async processing
#  20.  entity resolve — deduplicate artifact identity
#  21.  time schedule  — queue artifact for re-processing
#  22.  space put      — snapshot iteration summary
#  23.  fragment create— behavioral record with confidence
#
# Promote: after PROMOTE_AFTER iterations, winning compete variant is promoted
# to production.
#
# Usage:
#   bash scripts/bonfyre-selfloop-v2.sh [--iters N] [--dir WORKDIR]

set -euo pipefail

ITERS=10
WORKDIR="/tmp/bonfyre-selfloop-v2"
PROMOTE_AFTER=5

while [[ $# -gt 0 ]]; do
  case $1 in
    --iters)        ITERS=$2; shift 2;;
    --dir)          WORKDIR=$2; shift 2;;
    --promote-after) PROMOTE_AFTER=$2; shift 2;;
    *) echo "usage: $0 [--iters N] [--dir WORKDIR] [--promote-after N]" >&2; exit 1;;
  esac
done

mkdir -p "$WORKDIR"
LOG="$WORKDIR/selfloop.log"
exec > >(tee -a "$LOG") 2>&1

BF="bonfyre"
VEC_DB="$WORKDIR/selfloop.vecdb"
FRAG_DB="$WORKDIR/fragments.db"
SPACE_NAME="bonfyre-selfloop-v2"

banner()  { echo; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; echo "  $*"; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; }
step()    { echo; echo "  ▶ $*"; }
ok()      { echo "    ✓ $*"; }
warn()    { echo "    ⚠ $*"; }
metric()  { printf "    %-24s %s\n" "$1:" "$2"; }

# ── Synthetic artifact generator ─────────────────────────────────────────
make_artifact() {
  local iter=$1
  local outdir="$WORKDIR/iter-${iter}/input"
  mkdir -p "$outdir"
  local txt="$outdir/doc.txt"
  # Vary the content across iterations to drive score variance
  local category=("shopping" "dating" "food" "events" "travel" "health" "finance" "music")
  local cat="${category[$((iter % ${#category[@]}))]}"
  local conf
  conf=$(python3 -c "import random; random.seed($iter*7+3); print(f'{random.uniform(0.55,0.99):.3f}')")
  cat > "$txt" <<EOF
Bonfyre self-optimization loop v2 — iteration ${iter}
Date: $(date -u +"%Y-%m-%dT%H:%M:%SZ")
Category: ${cat}

This artifact documents a behavioral pattern observed during session ${iter}.
The system monitored user engagement across $(( iter * 3 + 7 )) interaction points.

Pattern signals:
  - Engagement depth: $(( RANDOM % 30 + 5 )) events
  - Confidence: ${conf}
  - Session category: ${cat}
  - Prior sessions in cluster: $((iter * 2))
  - Signal strength: $((50 + iter * 9))%
  - Anomaly detected: $([ $((iter % 3)) -eq 0 ] && echo yes || echo no)

Trend analysis:
  The system identified a $([ $((iter % 2)) -eq 0 ] && echo "rising" || echo "stable") trend
  across the last $((iter + 4)) observation windows. The ${cat} cluster shows
  $([ $((iter % 4)) -lt 2 ] && echo "high" || echo "moderate") correlation with past patterns.
  Recommended action: $([ $((iter % 5)) -eq 0 ] && echo "escalate-P1" || echo "continue-nominal")

This artifact is suitable for embedding, vectorization, quality scoring,
and feedback loop integration.
EOF
  python3 -c "
import json
with open('$outdir/doc.txt') as f: text = f.read()
import hashlib
h = hashlib.sha256(text.encode()).hexdigest()
obj = {
  'id': 'selfloop-v2-iter${iter}',
  'family': 'T_SELFLOOP',
  'stage': 'ingest',
  'hash': h,
  'created': __import__('time').time().__int__(),
  'iteration': ${iter},
  'category': '${cat}',
  'confidence': float('${conf}'),
  'source': 'bonfyre-selfloop-v2',
  'inputs': [{'path': 'doc.txt', 'type': 'text'}]
}
json.dump(obj, open('$outdir/artifact.json', 'w'), indent=2)
" 2>/dev/null
  echo "$outdir"
}

snapshot_learn() {
  $BF learn export 2>/dev/null || echo "{}"
}

# ── Initial setup ────────────────────────────────────────────────────────
banner "bonfyre-selfloop-v2: ${ITERS} iterations"
echo "  workdir: $WORKDIR"
echo "  log:     $LOG"
echo "  promote winner after ${PROMOTE_AFTER} iterations"
echo ""
$BF doctor 2>&1 | grep -E "catalog:|surfaces:|summary:" || true

LEARN_BEFORE=$(snapshot_learn)
echo ""
echo "  initial thresholds:"
echo "$LEARN_BEFORE" | python3 -c "
import sys, json
try:
  d = json.load(sys.stdin)
  thr = d.get('thresholds', d)
  if isinstance(thr, list):
    for t in thr: print(f'    {t[\"stage\"]}: quality={t[\"quality\"]:.3f}  lat={t[\"latency_ms\"]}ms')
  else:
    for k,v in d.items(): print(f'    {k}: {v}')
except Exception as e: print(f'    (empty — {e})')
"

# Economy: seed local model costs so router prefers local backends
$BF economy budget set selfloop-v2 1.00 2>/dev/null || true
$BF economy cost record selfloop-v2 embed local   0.0000001 2>/dev/null || true
$BF economy cost record selfloop-v2 embed hash    0.000001  2>/dev/null || true
$BF economy cost record selfloop-v2 embed onnx    0.000002  2>/dev/null || true
echo "  economy: seeded local/hash/onnx cost records"

# Tier: register SLA for embed + transcribe
$BF tier set selfloop-v2 embed    fast  2>/dev/null || true
$BF tier set selfloop-v2 compress batch 2>/dev/null || true
echo "  tier: embed=fast, compress=batch"

# Compete: register A/B competition (persists across iterations)
COMP_RAW=$($BF compete pair selfloop-v2 embed 2>/dev/null || echo "")
COMP_ID=$(echo "$COMP_RAW" | sed -n 's/.*competition created: //p' | tr -d '[:space:]')
if [ -n "$COMP_ID" ]; then
  $BF compete add-variant "$COMP_ID" "hash"  '{"backend":"hash","dims":384}'   2>/dev/null || true
  $BF compete add-variant "$COMP_ID" "onnx"  '{"backend":"onnx","dims":384}'   2>/dev/null || true
  $BF compete add-variant "$COMP_ID" "local" '{"backend":"local","dims":384}'  2>/dev/null || true
  ok "compete: registered 3-way A/B (hash vs onnx vs local) — comp=$COMP_ID"
else
  warn "compete: could not register A/B competition"
  COMP_ID=""
fi

# Learn: register 'embed' and 'compress' as custom stages so tune works
for stage in embed compress tag ingest; do
  $BF learn feedback "init-${stage}" 0.75 neutral 2>/dev/null || true
done
echo "  learn: seeded initial feedback for embed/compress/tag/ingest stages"

# Time: triggers
$BF time trigger add model_updated  selfloop-v2 2>/dev/null || true
$BF time trigger add quality_drop   selfloop-v2 2>/dev/null || true
$BF time trigger add cost_overrun   selfloop-v2 2>/dev/null || true

# Vec: initialize DB
$BF vec init "$VEC_DB" 2>/dev/null || true
echo ""

LAST_EMBED_FILE=""
WINNER="hash"
PROMOTED=false

# ── Main iteration loop ──────────────────────────────────────────────────
for iter in $(seq 1 $ITERS); do
  banner "ITERATION ${iter} / ${ITERS}"
  ITER_DIR="$WORKDIR/iter-${iter}"
  mkdir -p "$ITER_DIR"
  ARTIFACT_ID="selfloop-v2-iter${iter}"

  # ── 1. Ingest ──────────────────────────────────────────────────────────
  step "1. ingest: generate behavioral artifact (iter=${iter})"
  INPUT_DIR=$(make_artifact $iter)
  ARTIFACT_CONF=$(python3 -c "import json; d=json.load(open('$INPUT_DIR/artifact.json')); print(d['confidence'])" 2>/dev/null || echo "0.75")
  ok "artifact: $INPUT_DIR/artifact.json  [cat=$(python3 -c "import json; print(json.load(open('$INPUT_DIR/artifact.json'))['category'])" 2>/dev/null)  conf=${ARTIFACT_CONF}]"

  # ── 2. Hash ────────────────────────────────────────────────────────────
  step "2. hash: content-address the document"
  DOC_HASH=$($BF hash file "$INPUT_DIR/doc.txt" 2>/dev/null | awk '{print $1}' || echo "?")
  ok "sha256: ${DOC_HASH:0:16}…"

  # ── 3. Compress ────────────────────────────────────────────────────────
  step "3. compress: zstd pack + track savings"
  COMPRESS_START=$(python3 -c "import time; print(int(time.time()*1000))" 2>/dev/null || echo "0")
  $BF compress pack "$INPUT_DIR/doc.txt" "$ITER_DIR/doc.zst" 2>/dev/null \
    && ok "compressed: $ITER_DIR/doc.zst" \
    || warn "compress: skipped (zstd not available)"
  COMPRESS_END=$(python3 -c "import time; print(int(time.time()*1000))" 2>/dev/null || echo "0")
  COMPRESS_MS=$(( COMPRESS_END - COMPRESS_START )); [ "$COMPRESS_MS" -lt 1 ] && COMPRESS_MS=1
  COMPRESS_SAVINGS=$($BF compress savings "$INPUT_DIR" 2>/dev/null | grep -oE "[0-9]+\.[0-9]+%" | head -1 || echo "n/a")
  metric "compress savings" "$COMPRESS_SAVINGS  (${COMPRESS_MS}ms)"
  $BF economy cost record selfloop-v2 compress hash "$( printf '%.6f' "$(echo "$COMPRESS_MS * 0.000001" | bc -l 2>/dev/null || echo '0.000001')" )" 2>/dev/null || true
  $BF tier record selfloop-v2 compress "$COMPRESS_MS" 2>/dev/null || true

  # ── 4. Embed ──────────────────────────────────────────────────────────
  step "4. embed: 384-dim hash embedding"
  EMBED_START=$(python3 -c "import time; print(int(time.time()*1000))")
  $BF embed --text "$INPUT_DIR/doc.txt" --out "$ITER_DIR/embedding.json" \
      --backend hash --dims 384 --output-format json 2>/dev/null \
    || warn "embed: skipped"
  EMBED_END=$(python3 -c "import time; print(int(time.time()*1000))")
  EMBED_MS=$(( EMBED_END - EMBED_START )); [ "$EMBED_MS" -lt 1 ] && EMBED_MS=5
  ok "embedding: $ITER_DIR/embedding.json  (${EMBED_MS}ms)"
  LAST_EMBED_FILE="$ITER_DIR/embedding.json"

  # ── 5. Vec insert ─────────────────────────────────────────────────────
  step "5. vec: insert embedding"
  if [ -f "$ITER_DIR/embedding.json" ]; then
    python3 -c "
import json
d = json.load(open('$ITER_DIR/embedding.json'))
vec = d.get('vector', d.get('embedding', d.get('vec', [0.0]*384)))
if len(vec) < 384: vec = vec + [0.0]*(384-len(vec))
else: vec = vec[:384]
wrapped = {'embeddings': [{'id': '$ARTIFACT_ID', 'source': 'selfloop-v2', 'type': 'text', 'embedding': vec}]}
json.dump(wrapped, open('$ITER_DIR/vec_insert.json', 'w'))
" 2>/dev/null
    INSERT_OUT=$($BF vec insert "$VEC_DB" "$ITER_DIR/vec_insert.json" 2>/dev/null || echo "0 vectors inserted")
    ok "inserted: $ARTIFACT_ID  ($INSERT_OUT)"
    VEC_COUNT=$($BF vec count "$VEC_DB" 2>/dev/null | grep -oE "vectors: [0-9]+" | grep -oE "[0-9]+" || echo "?")
    metric "vec DB size" "$VEC_COUNT vectors total"
  fi

  # ── 6. Vec search (recall) ────────────────────────────────────────────
  step "6. vec search: recall similar past artifacts"
  if [ -f "$ITER_DIR/embedding.json" ] && [ "$iter" -gt 1 ]; then
    SEARCH_OUT=$($BF vec search "$VEC_DB" "$ITER_DIR/embedding.json" --top 3 2>/dev/null || echo "")
    if [ -n "$SEARCH_OUT" ]; then
      TOP_MATCH=$(echo "$SEARCH_OUT" | python3 -c "
import json,sys
try:
  d=json.load(sys.stdin)
  r=d.get('results',[])
  # skip self-match (distance=0)
  for x in r:
    if x['distance']>0.001:
      print(f\"{x['id']} (dist={x['distance']:.4f})\")
      break
except: pass
" 2>/dev/null || echo "none")
      ok "nearest neighbor: $TOP_MATCH"
    else
      ok "vec search: no prior similar artifacts yet"
    fi
  else
    ok "vec search: skipped (first iteration)"
  fi

  # ── 7. Vec compare (consecutive cosine similarity) ───────────────────
  step "7. vec compare: similarity to previous iteration"
  if [ "$iter" -gt 1 ]; then
    PREV_ID="selfloop-v2-iter$((iter-1))"
    COS=$($BF vec compare "$VEC_DB" "$PREV_ID" "$ARTIFACT_ID" 2>/dev/null \
      | python3 -c "import json,sys; d=json.load(sys.stdin); print(f\"{d.get('cosine_similarity',0):.4f}\")" 2>/dev/null || echo "n/a")
    ok "cosine similarity to iter-$((iter-1)): $COS"
  else
    ok "vec compare: skipped (first iteration)"
  fi

  # ── 8. SLI chain ──────────────────────────────────────────────────────
  step "8. sli chain: T04:gdn:T16 pattern inference"
  MODELS_DIR="$HOME/.local/share/bonfyre/models"
  if [ -f "$ITER_DIR/embedding.json" ]; then
    python3 -c "
import json, struct
d = json.load(open('$ITER_DIR/embedding.json'))
vec = d.get('vector', d.get('embedding', d.get('vec', [0.0]*64)))
n, dim = 1, len(vec)
with open('$ITER_DIR/vecs.bin','wb') as f:
    f.write(struct.pack('<II', n, dim))
    for v in vec: f.write(struct.pack('<f', float(v)))
" 2>/dev/null
    $BF sli chain --in "$ITER_DIR/vecs.bin" --chain T04:gdn:T16 \
        --models-dir "$MODELS_DIR" --out "$ITER_DIR/sli_out.bin" 2>/dev/null \
      && ok "sli chain: wrote sli_out.bin" \
      || warn "sli chain: T04/T16 not present — skipping (expected)"
  fi

  # ── 9. Control: HE-SLI score ──────────────────────────────────────────
  step "9. control score: HE-SLI quality"
  SCORE_OUT=$($BF control score "$INPUT_DIR/artifact.json" 2>/dev/null || echo "")
  if [ -n "$SCORE_OUT" ]; then
    echo "$SCORE_OUT" | grep -E "relevance|completeness|coherence|composite" | sed 's/^/    /'
    COMPOSITE=$(echo "$SCORE_OUT" | grep -oE "composite[: ]+[0-9.]+" | grep -oE "[0-9.]+$" | head -1 || echo "0.75")
  else
    COMPOSITE="0.75"
    warn "control score: using default"
  fi

  # ── 10. Control: route decision ────────────────────────────────────────
  step "10. control route: policy engine decision"
  $BF control route selfloop-v2 "$INPUT_DIR/artifact.json" 2>/dev/null | grep "verdict\|policy" | sed 's/^/    /' || warn "control route: skipped"

  # ── 11. Control: entropy pre-flight ────────────────────────────────────
  step "11. entropy gate: Shannon pre-flight"
  $BF control entropy-check "$INPUT_DIR/artifact.json" 2>/dev/null \
    | grep "result\|entropy.*:" | sed 's/^/    /' || warn "entropy: skipped"
  # Trap exit 2 (low entropy) without aborting loop
  true

  # ── 12. Ledger ─────────────────────────────────────────────────────────
  step "12. ledger: assess artifact value"
  $BF ledger assess "$INPUT_DIR/artifact.json" 2>/dev/null | head -6 || warn "ledger: skipped"

  # ── 13. Compete ────────────────────────────────────────────────────────
  step "13. compete: A/B embed tournament"
  if [ -n "$COMP_ID" ]; then
    INPUT_JSON="{\"artifact\":\"$ARTIFACT_ID\",\"iter\":$iter,\"conf\":$ARTIFACT_CONF}"
    $BF compete run "$COMP_ID" "$INPUT_JSON" 2>/dev/null \
      && ok "compete: run recorded" \
      || warn "compete run: skipped"
    SCOREBOARD=$($BF compete score "$COMP_ID" 2>/dev/null || echo "")
    if [ -n "$SCOREBOARD" ]; then
      echo "$SCOREBOARD" | head -5 | sed 's/^/    /'
      # Extract winner by label (col 2)
      WINNER=$( echo "$SCOREBOARD" | awk 'NR>1 && /[0-9]/{print $2; exit}' || echo "hash")
    fi

    # Promote after PROMOTE_AFTER iterations
    if [ "$iter" -eq "$PROMOTE_AFTER" ] && [ "$PROMOTED" = "false" ]; then
      WINNER_VAR=$( echo "$SCOREBOARD" | awk 'NR>1 && /[0-9]/{print $1; exit}' || echo "")
      if [ -n "$WINNER_VAR" ]; then
        $BF compete promote "$WINNER_VAR" 2>/dev/null \
          && ok "compete PROMOTED: $WINNER_VAR → production" \
          || warn "compete promote: skipped"
        PROMOTED=true
      fi
    fi
  else
    WINNER="hash"
    warn "compete: no A/B registered"
  fi

  # ── 14. Learn feedback ─────────────────────────────────────────────────
  step "14. learn: feedback + threshold tuning"
  RUN_ID="selfloop-v2-${iter}"
  LEARN_SCORE=$(python3 -c "print(f'{float($COMPOSITE):.3f}')")
  $BF learn feedback "$RUN_ID" "$LEARN_SCORE" good 2>/dev/null \
    && ok "feedback: run=$RUN_ID  score=$LEARN_SCORE  label=good" \
    || warn "learn feedback: skipped"

  # Tune after enough samples
  for stage in embed ingest; do
    TUNE_OUT=$($BF learn tune "$stage" 2>/dev/null || echo "")
    if echo "$TUNE_OUT" | grep -qE "threshold.*updated|tuned"; then
      ok "learn tune [$stage]: $TUNE_OUT"
    fi
  done

  # ── 15. Economy cost + routing ────────────────────────────────────────
  step "15. economy: cost record + routing"
  COST=$(python3 -c "import random; random.seed($iter); print(f'{random.uniform(0.000001,0.00005):.7f}')")
  $BF economy cost record selfloop-v2 embed "$WINNER" "$COST" 2>/dev/null \
    && ok "cost recorded: \$$COST for $WINNER" \
    || warn "economy cost record: skipped"
  ROUTE_MODEL=$($BF economy route selfloop-v2 2>/dev/null | grep "^recommended" | grep -oE ": \S+" | tr -d ': ' || echo "?")
  metric "economy route" "$ROUTE_MODEL"

  # ── 16. Tier latency ──────────────────────────────────────────────────
  step "16. tier: SLA record"
  $BF tier record selfloop-v2 embed "$EMBED_MS" 2>/dev/null \
    && ok "tier: ${EMBED_MS}ms for embed" \
    || warn "tier record: skipped"

  # ── 17. Queue enqueue ────────────────────────────────────────────────
  step "17. queue: enqueue artifact for async processing"
  JOB_OUT=$($BF queue enqueue embed "{\"file\":\"$INPUT_DIR/doc.txt\",\"artifact_id\":\"$ARTIFACT_ID\"}" \
      --source selfloop-v2 --priority $iter 2>/dev/null || echo "")
  if [ -n "$JOB_OUT" ]; then
    JOB_ID=$(echo "$JOB_OUT" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('id','?'))" 2>/dev/null || echo "?")
    ok "queued: job_id=$JOB_ID  priority=$iter"
  else
    warn "queue enqueue: skipped"
  fi

  # ── 18. Entity resolve ───────────────────────────────────────────────
  step "18. entity: resolve artifact identity"
  $BF entity resolve text "$ARTIFACT_ID" 2>/dev/null | head -3 || warn "entity: skipped"

  # ── 19. Time schedule ───────────────────────────────────────────────
  step "19. time: schedule for re-processing"
  $BF time schedule "$ARTIFACT_ID" selfloop-v2 2>/dev/null \
    && ok "time: $ARTIFACT_ID queued" \
    || warn "time schedule: skipped"

  # ── 20. Space snapshot ──────────────────────────────────────────────
  step "20. space: snapshot iteration state"
  $BF space open "$SPACE_NAME" 2>/dev/null || true
  SUMMARY="{\"iter\":$iter,\"composite\":$COMPOSITE,\"winner\":\"$WINNER\",\"embed_ms\":$EMBED_MS,\"cost\":$COST,\"vec_count\":${VEC_COUNT:-0},\"route\":\"${ROUTE_MODEL:-?}\"}"
  $BF space put "$SPACE_NAME" "iter-${iter}" "$SUMMARY" 2>/dev/null \
    && ok "space: saved iter-${iter}" \
    || warn "space: put skipped"

  # ── 21. Fragment create ─────────────────────────────────────────────
  step "21. fragment: behavioral record"
  PAYLOAD="{\"artifact_id\":\"$ARTIFACT_ID\",\"iter\":$iter,\"score\":$COMPOSITE,\"winner\":\"$WINNER\"}"
  $BF fragment create \
      --store "$FRAG_DB" \
      --kind behavior \
      --persp selfloop-v2 \
      --conf "$COMPOSITE" \
      --payload "$PAYLOAD" 2>/dev/null \
    && ok "fragment: created (conf=$COMPOSITE)" \
    || warn "fragment: skipped"

  ok "iteration ${iter} complete  [composite=${COMPOSITE}  winner=${WINNER}  embed=${EMBED_MS}ms  route=${ROUTE_MODEL:-?}]"
done

# ── Final report ─────────────────────────────────────────────────────────
banner "FINAL REPORT — ${ITERS} iterations complete"

step "learn: threshold evolution"
LEARN_AFTER=$(snapshot_learn)
echo "  BEFORE:"
echo "$LEARN_BEFORE" | python3 -c "
import sys,json
try:
  d=json.load(sys.stdin)
  thr=d.get('thresholds',d)
  if isinstance(thr,list):
    for t in thr: print(f'    {t[\"stage\"]}: {t[\"quality\"]:.3f}')
  else:
    for k,v in d.items(): print(f'    {k}: {v}')
except: print('  (empty)')
" 2>/dev/null
echo "  AFTER:"
echo "$LEARN_AFTER" | python3 -c "
import sys,json
try:
  d=json.load(sys.stdin)
  thr=d.get('thresholds',d)
  if isinstance(thr,list):
    for t in thr: print(f'    {t[\"stage\"]}: {t[\"quality\"]:.3f}')
  else:
    for k,v in d.items(): print(f'    {k}: {v}')
except: print('  (no change)')
" 2>/dev/null

step "learn: stage thresholds"
$BF learn list 2>/dev/null | head -12 || true

step "learn: feedback history"
$BF learn history 2>/dev/null | tail -10 || true

step "economy: routing + spend"
$BF economy route selfloop-v2  2>/dev/null | head -4 || true
$BF economy status             2>/dev/null | head -10 || true

step "economy: cost breakdown"
$BF economy report --last 15 2>/dev/null | head -20 || true

step "compete: final A/B rankings"
if [ -n "${COMP_ID:-}" ]; then
  $BF compete score "$COMP_ID" 2>/dev/null | head -12 || true
fi

step "control: operations dashboard"
$BF control ops 2>/dev/null || true

step "tier: SLA compliance"
$BF tier history selfloop-v2 2>/dev/null | head -12 || true
$BF tier violations selfloop-v2 2>/dev/null | head -10 || true

step "queue: job stats"
$BF queue stats 2>/dev/null || true
$BF queue list --limit 5 2>/dev/null | python3 -c "
import json,sys
try:
  jobs = json.load(sys.stdin)
  for j in jobs[:5]: print(f'    job={j[\"id\"]}  type={j[\"type\"]}  status={j[\"status\"]}')
except: pass
" 2>/dev/null || true

step "entity: identity map"
$BF entity status 2>/dev/null | head -6 || true

step "time: scheduled artifacts"
$BF time status 2>/dev/null | head -6 || true

step "space: iteration summaries"
$BF space list "$SPACE_NAME" 2>/dev/null || true

step "vec: vector store"
$BF vec count "$VEC_DB" 2>/dev/null || true

step "fragment: behavioral records"
$BF fragment stats --store "$FRAG_DB" 2>/dev/null | head -8 || true

step "self-similarity: cosine distance matrix"
echo "  Comparing all consecutive pairs:"
for i in $(seq 1 $((ITERS-1))); do
  ID1="selfloop-v2-iter${i}"
  ID2="selfloop-v2-iter$((i+1))"
  SIM=$($BF vec compare "$VEC_DB" "$ID1" "$ID2" 2>/dev/null \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print(f\"{d.get('cosine_similarity',0):.4f}\")" 2>/dev/null || echo "n/a")
  printf "    iter-%-2d → iter-%-2d  cosine=%-8s\n" "$i" "$((i+1))" "$SIM"
done

banner "bonfyre-selfloop-v2 complete — log: $LOG"
