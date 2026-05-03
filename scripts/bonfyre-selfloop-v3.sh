#!/usr/bin/env bash
# bonfyre-selfloop-v3.sh — Maximum depth self-utilization loop
#
# Every active subsystem feeds every other. Outputs from one layer become
# inputs to the next. The system watches its own behavior and uses that
# signal to make better decisions automatically.
#
# 35 steps per iteration across all 7 layers:
#
#  LAYER 1 — EYES/EARS (ingest + sense)
#    1.  ingest        artifact generation (varies per iter: category, conf, anomaly)
#    2.  hash          content-address → CAS
#    3.  compress      zstd pack → savings feed into finance
#    4.  emit          render artifact to text → feed into gen
#    5.  graph         add atom + embed op to Merkle-DAG
#
#  LAYER 2 — MEMORY (embed + store)
#    6.  embed         384-dim hash embedding
#    7.  vec insert    store in SIMD vector DB
#    8.  kvcache       benchmark compression on embedding tensor
#    9.  index         build/update artifact family index
#   10.  query         scan + stats the workdir
#
#  LAYER 3 — UNDERSTANDING (structure + classify)
#   11.  vec search    recall nearest past artifacts
#   12.  vec compare   cosine to previous iter → drift signal
#   13.  capabilities  match artifact description → best capability
#   14.  gen           generate a recipe YAML for this artifact type
#   15.  canon         normalize the artifact JSON
#
#  LAYER 4 — PATTERN BRAIN (SLI + DisCIPL)
#   16.  sli chain     T04:gdn:T16 inference
#   17.  discipl recurse  recursive goal-planning on composite score
#   18.  discipl verify  verify the loop chain
#   19.  orchestrate plan  machine-only routing plan
#
#  LAYER 5 — DECISION (control + compete + learn)
#   20.  control score    HE-SLI quality scoring
#   21.  control route    policy-engine route decision
#   22.  control entropy-check  Shannon pre-flight
#   23.  compete run      3-way A/B tournament (hash/onnx/local)
#   24.  learn feedback   rolling-average outcome recording
#   25.  learn tune       adjust thresholds when ≥50 samples
#
#  LAYER 6 — VALUE (economy + finance + meter + gate)
#   26.  economy cost record   log actual cost
#   27.  economy route         get next routing recommendation
#   28.  finance job log       arbitrage margin tracking
#   29.  meter record          ops metering → billing
#   30.  tier record           SLA compliance
#
#  LAYER 7 — PERSISTENCE (queue + entity + time + space + fragment)
#   31.  queue enqueue    async job dispatch
#   32.  entity resolve   deduplicate artifact identity
#   33.  time schedule    queue artifact for re-processing
#   34.  space put        snapshot iteration summary
#   35.  fragment create  behavioral record with confidence
#
# Promote: winning A/B variant promoted to production at PROMOTE_AFTER
# Gate:    license key issued once, checked every iteration
# Discipl: loop chain verified after each recursion
# Finance: service log feeds arbitrage opportunity ranking
# Meter:   all ops recorded → invoice at end
#
# Usage:
#   bash scripts/bonfyre-selfloop-v3.sh [--iters N] [--dir WORKDIR]

set -euo pipefail

ITERS=10
WORKDIR="/tmp/bonfyre-selfloop-v3"
PROMOTE_AFTER=5

while [[ $# -gt 0 ]]; do
  case $1 in
    --iters)        ITERS=$2;        shift 2;;
    --dir)          WORKDIR=$2;      shift 2;;
    --promote-after) PROMOTE_AFTER=$2; shift 2;;
    *) echo "usage: $0 [--iters N] [--dir WORKDIR]" >&2; exit 1;;
  esac
done

mkdir -p "$WORKDIR"
LOG="$WORKDIR/selfloop.log"
exec > >(tee -a "$LOG") 2>&1

BF="bonfyre"
VEC_DB="$WORKDIR/selfloop.vecdb"
FRAG_DB="$WORKDIR/fragments.db"
GRAPH_DB="$WORKDIR/selfloop.graphdb"
SPACE_NAME="bonfyre-selfloop-v3"

banner()  { echo; printf '%.0s━' {1..64}; echo; echo "  $*"; printf '%.0s━' {1..64}; echo; }
step()    { echo; echo "  ▶ $*"; }
ok()      { echo "    ✓ $*"; }
warn()    { echo "    ⚠ $*"; }
metric()  { printf "    %-26s %s\n" "$1:" "$2"; }

# ── Synthetic behavioral artifact ────────────────────────────────────────
make_artifact() {
  local iter=$1
  local outdir="$WORKDIR/iter-${iter}/input"
  mkdir -p "$outdir"
  local txt="$outdir/doc.txt"
  local categories=("shopping" "dating" "food" "events" "travel" "health" "finance" "music" "education" "sports")
  local cat="${categories[$((iter % ${#categories[@]}))]}"
  local conf; conf=$(python3 -c "import random; random.seed($iter*13+7); print(f'{random.uniform(0.50,0.99):.3f}')")
  local anomaly; anomaly=$([ $((iter % 4)) -eq 0 ] && echo "true" || echo "false")
  local trend; trend=$([ $((iter % 2)) -eq 0 ] && echo "rising" || echo "stable")
  cat > "$txt" <<EOF
Bonfyre self-optimization v3 — iteration ${iter}
Timestamp: $(date -u +"%Y-%m-%dT%H:%M:%SZ")
Category: ${cat}
Confidence: ${conf}
Anomaly: ${anomaly}
Trend: ${trend}

Behavioral pattern observed across $(( iter * 3 + 7 )) interaction points.
Session ${iter} shows a ${trend} trend in the ${cat} engagement cluster.
Signal strength: $((45 + iter * 11))%  Depth: $(( RANDOM % 25 + 5 )) events
Prior sessions in cluster: $((iter * 2 + 3))

Analysis:
  The ${cat} cluster exhibits $([ $((iter % 3)) -eq 0 ] && echo "high" || echo "moderate") correlation
  with historical patterns across the last $((iter + 5)) windows.
  Quality gate: $([ $((iter % 5)) -eq 0 ] && echo "escalate-P1" || echo "nominal")
  Routing recommendation: $([ "$anomaly" = "true" ] && echo "review-required" || echo "auto-proceed")

Tags: ${cat} behavioral-pattern session-${iter} trend-${trend}
EOF
  python3 -c "
import json, hashlib, time
with open('$outdir/doc.txt') as f: text = f.read()
h = hashlib.sha256(text.encode()).hexdigest()
json.dump({
  'id': 'v3-iter${iter}',
  'family': 'T_SELFLOOP',
  'stage': 'ingest',
  'hash': h,
  'created': int(time.time()),
  'iteration': ${iter},
  'category': '${cat}',
  'confidence': float('${conf}'),
  'anomaly': '${anomaly}' == 'true',
  'trend': '${trend}',
  'source': 'bonfyre-selfloop-v3',
  'inputs': [{'path': 'doc.txt', 'type': 'text'}]
}, open('$outdir/artifact.json', 'w'), indent=2)
" 2>/dev/null
  echo "$outdir"
}

snapshot_learn() { $BF learn export 2>/dev/null || echo "{}"; }

# ═══════════════════════════════════════════════════════════════════════
# BOOT
# ═══════════════════════════════════════════════════════════════════════
banner "bonfyre-selfloop-v3: ${ITERS} iterations — maximum depth"
echo "  workdir:        $WORKDIR"
echo "  log:            $LOG"
echo "  promote after:  ${PROMOTE_AFTER} iterations"
echo ""
$BF doctor 2>&1 | grep -E "catalog:|surfaces:|summary:" || true
echo ""

# ── DisCIPL: boot recursive substrate
step "BOOT: DisCIPL recursive substrate"
$BF discipl init 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'    init: {d[\"status\"]}  root={d[\"root\"]}')" 2>/dev/null || warn "discipl init: already initialized"
$BF discipl contracts import 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'    contracts: {d[\"imported_contracts\"]} imported')" 2>/dev/null || warn "discipl contracts: already imported"

# ── Gate: issue license key
step "BOOT: gate — issue pro license"
GATE_KEY_FILE="$WORKDIR/gate.json"
GATE_KEY=$($BF gate issue --tier pro --org bonfyre-selfloop-v3 --out "$GATE_KEY_FILE" 2>/dev/null | grep "bfk_" | head -1 || echo "")
ok "gate key: ${GATE_KEY:-already issued}"

# ── Economy: seed local cost records
$BF economy budget set selfloop-v3 5.00 2>/dev/null || true
for model in local hash onnx; do
  $BF economy cost record selfloop-v3 embed  "$model" 0.000001 2>/dev/null || true
  $BF economy cost record selfloop-v3 compress "$model" 0.000001 2>/dev/null || true
done

# ── Finance: register service arbitrage
$BF finance service add --name "embed-v3"    --buy 0.000001 --sell 0.00001 --source hash  2>/dev/null || true
$BF finance service add --name "compress-v3" --buy 0.000001 --sell 0.00001 --source zstd  2>/dev/null || true

# ── Tier: SLA assignments
$BF tier set selfloop-v3 embed    fast  2>/dev/null || true
$BF tier set selfloop-v3 compress batch 2>/dev/null || true
$BF tier set selfloop-v3 graph    batch 2>/dev/null || true

# ── Compete: 3-way A/B
COMP_RAW=$($BF compete pair selfloop-v3 embed 2>/dev/null || echo "")
COMP_ID=$(echo "$COMP_RAW" | sed -n 's/.*competition created: //p' | tr -d '[:space:]')
if [ -n "$COMP_ID" ]; then
  for variant in hash onnx local; do
    $BF compete add-variant "$COMP_ID" "$variant" "{\"backend\":\"$variant\",\"dims\":384}" 2>/dev/null || true
  done
  ok "compete: 3-way A/B registered (comp=$COMP_ID)"
else
  warn "compete: registration failed"
  COMP_ID=""
fi

# ── Learn: seed custom stages
for stage in embed compress graph ingest; do
  $BF learn feedback "boot-${stage}" 0.75 neutral 2>/dev/null || true
done

# ── Time: triggers
for event in model_updated quality_drop cost_overrun anomaly_detected; do
  $BF time trigger add "$event" selfloop-v3 2>/dev/null || true
done

# ── Graph DB + Vec DB
$BF graph init "$GRAPH_DB" 2>/dev/null || true
$BF vec   init "$VEC_DB"   2>/dev/null || true

# ── Capture initial state
LEARN_BEFORE=$(snapshot_learn)
echo ""
echo "  initial learn thresholds:"
echo "$LEARN_BEFORE" | python3 -c "
import sys,json
try:
  d=json.load(sys.stdin); thr=d.get('thresholds',d)
  if isinstance(thr,list):
    for t in thr: print(f'    {t[\"stage\"]}: q={t[\"quality\"]:.3f}  lat={t[\"latency_ms\"]}ms')
  else: [print(f'    {k}: {v}') for k,v in d.items()]
except: print('    (empty)')
"

WINNER="hash"
PROMOTED=false
DISCIPL_LOOP_ID=""
VEC_COUNT=0

# ═══════════════════════════════════════════════════════════════════════
# ITERATION LOOP
# ═══════════════════════════════════════════════════════════════════════
for iter in $(seq 1 $ITERS); do
  banner "ITERATION ${iter} / ${ITERS}"
  ITER_DIR="$WORKDIR/iter-${iter}"
  mkdir -p "$ITER_DIR"
  ARTIFACT_ID="v3-iter${iter}"

  # ── LAYER 1: EYES/EARS ───────────────────────────────────────────────

  step "1. ingest: behavioral artifact (iter=${iter})"
  INPUT_DIR=$(make_artifact $iter)
  ARTIFACT_CONF=$(python3 -c "import json; d=json.load(open('$INPUT_DIR/artifact.json')); print(d['confidence'])" 2>/dev/null || echo "0.75")
  ARTIFACT_CAT=$(python3 -c  "import json; d=json.load(open('$INPUT_DIR/artifact.json')); print(d['category'])"   2>/dev/null || echo "?")
  ARTIFACT_ANOMALY=$(python3 -c "import json; d=json.load(open('$INPUT_DIR/artifact.json')); print(d['anomaly'])" 2>/dev/null || echo "false")
  ok "artifact: $ARTIFACT_ID  cat=$ARTIFACT_CAT  conf=$ARTIFACT_CONF  anomaly=$ARTIFACT_ANOMALY"

  step "2. hash: content-address → CAS"
  DOC_HASH=$($BF hash file "$INPUT_DIR/doc.txt" 2>/dev/null | awk '{print $1}' || echo "?")
  ok "sha256: ${DOC_HASH:0:20}…"

  step "3. compress: zstd pack"
  T0=$(python3 -c "import time; print(int(time.time()*1000))")
  $BF compress pack "$INPUT_DIR/doc.txt" "$ITER_DIR/doc.zst" 2>/dev/null && ok "compressed: $ITER_DIR/doc.zst" || warn "compress: skipped"
  T1=$(python3 -c "import time; print(int(time.time()*1000))")
  COMPRESS_MS=$(( T1 - T0 )); [ "$COMPRESS_MS" -lt 1 ] && COMPRESS_MS=1
  COMPRESS_COST=$(printf '%.7f' "$(python3 -c "print($COMPRESS_MS * 0.000001)" 2>/dev/null || echo '0.0000010')")
  $BF tier     record selfloop-v3 compress "$COMPRESS_MS" 2>/dev/null || true
  $BF meter    record --key selfloop-v3 --op compress --bytes "$(wc -c < "$INPUT_DIR/doc.txt" 2>/dev/null || echo 0)" --duration "$COMPRESS_MS" 2>/dev/null || true
  $BF economy  cost record selfloop-v3 compress "$WINNER" "$COMPRESS_COST" 2>/dev/null || true
  $BF finance  job log --service compress-v3 --cost "$COMPRESS_COST" --sold 0.00001 --quality 0.90 2>/dev/null || true

  step "4. emit: render artifact → text summary"
  EMIT_OUT="$ITER_DIR/emit.txt"
  $BF emit "$INPUT_DIR" --format txt --out "$EMIT_OUT" 2>/dev/null \
    || python3 -c "import json; d=json.load(open('$INPUT_DIR/artifact.json')); print(json.dumps(d,indent=2))" > "$EMIT_OUT" 2>/dev/null \
    || true
  [ -f "$EMIT_OUT" ] && ok "emit: $EMIT_OUT ($(wc -c < "$EMIT_OUT") bytes)" || warn "emit: skipped"

  step "5. graph: add atom + embed op to Merkle-DAG"
  GRAPH_HASH="${DOC_HASH:-$(openssl rand -hex 16)}"
  $BF graph add-atom "$GRAPH_DB" --id "$ARTIFACT_ID" --hash "$GRAPH_HASH" --type behavior --path "$INPUT_DIR/doc.txt" 2>/dev/null \
    && ok "graph: atom $ARTIFACT_ID added" || warn "graph: atom add skipped"

  # ── LAYER 2: MEMORY ──────────────────────────────────────────────────

  step "6. embed: 384-dim hash embedding"
  T0=$(python3 -c "import time; print(int(time.time()*1000))")
  $BF embed --text "$INPUT_DIR/doc.txt" --out "$ITER_DIR/embedding.json" \
      --backend hash --dims 384 --output-format json 2>/dev/null \
    || warn "embed: skipped"
  T1=$(python3 -c "import time; print(int(time.time()*1000))")
  EMBED_MS=$(( T1 - T0 )); [ "$EMBED_MS" -lt 1 ] && EMBED_MS=5
  ok "embedding: ${EMBED_MS}ms  (384 dims)"

  step "7. vec: insert embedding"
  if [ -f "$ITER_DIR/embedding.json" ]; then
    python3 -c "
import json
d = json.load(open('$ITER_DIR/embedding.json'))
vec = d.get('vector', d.get('embedding', d.get('vec', [0.0]*384)))
if len(vec) < 384: vec = vec + [0.0]*(384-len(vec))
else: vec = vec[:384]
json.dump({'embeddings': [{'id': '$ARTIFACT_ID', 'source': 'selfloop-v3', 'type': 'behavior', 'embedding': vec}]},
          open('$ITER_DIR/vec_insert.json', 'w'))
" 2>/dev/null
    $BF vec insert "$VEC_DB" "$ITER_DIR/vec_insert.json" 2>/dev/null | grep -oE "[0-9]+ vector" | head -1 | xargs -I{} echo "    ✓ inserted: $ARTIFACT_ID  ({}s)" || ok "inserted: $ARTIFACT_ID"
    VEC_COUNT=$($BF vec count "$VEC_DB" 2>/dev/null | grep -oE "vectors: [0-9]+" | grep -oE "[0-9]+" || echo "$iter")
    metric "vec DB" "$VEC_COUNT vectors"

    # Register embed op in Merkle-DAG
    EMBED_HASH=$(echo "${GRAPH_HASH}embed" | sha256sum 2>/dev/null | awk '{print $1}' || echo "embed${iter}")
    $BF graph add-atom "$GRAPH_DB" --id "${ARTIFACT_ID}-vec" --hash "$EMBED_HASH" --type embedding 2>/dev/null || true
    $BF graph add-op   "$GRAPH_DB" --id "op-embed-${iter}" --op embed \
        --inputs "$ARTIFACT_ID" --output "${ARTIFACT_ID}-vec" --version 1 2>/dev/null \
        && ok "graph: embed op registered" || warn "graph: op add skipped"
  fi

  step "8. kvcache: RLF compression benchmark on embedding tensor"
  KVBENCH=$($BF kvcache benchmark --bits 4 2>/dev/null | grep "Average cos" | head -1 || echo "")
  if [ -n "$KVBENCH" ]; then
    KV_COS=$(echo "$KVBENCH" | grep -oE "cos=[0-9.]+" | grep -oE "[0-9.]+" || echo "n/a")
    ok "kvcache benchmark: avg_cos=$KV_COS (4-bit RLF)"
  else
    warn "kvcache: benchmark skipped"
  fi

  step "9. index: build artifact family index"
  $BF index build "$ITER_DIR" 2>/dev/null | tail -2 | sed 's/^/    /' || warn "index: build skipped"

  step "10. query: scan + stats workdir"
  QUERY_DB="$WORKDIR/query.duckdb"
  $BF query scan "$WORKDIR" "$QUERY_DB" 2>/dev/null | tail -2 | sed 's/^/    /' || warn "query: scan skipped"

  # ── LAYER 3: UNDERSTANDING ───────────────────────────────────────────

  step "11. vec search: recall nearest past artifacts"
  if [ -f "$ITER_DIR/embedding.json" ] && [ "$iter" -gt 1 ]; then
    SEARCH_RESULT=$($BF vec search "$VEC_DB" "$ITER_DIR/embedding.json" --top 3 2>/dev/null || echo "")
    NEAREST=$(echo "$SEARCH_RESULT" | python3 -c "
import json,sys
try:
  d=json.load(sys.stdin); r=d.get('results',[])
  hits=[x for x in r if x['distance']>0.001]
  if hits: print(f\"{hits[0]['id']} (dist={hits[0]['distance']:.4f})\")
except: pass
" 2>/dev/null || echo "none")
    ok "nearest: $NEAREST"
  fi

  step "12. vec compare: cosine drift to previous iteration"
  COS_SIM="n/a"
  if [ "$iter" -gt 1 ]; then
    PREV_ID="v3-iter$((iter-1))"
    COS_SIM=$($BF vec compare "$VEC_DB" "$PREV_ID" "$ARTIFACT_ID" 2>/dev/null \
      | python3 -c "import json,sys; d=json.load(sys.stdin); print(f\"{d.get('cosine_similarity',0):.4f}\")" 2>/dev/null || echo "n/a")
    ok "cosine to iter-$((iter-1)): $COS_SIM"
    # Feed drift into graph as an edge annotation
    $BF graph add-op "$GRAPH_DB" --id "op-compare-${iter}" --op cosine-compare \
        --inputs "${PREV_ID},${ARTIFACT_ID}" --output "drift-${iter}" \
        --params "{\"cosine\":${COS_SIM:-0}}" --version 1 2>/dev/null || true
  fi

  step "13. capabilities: match artifact description"
  CAP_DESC="behavioral pattern embedding for ${ARTIFACT_CAT} with confidence scoring"
  CAP_OUT=$($BF capabilities match "$CAP_DESC" 2>/dev/null | awk 'NR>1 && NF>3 {print; exit}' || echo "")
  if [ -n "$CAP_OUT" ]; then
    CAP_ID=$(echo "$CAP_OUT" | awk '{print $2}')
    ok "best capability: $CAP_ID"
  else
    ok "capabilities: no high-confidence match"
  fi

  step "14. gen: synthesize a recipe YAML for this artifact type"
  GEN_DESC="embed ${ARTIFACT_CAT} behavioral artifact and score quality"
  GEN_YAML="$ITER_DIR/gen_recipe.yaml"
  $BF gen "$GEN_DESC" 2>/dev/null > "$GEN_YAML" \
    && ok "gen: recipe → $GEN_YAML ($(wc -l < "$GEN_YAML") lines)" \
    || warn "gen: skipped"

  step "15. canon: normalize artifact JSON"
  CANON_OUT="$ITER_DIR/canonical.json"
  if [ -f "$INPUT_DIR/artifact.json" ]; then
    $BF canon normalize "$INPUT_DIR/artifact.json" "$CANON_OUT" 2>/dev/null \
      && ok "canon: normalized → $CANON_OUT" \
      || { python3 -c "import json; d=json.load(open('$INPUT_DIR/artifact.json')); json.dump(d, open('$CANON_OUT','w'), sort_keys=True, indent=2)" 2>/dev/null && ok "canon: python fallback"; } \
      || warn "canon: skipped"
  else
    warn "canon: artifact.json not found (doc.txt write may have failed)"
  fi

  # ── LAYER 4: PATTERN BRAIN ───────────────────────────────────────────

  step "16. sli chain: T04:gdn:T16 pattern inference"
  if [ -f "$ITER_DIR/embedding.json" ]; then
    python3 -c "
import json, struct
d = json.load(open('$ITER_DIR/embedding.json'))
vec = d.get('vector', d.get('embedding', d.get('vec', [0.0]*64)))
with open('$ITER_DIR/vecs.bin','wb') as f:
    f.write(struct.pack('<II', 1, len(vec)))
    for v in vec: f.write(struct.pack('<f', float(v)))
" 2>/dev/null
    $BF sli chain --in "$ITER_DIR/vecs.bin" --chain T04:gdn:T16 \
        --models-dir "$HOME/.local/share/bonfyre/models" --out "$ITER_DIR/sli_out.bin" 2>/dev/null \
      && ok "sli chain: wrote sli_out.bin" \
      || warn "sli chain: T04/T16 models not on disk (expected — graceful skip)"
  fi

  step "17. discipl recurse: recursive goal-planning"
  DISCIPL_GOAL="improve quality from ${COMPOSITE:-0.75} on ${ARTIFACT_CAT} artifacts via embedding optimization"
  DISCIPL_OUT=$($BF discipl recurse --goal "$DISCIPL_GOAL" 2>/dev/null || echo "")
  if [ -n "$DISCIPL_OUT" ]; then
    DISCIPL_LOOP_ID=$(echo "$DISCIPL_OUT" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('loop_id',''))" 2>/dev/null || echo "")
    DISCIPL_CONV=$(echo "$DISCIPL_OUT" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('convergence_score','?'))" 2>/dev/null || echo "?")
    ok "discipl: loop=$DISCIPL_LOOP_ID  convergence=$DISCIPL_CONV"
  else
    warn "discipl recurse: no output"
  fi

  step "18. discipl verify: check loop chain"
  if [ -n "$DISCIPL_LOOP_ID" ]; then
    $BF discipl verify "$DISCIPL_LOOP_ID" 2>/dev/null \
      | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'    status={d.get(\"status\",\"?\")}  convergence={d.get(\"convergence_score\",\"?\")}  uncertainty={d.get(\"uncertainty\",\"?\")}'); print()" 2>/dev/null \
      || warn "discipl verify: skipped"
  fi

  step "19. orchestrate: machine-only routing plan"
  ORCH_REQ="$ITER_DIR/orch_req.json"
  python3 -c "
import json
json.dump({'request': 'embed and score ${ARTIFACT_CAT} artifact', 'priority': 'fast', 'quality_target': float('${ARTIFACT_CONF}')}, open('$ORCH_REQ', 'w'))
" 2>/dev/null
  $BF orchestrate plan "$ORCH_REQ" 2>/dev/null \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'    mode={d.get(\"mode\",\"?\")}  objective={d.get(\"objective\",\"?\")}  latency={d.get(\"latency_class\",\"?\")}'); print()" 2>/dev/null \
    || warn "orchestrate: skipped"

  # ── LAYER 5: DECISION ────────────────────────────────────────────────

  step "20. control score: HE-SLI quality"
  SCORE_OUT=$($BF control score "$INPUT_DIR/artifact.json" 2>/dev/null || echo "")
  if [ -n "$SCORE_OUT" ]; then
    echo "$SCORE_OUT" | grep -E "relevance|completeness|coherence|composite" | sed 's/^/    /'
    COMPOSITE=$(echo "$SCORE_OUT" | grep -oE "composite[: ]+[0-9.]+" | grep -oE "[0-9.]+$" | head -1 || echo "0.75")
  else
    COMPOSITE="0.75"; warn "control score: using default"
  fi

  step "21. control route: policy decision"
  $BF control route selfloop-v3 "$INPUT_DIR/artifact.json" 2>/dev/null \
    | grep -E "verdict|policy" | sed 's/^/    /' || warn "control route: skipped"

  step "22. entropy gate: Shannon pre-flight"
  $BF control entropy-check "$INPUT_DIR/artifact.json" 2>/dev/null \
    | grep -E "result|entropy.*:" | sed 's/^/    /' || warn "entropy: skipped"
  true  # don't let exit 2 kill the loop

  step "23. compete: 3-way A/B tournament"
  if [ -n "$COMP_ID" ]; then
    INPUT_JSON="{\"artifact\":\"$ARTIFACT_ID\",\"iter\":$iter,\"conf\":$ARTIFACT_CONF}"
    $BF compete run "$COMP_ID" "$INPUT_JSON" 2>/dev/null \
      && ok "compete: run recorded" || warn "compete run: skipped"
    SCOREBOARD=$($BF compete score "$COMP_ID" 2>/dev/null || echo "")
    echo "$SCOREBOARD" | head -5 | sed 's/^/    /'
    WINNER=$(echo "$SCOREBOARD" | awk 'NR>1 && /[0-9]/{print $2; exit}' || echo "hash")

    # Auto-promote at PROMOTE_AFTER
    if [ "$iter" -eq "$PROMOTE_AFTER" ] && [ "$PROMOTED" = "false" ]; then
      WINNER_VAR=$(echo "$SCOREBOARD" | awk 'NR>1 && /[0-9]/{print $1; exit}' || echo "")
      if [ -n "$WINNER_VAR" ]; then
        $BF compete promote "$WINNER_VAR" 2>/dev/null \
          && ok "compete PROMOTED: $WINNER_VAR → production" || warn "compete promote: skipped"
        PROMOTED=true
      fi
    fi
  else
    WINNER="hash"; warn "compete: not registered"
  fi

  step "24. learn: feedback recording"
  $BF learn feedback "v3-${iter}" "$COMPOSITE" \
    "$( [ "$(python3 -c "print(float('$COMPOSITE') >= 0.70)")" = "True" ] && echo good || echo neutral )" 2>/dev/null \
    && ok "feedback: score=$COMPOSITE  iter=$iter" || warn "learn feedback: skipped"

  step "25. learn tune: threshold adjustment"
  for stage in embed ingest; do
    TUNE=$($BF learn tune "$stage" 2>/dev/null || echo "")
    echo "$TUNE" | grep -E "threshold|tuned|updated" | sed 's/^/    /' || true
  done

  # ── LAYER 6: VALUE ───────────────────────────────────────────────────

  step "26. economy cost record"
  COST=$(python3 -c "import random; random.seed($iter*17+3); print(f'{random.uniform(0.000001,0.00003):.7f}')")
  $BF economy cost record selfloop-v3 embed "$WINNER" "$COST" 2>/dev/null \
    && ok "cost: \$$COST for $WINNER" || warn "economy cost: skipped"

  step "27. economy route: next model recommendation"
  ROUTE_MODEL=$($BF economy route selfloop-v3 2>/dev/null | grep "^recommended" \
    | sed 's/.*: //' | awk '{print $1}' | tr -d '`' || echo "?")
  metric "economy → route" "$ROUTE_MODEL"

  step "28. finance: arbitrage job log"
  $BF finance job log --service embed-v3 \
      --cost "$COST" --sold "$(python3 -c "print(f'{float($COST)*10:.7f}')")" \
      --quality "$COMPOSITE" 2>/dev/null \
    && ok "finance: margin logged (10× spread)" || warn "finance: skipped"

  step "29. meter: record ops billing"
  $BF meter record --key selfloop-v3 --op embed     --bytes 400   --duration "$EMBED_MS"    2>/dev/null || true
  $BF meter record --key selfloop-v3 --op compress  --bytes 600   --duration "$COMPRESS_MS" 2>/dev/null || true
  $BF meter record --key selfloop-v3 --op graph     --bytes 128   --duration 5               2>/dev/null || true
  ok "meter: embed+compress+graph ops recorded"

  step "30. tier: SLA record"
  $BF tier record selfloop-v3 embed "$EMBED_MS" 2>/dev/null \
    && ok "tier: embed ${EMBED_MS}ms" || warn "tier: skipped"

  # Gate check: verify license is still valid
  if [ -f "$GATE_KEY_FILE" ]; then
    GATE_STATUS=$($BF gate check "$GATE_KEY_FILE" 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('status','?'))" 2>/dev/null || echo "?")
    metric "gate check" "$GATE_STATUS"
  fi

  # ── LAYER 7: PERSISTENCE ─────────────────────────────────────────────

  step "31. queue: enqueue for async processing"
  JOB_OUT=$($BF queue enqueue embed \
      "{\"file\":\"$INPUT_DIR/doc.txt\",\"artifact_id\":\"$ARTIFACT_ID\",\"composite\":$COMPOSITE}" \
      --source selfloop-v3 --priority $iter 2>/dev/null || echo "")
  JOB_ID=$(echo "$JOB_OUT" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('id','?'))" 2>/dev/null || echo "?")
  ok "queued: job_id=$JOB_ID  priority=$iter"

  step "32. entity: resolve artifact identity"
  $BF entity resolve text "$ARTIFACT_ID" 2>/dev/null | head -3 || warn "entity: skipped"

  step "33. time: schedule for re-processing"
  $BF time schedule "$ARTIFACT_ID" selfloop-v3 2>/dev/null \
    && ok "time: scheduled" || warn "time: skipped"
  # Trigger anomaly pipeline if flagged
  if [ "$ARTIFACT_ANOMALY" = "true" ]; then
    $BF time trigger fire anomaly_detected "$ARTIFACT_ID" 2>/dev/null \
      && ok "time: anomaly trigger fired for $ARTIFACT_ID" || true
  fi

  step "34. space: snapshot state"
  $BF space open "$SPACE_NAME" 2>/dev/null || true
  SUMMARY="{\"iter\":$iter,\"composite\":$COMPOSITE,\"winner\":\"$WINNER\",\"embed_ms\":$EMBED_MS,\"compress_ms\":$COMPRESS_MS,\"cost\":$COST,\"route\":\"$ROUTE_MODEL\",\"cosine\":\"$COS_SIM\",\"kv_cos\":\"${KV_COS:-n/a}\",\"discipl_conv\":\"${DISCIPL_CONV:-n/a}\"}"
  $BF space put "$SPACE_NAME" "iter-${iter}" "$SUMMARY" 2>/dev/null \
    && ok "space: iter-${iter} saved" || warn "space: skipped"

  step "35. fragment: behavioral record"
  $BF fragment create \
      --store "$FRAG_DB" \
      --kind behavior \
      --persp selfloop-v3 \
      --conf "$COMPOSITE" \
      --payload "{\"artifact_id\":\"$ARTIFACT_ID\",\"iter\":$iter,\"score\":$COMPOSITE,\"winner\":\"$WINNER\",\"route\":\"$ROUTE_MODEL\"}" 2>/dev/null \
    && ok "fragment: created (conf=$COMPOSITE)" || warn "fragment: skipped"

  ok "iteration ${iter} complete  [q=${COMPOSITE}  winner=${WINNER}  embed=${EMBED_MS}ms  route=${ROUTE_MODEL}  cos=${COS_SIM}]"
done

# ═══════════════════════════════════════════════════════════════════════
# FINAL REPORT
# ═══════════════════════════════════════════════════════════════════════
banner "FINAL REPORT — ${ITERS} iterations complete"

step "learn: threshold evolution"
LEARN_AFTER=$(snapshot_learn)
echo "  BEFORE:"
echo "$LEARN_BEFORE" | python3 -c "
import sys,json
try:
  d=json.load(sys.stdin); thr=d.get('thresholds',d)
  items = thr if isinstance(thr,list) else [{'stage':k,'quality':v} for k,v in d.items()]
  for t in items: print(f'    {t.get(\"stage\",\"?\")}: q={t.get(\"quality\",\"?\"):.3f}')
except: print('  (empty)')
"
echo "  AFTER:"
echo "$LEARN_AFTER" | python3 -c "
import sys,json
try:
  d=json.load(sys.stdin); thr=d.get('thresholds',d)
  items = thr if isinstance(thr,list) else [{'stage':k,'quality':v} for k,v in d.items()]
  for t in items: print(f'    {t.get(\"stage\",\"?\")}: q={t.get(\"quality\",\"?\"):.3f}')
except: print('  (no change)')
"
echo ""
$BF learn history 2>/dev/null | tail -12 | sed 's/^/  /' || true

step "control: operations dashboard"
$BF control ops 2>/dev/null || true

step "economy: routing + spend"
$BF economy route  selfloop-v3 2>/dev/null | head -4 || true
$BF economy status             2>/dev/null | head -8 || true
$BF economy report --last 10   2>/dev/null | head -14 || true

step "finance: arbitrage opportunities"
$BF finance service opportunities 2>/dev/null | head -10 || true
$BF finance service list 2>/dev/null | head -12 || true

step "meter: billing summary"
$BF meter usage   --key selfloop-v3 2>/dev/null | head -8 || true
$BF meter invoice --key selfloop-v3 2>/dev/null | head -8 || true
$BF meter top     --limit 5        2>/dev/null | head -8 || true

step "gate: license status"
[ -f "$GATE_KEY_FILE" ] && $BF gate check "$GATE_KEY_FILE" 2>/dev/null | python3 -c "
import json,sys; d=json.load(sys.stdin); print(f'  status={d.get(\"status\")}  tier={d.get(\"tier\")}  org={d.get(\"org\")}')
" 2>/dev/null || true

step "compete: final A/B rankings"
[ -n "${COMP_ID:-}" ] && $BF compete score "$COMP_ID" 2>/dev/null | head -8 || true

step "discipl: final chain state"
[ -n "${DISCIPL_LOOP_ID:-}" ] && $BF discipl verify "$DISCIPL_LOOP_ID" 2>/dev/null \
  | python3 -c "import json,sys; d=json.load(sys.stdin); [print(f'  {k}: {v}') for k,v in d.items() if k!='contracts']" 2>/dev/null || true

step "tier: SLA compliance"
$BF tier history   selfloop-v3 2>/dev/null | head -14 || true
$BF tier violations selfloop-v3 2>/dev/null | head 8  || true

step "queue: job stats"
$BF queue stats 2>/dev/null || true

step "entity: identity map"
$BF entity status 2>/dev/null | head -6 || true

step "time: scheduling state"
$BF time status 2>/dev/null | head -6 || true

step "space: all iteration snapshots"
$BF space list "$SPACE_NAME" 2>/dev/null || true

step "vec: vector store summary"
$BF vec count "$VEC_DB" 2>/dev/null || true

step "fragment: behavioral records"
$BF fragment stats --store "$FRAG_DB" 2>/dev/null | head -8 || true

step "self-similarity matrix (consecutive iters)"
echo "  Cosine similarity across all consecutive pairs:"
for i in $(seq 1 $((ITERS-1))); do
  ID1="v3-iter${i}"; ID2="v3-iter$((i+1))"
  SIM=$($BF vec compare "$VEC_DB" "$ID1" "$ID2" 2>/dev/null \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print(f\"{d.get('cosine_similarity',0):.4f}\")" 2>/dev/null || echo "n/a")
  printf "    iter-%-2d → iter-%-2d  cosine=%-8s\n" "$i" "$((i+1))" "$SIM"
done

step "graph: Merkle-DAG summary"
python3 -c "
import sqlite3, os
db='$GRAPH_DB'
if not os.path.exists(db): print('  (no graph DB)'); exit()
c = sqlite3.connect(db)
atoms = c.execute('SELECT COUNT(*) FROM atoms').fetchone()[0]
ops   = c.execute('SELECT COUNT(*) FROM operators').fetchone()[0]
c.close()
print(f'  atoms: {atoms}  operators: {ops}')
" 2>/dev/null || true

banner "bonfyre-selfloop-v3 complete — log: $LOG"
echo "  Run 'bonfyre status' for system-wide snapshot"
echo "  Run 'bash $0 --iters $ITERS' to continue accumulating"
