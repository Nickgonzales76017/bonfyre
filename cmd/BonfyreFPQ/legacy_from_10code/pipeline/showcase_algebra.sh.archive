#!/bin/bash
# showcase_algebra.sh — Compound algebra recipes on RunPod
# Downloads models, runs algebra operations, uploads results to HF
set -e

BIN="/workspace/bonfyre/10-Code/BonfyreFPQ/bonfyre-fpq"
MODELS="/workspace/models/original"
OUT="/workspace/models/algebra"
LOGS="/workspace/logs"
mkdir -p "$OUT" "$LOGS"

export OMP_NUM_THREADS=$(nproc)
export OPENBLAS_NUM_THREADS=1

echo "═══════════════════════════════════════════════════════"
echo " Bonfyre Weight Algebra — Compound Showcase"
echo " GPU: $(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null || echo 'CPU')"
echo " Threads: $OMP_NUM_THREADS"
echo "═══════════════════════════════════════════════════════"

# ─── Helper: download model if not cached ───
dl_model() {
    local repo="$1" tgt="$2"
    if [ -d "$tgt" ] && ls "$tgt"/*.safetensors >/dev/null 2>&1; then
        echo "  [cached] $repo"
        return
    fi
    echo "  Downloading $repo..."
    mkdir -p "$tgt"
    python3 -c "
from huggingface_hub import snapshot_download
snapshot_download('$repo', local_dir='$tgt',
    allow_patterns=['*.safetensors','*.json','*.txt','*.model'],
    ignore_patterns=['*.bin','*.onnx','*.pt','*.msgpack','optimizer*','training*'])
"
}

# ─── Download target models ───
echo ""
echo "Phase 1: Model acquisition"
dl_model "Qwen/Qwen2.5-3B"          "$MODELS/qwen2.5-3b"
dl_model "Qwen/Qwen2.5-3B-Instruct" "$MODELS/qwen2.5-3b-instruct"
dl_model "openai/whisper-large-v3"   "$MODELS/whisper-large-v3"

# Find the main safetensors file for each
QWEN_BASE=$(ls "$MODELS/qwen2.5-3b"/model*.safetensors | head -1)
QWEN_INST=$(ls "$MODELS/qwen2.5-3b-instruct"/model*.safetensors | head -1)
WHISPER=$(ls "$MODELS/whisper-large-v3"/model.safetensors 2>/dev/null || ls "$MODELS/whisper-large-v3"/*.safetensors | head -1)

echo ""
echo "Phase 2: Algebra analysis (classify models)"
echo "─────────────────────────────────────"
$BIN algebra-analyze "$QWEN_BASE" 2>&1 | tee "$LOGS/analyze_qwen_base.log"
$BIN algebra-analyze "$WHISPER"    2>&1 | tee "$LOGS/analyze_whisper.log"

# ═══════════════════════════════════════════════════════
# RECIPE 1: algebra-compress (the 20× pipeline)
# Decompose → hybrid prune → FPQ v9
# ═══════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════════════════════"
echo " Recipe 1: algebra-compress — Qwen 2.5 3B"
echo "═══════════════════════════════════════════════════════"
mkdir -p "$OUT/qwen2.5-3b-algebra-fpq3"
START=$SECONDS
for shard in "$MODELS/qwen2.5-3b"/model*.safetensors; do
    bn=$(basename "$shard")
    $BIN algebra-compress "$shard" "$OUT/qwen2.5-3b-algebra-fpq3/$bn" --bits 3 \
        2>&1 | tee -a "$LOGS/compress_qwen_base.log"
done
# Copy config files
cp "$MODELS/qwen2.5-3b"/*.json "$MODELS/qwen2.5-3b"/*.txt "$MODELS/qwen2.5-3b"/*.model "$OUT/qwen2.5-3b-algebra-fpq3/" 2>/dev/null || true
echo "Recipe 1 done in $((SECONDS-START))s"
echo "Original: $(du -sh "$MODELS/qwen2.5-3b" | cut -f1)"
echo "Compressed: $(du -sh "$OUT/qwen2.5-3b-algebra-fpq3" | cut -f1)"

# ═══════════════════════════════════════════════════════
# RECIPE 2: algebra-compress — Whisper Large V3
# LR-heavy model should show best compression
# ═══════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════════════════════"
echo " Recipe 2: algebra-compress — Whisper Large V3"
echo "═══════════════════════════════════════════════════════"
mkdir -p "$OUT/whisper-large-v3-algebra-fpq3"
START=$SECONDS
$BIN algebra-compress "$WHISPER" "$OUT/whisper-large-v3-algebra-fpq3/model.safetensors" --bits 3 \
    2>&1 | tee "$LOGS/compress_whisper.log"
cp "$MODELS/whisper-large-v3"/*.json "$OUT/whisper-large-v3-algebra-fpq3/" 2>/dev/null || true
echo "Recipe 2 done in $((SECONDS-START))s"
echo "Original: $(du -sh "$MODELS/whisper-large-v3" | cut -f1)"
echo "Compressed: $(du -sh "$OUT/whisper-large-v3-algebra-fpq3" | cut -f1)"

# ═══════════════════════════════════════════════════════
# RECIPE 3: algebra-merge + compress (model fusion)
# Merge Qwen base + instruct → algebra-compress result
# ═══════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════════════════════"
echo " Recipe 3: algebra-merge (base+instruct) → compress"
echo "═══════════════════════════════════════════════════════"
mkdir -p "$OUT/qwen2.5-3b-merged-algebra"
START=$SECONDS
# Merge at 0.7 base / 0.3 instruct (keep base character, add instruction following)
for shard in "$MODELS/qwen2.5-3b"/model*.safetensors; do
    bn=$(basename "$shard")
    inst_shard="$MODELS/qwen2.5-3b-instruct/$bn"
    [ -f "$inst_shard" ] || continue
    $BIN algebra-merge "$shard" "$inst_shard" "/tmp/merged_$bn" --alpha 0.7 \
        2>&1 | tee -a "$LOGS/merge_qwen.log"
    # Now compress the merged result
    $BIN algebra-compress "/tmp/merged_$bn" "$OUT/qwen2.5-3b-merged-algebra/$bn" --bits 3 \
        2>&1 | tee -a "$LOGS/compress_merged_qwen.log"
    rm -f "/tmp/merged_$bn"
done
cp "$MODELS/qwen2.5-3b"/*.json "$MODELS/qwen2.5-3b"/*.txt "$MODELS/qwen2.5-3b"/*.model "$OUT/qwen2.5-3b-merged-algebra/" 2>/dev/null || true
echo "Recipe 3 done in $((SECONDS-START))s"
echo "Result: $(du -sh "$OUT/qwen2.5-3b-merged-algebra" | cut -f1)"

# ═══════════════════════════════════════════════════════
# RECIPE 4: algebra-prune (aggressive) → compress
# 50% weight pruning + FPQ = extreme compression
# ═══════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════════════════════"
echo " Recipe 4: algebra-prune 50% → compress — Whisper"
echo "═══════════════════════════════════════════════════════"
mkdir -p "$OUT/whisper-large-v3-pruned-algebra"
START=$SECONDS
$BIN algebra-prune "$WHISPER" "/tmp/whisper_pruned.safetensors" --keep-ratio 0.5 --mode hybrid \
    2>&1 | tee "$LOGS/prune_whisper.log"
$BIN algebra-compress "/tmp/whisper_pruned.safetensors" "$OUT/whisper-large-v3-pruned-algebra/model.safetensors" --bits 3 \
    2>&1 | tee "$LOGS/compress_pruned_whisper.log"
cp "$MODELS/whisper-large-v3"/*.json "$OUT/whisper-large-v3-pruned-algebra/" 2>/dev/null || true
rm -f /tmp/whisper_pruned.safetensors
echo "Recipe 4 done in $((SECONDS-START))s"
echo "Original: $(du -sh "$MODELS/whisper-large-v3" | cut -f1)"
echo "Pruned+Compressed: $(du -sh "$OUT/whisper-large-v3-pruned-algebra" | cut -f1)"

# ═══════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════════════════════"
echo " ALL RECIPES COMPLETE"
echo "═══════════════════════════════════════════════════════"
echo ""
echo "Results:"
du -sh "$OUT"/*
echo ""
echo "Logs:"
ls -la "$LOGS"/*.log
echo ""
echo "Next: Upload to HuggingFace with:"
echo "  pip install huggingface_hub"
echo "  huggingface-cli upload NICKO/Qwen2.5-3B-BonfyreAlgebra-FPQ3 $OUT/qwen2.5-3b-algebra-fpq3"
echo "  huggingface-cli upload NICKO/Whisper-Large-V3-BonfyreAlgebra-FPQ3 $OUT/whisper-large-v3-algebra-fpq3"
echo "  huggingface-cli upload NICKO/Qwen2.5-3B-Merged-BonfyreAlgebra $OUT/qwen2.5-3b-merged-algebra"
echo "  huggingface-cli upload NICKO/Whisper-Large-V3-Pruned-BonfyreAlgebra $OUT/whisper-large-v3-pruned-algebra"
