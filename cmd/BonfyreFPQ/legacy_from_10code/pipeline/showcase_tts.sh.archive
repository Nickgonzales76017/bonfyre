#!/bin/bash
# showcase_tts.sh — TTS + Voice Clone model compression showcase
# Phase 1: Orpheus 3B + Chatterbox (commercial-license, safetensors)
# Phase 2: F5-TTS + Voxtral 4B (high-impact, safetensors)
set -e

BIN="/workspace/bonfyre/10-Code/BonfyreFPQ/bonfyre-fpq"
MODELS="/workspace/models/original"
OUT="/workspace/models/algebra"
LOGS="/workspace/logs"
mkdir -p "$OUT" "$LOGS"

export OMP_NUM_THREADS=$(nproc)
export OPENBLAS_NUM_THREADS=1
export HF_TOKEN="${HF_TOKEN:-}"

echo "═══════════════════════════════════════════════════════"
echo " Bonfyre TTS Compression Showcase"
echo " GPU: $(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null || echo 'CPU')"
echo " Threads: $OMP_NUM_THREADS"
echo "═══════════════════════════════════════════════════════"

# ─── Helper: download model if not cached ───
dl_model() {
    local repo="$1" tgt="$2" patterns="${3:-*.safetensors,*.json,*.txt,*.model,*.tiktoken}"
    if [ -d "$tgt" ] && ls "$tgt"/*.safetensors >/dev/null 2>&1; then
        echo "  [cached] $repo"
        return
    fi
    echo "  Downloading $repo..."
    mkdir -p "$tgt"
    python3 -c "
from huggingface_hub import snapshot_download
snapshot_download('$repo', local_dir='$tgt',
    allow_patterns=[$(echo "$patterns" | sed "s/,/','/g" | sed "s/^/'/" | sed "s/$/'/" )],
    ignore_patterns=['*.bin','*.onnx','*.pt','*.msgpack','optimizer*','training*','*.md'])
"
}

# ═══════════════════════════════════════════════════════
# PHASE 1A: Orpheus TTS 3B (Apache 2.0, Llama-based)
# ═══════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════════════════════"
echo " Phase 1A: Orpheus TTS 3B — algebra-compress"
echo "═══════════════════════════════════════════════════════"

dl_model "canopylabs/orpheus-3b-0.1-ft" "$MODELS/orpheus-3b"

echo ""
echo "  Analyzing Orpheus TTS..."
for shard in "$MODELS/orpheus-3b"/model*.safetensors; do
    $BIN algebra-analyze "$shard" 2>&1 | tee -a "$LOGS/analyze_orpheus.log"
done

echo ""
echo "  Compressing Orpheus TTS..."
mkdir -p "$OUT/orpheus-3b-algebra-fpq3"
START=$SECONDS
for shard in "$MODELS/orpheus-3b"/model*.safetensors; do
    bn=$(basename "$shard")
    $BIN algebra-compress "$shard" "$OUT/orpheus-3b-algebra-fpq3/$bn" --bits 3 \
        2>&1 | tee -a "$LOGS/compress_orpheus.log"
done
cp "$MODELS/orpheus-3b"/*.json "$MODELS/orpheus-3b"/*.model "$MODELS/orpheus-3b"/*.tiktoken "$OUT/orpheus-3b-algebra-fpq3/" 2>/dev/null || true
cp "$MODELS/orpheus-3b"/tokenizer* "$OUT/orpheus-3b-algebra-fpq3/" 2>/dev/null || true
echo "Orpheus done in $((SECONDS-START))s"
echo "Original: $(du -sh "$MODELS/orpheus-3b" | cut -f1)"
echo "Compressed: $(du -sh "$OUT/orpheus-3b-algebra-fpq3" | cut -f1)"

# ═══════════════════════════════════════════════════════
# PHASE 1B: Chatterbox (MIT, multi-component)
# ═══════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════════════════════"
echo " Phase 1B: Chatterbox — per-component algebra-compress"
echo "═══════════════════════════════════════════════════════"

dl_model "ResembleAI/chatterbox" "$MODELS/chatterbox" "*.safetensors,*.json,*.txt,*.yaml"

echo ""
echo "  Analyzing Chatterbox components..."
for sf in "$MODELS/chatterbox"/*.safetensors; do
    echo "  --- $(basename $sf) ---"
    $BIN algebra-analyze "$sf" 2>&1 | tee -a "$LOGS/analyze_chatterbox.log"
done

echo ""
echo "  Compressing Chatterbox components..."
mkdir -p "$OUT/chatterbox-algebra-fpq3"
START=$SECONDS
for sf in "$MODELS/chatterbox"/*.safetensors; do
    bn=$(basename "$sf")
    $BIN algebra-compress "$sf" "$OUT/chatterbox-algebra-fpq3/$bn" --bits 3 \
        2>&1 | tee -a "$LOGS/compress_chatterbox.log"
done
cp "$MODELS/chatterbox"/*.json "$MODELS/chatterbox"/*.yaml "$MODELS/chatterbox"/*.txt "$OUT/chatterbox-algebra-fpq3/" 2>/dev/null || true
echo "Chatterbox done in $((SECONDS-START))s"
echo "Original: $(du -sh "$MODELS/chatterbox" | cut -f1)"
echo "Compressed: $(du -sh "$OUT/chatterbox-algebra-fpq3" | cut -f1)"

# ═══════════════════════════════════════════════════════
# PHASE 2A: F5-TTS (DiT, Flow Matching)
# ═══════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════════════════════"
echo " Phase 2A: F5-TTS — algebra-compress"
echo "═══════════════════════════════════════════════════════"

dl_model "SWivid/F5-TTS" "$MODELS/f5-tts" "F5TTS_v1_Base/*.safetensors,*.json"

# F5-TTS stores weights in a subfolder
F5_MODEL=$(find "$MODELS/f5-tts" -name "*.safetensors" | head -1)
if [ -n "$F5_MODEL" ]; then
    echo "  Analyzing F5-TTS..."
    $BIN algebra-analyze "$F5_MODEL" 2>&1 | tee "$LOGS/analyze_f5tts.log"

    echo ""
    echo "  Compressing F5-TTS..."
    mkdir -p "$OUT/f5-tts-algebra-fpq3"
    START=$SECONDS
    $BIN algebra-compress "$F5_MODEL" "$OUT/f5-tts-algebra-fpq3/$(basename $F5_MODEL)" --bits 3 \
        2>&1 | tee "$LOGS/compress_f5tts.log"
    find "$MODELS/f5-tts" -name "*.json" -exec cp {} "$OUT/f5-tts-algebra-fpq3/" \; 2>/dev/null || true
    echo "F5-TTS done in $((SECONDS-START))s"
    echo "Original: $(du -sh "$F5_MODEL" | cut -f1)"
    echo "Compressed: $(du -sh "$OUT/f5-tts-algebra-fpq3" | cut -f1)"
else
    echo "  ERROR: F5-TTS safetensors not found"
fi

# ═══════════════════════════════════════════════════════
# PHASE 2B: Voxtral TTS 4B (Mistral, Enterprise)
# ═══════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════════════════════"
echo " Phase 2B: Voxtral TTS 4B — algebra-compress"
echo "═══════════════════════════════════════════════════════"

dl_model "mistralai/Voxtral-4B-TTS-2603" "$MODELS/voxtral-tts" "*.safetensors,*.json,*.txt,*.model"

echo ""
echo "  Analyzing Voxtral TTS..."
for sf in "$MODELS/voxtral-tts"/*.safetensors; do
    $BIN algebra-analyze "$sf" 2>&1 | tee -a "$LOGS/analyze_voxtral.log"
done

echo ""
echo "  Compressing Voxtral TTS..."
mkdir -p "$OUT/voxtral-tts-algebra-fpq3"
START=$SECONDS
for sf in "$MODELS/voxtral-tts"/*.safetensors; do
    bn=$(basename "$sf")
    $BIN algebra-compress "$sf" "$OUT/voxtral-tts-algebra-fpq3/$bn" --bits 3 \
        2>&1 | tee -a "$LOGS/compress_voxtral.log"
done
cp "$MODELS/voxtral-tts"/*.json "$MODELS/voxtral-tts"/*.model "$MODELS/voxtral-tts"/*.txt "$OUT/voxtral-tts-algebra-fpq3/" 2>/dev/null || true
echo "Voxtral done in $((SECONDS-START))s"
echo "Original: $(du -sh "$MODELS/voxtral-tts" | cut -f1)"
echo "Compressed: $(du -sh "$OUT/voxtral-tts-algebra-fpq3" | cut -f1)"

# ═══════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════════════════════"
echo " ALL TTS MODELS COMPLETE"
echo "═══════════════════════════════════════════════════════"
echo ""
echo "Results:"
du -sh "$OUT"/*
echo ""
echo "Analysis logs:"
ls -la "$LOGS"/analyze_*.log
echo ""
echo "Next: Upload to HuggingFace"
echo "  python3 -c \"from huggingface_hub import HfApi; api=HfApi(); api.upload_folder(...)\""
