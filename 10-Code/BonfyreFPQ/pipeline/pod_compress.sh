#!/bin/bash
# pod_compress.sh — Generic FPQ compression runner for RunPod
# Usage: bash pod_compress.sh <models_file>
# models_file: one HuggingFace model ID per line
set -euo pipefail

HF_TOKEN="${HF_TOKEN:-}"  # set via env or pipeline/.env
BIN="/workspace/bonfyre/10-Code/BonfyreFPQ/bonfyre-fpq"
BITS="${FPQ_BITS:-3}"

export OMP_NUM_THREADS="${OMP_NUM_THREADS:-16}"
export OMP_STACKSIZE=8M
export OPENBLAS_NUM_THREADS=4

MODELS_FILE="$1"

# ── Setup ──
echo "=== BonfyreFPQ Pod Setup ==="
apt-get update -qq && apt-get install -y -qq build-essential libopenblas-dev liblapack-dev > /dev/null 2>&1

# ── Setup source ──
FPQDIR="/workspace/bonfyre/10-Code/BonfyreFPQ"
mkdir -p "$FPQDIR"

# Extract source tarball if present (preferred — has all files)
if [ -f /workspace/bonfyrefpq_src.tar.gz ]; then
    tar xzf /workspace/bonfyrefpq_src.tar.gz -C "$FPQDIR" --no-same-owner 2>/dev/null || true
    echo "Extracted source tarball"
elif [ ! -d "$FPQDIR/src" ] || [ ! -f "$FPQDIR/include/fpq.h" ]; then
    git clone https://github.com/Nickgonzales76017/bonfyre.git /workspace/bonfyre 2>/dev/null || \
        (cd /workspace/bonfyre && git pull origin main 2>/dev/null || true)
fi

# Overlay SCP'd source files if present (newer than git/tarball)
if [ -f /workspace/v4_optimizations.c ]; then
    cp /workspace/v4_optimizations.c "$FPQDIR/src/v4_optimizations.c"
    echo "Overlaid v4_optimizations.c from SCP"
fi

cd "$FPQDIR"

# Use SCP'd Makefile.linux if present (avoids heredoc tab issues)
if [ -f /workspace/Makefile.linux ]; then
    cp /workspace/Makefile.linux ./Makefile.linux
    echo "Using SCP'd Makefile.linux"
fi

make -f Makefile.linux clean 2>/dev/null || true
make -f Makefile.linux -j$(nproc) 2>&1 | tail -2

pip install -q huggingface_hub 2>/dev/null

mkdir -p /workspace/models/original /workspace/models/fpq /workspace/logs

echo "=== Build done, starting compression ==="
echo "Models file: $MODELS_FILE"
echo "Bits: $BITS | OMP threads: $OMP_NUM_THREADS"
echo "Start: $(date -u)"

TOTAL_START=$SECONDS

# ── Process each model ──
while IFS= read -r MODEL_ID || [ -n "$MODEL_ID" ]; do
    [ -z "$MODEL_ID" ] && continue
    [[ "$MODEL_ID" == \#* ]] && continue

    SAFE_NAME=$(echo "$MODEL_ID" | tr '/' '_')
    ORIG_DIR="/workspace/models/original/$SAFE_NAME"
    FPQ_DIR="/workspace/models/fpq/$SAFE_NAME"
    LOG="/workspace/logs/${SAFE_NAME}.log"

    # Skip if already done
    if [ -d "$FPQ_DIR" ] && [ "$(ls "$FPQ_DIR"/*.safetensors 2>/dev/null | wc -l)" -gt 0 ]; then
        echo "[SKIP] $MODEL_ID (already compressed)"
        continue
    fi

    echo ""
    echo "══════════════════════════════════════════════"
    echo " Downloading: $MODEL_ID"
    echo "══════════════════════════════════════════════"
    MODEL_START=$SECONDS

    python3 -c "
from huggingface_hub import snapshot_download
snapshot_download(
    '$MODEL_ID',
    local_dir='$ORIG_DIR',
    allow_patterns=['*.safetensors', '*.json', '*.txt', '*.model', 'tokenizer*'],
    ignore_patterns=['*.bin', '*.onnx', '*.pt', '*.msgpack', 'consolidated*'],
    token='$HF_TOKEN',
)
print('Download complete')
" 2>&1

    # Copy config files to output
    mkdir -p "$FPQ_DIR"
    for f in "$ORIG_DIR"/*.json "$ORIG_DIR"/tokenizer* "$ORIG_DIR"/*.txt "$ORIG_DIR"/*.model; do
        [ -f "$f" ] && cp "$f" "$FPQ_DIR/" 2>/dev/null || true
    done

    echo ""
    echo "══════════════════════════════════════════════"
    echo " Compressing: $MODEL_ID → FPQ$BITS"
    echo "══════════════════════════════════════════════"

    SHARD_COUNT=0
    SHARD_DONE=0
    for sf in "$ORIG_DIR"/*.safetensors; do
        [ -f "$sf" ] && SHARD_COUNT=$((SHARD_COUNT + 1))
    done

    for sf in "$ORIG_DIR"/*.safetensors; do
        [ -f "$sf" ] || continue
        bn=$(basename "$sf")
        out="$FPQ_DIR/$bn"

        if [ -f "$out" ] && [ -s "$out" ]; then
            SHARD_DONE=$((SHARD_DONE + 1))
            echo "  [SKIP] $bn (exists)"
            continue
        fi

        SHARD_DONE=$((SHARD_DONE + 1))
        echo "  [$SHARD_DONE/$SHARD_COUNT] $bn ..."

        "$BIN" quantize "$sf" "$out" --bits "$BITS" >> "$LOG" 2>&1
        
        if [ $? -ne 0 ]; then
            echo "  [FAIL] $bn — see $LOG"
        else
            SIZE=$(du -h "$out" | cut -f1)
            echo "  [OK] $bn → $SIZE"
        fi
    done

    MODEL_TIME=$(( SECONDS - MODEL_START ))
    ORIG_SIZE=$(du -sh "$ORIG_DIR" 2>/dev/null | cut -f1)
    FPQ_SIZE=$(du -sh "$FPQ_DIR" 2>/dev/null | cut -f1)
    echo ""
    echo "✓ $MODEL_ID complete in ${MODEL_TIME}s"
    echo "  Original: $ORIG_SIZE → FPQ: $FPQ_SIZE"
    echo ""

    # Clean up original to save disk
    rm -rf "$ORIG_DIR"
    echo "  Cleaned original to save disk"

done < "$MODELS_FILE"

TOTAL_TIME=$(( SECONDS - TOTAL_START ))
echo ""
echo "═══════════════════════════════════════════════════"
echo " ALL DONE in $((TOTAL_TIME / 60))m ${TOTAL_TIME}s"
echo " Output: $(du -sh /workspace/models/fpq/ | cut -f1)"
echo "═══════════════════════════════════════════════════"
