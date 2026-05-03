#!/bin/bash
# BonfyreFPQ Parallel Compression v2 — Maximize 128-core utilization
# Uses xargs with proper process isolation
set -uo pipefail

MODEL_DIR="/workspace/models/original/zai-org_GLM-5.1-FP8"
OUT_DIR="/workspace/models/fpq/zai-org_GLM-5.1-FP8"
FPQ_BIN="/workspace/bonfyre/10-Code/BonfyreFPQ/bonfyre-fpq"
LOG_DIR="/workspace/logs"
BITS=3

# ── Tuning ──
# 128 cores, 503GB RAM. Each shard: ~5GB input, ~30GB peak RAM (big tensors)
# 4 parallel × 32 OMP threads = 128 threads saturating all cores
PARALLEL_JOBS=4
THREADS_PER_JOB=32

# OpenBLAS is NOT fork-safe — single thread only, use OMP instead
export OPENBLAS_NUM_THREADS=1
export GOTO_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OMP_NUM_THREADS=$THREADS_PER_JOB

mkdir -p "$OUT_DIR" "$LOG_DIR"

echo "============================================"
echo " BonfyreFPQ Parallel Compression v2"
echo " $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo " Jobs: $PARALLEL_JOBS parallel"
echo " Threads/job: $THREADS_PER_JOB (OMP)"
echo " Total threads: $((PARALLEL_JOBS * THREADS_PER_JOB))"
echo " CPU cores: $(nproc)"
echo " RAM: $(free -h | awk '/Mem:/{print $2}')"
echo "============================================"

# Build shard list
mapfile -t SHARDS < <(find "$MODEL_DIR" -name "*.safetensors" -type f | sort)
TOTAL=${#SHARDS[@]}
echo "Total shards: $TOTAL"

# Progress tracking with atomic file-per-shard (no race conditions)
PROGRESS_DIR="/tmp/fpq_progress"
rm -rf "$PROGRESS_DIR"
mkdir -p "$PROGRESS_DIR"
START_TIME=$SECONDS

echo ""
echo "Starting $PARALLEL_JOBS parallel workers at $(date -u +%H:%M:%S)..."
echo ""

# Process function — runs in subshell via xargs
process_shard() {
    local shard="$1"
    local basename
    basename=$(basename "$shard")
    local out_file="$OUT_DIR/$basename"
    local log_file="$LOG_DIR/shard_${basename}.log"

    # Skip if already done
    if [ -f "$out_file" ] && [ -s "$out_file" ]; then
        touch "$PROGRESS_DIR/done_${basename}"
        echo "[SKIP] $basename"
        return 0
    fi

    export OMP_NUM_THREADS=$THREADS_PER_JOB
    export OPENBLAS_NUM_THREADS=1
    export GOTO_NUM_THREADS=1

    local t0=$SECONDS

    # Run compression
    "$FPQ_BIN" quantize "$shard" "$out_file" --bits "$BITS" \
        > "$log_file" 2>&1 || true
    local rc=${PIPESTATUS[0]:-$?}

    local elapsed=$(( SECONDS - t0 ))

    if [ -f "$out_file" ] && [ -s "$out_file" ]; then
        touch "$PROGRESS_DIR/done_${basename}"
        local done_n
        done_n=$(ls -1 "$PROGRESS_DIR"/done_* 2>/dev/null | wc -l)
        local fail_n
        fail_n=$(ls -1 "$PROGRESS_DIR"/fail_* 2>/dev/null | wc -l)
        local total_processed=$((done_n + fail_n))
        local global_elapsed=$(( SECONDS - START_TIME ))

        if [ $done_n -gt 2 ] && [ $global_elapsed -gt 0 ]; then
            local remaining_shards=$(( TOTAL - total_processed ))
            local avg_wall=$(( global_elapsed * PARALLEL_JOBS / done_n ))
            local eta_s=$(( remaining_shards * avg_wall / PARALLEL_JOBS ))
            local eta_m=$(( eta_s / 60 ))
            local out_sz
            out_sz=$(du -sh "$out_file" 2>/dev/null | cut -f1)
            echo "[DONE $done_n/$TOTAL] $basename — ${elapsed}s — $out_sz — ETA: ~${eta_m}m"
        else
            echo "[DONE $done_n/$TOTAL] $basename — ${elapsed}s"
        fi
    else
        touch "$PROGRESS_DIR/fail_${basename}"
        echo "[FAIL] $basename — exit $rc — ${elapsed}s — see $log_file"
    fi
    return 0
}

export -f process_shard
export OUT_DIR FPQ_BIN BITS LOG_DIR THREADS_PER_JOB PROGRESS_DIR TOTAL START_TIME PARALLEL_JOBS

# ── Launch ──
printf '%s\n' "${SHARDS[@]}" | xargs -P "$PARALLEL_JOBS" -I {} bash -c 'process_shard "$@"' _ {}

# ── Summary ──
ELAPSED=$(( SECONDS - START_TIME ))
DONE_COUNT=$(ls -1 "$PROGRESS_DIR"/done_* 2>/dev/null | wc -l)
FAIL_COUNT=$(ls -1 "$PROGRESS_DIR"/fail_* 2>/dev/null | wc -l)

# Copy config/tokenizer
for f in "$MODEL_DIR"/*.json "$MODEL_DIR"/*.model "$MODEL_DIR"/*.tiktoken "$MODEL_DIR"/tokenizer*; do
    [ -f "$f" ] && cp "$f" "$OUT_DIR/" 2>/dev/null || true
done

ORIG_SIZE=$(du -sh "$MODEL_DIR" | cut -f1)
FPQ_SIZE=$(du -sh "$OUT_DIR" | cut -f1)

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║  Parallel Compression Complete               ║"
echo "║  Done:     $DONE_COUNT / $TOTAL shards"
echo "║  Failed:   $FAIL_COUNT"
echo "║  Time:     $(( ELAPSED / 3600 ))h $(( (ELAPSED % 3600) / 60 ))m $(( ELAPSED % 60 ))s"
echo "║  Original: $ORIG_SIZE"
echo "║  FPQ:      $FPQ_SIZE"
echo "╚══════════════════════════════════════════════╝"

if [ "$FAIL_COUNT" -gt 0 ]; then
    echo ""
    echo "Failed shards:"
    ls "$PROGRESS_DIR"/fail_* 2>/dev/null | sed 's|.*/fail_||'
fi
