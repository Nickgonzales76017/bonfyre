#!/bin/bash
# BonfyreFPQ Worker — Pulls jobs, compresses models, pushes to GitHub.
# Run on a RunPod pod after pod_setup.sh.
#
# Usage:
#   GITHUB_TOKEN=ghp_xxx HF_TOKEN=hf_xxx bash worker.sh
#   GITHUB_TOKEN=ghp_xxx HF_TOKEN=hf_xxx bash worker.sh --once   # single job then exit
#   GITHUB_TOKEN=ghp_xxx bash worker.sh --model THUDM/glm-4-9b   # direct model
set -euo pipefail

# ── Config ──
FPQ_BIN="/workspace/bonfyre/10-Code/BonfyreFPQ/bonfyre-fpq"
JOBS_DIR="/workspace/jobs"
MODELS_DIR="/workspace/models"
LOGS_DIR="/workspace/logs"
GITHUB_REPO="${GITHUB_REPO:-Nickgonzales76017/bonfyre-fpq-models}"
GITHUB_TOKEN="${GITHUB_TOKEN:-}"
HF_TOKEN="${HF_TOKEN:-}"
BITS="${BITS:-3}"
ONCE=false
DIRECT_MODEL=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --once)   ONCE=true; shift ;;
        --model)  DIRECT_MODEL="$2"; shift 2 ;;
        --bits)   BITS="$2"; shift 2 ;;
        *)        echo "Unknown arg: $1"; exit 1 ;;
    esac
done

# ── Preflight ──
if [ ! -x "$FPQ_BIN" ]; then
    echo "ERROR: bonfyre-fpq not found at $FPQ_BIN"
    echo "Run pod_setup.sh first."
    exit 1
fi

if [ -z "$GITHUB_TOKEN" ]; then
    echo "WARNING: GITHUB_TOKEN not set — results won't be pushed to GitHub."
    echo "  Set: export GITHUB_TOKEN=ghp_xxx"
fi

if [ -n "$HF_TOKEN" ]; then
    echo "$HF_TOKEN" | huggingface-cli login --token "$(cat /dev/stdin)" 2>/dev/null || true
fi

echo "============================================"
echo " BonfyreFPQ Worker"
echo " $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo " GPU: $(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null || echo 'N/A')"
echo " RAM: $(free -h | awk '/Mem:/{print $2}')"
echo "============================================"

# ── Functions ──

download_model() {
    local model_id="$1"
    local dest="$MODELS_DIR/original/$(echo "$model_id" | tr '/' '__')"

    if [ -d "$dest" ] && ls "$dest"/*.safetensors "$dest"/*.bin 2>/dev/null | head -1 > /dev/null; then
        echo "  Model already downloaded: $dest"
        echo "$dest"
        return
    fi

    echo "  Downloading $model_id..."
    python3 -c "
from huggingface_hub import snapshot_download
snapshot_download(
    '$model_id',
    local_dir='$dest',
    allow_patterns=['*.safetensors', '*.json', '*.model', '*.tiktoken', 'tokenizer*'],
    ignore_patterns=['*.bin', '*.onnx', '*.pt', '*.h5', '*.msgpack', 'onnx/*', 'flax_model*'],
)
print('DONE')
"
    echo "$dest"
}

compress_model() {
    local model_dir="$1"
    local model_id="$2"
    local safe_name
    safe_name="$(echo "$model_id" | tr '/' '__')"
    local out_dir="$MODELS_DIR/fpq/$safe_name"
    local log_file="$LOGS_DIR/${safe_name}_$(date +%s).log"

    mkdir -p "$out_dir"

    echo "  Compressing: $model_id → $out_dir"
    echo "  Log: $log_file"

    # Find all safetensor shards
    local shards
    shards=$(find "$model_dir" -name "*.safetensors" -type f | sort)

    if [ -z "$shards" ]; then
        echo "  ERROR: No .safetensors files found in $model_dir"
        return 1
    fi

    local shard_count
    shard_count=$(echo "$shards" | wc -l | tr -d ' ')
    echo "  Found $shard_count shard(s)"

    local start_time=$SECONDS

    # Process each shard
    local i=0
    while IFS= read -r shard; do
        i=$((i + 1))
        local shard_basename
        shard_basename=$(basename "$shard")
        local out_file="$out_dir/$shard_basename"

        echo "  [$i/$shard_count] $shard_basename"

        "$FPQ_BIN" quantize "$shard" "$out_file" --bits "$BITS" \
            2>&1 | tee -a "$log_file"

        if [ $? -ne 0 ]; then
            echo "  ERROR: Compression failed for $shard_basename"
            return 1
        fi
    done <<< "$shards"

    # Copy non-weight files (config, tokenizer, etc.)
    for f in "$model_dir"/*.json "$model_dir"/*.model "$model_dir"/*.tiktoken "$model_dir"/tokenizer*; do
        [ -f "$f" ] && cp "$f" "$out_dir/" 2>/dev/null || true
    done

    local elapsed=$(( SECONDS - start_time ))
    local orig_size
    orig_size=$(du -sh "$model_dir" | cut -f1)
    local fpq_size
    fpq_size=$(du -sh "$out_dir" | cut -f1)

    echo ""
    echo "  ╔══════════════════════════════════════╗"
    echo "  ║  Compression complete                ║"
    echo "  ║  Model:    $model_id"
    echo "  ║  Original: $orig_size"
    echo "  ║  FPQ:      $fpq_size"
    echo "  ║  Time:     ${elapsed}s"
    echo "  ║  Bits:     $BITS"
    echo "  ╚══════════════════════════════════════╝"
    echo ""

    # Write metadata
    cat > "$out_dir/fpq_metadata.json" << METAEOF
{
    "model_id": "$model_id",
    "bits": $BITS,
    "fpq_version": "v9",
    "original_size": "$orig_size",
    "compressed_size": "$fpq_size",
    "compression_time_s": $elapsed,
    "shards": $shard_count,
    "date": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "gpu": "$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null || echo 'N/A')",
    "hostname": "$(hostname)"
}
METAEOF

    echo "$out_dir"
}

push_to_github() {
    local model_dir="$1"
    local model_id="$2"
    local safe_name
    safe_name="$(echo "$model_id" | tr '/' '__')"

    if [ -z "$GITHUB_TOKEN" ]; then
        echo "  Skipping GitHub push (no GITHUB_TOKEN)"
        return
    fi

    echo "  Pushing to GitHub..."

    # Strategy: create a GitHub release with the compressed files as assets.
    # This avoids Git LFS for large files.

    local tag="fpq-${safe_name}-v9-${BITS}bit"
    local release_name="FPQ v9 ${BITS}-bit: $model_id"
    local body
    body=$(cat "$model_dir/fpq_metadata.json" 2>/dev/null || echo "{}")

    # Create release
    local release_response
    release_response=$(curl -s -X POST \
        -H "Authorization: token $GITHUB_TOKEN" \
        -H "Content-Type: application/json" \
        "https://api.github.com/repos/$GITHUB_REPO/releases" \
        -d "{
            \"tag_name\": \"$tag\",
            \"name\": \"$release_name\",
            \"body\": $(echo "$body" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))'),
            \"draft\": false,
            \"prerelease\": false
        }")

    local upload_url
    upload_url=$(echo "$release_response" | python3 -c "
import sys, json
d = json.load(sys.stdin)
if 'upload_url' in d:
    print(d['upload_url'].split('{')[0])
else:
    print('ERROR: ' + d.get('message', 'unknown'))
    sys.exit(1)
" 2>/dev/null) || {
        echo "  WARNING: Could not create release. Response:"
        echo "$release_response" | head -5
        echo "  Trying git push instead..."
        push_via_git "$model_dir" "$model_id"
        return
    }

    if [[ "$upload_url" == ERROR:* ]]; then
        echo "  $upload_url"
        echo "  Trying git push instead..."
        push_via_git "$model_dir" "$model_id"
        return
    fi

    echo "  Release created: $tag"
    echo "  Uploading assets..."

    # Upload each file as a release asset
    for f in "$model_dir"/*; do
        local fname
        fname=$(basename "$f")
        local fsize
        fsize=$(du -h "$f" | cut -f1)
        echo "    Uploading $fname ($fsize)..."

        curl -s -X POST \
            -H "Authorization: token $GITHUB_TOKEN" \
            -H "Content-Type: application/octet-stream" \
            "${upload_url}?name=${fname}" \
            --data-binary "@$f" > /dev/null

        echo "    Done: $fname"
    done

    echo "  All assets uploaded to release: $tag"
}

push_via_git() {
    local model_dir="$1"
    local model_id="$2"
    local safe_name
    safe_name="$(echo "$model_id" | tr '/' '__')"
    local branch="models/${safe_name}"

    echo "  Pushing via git (branch: $branch)..."

    local repo_dir="/workspace/fpq-models-repo"
    if [ ! -d "$repo_dir/.git" ]; then
        git clone --depth 1 "https://${GITHUB_TOKEN}@github.com/${GITHUB_REPO}.git" "$repo_dir" 2>/dev/null || {
            # Repo might not exist — create it
            echo "  Creating repo $GITHUB_REPO..."
            curl -s -X POST \
                -H "Authorization: token $GITHUB_TOKEN" \
                -H "Content-Type: application/json" \
                "https://api.github.com/user/repos" \
                -d "{\"name\": \"$(echo "$GITHUB_REPO" | cut -d/ -f2)\", \"private\": false, \"description\": \"BonfyreFPQ compressed model weights\"}" > /dev/null
            git clone "https://${GITHUB_TOKEN}@github.com/${GITHUB_REPO}.git" "$repo_dir" 2>/dev/null || {
                git init "$repo_dir"
                cd "$repo_dir"
                git remote add origin "https://${GITHUB_TOKEN}@github.com/${GITHUB_REPO}.git"
            }
        }
    fi

    cd "$repo_dir"
    git checkout -B "$branch"

    # Copy metadata and small files (not giant weight files — those go in releases)
    mkdir -p "$safe_name"
    cp "$model_dir/fpq_metadata.json" "$safe_name/" 2>/dev/null || true
    cp "$model_dir"/*.json "$safe_name/" 2>/dev/null || true

    # Create a README for this model
    cat > "$safe_name/README.md" << READMEEOF
# $model_id — BonfyreFPQ v9 ${BITS}-bit

Compressed with [BonfyreFPQ](https://github.com/Nickgonzales76017/bonfyre).

## Details
$(cat "$model_dir/fpq_metadata.json" 2>/dev/null || echo "See fpq_metadata.json")

## Download
Check the [Releases](https://github.com/$GITHUB_REPO/releases) for weight files.

## Usage
\`\`\`bash
# These weights load directly into any framework expecting safetensors/GGUF
# No special runtime needed — standard BF16/F16 outputs
\`\`\`
READMEEOF

    git add -A
    git commit -m "Add FPQ v9 ${BITS}-bit: $model_id" 2>/dev/null || true
    git push -u origin "$branch" 2>/dev/null || echo "  WARNING: git push failed"
    echo "  Pushed metadata to $GITHUB_REPO branch $branch"
}

process_job() {
    local job_file="$1"
    local job_basename
    job_basename=$(basename "$job_file")

    echo ""
    echo "════════════════════════════════════════"
    echo " Processing: $job_basename"
    echo " $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "════════════════════════════════════════"

    # Move to running
    mv "$job_file" "$JOBS_DIR/running/$job_basename"
    local running_file="$JOBS_DIR/running/$job_basename"

    # Parse job
    local model_id bits
    model_id=$(python3 -c "import json; print(json.load(open('$running_file'))['model'])")
    bits=$(python3 -c "import json; print(json.load(open('$running_file')).get('bits', 3))")
    BITS="$bits"

    echo "  Model: $model_id"
    echo "  Bits:  $bits"

    # Download
    local model_dir
    model_dir=$(download_model "$model_id")

    # Compress
    local out_dir
    out_dir=$(compress_model "$model_dir" "$model_id") || {
        echo "  FAILED: $model_id"
        mv "$running_file" "$JOBS_DIR/failed/$job_basename"
        return 1
    }

    # Push to GitHub
    push_to_github "$out_dir" "$model_id"

    # Move to done
    mv "$running_file" "$JOBS_DIR/done/$job_basename"
    echo "  DONE: $model_id"
}

# ── Direct model mode ──
if [ -n "$DIRECT_MODEL" ]; then
    echo "Direct mode: $DIRECT_MODEL"
    model_dir=$(download_model "$DIRECT_MODEL")
    out_dir=$(compress_model "$model_dir" "$DIRECT_MODEL")
    push_to_github "$out_dir" "$DIRECT_MODEL"
    echo "Complete."
    exit 0
fi

# ── Job queue loop ──
echo ""
echo "Starting job queue worker..."
echo "  Jobs dir: $JOBS_DIR/pending/"
echo "  Drop .json files there, or use: python deploy.py submit <model>"
echo ""

while true; do
    # Pick the next job (sorted by filename, which includes priority)
    job=$(find "$JOBS_DIR/pending" -name "*.json" -type f 2>/dev/null | sort | head -1)

    if [ -n "$job" ]; then
        process_job "$job" || true
        if $ONCE; then
            echo "Single-job mode. Exiting."
            exit 0
        fi
    else
        if $ONCE; then
            echo "No jobs found. Exiting."
            exit 0
        fi
        sleep 10
    fi
done
