#!/bin/bash
# BonfyreFPQ Remote Pipeline — Quick Launch
# Spins up a pod, copies jobs, starts the worker.
#
# Usage:
#   bash go.sh                      # Launch H200 + GLM queue
#   bash go.sh --gpu RTX6000PRO     # Use cheaper GPU
#   bash go.sh --model THUDM/glm-4-9b  # Single model, skip queue
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

GPU="${GPU:-RTX6000PRO}"
DIRECT_MODEL=""
BITS=3

while [[ $# -gt 0 ]]; do
    case $1 in
        --gpu)   GPU="$2"; shift 2 ;;
        --model) DIRECT_MODEL="$2"; shift 2 ;;
        --bits)  BITS="$2"; shift 2 ;;
        *)       echo "Unknown arg: $1"; exit 1 ;;
    esac
done

# ── Preflight ──
if [ ! -f .env ]; then
    echo "ERROR: pipeline/.env not found. Create it with:"
    echo "  RUNPOD_API_KEY=rpa_xxx"
    echo "  GITHUB_TOKEN=ghp_xxx"
    echo "  HF_TOKEN=hf_xxx"
    exit 1
fi

source <(grep -v '^#' .env | sed 's/^/export /')

echo "╔══════════════════════════════════════════╗"
echo "║  BonfyreFPQ Remote Pipeline              ║"
echo "╚══════════════════════════════════════════╝"
echo ""

# ── Step 1: Launch pod ──
echo "[1/4] Launching $GPU pod..."
POD_OUTPUT=$(python3 deploy.py launch --gpu "$GPU" --volume 1200 2>&1)
echo "$POD_OUTPUT"
POD_ID=$(echo "$POD_OUTPUT" | grep "ID:" | awk '{print $2}')

if [ -z "$POD_ID" ]; then
    echo "ERROR: Could not extract pod ID"
    exit 1
fi

echo ""
echo "  Pod ID: $POD_ID"

# ── Step 2: Wait for pod to be ready ──
echo "[2/4] Waiting for pod to start..."
MAX_WAIT=300
WAITED=0
while [ $WAITED -lt $MAX_WAIT ]; do
    STATUS=$(python3 -c "
import json, urllib.request, os
env = {}
with open('.env') as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip()
key = env['RUNPOD_API_KEY']
q = '{\"query\": \"{ pod(input: {podId: \\\"$POD_ID\\\"}) { id desiredStatus runtime { ports { ip isIpPublic privatePort publicPort } } } }\"}'.replace('\$POD_ID', '$POD_ID')
req = urllib.request.Request('https://api.runpod.io/graphql?api_key=' + key, q.encode(), {'Content-Type': 'application/json', 'User-Agent': 'BonfyreFPQ/1.0'})
r = json.loads(urllib.request.urlopen(req, timeout=15).read())
pod = r.get('data',{}).get('pod',{})
ports = (pod.get('runtime') or {}).get('ports') or []
ssh = [p for p in ports if p.get('privatePort') == 22]
if ssh:
    print(f\"READY {ssh[0]['ip']} {ssh[0]['publicPort']}\")
else:
    print(pod.get('desiredStatus', 'UNKNOWN'))
" 2>/dev/null) || STATUS="WAITING"

    if [[ "$STATUS" == READY* ]]; then
        SSH_IP=$(echo "$STATUS" | awk '{print $2}')
        SSH_PORT=$(echo "$STATUS" | awk '{print $3}')
        echo "  Pod ready! SSH: $SSH_IP:$SSH_PORT"
        break
    fi

    echo "  Status: $STATUS (${WAITED}s elapsed)"
    sleep 15
    WAITED=$((WAITED + 15))
done

if [ -z "${SSH_IP:-}" ]; then
    echo "ERROR: Pod didn't become ready in ${MAX_WAIT}s"
    echo "Check manually: python3 deploy.py list"
    exit 1
fi

SSH_CMD="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@$SSH_IP -p $SSH_PORT"
SCP_CMD="scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -P $SSH_PORT"

# ── Step 3: Setup pod ──
echo ""
echo "[3/4] Setting up pod (compiling bonfyre-fpq, installing deps)..."
$SSH_CMD 'bash -s' < pod_setup.sh

# ── Step 4: Copy jobs and start worker ──
echo ""
echo "[4/4] Deploying jobs and starting worker..."

# Copy job manifests to pod
JOB_COUNT=$(find jobs/pending -name "*.json" 2>/dev/null | wc -l | tr -d ' ')
if [ "$JOB_COUNT" -gt 0 ]; then
    echo "  Copying $JOB_COUNT jobs to pod..."
    $SCP_CMD jobs/pending/*.json "root@$SSH_IP:/workspace/jobs/pending/"
fi

# Start worker in background via tmux
WORKER_ARGS=""
if [ -n "$DIRECT_MODEL" ]; then
    WORKER_ARGS="--model $DIRECT_MODEL --bits $BITS"
fi

$SSH_CMD "
    export GITHUB_TOKEN='${GITHUB_TOKEN:-}'
    export HF_TOKEN='${HF_TOKEN:-}'
    export GITHUB_REPO='${GITHUB_REPO:-Nickgonzales76017/bonfyre-fpq-models}'
    export BITS='$BITS'

    # Install tmux if not present
    which tmux >/dev/null 2>&1 || apt-get install -y -qq tmux

    # Start worker in a tmux session so it survives SSH disconnect
    tmux new-session -d -s fpq-worker \
        \"bash /workspace/bonfyre/10-Code/BonfyreFPQ/pipeline/worker.sh $WORKER_ARGS 2>&1 | tee /workspace/logs/worker_\$(date +%s).log\"

    echo 'Worker started in tmux session: fpq-worker'
    echo 'To attach:  tmux attach -t fpq-worker'
    echo 'To detach:  Ctrl-B then D'
"

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║  Pipeline launched!                      ║"
echo "╠══════════════════════════════════════════╣"
echo "║  Pod ID:    $POD_ID"
echo "║  GPU:       $GPU"
echo "║  SSH:       $SSH_CMD"
echo "║  Jobs:      $JOB_COUNT queued"
echo "║                                          ║"
echo "║  Monitor:   $SSH_CMD tmux attach -t fpq-worker"
echo "║  Stop pod:  python3 deploy.py stop $POD_ID"
echo "╚══════════════════════════════════════════╝"
