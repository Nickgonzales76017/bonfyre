#!/usr/bin/env python3
"""
launch_7b_validation.py — E8 + K-FAC validation on a 7B model.

Launches an A40 RunPod, trains E8+K-FAC on Mistral-7B-v0.1 (attn layers,
200 steps), encodes the checkpoint to .e8 format, benchmarks PPL, then
terminates the pod.

This is the scale validation: TinyLlama-1.1B → Mistral-7B.
Same K-FAC config, same protocol. Expected:
  - E8+K-FAC training:   ~600-900 PPL  (7B has more capacity, better PPL)
  - .e8 inference PPL:   < training PPL (exact lattice still beats noisy FP32)
  - .e8 file size:       ~8-9 GB  (vs ~28 GB FP32 = ~3.4× compression)

Memory budget on A40 (46 GB):
  - Mistral-7B FP32 weights: ~28 GB
  - K-FAC A/G buffers (attn only): q_proj/o_proj A = [4096×4096] × 32 layers = 2 GB
  - AdamW states: ~1 GB (only E8 w_latent params trained)
  - Total: ~31 GB — fits A40 with margin

Usage:
    python3 pipeline/launch_7b_validation.py
"""

import json
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

# ─── Config ──────────────────────────────────────────────────────────────────

POD_NAME    = "e8-7b-validation"
GPU_ID      = "NVIDIA A40"
CLOUD_TYPE  = "SECURE"
DOCKER_IMG  = "runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04"
DISK_GB     = 80    # 28 GB model + 8 GB .e8 + working room
VOLUME_GB   = 80
WORKDIR     = "/workspace/e8_7b"
SSH_KEY     = os.path.expanduser("~/.ssh/id_ed25519")

# 7B model — ungated, widely benchmarked, matches Llama architecture
MODEL_7B    = "mistralai/Mistral-7B-v0.1"

REPO_ROOT   = Path(__file__).parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"


# ─── RunPod API (same helpers as launch_e8_format.py) ────────────────────────

def load_api_key():
    key = os.environ.get("RUNPOD_API_KEY")
    if not key:
        env_path = Path(__file__).parent / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("RUNPOD_API_KEY="):
                    key = line.split("=", 1)[1].strip()
    if not key:
        print("ERROR: RUNPOD_API_KEY not set. Add to pipeline/.env or environment.")
        sys.exit(1)
    return key


def load_hf_token():
    token = os.environ.get("HF_TOKEN", "")
    if not token:
        env_path = Path(__file__).parent / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("HF_TOKEN="):
                    token = line.split("=", 1)[1].strip()
    return token


def graphql(api_key, query, variables=None):
    url = "https://api.runpod.io/graphql?api_key=" + api_key
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "BonfyreFPQ/1.0"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def launch_pod(api_key):
    query = """
    mutation CreatePod($input: PodFindAndDeployOnDemandInput!) {
        podFindAndDeployOnDemand(input: $input) {
            id name desiredStatus
            machine { gpuDisplayName podHostId }
        }
    }
    """
    variables = {
        "input": {
            "name": POD_NAME,
            "imageName": DOCKER_IMG,
            "gpuTypeId": GPU_ID,
            "cloudType": CLOUD_TYPE,
            "gpuCount": 1,
            "volumeInGb": VOLUME_GB,
            "containerDiskInGb": DISK_GB,
            "minVcpuCount": 4,
            "minMemoryInGb": 32,
            "dockerArgs": "",
            "ports": "22/tcp",
            "volumeMountPath": "/workspace",
            "startSsh": True,
        }
    }
    result = graphql(api_key, query, variables)
    if "errors" in result:
        print(f"Launch error: {result['errors']}")
        sys.exit(1)
    pod = result["data"]["podFindAndDeployOnDemand"]
    return pod["id"]


def get_pod_ssh(api_key, pod_id):
    query = """
    query Pod($input: PodFilter!) {
        pod(input: $input) {
            id desiredStatus
            runtime { ports { ip isIpPublic privatePort publicPort type } }
        }
    }
    """
    for attempt in range(60):
        result = graphql(api_key, query, {"input": {"podId": pod_id}})
        pod = result.get("data", {}).get("pod", {})
        for p in (pod.get("runtime") or {}).get("ports") or []:
            if p.get("privatePort") == 22 and p.get("publicPort") and p.get("ip"):
                return p["ip"], p["publicPort"]
        print(f"  Waiting for pod... ({attempt+1}/60)")
        time.sleep(10)
    print("Timed out waiting for pod SSH.")
    sys.exit(1)


def terminate_pod(api_key, pod_id):
    query = """
    mutation TerminatePod($input: PodTerminateInput!) {
        podTerminate(input: $input)
    }
    """
    graphql(api_key, query, {"input": {"podId": pod_id}})
    print(f"  Pod {pod_id} terminated.")


# ─── SSH helpers ─────────────────────────────────────────────────────────────

def _ssh_opts(port):
    return ["-i", SSH_KEY, "-p", str(port),
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=10",
            "-o", "ServerAliveInterval=60"]


def ssh_run(ip, port, cmd, check=True):
    full = ["ssh", *_ssh_opts(port), f"root@{ip}", cmd]
    result = subprocess.run(full, check=check)
    return result.returncode


def scp_to(ip, port, local, remote):
    full = ["scp", "-i", SSH_KEY, "-P", str(port),
            "-o", "StrictHostKeyChecking=no",
            str(local), f"root@{ip}:{remote}"]
    subprocess.run(full, check=True)


def wait_ssh_ready(ip, port, retries=20):
    import socket
    for i in range(retries):
        try:
            s = socket.create_connection((ip, port), timeout=5)
            s.close()
            time.sleep(2)
            return
        except OSError:
            print(f"  SSH not ready ({i+1}/{retries})...")
            time.sleep(5)
    print("SSH never became reachable.")
    sys.exit(1)


# ─── Remote script ────────────────────────────────────────────────────────────

def make_remote_script(hf_token: str) -> str:
    """
    All-in-one bash script that runs on the pod:
      install → download 7B model → train E8+K-FAC → encode .e8 → bench
    """
    hf_login = f"hf auth login --token {hf_token} 2>/dev/null || true" if hf_token else \
               "echo 'No HF token — downloading publicly'"
    return f"""#!/bin/bash
set -euo pipefail
cd {WORKDIR}
echo "═══════════════════════════════════════════════════════════════"
echo " E8-Native 7B Validation"
echo "═══════════════════════════════════════════════════════════════"

# ── Deps ──────────────────────────────────────────────────────────────────
echo ""
echo "STEP 1: Installing dependencies..."
pip install -q transformers datasets accelerate safetensors 2>&1 | tail -3
{hf_login}

# ── Train E8 + K-FAC on Mistral 7B ───────────────────────────────────────
echo ""
echo "STEP 2: Training E8 on {MODEL_7B} (attn, 200 steps, no K-FAC)..."
echo "  K-FAC skipped — already proven at 1.1B scale"
echo "  This run answers: does E8 converge at 7B? E8 vs BitNet at scale?"
echo "  Expected runtime: ~25-35 min on A40"
PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True \\
python3 train_e8_native.py \\
    --model {MODEL_7B} \\
    --device cuda \\
    --steps 200 \\
    --batch-size 2 \\
    --seq-len 256 \\
    --replace-layers attn \\
    --bf16 \\
    --compare-bitnet \\
    --save checkpoints/e8_7b_200steps.pt
echo ""
echo "Checkpoint saved."
ls -lh checkpoints/e8_7b_kfac_200steps.pt

# ── Encode to .e8 ─────────────────────────────────────────────────────────
echo ""
echo "STEP 3: Encoding checkpoint → .e8 format..."
python3 e8_format.py encode \\
    --checkpoint checkpoints/e8_7b_200steps.pt \
    --model {MODEL_7B} \\
    --out model_7b.e8
ls -lh model_7b.e8

# ── Info ──────────────────────────────────────────────────────────────────
echo ""
echo "STEP 4: .e8 file stats..."
python3 e8_format.py info model_7b.e8

# ── Bench ─────────────────────────────────────────────────────────────────
echo ""
echo "STEP 5: PPL benchmark from .e8 (no .pt checkpoint needed)..."
python3 e8_format.py bench \\
    --file model_7b.e8 \\
    --device cuda \\
    --n-batches 40 \\
    --seq-len 256 \\
    --batch-size 2

# ── Infer ─────────────────────────────────────────────────────────────────
echo ""
echo "STEP 6: Text generation from .e8..."
python3 e8_format.py infer \\
    --file model_7b.e8 \\
    --device cuda \\
    --prompt "The E8 lattice is the unique densest sphere packing in 8 dimensions because" \\
    --max-new-tokens 100

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo " ALL PHASES COMPLETE"
echo "═══════════════════════════════════════════════════════════════"
"""


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    api_key = load_api_key()
    hf_token = load_hf_token()

    print("═══════════════════════════════════════════════════════════════")
    print(f" E8-Native 7B Scale Validation")
    print("═══════════════════════════════════════════════════════════════")
    print(f"  GPU:    {GPU_ID}")
    print(f"  Model:  {MODEL_7B}")
    print(f"  Config: attn-only K-FAC, 200 steps, compare-bitnet")
    print(f"  Est runtime: ~35-45 min")
    print()

    # ── Launch pod ────────────────────────────────────────────────────────────
    print("PHASE 0: Launching pod...")
    pod_id = launch_pod(api_key)
    print(f"  Pod ID: {pod_id}")

    try:
        print("  Waiting for SSH...")
        ip, port = get_pod_ssh(api_key, pod_id)
        print(f"  SSH ready: {ip}:{port}")
        wait_ssh_ready(ip, port)

        # ── Upload scripts ────────────────────────────────────────────────────
        print()
        print("PHASE 1: Uploading scripts...")
        ssh_run(ip, port, f"mkdir -p {WORKDIR}/checkpoints")
        scp_to(ip, port, SCRIPTS_DIR / "train_e8_native.py",
               f"{WORKDIR}/train_e8_native.py")
        scp_to(ip, port, SCRIPTS_DIR / "e8_format.py",
               f"{WORKDIR}/e8_format.py")
        print("  train_e8_native.py uploaded")
        print("  e8_format.py uploaded")

        # Upload run script and execute
        remote_script = make_remote_script(hf_token)
        # Write to temp file, scp, then run
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh",
                                          delete=False) as tmp:
            tmp.write(remote_script)
            tmp_path = tmp.name
        scp_to(ip, port, tmp_path, f"{WORKDIR}/run_all.sh")
        os.unlink(tmp_path)

        # ── Execute ───────────────────────────────────────────────────────────
        print()
        print("PHASE 2: Running all steps on pod (streaming output)...")
        print("─" * 60)
        ssh_run(ip, port, f"bash {WORKDIR}/run_all.sh 2>&1")
        print("─" * 60)

    finally:
        print()
        print("Terminating pod...")
        terminate_pod(api_key, pod_id)
        print("Done — pod terminated.")


if __name__ == "__main__":
    main()
