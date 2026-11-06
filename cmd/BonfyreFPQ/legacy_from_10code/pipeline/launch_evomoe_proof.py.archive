#!/usr/bin/env python3
"""
launch_evomoe_proof.py — Fire-and-forget pod launcher for EvoMoE proof run.

Proof protocol:
  Model:  TinyLlama/TinyLlama-1.1B-Chat-v1.0  (fast, already proven with E8)
  GPU:    A40 (44 GB) — overkill for 1.1B but ensures no OOM
  Target: Dense E8Direct → MoE-2 → show MoE PPL < dense PPL

  Phase 1:  K-FAC split oracle (50 batches accumulation)
  Phase 2:  Dense E8Direct training (200 steps)  → dense_ppl
  Phase 3:  EvoMoE expansion (top-16 attn layers → 2 experts each)
  Phase 4:  Router fine-tuning (200 steps)       → moe_ppl
  Result:   Print comparison table

Success criterion:
  moe_ppl < dense_ppl  (MoE with same training budget outperforms dense)

Memory budget on A40:
  TinyLlama 1.1B bf16:     ~2.2 GB
  E8Direct error (FP16):   ~1.1 GB  (half of w_latent FP32)
  AdamW on error:          ~2.2 GB  (2× FP16 = 1.1 GB m1+m2 in FP32)
  MoE-2 (2× expert):       ~2.2 GB expert params + router overhead (~tiny)
  Total:                   ~8 GB peak — very comfortable on A40

Usage:
    python3 pipeline/launch_evomoe_proof.py
"""

import json
import os
import subprocess
import sys
import time
import tempfile
import urllib.request
import urllib.error
from pathlib import Path

# ─── Config ──────────────────────────────────────────────────────────────────

POD_NAME    = "evomoe-proof"
GPU_ID      = "NVIDIA A100 80GB PCIe"
CLOUD_TYPE  = "SECURE"
DOCKER_IMG  = "runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04"
DISK_GB     = 40
VOLUME_GB   = 40
WORKDIR     = "/workspace/evomoe"
SSH_KEY     = os.path.expanduser("~/.ssh/id_ed25519")

MODEL       = "TinyLlama/TinyLlama-1.1B-Chat-v1.0"

REPO_ROOT   = Path(__file__).parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"


# ─── RunPod API helpers (copied from launch_7b_validation.py) ─────────────────

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
            "minMemoryInGb": 16,
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
    return result["data"]["podFindAndDeployOnDemand"]["id"]


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
    hf_login = (f"hf auth login --token {hf_token} 2>/dev/null || true"
                if hf_token else "echo 'No HF token'")
    return f"""#!/bin/bash
set -euo pipefail
cd {WORKDIR}

echo "═══════════════════════════════════════════════════════════════"
echo " EvoMoE Proof Run — TinyLlama 1.1B"
echo " E8Direct dense → MoE-2"
echo "═══════════════════════════════════════════════════════════════"

# ── Deps ─────────────────────────────────────────────────────────────────────
echo ""
echo "STEP 1: Installing dependencies..."
pip install -q transformers datasets accelerate safetensors scikit-learn 2>&1 | tail -3
{hf_login}

# ── Phase 1: K-FAC split oracle ───────────────────────────────────────────────
echo ""
echo "STEP 2: K-FAC split oracle — accumulating A-factors (50 batches)..."
python3 evomoe_split.py \\
    --model {MODEL} \\
    --device cuda \\
    --replace-layers attn \\
    --n-accumulate 50 \\
    --n-experts 2 \\
    --top-n-layers 16 \\
    --out-map split_map.json
echo ""
echo "Split map written to split_map.json"
cat split_map.json | python3 -c "
import sys, json
m = json.load(sys.stdin)
split_layers = [(k, v) for k, v in m['layers'].items() if v.get('split')]
print(f'  Layers to split: {{len(split_layers)}}')
for k, v in sorted(split_layers, key=lambda x: -x[1].get('split_score', 0))[:5]:
    print(f'    {{k}}  score={{v.get(\\\"split_score\\\", 0):.4f}}')
"

# ── Phase 2+3+4: Full EvoMoE pipeline ────────────────────────────────────────
echo ""
echo "STEP 3: EvoMoE full pipeline..."
echo "  Phase 2: Dense E8Direct training (200 steps)"
echo "  Phase 3: EvoMoE expand (top-16 attn layers → 2 experts)"
echo "  Phase 4: Router fine-tuning (200 steps)"
echo ""
PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True \\
python3 train_evomoe.py \\
    --model {MODEL} \\
    --device cuda \\
    --bf16 \\
    --dense-steps 200 \\
    --router-steps 200 \\
    --n-experts 2 \\
    --split-map split_map.json \\
    --replace-layers attn \\
    --top-n-layers 16 \\
    --lr 1e-4 \\
    --router-lr 3e-4 \\
    --log-every 20 \\
    --save checkpoints/evomoe_proof.pt

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo " EVOMOE PROOF COMPLETE"
echo "═══════════════════════════════════════════════════════════════"
ls -lh checkpoints/evomoe_proof.pt
"""


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    api_key = load_api_key()
    hf_token = load_hf_token()

    print("═══════════════════════════════════════════════════════════════")
    print(" EvoMoE Proof — TinyLlama 1.1B Dense→MoE-2")
    print("═══════════════════════════════════════════════════════════════")
    print(f"  GPU:       {GPU_ID}")
    print(f"  Model:     {MODEL}")
    print(f"  Protocol:  K-FAC split → E8Direct dense 200 steps → MoE-2 → router 200 steps")
    print(f"  Target:    MoE PPL < Dense PPL (same training budget)")
    print(f"  Est time:  ~25-35 min")
    print()

    print("PHASE 0: Launching pod...")
    pod_id = launch_pod(api_key)
    print(f"  Pod ID: {pod_id}")

    try:
        print("  Waiting for SSH...")
        ip, port = get_pod_ssh(api_key, pod_id)
        print(f"  SSH ready: {ip}:{port}")
        wait_ssh_ready(ip, port)

        print("\nPHASE 1: Uploading scripts...")
        ssh_run(ip, port, f"mkdir -p {WORKDIR}/checkpoints")

        for script in ["train_e8_native.py", "evomoe_split.py", "train_evomoe.py"]:
            scp_to(ip, port, SCRIPTS_DIR / script, f"{WORKDIR}/{script}")
            print(f"  {script} uploaded")

        remote_script = make_remote_script(hf_token)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as tmp:
            tmp.write(remote_script)
            tmp_path = tmp.name
        scp_to(ip, port, tmp_path, f"{WORKDIR}/run_all.sh")
        os.unlink(tmp_path)

        print("\nPHASE 2: Running proof (streaming output)...")
        print("─" * 64)
        ssh_run(ip, port, f"bash {WORKDIR}/run_all.sh 2>&1")
        print("─" * 64)

    finally:
        print("\nTerminating pod...")
        terminate_pod(api_key, pod_id)
        print("Done — pod terminated.")


if __name__ == "__main__":
    main()
