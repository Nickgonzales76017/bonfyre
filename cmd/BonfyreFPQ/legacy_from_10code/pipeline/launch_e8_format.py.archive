#!/usr/bin/env python3
"""
launch_e8_format.py — Fire-and-forget .e8 format validation.

Launches an A40 RunPod, uploads the checkpoint + e8_format.py,
runs all 5 phases (encode → info → bench → infer → decode),
downloads the result, then terminates the pod.

Usage:
    python3 pipeline/launch_e8_format.py

Reads RUNPOD_API_KEY from pipeline/.env or environment.
SSH key: ~/.ssh/id_ed25519 (same as deploy.py).
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

POD_NAME    = "e8-format-validation"
GPU_ID      = "NVIDIA A40"
CLOUD_TYPE  = "SECURE"
DOCKER_IMG  = "runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04"
DISK_GB     = 50
VOLUME_GB   = 50
WORKDIR     = "/workspace/e8format"
SSH_KEY     = os.path.expanduser("~/.ssh/id_ed25519")

REPO_ROOT    = Path(__file__).parent.parent
SCRIPTS_DIR  = REPO_ROOT / "scripts"
CKPT_DIR     = REPO_ROOT / "checkpoints"

# Prefer K-FAC checkpoint (best PPL), fall back to plain E8
for _ckpt_name in ["e8_kfac_attn_200steps.pt", "e8_native_200steps.pt",
                    "e8_scratch_500steps.pt"]:
    _ckpt = CKPT_DIR / _ckpt_name
    if _ckpt.exists():
        CHECKPOINT = _ckpt
        CHECKPOINT_NAME = _ckpt_name
        break
else:
    print("ERROR: No checkpoint found in scripts/. Expected e8_kfac_attn_200steps.pt")
    sys.exit(1)


# ─── RunPod API ───────────────────────────────────────────────────────────────

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
    """Poll until SSH port is available. Returns (ip, port)."""
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
        runtime = pod.get("runtime") or {}
        for p in (runtime.get("ports") or []):
            if p.get("privatePort") == 22 and p.get("publicPort") and p.get("ip"):
                return p["ip"], p["publicPort"]
        print(f"  Waiting for pod to come up... ({attempt+1}/60)")
        time.sleep(10)
    print("Timed out waiting for pod SSH port.")
    sys.exit(1)


def terminate_pod(api_key, pod_id):
    query = """
    mutation TerminatePod($input: PodTerminateInput!) {
        podTerminate(input: $input)
    }
    """
    graphql(api_key, query, {"input": {"podId": pod_id}})
    print(f"  Pod {pod_id} terminated.")


# ─── SSH / SCP helpers ────────────────────────────────────────────────────────

def _ssh_opts(port):
    return ["-i", SSH_KEY, "-p", str(port),
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=10",
            "-o", "ServerAliveInterval=30"]


def ssh_run(ip, port, cmd, check=True):
    """Run a remote command, stream output live."""
    full = ["ssh", *_ssh_opts(port), f"root@{ip}", cmd]
    result = subprocess.run(full, check=check)
    return result.returncode


def scp_to(ip, port, local, remote):
    """Copy local → remote."""
    full = ["scp", "-i", SSH_KEY, "-P", str(port),
            "-o", "StrictHostKeyChecking=no",
            str(local), f"root@{ip}:{remote}"]
    subprocess.run(full, check=True)


def scp_from(ip, port, remote, local):
    """Copy remote → local."""
    full = ["scp", "-i", SSH_KEY, "-P", str(port),
            "-o", "StrictHostKeyChecking=no",
            f"root@{ip}:{remote}", str(local)]
    subprocess.run(full, check=True)


def wait_ssh_ready(ip, port, retries=20):
    """Retry SSH echo until the pod's sshd is alive."""
    import socket
    for i in range(retries):
        try:
            s = socket.create_connection((ip, port), timeout=5)
            s.close()
            time.sleep(2)   # sshd handshake start-up
            return
        except OSError:
            print(f"  SSH not ready yet ({i+1}/{retries})...")
            time.sleep(5)
    print("SSH never became reachable.")
    sys.exit(1)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    api_key = load_api_key()
    ckpt_size = CHECKPOINT.stat().st_size / 1e9

    print("═══════════════════════════════════════════════════════════════")
    print(f" .e8 Format Validation — Fire and Forget")
    print("═══════════════════════════════════════════════════════════════")
    print(f"  GPU:        {GPU_ID}")
    print(f"  Checkpoint: {CHECKPOINT_NAME}  ({ckpt_size:.2f} GB)")
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

        # ── Upload ────────────────────────────────────────────────────────────
        print()
        print("PHASE 1: Uploading files...")
        ssh_run(ip, port, f"mkdir -p {WORKDIR}")
        scp_to(ip, port, SCRIPTS_DIR / "e8_format.py", f"{WORKDIR}/e8_format.py")
        print(f"  e8_format.py uploaded")
        print(f"  Uploading checkpoint ({ckpt_size:.2f} GB)...")
        scp_to(ip, port, CHECKPOINT, f"{WORKDIR}/{CHECKPOINT_NAME}")
        print(f"  Checkpoint uploaded")

        # ── Deps ──────────────────────────────────────────────────────────────
        print()
        print("PHASE 2: Installing dependencies...")
        ssh_run(ip, port,
            "pip install -q transformers datasets accelerate safetensors 2>&1 | tail -3"
        )

        # ── Encode ────────────────────────────────────────────────────────────
        print()
        print("PHASE 3: Encoding checkpoint → .e8...")
        ssh_run(ip, port, f"""
cd {WORKDIR}
python3 e8_format.py encode \\
    --checkpoint {CHECKPOINT_NAME} \\
    --model TinyLlama/TinyLlama-1.1B-Chat-v1.0 \\
    --out model.e8
ls -lh model.e8
""")

        # ── Info ──────────────────────────────────────────────────────────────
        print()
        print("PHASE 4: .e8 file info...")
        ssh_run(ip, port, f"cd {WORKDIR} && python3 e8_format.py info model.e8")

        # ── Bench ─────────────────────────────────────────────────────────────
        print()
        print("PHASE 5: PPL benchmark from .e8...")
        ssh_run(ip, port, f"""
cd {WORKDIR}
python3 e8_format.py bench \\
    --file model.e8 \\
    --device cuda \\
    --n-batches 40 \\
    --seq-len 256 \\
    --batch-size 4
""")

        # ── Infer ─────────────────────────────────────────────────────────────
        print()
        print("PHASE 6: Text generation from .e8...")
        ssh_run(ip, port, f"""
cd {WORKDIR}
python3 e8_format.py infer \\
    --file model.e8 \\
    --device cuda \\
    --prompt 'The E8 lattice achieves optimal sphere packing because' \\
    --max-new-tokens 80
""")

        # ── Download result ───────────────────────────────────────────────────
        print()
        print("Downloading model.e8...")
        local_e8 = SCRIPTS_DIR / "model_kfac.e8"
        scp_from(ip, port, f"{WORKDIR}/model.e8", local_e8)
        e8_size = local_e8.stat().st_size / 1e9
        print(f"  Saved: {local_e8}  ({e8_size:.2f} GB)")
        print()
        print("═══════════════════════════════════════════════════════════════")
        print(" RESULTS")
        print("═══════════════════════════════════════════════════════════════")
        print(f"  Checkpoint .pt : {ckpt_size:.2f} GB")
        print(f"  .e8 file       : {e8_size:.2f} GB")
        print(f"  Compression    : {ckpt_size / e8_size:.2f}×  (checkpoint vs .e8)")
        print()
        print(f"  Local .e8: {local_e8}")
        print()

    finally:
        print("Terminating pod...")
        terminate_pod(api_key, pod_id)
        print("Done — pod terminated, no ongoing cost.")


if __name__ == "__main__":
    main()
