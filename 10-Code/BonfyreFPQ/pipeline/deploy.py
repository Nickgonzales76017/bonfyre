#!/usr/bin/env python3
"""
RunPod GPU pod deployment for BonfyreFPQ compression pipeline.

Usage:
    python deploy.py launch --gpu H200       # single H200 for monster models
    python deploy.py launch --gpu RTX6000    # cheaper worker
    python deploy.py launch --gpu H200 --name glm-heavy
    python deploy.py list                    # show running pods
    python deploy.py stop <pod_id>           # stop a pod
    python deploy.py stop-all                # stop everything
    python deploy.py ssh <pod_id>            # print SSH command
    python deploy.py wan-sli                 # launch pod + run Wan SLI benchmark
"""

import argparse
import json
import os
import sys
import urllib.request
import urllib.error

ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")

def load_env():
    env = {}
    if os.path.exists(ENV_PATH):
        with open(ENV_PATH) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()
    return env

def get_api_key():
    key = os.environ.get("RUNPOD_API_KEY") or load_env().get("RUNPOD_API_KEY")
    if not key:
        print("Error: RUNPOD_API_KEY not set. Add it to pipeline/.env or environment.")
        sys.exit(1)
    return key

# GPU presets: (runpod gpu id, cloud type, description)
GPU_PRESETS = {
    "H200":    ("NVIDIA H200",         "SECURE", "141GB VRAM, 276GB RAM — heavy lift"),
    "H100":    ("NVIDIA H100 80GB HBM3","SECURE","80GB VRAM — good mid-tier"),
    "A100":    ("NVIDIA A100 80GB PCIe","SECURE", "80GB VRAM — value option"),
    "RTX6000": ("NVIDIA RTX 6000 Ada Generation", "SECURE", "48GB VRAM — cheap worker"),
    "RTX6000PRO": ("NVIDIA RTX PRO 6000","SECURE","96GB VRAM — best value"),
}

SETUP_SCRIPT = r'''#!/bin/bash
set -e

echo "=== BonfyreFPQ Pod Setup ==="
apt-get update -qq
apt-get install -y -qq git build-essential libopenblas-dev liblapack-dev wget curl jq

# Clone bonfyre repo
if [ ! -d /workspace/bonfyre ]; then
    git clone https://github.com/Nickgonzales76017/bonfyre-oss.git /workspace/bonfyre
fi
cd /workspace/bonfyre/10-Code/BonfyreFPQ
git -C /workspace/bonfyre pull --ff-only 2>/dev/null || true

# Build libfpq.so (shared library for Python bridge) and bonfyre-fpq binary
cat > Makefile.linux << 'MKEOF'
CC      := gcc
CFLAGS  := -D_GNU_SOURCE -O3 -march=native -Wall -Wextra -std=c11 -Iinclude -fopenmp -DHAVE_OPENBLAS -fPIC
LDFLAGS := -lm -lopenblas -llapack -lgomp
SRC_DIR := src
BUILD   := build

SRCS := $(SRC_DIR)/fwht.c \
        $(SRC_DIR)/polar.c \
        $(SRC_DIR)/seed.c \
        $(SRC_DIR)/qjl.c \
        $(SRC_DIR)/debruijn.c \
        $(SRC_DIR)/fpq_codec.c \
        $(SRC_DIR)/ggml_reader.c \
        $(SRC_DIR)/safetensors_reader.c \
        $(SRC_DIR)/serialize.c \
        $(SRC_DIR)/v4_optimizations.c \
        $(SRC_DIR)/weight_algebra.c \
        $(SRC_DIR)/fpq_native.c \
        $(SRC_DIR)/fpqx_ops.c \
        $(SRC_DIR)/fpq_neon.c \
        $(SRC_DIR)/libfpq.c \
        $(SRC_DIR)/fpq_cli.c

OBJS := $(patsubst $(SRC_DIR)/%.c,$(BUILD)/%.o,$(SRCS))
BIN  := fpq
LIBFPQ := libfpq.so

.PHONY: all clean
all: $(BIN) $(LIBFPQ)
$(BIN): $(OBJS) $(SRC_DIR)/fpq_cli.c
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LDFLAGS)
$(LIBFPQ): $(OBJS)
	$(CC) $(CFLAGS) -shared -o $@ $(OBJS) $(LDFLAGS)
$(BUILD)/%.o: $(SRC_DIR)/%.c include/fpq.h | $(BUILD)
	$(CC) $(CFLAGS) -c -o $@ $<
$(BUILD):
	mkdir -p $(BUILD)
clean:
	rm -rf $(BUILD) $(BIN) $(LIBFPQ)
MKEOF

make -f Makefile.linux clean
make -f Makefile.linux -j$(nproc)
echo "Built: $(ls -lh fpq libfpq.so)"

# Install Python deps
pip install -q torch diffusers transformers safetensors numpy huggingface_hub

# Set up workspace directories
mkdir -p /workspace/jobs/pending /workspace/jobs/running /workspace/jobs/done
mkdir -p /workspace/models/wan-orig /workspace/models/wan-fpq /workspace/logs

echo "=== Setup complete. fpq binary and libfpq.so ready at $(pwd) ==="
echo "=== Run the Wan SLI benchmark: ==="
echo "  python3 scripts/test_e2e_wan_sli.py --safetensors /workspace/models/wan-orig/diffusion_pytorch_model.safetensors --fpq /workspace/models/wan-fpq/*.fpq --sweep --out /workspace/results/wan_sli.json"
'''

WAN_SLI_SCRIPT = r'''#!/bin/bash
# wan_sli_pod.sh — Full Wan SLI benchmark on RunPod
# Run after SETUP_SCRIPT completes.
set -e

cd /workspace/bonfyre/10-Code/BonfyreFPQ
export LIBFPQ_PATH="$(pwd)/libfpq.so"

echo "=== Downloading Wan2.1-T2V-1.3B original weights ==="
huggingface-cli download Wan-AI/Wan2.1-T2V-1.3B \
    diffusion_pytorch_model.safetensors \
    --local-dir /workspace/models/wan-orig \
    --quiet

echo "=== Downloading Wan2.1-T2V-1.3B v12 .fpq ==="
huggingface-cli download NICKO/wan2.1-t2v-1.3b-v12-fpq \
    --local-dir /workspace/models/wan-fpq \
    --quiet

FPQ_FILE=$(find /workspace/models/wan-fpq -name "*.fpq" | head -1)
if [ -z "$FPQ_FILE" ]; then
    echo "ERROR: No .fpq file found in /workspace/models/wan-fpq"
    ls /workspace/models/wan-fpq/
    exit 1
fi

echo "=== .fpq: $FPQ_FILE ==="
mkdir -p /workspace/results

echo "=== Running Wan SLI end-to-end benchmark ==="
python3 scripts/test_e2e_wan_sli.py \
    --safetensors /workspace/models/wan-orig/diffusion_pytorch_model.safetensors \
    --fpq "$FPQ_FILE" \
    --sweep \
    --device cuda \
    --out /workspace/results/wan_sli.json \
    2>&1 | tee /workspace/logs/wan_sli.log

echo "=== Done. Results at /workspace/results/wan_sli.json ==="
cat /workspace/results/wan_sli.json | python3 -c "
import json,sys
r=json.load(sys.stdin)
m=r['main_metrics']
print(f\"  Cosine:  {m['cosine']:.8f}\")
print(f\"  PSNR:    {m['psnr_db']:.2f} dB\")
print(f\"  SLI layers: {r['patched_layers']}\")
if r['sweep']:
    cosines=[s['cosine'] for s in r['sweep']]
    print(f\"  Sweep range: {min(cosines):.8f} – {max(cosines):.8f}\")
"
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ""
        print(f"API error {e.code}: {body}")
        sys.exit(1)

def cmd_launch(args):
    api_key = get_api_key()
    preset = args.gpu.upper()
    if preset not in GPU_PRESETS:
        print(f"Unknown GPU preset: {args.gpu}")
        print(f"Available: {', '.join(GPU_PRESETS.keys())}")
        sys.exit(1)

    gpu_id, cloud_type, desc = GPU_PRESETS[preset]
    name = args.name or f"fpq-{preset.lower()}"

    print(f"Launching: {name}")
    print(f"  GPU: {gpu_id} ({desc})")
    print(f"  Cloud: {cloud_type}")
    print(f"  Disk: {args.disk}GB container + {args.volume}GB volume")

    # Docker image with CUDA + Python pre-installed
    docker_image = args.image or "runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04"

    query = """
    mutation CreatePod($input: PodFindAndDeployOnDemandInput!) {
        podFindAndDeployOnDemand(input: $input) {
            id
            name
            desiredStatus
            imageName
            machine { gpuDisplayName podHostId }
        }
    }
    """
    variables = {
        "input": {
            "name": name,
            "imageName": docker_image,
            "gpuTypeId": gpu_id,
            "cloudType": cloud_type,
            "gpuCount": 1,
            "volumeInGb": args.volume,
            "containerDiskInGb": args.disk,
            "minVcpuCount": 4,
            "minMemoryInGb": 32,
            "dockerArgs": "",
            "ports": "22/tcp,8888/http",
            "volumeMountPath": "/workspace",
            "startSsh": True,
        }
    }

    result = graphql(api_key, query, variables)
    if "errors" in result:
        print(f"Error: {result['errors']}")
        sys.exit(1)

    pod = result["data"]["podFindAndDeployOnDemand"]
    print(f"\nPod created!")
    print(f"  ID:     {pod['id']}")
    print(f"  Name:   {pod['name']}")
    print(f"  Status: {pod['desiredStatus']}")
    print(f"  GPU:    {pod['machine']['gpuDisplayName']}")
    print(f"\nNext steps:")
    print(f"  1. Wait for pod to be RUNNING:  python deploy.py list")
    print(f"  2. Get SSH command:              python deploy.py ssh {pod['id']}")
    print(f"  3. On the pod, run setup:        bash /workspace/bonfyre/10-Code/BonfyreFPQ/pipeline/pod_setup.sh")
    print(f"  4. Then start worker:            bash /workspace/bonfyre/10-Code/BonfyreFPQ/pipeline/worker.sh")

    # Save setup script to be scp'd or run via SSH
    setup_path = os.path.join(os.path.dirname(__file__), "pod_setup.sh")
    print(f"\n  Setup script saved to: {setup_path}")

    return pod["id"]

def cmd_list(args):
    api_key = get_api_key()
    query = """
    query Pods {
        myself {
            pods {
                id name desiredStatus
                runtime { uptimeInSeconds gpus { gpuUtilPercent memoryUtilPercent } }
                machine { gpuDisplayName podHostId }
                costPerHr
            }
        }
    }
    """
    result = graphql(api_key, query)
    pods = result.get("data", {}).get("myself", {}).get("pods", [])
    if not pods:
        print("No pods running.")
        return

    print(f"{'ID':<16} {'Name':<20} {'Status':<12} {'GPU':<30} {'$/hr':<8} {'Uptime'}")
    print("-" * 100)
    for p in pods:
        uptime = ""
        if p.get("runtime") and p["runtime"].get("uptimeInSeconds"):
            secs = p["runtime"]["uptimeInSeconds"]
            uptime = f"{secs//3600}h{(secs%3600)//60}m"
        gpu = p.get("machine", {}).get("gpuDisplayName", "?")
        cost = p.get("costPerHr", "?")
        print(f"{p['id']:<16} {p['name']:<20} {p['desiredStatus']:<12} {gpu:<30} ${cost:<7} {uptime}")

def cmd_stop(args):
    api_key = get_api_key()
    pod_id = args.pod_id
    query = """
    mutation StopPod($input: PodStopInput!) {
        podStop(input: $input) { id desiredStatus }
    }
    """
    result = graphql(api_key, query, {"input": {"podId": pod_id}})
    if "errors" in result:
        print(f"Error: {result['errors']}")
    else:
        print(f"Pod {pod_id} stopping.")

def cmd_terminate(args):
    api_key = get_api_key()
    pod_id = args.pod_id
    query = """
    mutation TerminatePod($input: PodTerminateInput!) {
        podTerminate(input: $input)
    }
    """
    result = graphql(api_key, query, {"input": {"podId": pod_id}})
    if "errors" in result:
        print(f"Error: {result['errors']}")
    else:
        print(f"Pod {pod_id} TERMINATED. Storage released.")

def cmd_stop_all(args):
    api_key = get_api_key()
    query = """
    query Pods { myself { pods { id name desiredStatus } } }
    """
    result = graphql(api_key, query)
    pods = result.get("data", {}).get("myself", {}).get("pods", [])
    for p in pods:
        if p["desiredStatus"] == "RUNNING":
            print(f"Stopping {p['id']} ({p['name']})...")
            cmd_stop(argparse.Namespace(pod_id=p["id"]))

def cmd_ssh(args):
    api_key = get_api_key()
    query = """
    query Pod($input: PodFilter!) {
        pod(input: $input) {
            id name desiredStatus
            runtime { ports { ip isIpPublic privatePort publicPort type } }
        }
    }
    """
    result = graphql(api_key, query, {"input": {"podId": args.pod_id}})
    pod = result.get("data", {}).get("pod")
    if not pod:
        print(f"Pod {args.pod_id} not found.")
        sys.exit(1)

    runtime = pod.get("runtime") or {}
    ports = runtime.get("ports") or []
    ssh_port = None
    ssh_ip = None
    for p in ports:
        if p.get("privatePort") == 22:
            ssh_port = p.get("publicPort")
            ssh_ip = p.get("ip")
            break

    if ssh_port and ssh_ip:
        print(f"ssh root@{ssh_ip} -p {ssh_port} -i ~/.ssh/id_ed25519")
        print(f"\nOr for first-time setup:")
        print(f"scp -P {ssh_port} pipeline/pod_setup.sh root@{ssh_ip}:/workspace/")
        print(f"ssh root@{ssh_ip} -p {ssh_port} 'bash /workspace/pod_setup.sh'")
    else:
        print(f"Pod {args.pod_id} ({pod['name']}) — status: {pod['desiredStatus']}")
        print("SSH port not available yet. Pod may still be starting.")

def cmd_submit(args):
    """Submit a compression job by writing a job manifest."""
    job = {
        "model": args.model,
        "bits": args.bits,
        "format": args.format,
        "priority": args.priority,
    }
    job_name = f"{args.priority:02d}_{args.model.replace('/', '__')}"
    jobs_dir = os.path.join(os.path.dirname(__file__), "jobs", "pending")
    os.makedirs(jobs_dir, exist_ok=True)
    job_path = os.path.join(jobs_dir, f"{job_name}.json")
    with open(job_path, "w") as f:
        json.dump(job, f, indent=2)
    print(f"Job created: {job_path}")
    print(json.dumps(job, indent=2))
    print(f"\nTo deploy to pod:")
    print(f"  scp -P <port> {job_path} root@<ip>:/workspace/jobs/pending/")


def cmd_wan_sli(args):
    """Launch a pod and print commands to run the Wan SLI benchmark."""
    api_key = get_api_key()

    # Use A4000 or RTX6000 (enough VRAM, cheap, ~5.3 GB weights + overhead)
    gpu_preset = getattr(args, "gpu", "RTX6000")
    preset = gpu_preset.upper()
    if preset not in GPU_PRESETS:
        preset = "RTX6000"
    gpu_id, cloud_type, desc = GPU_PRESETS[preset]

    name = f"fpq-wan-sli"
    print(f"Launching Wan SLI benchmark pod...")
    print(f"  GPU: {gpu_id} ({desc})")

    docker_image = "runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04"

    combined_script = SETUP_SCRIPT.rstrip() + "\n\n" + WAN_SLI_SCRIPT

    query = """
    mutation CreatePod($input: PodFindAndDeployOnDemandInput!) {
        podFindAndDeployOnDemand(input: $input) {
            id
            name
            desiredStatus
            imageName
        }
    }
    """
    variables = {
        "input": {
            "name": name,
            "imageName": docker_image,
            "gpuTypeId": gpu_id,
            "cloudType": cloud_type,
            "containerDiskInGb": 60,
            "volumeInGb": 100,
            "startSsh": True,
            "dockerArgs": "",
            "env": [],
            "startupScript": combined_script,
        }
    }
    result = graphql(api_key, query, variables)
    pod = result.get("data", {}).get("podFindAndDeployOnDemand")
    if not pod:
        print(f"Launch failed: {result}")
        sys.exit(1)

    pod_id = pod["id"]
    print(f"\n  Pod ID: {pod_id}")
    print(f"  Status: {pod['desiredStatus']}")
    print(f"\nThe pod will automatically:")
    print(f"  1. Clone the bonfyre repo + build libfpq.so")
    print(f"  2. Download Wan2.1-T2V-1.3B from Wan-AI/Wan2.1-T2V-1.3B (HuggingFace)")
    print(f"  3. Download Wan v12 .fpq from NICKO/wan2.1-t2v-1.3b-v12-fpq (HuggingFace)")
    print(f"  4. Run test_e2e_wan_sli.py --sweep --device cuda")
    print(f"  5. Save results to /workspace/results/wan_sli.json")
    print(f"\nTo monitor:")
    print(f"  python3 pipeline/deploy.py ssh {pod_id}")
    print(f"  ssh root@<ip> -p <port> 'tail -f /workspace/logs/wan_sli.log'")
    print(f"\nTo fetch results when done:")
    print(f"  scp -P <port> root@<ip>:/workspace/results/wan_sli.json results/")


def main():
    parser = argparse.ArgumentParser(description="BonfyreFPQ RunPod Pipeline")
    sub = parser.add_subparsers(dest="command")

    # launch
    p_launch = sub.add_parser("launch", help="Launch a GPU pod")
    p_launch.add_argument("--gpu", default="RTX6000PRO", help="GPU preset: RTX6000PRO, H200, RTX6000, A100, H100")
    p_launch.add_argument("--name", help="Pod name (default: fpq-<gpu>)")
    p_launch.add_argument("--disk", type=int, default=50, help="Container disk GB")
    p_launch.add_argument("--volume", type=int, default=1000, help="Persistent volume GB (GLM-5.1-FP8 ~750GB + compressed ~280GB)")
    p_launch.add_argument("--image", help="Docker image override")

    # list
    sub.add_parser("list", help="List running pods")

    # stop
    p_stop = sub.add_parser("stop", help="Stop a pod")
    p_stop.add_argument("pod_id", help="Pod ID")

    # stop-all
    sub.add_parser("stop-all", help="Stop all pods")

    # terminate
    p_term = sub.add_parser("terminate", help="TERMINATE a pod (deletes storage!)")
    p_term.add_argument("pod_id", help="Pod ID")

    # ssh
    p_ssh = sub.add_parser("ssh", help="Get SSH command for a pod")
    p_ssh.add_argument("pod_id", help="Pod ID")

    # submit
    p_sub = sub.add_parser("submit", help="Submit a compression job")
    p_sub.add_argument("model", help="HuggingFace model ID (e.g., THUDM/glm-4-9b)")
    p_sub.add_argument("--bits", type=int, default=3, help="Quantization bits")
    p_sub.add_argument("--format", default="safetensors", help="Model format")
    p_sub.add_argument("--priority", type=int, default=5, help="Priority 1-10 (1=highest)")

    # wan-sli
    p_wan = sub.add_parser("wan-sli", help="Launch pod + run Wan2.1 SLI end-to-end benchmark")
    p_wan.add_argument("--gpu", default="RTX6000", help="GPU preset (default: RTX6000, 48GB VRAM)")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    {
        "launch": cmd_launch,
        "list": cmd_list,
        "stop": cmd_stop,
        "stop-all": cmd_stop_all,
        "terminate": cmd_terminate,
        "ssh": cmd_ssh,
        "submit": cmd_submit,
        "wan-sli": cmd_wan_sli,
    }[args.command](args)

if __name__ == "__main__":
    main()

