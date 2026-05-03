#!/bin/bash
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

#!/bin/bash
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
