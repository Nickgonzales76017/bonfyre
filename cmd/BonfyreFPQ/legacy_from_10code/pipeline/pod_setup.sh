#!/bin/bash
# BonfyreFPQ — RunPod Pod Setup Script
# Run this once when a pod starts. Compiles bonfyre-fpq for Linux + OpenBLAS.
set -euo pipefail

echo "============================================"
echo " BonfyreFPQ Pod Setup"
echo " $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "============================================"

# 1. System dependencies
echo "[1/5] Installing system dependencies..."
apt-get update -qq
apt-get install -y -qq \
    git build-essential \
    libopenblas-dev liblapack-dev \
    wget curl jq \
    2>/dev/null

# 2. Clone or update bonfyre repo
echo "[2/5] Setting up bonfyre repo..."
if [ -d /workspace/bonfyre/.git ]; then
    cd /workspace/bonfyre
    git pull --rebase 2>/dev/null || true
else
    git clone https://github.com/Nickgonzales76017/bonfyre.git /workspace/bonfyre
fi

# 3. Build bonfyre-fpq for Linux
echo "[3/5] Building bonfyre-fpq..."
cd /workspace/bonfyre/10-Code/BonfyreFPQ

# Create Linux-specific Makefile
cat > Makefile.linux << 'EOF'
CC      := gcc
CFLAGS  := -O3 -march=native -Wall -Wextra -std=c11 -Iinclude -fopenmp -DHAVE_OPENBLAS
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
        $(SRC_DIR)/main.c

OBJS := $(patsubst $(SRC_DIR)/%.c,$(BUILD)/%.o,$(SRCS))
BIN  := bonfyre-fpq

.PHONY: all clean
all: $(BIN)
$(BIN): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
$(BUILD)/%.o: $(SRC_DIR)/%.c include/fpq.h | $(BUILD)
	$(CC) $(CFLAGS) -c -o $@ $<
$(BUILD):
	mkdir -p $(BUILD)
clean:
	rm -rf $(BUILD) $(BIN)
EOF

make -f Makefile.linux clean
make -f Makefile.linux -j"$(nproc)"

# Verify build
./bonfyre-fpq 2>&1 | head -3 || true
echo "  Binary: $(ls -lh bonfyre-fpq | awk '{print $5}')"

# 4. Install Python deps for model download
echo "[4/5] Installing Python dependencies..."
pip install -q huggingface_hub 2>/dev/null

# 5. Create workspace directories
echo "[5/5] Creating workspace structure..."
mkdir -p /workspace/jobs/{pending,running,done,failed}
mkdir -p /workspace/models/{original,fpq}
mkdir -p /workspace/logs

# Configure git for pushing results
git config --global user.email "fpq-worker@bonfyre.dev"
git config --global user.name "BonfyreFPQ Worker"

echo ""
echo "============================================"
echo " Setup complete!"
echo " Binary: /workspace/bonfyre/10-Code/BonfyreFPQ/bonfyre-fpq"
echo ""
echo " Next: bash /workspace/bonfyre/10-Code/BonfyreFPQ/pipeline/worker.sh"
echo "============================================"
