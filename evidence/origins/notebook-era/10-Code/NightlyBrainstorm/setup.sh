#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════
# setup.sh — Install llama.cpp and download a model for Nightly Brainstorm
# ═══════════════════════════════════════════════════════════════
# Run once. Installs llama.cpp via Homebrew (Apple Silicon optimized)
# and downloads a recommended model.
#
# Usage:
#   chmod +x setup.sh && ./setup.sh
# ═══════════════════════════════════════════════════════════════

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MODEL_DIR="$SCRIPT_DIR/models"

echo "═══════════════════════════════════════════════════"
echo "  Nightly Brainstorm — Setup"
echo "═══════════════════════════════════════════════════"
echo ""

# ── Step 1: Install llama.cpp ──
echo "▸ Checking llama.cpp..."
if command -v llama-cli &>/dev/null; then
    echo "  ✓ llama-cli already installed: $(which llama-cli)"
else
    echo "  Installing llama.cpp via Homebrew..."
    if ! command -v brew &>/dev/null; then
        echo "  ✗ Homebrew not found. Install from https://brew.sh first."
        exit 1
    fi
    brew install llama.cpp
    echo "  ✓ llama.cpp installed"
fi

echo ""

# ── Step 2: Create model directory ──
mkdir -p "$MODEL_DIR"

# ── Step 3: Download model ──
echo "▸ Checking for models..."

# Recommended: Mistral 7B Instruct Q5_K_M — good balance of quality and speed on M3 16GB
# Other options listed below for different needs.
MODEL_URL="https://huggingface.co/TheBloke/Mistral-7B-Instruct-v0.2-GGUF/resolve/main/mistral-7b-instruct-v0.2.Q5_K_M.gguf"
MODEL_FILE="$MODEL_DIR/mistral-7b-instruct-v0.2.Q5_K_M.gguf"
MODEL_NAME="mistral-7b-instruct-v0.2.Q5_K_M.gguf"

if [ -f "$MODEL_FILE" ]; then
    echo "  ✓ Model already downloaded: $MODEL_NAME"
else
    echo "  Downloading $MODEL_NAME (~5.1 GB)..."
    echo "  This may take a while depending on your connection."
    echo ""
    echo "  Alternative models you can download manually:"
    echo "    • Llama 3 8B Q5_K_M  — best overall quality, needs ~6 GB"
    echo "    • Phi-3 mini Q5_K_M  — smaller/faster, ~2.8 GB"
    echo "    • Qwen2.5 7B Q5_K_M  — strong reasoning, ~5 GB"
    echo ""
    echo "  Place any .gguf file in: $MODEL_DIR/"
    echo "  Then update nightly.json model_path to point to it."
    echo ""

    if command -v curl &>/dev/null; then
        curl -L --progress-bar -o "$MODEL_FILE" "$MODEL_URL"
    elif command -v wget &>/dev/null; then
        wget --show-progress -O "$MODEL_FILE" "$MODEL_URL"
    else
        echo "  ✗ Neither curl nor wget found. Download manually:"
        echo "    $MODEL_URL"
        echo "  Save to: $MODEL_FILE"
        exit 1
    fi

    echo "  ✓ Model downloaded: $MODEL_NAME"
fi

echo ""

# ── Step 4: Update config ──
echo "▸ Updating nightly.json..."

# Use perl to update the model_path in config
perl -i -pe "s|\"model_path\": \".*?\"|\"model_path\": \"$MODEL_FILE\"|" "$SCRIPT_DIR/nightly.json"

# Set chat template based on model name
if echo "$MODEL_NAME" | grep -qi "mistral"; then
    perl -i -pe 's/"chat_template": ".*?"/"chat_template": "mistral"/' "$SCRIPT_DIR/nightly.json"
    echo "  ✓ Set chat_template to 'mistral'"
elif echo "$MODEL_NAME" | grep -qi "llama"; then
    perl -i -pe 's/"chat_template": ".*?"/"chat_template": "llama3"/' "$SCRIPT_DIR/nightly.json"
    echo "  ✓ Set chat_template to 'llama3'"
else
    echo "  Using default chat_template: chatml"
fi

echo "  ✓ Config updated with model path"

echo ""

# ── Step 5: Verify ──
echo "▸ Verifying installation..."

echo -n "  llama-cli: "
if command -v llama-cli &>/dev/null; then
    echo "✓ $(llama-cli --version 2>&1 | head -1)"
else
    echo "✗ not in PATH"
fi

echo -n "  Model: "
if [ -f "$MODEL_FILE" ]; then
    MODEL_SIZE=$(du -h "$MODEL_FILE" | cut -f1)
    echo "✓ $MODEL_NAME ($MODEL_SIZE)"
else
    echo "✗ not found"
fi

echo -n "  Perl: "
perl -e 'print "✓ $^V\n"'

echo -n "  JSON::PP: "
perl -e 'use JSON::PP; print "✓\n"' 2>/dev/null || echo "✗"

echo ""

# ── Step 6: Dry run test ──
echo "▸ Running dry-run test..."
cd "$SCRIPT_DIR"
perl nightly.pl --dry-run --pass idea_expand --limit 1 2>&1 | tail -10

echo ""
echo "═══════════════════════════════════════════════════"
echo "  Setup complete!"
echo ""
echo "  Next steps:"
echo "    1. Review nightly.json — model path, backend, template"
echo "    2. Test:  perl nightly.pl --dry-run --verbose"
echo "    3. Real:  perl nightly.pl --pass idea_expand --limit 1"
echo "    4. Full:  perl nightly.pl"
echo "    5. Cron:  crontab -e → '0 2 * * * cd $SCRIPT_DIR && perl nightly.pl'"
echo "═══════════════════════════════════════════════════"
