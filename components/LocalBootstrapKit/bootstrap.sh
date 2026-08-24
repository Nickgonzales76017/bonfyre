#!/usr/bin/env bash
set -euo pipefail

# Local Bootstrap Kit — get a fresh machine ready for the transcription workflow.

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
warn() { echo -e "  ${YELLOW}!${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; }

echo "=== Local Bootstrap Kit ==="
echo ""

# ── Homebrew ──
if command -v brew &>/dev/null; then
    ok "Homebrew installed ($(brew --version | head -1))"
else
    warn "Homebrew not found — installing..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    ok "Homebrew installed"
fi

# ── ffmpeg ──
if command -v ffmpeg &>/dev/null; then
    ok "ffmpeg installed ($(ffmpeg -version | head -1 | awk '{print $3}'))"
else
    warn "ffmpeg not found — installing..."
    brew install ffmpeg
    ok "ffmpeg installed"
fi

# ── Python ──
PYTHON=""
for py in python3.12 python3.11 python3.10 python3; do
    if command -v "$py" &>/dev/null; then
        PYTHON="$py"
        break
    fi
done

if [[ -n "$PYTHON" ]]; then
    ok "Python found: $($PYTHON --version)"
else
    warn "Python 3.10+ not found — installing..."
    brew install python@3.12
    PYTHON="python3.12"
    ok "Python installed"
fi

# ── pip packages ──
echo ""
echo "Checking Python packages..."
$PYTHON -m pip install --quiet --upgrade pip 2>/dev/null || true

for pkg in whisper; do
    if $PYTHON -c "import $pkg" 2>/dev/null; then
        ok "$pkg available"
    else
        warn "$pkg not found — install with: $PYTHON -m pip install openai-whisper"
    fi
done

# ── Directory structure ──
echo ""
echo "Checking directory structure..."
DIRS=("inputs" "outputs" "models" "exports")
for d in "${DIRS[@]}"; do
    if [[ -d "$d" ]]; then
        ok "$d/ exists"
    else
        mkdir -p "$d"
        ok "$d/ created"
    fi
done

# ── Summary ──
echo ""
echo "=== Bootstrap Complete ==="
echo ""
echo "Next steps:"
echo "  1. Place audio files in inputs/"
echo "  2. Run: cd ../LocalAITranscriptionService && python3 -m local_ai_transcription_service.cli --check-env"
echo "  3. Process: python3 -m local_ai_transcription_service.cli --audio-file ../inputs/your-file.mp3"
echo ""
