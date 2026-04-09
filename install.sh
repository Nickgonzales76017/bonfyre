#!/bin/bash
# Bonfyre — Unified Build & Install
#
# Builds all Bonfyre binaries from source. Single machine, zero cloud.
#
# Usage:
#   ./install.sh              # Build all, install to ~/.local/bin
#   ./install.sh --prefix /usr/local
#   ./install.sh --check      # Check dependencies only
#   ./install.sh --list       # List all binaries
#   ./install.sh --only cms,brief,proof   # Build subset

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PREFIX="${HOME}/.local"
CHECK_ONLY=0
LIST_ONLY=0
ONLY=""
JOBS=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)

# ---- All Bonfyre binary projects (build order matters for deps) ----
# Library first, then CMS (depends on library), then everything else
PROJECTS=(
    "liblambda-tensors|liblambda-tensors.a|library"
    "BonfyreCMS|bonfyre-cms|infrastructure"
    "BonfyreCLI|bonfyre|orchestration"
    "BonfyrePipeline|bonfyre-pipeline|orchestration"
    "BonfyreOrchestrate|bonfyre-orchestrate|orchestration"
    "BonfyreQueue|bonfyre-queue|orchestration"
    "BonfyreStitch|bonfyre-stitch|orchestration"
    "BonfyreSync|bonfyre-sync|orchestration"
    "BonfyreIngest|bonfyre-ingest|pipeline"
    "BonfyreHash|bonfyre-hash|pipeline"
    "BonfyreMediaPrep|bonfyre-media-prep|pipeline"
    "BonfyreTranscribe|bonfyre-transcribe|pipeline"
    "BonfyreTranscriptClean|bonfyre-transcript-clean|pipeline"
    "BonfyreTranscriptFamily|bonfyre-transcript-family|pipeline"
    "BonfyreParagraph|bonfyre-paragraph|pipeline"
    "BonfyreNarrate|bonfyre-narrate|pipeline"
    "BonfyreBrief|bonfyre-brief|pipeline"
    "BonfyreProof|bonfyre-proof|pipeline"
    "BonfyreOffer|bonfyre-offer|value"
    "BonfyrePack|bonfyre-pack|pipeline"
    "BonfyreDistribute|bonfyre-distribute|pipeline"
    "BonfyreCompress|bonfyre-compress|pipeline"
    "BonfyreEmbed|bonfyre-embed|pipeline"
    "BonfyreEmit|bonfyre-emit|pipeline"
    "BonfyreRender|bonfyre-render|pipeline"
    "BonfyreProject|bonfyre-project|pipeline"
    "BonfyreIndex|bonfyre-index|infrastructure"
    "BonfyreGate|bonfyre-gate|value"
    "BonfyreMeter|bonfyre-meter|value"
    "BonfyreLedger|bonfyre-ledger|value"
    "BonfyreRuntime|bonfyre-runtime|infrastructure"
    "BonfyreMFADict|bonfyre-mfa-dict|pipeline"
    "BonfyreWeaviateIndex|bonfyre-weaviate-index|pipeline"
    "BonfyreGraph|bonfyre-graph|infrastructure"
    "BonfyreFinance|bonfyre-finance|value"
    "BonfyreOutreach|bonfyre-outreach|value"
    "BonfyreAPI|bonfyre-api|infrastructure"
    "BonfyreAuth|bonfyre-auth|infrastructure"
    "BonfyrePay|bonfyre-pay|value"
)

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log()  { echo -e "${CYAN}[bonfyre]${NC} $*"; }
ok()   { echo -e "${GREEN}  ✓${NC} $*"; }
warn() { echo -e "${YELLOW}  ⚠${NC} $*"; }
fail() { echo -e "${RED}  ✗${NC} $*"; }

# ---- Parse args ----
while [[ $# -gt 0 ]]; do
    case "$1" in
        --prefix)  PREFIX="$2"; shift 2 ;;
        --check)   CHECK_ONLY=1; shift ;;
        --list)    LIST_ONLY=1; shift ;;
        --only)    ONLY="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--prefix DIR] [--check] [--list] [--only name,name,...]"
            echo ""
            echo "Options:"
            echo "  --prefix DIR   Install prefix (default: ~/.local)"
            echo "  --check        Check build dependencies only"
            echo "  --list         List all binaries and exit"
            echo "  --only NAMES   Comma-separated list of binaries to build"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ---- List mode ----
if [[ $LIST_ONLY -eq 1 ]]; then
    echo -e "${BOLD}Bonfyre Binary Family — 39+ binaries${NC}"
    echo ""
    printf "  %-28s %-24s %s\n" "PROJECT" "BINARY" "LAYER"
    printf "  %-28s %-24s %s\n" "-------" "------" "-----"
    for entry in "${PROJECTS[@]}"; do
        IFS='|' read -r dir binary layer <<< "$entry"
        printf "  %-28s %-24s %s\n" "$dir" "$binary" "$layer"
    done
    echo ""
    echo "Layers: library → infrastructure → orchestration → pipeline → value"
    exit 0
fi

# ---- Dependency check ----
log "Checking build dependencies..."

MISSING=0
check_cmd() {
    if command -v "$1" &>/dev/null; then
        ok "$1 $(command -v "$1")"
    else
        fail "$1 not found"
        MISSING=$((MISSING + 1))
    fi
}

check_lib() {
    # Try pkg-config, fall back to header search, fall back to compile test
    if pkg-config --exists "$1" 2>/dev/null; then
        ok "$1 ($(pkg-config --modversion "$1"))"
    elif echo "#include <$2>" | cc -fsyntax-only -x c - 2>/dev/null; then
        ok "$1 (header found)"
    else
        fail "$1 not found — install: $3"
        MISSING=$((MISSING + 1))
    fi
}

check_cmd cc
check_cmd make
check_lib sqlite3 sqlite3.h "brew install sqlite3 / apt install libsqlite3-dev"
check_lib zlib zlib.h "brew install zlib / apt install zlib1g-dev"

echo ""
if [[ $MISSING -gt 0 ]]; then
    fail "$MISSING missing dependencies"
    exit 1
fi
ok "All dependencies satisfied"

if [[ $CHECK_ONLY -eq 1 ]]; then
    exit 0
fi

# ---- Build ----
log "Building Bonfyre binary family..."
echo ""

BUILT=0
FAILED=0
SKIPPED=0
TOTAL_SIZE=0
BINARIES=()

# Filter by --only if specified
should_build() {
    if [[ -z "$ONLY" ]]; then return 0; fi
    local name="$1"
    local binary="$2"
    IFS=',' read -ra FILTER <<< "$ONLY"
    for f in "${FILTER[@]}"; do
        f=$(echo "$f" | xargs)  # trim
        if [[ "$name" == "$f" ]] || [[ "$binary" == "$f" ]]; then
            return 0
        fi
    done
    return 1
}

for entry in "${PROJECTS[@]}"; do
    IFS='|' read -r dir binary layer <<< "$entry"

    if ! should_build "$dir" "$binary"; then
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    # Library lives in lib/, binaries in cmd/
    if [[ "$layer" == "library" ]]; then
        project_dir="${SCRIPT_DIR}/lib/${dir}"
    else
        project_dir="${SCRIPT_DIR}/cmd/${dir}"
    fi
    if [[ ! -d "$project_dir" ]]; then
        fail "$dir — directory not found"
        FAILED=$((FAILED + 1))
        continue
    fi

    if [[ ! -f "$project_dir/Makefile" ]]; then
        fail "$dir — no Makefile"
        FAILED=$((FAILED + 1))
        continue
    fi

    printf "  %-28s " "$dir"

    if make -C "$project_dir" -j"$JOBS" 2>/dev/null 1>/dev/null; then
        # Find the built binary
        bin_path=""
        if [[ -f "$project_dir/$binary" ]]; then
            bin_path="$project_dir/$binary"
        elif [[ -f "$project_dir/build/$binary" ]]; then
            bin_path="$project_dir/build/$binary"
        fi

        if [[ -n "$bin_path" ]]; then
            size=$(wc -c < "$bin_path" | xargs)
            size_kb=$((size / 1024))
            TOTAL_SIZE=$((TOTAL_SIZE + size))
            echo -e "${GREEN}✓${NC} ${binary} (${size_kb}KB)"
            BINARIES+=("$project_dir|$binary|$bin_path")
            BUILT=$((BUILT + 1))
        else
            echo -e "${YELLOW}⚠${NC} built but binary not found"
            BUILT=$((BUILT + 1))
        fi
    else
        echo -e "${RED}✗${NC} build failed"
        FAILED=$((FAILED + 1))
    fi
done

echo ""
log "Build complete: ${GREEN}${BUILT} built${NC}, ${RED}${FAILED} failed${NC}, ${SKIPPED} skipped"
log "Total binary size: $((TOTAL_SIZE / 1024))KB ($((TOTAL_SIZE / 1024 / 1024))MB)"

# ---- Install ----
if [[ $FAILED -gt 0 ]]; then
    warn "Some builds failed. Install anyway? (y/N)"
    read -r ans
    if [[ "$ans" != "y" && "$ans" != "Y" ]]; then
        exit 1
    fi
fi

BIN_DIR="${PREFIX}/bin"
mkdir -p "$BIN_DIR"

log "Installing to ${BIN_DIR}..."
INSTALLED=0
for entry in "${BINARIES[@]}"; do
    IFS='|' read -r _dir binary bin_path <<< "$entry"
    # Skip library — not an installable binary
    if [[ "$binary" == *.a ]] || [[ "$binary" == *.so ]]; then
        continue
    fi
    if install -m 755 "$bin_path" "${BIN_DIR}/${binary}" 2>/dev/null; then
        ok "$binary → ${BIN_DIR}/${binary}"
        INSTALLED=$((INSTALLED + 1))
    else
        fail "Failed to install $binary"
    fi
done

echo ""
log "${GREEN}${BOLD}${INSTALLED} binaries installed to ${BIN_DIR}${NC}"

# Check if BIN_DIR is in PATH
if [[ ":$PATH:" != *":${BIN_DIR}:"* ]]; then
    warn "${BIN_DIR} is not in your PATH"
    echo "  Add to ~/.zshrc:  export PATH=\"${BIN_DIR}:\$PATH\""
fi

echo ""
echo -e "${BOLD}Bonfyre is ready.${NC}"
echo "  Run: bonfyre --help"
echo "  CMS: bonfyre-cms serve"
echo "  Full pipeline: bonfyre-pipeline --help"

# ---- Models (optional) ----
echo ""
log "Checking models..."
MODDIR="$HOME/.bonfyre/models"
WHISPER_DIR="$HOME/.local/share/whisper"
mkdir -p "$MODDIR" "$WHISPER_DIR"

download_model() {
    local dest="$1" url="$2" label="$3"
    if [[ -f "$dest" ]]; then
        ok "$label (exists)"
    else
        echo -n "  ↓ $label ... "
        if curl -fSL -o "$dest" "$url" 2>/dev/null; then
            echo -e "${GREEN}✓${NC}"
        else
            echo -e "${RED}✗${NC}"
        fi
    fi
}

download_model "$WHISPER_DIR/ggml-base.en.bin" \
    "https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-base.en.bin" \
    "whisper base.en (~140MB)"

download_model "$MODDIR/lid.176.bin" \
    "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin" \
    "fastText lid.176 (~125MB)"

download_model "$MODDIR/all-MiniLM-L6-v2.onnx" \
    "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/onnx/model.onnx" \
    "sentence-transformer ONNX (~22MB)"

echo ""
echo -e "${BOLD}Installation complete.${NC}"
