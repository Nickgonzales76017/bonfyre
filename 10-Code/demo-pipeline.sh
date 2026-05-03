#!/bin/bash
# Bonfyre Full Pipeline Demo — Audio File → Delivered Offer
#
# Demonstrates the complete Bonfyre binary pipeline on a single machine.
# Every step uses a Bonfyre binary. No cloud. No containers. No runtime.
#
# Prerequisites:
#   - All Bonfyre binaries installed (./install.sh)
#   - Whisper model downloaded (for transcription)
#   - An audio file to process
#
# Usage:
#   ./demo-pipeline.sh <audio-file> [--out DIR]

set -euo pipefail

CYAN='\033[0;36m'
GREEN='\033[0;32m'
BOLD='\033[1m'
NC='\033[0m'

step() { echo -e "\n${CYAN}[$1]${NC} ${BOLD}$2${NC}"; }

INPUT="${1:?Usage: $0 <audio-file> [--out DIR]}"
OUT="${3:-./pipeline-output}"
mkdir -p "$OUT"

echo -e "${BOLD}Bonfyre Pipeline — Full Demo${NC}"
echo "Input:  $INPUT"
echo "Output: $OUT"

# ---- Stage 1: Intake ----
step "1/10" "Ingest — intake manifest + validation"
bonfyre-ingest "$INPUT" --out "$OUT/01-intake"

step "2/10" "MediaPrep — normalize audio (16kHz mono)"
bonfyre-media-prep normalize "$OUT/01-intake/normalized.wav" -o "$OUT/02-normalized.wav"

step "3/10" "Hash — content-address the source"
bonfyre-hash "$INPUT" > "$OUT/03-source.sha256"
echo "  SHA-256: $(cat "$OUT/03-source.sha256")"

# ---- Stage 2: Transcription ----
step "4/10" "Transcribe — speech to text (Whisper)"
bonfyre-transcribe "$OUT/02-normalized.wav" "$OUT/04-transcript"

step "5/10" "TranscriptClean — remove filler, hallucinations"
bonfyre-transcript-clean "$OUT/04-transcript/normalized.txt" -o "$OUT/05-clean.txt"

step "6/10" "Paragraph — structure into readable paragraphs"
bonfyre-paragraph "$OUT/05-clean.txt" -o "$OUT/06-paragraphed.md"

# ---- Stage 3: Analysis ----
step "7/10" "Brief — extract summary + action items"
bonfyre-brief "$OUT/06-paragraphed.md" -o "$OUT/07-brief"

step "8/10" "Proof — quality score + review"
bonfyre-proof "$OUT/07-brief" -o "$OUT/08-proof"

# ---- Stage 4: Monetization ----
step "9/10" "Offer — generate pricing + outreach"
bonfyre-offer "$OUT/08-proof" -o "$OUT/09-offer"

step "10/10" "Pack — assemble deliverable package"
bonfyre-pack --proof "$OUT/08-proof" --offer "$OUT/09-offer" -o "$OUT/10-package"

echo ""
echo -e "${GREEN}${BOLD}Pipeline complete.${NC}"
echo ""
echo "Deliverables in $OUT/10-package/:"
ls -la "$OUT/10-package/" 2>/dev/null || echo "  (check output directory)"
echo ""
echo "Distribution:"
echo "  bonfyre-distribute offers --dir $OUT/10-package"
echo "  bonfyre-distribute message --offer $OUT/09-offer/offer.json --channel email"
