#!/bin/sh
# Bonfyre Full Pipeline Demo
# Usage: ./run.sh /path/to/audio.mp3
#
# Runs the unified pipeline binary which executes all 10 stages
# in a single process (5–8 ms on Apple Silicon).

set -e

BONFYRE_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PIPELINE="$BONFYRE_ROOT/cmd/BonfyrePipeline/bonfyre-pipeline"
OUTPUT="$(dirname "$0")/output"

if [ -z "$1" ]; then
    echo "Usage: $0 <audio-file>"
    echo ""
    echo "Example:"
    echo "  $0 interview.mp3"
    echo ""
    echo "This runs the full Bonfyre pipeline:"
    echo "  ingest → normalize → hash → transcribe → clean →"
    echo "  paragraph → brief → proof → offer → pack"
    exit 1
fi

INPUT="$1"

if [ ! -f "$INPUT" ]; then
    echo "Error: file not found: $INPUT"
    exit 1
fi

# Build if needed
if [ ! -f "$PIPELINE" ]; then
    echo "Building bonfyre-pipeline..."
    make -C "$BONFYRE_ROOT" cmd/BonfyrePipeline
fi

mkdir -p "$OUTPUT"

echo "Running pipeline on: $INPUT"
echo "Output directory: $OUTPUT"
echo ""

"$PIPELINE" run --input "$INPUT" --out "$OUTPUT"

echo ""
echo "Done. Check $OUTPUT/ for results."
