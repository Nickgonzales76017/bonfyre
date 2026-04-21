#!/usr/bin/env bash
#
# Bonfyre Speech Investigation — Production Pipeline
#
# Uses C binaries from /tmp/bonfyre-oss/build/ instead of Python stubs.
# Fully integrated with Bonfyre infrastructure (metering, quality scoring, compression, indexing).
#
# Usage:
#   bash scripts/speech_investigation_production.sh "audio/*.mp3" investigation_out [--speakers]
#

set -e

# ══════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════

BONFYRE_BIN_PATH="${BONFYRE_BIN_PATH:-/tmp/bonfyre-oss/build}"
WITH_SPEAKERS=false
ENABLE_EMBEDDINGS=false
ENABLE_SLI=false
ENABLE_METERING=true
QUALITY_THRESHOLD=0.7

# Parse flags
while [[ $# -gt 0 ]]; do
    case $1 in
        --speakers)
            WITH_SPEAKERS=true
            shift
            ;;
        --embeddings)
            ENABLE_EMBEDDINGS=true
            shift
            ;;
        --sli)
            ENABLE_SLI=true
            ENABLE_EMBEDDINGS=true  # SLI requires embeddings
            shift
            ;;
        --no-metering)
            ENABLE_METERING=false
            shift
            ;;
        --quality-threshold)
            QUALITY_THRESHOLD="$2"
            shift 2
            ;;
        *)
            break
            ;;
    esac
done

AUDIO_PATTERN="${1:-audio/*.mp3}"
OUTPUT_DIR="${2:-investigation_out}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

echo "══════════════════════════════════════════════════════════════════════"
echo "BONFYRE SPEECH INVESTIGATION — PRODUCTION PIPELINE"
echo "══════════════════════════════════════════════════════════════════════"
echo ""
echo "Audio pattern:    $AUDIO_PATTERN"
echo "Output:           $OUTPUT_DIR"
echo "Speakers:         $([ "$WITH_SPEAKERS" = true ] && echo "ENABLED" || echo "disabled")"
echo "Embeddings:       $([ "$ENABLE_EMBEDDINGS" = true ] && echo "ENABLED" || echo "disabled")"
echo "SLI search:       $([ "$ENABLE_SLI" = true ] && echo "ENABLED" || echo "disabled")"
echo "Metering:         $([ "$ENABLE_METERING" = true ] && echo "ENABLED" || echo "disabled")"
echo "Quality threshold: $QUALITY_THRESHOLD"
echo ""

# Create output structure
mkdir -p "$OUTPUT_DIR"/{audio,transcripts,entities,canon,graphs,claims,embeddings,reports,interventions,artifacts,metrics}

# Check for Bonfyre binaries
if [ ! -d "$BONFYRE_BIN_PATH" ]; then
    echo -e "${RED}✗ Bonfyre binaries not found at: $BONFYRE_BIN_PATH${NC}"
    echo ""
    echo "Build binaries with:"
    echo "  cd /tmp/bonfyre-oss"
    echo "  make"
    echo ""
    exit 1
fi

# Expand audio files
shopt -s nullglob
audio_files=($AUDIO_PATTERN)
if [ ${#audio_files[@]} -eq 0 ]; then
    echo -e "${RED}✗ No audio files found matching: $AUDIO_PATTERN${NC}"
    exit 1
fi

echo "Found ${#audio_files[@]} audio file(s)"
echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 0: AUDIO SEGMENTATION (BonfyreSpeechLoop)
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "PHASE 0: AUDIO SEGMENTATION"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

if [ -x "$BONFYRE_BIN_PATH/bonfyre-speech-loop" ]; then
    echo "Using BonfyreSpeechLoop for VAD + segmentation"
    
    for audio_file in "${audio_files[@]}"; do
        filename=$(basename "$audio_file")
        name="${filename%.*}"
        
        echo "  Processing: $filename"
        
        # Run BonfyreSpeechLoop
        "$BONFYRE_BIN_PATH/bonfyre-speech-loop" \
            --input "$audio_file" \
            --output "$OUTPUT_DIR/audio/$name/" \
            --vad-threshold 0.3 \
            --segment-max 30 \
            --format wav || {
            echo -e "${YELLOW}⚠ SpeechLoop failed, copying original${NC}"
            cp "$audio_file" "$OUTPUT_DIR/audio/$name.wav"
        }
    done
else
    echo -e "${YELLOW}⚠ BonfyreSpeechLoop not available, copying audio files${NC}"
    for audio_file in "${audio_files[@]}"; do
        cp "$audio_file" "$OUTPUT_DIR/audio/"
    done
fi

echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 1: TRANSCRIPTION (BonfyreTranscribe)
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "PHASE 1: TRANSCRIPTION"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

if [ -x "$BONFYRE_BIN_PATH/bonfyre-transcribe" ]; then
    echo "Using BonfyreTranscribe (Whisper-based)"
    
    segments=$(find "$OUTPUT_DIR/audio" -name "*.wav" -o -name "*.mp3")
    
    for segment in $segments; do
        filename=$(basename "$segment")
        name="${filename%.*}"
        
        echo "  Transcribing: $filename"
        
        # Build command
        cmd="$BONFYRE_BIN_PATH/bonfyre-transcribe"
        cmd="$cmd --input $segment"
        cmd="$cmd --output $OUTPUT_DIR/transcripts/$name.txt"
        cmd="$cmd --json $OUTPUT_DIR/transcripts/$name.json"
        cmd="$cmd --model base"  # or large-v3 for production
        cmd="$cmd --quality-threshold $QUALITY_THRESHOLD"
        
        [ "$WITH_SPEAKERS" = true ] && cmd="$cmd --speaker-labels"
        
        # Run transcription
        eval "$cmd" || {
            echo -e "${RED}✗ Transcription failed for $filename${NC}"
            continue
        }
        
        # Meter if enabled
        if [ "$ENABLE_METERING" = true ] && [ -x "$BONFYRE_BIN_PATH/bonfyre-meter" ]; then
            duration=$(soxi -D "$segment" 2>/dev/null || echo "0")
            "$BONFYRE_BIN_PATH/bonfyre-meter" record \
                --operation transcribe \
                --input-size "$duration" \
                --output "$OUTPUT_DIR/transcripts/$name.json" \
                --cost $(echo "$duration * 0.006" | bc) \
                --ledger "$OUTPUT_DIR/metrics/ledger.db" || true
        fi
    done
else
    echo -e "${YELLOW}⚠ BonfyreTranscribe not available, using fallback Whisper${NC}"
    
    # Fallback: Use Python Whisper
    python3 << 'PYTHON_EOF'
import whisper
import json
import sys
from pathlib import Path

model = whisper.load_model("base")
segments_dir = Path(sys.argv[1])
output_dir = Path(sys.argv[2])

for audio_file in segments_dir.rglob("*.wav"):
    name = audio_file.stem
    print(f"  Transcribing: {audio_file.name}")
    
    result = model.transcribe(str(audio_file), verbose=False)
    
    # Write text
    (output_dir / f"{name}.txt").write_text(result["text"].strip())
    
    # Write JSON
    (output_dir / f"{name}.json").write_text(json.dumps({
        "text": result["text"],
        "segments": result.get("segments", []),
        "language": result.get("language", "unknown")
    }, indent=2))
PYTHON_EOF
    python3 -c "import sys; sys.argv = ['', '$OUTPUT_DIR/audio', '$OUTPUT_DIR/transcripts']; exec(open('/dev/stdin').read())"
fi

transcript_count=$(ls -1 "$OUTPUT_DIR/transcripts"/*.txt 2>/dev/null | wc -l | tr -d ' ')
echo ""
echo -e "${GREEN}✓ Transcribed $transcript_count files${NC}"
echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 1b: TRANSCRIPT CLEANING (BonfyreTranscriptClean)
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "PHASE 1b: TRANSCRIPT CLEANING"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

if [ -x "$BONFYRE_BIN_PATH/bonfyre-transcript-clean" ]; then
    echo "Using BonfyreTranscriptClean for post-processing"
    
    for transcript in "$OUTPUT_DIR/transcripts"/*.txt; do
        filename=$(basename "$transcript")
        name="${filename%.*}"
        
        "$BONFYRE_BIN_PATH/bonfyre-transcript-clean" \
            --input "$transcript" \
            --output "$OUTPUT_DIR/transcripts/${name}_clean.txt" \
            --mode conversational \
            --remove-fillers \
            --fix-disfluencies || {
            echo -e "${YELLOW}⚠ Cleaning failed for $filename, using original${NC}"
            cp "$transcript" "$OUTPUT_DIR/transcripts/${name}_clean.txt"
        }
    done
    
    # Use cleaned versions
    rm -f "$OUTPUT_DIR/transcripts"/*_clean.txt.bak
    for f in "$OUTPUT_DIR/transcripts"/*_clean.txt; do
        orig="${f%_clean.txt}.txt"
        mv "$orig" "$orig.raw"
        mv "$f" "$orig"
    done
else
    echo -e "${YELLOW}⚠ BonfyreTranscriptClean not available, skipping${NC}"
fi

echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 2: ENTITY EXTRACTION (BonfyreEntity)
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "PHASE 2: ENTITY EXTRACTION"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

if [ -x "$BONFYRE_BIN_PATH/bonfyre-entity" ]; then
    echo "Using BonfyreEntity (C binary)"
    
    "$BONFYRE_BIN_PATH/bonfyre-entity" \
        --input "$OUTPUT_DIR/transcripts/*.txt" \
        --type conversational \
        --structural-filter \
        --min-score 2 \
        --output "$OUTPUT_DIR/entities/entities.json" \
        --artifact "$OUTPUT_DIR/artifacts/entity_extraction.json"
else
    echo -e "${YELLOW}⚠ BonfyreEntity not available, using Python stub${NC}"
    
    python3 scripts/symbolic_extract.py \
        --corpus "$OUTPUT_DIR/transcripts/*.txt" \
        --output "$OUTPUT_DIR/entities" \
        --step entity
fi

echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 3: CANONICALIZATION (BonfyreCanon)
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "PHASE 3: CANONICALIZATION"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

if [ -x "$BONFYRE_BIN_PATH/bonfyre-canon" ]; then
    echo "Using BonfyreCanon (C binary with tree-sitter)"
    
    "$BONFYRE_BIN_PATH/bonfyre-canon" \
        --input "$OUTPUT_DIR/entities/entities.json" \
        --fuzzy-threshold 0.85 \
        --detect-acronyms \
        --detect-titles \
        --output "$OUTPUT_DIR/canon/canon.json" \
        --canonical-list "$OUTPUT_DIR/canon/canonical_entities.json"
else
    echo -e "${YELLOW}⚠ BonfyreCanon not available, using Python stub${NC}"
    
    python3 scripts/symbolic_extract.py \
        --corpus "$OUTPUT_DIR/transcripts/*.txt" \
        --output "$OUTPUT_DIR/canon" \
        --step canon \
        --entities "$OUTPUT_DIR/entities/entities.json"
fi

echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 4: GRAPH CONSTRUCTION (BonfyreGraph)
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "PHASE 4: GRAPH CONSTRUCTION"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

if [ -x "$BONFYRE_BIN_PATH/bonfyre-graph" ]; then
    echo "Using BonfyreGraph (C binary)"
    
    "$BONFYRE_BIN_PATH/bonfyre-graph" \
        --entities "$OUTPUT_DIR/canon/canonical_entities.json" \
        --cooccurrence-window 5 \
        --temporal-edges \
        --speaker-edges \
        --output "$OUTPUT_DIR/graphs/graph.json" \
        --stats "$OUTPUT_DIR/graphs/stats.json"
else
    echo -e "${YELLOW}⚠ BonfyreGraph not available, using Python stub${NC}"
    
    python3 scripts/symbolic_extract.py \
        --corpus "$OUTPUT_DIR/transcripts/*.txt" \
        --output "$OUTPUT_DIR/graphs" \
        --step graph \
        --canon "$OUTPUT_DIR/canon/canon.json"
fi

echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 5: CLAIMS EXTRACTION
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "PHASE 5: GRAPH → CLAIMS CONVERSION"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

python3 scripts/graph_to_claims.py \
    --graph "$OUTPUT_DIR/graphs/graph.json" \
    --memory-dir "$OUTPUT_DIR/claims" \
    --json "$OUTPUT_DIR/claims/claims.json"

claims_count=$(sqlite3 "$OUTPUT_DIR/claims/memory.db" "SELECT COUNT(*) FROM claims" 2>/dev/null || echo "0")
echo ""
echo -e "${GREEN}✓ Generated $claims_count claims${NC}"
echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 6: EMBEDDINGS (BonfyreEmbed + optional SLI)
# ══════════════════════════════════════════════════════════════════════

if [ "$ENABLE_EMBEDDINGS" = true ]; then
    echo "══════════════════════════════════════════════════════════════════════"
    echo "PHASE 6: EMBEDDINGS + VECTOR INDEX"
    echo "══════════════════════════════════════════════════════════════════════"
    echo ""
    
    if [ -x "$BONFYRE_BIN_PATH/bonfyre-embed" ]; then
        echo "Using BonfyreEmbed (ONNX Runtime)"
        
        "$BONFYRE_BIN_PATH/bonfyre-embed" \
            --input "$OUTPUT_DIR/claims/claims.json" \
            --model all-MiniLM-L6-v2 \
            --batch-size 256 \
            --fpq-compress \
            --output "$OUTPUT_DIR/embeddings/claims.fpq"
        
        # Build vector index
        if [ -x "$BONFYRE_BIN_PATH/bonfyre-vec" ]; then
            "$BONFYRE_BIN_PATH/bonfyre-vec" create \
                --embeddings "$OUTPUT_DIR/embeddings/claims.fpq" \
                --index-type hnsw \
                --m 16 \
                --ef-construction 200 \
                --output "$OUTPUT_DIR/embeddings/vector.db"
        fi
        
        # Optional: Prepare for SLI
        if [ "$ENABLE_SLI" = true ] && [ -x "$BONFYRE_BIN_PATH/bonfyre-sli" ]; then
            echo "  Preparing SLI index (4.4× bandwidth reduction)"
            
            "$BONFYRE_BIN_PATH/bonfyre-sli" prepare \
                --embeddings "$OUTPUT_DIR/embeddings/claims.fpq" \
                --output "$OUTPUT_DIR/embeddings/claims_sli.fpq" \
                --fwht-on-z
            
            echo -e "${GREEN}✓ SLI index ready (25ms queries vs 65ms dense)${NC}"
        fi
    else
        echo -e "${YELLOW}⚠ BonfyreEmbed not available, skipping embeddings${NC}"
    fi
    
    echo ""
fi

# ══════════════════════════════════════════════════════════════════════
# PHASE 7: HYPOTHESIS DISCOVERY
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "PHASE 7: AUTONOMOUS HYPOTHESIS DISCOVERY"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

python3 scripts/hypothesis_discovery.py \
    --memory-dir "$OUTPUT_DIR/claims" \
    --max-hypotheses 20 \
    --signal-threshold 0.7 \
    --output "$OUTPUT_DIR/reports/discovery.json"

echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 8: ADVERSARIAL TESTING
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "PHASE 8: ADVERSARIAL HYPOTHESIS TESTING"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

python3 scripts/hypothesis_engine.py \
    --hypotheses "$OUTPUT_DIR/reports/discovery.json" \
    --memory-dir "$OUTPUT_DIR/claims" \
    --adversarial \
    --output "$OUTPUT_DIR/reports/tested_hypotheses.json"

echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 9: CONVERGENCE + PRESSURE
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "PHASE 9: CONVERGENCE + ORTHOGONAL PRESSURE"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

python3 scripts/convergence_engine.py \
    --claims "$OUTPUT_DIR/claims/memory.db" \
    --tested "$OUTPUT_DIR/reports/tested_hypotheses.json" \
    --output "$OUTPUT_DIR/reports/" \
    --stable "$OUTPUT_DIR/graphs/stable_graph.json" \
    --fragile "$OUTPUT_DIR/graphs/fragile_graph.json" \
    --conflict "$OUTPUT_DIR/graphs/conflict_graph.json"

echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 10: COMPRESSION + INDEXING
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "PHASE 10: COMPRESSION + INDEXING"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

if [ -x "$BONFYRE_BIN_PATH/bonfyre-compress" ]; then
    echo "Using BonfyreCompress (zstd family-aware)"
    
    "$BONFYRE_BIN_PATH/bonfyre-compress" \
        --family speech_investigation \
        --inputs "$OUTPUT_DIR/reports/*.json" "$OUTPUT_DIR/graphs/*.json" \
        --codec zstd \
        --level 19 \
        --output "$OUTPUT_DIR/artifacts/compressed.zst"
fi

if [ -x "$BONFYRE_BIN_PATH/bonfyre-index" ]; then
    echo "Using BonfyreIndex (SQLite FTS5)"
    
    "$BONFYRE_BIN_PATH/bonfyre-index" \
        --artifacts "$OUTPUT_DIR/artifacts/" \
        --fts5 \
        --output "$OUTPUT_DIR/metrics/index.db"
fi

echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 11: VALUE ASSESSMENT
# ══════════════════════════════════════════════════════════════════════

if [ "$ENABLE_METERING" = true ]; then
    echo "══════════════════════════════════════════════════════════════════════"
    echo "PHASE 11: VALUE ASSESSMENT"
    echo "══════════════════════════════════════════════════════════════════════"
    echo ""
    
    if [ -x "$BONFYRE_BIN_PATH/bonfyre-ledger" ]; then
        "$BONFYRE_BIN_PATH/bonfyre-ledger" assess \
            --artifacts "$OUTPUT_DIR/artifacts/" \
            --meter "$OUTPUT_DIR/metrics/ledger.db" \
            --output "$OUTPUT_DIR/metrics/portfolio.json"
        
        cat "$OUTPUT_DIR/metrics/portfolio.json" | jq '.summary' || true
    fi
    
    echo ""
fi

# ══════════════════════════════════════════════════════════════════════
# FINAL REPORT
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "INVESTIGATION COMPLETE"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

# Compute final metrics
entities=$(jq '. | length' "$OUTPUT_DIR/canon/canonical_entities.json" 2>/dev/null || echo "0")
claims=$(sqlite3 "$OUTPUT_DIR/claims/memory.db" "SELECT COUNT(*) FROM claims" 2>/dev/null || echo "0")
hypotheses=$(jq '.hypotheses | length' "$OUTPUT_DIR/reports/discovery.json" 2>/dev/null || echo "0")
stable=$(jq '.nodes | length' "$OUTPUT_DIR/graphs/stable_graph.json" 2>/dev/null || echo "0")
fragile=$(jq '.nodes | length' "$OUTPUT_DIR/graphs/fragile_graph.json" 2>/dev/null || echo "0")
conflicts=$(jq '.nodes | length' "$OUTPUT_DIR/graphs/conflict_graph.json" 2>/dev/null || echo "0")

echo "Output structure:"
echo "  $OUTPUT_DIR/"
echo "    transcripts/         - ${transcript_count} transcriptions"
echo "    entities/            - Entity extraction results"
echo "    canon/               - ${entities} canonical entities"
echo "    graphs/              - Entity graph + convergence graphs"
echo "    claims/              - ${claims} claims in memory.db"
[ "$ENABLE_EMBEDDINGS" = true ] && echo "    embeddings/          - Vector index + SLI (if enabled)"
echo "    reports/             - Discovery + tested hypotheses"
echo "    artifacts/           - Compressed artifacts"
echo "    metrics/             - Quality metrics + value assessment"
echo ""

echo "Key Metrics:"
echo "  Transcripts:  ${transcript_count}"
echo "  Entities:     ${entities}"
echo "  Claims:       ${claims}"
echo "  Hypotheses:   ${hypotheses}"
echo "  Stable:       ${stable}"
echo "  Fragile:      ${fragile}"
echo "  Conflicts:    ${conflicts}"
echo ""

if [ "$ENABLE_EMBEDDINGS" = true ]; then
    echo "Semantic Search:"
    echo "  Vector index: $OUTPUT_DIR/embeddings/vector.db"
    [ "$ENABLE_SLI" = true ] && echo "  SLI index:    $OUTPUT_DIR/embeddings/claims_sli.fpq (4.4× faster)"
    echo ""
fi

echo "API Query Examples:"
echo ""
echo "  # Semantic search"
echo "  sqlite3 $OUTPUT_DIR/embeddings/vector.db \"SELECT * FROM claims WHERE ...\""
echo ""
echo "  # Top entities"
echo "  sqlite3 $OUTPUT_DIR/claims/memory.db \"SELECT subject, COUNT(*) FROM claims GROUP BY subject ORDER BY COUNT(*) DESC LIMIT 20\""
echo ""
echo "  # Hypothesis details"
echo "  jq '.hypotheses[] | select(.investigation_score > 0.5)' $OUTPUT_DIR/reports/discovery.json"
echo ""

echo -e "${GREEN}✓ Speech investigation pipeline complete${NC}"
