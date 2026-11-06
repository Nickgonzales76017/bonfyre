#!/usr/bin/env bash
#
# Speech → Investigation Pipeline
# 
# Extends run_investigation.sh to accept audio input:
#   audio → transcription → entity/canon/graph → claims → hypotheses → pressure → outcomes
#
# Usage:
#   bash scripts/speech_to_investigation.sh "audio/*.mp3" investigation_out
#   bash scripts/speech_to_investigation.sh --with-speakers "audio/*.wav" investigation_out
#

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Parse arguments
WITH_SPEAKERS=false
if [[ "$1" == "--with-speakers" ]]; then
    WITH_SPEAKERS=true
    shift
fi

AUDIO_PATTERN="${1:-audio/*.mp3}"
OUTPUT_DIR="${2:-investigation_out}"

# Create output directories
TRANSCRIPT_DIR="$OUTPUT_DIR/transcripts"
mkdir -p "$TRANSCRIPT_DIR"

echo "══════════════════════════════════════════════════════════════════════"
echo "SPEECH → INVESTIGATION PIPELINE"
echo "══════════════════════════════════════════════════════════════════════"
echo ""
echo "Audio:   $AUDIO_PATTERN"
echo "Output:  $OUTPUT_DIR"
echo "Speakers: $([ "$WITH_SPEAKERS" = true ] && echo "ENABLED" || echo "disabled")"
echo ""

# Check dependencies
check_dependency() {
    if ! command -v "$1" &> /dev/null; then
        echo -e "${RED}✗ Missing dependency: $1${NC}"
        echo ""
        echo "Install with:"
        echo "  $2"
        echo ""
        exit 1
    fi
}

echo "Checking dependencies..."
check_dependency "python3" "brew install python3"

# Check for Whisper
if ! python3 -c "import whisper" 2>/dev/null; then
    echo -e "${YELLOW}⚠ Whisper not found${NC}"
    echo ""
    echo "Install with:"
    echo "  pip3 install openai-whisper"
    echo ""
    echo "Or for faster CPU inference:"
    echo "  pip3 install whisper-cpp-python"
    echo ""
    read -p "Install now? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        pip3 install openai-whisper
    else
        exit 1
    fi
fi

echo -e "${GREEN}✓ Dependencies OK${NC}"
echo ""

# ══════════════════════════════════════════════════════════════════════
# PHASE 0: AUDIO → TRANSCRIPTION
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "PHASE 0: AUDIO TRANSCRIPTION"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

# Create transcription script
TRANSCRIBE_SCRIPT="$TRANSCRIPT_DIR/_transcribe.py"
cat > "$TRANSCRIBE_SCRIPT" << 'PYTHON_EOF'
#!/usr/bin/env python3
"""
Transcribe audio files using Whisper.
Optionally include speaker diarization.
"""

import sys
import json
import whisper
from pathlib import Path

def transcribe_file(audio_path, output_path, with_speakers=False):
    """Transcribe single audio file."""
    print(f"  Transcribing: {Path(audio_path).name}")
    
    # Load Whisper model (base is good balance of speed/quality)
    model = whisper.load_model("base")
    
    # Transcribe
    result = model.transcribe(
        str(audio_path),
        verbose=False,
        word_timestamps=True if with_speakers else False
    )
    
    # Write plain text transcript
    text_output = output_path.replace('.json', '.txt')
    with open(text_output, 'w') as f:
        f.write(result["text"].strip())
    
    # Write detailed JSON (includes timestamps, segments)
    with open(output_path, 'w') as f:
        json.dump({
            "source": str(audio_path),
            "text": result["text"],
            "segments": result.get("segments", []),
            "language": result.get("language", "unknown")
        }, f, indent=2)
    
    word_count = len(result["text"].split())
    print(f"    → {word_count} words")
    
    return text_output

if __name__ == "__main__":
    audio_file = sys.argv[1]
    output_file = sys.argv[2]
    with_speakers = sys.argv[3].lower() == "true" if len(sys.argv) > 3 else False
    
    transcribe_file(audio_file, output_file, with_speakers)
PYTHON_EOF

chmod +x "$TRANSCRIBE_SCRIPT"

# Transcribe all audio files
shopt -s nullglob
audio_files=($AUDIO_PATTERN)

if [ ${#audio_files[@]} -eq 0 ]; then
    echo -e "${RED}✗ No audio files found matching: $AUDIO_PATTERN${NC}"
    exit 1
fi

echo "Found ${#audio_files[@]} audio file(s)"
echo ""

transcript_files=()
for audio_file in "${audio_files[@]}"; do
    filename=$(basename "$audio_file")
    name="${filename%.*}"
    
    json_output="$TRANSCRIPT_DIR/${name}.json"
    text_output="$TRANSCRIPT_DIR/${name}.txt"
    
    # Skip if already transcribed
    if [ -f "$text_output" ]; then
        echo "  ✓ Already transcribed: $filename"
        transcript_files+=("$text_output")
        continue
    fi
    
    # Transcribe
    python3 "$TRANSCRIBE_SCRIPT" "$audio_file" "$json_output" "$WITH_SPEAKERS"
    
    transcript_files+=("$text_output")
done

echo ""
echo -e "${GREEN}✓ Transcribed ${#transcript_files[@]} files${NC}"
echo ""

# Show sample of first transcript
if [ ${#transcript_files[@]} -gt 0 ]; then
    echo "Sample from ${transcript_files[0]}:"
    echo "─────────────────────────────────────────────────────────────────────"
    head -c 500 "${transcript_files[0]}"
    echo ""
    echo "─────────────────────────────────────────────────────────────────────"
    echo ""
fi

# ══════════════════════════════════════════════════════════════════════
# PHASE 1-6: RUN STANDARD INVESTIGATION PIPELINE
# ══════════════════════════════════════════════════════════════════════

echo "══════════════════════════════════════════════════════════════════════"
echo "RUNNING INVESTIGATION PIPELINE ON TRANSCRIPTS"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

# Run existing investigation pipeline on transcripts
bash scripts/run_investigation.sh "$TRANSCRIPT_DIR/*.txt" "$OUTPUT_DIR"

# ══════════════════════════════════════════════════════════════════════
# SPEECH-SPECIFIC ANALYSIS
# ══════════════════════════════════════════════════════════════════════

echo ""
echo "══════════════════════════════════════════════════════════════════════"
echo "SPEECH-SPECIFIC INSIGHTS"
echo "══════════════════════════════════════════════════════════════════════"
echo ""

# Analyze speech patterns
ANALYSIS_SCRIPT="$OUTPUT_DIR/_speech_analysis.py"
cat > "$ANALYSIS_SCRIPT" << 'PYTHON_EOF'
#!/usr/bin/env python3
"""
Speech-specific analysis on top of investigation results.
"""

import json
import sqlite3
from pathlib import Path
from collections import Counter, defaultdict
import sys

def analyze_speech_patterns(output_dir):
    """Analyze patterns specific to speech data."""
    
    output_path = Path(output_dir)
    
    # Load discovery results
    discovery_file = output_path / "reports/discovery.json"
    if not discovery_file.exists():
        print("✗ No discovery results found")
        return
    
    with open(discovery_file) as f:
        discovery = json.load(f)
    
    # Load claims from database
    db_file = output_path / "graphs/memory.db"
    if not db_file.exists():
        print("✗ No claims database found")
        return
    
    conn = sqlite3.connect(str(db_file))
    cursor = conn.cursor()
    
    # Speech-specific metrics
    print("🎙  SPEECH PATTERN ANALYSIS")
    print("─" * 70)
    print()
    
    # 1. Repetition patterns (common in speech)
    print("1️⃣  REPETITION PATTERNS (unique to speech):")
    cursor.execute("""
        SELECT predicate, COUNT(*) as repetitions
        FROM claims
        GROUP BY predicate
        HAVING repetitions > 5
        ORDER BY repetitions DESC
        LIMIT 10
    """)
    
    print("   Top repeated predicates (filler patterns):")
    for predicate, count in cursor.fetchall():
        print(f"     '{predicate}' repeated {count}x")
    print()
    
    # 2. Entity co-occurrence (speaker relationships)
    print("2️⃣  ENTITY RELATIONSHIPS (who's connected):")
    cursor.execute("""
        SELECT subject, object, COUNT(*) as connections
        FROM claims
        WHERE subject != object
        GROUP BY subject, object
        HAVING connections > 3
        ORDER BY connections DESC
        LIMIT 10
    """)
    
    print("   Top entity pairs (potential speaker clusters):")
    for subj, obj, count in cursor.fetchall():
        print(f"     {subj} ↔ {obj} ({count} connections)")
    print()
    
    # 3. Contradiction clusters (from hypotheses)
    print("3️⃣  CONTRADICTION CLUSTERS:")
    signals_by_type = defaultdict(list)
    for signal in discovery.get("signals", []):
        signals_by_type[signal["type"]].append(signal)
    
    if signals_by_type:
        for sig_type, sigs in list(signals_by_type.items())[:5]:
            print(f"   {sig_type}: {len(sigs)} instances")
    else:
        print("   No contradiction signals detected")
    print()
    
    # 4. Hypothesis quality for speech
    print("4️⃣  HYPOTHESIS QUALITY (speech-specific):")
    hypotheses = discovery.get("hypotheses", [])
    
    if hypotheses:
        # Count hypothesis types
        h_types = Counter(h["type"] for h in hypotheses)
        print("   Hypothesis types detected:")
        for h_type, count in h_types.most_common():
            print(f"     {h_type}: {count}")
        
        print()
        print("   Top 3 hypotheses:")
        for i, h in enumerate(hypotheses[:3], 1):
            print(f"     {i}. {h['name']}")
            print(f"        score: {h.get('investigation_score', 0):.3f}")
    else:
        print("   No hypotheses generated")
    
    print()
    
    conn.close()
    
    print("─" * 70)
    print()
    print("💡 INTERPRETATION GUIDE:")
    print()
    print("  • High repetition → conversational patterns, emphasis")
    print("  • Entity clusters → speaker alliances, topic focus")
    print("  • Contradictions → competing narratives, memory drift")
    print("  • Alias hypotheses → same person/thing, different names")
    print()

if __name__ == "__main__":
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "investigation_out"
    analyze_speech_patterns(output_dir)
PYTHON_EOF

python3 "$ANALYSIS_SCRIPT" "$OUTPUT_DIR"

echo "══════════════════════════════════════════════════════════════════════"
echo "SPEECH INVESTIGATION COMPLETE"
echo "══════════════════════════════════════════════════════════════════════"
echo ""
echo "Output structure:"
echo "  $OUTPUT_DIR/"
echo "    transcripts/         - Audio transcriptions (.txt + .json)"
echo "    symbolic/            - Entity/Canon/Graph"
echo "    graphs/              - Claims database + stable/fragile/conflict"
echo "    reports/             - Discovery + tested hypotheses"
echo "    interventions/       - Structural patches (if needed)"
echo ""
echo "Speech-specific outputs:"
echo "  - Transcripts preserve timestamps + segments"
echo "  - Entity graph shows speaker relationships"
echo "  - Hypotheses detect contradictions across speech"
echo "  - Repetition patterns identify emphasis/filler"
echo ""
echo "✓ Speech → Investigation pipeline complete"
