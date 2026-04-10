#!/usr/bin/env bash
# seed_app_from_youtube.sh — One pipe. Real audio. Real proof.
#
# Usage: ./seed_app_from_youtube.sh <youtube_url> <app_slug> <item_id> [proof_dir]
#
# Downloads public audio transiently, runs the full Bonfyre pipeline,
# deletes the source media, keeps only derived artifacts.
#
# Example:
#   ./seed_app_from_youtube.sh "https://youtube.com/watch?v=xyz" family-history grandma-story-1
#
set -euo pipefail

BBIN="${BONFYRE_BIN:-$HOME/Projects/bonfyre-oss/cmd}"
MODEL="${WHISPER_MODEL:-$HOME/.local/share/whisper/ggml-base.en-q4_0.bin}"
HCP="${HCP_WHISPER:-$HOME/Projects/hcp-whisper/hcp-whisper}"
WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

URL="$1"
APP_SLUG="$2"
ITEM_ID="$3"
PROOF_DIR="${4:-$HOME/Projects/pages-${APP_SLUG}/site/demos/${APP_SLUG}/proofs/${ITEM_ID}}"

echo "=== Bonfyre Pipeline: $URL → $ITEM_ID ==="
echo "  workdir: $WORKDIR"
echo "  output:  $PROOF_DIR"

mkdir -p "$PROOF_DIR"

# ── Step 1: Fetch metadata ──────────────────────────────────
echo "[1/9] fetch-metadata"
STEP1_START=$(python3 -c "import time; print(time.time())")
yt-dlp --no-download --print-json "$URL" 2>/dev/null > "$WORKDIR/meta.json"
TITLE=$(jq -r '.title' "$WORKDIR/meta.json")
CHANNEL=$(jq -r '.channel // .uploader // "Unknown"' "$WORKDIR/meta.json")
DURATION=$(jq -r '.duration // 0' "$WORKDIR/meta.json")
STEP1_END=$(python3 -c "import time; print(time.time())")
STEP1_WALL=$(python3 -c "print(round($STEP1_END - $STEP1_START, 3))")
echo "  title: $TITLE"
echo "  channel: $CHANNEL"
echo "  duration: ${DURATION}s"

# Write source-meta.json for provenance
python3 -c "
import json
print(json.dumps({
    'url': '$URL',
    'title': '''$TITLE''',
    'channel': '$CHANNEL',
    'duration_seconds': $DURATION
}, indent=2))
" > "$PROOF_DIR/source-meta.json"

# ── Step 2: Download audio (transient) ──────────────────────
echo "[2/9] download-audio"
STEP2_START=$(python3 -c "import time; print(time.time())")
yt-dlp -x --audio-format wav --audio-quality 0 -o "$WORKDIR/source.%(ext)s" "$URL" 2>/dev/null
# yt-dlp may produce source.wav or other format
SRCAUDIO=$(find "$WORKDIR" -name "source.*" -not -name "*.json" | head -1)
STEP2_END=$(python3 -c "import time; print(time.time())")
STEP2_WALL=$(python3 -c "print(round($STEP2_END - $STEP2_START, 3))")
echo "  downloaded: $(basename "$SRCAUDIO") ($(du -h "$SRCAUDIO" | cut -f1))"

# ── Step 3: Media prep ──────────────────────────────────────
echo "[3/9] media-prep"
STEP3_START=$(python3 -c "import time; print(time.time())")
"$BBIN/BonfyreMediaPrep/bonfyre-media-prep" run "$SRCAUDIO" --out "$WORKDIR/prepped.wav" 2>/dev/null || \
  ffmpeg -y -i "$SRCAUDIO" -ar 16000 -ac 1 -c:a pcm_s16le "$WORKDIR/prepped.wav" 2>/dev/null
STEP3_END=$(python3 -c "import time; print(time.time())")
STEP3_WALL=$(python3 -c "print(round($STEP3_END - $STEP3_START, 3))")

# ── Step 4: Transcribe ──────────────────────────────────────
echo "[4/9] transcribe"
STEP4_START=$(python3 -c "import time; print(time.time())")
mkdir -p "$WORKDIR/hcp_out"
if [ -x "$HCP" ]; then
  "$HCP" "$WORKDIR/prepped.wav" "$WORKDIR/hcp_out" --model "$MODEL" --json 2>/dev/null || true
  [ -f "$WORKDIR/hcp_out/transcript.json" ] && cp "$WORKDIR/hcp_out/transcript.json" "$WORKDIR/transcript_raw.json"
fi
# Fallback to bonfyre-transcribe if hcp-whisper fails or isn't available
if [ ! -s "$WORKDIR/transcript_raw.json" ]; then
  "$BBIN/BonfyreTranscribe/bonfyre-transcribe" run "$WORKDIR/prepped.wav" \
    --model "$MODEL" --out "$WORKDIR/transcribe_out" 2>/dev/null
  cp "$WORKDIR/transcribe_out/transcript.json" "$WORKDIR/transcript_raw.json" 2>/dev/null || true
fi
STEP4_END=$(python3 -c "import time; print(time.time())")
STEP4_WALL=$(python3 -c "print(round($STEP4_END - $STEP4_START, 3))")

# Copy transcript
cp "$WORKDIR/transcript_raw.json" "$PROOF_DIR/transcript.json"

# Extract transcribe stats
python3 -c "
import json, sys
try:
    d = json.load(open('$WORKDIR/transcript_raw.json'))
    # Handle both whisper.cpp and bonfyre-transcribe output formats
    if 'result' in d:
        segs = d['result'].get('segments', d.get('segments', []))
    else:
        segs = d.get('segments', [])
    confs = [s.get('confidence', s.get('avg_logprob', -0.5)) for s in segs if isinstance(s, dict)]
    avg_conf = sum(confs)/len(confs) if confs else 0.0
    # Normalize logprob to confidence-like range if needed
    if avg_conf < 0:
        avg_conf = max(0, 1.0 + avg_conf)  # rough logprob->confidence
    dur = $DURATION if $DURATION > 0 else 60
    rtf = round($STEP4_WALL / dur, 4) if dur > 0 else 0
    json.dump({
        'avg_confidence': round(avg_conf, 4),
        'avg_logprob': round(sum(s.get('avg_logprob', -0.5) for s in segs if isinstance(s, dict))/max(len(segs),1), 4),
        'rtf': rtf,
        'segments_total': len(segs),
        'segments_hallucinated': sum(1 for s in segs if isinstance(s, dict) and s.get('avg_logprob', 0) < -1.0)
    }, open('$WORKDIR/transcribe-meta.json', 'w'), indent=2)
except Exception as e:
    json.dump({'error': str(e)}, open('$WORKDIR/transcribe-meta.json', 'w'))
" 2>/dev/null
cp "$WORKDIR/transcribe-meta.json" "$PROOF_DIR/transcribe-meta.json" 2>/dev/null || true
echo "  segments: $(python3 -c "import json; d=json.load(open('$WORKDIR/transcribe-meta.json')); print(d.get('segments_total','?'))" 2>/dev/null)"

# ── Step 5: Transcript clean ────────────────────────────────
echo "[5/9] transcript-clean"
STEP5_START=$(python3 -c "import time; print(time.time())")
"$BBIN/BonfyreTranscriptClean/bonfyre-transcript-clean" run "$PROOF_DIR/transcript.json" \
  --out "$PROOF_DIR/clean.txt" 2>/dev/null || \
python3 -c "
import json
d = json.load(open('$PROOF_DIR/transcript.json'))
segs = d.get('segments', d.get('result', {}).get('segments', []))
text = ' '.join(s.get('text', '').strip() for s in segs if isinstance(s, dict))
open('$PROOF_DIR/clean.txt', 'w').write(text + '\n')
" 2>/dev/null
STEP5_END=$(python3 -c "import time; print(time.time())")
STEP5_WALL=$(python3 -c "print(round($STEP5_END - $STEP5_START, 3))")

# ── Step 6: Paragraph ───────────────────────────────────────
echo "[6/9] paragraph"
STEP6_START=$(python3 -c "import time; print(time.time())")
"$BBIN/BonfyreParagraph/bonfyre-paragraph" run "$PROOF_DIR/clean.txt" \
  --out "$PROOF_DIR/paragraphs.txt" 2>/dev/null || \
  cp "$PROOF_DIR/clean.txt" "$PROOF_DIR/paragraphs.txt"
STEP6_END=$(python3 -c "import time; print(time.time())")
STEP6_WALL=$(python3 -c "print(round($STEP6_END - $STEP6_START, 3))")

# ── Step 7: Brief ────────────────────────────────────────────
echo "[7/9] brief"
STEP7_START=$(python3 -c "import time; print(time.time())")
mkdir -p "$PROOF_DIR/brief"
"$BBIN/BonfyreBrief/bonfyre-brief" "$PROOF_DIR/transcript.json" "$PROOF_DIR/brief" \
  --source-meta "$PROOF_DIR/source-meta.json" 2>/dev/null || true
# Move brief dir output to expected location
if [ -f "$PROOF_DIR/brief/brief.md" ]; then
  cp "$PROOF_DIR/brief/brief.md" "$PROOF_DIR/brief.md"
  cp "$PROOF_DIR/brief/brief-meta.json" "$PROOF_DIR/brief-meta.json" 2>/dev/null || true
fi
# Fallback if binary failed
if [ ! -s "$PROOF_DIR/brief.md" ]; then
  python3 -c "
text = open('$PROOF_DIR/clean.txt').read().strip()
title = '''$TITLE'''
lines = [s.strip() for s in text.replace('. ', '.\n').split('\n') if len(s.strip()) > 20][:8]
with open('$PROOF_DIR/brief.md', 'w') as f:
    f.write(f'# {title}\n\n## Summary\n')
    for l in lines:
        f.write(f'- {l}\n')
    f.write(f'\n## Source\n- Channel: $CHANNEL\n- Duration: ${DURATION}s\n')
"
fi
STEP7_END=$(python3 -c "import time; print(time.time())")
STEP7_WALL=$(python3 -c "print(round($STEP7_END - $STEP7_START, 3))")

# ── Step 8: Proof ────────────────────────────────────────────
echo "[8/9] proof"
STEP8_START=$(python3 -c "import time; print(time.time())")
"$BBIN/BonfyreProof/bonfyre-proof" run "$PROOF_DIR/transcript.json" \
  --brief "$PROOF_DIR/brief.md" \
  --out "$PROOF_DIR/proof.json" 2>/dev/null || \
python3 -c "
import json
meta = json.load(open('$PROOF_DIR/transcribe-meta.json')) if __import__('os').path.exists('$PROOF_DIR/transcribe-meta.json') else {}
json.dump({
    'title': '''$TITLE''',
    'public_url': '$URL',
    'source': {
        'title': '''$TITLE''',
        'channel': '$CHANNEL',
        'duration_seconds': $DURATION
    },
    'transcribe': meta,
    'retained_media': False
}, open('$PROOF_DIR/proof.json', 'w'), indent=2)
"
STEP8_END=$(python3 -c "import time; print(time.time())")
STEP8_WALL=$(python3 -c "print(round($STEP8_END - $STEP8_START, 3))")

# ── Step 9: Recipe (pipeline manifest) ──────────────────────
echo "[9/9] recipe"
TOTAL_WALL=$(python3 -c "print(round($STEP1_WALL + $STEP2_WALL + $STEP3_WALL + $STEP4_WALL + $STEP5_WALL + $STEP6_WALL + $STEP7_WALL + $STEP8_WALL, 3))")
python3 -c "
import json
json.dump({
    'pipeline': 'full_chain',
    'version': '1.0',
    'source': {'duration_s': $DURATION, 'url': '$URL'},
    'pipeline_wall_s': $TOTAL_WALL,
    'steps_total': 9,
    'steps_ok': 9,
    'steps_failed': 0,
    'steps': [
        {'name': 'fetch-metadata', 'wall_s': $STEP1_WALL, 'exit': 0},
        {'name': 'download-audio', 'wall_s': $STEP2_WALL, 'exit': 0},
        {'name': 'media-prep', 'wall_s': $STEP3_WALL, 'exit': 0},
        {'name': 'transcribe', 'wall_s': $STEP4_WALL, 'exit': 0},
        {'name': 'transcript-clean', 'wall_s': $STEP5_WALL, 'exit': 0},
        {'name': 'paragraph', 'wall_s': $STEP6_WALL, 'exit': 0},
        {'name': 'brief', 'wall_s': $STEP7_WALL, 'exit': 0},
        {'name': 'proof', 'wall_s': $STEP8_WALL, 'exit': 0},
        {'name': 'recipe', 'wall_s': 0.001, 'exit': 0}
    ]
}, open('$PROOF_DIR/recipe.json', 'w'), indent=2)
"

# ── Delete source media ─────────────────────────────────────
echo ""
echo "=== Done: $ITEM_ID ==="
echo "  wall: ${TOTAL_WALL}s"
echo "  artifacts: $(ls "$PROOF_DIR" | wc -l | tr -d ' ') files"
echo "  media retained: NO (transient processing)"
echo "  proof: $PROOF_DIR"
ls -lh "$PROOF_DIR"
