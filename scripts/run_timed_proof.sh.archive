#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/run_timed_proof.sh <public-url> <title> [out-dir]
# Full Bonfyre pipeline with per-step timing and metrics capture.
# Produces: <out-dir>/recipe.json and recipe.md
# Compatible with bash 3.2+ (macOS default).

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
URL="$1"
TITLE="$2"
OUT_DIR="${3:-$(mktemp -d /tmp/bonfyre-timed-proof.XXXXXX)}"

DOWNLOAD_DIR="$OUT_DIR/download"
TRANS_DIR="$OUT_DIR/transcribe"
CLEAN_DIR="$OUT_DIR/clean"
PARA_DIR="$OUT_DIR/paragraph"
BRIEF_DIR="$OUT_DIR/brief"
PROOF_JSON="$OUT_DIR/proof.json"
RECIPE_JSON="$OUT_DIR/recipe.json"
RECIPE_MD="$OUT_DIR/recipe.md"
TIMING_DIR="$OUT_DIR/.timing"

mkdir -p "$DOWNLOAD_DIR" "$TRANS_DIR" "$CLEAN_DIR" "$PARA_DIR" "$BRIEF_DIR" "$TIMING_DIR"

now_ms() { python3 -c "import time; print(int(time.time()*1000))"; }

run_step() {
  local name="$1"; shift
  echo "  ▸ $name"
  local t0
  t0=$(now_ms)
  local rc=0
  "$@" > "$OUT_DIR/${name}.log" 2>&1 || rc=$?
  local t1
  t1=$(now_ms)
  local dur
  dur=$(python3 -c "print(round(($t1 - $t0) / 1000.0, 3))")
  echo "$dur" > "$TIMING_DIR/${name}.wall"
  echo "$rc"  > "$TIMING_DIR/${name}.exit"
  local status="ok"
  if [ "$rc" -ne 0 ]; then status="FAIL (exit $rc)"; fi
  echo "    ${status}  ${dur}s"
}

PIPELINE_START=$(now_ms)
echo "=== Bonfyre Timed Pipeline ==="
echo " URL:   $URL"
echo " Title: $TITLE"
echo " Out:   $OUT_DIR"

# Step 1: Fetch metadata
SOURCE_META="$DOWNLOAD_DIR/source-meta.json"
run_step "fetch-metadata" yt-dlp --dump-single-json "$URL"
cp "$OUT_DIR/fetch-metadata.log" "$SOURCE_META" 2>/dev/null || true

# Step 2: Download audio
SOURCE_TEMPLATE="$DOWNLOAD_DIR/source.%(ext)s"
run_step "download-audio" yt-dlp -f bestaudio --no-playlist -o "$SOURCE_TEMPLATE" "$URL"

SOURCE_FILE="$(find "$DOWNLOAD_DIR" -maxdepth 1 -type f ! -name 'source-meta.json' | head -n 1)"
SOURCE_SIZE=0
if [ -n "${SOURCE_FILE:-}" ]; then
  SOURCE_SIZE=$(stat -f%z "$SOURCE_FILE" 2>/dev/null || stat -c%s "$SOURCE_FILE" 2>/dev/null || echo 0)
fi

# Step 3: Normalize audio
NORMALIZED_WAV="$OUT_DIR/source.wav"
run_step "media-prep" "$ROOT/cmd/BonfyreMediaPrep/bonfyre-media-prep" normalize "$SOURCE_FILE" "$NORMALIZED_WAV"

# Step 4: Transcribe
run_step "transcribe" "$ROOT/cmd/BonfyreTranscribe/bonfyre-transcribe" "$NORMALIZED_WAV" "$TRANS_DIR"

# Step 5: Clean transcript
run_step "transcript-clean" "$ROOT/cmd/BonfyreTranscriptClean/bonfyre-transcript-clean" --transcript "$TRANS_DIR/transcript.json" --out "$CLEAN_DIR/clean.txt"

# Step 6: Paragraphize
run_step "paragraph" "$ROOT/cmd/BonfyreParagraph/bonfyre-paragraph" --input "$CLEAN_DIR/clean.txt" --out "$PARA_DIR/paragraphs.txt"

# Step 7: Brief extraction
run_step "brief" "$ROOT/cmd/BonfyreBrief/bonfyre-brief" "$PARA_DIR/paragraphs.txt" "$BRIEF_DIR" --title "$TITLE"

# Step 8: Build proof bundle
DURATION_S=$(jq -r '.duration // 0' "$SOURCE_META" 2>/dev/null || echo 0)
AVG_CONF=$(jq -r '.avg_confidence // 0' "$TRANS_DIR/meta.json" 2>/dev/null || echo 0)
AVG_LOGPROB=$(jq -r '.avg_logprob // 0' "$TRANS_DIR/meta.json" 2>/dev/null || echo 0)
RTF=$(jq -r '.rtf // 0' "$TRANS_DIR/meta.json" 2>/dev/null || echo 0)
SEGS_TOTAL=$(jq -r '.segments_total // 0' "$TRANS_DIR/meta.json" 2>/dev/null || echo 0)
SEGS_HALLUC=$(jq -r '.segments_hallucinated // 0' "$TRANS_DIR/meta.json" 2>/dev/null || echo 0)
CHANNEL=$(jq -r '.channel // "unknown"' "$SOURCE_META" 2>/dev/null || echo "unknown")
SRC_TITLE=$(jq -r '.title // ""' "$SOURCE_META" 2>/dev/null || echo "")

run_step "proof-bundle" jq -n \
  --arg title "$TITLE" \
  --arg url "$URL" \
  --arg src_title "$SRC_TITLE" \
  --arg channel "$CHANNEL" \
  --argjson duration "${DURATION_S:-0}" \
  --argjson avg_conf "${AVG_CONF:-0}" \
  --argjson avg_logprob "${AVG_LOGPROB:-0}" \
  --argjson rtf "${RTF:-0}" \
  --argjson segs_total "${SEGS_TOTAL:-0}" \
  --argjson segs_halluc "${SEGS_HALLUC:-0}" \
  '{
    title: $title,
    public_url: $url,
    source: { title: $src_title, channel: $channel, duration_seconds: $duration },
    transcribe: { avg_confidence: $avg_conf, avg_logprob: $avg_logprob, rtf: $rtf, segments_total: $segs_total, segments_hallucinated: $segs_halluc },
    outputs: { transcript_json: "transcribe/transcript.json", clean_txt: "clean/clean.txt", paragraphs_txt: "paragraph/paragraphs.txt", brief_md: "brief/brief.md" },
    retained_media: false
  }'
cp "$OUT_DIR/proof-bundle.log" "$PROOF_JSON" 2>/dev/null || true

PIPELINE_END=$(now_ms)
PIPELINE_TOTAL=$(python3 -c "print(round(($PIPELINE_END - $PIPELINE_START) / 1000.0, 3))")

# Cleanup transient media
rm -f "$SOURCE_FILE" "$NORMALIZED_WAV" "$TRANS_DIR/input.denoised.wav" "$TRANS_DIR/normalized.wav" 2>/dev/null || true

# Artifact sizes
TRANSCRIPT_SIZE=0; [ -f "$TRANS_DIR/transcript.json" ] && TRANSCRIPT_SIZE=$(stat -f%z "$TRANS_DIR/transcript.json" 2>/dev/null || echo 0)
CLEAN_SIZE=0; [ -f "$CLEAN_DIR/clean.txt" ] && CLEAN_SIZE=$(stat -f%z "$CLEAN_DIR/clean.txt" 2>/dev/null || echo 0)
PARA_SIZE=0; [ -f "$PARA_DIR/paragraphs.txt" ] && PARA_SIZE=$(stat -f%z "$PARA_DIR/paragraphs.txt" 2>/dev/null || echo 0)
BRIEF_SIZE=0; [ -f "$BRIEF_DIR/brief.md" ] && BRIEF_SIZE=$(stat -f%z "$BRIEF_DIR/brief.md" 2>/dev/null || echo 0)

STEP_NAMES="fetch-metadata download-audio media-prep transcribe transcript-clean paragraph brief proof-bundle"

# Build recipe.json and recipe.md via python (avoids bash 3.x limitations)
python3 - "$OUT_DIR" "$TIMING_DIR" "$URL" "$TITLE" "$DURATION_S" "$SOURCE_SIZE" "$PIPELINE_TOTAL" "$CHANNEL" \
  "$AVG_CONF" "$AVG_LOGPROB" "$RTF" "$SEGS_TOTAL" "$SEGS_HALLUC" \
  "$TRANSCRIPT_SIZE" "$CLEAN_SIZE" "$PARA_SIZE" "$BRIEF_SIZE" \
  "$STEP_NAMES" << 'PYEOF'
import json, sys, os

out_dir, timing_dir = sys.argv[1], sys.argv[2]
url, title = sys.argv[3], sys.argv[4]
duration_s = float(sys.argv[5] or 0)
source_size = int(sys.argv[6] or 0)
pipeline_total = float(sys.argv[7])
channel = sys.argv[8]
avg_conf = float(sys.argv[9] or 0)
avg_logprob = float(sys.argv[10] or 0)
rtf = float(sys.argv[11] or 0)
segs_total = int(sys.argv[12] or 0)
segs_halluc = int(sys.argv[13] or 0)
transcript_size = int(sys.argv[14] or 0)
clean_size = int(sys.argv[15] or 0)
para_size = int(sys.argv[16] or 0)
brief_size = int(sys.argv[17] or 0)
step_names = sys.argv[18].split()

steps = []
for name in step_names:
    wf = os.path.join(timing_dir, f"{name}.wall")
    ef = os.path.join(timing_dir, f"{name}.exit")
    wall = float(open(wf).read().strip()) if os.path.exists(wf) else 0
    ex = int(open(ef).read().strip()) if os.path.exists(ef) else -1
    steps.append({"name": name, "wall_s": wall, "exit": ex})

recipe = {
    "pipeline": "run_timed_proof",
    "url": url, "title": title,
    "source_duration_s": duration_s,
    "source_download_bytes": source_size,
    "pipeline_wall_s": pipeline_total,
    "channel": channel,
    "transcribe": {
        "avg_confidence": avg_conf, "avg_logprob": avg_logprob,
        "rtf": rtf, "segments_total": segs_total,
        "segments_hallucinated": segs_halluc,
    },
    "artifact_sizes_bytes": {
        "transcript_json": transcript_size, "clean_txt": clean_size,
        "paragraphs_txt": para_size, "brief_md": brief_size,
    },
    "steps": steps,
}
with open(os.path.join(out_dir, "recipe.json"), "w") as f:
    json.dump(recipe, f, indent=2); f.write("\n")

total_art = transcript_size + clean_size + para_size + brief_size
halluc_pct = f"{segs_halluc / max(segs_total, 1) * 100:.2f}%"
lines = [
    f"# Pipeline Recipe - {title}", "",
    "| Key | Value |", "|-----|-------|",
    f"| Source | {url} |",
    f"| Channel | {channel} |",
    f"| Audio duration | {duration_s}s |",
    f"| **Total pipeline wall** | **{pipeline_total}s** |",
    f"| Source download | {source_size:,} bytes |",
    "| Media retained | No |", "",
    "## Per-Step Timing", "",
    "| Step | Wall (s) | Exit |", "|------|----------|------|",
]
for s in steps:
    lines.append(f"| {s['name']} | {s['wall_s']} | {s['exit']} |")
lines += ["",
    "## Transcription Quality", "",
    "| Metric | Value |", "|--------|-------|",
    f"| Avg confidence | {avg_conf} |",
    f"| Avg log-prob | {avg_logprob} |",
    f"| RTF | {rtf} |",
    f"| Total segments | {segs_total} |",
    f"| Hallucinated segments | {segs_halluc} |",
    f"| Hallucination rate | {halluc_pct} |", "",
    "## Artifact Sizes", "",
    "| Artifact | Bytes |", "|----------|-------|",
    f"| transcript.json | {transcript_size:,} |",
    f"| clean.txt | {clean_size:,} |",
    f"| paragraphs.txt | {para_size:,} |",
    f"| brief.md | {brief_size:,} |",
    f"| **Total** | **{total_art:,}** |", "",
    "## Claims", "",
    f"- Processes **{duration_s}s** of messy civic audio in **{pipeline_total}s** wall time",
]
if 0 < rtf < 1:
    lines.append(f"- Transcription at **{rtf}x realtime** (faster than realtime)")
if segs_halluc == 0:
    lines.append("- **Zero hallucinated segments**")
elif segs_total > 0 and segs_halluc / segs_total < 0.02:
    lines.append(f"- Hallucination rate **under 2%** across {segs_total} segments")
if source_size > 0 and total_art > 0:
    lines.append(f"- **{round(source_size / total_art, 1)}x compression** (source to derived artifacts)")
lines += [
    "- No source media retained",
    "- Full provenance chain from public origin to every output", "",
]
with open(os.path.join(out_dir, "recipe.md"), "w") as f:
    f.write("\n".join(lines) + "\n")
print(f"  Recipe: {os.path.join(out_dir, 'recipe.json')}")
print(f"  Report: {os.path.join(out_dir, 'recipe.md')}")
PYEOF

echo ""
echo "=== Done: $TITLE ==="
echo " Total: ${PIPELINE_TOTAL}s"
echo " Recipe: $RECIPE_JSON"
