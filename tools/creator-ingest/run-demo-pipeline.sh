#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# run-demo-pipeline.sh — runs ingested creator audio through
# the full Bonfyre demo pipeline using NATIVE binaries.
#
# Chain:  bonfyre-transcribe → bonfyre-transcript-clean → bonfyre-brief
#         → bonfyre-proof score → bonfyre-tag → bonfyre-pack
#
# All binaries use plain text I/O. No JSON until proof scoring.
#
# Input:  ./output/<creator-slug>/<video-id>/ (from ingest.sh)
# Output: ./output/<creator-slug>/<video-id>/demo/
#           ├── transcribe/           (bonfyre-transcribe output)
#           │   ├── normalized.txt    (plain text transcript)
#           │   ├── normalized.wav    (16kHz mono preprocessed)
#           │   └── meta.json
#           ├── clean.txt             (bonfyre-transcript-clean)
#           ├── brief/                (bonfyre-brief output)
#           │   ├── brief.md
#           │   ├── brief-meta.json
#           │   └── artifact.json
#           ├── proof/                (bonfyre-proof score output)
#           │   ├── proof-summary.json
#           │   ├── proof-review.json
#           │   └── deliverable.md
#           ├── tags/                 (bonfyre-tag output)
#           └── pack/                 (bonfyre-pack deliverable)
#
# Usage:
#   ./run-demo-pipeline.sh                    # all ingested videos
#   ./run-demo-pipeline.sh --creator ali-abdaal
#   ./run-demo-pipeline.sh --dry-run
# ──────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/output"
CREATOR_FILTER=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --creator)  CREATOR_FILTER="$2"; shift 2 ;;
    --dry-run)  DRY_RUN=true; shift ;;
    --output)   OUTPUT_DIR="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

BIN_DIR="${BONFYRE_BIN:-$HOME/.local/bin}"

# Verify native binaries
missing=0
for bin in bonfyre-transcribe bonfyre-transcript-clean bonfyre-brief bonfyre-proof bonfyre-tag bonfyre-pack; do
  if [[ ! -x "$BIN_DIR/$bin" ]]; then
    echo "ERROR: $bin not found in $BIN_DIR"
    missing=$((missing + 1))
  fi
done
if [[ $missing -gt 0 ]]; then
  echo "Run: cd bonfyre-oss && make && make install"
  exit 1
fi

total=0
processed=0

for creator_dir in "$OUTPUT_DIR"/*/; do
  [[ -d "$creator_dir" ]] || continue
  creator_slug=$(basename "$creator_dir")

  if [[ -n "$CREATOR_FILTER" ]] && [[ "$creator_slug" != *"$CREATOR_FILTER"* ]]; then continue; fi

  for vid_dir in "$creator_dir"*/; do
    [[ -d "$vid_dir" ]] || continue
    [[ -f "$vid_dir/normalized.wav" ]] || [[ -f "$vid_dir/downloaded.wav" ]] || continue

    vid_id=$(basename "$vid_dir")
    total=$((total + 1))

    # Find the audio file (prefer normalized from ingest)
    if [[ -f "$vid_dir/normalized.wav" ]]; then
      audio="$vid_dir/normalized.wav"
    else
      audio="$vid_dir/downloaded.wav"
    fi

    demo_dir="$vid_dir/demo"

    # Skip if fully processed (pack dir exists with bundle)
    if [[ -d "$demo_dir/pack" ]] && [[ -f "$demo_dir/pack/pack-manifest.json" ]]; then
      echo "[$total] $creator_slug/$vid_id — already processed, skipping"
      continue
    fi

    echo "━━━ [$total] $creator_slug/$vid_id ━━━"
    echo "  Audio: $audio"

    if $DRY_RUN; then
      echo "  [dry-run] transcribe → clean → brief → proof score → tag → proof bundle → offer → pack"
      continue
    fi

    mkdir -p "$demo_dir"

    # ── Step 1: bonfyre-transcribe <audio> <output-dir> ──
    # Outputs: demo/transcribe/normalized.txt (plain text)
    transcript_txt="$demo_dir/transcribe/normalized.txt"
    if [[ ! -f "$transcript_txt" ]]; then
      echo "  [1/8] bonfyre-transcribe..."
      "$BIN_DIR/bonfyre-transcribe" "$audio" "$demo_dir/transcribe" \
        --media-prep-binary "$BIN_DIR/bonfyre-media-prep" 2>&1 | tail -5 || {
        echo "  WARN: bonfyre-transcribe failed"
        transcript_txt=""
      }
    else
      echo "  [1/8] bonfyre-transcribe — cached"
    fi

    # ── Step 2: bonfyre-transcript-clean --transcript <.txt> --out <.txt> ──
    clean_txt="$demo_dir/clean.txt"
    if [[ -n "$transcript_txt" ]] && [[ -f "$transcript_txt" ]] && [[ ! -f "$clean_txt" ]]; then
      echo "  [2/8] bonfyre-transcript-clean..."
      "$BIN_DIR/bonfyre-transcript-clean" \
        --transcript "$transcript_txt" \
        --out "$clean_txt" 2>&1 | tail -3 || {
        echo "  WARN: bonfyre-transcript-clean failed"
        clean_txt=""
      }
    elif [[ -f "$clean_txt" ]]; then
      echo "  [2/8] bonfyre-transcript-clean — cached"
    fi

    # Choose best text input for downstream (prefer cleaned)
    text_input=""
    [[ -f "$clean_txt" ]] && text_input="$clean_txt"
    [[ -z "$text_input" && -f "$transcript_txt" ]] && text_input="$transcript_txt"

    # ── Step 3: bonfyre-brief <text-file> <output-dir> ──
    # Outputs: demo/brief/brief.md, brief-meta.json, artifact.json
    if [[ -n "$text_input" ]] && [[ ! -d "$demo_dir/brief" ]]; then
      echo "  [3/8] bonfyre-brief..."
      "$BIN_DIR/bonfyre-brief" "$text_input" "$demo_dir/brief" 2>&1 | tail -3 || echo "  WARN: bonfyre-brief failed"
    elif [[ -d "$demo_dir/brief" ]]; then
      echo "  [3/8] bonfyre-brief — cached"
    fi

    # ── Step 4: bonfyre-proof score <brief-dir> <output-dir> ──
    # Outputs: demo/proof/proof-summary.json, proof-review.json, deliverable.md
    if [[ -d "$demo_dir/brief" ]] && [[ ! -d "$demo_dir/proof" ]]; then
      echo "  [4/8] bonfyre-proof score..."
      # Copy transcript into brief dir so proof can find it
      [[ -f "$transcript_txt" ]] && cp "$transcript_txt" "$demo_dir/brief/transcript.txt" 2>/dev/null
      "$BIN_DIR/bonfyre-proof" score "$demo_dir/brief" "$demo_dir/proof" 2>&1 | tail -3 || echo "  WARN: bonfyre-proof failed"
    elif [[ -d "$demo_dir/proof" ]]; then
      echo "  [4/8] bonfyre-proof — cached"
    fi

    # ── Step 5: bonfyre-tag detect-lang <text-file> [output-dir] ──
    if [[ -n "$text_input" ]] && [[ ! -d "$demo_dir/tags" ]]; then
      echo "  [5/8] bonfyre-tag..."
      "$BIN_DIR/bonfyre-tag" detect-lang "$text_input" "$demo_dir/tags" 2>&1 | tail -3 || echo "  WARN: bonfyre-tag failed"
    elif [[ -d "$demo_dir/tags" ]]; then
      echo "  [5/8] bonfyre-tag — cached"
    fi

    # ── Step 6a: bonfyre-proof bundle <proof-dir> <output-dir> ──
    # Generates proof-bundle.json needed by bonfyre-offer/pack
    if [[ -d "$demo_dir/proof" ]] && [[ ! -f "$demo_dir/proof/proof-bundle.json" ]]; then
      echo "  [6/8] bonfyre-proof bundle..."
      "$BIN_DIR/bonfyre-proof" bundle "$demo_dir/proof" "$demo_dir/proof" 2>&1 | tail -3 || echo "  WARN: bonfyre-proof bundle failed"
    elif [[ -f "$demo_dir/proof/proof-bundle.json" ]]; then
      echo "  [6/8] bonfyre-proof bundle — cached"
    fi

    # ── Step 6b: bonfyre-offer generate <proof-bundle.json> <output-dir> ──
    # Generates offer.json (pricing) from proof bundle
    if [[ -f "$demo_dir/proof/proof-bundle.json" ]] && [[ ! -d "$demo_dir/offer" ]]; then
      echo "  [7/8] bonfyre-offer..."
      "$BIN_DIR/bonfyre-offer" generate "$demo_dir/proof/proof-bundle.json" "$demo_dir/offer" 2>&1 | tail -3 || echo "  WARN: bonfyre-offer failed"
    elif [[ -d "$demo_dir/offer" ]]; then
      echo "  [7/8] bonfyre-offer — cached"
    fi

    # ── Step 7: bonfyre-pack assemble <proof-dir> <offer-dir> <output-dir> ──
    if [[ -d "$demo_dir/proof" ]] && [[ -d "$demo_dir/offer" ]] && [[ ! -d "$demo_dir/pack" ]]; then
      echo "  [8/8] bonfyre-pack..."
      "$BIN_DIR/bonfyre-pack" assemble "$demo_dir/proof" "$demo_dir/offer" "$demo_dir/pack" 2>&1 | tail -3 || echo "  WARN: bonfyre-pack failed"
    elif [[ -d "$demo_dir/pack" ]]; then
      echo "  [8/8] bonfyre-pack — cached"
    fi

    processed=$((processed + 1))
    echo "  Done."

  done
done

echo ""
echo "━━━ Pipeline complete ━━━"
echo "  Total videos: $total"
echo "  Processed: $processed"
echo "  Output: $OUTPUT_DIR/**/demo/"
