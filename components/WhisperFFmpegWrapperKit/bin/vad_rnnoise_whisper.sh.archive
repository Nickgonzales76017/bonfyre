#!/usr/bin/env bash
# Prototype pipeline: VAD (ffmpeg silence split) -> RNNoise denoise -> Whisper wrapper
# Idempotent: uses temp dir and skips steps when outputs exist.

set -euo pipefail

usage() {
  cat <<EOF
Usage: $(basename "$0") --in INPUT_FILE [--out-dir OUT_DIR] [--whisper-cmd "<cmd>"]

Runs: normalize -> optional RNNoise -> call whisper (or print whisper command).

Options:
  --in INPUT_FILE     Input audio file (wav/mp3)
  --out-dir DIR       Output directory (default: ./outputs)
  --whisper-cmd CMD   Command to run whisper on a file, use {file} placeholder
                      e.g. "python3 ../main.py {file}"
  --keep-temp         Keep temporary files
  --help
EOF
}

IN=""
OUT_DIR="$(pwd)/outputs"
WHISPER_CMD=""
KEEP_TEMP=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --in) IN="$2"; shift 2;;
    --out-dir) OUT_DIR="$2"; shift 2;;
    --whisper-cmd) WHISPER_CMD="$2"; shift 2;;
    --keep-temp) KEEP_TEMP=1; shift;;
    --help) usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 1;;
  esac
done

if [[ -z "$IN" || ! -f "$IN" ]]; then
  echo "Input file required and must exist." >&2
  usage
  exit 2
fi

mkdir -p "$OUT_DIR"

TMPDIR=$(mktemp -d)
trap '[[ $KEEP_TEMP -eq 1 ]] || rm -rf "$TMPDIR"' EXIT

BASE=$(basename "$IN")
BASE_NOEXT="${BASE%.*}"

# 1) Normalize / downmix to 16k mono wav
NORM="$TMPDIR/${BASE_NOEXT}_16k_mono.wav"
if [[ ! -f "$NORM" ]]; then
  echo "[1/3] Normalizing to 16k mono: $NORM"
  ffmpeg -y -hide_banner -loglevel error -i "$IN" -ar 16000 -ac 1 -vn -f wav "$NORM"
else
  echo "Normalized file exists, skipping: $NORM"
fi

# 2) RNNoise denoise (optional)
DN="$TMPDIR/${BASE_NOEXT}_denoised.wav"
if command -v rnnoise_demo >/dev/null 2>&1; then
  if [[ ! -f "$DN" ]]; then
    echo "[2/3] Running RNNoise denoise to: $DN"
    rnnoise_demo "$NORM" "$DN" >/dev/null 2>&1 || (echo "rnnoise failed" >&2; cp "$NORM" "$DN")
  else
    echo "Denoised file exists, skipping: $DN"
  fi
else
  echo "[2/3] rnnoise_demo not found — skipping denoise (using normalized file)"
  DN="$NORM"
fi

# 3) Whisper (use provided cmd or try common wrapper)
OUT_TRANSCRIPT="$OUT_DIR/${BASE_NOEXT}.txt"
if [[ -n "$WHISPER_CMD" ]]; then
  CMD=${WHISPER_CMD//\{file\}/"$DN"}
  echo "[3/3] Running whisper command: $CMD"
  eval "$CMD"
  # Some whisper wrappers write separate outputs; move transcript if created
  if [[ -f "$OUT_DIR/${BASE_NOEXT}.txt" ]]; then
    echo "Transcript written: $OUT_TRANSCRIPT"
  fi
else
  # Try common wrapper locations
  if [[ -f "$(dirname "$0")/../main.py" ]]; then
    echo "[3/3] Found local wrapper main.py — running it"
    python3 "$(dirname "$0")/../main.py" "$DN" > "$OUT_TRANSCRIPT" || echo "whisper wrapper failed" >&2
    echo "Transcript written: $OUT_TRANSCRIPT"
  else
    echo "[3/3] No whisper command provided and no local wrapper found. Preview command:" >&2
    echo "  whisper.cpp --model /path/to/model --file '$DN' --out '$OUT_TRANSCRIPT'" >&2
    echo "Provide --whisper-cmd or place a wrapper at ../main.py to run automatically." >&2
  fi
fi

echo "Pipeline complete. Outputs in: $OUT_DIR"

exit 0
