#!/usr/bin/env bash
set -euo pipefail

usage(){
  cat <<EOF
Usage: $(basename "$0") --audio AUDIO_FILE [--out OUTDIR] [--stream-binary PATH] [--dry-run]

Runs a low-latency streaming ASR (RNNT-like) for live captions and falls back to
batch reprocessing with whisper wrapper for final accuracy. Dry-run friendly.

Options:
  --audio FILE         Input audio file (wav/mp3)
  --out DIR            Output directory (default: ./out)
  --stream-binary PATH Path to streaming ASR binary (optional)
  --dry-run            Print planned commands instead of executing
  --help
EOF
}

AUDIO=""
OUT="$(pwd)/out"
STREAM_BIN=""
DRY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --audio) AUDIO="$2"; shift 2;;
    --out) OUT="$2"; shift 2;;
    --stream-binary) STREAM_BIN="$2"; shift 2;;
    --dry-run) DRY=1; shift;;
    --help) usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 1;;
  esac
done

if [[ -z "$AUDIO" || ! -f "$AUDIO" ]]; then
  echo "--audio is required and must point to a file" >&2
  usage
  exit 2
fi

mkdir -p "$OUT"

base=$(basename "$AUDIO")
name="${base%.*}"

# Step 1: start streaming ASR (simulated for file input) -> live captions
LIVE_OUT="$OUT/${name}_live.txt"
if [[ -f "$LIVE_OUT" ]]; then
  echo "Live captions exist, skipping: $LIVE_OUT"
else
  if [[ -n "$STREAM_BIN" && -x "$STREAM_BIN" ]]; then
    cmd=("$STREAM_BIN" --input "$AUDIO" --stream)
  elif command -v rnnt-stream >/dev/null 2>&1; then
    cmd=(rnnt-stream --input "$AUDIO" --output "$LIVE_OUT")
  else
    cmd=(echo "rnnt-stream --input $AUDIO --output $LIVE_OUT")
  fi
  if [[ $DRY -eq 1 ]]; then
    echo "Would run: ${cmd[*]}"
  else
    echo "Running streaming ASR (simulated for file) -> $LIVE_OUT"
    if ! "${cmd[@]}"; then
      echo "Streaming ASR failed or not installed. See printed command to run manually." >&2
    fi
  fi
fi

# Step 2: Batch fallback with whisper for final accuracy
FINAL_OUT="$OUT/${name}_whisper.txt"
if [[ -f "$FINAL_OUT" ]]; then
  echo "Final whisper transcript exists, skipping: $FINAL_OUT"
else
  WRAPPER="$(dirname "$0")/../../WhisperFFmpegWrapperKit/main.py"
  if [[ -f "$WRAPPER" ]]; then
    cmd=(python3 "$WRAPPER" "$AUDIO")
    if [[ $DRY -eq 1 ]]; then
      echo "Would run: ${cmd[*]} > $FINAL_OUT"
    else
      echo "Running batch whisper fallback -> $FINAL_OUT"
      "${cmd[@]}" > "$FINAL_OUT"
    fi
  else
    echo "No whisper wrapper found; suggested whisper command: whisper.cpp --model /path/to/model.gguf --file '$AUDIO' --output '$FINAL_OUT'" >&2
  fi
fi

echo "Streaming ASR + fallback complete. Check: $OUT"

exit 0
