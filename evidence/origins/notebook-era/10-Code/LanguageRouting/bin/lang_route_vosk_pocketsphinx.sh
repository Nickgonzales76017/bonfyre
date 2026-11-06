#!/usr/bin/env bash
set -euo pipefail

usage(){
  cat <<EOF
Usage: $(basename "$0") --audio AUDIO_FILE [--out OUTDIR] [--vosk-langs en,es] [--dry-run]

Fast language detection + ASR router. Produces a short transcript (via local wrapper if present),
detects language (using `langdetect` if available), and routes to Vosk/PocketSphinx for configured
languages or to whisper wrapper otherwise.

Options:
  --audio FILE       Input audio file (wav/mp3)
  --out DIR          Output directory (default: ./out)
  --vosk-langs LIST  Comma-separated language codes to route to Vosk (default: en)
  --dry-run          Print planned commands instead of executing
  --help
EOF
}

AUDIO=""
OUT="$(pwd)/out"
VOSK_LANGS="en"
DRY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --audio) AUDIO="$2"; shift 2;;
    --out) OUT="$2"; shift 2;;
    --vosk-langs) VOSK_LANGS="$2"; shift 2;;
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
TRANS="$OUT/${name}_sample.txt"

# Step 0: generate a short sample transcript if possible
WRAPPER="$(dirname "$0")/../../WhisperFFmpegWrapperKit/main.py"
if [[ -f "$WRAPPER" ]]; then
  SAMPLE_CMD=(python3 "$WRAPPER" --sample "$AUDIO")
  if [[ $DRY -eq 1 ]]; then
    echo "Would run: ${SAMPLE_CMD[*]} > $TRANS"
  else
    echo "Producing sample transcript via local whisper wrapper -> $TRANS"
    "${SAMPLE_CMD[@]}" > "$TRANS" 2>/dev/null || true
  fi
else
  echo "No local whisper wrapper found; will attempt language detection via python if available." >&2
fi

# Step 1: language detection (from sample transcript)
DETECTED=""
if python3 - <<PY >/dev/null 2>&1
try:
    import langdetect
    print('ok')
except Exception:
    raise SystemExit(1)
PY
then
  if [[ -f "$TRANS" && -s "$TRANS" ]]; then
    if [[ $DRY -eq 1 ]]; then
      echo "Would run: python3 -c 'from langdetect import detect; print(detect(open("$TRANS").read()))'"
    else
      DETECTED=$(python3 - <<PY
from langdetect import detect
txt = open("$TRANS", encoding="utf-8").read()
try:
    print(detect(txt))
except Exception:
    print('')
PY
)
    fi
  fi
else
  echo "python langdetect not available; skipping programmatic detection." >&2
fi

if [[ -z "$DETECTED" ]]; then
  echo "Language not detected from sample; defaulting to 'en' for routing decisions." >&2
  DETECTED="en"
fi

echo "Detected language: $DETECTED"

IFS=',' read -ra VOSK_ARR <<< "$VOSK_LANGS"
USE_VOSK=0
for l in "${VOSK_ARR[@]}"; do
  if [[ "$l" == "$DETECTED" ]]; then
    USE_VOSK=1
    break
  fi
done

if [[ $USE_VOSK -eq 1 ]]; then
  # Route to Vosk (fast language-specific model)
  VOSK_CMD=(vosk-transcriber --model /path/to/vosk-model-$DETECTED --input "$AUDIO" --output "$OUT/${name}_vosk.txt")
  if [[ $DRY -eq 1 ]]; then
    echo "Would run: ${VOSK_CMD[*]}"
  else
    echo "Routing to Vosk model for $DETECTED"
    "${VOSK_CMD[@]}"
  fi
else
  # Fallback to whisper wrapper
  WHISPER_CMD=(python3 "$WRAPPER" "$AUDIO" )
  if [[ $DRY -eq 1 ]]; then
    echo "Would run: ${WHISPER_CMD[*]} > $OUT/${name}_whisper.txt"
  else
    if [[ -f "$WRAPPER" ]]; then
      echo "Routing to whisper wrapper for full transcription"
      "${WHISPER_CMD[@]}" > "$OUT/${name}_whisper.txt"
    else
      echo "No whisper wrapper available; suggest: whisper.cpp --model /path/to/model.gguf --file '$AUDIO' --output '$OUT/${name}_whisper.txt'" >&2
    fi
  fi
fi

echo "Language routing complete. Outputs (if produced) in: $OUT"

exit 0
