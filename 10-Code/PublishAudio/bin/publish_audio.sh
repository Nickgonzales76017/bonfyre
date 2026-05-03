#!/usr/bin/env bash
set -euo pipefail

usage(){
  cat <<EOF
Usage: $(basename "$0") --audio AUDIO_FILE [--out OUTDIR] [--tts engine] [--dry-run]

Runs: normalize (ffmpeg-normalize or sox) -> optional TTS intro (Piper/Coqui) -> stitch with FFmpeg.
Designed for dry-run and idempotent behavior; prints commands if tools are missing.

Options:
  --audio FILE   Input audio file (wav/mp3)
  --out DIR      Output directory (default: ./out)
  --tts ENGINE   tts engine: piper | coqui (default: none)
  --dry-run      Print planned commands instead of executing
  --help
EOF
}

AUDIO=""
OUT="$(pwd)/out"
TTS="none"
DRY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --audio) AUDIO="$2"; shift 2;;
    --out) OUT="$2"; shift 2;;
    --tts) TTS="$2"; shift 2;;
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
STATUS_JSON="$OUT/status.json"
NORMALIZE_STATUS="skipped"
TTS_STATUS="skipped"
PUBLISH_STATUS="skipped"
SIMULATED=0

base=$(basename "$AUDIO")
name="${base%.*}"

# Step 1: Normalize loudness (prefer ffmpeg-normalize if available)
NORM_OUT="$OUT/${name}_normalized.wav"
if [[ -f "$NORM_OUT" ]]; then
  echo "Normalized file exists, skipping: $NORM_OUT"
else
  if command -v ffmpeg-normalize >/dev/null 2>&1; then
    cmd=(ffmpeg-normalize "$AUDIO" -o "$NORM_OUT" -t -ar 16000 -b 128k --keep-loudness)
  elif command -v sox >/dev/null 2>&1; then
    cmd=(sox "$AUDIO" "$NORM_OUT" gain -n)
  else
    cmd=(ffmpeg -y -i "$AUDIO" -ar 16000 -ac 1 "$NORM_OUT")
  fi
  if [[ $DRY -eq 1 ]]; then
    echo "Would run: ${cmd[*]}"
    NORMALIZE_STATUS="dry-run"
    SIMULATED=1
  else
    echo "Running normalization -> $NORM_OUT"
    if ! "${cmd[@]}"; then
      echo "Normalization failed. Ensure ffmpeg-normalize or sox is installed." >&2
      NORMALIZE_STATUS="failed"
    else
      NORMALIZE_STATUS="completed"
    fi
  fi
fi

# Step 2: Optional TTS intro/outro
TTS_OUT="$OUT/${name}_tts_intro.wav"
if [[ "$TTS" == "none" ]]; then
  echo "No TTS requested; skipping TTS step."
  TTS_STATUS="skipped"
else
  if [[ -f "$TTS_OUT" ]]; then
    echo "TTS intro exists, skipping: $TTS_OUT"
  else
    if [[ "$TTS" == "piper" ]]; then
      if command -v piper >/dev/null 2>&1; then
        cmd=(piper --text "Intro" --model /path/to/piper_model --out "$TTS_OUT")
      else
        cmd=(echo "piper --text 'Intro' --model /path/to/piper_model --out $TTS_OUT")
      fi
    elif [[ "$TTS" == "coqui" ]]; then
      if command -v tts >/dev/null 2>&1; then
        cmd=(tts --text "Intro" --model /path/to/coqui_model --out_path "$TTS_OUT")
      else
        cmd=(echo "tts --text 'Intro' --model /path/to/coqui_model --out_path $TTS_OUT")
      fi
    else
      echo "Unknown TTS engine: $TTS" >&2
      exit 3
    fi
    if [[ $DRY -eq 1 ]]; then
      echo "Would run: ${cmd[*]}"
      TTS_STATUS="dry-run"
      SIMULATED=1
    else
      echo "Running TTS intro -> $TTS_OUT"
      if ! "${cmd[@]}"; then
        echo "TTS command failed or not installed. See printed command to run manually." >&2
        TTS_STATUS="failed"
        SIMULATED=1
      else
        TTS_STATUS="completed"
      fi
    fi
  fi
fi

# Step 3: Stitch intro + normalized audio -> final publishable file
FINAL_OUT="$OUT/${name}_publishable.wav"
if [[ -f "$FINAL_OUT" ]]; then
  echo "Final output exists, skipping: $FINAL_OUT"
else
  if [[ "$TTS" == "none" ]]; then
    cmd=(cp "$NORM_OUT" "$FINAL_OUT")
  else
    CONCAT_LIST="$OUT/${name}_concat.txt"
    printf "file '%s'\nfile '%s'\n" "$TTS_OUT" "$NORM_OUT" > "$CONCAT_LIST"
    cmd=(ffmpeg -y -f concat -safe 0 -i "$CONCAT_LIST" -ar 16000 -ac 1 "$FINAL_OUT")
  fi
  if [[ $DRY -eq 1 ]]; then
    echo "Would run: ${cmd[*]}"
    PUBLISH_STATUS="dry-run"
    SIMULATED=1
  else
    echo "Creating final publishable audio -> $FINAL_OUT"
    if ! "${cmd[@]}"; then
      echo "Stitching failed. Ensure ffmpeg is installed." >&2
      PUBLISH_STATUS="failed"
      SIMULATED=1
    else
      PUBLISH_STATUS="completed"
    fi
  fi
fi

cat > "$STATUS_JSON" <<EOF
{
  "sourceSystem": "PublishAudio",
  "audioPath": "$AUDIO",
  "normalizeStatus": "$NORMALIZE_STATUS",
  "ttsStatus": "$TTS_STATUS",
  "publishStatus": "$PUBLISH_STATUS",
  "ttsEngine": "$TTS",
  "simulated": $SIMULATED
}
EOF

echo "Publish audio pipeline complete. Check: $OUT"

exit 0
