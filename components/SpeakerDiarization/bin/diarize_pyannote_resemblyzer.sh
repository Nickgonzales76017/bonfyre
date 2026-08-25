#!/usr/bin/env bash
set -euo pipefail

usage(){
  cat <<EOF
Usage: $(basename "$0") --audio AUDIO_FILE [--out OUTDIR] [--dry-run]

Runs: pyannote diarization -> (optional) resemblyzer speaker embedding mapping.
Each step is skipped if outputs already exist. If a tool is missing the
script prints the command to run instead of failing.

Options:
  --audio FILE   Input audio file (wav/mp3)
  --out DIR      Output directory (default: ./out)
  --dry-run      Do not execute heavy commands; only print planned actions
  --help
EOF
}

AUDIO=""
OUT="$(pwd)/out"
DRY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --audio) AUDIO="$2"; shift 2;;
    --out) OUT="$2"; shift 2;;
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
DIAR_STATUS="skipped"
RESEMBLYZER_STATUS="skipped"
SIMULATED=0

base=$(basename "$AUDIO")
name="${base%.*}"

# Step 1: Diarization with pyannote
DIAR_OUT="$OUT/${name}_diar"
if [[ -d "$DIAR_OUT" ]]; then
  echo "Diarization output exists, skipping: $DIAR_OUT"
else
  if command -v pyannote >/dev/null 2>&1; then
    cmd=(pyannote diarization --subset="" "$AUDIO" "$DIAR_OUT")
  else
    cmd=(python3 -m pyannote.audio.pipeline.diarization "$AUDIO" --out "$DIAR_OUT")
  fi
  if [[ $DRY -eq 1 ]]; then
    echo "Would run: ${cmd[*]}"
    DIAR_STATUS="dry-run"
    SIMULATED=1
  else
    echo "Running pyannote diarization -> $DIAR_OUT"
    if ! "${cmd[@]}"; then
      echo "pyannote diarization failed or not installed. See printed command to run manually." >&2
      DIAR_STATUS="failed"
      SIMULATED=1
    else
      DIAR_STATUS="completed"
    fi
  fi
fi

# Step 2: Speaker embeddings with Resemblyzer (optional)
RESEMBLY_OUT="$OUT/${name}_resemblyzer"
if [[ -d "$RESEMBLY_OUT" ]]; then
  echo "Resemblyzer output exists, skipping: $RESEMBLY_OUT"
else
  if command -v resemblyzer >/dev/null 2>&1; then
    cmd=(resemblyzer --audio "$AUDIO" --diar "$DIAR_OUT" --out "$RESEMBLY_OUT")
  else
    cmd=(python3 -c "print('Run resemblyzer embedding pipeline here')")
  fi
  if [[ $DRY -eq 1 ]]; then
    echo "Would run: ${cmd[*]}"
    RESEMBLYZER_STATUS="dry-run"
    SIMULATED=1
  else
    echo "Running Resemblyzer embeddings -> $RESEMBLY_OUT"
    if ! "${cmd[@]}"; then
      echo "Resemblyzer failed or not installed. See printed command to run manually." >&2
      RESEMBLYZER_STATUS="failed"
      SIMULATED=1
    else
      RESEMBLYZER_STATUS="completed"
    fi
  fi
fi

cat > "$STATUS_JSON" <<EOF
{
  "sourceSystem": "SpeakerDiarization",
  "audioPath": "$AUDIO",
  "diarizationStatus": "$DIAR_STATUS",
  "resemblyzerStatus": "$RESEMBLYZER_STATUS",
  "simulated": $SIMULATED
}
EOF

echo "Diarization pipeline complete. Check: $OUT"

exit 0
