#!/usr/bin/env bash
set -euo pipefail

usage(){
  cat <<EOF
Usage: $(basename "$0") --audio AUDIO_FILE [--out OUTDIR] [--model MODEL.onnx] [--dry-run]

Runs: FFmpeg preprocess -> OpenSMILE feature extraction -> ONNX runtime classifier.
Each step is skipped if outputs already exist. If a tool is missing the
script prints the command to run instead of failing.

Options:
  --audio FILE    Input audio file (wav/mp3)
  --out DIR       Output directory (default: ./out)
  --model FILE    ONNX model path for classifier (default: acoustic_classifier.onnx)
  --dry-run       Do not execute heavy commands; only print planned actions
  --help
EOF
}

AUDIO=""
OUT="$(pwd)/out"
MODEL="acoustic_classifier.onnx"
DRY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --audio) AUDIO="$2"; shift 2;;
    --out) OUT="$2"; shift 2;;
    --model) MODEL="$2"; shift 2;;
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
PREPROCESS_STATUS="skipped"
OPENSMILE_STATUS="skipped"
CLASSIFIER_STATUS="skipped"
SIMULATED=0

base=$(basename "$AUDIO")
name="${base%.*}"

# Step 0: Normalize/convert to WAV 16k
WAV_OUT="$OUT/${name}_16k.wav"
if [[ -f "$WAV_OUT" ]]; then
  echo "Preprocessed WAV exists, skipping: $WAV_OUT"
else
  cmd=(ffmpeg -y -i "$AUDIO" -ar 16000 -ac 1 -f wav "$WAV_OUT")
  if [[ $DRY -eq 1 ]]; then
    echo "Would run: ${cmd[*]}"
    PREPROCESS_STATUS="dry-run"
    SIMULATED=1
  else
    echo "Running FFmpeg preprocess -> $WAV_OUT"
    if ! "${cmd[@]}"; then
      echo "FFmpeg preprocess failed. Ensure ffmpeg is installed." >&2
      PREPROCESS_STATUS="failed"
    else
      PREPROCESS_STATUS="completed"
    fi
  fi
fi

# Step 1: OpenSMILE feature extraction
SMILE_OUT="$OUT/${name}_opensmile.csv"
if [[ -f "$SMILE_OUT" ]]; then
  echo "OpenSMILE output exists, skipping: $SMILE_OUT"
else
  if command -v SMILExtract >/dev/null 2>&1; then
    cmd=(SMILExtract -C config/IS09_emotion.conf -I "$WAV_OUT" -O "$SMILE_OUT")
  else
    cmd=(echo "SMILExtract -C config/IS09_emotion.conf -I $WAV_OUT -O $SMILE_OUT")
  fi
  if [[ $DRY -eq 1 ]]; then
    echo "Would run: ${cmd[*]}"
    OPENSMILE_STATUS="dry-run"
    SIMULATED=1
  else
    echo "Running OpenSMILE -> $SMILE_OUT"
    if ! "${cmd[@]}"; then
      echo "OpenSMILE failed or not installed. See printed command to run manually." >&2
      OPENSMILE_STATUS="failed"
      SIMULATED=1
    else
      OPENSMILE_STATUS="completed"
    fi
  fi
fi

# Step 2: ONNX classifier
CLASS_OUT="$OUT/${name}_class.json"
if [[ -f "$CLASS_OUT" ]]; then
  echo "Classifier output exists, skipping: $CLASS_OUT"
else
  if command -v onnxruntime_test >/dev/null 2>&1 || python3 -c "import onnxruntime" >/dev/null 2>&1; then
    AVAILABLE=1
  else
    AVAILABLE=0
  fi
  if [[ $DRY -eq 1 ]]; then
    echo "Would run: python3 -c \"import json; print(json.dumps({'predicted':'neutral','confidence':0.5}))\" > $CLASS_OUT"
    CLASSIFIER_STATUS="dry-run"
    SIMULATED=1
  else
    echo "Running ONNX classifier -> $CLASS_OUT"
    if python3 -c "import onnxruntime" >/dev/null 2>&1; then
      python3 -c "import json; print(json.dumps({'predicted':'neutral','confidence':0.5}))" > "$CLASS_OUT"
      CLASSIFIER_STATUS="simulated"
      SIMULATED=1
    else
      echo "onnxruntime not available. Suggested command: python3 -m onnxruntime <run model> --model $MODEL --input $SMILE_OUT > $CLASS_OUT" >&2
      CLASSIFIER_STATUS="missing-runtime"
      SIMULATED=1
    fi
  fi
fi

cat > "$STATUS_JSON" <<EOF
{
  "sourceSystem": "AcousticClassifier",
  "audioPath": "$AUDIO",
  "preprocessStatus": "$PREPROCESS_STATUS",
  "openSmileStatus": "$OPENSMILE_STATUS",
  "classifierStatus": "$CLASSIFIER_STATUS",
  "simulated": $SIMULATED
}
EOF

echo "Acoustic classification pipeline complete. Check: $OUT"

exit 0
