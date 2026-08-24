#!/usr/bin/env bash
set -euo pipefail

usage(){
  cat <<EOF
Usage: $(basename "$0") --audio AUDIO_FILE [--transcript TRANS.txt] [--out OUTDIR] [--dict DICT] [--dry-run]

Runs: whisper (if needed) -> whisperx (word timestamps) -> MFA (phoneme alignment).
Each step is skipped if the expected output already exists. If a tool is missing the
script prints the command to run instead of failing.

Options:
  --audio FILE       Input audio file (wav/mp3)
  --transcript FILE  Existing transcript (.txt). If omitted, script will try to call local wrapper or print whisper command.
  --out DIR          Output directory (default: ./out)
  --dict FILE        MFA dictionary file. If omitted, auto-build from transcript when possible.
  --model NAME       Whisper model name for whisperx stage (default: small)
  --dry-run          Do not execute heavy commands; only print planned actions
  --help
EOF
}

AUDIO=""
TRANS=""
OUT="$(pwd)/out"
MODEL="small"
DICT=""
DRY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --audio) AUDIO="$2"; shift 2;;
    --transcript) TRANS="$2"; shift 2;;
    --out) OUT="$2"; shift 2;;
    --dict) DICT="$2"; shift 2;;
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
WHISPERX_STATUS="skipped"
MFA_STATUS="skipped"
DICT_STATUS="skipped"
SIMULATED=0

base=$(basename "$AUDIO")
name="${base%.*}"

# Step 0: transcript (if not provided)
if [[ -z "$TRANS" ]]; then
  # try local wrapper
  WRAPPER="$(dirname "$0")/../../WhisperFFmpegWrapperKit/main.py"
  if [[ -f "$WRAPPER" ]]; then
    TRANS="$OUT/${name}.txt"
    cmd=(python3 "$WRAPPER" "$AUDIO")
    if [[ $DRY -eq 1 ]]; then
      echo "Would run: ${cmd[*]} > $TRANS"
    else
      echo "Running whisper wrapper to produce transcript: $TRANS"
      "${cmd[@]}" > "$TRANS"
    fi
  else
    TRANS="$OUT/${name}.txt"
    echo "No local wrapper found. Suggested whisper command (dry-run prints only):"
    echo "  whisper.cpp --model /path/to/model.gguf --file '$AUDIO' --output '$TRANS'"
    if [[ $DRY -eq 1 ]]; then
      :
    else
      echo "Not running transcription (no wrapper). Provide --transcript or install wrapper." >&2
    fi
  fi
fi

# Step 0.5: build MFA dictionary if needed
if [[ -z "$DICT" && -n "$TRANS" ]]; then
  DICT="$OUT/${name}.dict"
  DICT_BUILDER="$(dirname "$0")/../../MFA_DictBuilder/bin/build_mfa_dict.py"
  NATIVE_DICT_BUILDER="$(dirname "$0")/../../BonfyreMFADict/bonfyre-mfa-dict"
  if [[ -f "$NATIVE_DICT_BUILDER" ]]; then
    cmd=(/usr/bin/arch -arm64 "$NATIVE_DICT_BUILDER" --transcript "$TRANS" --out "$DICT")
    if [[ $DRY -eq 1 ]]; then
      echo "Would run: ${cmd[*]}"
      DICT_STATUS="dry-run"
      SIMULATED=1
    else
      echo "Building MFA dictionary with BonfyreMFADict -> $DICT"
      if ! "${cmd[@]}"; then
        echo "Native dictionary build failed." >&2
        DICT_STATUS="failed"
        SIMULATED=1
      else
        DICT_STATUS="completed"
      fi
    fi
  elif [[ -f "$DICT_BUILDER" ]]; then
    cmd=(python3 "$DICT_BUILDER" --transcript "$TRANS" --out "$DICT")
    if [[ $DRY -eq 1 ]]; then
      echo "Would run: ${cmd[*]}"
      DICT_STATUS="dry-run"
      SIMULATED=1
    else
      echo "Building MFA dictionary -> $DICT"
      if ! "${cmd[@]}"; then
        echo "Dictionary build failed." >&2
        DICT_STATUS="failed"
        SIMULATED=1
      else
        DICT_STATUS="completed"
      fi
    fi
  else
    echo "MFA dictionary builder not found." >&2
    DICT_STATUS="missing"
    SIMULATED=1
  fi
elif [[ -n "$DICT" && -f "$DICT" ]]; then
  DICT_STATUS="provided"
elif [[ -n "$DICT" ]]; then
  DICT_STATUS="missing"
  SIMULATED=1
fi

# Step 1: WhisperX (word-level timestamps)
WHISPERX_OUT="$OUT/${name}_whisperx"
if [[ -d "$WHISPERX_OUT" ]]; then
  echo "WhisperX output exists, skipping: $WHISPERX_OUT"
else
  # try whisperx cli or module
  if command -v whisperx >/dev/null 2>&1; then
    cmd=(whisperx --model "$MODEL" --file "$AUDIO" --transcript "$TRANS" --output_dir "$WHISPERX_OUT")
  else
    cmd=(python3 -m whisperx --model "$MODEL" --file "$AUDIO" --transcript "$TRANS" --output_dir "$WHISPERX_OUT")
  fi
  if [[ $DRY -eq 1 ]]; then
    echo "Would run: ${cmd[*]}"
    WHISPERX_STATUS="dry-run"
    SIMULATED=1
  else
    echo "Running WhisperX -> $WHISPERX_OUT"
    if ! "${cmd[@]}"; then
      echo "WhisperX failed or not installed. See printed command to run manually." >&2
      WHISPERX_STATUS="failed"
      SIMULATED=1
    else
      WHISPERX_STATUS="completed"
    fi
  fi
fi

# Step 2: MFA phoneme alignment (requires transcript and word timestamps)
MFA_OUT="$OUT/${name}_mfa"
if [[ -d "$MFA_OUT" ]]; then
  echo "MFA output exists, skipping: $MFA_OUT"
else
  # MFA CLI (mfa align) expects: mfa align <wav_dir> <transcript_dir> <dict> <outdir>
  # We will attempt a simple call if `mfa` is available; otherwise print instructions.
  if command -v mfa >/dev/null 2>&1; then
    # Prepare directories
    WAV_DIR="$OUT/${name}_wav"
    TXT_DIR="$OUT/${name}_txt"
    mkdir -p "$WAV_DIR" "$TXT_DIR"
    cp "$AUDIO" "$WAV_DIR/"
    cp "$TRANS" "$TXT_DIR/${name}.txt" || true
    if [[ -n "$DICT" && -f "$DICT" ]]; then
      cmd=(mfa align "$WAV_DIR" "$TXT_DIR" "$DICT" "$MFA_OUT")
    else
      cmd=(echo "mfa align <wav_dir> <txt_dir> <dict> <outdir>")
      SIMULATED=1
    fi
  else
    cmd=(echo "mfa align <wav_dir> <txt_dir> <dict> <outdir>")
    SIMULATED=1
  fi
  if [[ $DRY -eq 1 ]]; then
    echo "Would run: ${cmd[*]}"
    MFA_STATUS="dry-run"
    SIMULATED=1
  else
    echo "Running MFA alignment -> $MFA_OUT"
    if ! "${cmd[@]}"; then
      echo "MFA failed or not installed. See printed command to run manually." >&2
      MFA_STATUS="failed"
      SIMULATED=1
    else
      MFA_STATUS="completed"
    fi
  fi
fi

cat > "$STATUS_JSON" <<EOF
{
  "sourceSystem": "WhisperAlignment",
  "audioPath": "$AUDIO",
  "transcriptPath": "$TRANS",
  "dictionaryPath": "$DICT",
  "dictionaryStatus": "$DICT_STATUS",
  "whisperxStatus": "$WHISPERX_STATUS",
  "mfaStatus": "$MFA_STATUS",
  "simulated": $SIMULATED
}
EOF

echo "Alignment pipeline complete. Check: $OUT"

exit 0
