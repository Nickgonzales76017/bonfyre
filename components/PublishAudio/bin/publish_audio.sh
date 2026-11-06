#!/usr/bin/env bash
set -euo pipefail

usage(){
  cat <<USAGE
Usage: $(basename "$0") --audio AUDIO_FILE [--out OUTDIR] [--intro-text TEXT_FILE]

Normalizes the recording with the media-prep organ, speaks the intro text with
the narrate organ when one is given, and joins them into one publishable file.
Every step runs or the script fails; it has no planned-command mode and writes
no status for a step that did not happen.

Options:
  --audio FILE        Input recording (wav/mp3)
  --out DIR           Output directory (default: ./out)
  --intro-text FILE   Text to speak before the recording
  --help
USAGE
}

AUDIO="" OUT="$(pwd)/out" INTRO=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --audio) AUDIO="$2"; shift 2;;
    --out) OUT="$2"; shift 2;;
    --intro-text) INTRO="$2"; shift 2;;
    --help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage >&2; exit 1;;
  esac
done

[[ -n "$AUDIO" && -f "$AUDIO" ]] || { echo "--audio is required and must point to a file" >&2; exit 2; }
[[ -z "$INTRO" || -s "$INTRO" ]] || { echo "--intro-text must point to a file with text in it" >&2; exit 2; }

organ(){ command -v "bonfyre-$1" 2>/dev/null || { [[ -x "$HOME/.bonfyre/bin/bonfyre-$1" ]] && echo "$HOME/.bonfyre/bin/bonfyre-$1"; } || { echo "bonfyre-$1 is not installed" >&2; exit 3; }; }
MEDIAPREP=$(organ media-prep)
command -v ffmpeg >/dev/null 2>&1 || { echo "ffmpeg is not installed" >&2; exit 3; }

mkdir -p "$OUT"
base=$(basename "$AUDIO"); name="${base%.*}"
NORM_OUT="$OUT/${name}_normalized.wav"
FINAL_OUT="$OUT/${name}_publishable.wav"

"$MEDIAPREP" normalize "$AUDIO" "$NORM_OUT" --sample-rate 16000 --channels 1 --loudnorm
[[ -s "$NORM_OUT" ]] || { echo "normalization wrote nothing" >&2; exit 4; }

if [[ -n "$INTRO" ]]; then
  NARRATE=$(organ narrate)
  "$NARRATE" "$INTRO" "$OUT/intro"
  INTRO_WAV=$(find "$OUT/intro" -name '*.wav' -size +0 | head -1)
  [[ -n "$INTRO_WAV" ]] || { echo "the intro was not spoken; nothing to join" >&2; exit 4; }
  INTRO_16K="$OUT/${name}_intro.wav"
  "$MEDIAPREP" normalize "$INTRO_WAV" "$INTRO_16K" --sample-rate 16000 --channels 1
  CONCAT_LIST="$OUT/${name}_concat.txt"
  printf "file '%s'\nfile '%s'\n" "$INTRO_16K" "$NORM_OUT" > "$CONCAT_LIST"
  ffmpeg -nostdin -loglevel error -y -f concat -safe 0 -i "$CONCAT_LIST" -ar 16000 -ac 1 "$FINAL_OUT"
else
  cp "$NORM_OUT" "$FINAL_OUT"
fi
[[ -s "$FINAL_OUT" ]] || { echo "no publishable file was written" >&2; exit 4; }

seconds(){ ffprobe -v error -show_entries format=duration -of csv=p=0 "$1"; }
cat > "$OUT/status.json" <<JSON
{
  "sourceSystem": "PublishAudio",
  "audioPath": "$AUDIO",
  "publishable": "$FINAL_OUT",
  "recordingSeconds": $(seconds "$NORM_OUT"),
  "publishableSeconds": $(seconds "$FINAL_OUT"),
  "intro": $([[ -n "$INTRO" ]] && echo true || echo false)
}
JSON
echo "Publishable audio: $FINAL_OUT"
