#!/usr/bin/env bash
set -euo pipefail

usage(){
  cat <<EOF
Usage: $(basename "$0") [--target PATH] [--activate]

Copies the pipeline script into a target project's bin directory safely.
By default the target is ../LocalAITranscriptionService/bin relative to this script.

Options:
  --target PATH   Destination bin directory
  --activate      Move the new script into place (will backup existing file)
  --help          Show this message
EOF
}

TARGET="$(dirname "$0")/../LocalAITranscriptionService/bin"
ACTIVATE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target) TARGET="$2"; shift 2;;
    --activate) ACTIVATE=1; shift;;
    --help) usage; exit 0;;
    *) echo "Unknown: $1"; usage; exit 1;;
  esac
done

mkdir -p "$TARGET"

SRC="$(dirname "$0")/vad_rnnoise_whisper.sh"
DST="$TARGET/$(basename "$SRC")"
TMP="$DST.new"

echo "Preparing integration: will copy to $TMP"
cp -p "$SRC" "$TMP"
chmod +x "$TMP"

if [[ $ACTIVATE -eq 0 ]]; then
  echo "Dry-run: new script staged at $TMP. To install, rerun with --activate."
  exit 0
fi

if [[ -f "$DST" ]]; then
  BAK="$DST.bak.$(date +%s)"
  echo "Backing up existing $DST -> $BAK"
  mv "$DST" "$BAK"
fi

mv "$TMP" "$DST"
echo "Installed $DST"

exit 0
