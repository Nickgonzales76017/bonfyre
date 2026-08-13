#!/usr/bin/env bash
set -euo pipefail
# ImageMagickOps — deterministic image/waveform transforms via ImageMagick
# Usage: imagemagick_ops.sh <subcommand> <input> [--out FILE] [--dry-run]
#   subcommands: thumbnail | waveform | resize | convert | strip-meta

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DRY_RUN=false
for arg in "$@"; do [[ "$arg" == "--dry-run" ]] && DRY_RUN=true; done

subcmd="${1:-}"
input="${2:-}"

if [[ -z "$subcmd" || -z "$input" ]]; then
  echo "Usage: imagemagick_ops.sh <thumbnail|waveform|resize|convert|strip-meta> <input> [--out FILE] [--size WxH] [--dry-run]"
  exit 1
fi

OUT="" SIZE="256x256"
prev=""
for i in "${@:3}"; do
  case "$prev" in --out) OUT="$i";; --size) SIZE="$i";; esac
  prev="$i"
done

case "$subcmd" in
  thumbnail)
    out="${OUT:-${input%.*}-thumb.png}"
    cmd="magick \"$input\" -thumbnail $SIZE \"$out\""
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v magick &>/dev/null; then echo "imagemagick not found. Install: brew install imagemagick"; exit 2; fi
    eval "$cmd"
    echo "Output: $out"
    ;;
  waveform)
    # Generate a waveform PNG from audio using sox + imagemagick
    out="${OUT:-${input%.*}-waveform.png}"
    cmd="sox \"$input\" -n spectrogram -o /tmp/_spec.png && magick /tmp/_spec.png -resize 800x200 \"$out\""
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v magick &>/dev/null || ! command -v sox &>/dev/null; then echo "Requires sox + imagemagick"; exit 2; fi
    eval "$cmd"
    echo "Output: $out"
    ;;
  resize)
    out="${OUT:-${input%.*}-resized.${input##*.}}"
    cmd="magick \"$input\" -resize $SIZE \"$out\""
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v magick &>/dev/null; then echo "imagemagick not found."; exit 2; fi
    eval "$cmd"
    echo "Output: $out"
    ;;
  convert)
    out="${OUT:-${input%.*}.png}"
    cmd="magick \"$input\" \"$out\""
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v magick &>/dev/null; then echo "imagemagick not found."; exit 2; fi
    eval "$cmd"
    echo "Output: $out"
    ;;
  strip-meta)
    out="${OUT:-${input%.*}-clean.${input##*.}}"
    cmd="magick \"$input\" -strip \"$out\""
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v magick &>/dev/null; then echo "imagemagick not found."; exit 2; fi
    eval "$cmd"
    echo "Output: $out"
    ;;
  *)
    echo "Unknown subcommand: $subcmd"
    exit 1
    ;;
esac
