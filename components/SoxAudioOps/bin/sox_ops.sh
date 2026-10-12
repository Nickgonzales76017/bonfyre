#!/usr/bin/env bash
set -euo pipefail
# SoxAudioOps — silence detection, waveform shaping, audio fingerprinting
# Usage: sox_ops.sh <subcommand> <input> [options] [--dry-run]
#   subcommands: silence-detect | fingerprint | normalize-peak | trim-silence

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DRY_RUN=false

for arg in "$@"; do
  [[ "$arg" == "--dry-run" ]] && DRY_RUN=true
done

subcmd="${1:-}"
input="${2:-}"

if [[ -z "$subcmd" || -z "$input" ]]; then
  echo "Usage: sox_ops.sh <silence-detect|fingerprint|normalize-peak|trim-silence> <input> [--out FILE] [--dry-run]"
  exit 1
fi

OUT=""
prev=""
for i in "${@:3}"; do
  if [[ "$prev" == "--out" ]]; then OUT="$i"; fi
  prev="$i"
done

case "$subcmd" in
  silence-detect)
    cmd="sox \"$input\" -n silence 1 0.5 0.1% reverse silence 1 0.5 0.1% reverse stat"
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v sox &>/dev/null; then echo "sox not found. Install: brew install sox"; exit 2; fi
    eval "$cmd" 2>&1 || true
    ;;
  fingerprint)
    cmd="sox \"$input\" -n stat 2>&1 | head -20"
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v sox &>/dev/null; then echo "sox not found. Install: brew install sox"; exit 2; fi
    eval "$cmd" || true
    ;;
  normalize-peak)
    out="${OUT:-${input%.wav}-normpeak.wav}"
    cmd="sox \"$input\" \"$out\" gain -n -3"
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v sox &>/dev/null; then echo "sox not found. Install: brew install sox"; exit 2; fi
    eval "$cmd"
    echo "Output: $out"
    ;;
  trim-silence)
    out="${OUT:-${input%.wav}-trimmed.wav}"
    cmd="sox \"$input\" \"$out\" silence 1 0.3 0.1% reverse silence 1 0.3 0.1% reverse"
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v sox &>/dev/null; then echo "sox not found. Install: brew install sox"; exit 2; fi
    eval "$cmd"
    echo "Output: $out"
    ;;
  *)
    echo "Unknown subcommand: $subcmd"
    exit 1
    ;;
esac
