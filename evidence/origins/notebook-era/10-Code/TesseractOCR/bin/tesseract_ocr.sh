#!/usr/bin/env bash
set -euo pipefail
# TesseractOCR — document ingestion via tesseract (visual → structured text)
# Usage: tesseract_ocr.sh <subcommand> <input> [--lang LANG] [--out FILE] [--dry-run]
#   subcommands: ocr | ocr-json | ocr-pdf | batch

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DRY_RUN=false
for arg in "$@"; do [[ "$arg" == "--dry-run" ]] && DRY_RUN=true; done

subcmd="${1:-}"
input="${2:-}"

if [[ -z "$subcmd" || -z "$input" ]]; then
  echo "Usage: tesseract_ocr.sh <ocr|ocr-json|ocr-pdf|batch> <input> [--lang eng] [--out FILE] [--dry-run]"
  exit 1
fi

LANG="eng" OUT=""
prev=""
for i in "${@:3}"; do
  case "$prev" in --lang) LANG="$i";; --out) OUT="$i";; esac
  prev="$i"
done

case "$subcmd" in
  ocr)
    out="${OUT:-${input%.*}}"
    cmd="tesseract \"$input\" \"$out\" -l $LANG"
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v tesseract &>/dev/null; then echo "tesseract not found. Install: brew install tesseract"; exit 2; fi
    eval "$cmd"
    echo "Output: ${out}.txt"
    ;;
  ocr-json)
    out="${OUT:-${input%.*}}"
    cmd="tesseract \"$input\" \"$out\" -l $LANG tsv"
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v tesseract &>/dev/null; then echo "tesseract not found."; exit 2; fi
    eval "$cmd"
    # Convert TSV to JSON
    python3 -c "
import csv, json, sys
with open('${out}.tsv') as f:
    rows = list(csv.DictReader(f, delimiter='\t'))
with open('${out}.json', 'w') as f:
    json.dump(rows, f, indent=2)
print(f'Output: ${out}.json')
"
    ;;
  ocr-pdf)
    out="${OUT:-${input%.*}}"
    cmd="tesseract \"$input\" \"$out\" -l $LANG pdf"
    if $DRY_RUN; then echo "Would run: $cmd"; exit 0; fi
    if ! command -v tesseract &>/dev/null; then echo "tesseract not found."; exit 2; fi
    eval "$cmd"
    echo "Output: ${out}.pdf"
    ;;
  batch)
    # input is a directory
    if $DRY_RUN; then echo "Would run: tesseract on all images in $input"; exit 0; fi
    if ! command -v tesseract &>/dev/null; then echo "tesseract not found."; exit 2; fi
    for img in "$input"/*.{png,jpg,jpeg,tiff,bmp} ; do
      [[ -f "$img" ]] || continue
      base="${img%.*}"
      tesseract "$img" "$base" -l "$LANG"
      echo "  OCR'd: ${base}.txt"
    done
    ;;
  *)
    echo "Unknown subcommand: $subcmd"
    exit 1
    ;;
esac
