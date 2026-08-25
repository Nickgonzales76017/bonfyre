#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
echo "=== TesseractOCR smoke test ==="
for sub in ocr ocr-json ocr-pdf batch; do
  echo "--- $sub --dry-run ---"
  bash "$here/../bin/tesseract_ocr.sh" "$sub" /tmp/test.png --dry-run
done
echo "=== PASS ==="
