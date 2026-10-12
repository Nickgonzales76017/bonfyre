# TesseractOCR

Document ingestion operator via Tesseract OCR. Converts images/scans to structured text.

## Subcommands
- `ocr` — image → plain text
- `ocr-json` — image → JSON (via TSV intermediate)
- `ocr-pdf` — image → searchable PDF
- `batch` — OCR all images in a directory

## Usage
```bash
bash bin/tesseract_ocr.sh ocr scan.png --lang eng
bash bin/tesseract_ocr.sh batch ./documents/ --dry-run
```

## Install
```bash
brew install tesseract
```
