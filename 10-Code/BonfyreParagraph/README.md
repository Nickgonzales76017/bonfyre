# BonfyreParagraph

Small native transcript paragraphizer for Bonfyre.

## Purpose
- split cleaned transcript text into readable paragraph blocks
- optionally emit lightweight headers
- write `status.json`

## Build
```bash
make
```

## Usage
```bash
./bonfyre-paragraph --input cleaned.txt --out outputs/sample/paragraphed.md
./bonfyre-paragraph --input cleaned.txt --out outputs/sample/paragraphed.md --with-headers
```

## Notes
This is the compiled primitive version of the old `TranscriptParagraphizer` path.
It is intentionally deterministic and formatting-focused.
