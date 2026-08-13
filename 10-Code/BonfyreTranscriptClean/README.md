# BonfyreTranscriptClean

Small native transcript QA/cleanup binary for Bonfyre.

## Purpose
- remove chunk headers
- remove common filler tokens
- suppress a few repeated hallucination patterns
- normalize whitespace and punctuation
- emit a cleaned transcript plus `status.json`

## Build
```bash
make
```

## Usage
```bash
./bonfyre-transcript-clean \
  --transcript ../TranscriptQACleaner/tests/tmp-raw.txt \
  --out outputs/sample/cleaned.txt
```

## Outputs
- `cleaned.txt`
- `status.json`

## Notes
This is intended to be the compiled primitive version of the existing
`TranscriptQACleaner` script path. It stays narrow and deterministic on purpose.
