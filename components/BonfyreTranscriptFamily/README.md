# BonfyreTranscriptFamily

Thin compiled fusion binary for:
- `BonfyreTranscribe`
- `BonfyreTranscriptClean`
- `BonfyreParagraph`

## Purpose
- take audio in
- produce a cleaned transcript family
- emit:
  - `transcribe/`
  - `cleaned.txt`
  - `paragraphed.md`
  - `family-status.json`

## Build
```bash
make
```

## Usage
```bash
./bonfyre-transcript-family input.m4a outputs/sample
./bonfyre-transcript-family input.m4a outputs/sample --with-headers --split-speech --silero-vad
```

## Notes
- wrapper flags:
  - `--with-headers`
- all other flags are forwarded to `bonfyre-transcribe`
