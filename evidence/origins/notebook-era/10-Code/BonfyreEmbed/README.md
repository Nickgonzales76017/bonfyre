# BonfyreEmbed

Small native deterministic embedding binary for Bonfyre.

## Purpose
- read finalized text artifacts
- emit a deterministic local embedding vector
- write metadata and `status.json`

## Build
```bash
make
```

## Usage
```bash
./bonfyre-embed \
  --text ../LocalAITranscriptionService/samples/incoming-audio/founder-01-pickfu-assumptions.mp3 \
  --out outputs/sample/transcript-embedding.json
```

## Notes
This mirrors the current hashed-token fallback path from `ONNXEmbedder`, but as a
compiled Bonfyre primitive. It is intentionally deterministic and narrow.
