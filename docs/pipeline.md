# Pipeline Guide

Bonfyre's audio pipeline takes raw audio and produces a packaged deliverable: transcript, summary, quality score, pricing, and ZIP package.

## Full pipeline (one command)

```bash
bonfyre-pipeline run --input interview.mp3 --out ./output
```

This runs 10 stages in-process in **5–8 ms** (after transcription, which depends on audio length).

## Step by step

You can also run each step individually:

### 1. Ingest

```bash
bonfyre-ingest intake interview.mp3 --out ./work/
```

Validates the file, creates a manifest, copies to working directory.

### 2. Normalize

```bash
bonfyre-media-prep normalize ./work/interview.mp3
```

Converts to 16 kHz mono WAV, applies denoising, chunks if needed.

### 3. Hash

```bash
bonfyre-hash ./work/interview.wav
```

Produces SHA-256 content address for deduplication and integrity.

### 4. Transcribe

```bash
bonfyre-transcribe run ./work/interview.wav --out transcript.json
```

Speech-to-text using Whisper. Runs locally — no external API calls.

### 5. Clean

```bash
bonfyre-transcript-clean transcript.json --out clean.json
```

Removes filler words ("um", "uh"), hallucinations, normalizes punctuation.

### 6. Paragraph

```bash
bonfyre-paragraph clean.json --out paragraphed.json
```

Structures text into readable paragraphs with sentence boundary detection.

### 7. Brief

```bash
bonfyre-brief generate paragraphed.json --out brief.md
```

Extracts executive summary and action items.

### 8. Proof

```bash
bonfyre-proof score paragraphed.json
```

Quality scoring: length, filler ratio, hallucination probability, confidence.

### 9. Offer

```bash
bonfyre-offer generate --input brief.md --tier standard
```

Generates pricing proposal based on content quality and tier.

### 10. Pack

```bash
bonfyre-pack bundle ./work/ --out deliverable.zip
```

Assembles everything into a ZIP with manifest.

## Composing with pipes

Since every binary reads/writes JSON, you can pipe them:

```bash
bonfyre-ingest intake interview.mp3 | \
  bonfyre-transcribe run --stdin | \
  bonfyre-transcript-clean --stdin | \
  bonfyre-paragraph --stdin | \
  bonfyre-brief generate --stdin
```

## Adding custom steps

Write a binary that reads JSON from stdin and writes JSON to stdout. Drop it in the pipeline at any point. See [CONTRIBUTING.md](../CONTRIBUTING.md) for the binary template.

## Monitoring

Track pipeline jobs through the API:

```bash
# Start the API gateway
bonfyre-api --port 9090 serve

# Submit a job
curl -X POST http://localhost:9090/api/jobs \
  -H "Content-Type: application/json" \
  -d '{"binary": "bonfyre-pipeline", "args": ["run", "--input", "interview.mp3"]}'

# Check status
curl http://localhost:9090/api/jobs/1
```

## Usage metering

Every pipeline run is tracked by `bonfyre-meter`:

```bash
bonfyre-meter report
```

Shows per-operation costs, total usage, and billing summary.
