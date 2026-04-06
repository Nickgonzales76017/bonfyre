# Full Audio-to-Invoice Pipeline Demo

This example runs the complete Bonfyre pipeline on an audio file — from intake to packaged deliverable.

## What it does

```
interview.mp3 → normalize → hash → transcribe → clean → paragraph → brief → proof → offer → pack
```

Output: a ZIP containing transcript, summary, action items, quality score, and pricing proposal.

## Quick start

```bash
# 1. Build (if you haven't already)
cd ../../
make
cd examples/full-pipeline/

# 2. Run the pipeline on any audio file
./run.sh /path/to/your/audio.mp3
```

## What's inside

| File | Purpose |
|---|---|
| `run.sh` | Entry point — runs the full pipeline |
| `output/` | Created at runtime — holds all artifacts |

## Step-by-step (manual)

If you want to run each binary individually instead of the unified pipeline:

```bash
# Set up
BONFYRE=../../cmd
INPUT=interview.mp3
WORK=./work
mkdir -p "$WORK"

# 1. Intake
$BONFYRE/BonfyreIngest/bonfyre-ingest intake "$INPUT" --out "$WORK/"

# 2. Normalize audio (16 kHz mono WAV)
$BONFYRE/BonfyreMediaPrep/bonfyre-media-prep normalize "$WORK/$INPUT"

# 3. Content-address
$BONFYRE/BonfyreHash/bonfyre-hash file "$WORK/interview.wav"

# 4. Transcribe
$BONFYRE/BonfyreTranscribe/bonfyre-transcribe run "$WORK/interview.wav" --out "$WORK/transcript.json"

# 5. Clean transcript
$BONFYRE/BonfyreTranscriptClean/bonfyre-transcript-clean run "$WORK/transcript.json" --out "$WORK/clean.json"

# 6. Structure into paragraphs
$BONFYRE/BonfyreParagraph/bonfyre-paragraph run "$WORK/clean.json" --out "$WORK/paragraphs.json"

# 7. Generate brief
$BONFYRE/BonfyreBrief/bonfyre-brief generate "$WORK/paragraphs.json" --out "$WORK/brief.md"

# 8. Quality score
$BONFYRE/BonfyreProof/bonfyre-proof score "$WORK/" --out "$WORK/proof.json"

# 9. Generate pricing
$BONFYRE/BonfyreOffer/bonfyre-offer generate "$WORK/proof.json" --out "$WORK/offer.json"

# 10. Package
$BONFYRE/BonfyrePack/bonfyre-pack bundle "$WORK/" --out deliverable.zip
```

## Expected output

```
output/
├── transcript.json      # Full transcript
├── clean.json           # Cleaned transcript
├── paragraphs.json      # Structured paragraphs
├── brief.md             # Executive summary + action items
├── proof.json           # Quality score
├── offer.json           # Pricing proposal
└── deliverable.zip      # Everything packaged
```

## Performance

On Apple M-series:
- **Unified pipeline**: 5–8 ms (excluding transcription model load)
- **Sequential (10 binaries)**: ~76 ms
- **Memory**: < 3 MB peak RSS
