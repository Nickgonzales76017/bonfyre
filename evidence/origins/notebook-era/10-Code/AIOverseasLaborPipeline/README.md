# AI + Overseas Labor Pipeline

Hybrid fulfillment engine: AI handles the first pass, human reviewers handle edge cases. Tracks cost, quality, and margin per job.

## Boundary

- this project owns delivery operations after a service has already been sold
- it tracks job-level QA, reviewer cost, rework, and fulfillment margin
- it is downstream of pricing and offer design
- it should not be the source of truth for market spreads across services

## Usage

```bash
# Create a new review job from an AI output
python pipeline.py create --input outputs/transcript.txt --type transcription

# List pending review jobs
python pipeline.py list --status pending

# Record a reviewer's QA result
python pipeline.py review --job-id 1 --result pass --reviewer "maria" --notes "Minor punctuation fixes"

# View margin report
python pipeline.py margin

# Write a lightweight status snapshot
python pipeline.py status

# Flag a job for re-review
python pipeline.py review --job-id 1 --result fail --reviewer "maria" --notes "Speaker labels wrong"

# Carry upstream source metadata into fulfillment
python pipeline.py create --input outputs/transcript.txt --type full-deliverable \
  --source-system ServiceArbitrageHub --source-job-id 4 --service-name "AI Transcription"
```

## Schema

Jobs track: input file, type, AI cost estimate, review cost, reviewer, QA result, total cost, sell price, margin, and optional upstream source metadata.

## Dependencies

- Python 3.10+
- SQLite (built-in)

## House Style

- persistent data lives in `data/`
- generated snapshots live in `reports/`
- `status` gives the fast project-level read
