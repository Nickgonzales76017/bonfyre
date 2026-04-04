# Service Arbitrage Hub

Service arbitrage tracker. Map services with buy price, sell price, fulfillment source, and quality benchmarks. Track margin per job and find the best spreads.

## Boundary

- this project owns market-level pricing and source comparisons across services
- it tracks where spreads are attractive before delivery happens
- it is upstream of fulfillment operations
- once a job is sold and enters reviewer/QA flow, that work belongs in `AIOverseasLaborPipeline`

## Usage

```bash
# Add a service to track
python arbitrage.py add --name "AI Transcription" \
  --buy-price 1.50 --sell-price 15.00 \
  --source "local-whisper" --category transcription

# List all tracked services with margins
python arbitrage.py list

# Log a completed job
python arbitrage.py job --service "AI Transcription" \
  --actual-cost 1.80 --sold-for 15.00 --quality pass

# View margin report across all services
python arbitrage.py report

# Find best arbitrage opportunities (highest margin %)
python arbitrage.py opportunities

# Write a lightweight status snapshot
python arbitrage.py status

# Hand a sold job into the fulfillment pipeline
python arbitrage.py handoff --job-id 1 --type full-deliverable
```

## Dependencies

- Python 3.10+
- SQLite (built-in)

## House Style

- persistent data lives in `data/`
- generated snapshots live in `reports/`
- `status` gives the fast project-level read
- `handoff` is the boundary line into `AIOverseasLaborPipeline`
