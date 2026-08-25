# Repackaged Service Marketplace

Bundle designer and pricing engine. Combines individual service components into outcome-based packages with clear cost, margin, and delivery steps.

## Usage

```bash
# List available service components
python marketplace.py components

# Design a new bundle
python marketplace.py bundle --name "Founder Interview Package" \
  --components transcription,summary,action-items \
  --sell-price 35 --target founder

# List all bundles with margin analysis
python marketplace.py list

# Compare bundle vs à la carte pricing
python marketplace.py compare --bundle "Founder Interview Package"

# Export a bundle as offer copy
python marketplace.py export --bundle "Founder Interview Package"

# Write a lightweight status snapshot
python marketplace.py status
```

## Dependencies

- Python 3.10+
- JSON storage (no external deps)

## House Style

- persistent data lives in `data/`
- generated snapshots live in `reports/`
- `status` gives the fast project-level read
