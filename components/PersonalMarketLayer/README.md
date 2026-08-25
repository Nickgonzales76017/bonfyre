# Personal Market Layer

Turns reviewed proof assets into buyer-facing offer packages.

## Run
```bash
cd 10-Code/PersonalMarketLayer
PYTHONPATH=src python3 -m personal_market_layer.cli
```

Or point it at a specific proof:

```bash
cd 10-Code/PersonalMarketLayer
PYTHONPATH=src python3 -m personal_market_layer.cli --proof-dir ../LocalAITranscriptionService/samples/proof-deliverables/founder-sample-pickfu
```

Sync every reviewed proof into market outputs:

```bash
cd 10-Code/PersonalMarketLayer
PYTHONPATH=src python3 -m personal_market_layer.cli --sync-all
```

## Output
Each generated offer package includes:
- `offer.json`
- `offer.md`
- `outreach.md`
- `listing.md`
- `variants.md`
- `variant-outreach.md`
- a generated vault note in `05-Monetization/`
- a generated vault catalog and pipeline snapshot in `05-Monetization/`
