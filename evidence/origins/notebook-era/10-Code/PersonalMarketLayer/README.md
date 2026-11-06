# Personal Market Layer

Turns reviewed proof assets into buyer-facing offer packages.

## Run
```bash
cd components/PersonalMarketLayer
PYTHONPATH=src python3 -m personal_market_layer.cli
```

Or point it at a specific proof:

```bash
cd components/PersonalMarketLayer
PYTHONPATH=src python3 -m personal_market_layer.cli --proof-dir ../LocalAITranscriptionService/samples/proof-deliverables/founder-sample-pickfu
```

Sync every reviewed proof into market outputs:

```bash
cd components/PersonalMarketLayer
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
- a generated offer projection in `generated/projections/commercial/`
- a generated catalog and pipeline snapshot in `generated/projections/commercial/`
