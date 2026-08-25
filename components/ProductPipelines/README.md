# Product Pipelines

Thin orchestration layer over the active Bonfyre products.

This folder does not replace the underlying project code. It ties the working systems together into named loops so we can run products instead of manually remembering a dozen commands.

## Pipelines

### `transcription-revenue`
Refreshes the revenue-facing chain:
- `PersonalMarketLayer`
- `QuietDistributionEngine`
- `PersonalDataEngine`
- `LocalAITranscriptionService` proof state

Writes:
- `reports/transcription-revenue.json`
- `reports/transcription-revenue.md`

### `service-delivery`
Refreshes the delivery/arbitrage chain:
- `ServiceArbitrageHub`
- `AIOverseasLaborPipeline`
- `RepackagedServiceMarketplace`
- `PersonalDataEngine`

Writes:
- `reports/service-delivery.json`
- `reports/service-delivery.md`

### `all-active`
Runs both active product loops.

## Run

```bash
cd 10-Code/ProductPipelines
python3 orchestrate.py list
python3 orchestrate.py run transcription-revenue
python3 orchestrate.py run service-delivery
python3 orchestrate.py run all-active
```

## Rule

Use this layer to:
- refresh product state
- tie together existing systems
- generate one readable snapshot per product loop

Do not bury core business logic here.
