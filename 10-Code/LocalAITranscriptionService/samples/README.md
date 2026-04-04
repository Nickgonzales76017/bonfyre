# Sample Workspace

This folder is the working lane for proof clips, intake packages, and portfolio-grade outputs.

## Structure
- `incoming-audio/`
  Raw public clips or self-recorded memos before browser intake packaging.
- `intake-packages/`
  One-file `.intake-package.json` exports from `10-Code/WebWorkerSaaS`.
- `proof-deliverables/`
  Best finished outputs that can become proof assets or outreach samples.
- `benchmarks/public-clips/`
  Public-source clips used for repeatable evaluation and proof generation.
- `benchmarks/self-memos/`
  Founder/operator-style self-recorded memos used to test messy real workflows.

## Naming
Use:

`persona-##-topic-shortslug`

Examples:
- `founder-01-pickfu-assumptions`
- `customer-01-gaurav-conversations`
- `founder-02-ganesh-operator`
- `investor-01-masha-communications`
- `business-01-chris-side-hustle`

Keep related files aligned:
- `founder-01-pickfu-assumptions.m4a`
- `founder-01-pickfu-assumptions.intake-package.json`
- `founder-01-pickfu-assumptions-proof.md`

## Run Pattern
```bash
cd 10-Code/LocalAITranscriptionService
PYTHONPATH=src python3 -m local_ai_transcription_service.cli \
  --intake-package samples/intake-packages/founder-01-pickfu-assumptions.intake-package.json \
  --output-root outputs
```
