# Bonfyre Proof

Tiny native proof utility for proof-dir inspection and native proof bundle generation.

## What It Does

- `inspect` a proof directory
- `bundle` a proof directory into a small native proof package

## Build

```bash
cd 10-Code/BonfyreProof
make
```

## Usage

```bash
arch -arm64 ./bonfyre-proof inspect ../LocalAITranscriptionService/samples/proof-deliverables/founder-sample-pickfu
arch -arm64 ./bonfyre-proof bundle ../LocalAITranscriptionService/samples/proof-deliverables/founder-sample-pickfu ./outputs/founder-sample-pickfu
```
