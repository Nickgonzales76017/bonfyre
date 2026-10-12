# Bonfyre Sync

Tiny native sync/contract utility for Bonfyre browser and local artifacts.

## What It Does

- `inspect-intake` for `.intake-package.json`
- `inspect-status` for `browser-status.json`

## Build

```bash
cd 10-Code/BonfyreSync
make
```

## Usage

```bash
./bonfyre-sync inspect-intake ../LocalAITranscriptionService/samples/intake-packages/founder-01-pickfu-assumptions.intake-package.json
./bonfyre-sync inspect-status ../LocalAITranscriptionService/outputs/founder-sample-pickfu-assumptions/browser-status.json
```
