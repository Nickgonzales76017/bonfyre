# BonfyreRuntime

Thin compiled fusion binary for:
- `BonfyreQueue`
- `BonfyrePipeline`
- `BonfyreLedger`

## Purpose
- provide one front door for queue, execution, and replay-oriented history

## Build
```bash
make
```

## Usage
```bash
./bonfyre-runtime run file.txt --type text --out outputs/run
./bonfyre-runtime run-ledger file.txt --type text --out outputs/run
./bonfyre-runtime queue enqueue queue.tsv demo-job payload.json
./bonfyre-runtime ledger assess artifact.json
```
