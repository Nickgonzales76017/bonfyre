# BonfyreRender

Thin compiled fusion binary for:
- `BonfyreBrief`
- `BonfyreNarrate`
- `BonfyrePack`

## Purpose
- give the universal artifact renderer one front door

## Build
```bash
make
```

## Usage
```bash
./bonfyre-render artifact transcript.txt outputs/rendered --title "Sample"
./bonfyre-render package proof-dir offer-dir outputs/package
```
