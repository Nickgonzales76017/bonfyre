# BonfyreProject

Thin compiled fusion binary for:
- `BonfyreCMS`
- `BonfyreIndex`
- `BonfyreStitch`

## Purpose
- give the content graph projection engine one front door

## Build
```bash
make
```

## Usage
```bash
./bonfyre-project cms schema migrate --db /tmp/test.db --schemas ../BonfyreCMS/content-types
./bonfyre-project index build artifacts --db /tmp/index.db
./bonfyre-project refresh artifacts --db /tmp/index.db
./bonfyre-project stitch plan artifacts/family/artifact.json --target deliverable-md
```
