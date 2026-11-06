# JqCanon

JSON canonicalization and manifest transform operator using `jq`.

## Subcommands
- `canonicalize` — sort keys deterministically (for hash-stable manifests)
- `query` — extract data with jq filter
- `transform` — rewrite JSON with jq expression
- `diff` — structural diff between two JSON files

## Usage
```bash
bash bin/jq_canon.sh canonicalize artifact.json --out artifact.canon.json
bash bin/jq_canon.sh query artifact.json --filter '.operators | length'
bash bin/jq_canon.sh diff v1.json v2.json
```

## Install
```bash
brew install jq
```
