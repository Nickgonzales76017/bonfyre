# BonfyreWeaviateIndex

Small native Weaviate payload builder for Bonfyre.

## Purpose
- read a deterministic embedding artifact
- read Bonfyre metadata
- emit a stable local ingest payload
- write `status.json`

## Build
```bash
make
```

## Usage
```bash
./bonfyre-weaviate-index \
  --emb ../BonfyreEmbed/outputs/smoke/transcript-embedding.json \
  --meta ../LocalAITranscriptionService/outputs-smoke-native-embed/native-embed-smoke/meta.json \
  --out outputs/sample/weaviate-batch.json
```

## Notes
This binary owns deterministic payload generation. It does not try to be a full
live Weaviate client.
