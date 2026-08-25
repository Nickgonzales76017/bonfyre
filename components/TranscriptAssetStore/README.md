# Transcript Asset Store

Structured storage and retrieval for completed transcription jobs — searchable index of all deliverables.

## Run

```bash
python3 store.py save outputs/job-slug/
python3 store.py search "client meeting"
python3 store.py list --since 2026-04-01
```

## Requires
- Python 3.10+
