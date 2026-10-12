# Batch Job Runner

Batch execution for the transcription pipeline — process a directory of files in one command.

## Run

```bash
python3 batch.py inputs/ --output outputs/
python3 batch.py inputs/ --retry retry.json
```

## Requires
- Python 3.10+
- LocalAITranscriptionService on PATH or importable
