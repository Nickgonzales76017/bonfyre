# Whisper Model Cache Manager

Preflight checker and cache manager for Whisper models. Ensures the right model is downloaded, verified, and ready before any transcription job starts.

## Commands

```bash
# List cached models
python model_cache.py list

# Check if a model is cached and valid
python model_cache.py check --model base

# Download/warm a model into cache
python model_cache.py warm --model base

# Verify integrity of cached models
python model_cache.py verify
```

## Supported Models

tiny, base, small, medium, large-v2, large-v3

## Dependencies

- Python 3.10+
- requests (`pip install requests`)
