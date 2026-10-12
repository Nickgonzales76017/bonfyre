# Audio Intake Normalizer

Preprocessing layer that converts messy user audio into normalized 16kHz mono WAV for Whisper.

## Run

```bash
python3 normalize.py input.mp3 --output normalized.wav
python3 normalize.py --batch inputs/
```

## Requires
- ffmpeg
- Python 3.10+
