# Speaker Segmentation Layer

Speaker diarization for multi-person recordings — labels transcript segments by speaker.

## Run

```bash
python3 diarize.py input.wav --output labeled.txt
python3 diarize.py input.wav --speakers 2
```

## Requires
- Python 3.10+
- pyannote.audio or whisperx (for diarization models)
- ffmpeg
