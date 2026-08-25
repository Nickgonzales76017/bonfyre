# Silero VAD CLI

Local speech-splitting CLI with a stable chunk manifest contract.

## What It Does

- tries `silero_vad` when available
- falls back to `BonfyreMediaPrep split-speech` when it is not
- emits:
  - `chunk-000.wav`, `chunk-001.wav`, ...
  - `speech-chunks.json`
  - `status.json`

## Usage

```bash
python3 bin/silero_vad_cli.py --audio input.wav --out out/chunks
python3 bin/silero_vad_cli.py --audio input.wav --out out/chunks --min-speech 1.0 --padding 0.15
```

## Notes

- fallback backend is explicitly labeled in `status.json`
- this is designed to plug into `BonfyreTranscribe`
