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
The interpreter-backed CLI is archived. Speech segmentation is provided by
`bonfyre-media-prep split-speech` until a native VAD organ is bound.
```

## Notes

- fallback backend is explicitly labeled in `status.json`
- this is designed to plug into `BonfyreTranscribe`
