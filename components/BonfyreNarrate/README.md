# BonfyreNarrate

Small native renderer contract for narrated Bonfyre artifacts.

## Purpose
- read finalized artifact text
- emit narration text
- attempt Piper render when available
- always emit a render manifest honestly

## Build
```bash
make
```

## Usage
```bash
./bonfyre-narrate ../BonfyreBrief/outputs/pickfu-brief/brief.md outputs/sample
```

## Outputs
- `narration.txt`
- `artifact.manifest.json`
- `artifact.wav` when `piper` is available and render succeeds

## Notes
This is intentionally narrow. It stabilizes the render contract first, then heavier
Piper/browser integration can build on top of it.
