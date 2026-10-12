# Bonfyre Transcribe

Tiny native transcription binary built on top of `BonfyreMediaPrep` and Whisper CLI.

## What It Does

- normalize audio through `BonfyreMediaPrep`
- optionally denoise normalized audio through `BonfyreMediaPrep`
- optionally split speech through `BonfyreMediaPrep`
- optionally split speech through `SileroVADCLI`
- run Whisper with a stable local default
- emit:
  - `normalized.wav`
  - `transcript.txt`
  - `meta.json`
  - `transcribe-status.json`
  - `chunk-progress.json`

## Build

```bash
cd 10-Code/BonfyreTranscribe
make
```

## Usage

```bash
./bonfyre-transcribe input.m4a outputs/sample-job
./bonfyre-transcribe input.m4a outputs/sample-job --model base --language en
./bonfyre-transcribe input.m4a outputs/sample-job --whisper-binary /Users/nickgonzales/Library/Python/3.9/bin/whisper
./bonfyre-transcribe input.m4a outputs/sample-job --split-speech --min-silence 0.35 --min-speech 0.75 --padding 0.15
./bonfyre-transcribe input.m4a outputs/sample-job --split-speech --silero-vad --min-speech 1.0 --padding 0.15
```

## Notes

- default media-prep binary: `../BonfyreMediaPrep/bonfyre-media-prep`
- local default Whisper path on this machine:
  `/Users/nickgonzales/Library/Python/3.9/bin/whisper`
- when `--split-speech` is enabled, chunk transcripts are merged into one `normalized.txt`
- long split-aware runs expose progress through `chunk-progress.json`
