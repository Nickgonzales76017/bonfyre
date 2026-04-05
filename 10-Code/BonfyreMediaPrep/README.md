# Bonfyre Media Prep

Tiny native media-prep binary for hot-path audio work.

## What It Does

- `inspect` via `ffprobe`
- `normalize` via `ffmpeg`
- `chunk` via `ffmpeg`

This is the first low-level Bonfyre binary. It is meant to stay small, fast, and composable.

## Build

```bash
cd 10-Code/BonfyreMediaPrep
make
```

## Usage

```bash
./bonfyre-media-prep inspect input.m4a
./bonfyre-media-prep normalize input.m4a output.wav
./bonfyre-media-prep normalize input.m4a output.wav --sample-rate 16000 --channels 1 --trim-silence --loudnorm
./bonfyre-media-prep chunk input.m4a chunks/chunk-%03d.wav --segment-seconds 300
```

## Goal

This is the low-level path forward for:
- `WhisperFFmpegWrapperKit`
- `LocalAITranscriptionService`
- future `bonfyre-transcribe`
- future `bonfyre-sync`
