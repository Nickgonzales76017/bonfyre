# Bonfyre Brief

Tiny native briefing binary for transcript-to-brief conversion.

## What It Does

- reads transcript text
- emits:
  - `brief.md`
  - `brief-meta.json`

## Build

```bash
cd 10-Code/BonfyreBrief
make
```

## Usage

```bash
./bonfyre-brief transcript.txt outputs/brief-test
./bonfyre-brief transcript.txt outputs/brief-test --title "Founder Sample"
```

## Goal

This is the native output-shaping sibling to:
- `BonfyreMediaPrep`
- `BonfyreTranscribe`
