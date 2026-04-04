---
type: project
cssclasses:
  - project
title: Audio Intake Normalizer
created: 2026-04-04
updated: 2026-04-04
status: planned
stage: setup
priority: high
review_cadence: weekly
system_role: Enabler
idea_link: [[01-Ideas/Audio Intake Normalizer]]
tags:
  - project
  - planned
  - audio
  - preprocessing
  - automation
aliases:
  - Audio Intake Normalizer
---

## Related hubs
- [[04-Systems/02-Pipelines/Transcription Pipeline]]



# Project: Audio Intake Normalizer

## Summary
Preprocessing layer that converts messy user audio (voice notes, calls, exports) into a normalized mono format with stable bitrate and sampling defaults before transcription.

## Objective
Ship a CLI tool that accepts any common audio format and outputs a consistent 16kHz mono WAV ready for Whisper.

## Success Definition
- deliverable: `normalize.py` that handles mp3/m4a/ogg/wav/webm → 16kHz mono WAV
- proof: 20 real-world files successfully normalized without manual intervention
- deadline: before batch job runner is operational

## Tooling
### Project Tooling
- primary tools: Python, ffmpeg, subprocess
- supporting tools: test fixtures (varied audio formats), Whisper + FFmpeg Wrapper Kit

## Execution Plan
### Phase 1 - Core Normalizer
- [ ] Detect input format and codec via ffprobe
- [ ] Convert to 16kHz mono WAV via ffmpeg
- [ ] Handle edge cases: stereo, high sample rate, compressed formats
- [ ] Validate output matches Whisper expectations

### Phase 2 - Integration
- [ ] Wire into LocalAITranscriptionService pipeline as preprocessing step
- [ ] Add `--normalize` flag to CLI

### Phase 3 - Harden
- [ ] Add loudness normalization (EBU R128 or simple peak norm)
- [ ] Silence trimming option for voice memos with dead air

## Links
### Related Idea
- [[01-Ideas/Audio Intake Normalizer]]

### Adjacent Projects
- [[02-Projects/Project - Whisper + FFmpeg Wrapper Kit]]
- [[02-Projects/Project - Local AI Transcription Service]]
- [[02-Projects/Project - Batch Job Runner]]

---

## Execution Log
### 2026-04-04
- project created from idea promotion

---

## Status
planned
