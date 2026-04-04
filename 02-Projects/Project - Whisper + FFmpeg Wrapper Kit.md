---
type: project
cssclasses:
  - projecttitle: Whisper + FFmpeg Wrapper Kit
created: 2026-04-03
updated: 2026-04-03
status: active
stage: build
priority: high
review_cadence: weekly
system_role: Builder
idea_link: [[01-Ideas/Whisper + FFmpeg Wrapper Kit]]
tags:
  - project
  - active
  - planned
aliases:
  - Project - Whisper + FFmpeg Wrapper Kit
  - Whisper + FFmpeg Wrapper Kit
---

## Related hubs
- [[04-Systems/02-Pipelines/Transcription Pipeline]]


# Project: Whisper + FFmpeg Wrapper Kit

## Summary
Build a small, reusable wrapper that standardizes ffmpeg preprocessing and Whisper transcription so local audio jobs become predictable and low-friction.

## Objective
Turn raw audio into a reliable transcript artifact with sane defaults, minimal commands, and a publishable utility path.

## Success Definition
- deliverable: one wrapper flow that preprocesses audio and returns a stable transcript output
- proof: the transcription service can move from transcript import to raw audio input
- deadline: before deeper transcript cleanup or batch automation work

## Tooling
### Project Tooling
- primary tools: Whisper CLI, ffmpeg, Python wrapper code, local file workflow
- supporting tools: transcription fixtures, output folders, environment checks

### Meta Tooling
- shared tooling or infrastructure: tooling stack, local bootstrap ideas, automation roadmap
- linked meta system:
  - [[04-Systems/04-Meta/Meta-System]]
  - [[04-Systems/04-Meta/Vault Operating System]]
  - [[04-Systems/04-Meta/Tooling Stack]]
  - [[04-Systems/04-Meta/n8n Workflow Map]]

## Execution Plan
### Phase 1 - Environment ✅ DONE
- `transcription.py` detects whether Whisper and ffmpeg exist via `--check-env`
- `bootstrap.py` documents installation assumptions

### Phase 2 - Wrapper ✅ DONE
- `transcription.py` normalizes audio to 16kHz mono WAV via ffmpeg
- Whisper runs with stable defaults (base model, FP32 CPU fallback)
- Transcript artifacts saved predictably to `outputs/<job-slug>/`

### Phase 3 - Reuse ✅ DONE
- wrapper is fully integrated into `LocalAITranscriptionService` pipeline
- reusable from CLI via `--audio-file` flag

### Phase 4 - Harden (remaining)
- evaluate standalone extraction as a publishable utility
- add model selection flag (base vs small vs medium)

## Tasks
### Now
- [x] define the wrapper interface — `transcription.py` handles normalize → transcribe → save
- [x] specify default ffmpeg normalization settings — 16kHz mono WAV
- [x] specify default Whisper invocation settings — base model, FP32 CPU
- [x] connect the wrapper to `10-Code/LocalAITranscriptionService` — fully integrated
- [x] test with one real audio file — Gaurav sample completed successfully (42s)
- [x] capture install/setup friction — `--check-env` validates all deps

### Next
- [ ] evaluate model selection (base vs small) for quality/speed tradeoff
- [ ] decide whether to extract as a standalone publishable utility

### Later
- [ ] publish as a small open-source utility if demand signal exists
- [ ] connect it to bootstrap and batch workflows

## Constraints
- local dependency installation is still a blocker
- the wrapper should stay small and boring
- utility design must not distract from proving the service

## Risks
- env setup remains fragile even after wrapping
- abstraction grows faster than actual usefulness
- utility ambitions outrun the immediate project need

## Metrics
- success metric: raw audio can enter the local transcription workflow reliably
- leading indicator: one command path exists from audio to transcript artifact
- failure condition: wrapper adds complexity without reducing friction

## Dependencies
### Requires
- [[02-Projects/Project - Local AI Transcription Service]]
- [[04-Systems/01-Core Systems/Whisper + FFmpeg Wrapper Kit]]
- [[01-Ideas/Whisper + FFmpeg Wrapper Kit]]

### Blocks
- [[01-Ideas/Audio Intake Normalizer]]
- [[01-Ideas/Local Bootstrap Kit]]
- [[01-Ideas/Batch Job Runner]]

## Execution Log
### 2026-04-03
- project created from the wrapper kit idea
- Phase 1 and Phase 2 are effectively complete — `transcription.py` inside LocalAITranscriptionService already wraps ffmpeg normalization + Whisper invocation + artifact saving
- `--check-env` validates ffmpeg, whisper, and piper availability
- Gaurav sample processed successfully: ffmpeg normalize → Whisper base → transcript artifacts in 42 seconds
- Whisper falls back to FP32 on CPU (no CoreML/MPS acceleration yet)
- this project is now a thin wrapper around existing code — standalone extraction is the only remaining question

## Decisions
- the wrapper lives inside LocalAITranscriptionService, not as a separate project — extraction only makes sense if there's external demand
- base model is the default — small model evaluation is deferred until quality complaints arise
- standalone publication is parked until the transcription service proves demand

## Links
### Source Idea
- [[01-Ideas/Whisper + FFmpeg Wrapper Kit]]

### Related Projects
- [[02-Projects/Project - Local AI Transcription Service]]

### Systems
- [[04-Systems/04-Meta/Tooling Stack]]

### Concepts
- [[04-Systems/03-Concepts/Infrastructure]]
- [[04-Systems/03-Concepts/Local-First]]
- [[04-Systems/03-Concepts/Automation]]
- project created from captured blocker/upgrade idea
- positioned as the first concrete infrastructure upgrade for local transcription

## Decisions
- solve the raw audio -> transcript gap before polishing downstream formatting
- keep the wrapper reusable, but grounded in the transcription project first

## Links
### Source Idea
- [[01-Ideas/Whisper + FFmpeg Wrapper Kit]]

### Related Projects
- [[02-Projects/Project - Local AI Transcription Service]]
- [[02-Projects/Project - Personal Data Engine]]

### Systems
- [[04-Systems/01-Core Systems/Local AI Transcription Service]]
- [[04-Systems/01-Core Systems/Whisper + FFmpeg Wrapper Kit]]
- [[04-Systems/04-Meta/Tooling Stack]]

### Concepts
- [[04-Systems/03-Concepts/Automation]]
- [[04-Systems/03-Concepts/Infrastructure]]

## Tags
#project #planned #tooling #audio