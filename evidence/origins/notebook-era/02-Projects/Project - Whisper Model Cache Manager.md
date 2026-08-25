---
type: project
cssclasses:
  - project
title: Whisper Model Cache Manager
created: 2026-04-04
updated: 2026-04-04
status: planned
stage: setup
priority: high
review_cadence: weekly
system_role: Enabler
idea_link: [[01-Ideas/Whisper Model Cache Manager]]
tags:
  - project
  - planned
  - tooling
  - local-ai
  - operations
aliases:
  - Whisper Model Cache Manager
---

## Related hubs
- [[04-Systems/02-Pipelines/Transcription Pipeline]]



# Project: Whisper Model Cache Manager

## Summary
Manage model downloads, local model selection, and cache location so Whisper setup friction doesn't surprise operators or buyer workflows.

## Objective
Ship a CLI utility that preflights the selected Whisper model, reports cache status, and optionally warms it before a buyer-facing run.

## Success Definition
- deliverable: `model_cache.py` with `--check`, `--warm`, and `--list` commands
- proof: fresh machine can preflight and warm a model before first real job
- deadline: ships alongside Local Bootstrap Kit

## Tooling
### Project Tooling
- primary tools: Python, whisper model paths, file system checks
- supporting tools: Local Bootstrap Kit integration

## Execution Plan
### Phase 1 - Cache Inspector
- [ ] Detect default Whisper model cache location (~/.cache/whisper or custom)
- [ ] List available cached models with sizes
- [ ] Report whether selected model is cached: `--check <model>`

### Phase 2 - Warm / Download
- [ ] Add `--warm <model>` to trigger download if missing
- [ ] Show download progress
- [ ] Verify model integrity after download

### Phase 3 - Integration
- [ ] Wire into Local Bootstrap Kit as model setup step
- [ ] Add preflight check to transcription pipeline startup
- [ ] Support custom cache directory via env var or config

## Links
### Related Idea
- [[01-Ideas/Whisper Model Cache Manager]]

### Adjacent Projects
- [[02-Projects/Project - Local Bootstrap Kit]]
- [[02-Projects/Project - Whisper + FFmpeg Wrapper Kit]]
- [[02-Projects/Project - Local AI Transcription Service]]

---

## Execution Log
### 2026-04-04
- project created from idea promotion

---

## Status
planned
