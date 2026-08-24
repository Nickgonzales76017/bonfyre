---
type: project
cssclasses:
  - project
title: Local Bootstrap Kit
created: 2026-04-04
updated: 2026-04-04
status: planned
stage: setup
priority: high
review_cadence: weekly
system_role: Enabler
idea_link: [[01-Ideas/Local Bootstrap Kit]]
tags:
  - project
  - planned
  - tooling
  - setup
  - infrastructure
aliases:
  - Local Bootstrap Kit
---

## Related hubs
- [[04-Systems/02-Pipelines/Transcription Pipeline]]



# Project: Local Bootstrap Kit

## Summary
One-command setup kit that gets a new machine ready for the full local transcription workflow — installs deps, verifies tools, creates output folders, prints next steps.

## Objective
Ship a `bootstrap.sh` that takes a fresh macOS machine to a working transcription environment in under 5 minutes.

## Success Definition
- deliverable: `bootstrap.sh` that installs/verifies Whisper, ffmpeg, Python deps, creates folder structure
- proof: clean macOS install → working `transcription.py --check-env` in one script run
- deadline: before onboarding any collaborator or second machine

## Tooling
### Project Tooling
- primary tools: bash/zsh, Homebrew, pip, system checks
- supporting tools: NightlyBrainstorm setup.sh as reference

## Execution Plan
### Phase 1 - Core Bootstrap
- [ ] Check for Homebrew, install if missing
- [ ] Install/verify: ffmpeg, Python 3.10+, Whisper CLI
- [ ] Create standard directory structure (inputs/, outputs/, models/)
- [ ] Verify tool versions and print summary

### Phase 2 - Model Setup
- [ ] Wire in Whisper Model Cache Manager for model preflight
- [ ] Download default model if not cached
- [ ] Verify model loads successfully

### Phase 3 - Validation
- [ ] Run `--check-env` against LocalAITranscriptionService
- [ ] Run a smoke test with a sample audio file
- [ ] Print clear next-steps guide

## Links
### Related Idea
- [[01-Ideas/Local Bootstrap Kit]]

### Adjacent Projects
- [[02-Projects/Project - Whisper + FFmpeg Wrapper Kit]]
- [[02-Projects/Project - Whisper Model Cache Manager]]
- [[02-Projects/Project - Local AI Transcription Service]]

---

## Execution Log
### 2026-04-04
- project created from idea promotion

---

## Status
planned
