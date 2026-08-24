---
type: project
cssclasses:
  - project
title: Speaker Segmentation Layer
created: 2026-04-04
updated: 2026-04-04
status: planned
stage: setup
priority: medium
review_cadence: weekly
system_role: Enabler
idea_link: [[01-Ideas/Speaker Segmentation Layer]]
tags:
  - project
  - planned
  - audio
  - structure
  - quality
aliases:
  - Speaker Segmentation Layer
  - Speaker Diarization Layer
---

## Related hubs
- [[04-Systems/02-Pipelines/Transcription Pipeline]]



# Project: Speaker Segmentation Layer

## Summary
Add speaker-aware structure so multi-person recordings show who said what, making summaries and action items far more useful.

## Objective
Ship a diarization module that labels transcript segments by speaker before summary generation.

## Success Definition
- deliverable: `diarize.py` that splits a transcript into speaker-labeled segments
- proof: 5 two-speaker conversations correctly split with 80%+ accuracy
- deadline: after core transcription pipeline is stable

## Tooling
### Project Tooling
- primary tools: Python, pyannote.audio or whisperx diarization, ffmpeg for audio chunking
- supporting tools: test recordings with known speaker count

## Execution Plan
### Phase 1 - Basic Diarization
- [ ] Evaluate pyannote.audio vs whisperx for local speaker diarization
- [ ] Implement speaker segmentation on mono audio
- [ ] Output labeled transcript segments: `[Speaker A] text...`

### Phase 2 - Integration
- [ ] Wire into LocalAITranscriptionService pipeline between transcription and summarization
- [ ] Add `--diarize` flag to CLI
- [ ] Pass speaker labels into summary prompt for context

### Phase 3 - Refinement
- [ ] Handle 3+ speaker scenarios
- [ ] Add speaker name mapping from intake metadata
- [ ] Tune accuracy using Quality Benchmark Pack

## Links
### Related Idea
- [[01-Ideas/Speaker Segmentation Layer]]

### Adjacent Projects
- [[02-Projects/Project - Local AI Transcription Service]]
- [[02-Projects/Project - Quality Benchmark Pack]]
- [[02-Projects/Project - Summary Prompt Pack]]

---

## Execution Log
### 2026-04-04
- project created from idea promotion

---

## Status
planned
