---
type: project
cssclasses:
  - project
title: Transcript Cleanup Layer
created: 2026-04-04
updated: 2026-04-04
status: planned
stage: setup
priority: high
review_cadence: weekly
system_role: Enabler
idea_link: [[01-Ideas/Transcript Cleanup Layer]]
tags:
  - project
  - planned
  - text
  - quality
  - automation
aliases:
  - Transcript Cleanup Layer
---

## Related hubs
- [[04-Systems/02-Pipelines/Transcription Pipeline]]



# Project: Transcript Cleanup Layer

## Summary
Post-transcription pass that removes artifacts, fixes punctuation, and improves readability before summaries are generated.

## Objective
Ship a cleanup module that takes raw Whisper output and produces readable, filler-free text.

## Success Definition
- deliverable: `cleanup.py` that processes raw transcript into cleaned text
- proof: 10 real transcripts cleaned without losing meaning
- deadline: before Deliverable Formatter Engine work

## Tooling
### Project Tooling
- primary tools: Python, regex, text processing, optional LLM pass for structure
- supporting tools: test fixtures with raw Whisper output

## Execution Plan
### Phase 1 - Rule-Based Cleanup
- [ ] Remove common filler words (um, uh, like, you know)
- [ ] Fix repeated fragments and stutters
- [ ] Normalize punctuation and capitalization
- [ ] Remove timestamp artifacts if present

### Phase 2 - Structure Pass
- [ ] Add paragraph breaks at topic shifts
- [ ] Wire into Transcript Paragraphizer for deeper formatting
- [ ] Preserve speaker labels if diarization was applied

### Phase 3 - Integration
- [ ] Wire as pipeline step between transcription and summarization
- [ ] Add `--cleanup` / `--no-cleanup` CLI flags
- [ ] Compare cleaned vs raw output quality scores

## Links
### Related Idea
- [[01-Ideas/Transcript Cleanup Layer]]

### Adjacent Projects
- [[02-Projects/Project - Local AI Transcription Service]]
- [[02-Projects/Project - Transcript Paragraphizer]]
- [[02-Projects/Project - Quality Scoring Loop]]

---

## Execution Log
### 2026-04-04
- project created from idea promotion

---

## Status
planned
