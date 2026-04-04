---
type: project
cssclasses:
  - project
title: Batch Job Runner
created: 2026-04-04
updated: 2026-04-04
status: planned
stage: setup
priority: high
review_cadence: weekly
system_role: Enabler
idea_link: [[01-Ideas/Batch Job Runner]]
tags:
  - project
  - planned
  - automation
  - operations
  - throughput
aliases:
  - Batch Job Runner
---


# Project: Batch Job Runner

## Summary
Add batch execution so multiple audio files can be processed in one command instead of manual per-file runs.

## Objective
Ship a `--batch` mode that processes a directory of audio/intake files and produces structured deliverables for each.

## Success Definition
- deliverable: `--batch <dir>` flag on transcription CLI that processes N files and writes N output folders
- proof: 5-file batch completes end-to-end without manual intervention
- deadline: before first paying customer batch

## Tooling
### Project Tooling
- primary tools: Python, LocalAITranscriptionService CLI, file system operations
- supporting tools: test fixtures directory with varied audio files

## Execution Plan
### Phase 1 - Directory Scanner
- [ ] Accept `--batch <dir>` argument
- [ ] Discover audio files (mp3, m4a, wav, ogg, webm) or intake folders
- [ ] Build job queue from discovered files

### Phase 2 - Sequential Execution
- [ ] Process each job through existing single-file pipeline
- [ ] Write outputs to `outputs/<job-slug>/` per file
- [ ] Print summary: completed, failed, skipped

### Phase 3 - Resilience
- [ ] Integrate Batch Failure Queue for error handling
- [ ] Continue on single-file failure, emit retry list
- [ ] Partial-success reporting

## Links
### Related Idea
- [[01-Ideas/Batch Job Runner]]

### Adjacent Projects
- [[02-Projects/Project - Local AI Transcription Service]]
- [[02-Projects/Project - Batch Failure Queue]]
- [[02-Projects/Project - Audio Intake Normalizer]]

---

## Execution Log
### 2026-04-04
- project created from idea promotion

---

## Status
planned
