---
type: project
cssclasses:
  - project
title: Batch Failure Queue
created: 2026-04-04
updated: 2026-04-04
status: planned
stage: setup
priority: high
review_cadence: weekly
system_role: Enabler
idea_link: [[01-Ideas/Batch Failure Queue]]
tags:
  - project
  - planned
  - operations
  - automation
  - throughput
aliases:
  - Batch Failure Queue
---


# Project: Batch Failure Queue

## Summary
Failure capture, retry handling, and partial-success reporting so batch runs fail safely instead of requiring manual investigation.

## Objective
Ship a failure queue that catches per-file errors during batch execution, continues processing, and emits a retry manifest.

## Success Definition
- deliverable: failure queue module that captures errors, continues batch, and writes `retry.json`
- proof: a batch with 1 intentionally broken file completes all other files and reports the failure
- deadline: ships alongside Batch Job Runner

## Tooling
### Project Tooling
- primary tools: Python, JSON, logging
- supporting tools: test fixtures with intentionally malformed audio

## Execution Plan
### Phase 1 - Error Capture
- [ ] Wrap single-job execution in try/except
- [ ] Capture error type, message, file path, timestamp
- [ ] Store failures in memory during batch run

### Phase 2 - Retry Manifest
- [ ] Write `retry.json` with failed jobs after batch completes
- [ ] Add `--retry <retry.json>` CLI flag to reprocess only failures
- [ ] Include original job metadata in retry manifest

### Phase 3 - Reporting
- [ ] Print batch summary: total, completed, failed, skipped
- [ ] Optional `--fail-fast` flag for strict mode

## Links
### Related Idea
- [[01-Ideas/Batch Failure Queue]]

### Adjacent Projects
- [[02-Projects/Project - Batch Job Runner]]
- [[02-Projects/Project - Local AI Transcription Service]]

---

## Execution Log
### 2026-04-04
- project created from idea promotion

---

## Status
planned
