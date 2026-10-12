---
type: project
cssclasses:
  - project
title: Transcript Asset Store
created: 2026-04-04
updated: 2026-04-04
status: planned
stage: setup
priority: medium
review_cadence: weekly
system_role: Enabler
idea_link: [[01-Ideas/Transcript Asset Store]]
tags:
  - project
  - planned
  - storage
  - assets
  - data
aliases:
  - Transcript Asset Store
---

## Related hubs
- [[04-Systems/02-Pipelines/Transcription Pipeline]]



# Project: Transcript Asset Store

## Summary
Structured storage and retrieval layer for transcripts, summaries, and deliverables so completed jobs become reusable assets instead of one-off files.

## Objective
Ship an indexed asset store where every completed job is searchable, browsable, and reusable.

## Success Definition
- deliverable: `assets/` directory with per-job folders, index JSON, and a simple search CLI
- proof: 20+ completed jobs indexed and searchable by client, date, and keyword
- deadline: after first 20 paid jobs completed

## Tooling
### Project Tooling
- primary tools: Python, JSON index, file system, optional SQLite for search
- supporting tools: Personal Data Engine for pattern analysis

## Execution Plan
### Phase 1 - Structured Storage
- [ ] Define asset folder structure: `assets/<job-id>/` with transcript, summary, deliverable, metadata JSON
- [ ] Auto-save completed pipeline outputs to asset store
- [ ] Build index JSON with searchable metadata

### Phase 2 - Search
- [ ] CLI search: `--search <query>` across job titles, clients, notes
- [ ] Filter by date range, status, buyer type
- [ ] Output matching job metadata

### Phase 3 - Reuse
- [ ] Link asset store to Quality Benchmark Pack for evaluation
- [ ] Enable re-processing of stored transcripts with new prompts/templates
- [ ] Optional: export asset bundles for backup

## Links
### Related Idea
- [[01-Ideas/Transcript Asset Store]]

### Adjacent Projects
- [[02-Projects/Project - Local AI Transcription Service]]
- [[02-Projects/Project - Personal Data Engine]]
- [[02-Projects/Project - Quality Benchmark Pack]]

---

## Execution Log
### 2026-04-04
- project created from idea promotion

---

## Status
planned
