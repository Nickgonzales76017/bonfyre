---
type: project
cssclasses:
  - project
title: Summary Prompt Pack
created: 2026-04-04
updated: 2026-04-04
status: planned
stage: setup
priority: medium
review_cadence: weekly
system_role: Enabler
idea_link: [[01-Ideas/Summary Prompt Pack]]
tags:
  - project
  - planned
  - prompts
  - summaries
  - packaging
aliases:
  - Summary Prompt Pack
---

## Related hubs
- [[04-Systems/02-Pipelines/Transcription Pipeline]]



# Project: Summary Prompt Pack

## Summary
Reusable summary and action-item prompt templates tuned for different buyer types and recording contexts.

## Objective
Ship a prompt library that generates distinct outputs (executive summary, meeting notes, creator repurposing, student notes) from the same transcript.

## Success Definition
- deliverable: `prompts/` directory with 4+ prompt templates, selectable via intake metadata
- proof: same transcript produces meaningfully different outputs per template
- deadline: before expanding beyond founder-memo buyer type

## Tooling
### Project Tooling
- primary tools: prompt text files, Python template loader, LLM inference (llama.cpp or API)
- supporting tools: NightlyBrainstorm prompt infrastructure as reference

## Execution Plan
### Phase 1 - Core Templates
- [ ] Write prompt templates: executive-summary, meeting-notes, student-notes, creator-repurpose
- [ ] Define input/output contract (transcript text → structured markdown)
- [ ] Test each template against 3 sample transcripts

### Phase 2 - Selection Logic
- [ ] Map buyer type / output goal to prompt template
- [ ] Wire template selection into pipeline based on intake metadata
- [ ] Fallback to general-purpose template if no match

### Phase 3 - Iteration
- [ ] Collect feedback on output quality per template
- [ ] Add specialized templates: legal-deposition, podcast-episode, sales-call
- [ ] Version prompt templates for A/B comparison

## Links
### Related Idea
- [[01-Ideas/Summary Prompt Pack]]

### Adjacent Projects
- [[02-Projects/Project - Local AI Transcription Service]]
- [[02-Projects/Project - Deliverable Formatter Engine]]
- [[02-Projects/Project - Speaker Segmentation Layer]]

---

## Execution Log
### 2026-04-04
- project created from idea promotion

---

## Status
planned
