---
type: project
cssclasses:
  - project
title: Transcript Paragraphizer
created: 2026-04-04
updated: 2026-04-04
status: planned
stage: setup
priority: medium
review_cadence: weekly
system_role: Enabler
idea_link: [[01-Ideas/Transcript Paragraphizer]]
tags:
  - project
  - planned
  - text
  - readability
  - quality
aliases:
  - Transcript Paragraphizer
---

## Related hubs
- [[04-Systems/02-Pipelines/Transcription Pipeline]]



# Project: Transcript Paragraphizer

## Summary
Post-processing layer that turns cleaned transcript text into readable paragraphs and sections instead of a flat wall of text.

## Objective
Ship a paragraphing module that splits cleaned transcripts into sensible blocks.

## Success Definition
- deliverable: `paragraphize.py` that takes cleaned text and outputs paragraphed markdown
- proof: 10 transcripts paragraphed with natural-feeling breaks
- deadline: ships alongside or after Transcript Cleanup Layer

## Tooling
### Project Tooling
- primary tools: Python, text heuristics, optional LLM for topic-shift detection
- supporting tools: Transcript Cleanup Layer output as input

## Execution Plan
### Phase 1 - Heuristic Splitting
- [ ] Split on sentence boundaries + length thresholds
- [ ] Detect topic shifts via keyword/phrase changes
- [ ] Add blank lines between paragraph blocks

### Phase 2 - Section Headers
- [ ] Detect major topic transitions for section-level breaks
- [ ] Generate simple heading text from context
- [ ] Output as markdown with `##` headers

### Phase 3 - Integration
- [ ] Wire as pipeline step after cleanup, before summarization
- [ ] Preserve paragraph structure in deliverable output
- [ ] Compare readability scores before/after paragraphizing

## Links
### Related Idea
- [[01-Ideas/Transcript Paragraphizer]]

### Adjacent Projects
- [[02-Projects/Project - Transcript Cleanup Layer]]
- [[02-Projects/Project - Deliverable Formatter Engine]]
- [[02-Projects/Project - Local AI Transcription Service]]

---

## Execution Log
### 2026-04-04
- project created from idea promotion

---

## Status
planned
