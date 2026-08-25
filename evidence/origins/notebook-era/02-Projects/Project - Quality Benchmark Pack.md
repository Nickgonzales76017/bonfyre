---
type: project
cssclasses:
  - project
title: Quality Benchmark Pack
created: 2026-04-04
updated: 2026-04-04
status: planned
stage: setup
priority: medium
review_cadence: weekly
system_role: Enabler
idea_link: [[01-Ideas/Quality Benchmark Pack]]
tags:
  - project
  - planned
  - quality
  - evaluation
  - data
aliases:
  - Quality Benchmark Pack
---


# Project: Quality Benchmark Pack

## Summary
A small benchmark set of real transcripts with human quality judgments so heuristic scores can be calibrated against reality.

## Objective
Collect 10 transcripts with human ratings for transcript accuracy, summary usefulness, and action-item relevance.

## Success Definition
- deliverable: `benchmarks/` directory with 10 rated transcript sets (audio + transcript + rating JSON)
- proof: quality scoring loop can compare heuristic scores against human judgments
- deadline: after 10 real customer jobs completed

## Tooling
### Project Tooling
- primary tools: JSON rating schema, manual evaluation, file system
- supporting tools: Quality Scoring Loop for comparison

## Execution Plan
### Phase 1 - Schema
- [ ] Define rating JSON schema: transcript_accuracy (1-5), summary_quality (1-5), action_items_relevant (1-5), notes
- [ ] Create benchmark directory structure

### Phase 2 - Collect
- [ ] Select 10 diverse transcripts (varied length, speaker count, audio quality)
- [ ] Rate each transcript manually against the schema
- [ ] Save as `benchmarks/<id>/rating.json`

### Phase 3 - Calibrate
- [ ] Run Quality Scoring Loop heuristics against benchmark set
- [ ] Compare heuristic scores vs human ratings
- [ ] Identify where heuristics drift from reality

## Links
### Related Idea
- [[01-Ideas/Quality Benchmark Pack]]

### Adjacent Projects
- [[02-Projects/Project - Quality Scoring Loop]]
- [[02-Projects/Project - Transcript Asset Store]]
- [[02-Projects/Project - Local AI Transcription Service]]

---

## Execution Log
### 2026-04-04
- project created from idea promotion

---

## Status
planned
