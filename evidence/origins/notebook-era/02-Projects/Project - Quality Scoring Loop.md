---
type: project
cssclasses:
  - project
title: Quality Scoring Loop
created: 2026-04-04
updated: 2026-04-04
status: planned
stage: setup
priority: medium
review_cadence: weekly
system_role: Enabler
idea_link: [[01-Ideas/Quality Scoring Loop]]
tags:
  - project
  - planned
  - quality
  - evaluation
  - feedback
aliases:
  - Quality Scoring Loop
---


# Project: Quality Scoring Loop

## Summary
Lightweight evaluation loop that scores transcript and summary quality so improvements are based on evidence, not guesswork.

## Objective
Ship a scoring module that rates each job's transcript and summary output on readability, completeness, and action-item relevance.

## Success Definition
- deliverable: `score.py` that reads pipeline output and produces a quality score JSON
- proof: scores correlate with Quality Benchmark Pack human ratings within 1 point
- deadline: after first 10 deliverables shipped

## Tooling
### Project Tooling
- primary tools: Python, text heuristics (word count, sentence length, filler ratio, action-item count)
- supporting tools: Quality Benchmark Pack for calibration

## Execution Plan
### Phase 1 - Heuristic Scorer
- [ ] Implement text-based heuristics: readability score, filler word ratio, paragraph count, action-item extraction
- [ ] Accept pipeline output folder as input
- [ ] Write `quality.json` alongside deliverable

### Phase 2 - Comparison
- [ ] Compare heuristic scores against benchmark human ratings
- [ ] Tune thresholds based on calibration results

### Phase 3 - Feedback Loop
- [ ] Flag low-scoring deliverables for manual review
- [ ] Track score trends over time per buyer type
- [ ] Optional: gate deliverable release on minimum score

## Links
### Related Idea
- [[01-Ideas/Quality Scoring Loop]]

### Adjacent Projects
- [[02-Projects/Project - Quality Benchmark Pack]]
- [[02-Projects/Project - Local AI Transcription Service]]
- [[02-Projects/Project - Transcript Cleanup Layer]]

---

## Execution Log
### 2026-04-04
- project created from idea promotion

---

## Status
planned
