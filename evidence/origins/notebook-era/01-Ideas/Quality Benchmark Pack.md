---
type: idea
cssclasses:
  - ideatitle: Quality Benchmark Pack
created: 2026-04-03
updated: 2026-04-03
status: active
stage: exploration
system_role: Enabler
verdict: ACTIVE
project_created: yes
project_link: [[02-Projects/Project - Quality Benchmark Pack]]
project_status: planned
tags:
  - idea
  - active
  - quality
  - evaluation
  - data
aliases:
  - Quality Benchmark Pack
---


# Idea: Quality Benchmark Pack

## Summary
Create a small benchmark set of real transcripts and human judgments so heuristic quality scores can be calibrated against reality.

## Core Insight
The quality loop now exists, but it still grades based on heuristics. Without a benchmark pack, scores are useful for drift detection but weak for deciding whether the deliverable is actually buyer-ready.

## First Use Case
Collect 5 to 10 transcripts with simple human ratings for transcript usefulness, summary quality, and action-item relevance.

## Why It Might Work
- inefficiency: internal scores can drift away from real usefulness
- arbitrage: a tiny benchmark improves every future cleanup and summary upgrade
- trend: lightweight eval sets are becoming core infrastructure for practical AI systems

## Links
### Concepts
- [[04-Systems/03-Concepts/Data]]
- [[04-Systems/03-Concepts/Infrastructure]]

### Related Ideas
- [[01-Ideas/Quality Scoring Loop]]
- [[01-Ideas/Transcript Asset Store]]
- [[01-Ideas/Local AI Transcription Service]]