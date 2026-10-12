---
type: project
cssclasses:
  - project
title: Personal Data Engine
created: 2026-04-03
updated: 2026-04-03
status: planned
stage: setup
priority: high
review_cadence: weekly
system_role: Builder
idea_link: [[01-Ideas/Personal Data Engine]]
tags:
  - project
  - planned
aliases:
  - Personal Data Engine
---

# Project: Personal Data Engine

## Summary
Turn the vault into a feedback engine that captures execution, outcomes, and signals so future prioritization gets sharper instead of repeating guesswork.

## Objective
Build a usable weekly review and signal-tracking loop that improves what gets built, sold, and dropped.

## Success Definition
- deliverable: a repeatable daily-log to weekly-review workflow
- proof: one review note that clearly changes the next week of focus
- deadline: after a small base of logs and project updates exists

## Tooling
### Project Tooling
- primary tools: daily logs, weekly reviews, structured project notes, monetization notes
- supporting tools: Obsidian backlinks, templates, simple review heuristics

### Meta Tooling
- shared tooling or infrastructure: vault architecture, note templates, review workflows, future automation helpers
- linked meta system:
  - [[04-Systems/04-Meta/Meta-System]]
  - [[04-Systems/04-Meta/Vault Operating System]]
  - [[04-Systems/04-Meta/Tooling Stack]]
  - [[04-Systems/04-Meta/n8n Workflow Map]]

## Execution Plan
### Phase 1 - Capture
- use daily logs consistently
- make sure project updates include proof, blockers, and outcomes

### Phase 2 - Review
- generate the first weekly review from real vault activity
- identify top signals and dead ends

### Phase 3 - Feedback
- feed what worked back into project prioritization, offers, and distribution

## Tasks
### Now
- [ ] use the daily log template for several real execution days
- [ ] create the first weekly review note
- [ ] define which signals matter most

### Next
- [ ] compare projects by proof, momentum, and revenue potential
- [ ] create a lightweight scoreboard for the week

### Later
- [ ] automate parts of the review helper
- [ ] track offer performance and channel quality over time

## Constraints
- requires consistent use before insight appears
- can become over-tracking if not tied to decisions
- signal quality depends on honest logging

## Risks
- collecting data without changing behavior
- vague logs that are not useful later
- building analytics before there is enough activity

## Metrics
- success metric: one weekly review that materially changes focus
- leading indicator: logs contain concrete outputs and outcomes
- failure condition: system creates paperwork but not better decisions

## Dependencies
### Requires
- [[04-Systems/01-Core Systems/Personal Data Engine]]
- [[06-Logs/2026-04-02]]
- [[06-Logs/2026-04-03]]

### Blocks
- cleaner prioritization for [[02-Projects/Project - Personal Market Layer]]
- better feedback for [[02-Projects/Project - Local AI Transcription Service]]
- better message selection for [[02-Projects/Project - Quiet Distribution Engine]]

## Decisions
- this is a feedback system, not an analytics build — the goal is better decisions, not more dashboards
- daily logs and weekly reviews are the minimum viable loop before any automation
- NightlyBrainstorm already generates some of this automatically (idea_expand, project_review passes)
- do not build scoring until enough real execution data exists to score

## Links
### Source Idea
- [[01-Ideas/Personal Data Engine]]

### Related Projects
- [[02-Projects/Project - Local AI Transcription Service]]
- [[02-Projects/Project - Personal Market Layer]]
- [[02-Projects/Project - Quiet Distribution Engine]]

### Systems
- [[04-Systems/01-Core Systems/Personal Data Engine]]
- [[04-Systems/04-Meta/Meta-System]]
- [[04-Systems/04-Meta/Tooling Stack]]

### Concepts
- [[04-Systems/03-Concepts/Feedback Loops]]
- [[04-Systems/03-Concepts/Data]]
- [[04-Systems/03-Concepts/Personal Agency]]

## Execution Log
### 2026-04-03
- project created from the data-engine idea
- defined as a review and signal loop rather than a heavy analytics build
- NightlyBrainstorm `project_review` pass now generates AI-driven project assessments that feed into this system
- daily logs for 2026-04-02 and 2026-04-03 exist with real execution data
- Dashboard.md updated with live priorities and status tracking
### 2026-04-04
- `PersonalDataEngine` now reads generated monetization snapshots before raw tool databases
- distribution traction is now fed from `05-Monetization/_distribution-pipeline-snapshot.json`
- project momentum now reflects proof, offers, outbound activity, replies, and follow-up pressure from the documented operating surface

## Decisions
- start with simple reviews before building automation
- prioritize behavior-changing insights over dashboards
- generated vault snapshots are the preferred analytics boundary; raw SQLite is fallback only

## Links
### Source Idea
- [[01-Ideas/Personal Data Engine]]

### Systems
- [[04-Systems/01-Core Systems/Personal Data Engine]]
- [[04-Systems/04-Meta/Tooling Stack]]

### Related Projects
- [[02-Projects/Project - Local AI Transcription Service]]
- [[02-Projects/Project - Personal Market Layer]]
- [[02-Projects/Project - Quiet Distribution Engine]]

### Concepts
- [[04-Systems/03-Concepts/Data]]
- [[04-Systems/03-Concepts/Feedback Loops]]
- [[04-Systems/03-Concepts/Automation]]

## Tags
#project #planned #feedback
