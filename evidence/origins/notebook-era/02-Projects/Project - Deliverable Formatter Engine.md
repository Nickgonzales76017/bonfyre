---
type: project
cssclasses:
  - project
title: Deliverable Formatter Engine
created: 2026-04-04
updated: 2026-04-04
status: planned
stage: setup
priority: medium
review_cadence: weekly
system_role: Enabler
idea_link: [[01-Ideas/Deliverable Formatter Engine]]
tags:
  - project
  - planned
  - formatting
  - outputs
  - packaging
aliases:
  - Deliverable Formatter Engine
---

## Related hubs
- [[04-Systems/02-Pipelines/Transcription Pipeline]]



# Project: Deliverable Formatter Engine

## Summary
Build a formatter that turns transcripts and summaries into buyer-ready markdown, PDF, email, or exportable note packages.

## Objective
Ship a formatting module that takes raw pipeline output and produces polished deliverables with sections for transcript, summary, action items, and key decisions.

## Success Definition
- deliverable: `format_deliverable.py` that renders structured markdown from pipeline JSON
- proof: 5 real jobs produce deliverables that look professional without manual editing
- deadline: before first paid customer delivery

## Tooling
### Project Tooling
- primary tools: Python, Jinja2 or string templates, markdown
- supporting tools: optional PDF export via markdown-pdf or weasyprint

## Execution Plan
### Phase 1 - Markdown Formatter
- [ ] Define deliverable template: header, transcript excerpt, summary, action items, decisions
- [ ] Accept pipeline output JSON as input
- [ ] Render structured markdown deliverable

### Phase 2 - Multi-Format
- [ ] Add PDF export option
- [ ] Add plain-text email format
- [ ] Add Obsidian-compatible note format

### Phase 3 - Buyer Customization
- [ ] Template variants per buyer type (founder, consultant, creator)
- [ ] Wire template selection to intake metadata

## Links
### Related Idea
- [[01-Ideas/Deliverable Formatter Engine]]

### Adjacent Projects
- [[02-Projects/Project - Local AI Transcription Service]]
- [[02-Projects/Project - Summary Prompt Pack]]
- [[02-Projects/Project - Transcript Paragraphizer]]

---

## Execution Log
### 2026-04-04
- project created from idea promotion

---

## Status
planned
