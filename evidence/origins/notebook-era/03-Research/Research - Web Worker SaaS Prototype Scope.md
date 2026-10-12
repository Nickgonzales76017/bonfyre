---
type: research
cssclasses:
  - research
title: Web Worker SaaS Prototype Scope
created: 2026-04-03
updated: 2026-04-03
status: active
tags:
  - active
  - research
  - product-scope
aliases:
  - Web Worker SaaS Prototype Scope
---


# Research: Web Worker SaaS Prototype Scope

## Question
What is the smallest credible browser-based product path that extends the transcription workflow without overbuilding?

## Why It Matters
This keeps the browser SaaS project tied to a real workflow instead of drifting into vague product ideation.

## Source Workflow
- [[02-Projects/Project - Local AI Transcription Service]]
- [[04-Systems/01-Core Systems/Local AI Transcription Service]]

## Working Assumption
The first browser version should not try to solve everything. It should package one clear part of the transcription experience into a cleaner local-first interface.

## Candidate Prototype Shapes
### 1. Browser Intake + Output Organizer
- user uploads audio
- app stores file state locally
- app presents transcript, summary, and action-item outputs cleanly
- transcription may stay hybrid or externally triggered at first

Why this is attractive:
- easiest bridge from current manual workflow
- focuses on user experience and workflow structure
- does not require full in-browser model execution on day one

### 2. Fully Browser-Based Transcription Demo
- user uploads audio
- processing happens entirely in-browser
- transcript and summary are generated client-side

Why this is attractive:
- strongest local-first story
- highest infrastructure independence

Why this is risky:
- most technically ambitious
- may exceed what should be attempted before product proof

### 3. Hybrid Browser Product
- browser handles intake, progress, storage, and output UX
- local or hybrid processing handles the heavy AI work
- browser returns structured results and keeps session state

Why this is attractive:
- preserves local-first feel
- easier path from the current service model
- likely best balance between ambition and feasibility

## Recommended First Prototype
Start with the hybrid browser product.

Reason:
- it ties directly to the manual transcription project
- it keeps the UX/product layer separate from the hardest model-execution constraints
- it can later move more steps in-browser if the workflow proves valuable

## Prototype Boundary
- in scope:
  - file upload
  - local state and session persistence
  - transcript/summary/action-item display
  - simple project-style output organization
- out of scope:
  - perfect in-browser model execution
  - multi-user collaboration
  - cloud-heavy backend architecture

## Open Questions
- which processing steps are realistic in-browser on target devices?
- does the product need offline-first behavior immediately?
- is the first buyer a service customer, a software user, or both?

## Next Action
- turn this scope into a prototype note inside [[02-Projects/Project - Web Worker SaaS]]
