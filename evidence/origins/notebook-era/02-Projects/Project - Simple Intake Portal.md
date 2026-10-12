---
type: project
cssclasses:
  - project
title: Simple Intake Portal
created: 2026-04-04
updated: 2026-04-04
status: planned
stage: setup
priority: medium
review_cadence: weekly
system_role: Enabler
idea_link: [[01-Ideas/Simple Intake Portal]]
tags:
  - project
  - planned
  - intake
  - product
  - distribution
aliases:
  - Simple Intake Portal
---

## Related hubs
- [[04-Systems/02-Pipelines/Transcription Pipeline]]



# Project: Simple Intake Portal

## Summary
Lightweight upload-and-intake experience so the transcription workflow can accept customer files without manual handholding. Builds on the WebWorkerSaaS prototype.

## Objective
Ship a customer-facing intake portal that accepts audio uploads, captures minimal context, and creates structured job records.

## Success Definition
- deliverable: hosted intake page that a customer can use without operator assistance
- proof: 3 real customers submit files through the portal without help
- deadline: before scaling to paid customers beyond personal network

## Current State
- a first local-first browser prototype exists in `10-Code/WebWorkerSaaS`
- stores jobs in IndexedDB, captures context, exports handoff manifests
- handoff manifests consumable by `LocalAITranscriptionService`
- this layer should stay reusable across service flows

## Tooling
### Project Tooling
- primary tools: HTML/CSS/JS (vanilla), IndexedDB, optional lightweight backend for file upload
- supporting tools: WebWorkerSaaS as starting point

## Execution Plan
### Phase 1 - Customer Polish
- [ ] Simplify the WebWorkerSaaS drop-first flow for customer-facing use
- [ ] Remove operator-facing features (manifests, packages, advanced exports)
- [ ] Add clear status communication (submitted → processing → ready)

### Phase 2 - File Delivery
- [ ] Add lightweight upload endpoint (Cloudflare Worker or simple server)
- [ ] Notify operator on new submission
- [ ] Wire into pipeline intake

### Phase 3 - Multi-Service
- [ ] Abstract intake form to support non-transcription services
- [ ] Connect to Service Arbitrage Hub for routing

## Links
### Related Idea
- [[01-Ideas/Simple Intake Portal]]

### Adjacent Projects
- [[02-Projects/Project - Web Worker SaaS]]
- [[02-Projects/Project - Local AI Transcription Service]]
- [[02-Projects/Project - Service Arbitrage Hub]]

---

## Execution Log
### 2026-04-04
- project created from idea promotion

---

## Status
planned
