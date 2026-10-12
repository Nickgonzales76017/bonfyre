---
type: system
cssclasses:
  - system
title: Web Worker SaaS
created: 2026-04-03
updated: 2026-04-04
status: planned
stage: design
source_project: [[02-Projects/Project - Web Worker SaaS]]
system_role: Core
review_cadence: weekly
tags:
  - system
  - planned
aliases:
  - Web Worker SaaS
  - Browser-Based Compute SaaS
---


# System: Web Worker SaaS

## Purpose
Provide a browser-based local-first product layer for workflows that can run on user hardware, starting with a reusable intake and handoff system, with WebAssembly expanding how much real compute can live in the browser over time.

## Outcome
- value created: a software delivery path that reduces backend dependence
- customer: users who want local processing, privacy, and lightweight software
- measurable result: one credible browser workflow that supports a real use case

## Core Mechanism
Browser upload -> local job capture and persistent client-side state -> exportable handoff package -> optional Web Workers/WebAssembly-backed local processing or hybrid handoff -> structured output -> reusable product experience.

## Tooling
- operating tools: Web Workers, Service Workers, IndexedDB, browser file APIs, WebAssembly, frontend application code
- automation tools: client-side processing orchestration, lightweight sync, browser-first workflow state
- supporting infrastructure: Obsidian project/system notes, local transcription learnings, product specs
- meta tooling note:
  - [[04-Systems/04-Meta/Tooling Stack]]
  - [[04-Systems/04-Meta/Vault Operating System]]

## Inputs
- trigger: a proven manual workflow is ready to be translated into product form
- raw materials: validated service flow, transcript output format, prototype spec, user flow assumptions
- tools: browser runtime, local-first storage, frontend interface, WebAssembly-compatible compute modules, optional hybrid processing bridge

## Outputs
- artifact: browser-based workflow, intake console, or prototype definition
- decision: what belongs fully in-browser versus hybrid/local native
- downstream effect: productized software path for a proven service

## Flow
1. capture the validated service workflow from the transcription project
2. isolate the reusable intake and handoff layer from transcription-specific logic
3. separate browser-suitable steps from steps that need WebAssembly support and steps that still need hybrid or native support
4. define and prototype the smallest useful browser product path

## Control Layer
- owner: you
- quality check: browser workflow must stay simpler than the manual service it extends
- manual override: keep the service manual if the product path weakens quality or speed

## Current State
- the idea is now promoted into a project and system
- an intake-focused browser prototype now exists in `10-Code/WebWorkerSaaS`
- the strongest initial use case is still tied to transcription, but the intake layer is documented as reusable across adjacent services
- the browser intake layer now exports a handoff manifest that the local transcription pipeline can consume
- the local transcription pipeline can now process exported intake folders in batch
- the intake layer now also exports routing intelligence so jobs arrive as pre-routed operator work, not just uploaded files
- the queue now supports lightweight batch operator actions without requiring a separate dashboard
- batch results can now be imported back into the browser queue by slug, reducing one-by-one operator cleanup
- operator mode now filters out completed jobs so the browser queue behaves more like a live work surface
- imported markdown deliverables now render as structured sections with nested bullets instead of raw preformatted text
- the browser shell is now installable, caches its core assets, and has an offline fallback page for uncached navigation
- a thin file-based status sync now exists: pipeline-generated `browser-status.json` files can be imported by `jobId` or `jobSlug` to update browser jobs without manual status edits

## Bottlenecks
- manual transcription flow is not proven yet
- browser constraints on transcription and model execution are still uncertain
- the reusable WebAssembly layer has not been scoped as a shared compute primitive yet
- product scope could expand too early without a tight boundary
- the handoff automation exists at the folder level, but the end-to-end user/operator flow is still manual

## Metrics
- throughput: prototype decisions turned into scoped artifacts
- quality: clarity of the browser-first workflow and user experience
- revenue or time saved: reduction in backend dependence and increased product leverage

## Next Improvement
👉 pressure-test the installable shell across real browsers, then decide whether the next sync step should be push/poll or stay file-based longer

## Links
### Source Project
- [[02-Projects/Project - Web Worker SaaS]]

### Upstream
- [[04-Systems/01-Core Systems/Local AI Transcription Service]]
- [[02-Projects/Project - Local AI Transcription Service]]

### Downstream
- [[02-Projects/Project - Personal Market Layer]]
- [[02-Projects/Project - Local AI Transcription Service]]

### Related Pipelines
- [[04-Systems/02-Pipelines/Automation-and-External-Pipeline]]

### Related Concepts
- [[04-Systems/03-Concepts/WebAssembly]]
- [[04-Systems/03-Concepts/Local-First]]

### Adjacent Ideas
- [[01-Ideas/Simple Intake Portal]]

## Tags
#system #planned #infrastructure #compute
