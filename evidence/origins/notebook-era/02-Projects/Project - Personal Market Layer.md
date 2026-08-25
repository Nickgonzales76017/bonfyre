---
type: project
cssclasses:
  - project
title: Personal Market Layer
created: 2026-04-03
updated: 2026-04-04
status: active
stage: build
priority: high
review_cadence: weekly
system_role: Builder
idea_link: [[01-Ideas/Personal Market Layer]]
tags:
  - project
  - active
aliases:
  - Personal Market Layer
---

# Project: Personal Market Layer

## Summary
Create a repeatable way to turn outputs from this vault into clear, sellable offers.

## Objective
Turn one working system output into a buyer-ready offer with positioning, pricing, and a simple delivery workflow.

## Success Definition
- deliverable: one complete offer page and one outreach-ready pitch
- proof: first offer can be explained in under 30 seconds
- deadline: immediately after the transcription workflow works once

## Tooling
### Project Tooling
- primary tools: offer notes, research notes, Obsidian project/system notes
- supporting tools: message drafts, simple landing-page or listing copy, structured templates

### Meta Tooling
- shared tooling or infrastructure: vault note architecture, monetization templates, future automation for offer detection and review
- linked meta system:
  - [[04-Systems/04-Meta/Meta-System]]
  - [[04-Systems/04-Meta/Vault Operating System]]
  - [[04-Systems/04-Meta/Tooling Stack]]
  - [[04-Systems/04-Meta/n8n Workflow Map]]

## Current Thesis
The vault is already generating potential assets. The missing layer is packaging, not raw ideation.

## Execution Plan
### Phase 1 - Inventory
- identify existing outputs worth selling
- rank them by speed to proof and ease of delivery

### Phase 2 - Positioning
- define buyer, painful problem, and clear outcome
- write an offer that sounds like a result, not a feature

### Phase 3 - Distribution
- turn the offer into outreach copy, a listing, and a one-page explanation
- test demand with direct messages before building more infrastructure

## Tasks
### Now
- [x] package the transcription output as the first offer
- [x] write headline, promise, price, and turnaround

### Next
- [ ] create reusable offer template for future systems
- [ ] list 3 candidate buyer segments

### Later
- [ ] build a catalog of offers from systems in the vault
- [ ] create bundles or tiered packaging

## Constraints
- clarity of positioning
- limited real-world validation so far
- distribution is still manual

## Risks
- offers may stay too abstract
- pricing may not match perceived value yet
- packaging can drift away from actual fulfillment reality

## Metrics
- success metric: first offer generates a reply or sale
- leading indicator: a specific buyer can understand it instantly
- failure condition: offer stays vague and untestable

## Dependencies
### Requires
- [[02-Projects/Project - Local AI Transcription Service]]
- [[05-Monetization/Offer - Local AI Transcription Service]]
- [[04-Systems/01-Core Systems/Personal Market Layer]]
- [[02-Projects/Project - Quiet Distribution Engine]]
- [[02-Projects/Project - Repackaged Service Marketplace]]

### Blocks
- [[04-Systems/01-Core Systems/Service Arbitrage Hub]]

## Execution Log
### 2026-04-03
- project created
- narrowed to turning real outputs into marketable offers
- `10-Code/PersonalMarketLayer` now generates `offer.json`, `offer.md`, `outreach.md`, and `listing.md` from reviewed proof assets
- the first generated vault monetization note now exists at `05-Monetization/Offer - Founder Sample - PickFu Offer.md`
### 2026-04-04
- offer generation now reads richer intake context when present, including buyer, service lane, output shape, and next-step hints
- generated vault offers now document routing as part of the sales/delivery surface
- generated offer catalog now carries service-lane and output-shape metadata for live offers
- `10-Code/ProductPipelines/orchestrate.py` now ties this project into the named `transcription-revenue` loop with distribution and analytics refresh

## Decisions
- first offer should come from a real working workflow
- direct, plain-language positioning beats clever branding
- market packaging should inherit structured intake context instead of re-inferring everything from proof labels

## Links
### Source Idea
- [[01-Ideas/Personal Market Layer]]

### Systems
- [[04-Systems/01-Core Systems/Personal Market Layer]]
- [[04-Systems/01-Core Systems/Local AI Transcription Service]]
- [[04-Systems/04-Meta/Tooling Stack]]

### Related Projects
- [[02-Projects/Project - Quiet Distribution Engine]]
- [[02-Projects/Project - Repackaged Service Marketplace]]
- [[02-Projects/Project - Personal Data Engine]]

### Concepts
- [[04-Systems/03-Concepts/Monetization]]
- [[04-Systems/03-Concepts/Distribution]]
- [[04-Systems/03-Concepts/Coordination]]

## Tags
#project #execution #ACTIVE
