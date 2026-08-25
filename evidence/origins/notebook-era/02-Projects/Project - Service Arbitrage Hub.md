---
type: project
cssclasses:
  - project
title: Service Arbitrage Hub
created: 2026-04-03
updated: 2026-04-04
status: active
stage: validation
priority: high
review_cadence: weekly
system_role: Builder
idea_link: [[01-Ideas/Service Arbitrage Hub]]
tags:
  - project
  - active
aliases:
  - Service Arbitrage Hub
---

# Project: Service Arbitrage Hub

## Summary
Build a repeatable arbitrage loop where a service is sold at a higher price than fulfillment cost through coordination, packaging, and quality control.

## Objective
Validate one service loop with known buy price, sell price, delivery path, and margin.

## Success Definition
- deliverable: one mapped service with fulfillment workflow
- proof: margin is visible and quality can be controlled
- deadline: after the transcription offer establishes a baseline

## Tooling
### Project Tooling
- primary tools: offer mapping notes, cost/margin tracking notes, SOP-style workflow notes
- supporting tools: QA checklists, research notes, fulfillment routing documentation

### Meta Tooling
- shared tooling or infrastructure: vault operating structure, automation roadmap, future orchestration for routing and review
- linked meta system:
  - [[04-Systems/04-Meta/Meta-System]]
  - [[04-Systems/04-Meta/Vault Operating System]]
  - [[04-Systems/04-Meta/Tooling Stack]]
  - [[04-Systems/04-Meta/n8n Workflow Map]]

## Current Bet
Start with services adjacent to transcription, research, formatting, or lightweight admin work where AI plus cheap labor can create a clean spread.

## Execution Plan
### Phase 1 - Identify Loop
- pick one service narrow enough to map clearly
- define buyer, source of labor, and desired output

### Phase 2 - Unit Economics
- estimate fulfillment cost
- estimate sell price
- calculate margin and coordination overhead

### Phase 3 - Quality Control
- define acceptance criteria
- identify what AI can do vs what human review should do

## Tasks
### Now
- [ ] map one candidate service with buy price and sell price
- [ ] define the quality control checkpoint

### Next
- [ ] test the cheapest fulfillment source
- [ ] write the buyer-facing version of the offer

### Later
- [ ] turn the loop into a bundle or marketplace-style offer
- [ ] add tracking for margin by job type

## Constraints
- trust and reliability
- coordination overhead
- unclear quality benchmarks at the start

## Risks
- margin can disappear if management overhead is too high
- cheap fulfillment can create quality issues
- without a clear offer, arbitrage stays theoretical

## Metrics
- success metric: one validated arbitrageable service
- leading indicator: fulfillment cost and sell price are both known
- failure condition: cannot maintain quality and margin together

## Dependencies
### Requires
- [[04-Systems/01-Core Systems/Service Arbitrage Hub]]
- [[04-Systems/02-Pipelines/AI + Overseas Labor Pipeline]]
- [[02-Projects/Project - Personal Market Layer]]
- [[02-Projects/Project - AI + Overseas Labor Pipeline]]

### Blocks
- [[01-Ideas/Repackaged Service Bundles]]

## Execution Log
### 2026-04-03
- project created
- focused around proving one tight arbitrage loop first
### 2026-04-04
- `ServiceArbitrageHub` now feeds the named `service-delivery` loop through `10-Code/ProductPipelines/orchestrate.py`
- spread, handoff, downstream delivery, and analytics can now be refreshed as one product pipeline instead of isolated status checks

## Decisions
- start narrow, not broad
- map economics before building more sourcing complexity

## Links
### Source Idea
- [[01-Ideas/Service Arbitrage Hub]]

### Systems
- [[04-Systems/01-Core Systems/Service Arbitrage Hub]]
- [[04-Systems/02-Pipelines/AI + Overseas Labor Pipeline]]
- [[04-Systems/04-Meta/Tooling Stack]]

### Related Projects
- [[02-Projects/Project - AI + Overseas Labor Pipeline]]
- [[02-Projects/Project - Repackaged Service Marketplace]]

### Concepts
- [[04-Systems/03-Concepts/Arbitrage]]
- [[04-Systems/03-Concepts/Coordination]]
- [[04-Systems/03-Concepts/Monetization]]

## Tags
#project #execution #ACTIVE
