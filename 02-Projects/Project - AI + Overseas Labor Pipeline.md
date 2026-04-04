---
type: project
cssclasses:
  - project
title: AI + Overseas Labor Pipeline
created: 2026-04-03
updated: 2026-04-03
status: planned
stage: setup
priority: medium
review_cadence: weekly
system_role: Builder
idea_link: [[01-Ideas/Overseas + AI Hybrid Labor]]
tags:
  - project
  - planned
aliases:
  - AI + Overseas Labor Pipeline
  - Overseas + AI Hybrid Labor
---

# Project: AI + Overseas Labor Pipeline

## Summary
Operationalize a hybrid workflow where AI handles the first pass and low-cost human review handles edge cases, starting with transcription as the first real use case.

## Objective
Define a viable hybrid fulfillment loop that improves quality without destroying margin.

## Success Definition
- deliverable: one hybrid review workflow with cost, QA step, and delivery logic
- proof: a clear decision on when human review helps enough to justify the extra step
- deadline: after the manual transcription workflow is functioning

## Tooling
### Project Tooling
- primary tools: local AI outputs, QA checklists, SOP notes, cost tracking notes
- supporting tools: provider sourcing notes, fulfillment workflow notes, service delivery logs

### Meta Tooling
- shared tooling or infrastructure: automation roadmap, vault operating system, tooling stack, workflow orchestration notes
- linked meta system:
  - [[04-Systems/04-Meta/Meta-System]]
  - [[04-Systems/04-Meta/Vault Operating System]]
  - [[04-Systems/04-Meta/Tooling Stack]]
  - [[04-Systems/04-Meta/n8n Workflow Map]]

## Execution Plan
### Phase 1 - Review Need
- identify where pure AI output is weak
- define what a human reviewer should fix

### Phase 2 - Economics
- estimate review cost and time
- compare quality gain against margin loss

### Phase 3 - Workflow
- create one QA checklist
- document handoff and final approval flow

## Tasks
### Now
- [ ] define the first transcription QA checklist
- [ ] identify what would trigger human review
- [ ] estimate likely review cost per file

### Next
- [ ] test the hybrid workflow on one output
- [ ] compare pure AI and hybrid quality

### Later
- [ ] source a repeatable reviewer pool
- [ ] decide whether hybrid review becomes a premium tier or default path

## Constraints
- coordination overhead
- QA standards must be clear
- a human step only makes sense if it materially improves the outcome

## Risks
- hybrid fulfillment may add complexity without enough value
- reviewer quality may be inconsistent
- small spreads can disappear once management is counted

## Metrics
- success metric: one justified hybrid loop with believable margin
- leading indicator: QA improvements are visible and repeatable
- failure condition: the review step costs more than it adds

## Dependencies
### Requires
- [[02-Projects/Project - Local AI Transcription Service]]
- [[04-Systems/02-Pipelines/AI + Overseas Labor Pipeline]]
- [[04-Systems/04-Meta/Tooling Stack]]

### Blocks
- more scalable fulfillment for [[02-Projects/Project - Service Arbitrage Hub]]
- higher-quality bundles for [[02-Projects/Project - Repackaged Service Marketplace]]

## Execution Log
### 2026-04-03
- project created from the hybrid labor idea
- tied directly to transcription as the first proving ground

## Decisions
- do not activate this before the manual transcription loop exists
- start with QA and review, not a broad outsourcing operation

## Links
### Source Idea
- [[01-Ideas/Overseas + AI Hybrid Labor]]

### Related Projects
- [[02-Projects/Project - Local AI Transcription Service]]
- [[02-Projects/Project - Service Arbitrage Hub]]
- [[02-Projects/Project - Repackaged Service Marketplace]]

### Systems
- [[04-Systems/02-Pipelines/AI + Overseas Labor Pipeline]]
- [[04-Systems/04-Meta/Tooling Stack]]

### Concepts
- [[04-Systems/03-Concepts/Automation]]
- [[04-Systems/03-Concepts/Coordination]]
- [[04-Systems/03-Concepts/Arbitrage]]

## Tags
#project #planned #hybrid

## AI Project Review — 2026-04-03

## Refined Next Action
Implement AI transcription workflow with overseas human review for edge cases.

## Blockers
1. Unresolved QA standards: Cannot proceed without clear guidelines.
2. Unsigned contract with overseas labor provider: Cannot hire or onboard team.
3. Undecided cost tracking method: Cannot measure margin improvement.

## Scope Cut
1. Cut extensive research on multiple AI models — loses optimization time, but initial model suffices for proof.
2. Eliminate exploration of advanced natural language processing tools — saves resources, as basic transcription is the first use case.
3. Omit development of complex automation workflows — simplifies setup and reduces coordination overhead.

## Smallest Testable Deliverable
Demo a functional hybrid AI-human transcription workflow with cost tracking in 60 seconds.

## Hidden Assumptions
1. Assumes clear definition of edge cases for human review.
2. Assumes availability and reliability of overseas labor force.
3. Assumes compatibility of QA checklists with AI outputs.

## Milestones to Proof
1. N. Define edge cases — Accept: Pass/Fail test on agreed-upon definition.
2. N. Sign contract with overseas labor provider — Accept: Binary pass if signed.
3. N. Implement cost tracking system — Accept: Pass/Fail test on accurate cost measurement.
4. N. Build and test hybrid transcription workflow — Accept: Successful completion of 60-second demo.
5. N. Launch pilot project with client — Accept: Client satisfaction or revenue generation from the pilot.
