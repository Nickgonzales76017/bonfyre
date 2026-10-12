---
type: research
title: Public Sample Test Queue
created: 2026-04-03
updated: 2026-04-03
status: active
stage: working
tags:
  - active
  - research
  - transcription
  - proof
aliases:
  - Public Sample Test Queue
---


# Research: Public Sample Test Queue

## Purpose
Create a repeatable test queue of public and self-recorded audio samples that can serve both validation and proof-of-work marketing.

## Naming Rule
Use:

`persona-##-topic-shortslug`

Examples:
- `founder-01-pickfu-assumptions`
- `customer-01-gaurav-conversations`
- `founder-02-ganesh-operator`
- `investor-01-masha-communications`
- `business-01-chris-side-hustle`

## Codebase Home
- sample workspace: `10-Code/LocalAITranscriptionService/samples/`
- incoming raw clips: `10-Code/LocalAITranscriptionService/samples/incoming-audio/`
- browser handoff packages: `10-Code/LocalAITranscriptionService/samples/intake-packages/`
- proof assets: `10-Code/LocalAITranscriptionService/samples/proof-deliverables/`

## First Queue
| ID | Source Type | Source | Clip Target | Test Goal | Status |
|---|---|---|---|---|---|
| `founder-01-pickfu-assumptions` | Founder podcast | John Li / PickFu interview | 10-12 min | clean founder recap | promoted |
| `customer-01-gaurav-conversations` | Customer interview style | Gaurav Gupta / customer conversations | 12-15 min | pain points + next steps | queued |
| `founder-02-ganesh-operator` | Founder podcast | Ganesh Krishnan operator episode | 15-20 min | strategic founder summary | queued |
| `investor-01-masha-communications` | Founder / investor interview | Masha Bucher episode | 10-15 min | dense conversation recap | queued |
| `business-01-chris-side-hustle` | Business talk | Chris Koerner episode | 12-18 min | long-form readability | queued |

## Self Memo Queue
| ID | Scenario | Goal | Status |
|---|---|---|---|
| `memo-01-founder-brain-dump` | founder end-of-day voice dump | messy async recap | queued |
| `memo-02-customer-call-followup` | post-call next steps | action-item extraction | queued |
| `memo-03-ops-priority-stack` | operator prioritization memo | summary quality | queued |

## Run Pattern
1. source or clip the audio
2. save raw file into `samples/incoming-audio/`
3. create browser intake package in `WebWorkerSaaS`
4. export package into `samples/intake-packages/`
5. run `LocalAITranscriptionService` with `--intake-package`
6. promote best outputs into `samples/proof-deliverables/`

## Promoted Proofs
- `founder-01-pickfu-assumptions` → `samples/proof-deliverables/founder-sample-pickfu/`
- `founder-sample-pickfu` review result: `promote`

## Links
- [[02-Projects/Project - Local AI Transcription Service]]
- [[02-Projects/Project - Web Worker SaaS]]
- [[04-Systems/01-Core Systems/Local AI Transcription Service]]
- [[03-Research/Research - Local AI Transcription Buyers]]
