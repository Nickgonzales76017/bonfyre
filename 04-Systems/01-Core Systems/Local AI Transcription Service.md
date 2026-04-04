---
type: system
cssclasses:
  - system
title: Local AI Transcription Service
created: 2026-04-03
updated: 2026-04-04
status: active
stage: operating
source_project: [[02-Projects/Project - Local AI Transcription Service]]
system_role: Core
review_cadence: weekly
tags:
  - system
  - active
aliases:
  - Local AI Transcription Service
---

## Related hubs
- [[04-Systems/02-Pipelines/Transcription Pipeline]]



# System: Local AI Transcription Service

## Purpose
Turn raw audio into a sellable, structured output using local tools first and manual cleanup where necessary.

## Outcome
- value created: useful notes from audio without recurring API cost
- customer: creators, students, founders, and anyone with voice or meeting recordings
- measurable result: transcript + summary + action items delivered reliably

## Core Mechanism
Audio file -> local transcription -> cleanup -> summarization -> formatted delivery -> optional local speech artifact.

## Tooling
- operating tools: local transcription software, Piper TTS, markdown output templates, file-based delivery workflow
- automation tools: future audio intake automation and summary routing
- supporting infrastructure: Obsidian project/system notes, daily logs, offer notes
- implementation path: `10-Code/LocalAITranscriptionService`
- shared tooling dependency: [[04-Systems/01-Core Systems/Piper Audio Layer]]
- meta tooling note:
  - [[04-Systems/04-Meta/Tooling Stack]]
  - [[04-Systems/04-Meta/n8n Workflow Map]]

## Inputs
- trigger: customer sends an audio file
- raw materials: audio file, optional context, preferred output style
- tools: Whisper or equivalent local transcription, markdown template, optional LLM for summary

## Outputs
- artifact: transcript
- artifact: optional spoken summary / spoken deliverable as local `.wav`
- decision: what matters, what actions exist, and what can be ignored
- downstream effect: a deliverable that can be sold, stored, or reused

## Flow
1. intake the file and clarify desired output
2. transcribe locally and review for obvious errors
3. produce summary and action items in a standard format

## Control Layer
- owner: you
- quality check: scan transcript for major misses and confirm summary reflects the recording
- manual override: do summary manually if model output is weak
- runtime guardrail: shared heavy-process lock plus startup load check now prevent blind overlap with other Bonfyre compute jobs
- indexing guardrail: generated output and runtime folders are marked with `.metadata_never_index` so Spotlight stays out of hot artifact paths

## Current State
- project and idea are defined
- local toolchain is now live with `ffmpeg` and Whisper installed
- the operating script now supports raw audio, cleanup, paragraphing, batch processing, failure capture, model cache inspection, heuristic quality scoring, and benchmark pack evaluation
- deliverables can now be rebuilt from saved transcript artifacts without rerunning Whisper, which makes summary and formatting iteration much cheaper on the MacBook
- a browser-side intake prototype now exists to capture files and context before local processing
- a reusable intake manifest contract now connects browser job metadata to the local processing layer
- folder-based intake automation now lets the local pipeline process exported browser job bundles
- one-file intake package import now lets the local pipeline consume browser-exported job bundles without a separate audio download step
- a lightweight local queue now lets intake packages be staged and drained later so capture does not have to immediately become heavy compute
- an optional launchd auto-drain path now lets the queue be checked on a timer while still honoring shared load guardrails
- the same machine-local Piper voice layer used by `NightlyBrainstorm` is now reused here for optional spoken outputs
- the local pipeline now refuses to start new heavy jobs when the MacBook is already too busy unless explicitly overridden
- generated output folders are now excluded from Spotlight indexing to reduce background churn during active work
- browser-based productization is now tracked as a separate planned project
- the first public founder proof sample now produces a substantially better summary and a real action layer after summary/action extraction upgrades
- proof-worthy jobs can now be promoted into a dedicated proof library under `samples/proof-deliverables/` with an indexable manifest
- promoted proofs can now be reviewed with a lightweight scorecard that outputs `promote`, `usable-with-review`, or `hold`
- full-transcript jobs now generate a chunked deep-summary brief instead of relying only on one flat summary pass
- the top summary now derives from the deeper sectioned brief so the executive layer matches the stronger structured output

## Bottlenecks
- first real file has not been processed yet
- quality tolerance for noisy audio is unknown
- there is no saved canonical output template yet
- CPU-only inference speed needs timing benchmarks on longer files
- transcript paragraphing, section shaping, and nested deep-summary formatting now exist, but section-specific rewriting still needs refinement
- benchmark infrastructure exists, but a meaningful human-rated eval set is still missing
- summary and action extraction now look much better on the PickFu sample, but they still need broader validation before they can be trusted as default sellable output
- browser intake and handoff automation now exist at the folder level, but the overall operator flow is still manual and not yet product-smooth
- one-file handoff is smoother, but the real operator loop still needs proof on a customer-like file
- queueing reduces bottleneck pressure, and scheduled draining now exists, but it still needs a tighter operator workflow overall
- spoken output exists, but it is still raw `.wav` and not yet packaged for easier client delivery
- runtime safety exists, but the load threshold still needs tuning against real day-to-day machine usage
- editor/browser churn outside the project can still dominate load even after indexing exclusions
- intake and delivery are note-driven, not productized or automated

## Metrics
- throughput: files processed per week
- quality: transcript usability without heavy rewrite
- revenue or time saved: margin compared with subscription transcription tools

## Next Improvement
👉 run more full-length public customer-like files and tighten the section-specific rewriting layer until both the top summary and deep brief read consistently senior-grade

## Captured Upgrade Opportunities
- [[01-Ideas/Whisper + FFmpeg Wrapper Kit]]
- [[01-Ideas/Audio Intake Normalizer]]
- [[01-Ideas/Transcript Cleanup Layer]]
- [[01-Ideas/Speaker Segmentation Layer]]
- [[01-Ideas/Summary Prompt Pack]]
- [[01-Ideas/Deliverable Formatter Engine]]
- [[01-Ideas/Quality Scoring Loop]]
- [[01-Ideas/Batch Job Runner]]
- [[01-Ideas/Local Bootstrap Kit]]
- [[01-Ideas/Simple Intake Portal]]
- [[01-Ideas/Transcript Asset Store]]
- [[01-Ideas/Whisper Model Cache Manager]]
- [[01-Ideas/Transcript Paragraphizer]]
- [[01-Ideas/Quality Benchmark Pack]]
- [[01-Ideas/Batch Failure Queue]]

## Implemented Upgrades
- [[01-Ideas/Whisper Model Cache Manager]] is now partially implemented in code via cache inspection and warm-up commands
- [[01-Ideas/Transcript Paragraphizer]] is now partially implemented in code via paragraph grouping in deliverables
- [[01-Ideas/Batch Failure Queue]] is now partially implemented in code via retry candidates and failure artifacts
- [[01-Ideas/Quality Benchmark Pack]] is now partially implemented in code via benchmark pack evaluation and saved result artifacts
- [[01-Ideas/Simple Intake Portal]] is now partially implemented in code via a local-first browser intake prototype
- the intake manifest handoff boundary is now implemented in code between browser intake and local processing
- the intake automation layer is now implemented in code via `--intake-dir`
- the one-file intake package handoff is now implemented in code via `--intake-package`
- the shared Piper audio layer is now partially implemented here via optional `--tts` output reusing the Nightly Piper config/model

## Expansion Paths
- [[02-Projects/Project - Whisper + FFmpeg Wrapper Kit]] as the first infrastructure upgrade
- [[04-Systems/01-Core Systems/Whisper + FFmpeg Wrapper Kit]] as the raw audio entry system
- [[02-Projects/Project - Web Worker SaaS]] as a browser-based delivery and product layer
- [[04-Systems/01-Core Systems/Web Worker SaaS]] as the supporting browser product system
- [[04-Systems/01-Core Systems/Piper Audio Layer]] as the shared local speech-output system
- [[04-Systems/03-Concepts/WebAssembly]] as the portable compute layer for future browser-side processing
- hybrid local-plus-browser workflow if full in-browser processing is unrealistic

## Links
### Source Project
- [[02-Projects/Project - Local AI Transcription Service]]

### Upstream
- [[04-Systems/01-Core Systems/Personal Market Layer]]

### Downstream
- [[04-Systems/01-Core Systems/Personal Data Engine]]
- [[02-Projects/Project - Whisper + FFmpeg Wrapper Kit]]
- [[04-Systems/01-Core Systems/Whisper + FFmpeg Wrapper Kit]]
- [[02-Projects/Project - Web Worker SaaS]]
- [[04-Systems/01-Core Systems/Web Worker SaaS]]
- [[04-Systems/01-Core Systems/Piper Audio Layer]]

### Related Pipelines
- [[04-Systems/02-Pipelines/AI + Overseas Labor Pipeline]]
- [[04-Systems/02-Pipelines/Automation-and-External-Pipeline]]

### Related Concepts
- [[04-Systems/03-Concepts/WebAssembly]]

## Tags
#system #ACTIVE
