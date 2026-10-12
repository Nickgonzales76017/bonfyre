# Meta-System

## Summary
This vault is a leverage stack for turning ideas, local tools, and repeatable workflows into execution, monetization, and compounding learning.

It is not one product. It is an operating system for generating useful outputs, packaging them, selling them, and improving the machine with evidence.

One missing leverage layer is portable compute. WebAssembly belongs in the meta stack because it can connect local tools, browser products, and low-infrastructure delivery paths across multiple projects.

## Core Layers

### 1. Idea Layer
- [[01-Ideas/Local AI Transcription Service]]
- [[01-Ideas/Personal Market Layer]]
- [[01-Ideas/Service Arbitrage Hub]]
- [[01-Ideas/Browser-Based Compute SaaS]]
- [[01-Ideas/Personal Data Engine]]
- [[01-Ideas/Distribution Layer (Quiet Distribution Engine)]]
- [[01-Ideas/Repackaged Service Bundles]]
- [[01-Ideas/Overseas + AI Hybrid Labor]]

Purpose:
- capture asymmetric opportunities
- rank them by speed to proof, money, and leverage

### 2. Project Layer
- [[02-Projects/Project - Local AI Transcription Service]]
- [[02-Projects/Project - Personal Market Layer]]
- [[02-Projects/Project - Service Arbitrage Hub]]
- [[02-Projects/Project - Web Worker SaaS]]
- [[02-Projects/Project - Personal Data Engine]]
- [[02-Projects/Project - Quiet Distribution Engine]]
- [[02-Projects/Project - Repackaged Service Marketplace]]
- [[02-Projects/Project - AI + Overseas Labor Pipeline]]

Purpose:
- turn promising ideas into a concrete objective, deliverable, and next actions

### 3. System Layer
- [[04-Systems/01-Core Systems/Local AI Transcription Service]]
- [[04-Systems/01-Core Systems/Personal Market Layer]]
- [[04-Systems/01-Core Systems/Service Arbitrage Hub]]
- [[04-Systems/01-Core Systems/Personal Data Engine]]
- [[04-Systems/01-Core Systems/Web Worker SaaS]]
- [[04-Systems/01-Core Systems/Quiet Distribution Engine]]
- [[04-Systems/01-Core Systems/Repackaged Service Marketplace]]

Purpose:
- describe repeatable mechanics and bottlenecks
- stop useful work from living only in memory

### 4. Monetization Layer
- [[05-Monetization/Offer - Local AI Transcription Service]]
- [[04-Systems/03-Concepts/Monetization]]
- [[04-Systems/03-Concepts/Distribution]]

Purpose:
- convert working outputs into offers, pricing, and buyer-facing clarity

### 5. Feedback Layer
- [[06-Logs/2026-04-02]]
- [[06-Logs/2026-04-03]]
- [[04-Systems/01-Core Systems/Personal Data Engine]]

Purpose:
- record what happened
- identify what is worth repeating, fixing, or killing

## Core Loop
Inbox -> Idea -> Project -> System -> Offer -> Delivery -> Log -> Better Idea Selection

## Principles
- local-first when it lowers cost and preserves control
- manual proof before heavy automation
- packaging matters as much as building
- coordination is a source of value
- data should change future decisions
- portable compute matters when it turns proven workflows into cheaper product layers

## Current Build Sequence
1. make the transcription workflow real
2. package it into an understandable offer
3. use the results to strengthen the market and data layers
4. turn demand generation into a repeatable loop through [[02-Projects/Project - Quiet Distribution Engine]]
5. map browser-based productization through [[02-Projects/Project - Web Worker SaaS]]
6. identify where [[04-Systems/03-Concepts/WebAssembly]] makes the browser path materially stronger
7. only then expand into bundles and broader service arbitrage

## What This Vault Should Do Every Week
- produce at least one concrete output
- create or test one monetizable offer
- record what happened
- tighten one reusable system

## Active Infrastructure
The vault now has two live autonomous systems:

### NightlyBrainstorm
- 9 nightly passes (5 text + 4 audio) over the vault via llama.cpp + Mistral 7B + Piper TTS
- 7 Perl modules, 74 integration tests, file-backed job queue
- runs at 2 AM via launchd, or queued passes drain on demand

### LocalAITranscriptionService
- audio-in → deliverable-out pipeline: ffmpeg + Whisper + summary + action items + markdown template
- file-backed intake queue, browser intake bridge, proof promotion path
- 20+ Python modules covering CLI, pipeline, quality, benchmarking, intake, TTS

### Shared Runtime (`.bonfyre-runtime/`)
- unified queue dispatcher (`drain_all_queues.pl`) coordinates both systems
- two-lock architecture: `dispatcher.lock` + `heavy-process.lock`
- per-process load limits in `guardrails.json`
- single launchd plist replaces per-project drain plists

## Meta Insight
The vault wins when it behaves less like a notebook and more like an execution engine with memory.

It gets stronger when shared technical leverage, like WebAssembly, is documented once at the meta layer and then reused across adjacent projects instead of rediscovered note by note.
