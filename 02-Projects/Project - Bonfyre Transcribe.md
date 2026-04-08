---
type: project
title: Bonfyre Transcribe
created: 2026-04-04
updated: 2026-04-07
status: active
stage: build
priority: high
review_cadence: weekly
system_role: Builder
idea_link: [[01-Ideas/Bonfyre Transcribe Binary]]
tags:
  - project
  - active
  - tooling
  - native
  - audio
aliases:
  - Project - Bonfyre Transcribe
  - Bonfyre Transcribe
  - BonfyreTranscribe
---

# Project: Bonfyre Transcribe

## Summary
Build a native transcription binary that turns audio into transcript artifacts through `BonfyreMediaPrep` plus Whisper CLI.

## Objective
Ship one compiled binary that handles normalize -> transcribe -> artifact write with minimal startup overhead.

## Success Definition
- deliverable: one binary with stable transcript outputs
- proof: it runs on a real local sample
- deadline: now

## Dependencies
- [[02-Projects/Project - Bonfyre Media Prep]]
- [[02-Projects/Project - Local AI Transcription Service]]
- [[04-Systems/01-Core Systems/Bonfyre Transcribe]]

## Execution Log
### 2026-04-04
- project created
- first native binary scaffold started in `10-Code/BonfyreTranscribe`

### 2026-04-07 — v2.0-hcp: Direct libwhisper + HCP
- **Major rewrite:** fork+exec Python whisper → direct libwhisper C API
- **HCP algorithm:** Complex-domain Hierarchical Constraint Propagation
  - Dual-channel complex lifting: acoustic (sqrt(p), FNV phase) × morphological (sqrt(freq), FNV phase)
  - Radix-2 FFT → three-band adaptive spectral filter + Dirichlet anomaly detection → IFFT
  - Free signal integration: no_speech_prob, compression_ratio, speaker_turn, vlen anomaly, logprob
  - 6.6ms on 2274 tokens (O(N log N), negligible vs 50s decode)
- **Quality uplift:** 0.867 → 0.977 (+12.5%) on 720s test audio
- **Output:** JSON (with per-segment HCP metrics), TXT, SRT, VTT
- **Build:** `-lwhisper -lggml -lz -lm` (Metal GPU, DTW timestamps, beam=5, TinyDiarize)
- **Research:** [[03-Research/Research - Complex Domain HCP for ASR Refinement]]
