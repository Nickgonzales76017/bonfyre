---
type: system
title: Bonfyre Media Prep
created: 2026-04-04
updated: 2026-04-04
status: active
stage: build
source_project: [[02-Projects/Project - Bonfyre Media Prep]]
system_role: Core
review_cadence: weekly
tags:
  - system
  - active
  - native
  - audio
aliases:
  - Bonfyre Media Prep
  - BonfyreMediaPrep
---

# System: Bonfyre Media Prep

## Purpose
Provide a tiny native media-prep layer for inspect, normalize, and chunk so upstream audio handling is fast, boring, and reusable.

## Core Mechanism
Input media -> native binary -> `ffprobe` / `ffmpeg` execution -> predictable output artifact and metadata.

## Current State
- first native binary scaffold now exists in `10-Code/BonfyreMediaPrep`
- intended as the low-level evolution of the Whisper + FFmpeg wrapper path

## Next Improvement
👉 wire `BonfyreMediaPrep` into the transcription workflow as the preferred inspect/normalize path

## Links
- [[02-Projects/Project - Bonfyre Media Prep]]
- [[04-Systems/01-Core Systems/Whisper + FFmpeg Wrapper Kit]]
- [[02-Projects/Project - Local AI Transcription Service]]
