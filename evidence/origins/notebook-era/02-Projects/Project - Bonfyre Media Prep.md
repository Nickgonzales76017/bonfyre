---
type: project
title: Bonfyre Media Prep
created: 2026-04-04
updated: 2026-04-04
status: active
stage: build
priority: high
review_cadence: weekly
system_role: Builder
idea_link: [[01-Ideas/Bonfyre Media Prep Binary]]
tags:
  - project
  - active
  - tooling
  - native
  - audio
aliases:
  - Project - Bonfyre Media Prep
  - Bonfyre Media Prep
  - BonfyreMediaPrep
---

# Project: Bonfyre Media Prep

## Summary
Build a native low-level media binary that handles inspect, normalize, and chunk as a fast upstream layer for transcription and future audio products.

## Objective
Ship one tiny C binary that can be compiled locally and used immediately by the transcription stack without a Python dependency.

## Success Definition
- deliverable: one compiled binary with `inspect`, `normalize`, and `chunk`
- proof: it runs successfully against local media and can replace a slice of wrapper glue
- deadline: now

## Tooling
### Project Tooling
- primary tools: C, `ffmpeg`, `ffprobe`, Makefile, filesystem contracts
- supporting tools: Local AI Transcription Service, Whisper + FFmpeg Wrapper Kit

### Meta Tooling
- linked meta system:
  - [[04-Systems/04-Meta/Tooling Stack]]

## Execution Plan
### Phase 1 - Binary Core
- build CLI contract
- call `ffprobe` for inspect
- call `ffmpeg` for normalize and chunk

### Phase 2 - Stack Integration
- link the binary into wrapper-kit and transcription docs
- decide where it should replace Python glue first

### Phase 3 - Binary Family
- use this pattern for `bonfyre-transcribe`, `bonfyre-brief`, `bonfyre-sync`, and `bonfyre-queue`

## Dependencies
### Requires
- [[02-Projects/Project - Whisper + FFmpeg Wrapper Kit]]
- [[02-Projects/Project - Local AI Transcription Service]]
- [[04-Systems/01-Core Systems/Bonfyre Media Prep]]

## Execution Log
### 2026-04-04
- project created
- first native binary scaffold started in `10-Code/BonfyreMediaPrep`

## Links
### Source Idea
- [[01-Ideas/Bonfyre Media Prep Binary]]

### Systems
- [[04-Systems/01-Core Systems/Bonfyre Media Prep]]
- [[04-Systems/01-Core Systems/Whisper + FFmpeg Wrapper Kit]]
