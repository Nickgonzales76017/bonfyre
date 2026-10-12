---
type: system
title: Piper Audio Layer
created: 2026-04-03
updated: 2026-04-03
status: active
stage: operating
system_role: Core
source_project: 
review_cadence: weekly
cssclasses:
  - system-note
tags:
  - system
  - active
  - audio
aliases:
  - Piper Audio Layer
---

## Related hubs
- [[04-Systems/02-Pipelines/Transcription Pipeline]]



# System: Piper Audio Layer

## Purpose
Convert local AI-generated text outputs into spoken-word .wav files using Piper TTS. Turns the vault into an audio-native interface across nightly briefs, project updates, idea summaries, distribution snippets, and optional service deliverables.

---

## Outcome
- value created: hands-free consumption of AI-generated vault intelligence
- customer / user: vault owner (you)
- measurable result: nightly audio categories plus reusable spoken artifacts for adjacent local tools

---

## Core Mechanism
Piper TTS runs as a local subprocess (no cloud, no API keys). Text passes through `clean_for_speech()` to strip markdown/frontmatter/symbols, then Piper synthesizes natural speech to .wav files routed into `07-Audio/` subfolders.

Pipeline: `Text source → clean text → Piper subprocess → .wav file`

---

## Tooling
- operating tools: Piper TTS v1.4.2 (local neural TTS)
- voice model: `en_US-lessac-medium.onnx` (60.2MB, medium quality)
- automation tools: PiperTTS.pm Perl module in `NightlyBrainstorm`, shared Python Piper layer in `LocalAITranscriptionService`
- supporting infrastructure: llama.cpp (generates the text that gets spoken)
- meta tooling note: [[04-Systems/04-Meta/Tooling Stack]]

---

## Inputs
- trigger: nightly.pl orchestrator runs audio passes after text passes
- raw materials: AI-generated text from idea_expand, project_review, system_wire, agent_brief, morning_digest, plus optional deliverable text from local service pipelines
- tools: Piper binary at `~/Library/Python/3.9/bin/piper`

---

## Outputs
- artifact: .wav audio files in `07-Audio/` vault directory
- categories:
  - `07-Audio/Daily/` — morning brief summaries (500–1200 words spoken)
  - `07-Audio/Projects/` — per-project narrated updates (100–250 words)
  - `07-Audio/Ideas/` — per-idea spoken summaries (80–200 words)
  - `07-Audio/Distribution/` — punchy audio clips for sharing (40–100 words)
- downstream effect: audio consumption without screen, portable to phone/speaker

---

## Audio Passes (4 nightly)

### 1. Morning Brief Audio (`morning_brief_audio`)
- collects all active projects + latest log + overnight AI activity
- LLM generates a 500–1200 word spoken-format briefing
- writes markdown note + synthesizes to `07-Audio/Daily/Morning-Brief-{date}.wav`
- runs after `morning_digest` text pass

### 2. Project Narrator (`project_narrator`)
- one pass per active project
- LLM generates 100–250 word narrated status update
- audio-only mode (no text note written)
- output: `07-Audio/Projects/{project-name}-{date}.wav`

### 3. Idea Playback (`idea_playback`)
- one pass per active idea (verdict: ACTIVE, not yet project_created)
- LLM generates 80–200 word spoken summary
- audio-only mode
- output: `07-Audio/Ideas/{idea-name}-{date}.wav`

### 4. Distribution Snippet (`distribution_snippet`)
- one pass per active idea/project
- LLM generates 40–100 word punchy pitch clip
- audio-only mode
- output: `07-Audio/Distribution/{name}-{date}.wav`

---

## Flow
1. nightly.pl completes text passes (idea_expand → project_review → system_wire → agent_brief → morning_digest)
2. orchestrator enters audio passes (morning_brief_audio → project_narrator → idea_playback → distribution_snippet)
3. for each audio pass: load profile → select notes → build context → run LLM inference → clean text for speech → Piper synthesis → route .wav to correct 07-Audio/ subfolder
4. `--no-audio` flag skips all audio passes
5. audio skipped automatically during dry-run mode

---

## Control Layer
- owner: nightly.pl orchestrator
- quality check: dry-run mode tests pass routing without synthesis
- manual override: `--no-audio` CLI flag, or `--pass morning_brief_audio` to run single audio pass
- safety caps: same max_notes_per_pass limits apply to audio passes

---

## Current State
- Piper TTS installed and verified (produces .wav from test sentence)
- Voice model downloaded (en_US-lessac-medium, 60.2MB)
- PiperTTS.pm module built with clean_for_speech + synthesize + synthesize_note_audio
- a matching Python Piper layer now exists in `10-Code/LocalAITranscriptionService/src/local_ai_transcription_service/piper.py`
- 4 audio profile configs created
- Orchestrator integration complete (routing, skip_text mode, morning brief pass)
- `LocalAITranscriptionService` now reuses the same Piper binary/model path for optional `--tts` outputs
- 07-Audio/ directory structure created (Daily, Projects, Ideas, Distribution)
- launchd plist updated with Piper PATH
- 56/56 tests passing (including 10 PiperTTS clean_for_speech tests)
- dry_run mode: true (flip to false for live audio generation)

---

## Bottlenecks
- Piper binary not on default PATH — requires full path in config or PATH export
- Medium voice model is 60.2MB — high quality model would be larger but sound better
- .wav files are uncompressed — could add ffmpeg conversion to .mp3 later
- No automatic cleanup of old audio files yet

---

## Metrics
- throughput: 4 audio categories × notes-per-pass = potentially 40+ audio files per night
- quality: medium-quality neural voice (lessac model)
- time: Piper synthesis is fast (~1-3 seconds per clip on M3)

---

## Next Improvement
👉 Add ffmpeg post-processing to convert .wav → .mp3 for smaller file sizes and wider device compatibility

---

## Links
### Upstream
- [[04-Systems/04-Meta/Tooling Stack]]
- [[04-Systems/04-Meta/n8n Workflow Map]]

### Downstream
- [[07-Audio/]] (output directory)

### Related Systems
- [[04-Systems/01-Core Systems/Local AI Transcription Service]]

### Code
- `10-Code/NightlyBrainstorm/lib/PiperTTS.pm`
- `10-Code/NightlyBrainstorm/profiles/morning_brief_audio.json`
- `10-Code/NightlyBrainstorm/profiles/project_narrator.json`
- `10-Code/NightlyBrainstorm/profiles/idea_playback.json`
- `10-Code/NightlyBrainstorm/profiles/distribution_snippet.json`

---

## Tags
#system #ACTIVE #audio #piper #tts
