# Tooling Stack

## Purpose
Document the shared tooling layer that projects and systems rely on, so tooling choices are explicit and easy to backlink.

## Principle
Project notes should define the tools they directly use. When a tool or workflow is shared across multiple projects, it should also link back here or another meta note instead of being described in isolation every time.

## Tooling Layers

### 1. Vault Tooling
- Obsidian as the source of truth
- folder architecture for ideas, projects, systems, monetization, and logs
- backlinks, graph, properties, bookmarks, and search

Primary notes:
- [[04-Systems/04-Meta/Vault Operating System]]
- [[Dashboard]]

### 2. Template Tooling
- Templater folder templates
- idea, project, system, pipeline, research, offer, and log templates
- daily note automation into `06-Logs`

Primary notes:
- [[04-Systems/04-Meta/Vault Operating System]]
- [[Templates/-_tpl Project - Full]]
- [[Templates/-_tpl System Template]]

### 3. Automation Tooling
- future n8n workflows
- local scripts and file-based automations
- note generation and enrichment helpers
- shared runtime guardrails for heavy local jobs
- lightweight file-backed queue for staging heavy local jobs until the machine is ready
- optional launchd auto-drain wrappers for staged heavy jobs

Primary notes:
- [[04-Systems/04-Meta/n8n Workflow Map]]
- [[04-Systems/02-Pipelines/Automation-and-External-Pipeline]]

### 4. Delivery Tooling
- local AI tools for transcription and transformation
- markdown output templates
- research and monetization notes for buyer-facing packaging

Primary notes:
- [[04-Systems/01-Core Systems/Local AI Transcription Service]]
- [[05-Monetization/Offer - Local AI Transcription Service]]

### 5. Portable Compute Tooling
- WebAssembly as a reusable compute layer for browser and hybrid products
- Web Workers plus WebAssembly for background execution inside browser software
- wrappers around native or existing toolchains when portability creates leverage

Primary notes:
- [[04-Systems/03-Concepts/WebAssembly]]
- [[04-Systems/01-Core Systems/Web Worker SaaS]]
- [[02-Projects/Project - Web Worker SaaS]]

### 6. Nightly Cognition Tooling
- llama.cpp with Mistral 7B for local LLM inference (no cloud)
- 9 passes: 5 text (idea expansion, project review, system wiring, agent briefs, morning digest) + 4 audio (morning brief, project narrator, idea playback, distribution snippets)
- 7 Perl modules: VaultParse, VaultScan, ContextBundle, LlamaRun, VaultWrite, PiperTTS, NightlyQueue
- Perl orchestrator (nightly.pl) with dry-run, pass selection, queue management, and safety caps
- file-backed job queue in `.bonfyre-runtime/nightly-brainstorm-queue.json` for deferred pass execution
- macOS launchd scheduling at 2 AM for full nightly runs
- unified queue dispatcher in `.bonfyre-runtime/drain_all_queues.pl` coordinates both NightlyBrainstorm and LocalAITranscriptionService queues
- two-lock architecture: `dispatcher.lock` prevents overlapping drain cycles, `heavy-process.lock` prevents overlapping heavy jobs
- shared runtime lock plus load-average startup guard
- managed llama.cpp subprocess timeout and cleanup path to avoid lingering local inference jobs
- 74 integration tests covering all modules

Primary notes:
- [[04-Systems/04-Meta/n8n Workflow Map]]

### 7. Audio Tooling
- Piper TTS for local neural text-to-speech (no cloud, no API keys)
- 4 audio passes: morning brief, project narrator, idea playback, distribution snippets
- .wav output routed to `07-Audio/` vault directory by category
- integrated into nightly.pl as post-text audio synthesis step
- reused by `LocalAITranscriptionService` for optional spoken deliverables

### 8. Runtime Safety Tooling
- shared heavy-process lock in `.bonfyre-runtime/heavy-process.lock`
- dispatcher lock in `.bonfyre-runtime/dispatcher.lock` prevents overlapping queue drain cycles
- unified queue dispatcher (`drain_all_queues.pl`) coordinates both services with priority ordering
- per-process load limits in `.bonfyre-runtime/guardrails.json`
- startup refusal when current system load is already too high
- inter-service load recheck: if transcription spikes CPU, nightly brainstorm is deferred
- explicit override flag only when you intentionally want to risk machine contention
- lower-priority launchd settings for background cognition jobs
- `.metadata_never_index` markers on heavy model, log, runtime, and generated-output folders so Spotlight stays out of hot paths

Primary notes:
- [[04-Systems/01-Core Systems/Local AI Transcription Service]]
- [[04-Systems/01-Core Systems/Piper Audio Layer]]

## Documentation Rule
- if the tool is specific to one project, define it in that project note
- if the tool is reused across projects, add or link it here
- if the tool shapes how the whole vault works, link to a meta-system note

## Current Shared Stack
- Obsidian
- Templater
- daily note workflow
- folder templates
- graph and backlink navigation
- llama.cpp + Mistral 7B (nightly AI cognition — 9 passes, 7 modules, 74 tests)
- Piper TTS (nightly audio synthesis — 4 spoken passes)
- shared runtime guardrails for MacBook safety
- unified queue dispatcher for coordinated drain of both NightlyBrainstorm and LocalAITranscriptionService queues
- two-lock architecture: dispatcher.lock + heavy-process.lock
- per-process guardrails in `.bonfyre-runtime/guardrails.json`
- managed timeout/cleanup for Nightly local inference
- Spotlight exclusions for heavy generated/model folders
- future n8n orchestration
- markdown-first offer and research notes
- WebAssembly as a planned shared compute primitive for browser-facing projects

## Next Improvement
- add concrete WebAssembly candidate modules and wrapper opportunities once the first browser compute path is scoped
