# Local AI Transcription Service

Minimal local-first transcription delivery scaffold.

## What Exists
- CLI entrypoint
- job workspace creation
- transcript import flow
- raw audio wrapper flow with environment detection
- optional ffmpeg normalization before Whisper
- simple summary and action-item extraction
- markdown deliverable generation
- optional local Piper speech output

## Current Limitation
The main gap is no real customer-grade file has been run through the full intake -> transcription -> deliverable -> review loop yet.

## Run
```bash
cd 10-Code/LocalAITranscriptionService
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --transcript-file sample_transcript.txt
```

## Environment
```bash
cd 10-Code/LocalAITranscriptionService
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --check-env
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --bootstrap-plan
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --check-model-cache --whisper-model base
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --warm-model-cache --whisper-model base
```

`--check-env` now reports:
- `whisper_binary`
- `ffmpeg_binary`
- `piper_binary`
- `piper_model`

Heavy commands now use runtime guardrails by default:
- refuse to start if another heavy Bonfyre job is already running
- refuse to start if the machine's 1-minute load average is already too high
- bypass only with `--unsafe-skip-guardrails`
- generated output folders are marked with `.metadata_never_index` so Spotlight stays out of hot artifacts

Current tuned defaults for this MacBook live in `.bonfyre-runtime/guardrails.json`:
- `local_ai_transcription_service`: `12.0`
- `local_ai_transcription_service:model_warmup`: `5.0`
- `nightly_brainstorm`: `8.0`

Check them with:
```bash
cd 10-Code/LocalAITranscriptionService
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --check-runtime
```

## Raw Audio
```bash
cd 10-Code/LocalAITranscriptionService
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --audio-file sample_audio.m4a
```

## Intake Handoff
```bash
cd 10-Code/LocalAITranscriptionService
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --audio-file sample_audio.m4a --intake-manifest exported-job.intake.json
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --transcript-file sample_transcript.txt --intake-manifest exported-job.intake.json
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --intake-package exported-job.intake-package.json
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --intake-dir intake-drop
```

Browser intake manifests are exported by `10-Code/WebWorkerSaaS` and provide a reusable handoff contract:
- job identity
- client context
- output goal
- captured notes

For a smoother handoff, `10-Code/WebWorkerSaaS` can now export a one-file intake package with the source file embedded directly.

## Fast Rebuild
```bash
cd 10-Code/LocalAITranscriptionService
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --rebuild-job outputs/founder-sample-pickfu-assumptions
```

Use this when summary or formatting logic changes and you want to rebuild an existing deliverable from saved transcript artifacts without rerunning Whisper.

## Lightweight Queue
```bash
cd 10-Code/LocalAITranscriptionService
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --enqueue-intake-package samples/intake-packages/customer-01-gaurav-conversations.intake-package.json
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --queue-status
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --process-queued --max-queued-jobs 1
```

Use this to stage work when the machine is busy, then drain the queue later without losing track of what should run next.

## Auto-Drain
```bash
cd 10-Code/LocalAITranscriptionService
chmod +x drain_queue.sh
launchctl unload ~/Library/LaunchAgents/com.bonfyre.local-ai-transcription-queue.plist 2>/dev/null || true
cp com.bonfyre.local-ai-transcription-queue.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.bonfyre.local-ai-transcription-queue.plist
```

What it does:
- checks the queue every 20 minutes
- processes at most 1 queued job per run
- still honors the shared runtime guardrails
- writes logs to `10-Code/LocalAITranscriptionService/logs/`

## Proof Promotion
```bash
cd 10-Code/LocalAITranscriptionService
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --promote-proof outputs/founder-sample-pickfu-assumptions --proof-label "Founder Sample - PickFu"
```

What it does:
- copies the core job artifacts into `samples/proof-deliverables/<proof-slug>/`
- writes `proof-summary.json`
- updates `samples/proof-deliverables/index.json`

## Proof Review
```bash
cd 10-Code/LocalAITranscriptionService
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --review-proof samples/proof-deliverables/founder-sample-pickfu
```

What it does:
- writes `proof-review.json`
- gives a lightweight recommendation: `promote`, `usable-with-review`, or `hold`

## Speech Output
```bash
cd 10-Code/LocalAITranscriptionService
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --transcript-file sample_transcript.txt --tts
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --audio-file sample_audio.m4a --tts --tts-input deliverable
```

Speech defaults reuse the Piper config already present in `10-Code/NightlyBrainstorm/nightly.json`.

## Batch
```bash
cd 10-Code/LocalAITranscriptionService
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --transcript-dir samples/transcripts
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --audio-dir samples/audio --whisper-model base --language en
```

## Benchmark
```bash
cd 10-Code/LocalAITranscriptionService
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --benchmark-dir benchmarks/sample-pack --summary-bullets 3
```

## Output
By default jobs are written to `./outputs/<job-slug>/` with:
- `raw_transcript.txt`
- `transcript.txt`
- `deliverable.md`
- `meta.json`
- `speech.wav` when `--tts` is enabled

Batch runs also write:
- `batch-summary.json`
- `batch-failures.json`

Benchmark runs write:
- `benchmark-results.json`

## Next Upgrade
- improve install automation beyond bootstrap planning
- support richer summaries
- calibrate model cache and warm-up behavior for multiple Whisper models
