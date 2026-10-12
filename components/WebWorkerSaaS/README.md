# Web Worker SaaS

Drop-first browser app for submitting audio transcription jobs.

## What It Does

Drop an audio file, give it a name, hit submit. That's it.

Everything else — client details, output preferences, turnaround targets — is optional and tucked behind a "More details" toggle. Quick-pick presets (Founder Memo, Customer Call, Consultant Recap) fill in smart defaults with one click.

### Under the hood
- files and jobs stored locally in IndexedDB (nothing leaves the browser)
- file size validation (warns >25 MB, blocks >100 MB)
- search and filter across all submitted files
- inline detail drawer shows job metadata, brief, and results
- import markdown deliverables back into the job
- auto-completes jobs with turnaround timing when results arrive
- export: brief (markdown), manifest (JSON), full package (JSON + embedded audio)
- toast notifications on every action, confirm dialog on destructive ops
- installable PWA shell with offline cache and lightweight install prompt
- offline fallback page for uncached navigation failures

## Run

```bash
cd 10-Code/WebWorkerSaaS
python3 -m http.server 8080
```

Open `http://localhost:8080`.

For the install prompt and service worker, use a real origin like `http://localhost:8080`, not `file://`.

## Workflow
1. Drop or browse an audio file
2. Name it, optionally pick a preset or add details
3. Submit — job saved locally, appears in "Your files" below
4. Click a job card to open the detail drawer
5. Export the package and run the transcription pipeline
6. Import the `.md` deliverable — job auto-completes
7. View, copy, or export results from the drawer

## Pipeline Handoff

Use **Export Package** for a single-file handoff:

```bash
cd 10-Code/LocalAITranscriptionService
PYTHONPATH=src python3 -m local_ai_transcription_service.cli --intake-package job.package.json
```

## Architecture
```
Browser (IndexedDB + vanilla JS, zero deps)
 ├── Drop zone → file accepted, context form revealed
 ├── Submit → job stored in IndexedDB
 ├── Your Files → searchable/filterable job list
 ├── Detail Drawer → status, brief, exports, results
 ├── PWA Shell → manifest + service worker + install prompt
 └── Lifecycle: captured → ready → processing → done
```

Zero dependencies. Zero backend. Zero build step.
