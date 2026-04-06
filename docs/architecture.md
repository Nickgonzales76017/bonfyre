# Bonfyre Architecture

## Design philosophy

1. **One binary, one job.** Each tool does one thing. Compose via pipes and files.
2. **SQLite everywhere.** Every binary that needs state uses SQLite. One folder backup.
3. **Zero cloud.** Everything runs on your machine. No external API calls unless you choose to.
4. **C11 for size and speed.** Every binary is < 70 KB. Startup is < 50 ms.

## Layers

```
┌──────────────────────────────────────────────────────────┐
│  Layer 4: Monetization                                    │
│  gate · meter · ledger · finance · offer · outreach · pay │
├──────────────────────────────────────────────────────────┤
│  Layer 3: Audio Pipeline                                  │
│  ingest → media-prep → transcribe → clean → paragraph     │
│  → brief → proof → pack → distribute                      │
├──────────────────────────────────────────────────────────┤
│  Layer 2: Orchestration                                   │
│  pipeline · cli · queue · sync · stitch                   │
├──────────────────────────────────────────────────────────┤
│  Layer 1: Infrastructure                                  │
│  cms · api · auth · index · graph · runtime · hash        │
├──────────────────────────────────────────────────────────┤
│  Library: liblambda-tensors                               │
│  Structural compression for JSON families                 │
└──────────────────────────────────────────────────────────┘
```

## Data flow

Every binary reads from stdin or files and writes to stdout or files.

```
audio.mp3
  → bonfyre-ingest     (creates manifest.json + copies file)
  → bonfyre-media-prep (normalizes to 16kHz mono WAV)
  → bonfyre-hash       (SHA-256 content address)
  → bonfyre-transcribe (Whisper speech-to-text)
  → bonfyre-transcript-clean (remove filler)
  → bonfyre-paragraph  (structure text)
  → bonfyre-brief      (extract summary)
  → bonfyre-proof      (quality score)
  → bonfyre-offer      (generate pricing)
  → bonfyre-pack       (ZIP deliverable)
```

Or use `bonfyre-pipeline run` which does all of this in-process in 5–8 ms.

## Storage

All data lives in `~/.local/share/bonfyre/` by default:

```
~/.local/share/bonfyre/
├── cms.db          # BonfyreCMS schemas, content, tokens
├── jobs.db         # BonfyreAPI job tracking
├── meter.db        # Usage metering
├── index.db        # Artifact index
├── uploads/        # Uploaded files
└── artifacts/      # Pipeline outputs
```

## HTTP endpoints (bonfyre-api)

```
GET  /api/health           → {"status":"ok","version":"1.0.0"}
GET  /api/status           → job counts, upload counts, available binaries
POST /api/upload           → multipart file upload
POST /api/jobs             → submit pipeline job
GET  /api/jobs             → list all jobs
GET  /api/jobs/:id         → job detail
*    /api/binaries/:name/* → proxy to any bonfyre-* binary
GET  /*                    → static files (frontend SPA)
```

## Build system

Each binary has its own `Makefile` in `cmd/BonfyreName/`. The top-level `Makefile` builds everything:

```bash
make              # Build lib + all 38 binaries
make lib          # Build liblambda-tensors only
make install      # Install to ~/.local/bin
make clean        # Clean everything
make test         # Smoke test all binaries
```

## Adding a new binary

1. Create `cmd/BonfyreYourThing/src/main.c`
2. Create `cmd/BonfyreYourThing/Makefile` (copy any existing one)
3. Implement at minimum: `status` command, `--help`, `--version`
4. `make` in the new directory
5. Add to `install.sh`
6. Open a PR
