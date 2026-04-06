<p align="center">
  <h1 align="center">🔥 Bonfyre</h1>
  <p align="center">
    <strong>38 static C binaries. 1.6 MB total. A complete backend platform.</strong>
  </p>
  <p align="center">
    <a href="#install">Install</a> ·
    <a href="#what-is-this">What is this</a> ·
    <a href="#benchmarks">Benchmarks</a> ·
    <a href="#all-38-binaries">All 38 binaries</a> ·
    <a href="#docs">Docs</a> ·
    <a href="#contributing">Contributing</a>
  </p>
</p>

---

```bash
curl -fsSL https://raw.githubusercontent.com/nickgonzales/bonfyre/main/install.sh | sh
```

That installs 38 binaries into `~/.local/bin`. Total disk: **~1.6 MB**. No Node.js. No Python. No Docker. No npm. Just C and SQLite.

---

## What is this

Bonfyre is a complete self-hosted backend platform built as composable C11 binaries.

It started as a replacement for Strapi:

| | **Strapi** | **Bonfyre CMS** |
|---|---|---|
| Install size | ~500 MB | **299 KB** |
| Dependencies | Node.js + 400 npm packages | libc + SQLite |
| Startup | 30–120 seconds | **< 50 ms** |
| Language | JavaScript | C11 |

Then it grew into a full audio-to-invoice pipeline, API gateway, auth system, payment engine, and more — all as standalone static binaries that compose via stdin/stdout and SQLite.

### The compression engine

Bonfyre includes **Lambda Tensors** (`liblambda-tensors`), a structural compression library that encodes the *generative structure* behind data rather than just the bytes:

| Method | Size (N=10,000 JSON records) | Random access |
|---|---|---|
| Raw JSON | 100% | ✓ |
| gzip | 5.5% | ✗ |
| **Lambda Tensors (Huffman)** | **13.5%** | **✓** |

2.4× larger than gzip — but with O(1) per-field random access that gzip can never have. For structured data (APIs, configs, archives), that's a different category of tool.

## Install

### One command (macOS / Linux)

```bash
curl -fsSL https://raw.githubusercontent.com/nickgonzales/bonfyre/main/install.sh | sh
```

### From source

```bash
git clone https://github.com/nickgonzales/bonfyre.git
cd bonfyre
make            # builds all 38 binaries + liblambda-tensors
make install    # copies to ~/.local/bin (or PREFIX=/usr/local make install)
```

### npm (bindings coming soon)

```bash
npm install bonfyre
```

### Requirements

- C11 compiler (gcc or clang)
- SQLite3 development headers (`libsqlite3-dev` / `sqlite3` on macOS)
- zlib (`zlib1g-dev` / included on macOS)

## Quick start

### Start the CMS

```bash
bonfyre-cms serve --port 8800
# REST API running at http://localhost:8800
# Dynamic schemas, token auth, Lambda Tensors compression — 299 KB binary
```

### Run the full audio pipeline

```bash
bonfyre-pipeline run --input interview.mp3 --out ./output
# Ingest → Normalize → Hash → Transcribe → Clean → Paragraph → Brief → Proof → Offer → Pack
# 5–8 ms end-to-end on Apple Silicon
```

### Or run each step individually

```bash
bonfyre-ingest intake interview.mp3 --out ./work/
bonfyre-media-prep normalize ./work/interview.mp3
bonfyre-transcribe run ./work/interview.wav --out transcript.json
bonfyre-brief generate transcript.json --out brief.md
bonfyre-pack bundle ./work/ --out deliverable.zip
```

### Start the HTTP gateway + dashboard

```bash
bonfyre-api --port 9090 --static frontend/ serve
# Dashboard at http://localhost:9090
# REST API at http://localhost:9090/api/
```

## Benchmarks

### Pipeline latency (Apple M-series, single file)

| Mode | Latency |
|---|---|
| Sequential (10 binaries) | 76 ms |
| Optimized (inline SHA-256, direct SQLite) | 20–35 ms |
| **Unified pipeline** | **5–8 ms** |

### Lambda Tensors compression (N=10,000 structured JSON records)

| Encoding | % of raw | Notes |
|---|---|---|
| V1 (varint + zigzag) | 88% | Baseline binary packing |
| V2 (small-int, float32 downshift) | 64.9% | Type-aware encoding |
| V2 + Interned strings | 29% | Cross-member string dedup |
| **V2 + Huffman** | **13.5%** | Family-aware canonical Huffman |

### CMS operations (BonfyreCMS on Apple M-series)

| Operation | Throughput |
|---|---|
| Create | 921 μs/op |
| Update | 155 μs/op |
| Reconstruct (Lambda Tensors) | 89 μs/op |
| ANN k-NN query (N=10,000) | 1.13 ms |

### Memory

| Component | Peak RSS |
|---|---|
| BonfyrePipeline (3,000 artifacts) | 2.36 MB |
| BonfyreCMS idle | 15 MB |

## All 38 binaries

### Infrastructure

| Binary | Size | Purpose |
|---|---|---|
| `bonfyre-cms` | 299 KB | Content management system with Lambda Tensors compression |
| `bonfyre-api` | 69 KB | HTTP gateway, file upload, job management, static server |
| `bonfyre-auth` | 35 KB | User signup/login, session tokens, SHA-256 passwords |
| `bonfyre-index` | 68 KB | SQLite artifact index + full-text search |
| `bonfyre-graph` | 51 KB | Merkle-DAG artifact graph, SHA-256 content addressing |
| `bonfyre-runtime` | 33 KB | Runtime environment, process lifecycle |
| `bonfyre-hash` | 34 KB | Pure C SHA-256 (FIPS 180-4), content addressing |

### Orchestration

| Binary | Size | Purpose |
|---|---|---|
| `bonfyre-pipeline` | 51 KB | Unified in-process pipeline (5–8 ms end-to-end) |
| `bonfyre-cli` | 33 KB | Unified command dispatcher |
| `bonfyre-queue` | 33 KB | Persistent job queue (SQLite) |
| `bonfyre-sync` | 33 KB | Cross-instance replication |
| `bonfyre-stitch` | 33 KB | DAG materializer, result assembly |

### Audio pipeline

| Binary | Size | Purpose |
|---|---|---|
| `bonfyre-ingest` | 33 KB | File intake, type detection, manifest generation |
| `bonfyre-media-prep` | 34 KB | Audio normalization (16 kHz mono, denoise) |
| `bonfyre-transcribe` | 34 KB | Speech-to-text (Whisper) |
| `bonfyre-transcript-family` | 34 KB | Full transcription chain (intake → transcribe) |
| `bonfyre-transcript-clean` | 34 KB | Remove filler words, hallucinations |
| `bonfyre-paragraph` | 34 KB | Structure text into paragraphs |
| `bonfyre-brief` | 34 KB | Extract summary + action items |
| `bonfyre-narrate` | 34 KB | Text-to-speech (Piper TTS) |
| `bonfyre-proof` | 34 KB | Quality scoring + review |
| `bonfyre-pack` | 33 KB | Deliverable packaging (ZIP + manifest) |
| `bonfyre-compress` | 33 KB | File compression (zstd, async) |
| `bonfyre-embed` | 34 KB | Text embeddings (ONNX) |
| `bonfyre-emit` | 33 KB | Multi-format output (pandoc: HTML/PDF/EPUB/RSS) |
| `bonfyre-render` | 33 KB | Template rendering |
| `bonfyre-distribute` | 33 KB | Distribution + messaging (email, Slack, webhooks) |
| `bonfyre-project` | 33 KB | Project scaffolding |
| `bonfyre-mfa-dict` | 34 KB | MFA pronunciation dictionary |
| `bonfyre-weaviate-index` | 34 KB | Vector index (Weaviate semantic search) |

### Monetization

| Binary | Size | Purpose |
|---|---|---|
| `bonfyre-offer` | 33 KB | Dynamic pricing + proposal generation |
| `bonfyre-gate` | 33 KB | API key/tier validation (Free/Pro/Enterprise) |
| `bonfyre-meter` | 34 KB | Usage tracking + per-operation billing |
| `bonfyre-ledger` | 33 KB | Append-only financial records |
| `bonfyre-finance` | 51 KB | Service arbitrage, bundle pricing |
| `bonfyre-outreach` | 34 KB | Outreach tracking, follow-up routing |
| `bonfyre-pay` | 35 KB | Invoicing, payments, credits |

### Library

| Name | Size | Purpose |
|---|---|---|
| `liblambda-tensors` | 41 KB | Structural compression for JSON (static `.a` + shared `.so`) |

## Architecture

```
Audio File
  │
  ├─ bonfyre-ingest         Intake + validation
  ├─ bonfyre-media-prep     Normalize (16 kHz mono)
  ├─ bonfyre-hash           SHA-256 content addressing
  ├─ bonfyre-transcribe     Speech → text (Whisper)
  ├─ bonfyre-transcript-clean   Remove filler
  ├─ bonfyre-paragraph      Structure paragraphs
  ├─ bonfyre-brief          Summary + action items
  ├─ bonfyre-proof          Quality scoring
  ├─ bonfyre-offer          Pricing + proposal
  └─ bonfyre-pack           ZIP deliverable
         │
         ├── bonfyre-gate       Access control
         ├── bonfyre-meter      Usage tracking
         ├── bonfyre-pay        Billing
         └── bonfyre-distribute  Delivery
```

Or skip all that and run `bonfyre-pipeline run` for the unified 5–8 ms fast path.

## When to use Bonfyre

**Use Bonfyre if you:**
- Want a self-hosted CMS that isn't 500 MB of node_modules
- Process audio and need transcription → summary → delivery
- Want to build a SaaS on static binaries with zero cloud dependency
- Need structured JSON compression with random access
- Like Unix philosophy: small tools, stdin/stdout, SQLite

**Don't use Bonfyre if you:**
- Need a GUI content editor (use Strapi or WordPress)
- Want a managed cloud service (Bonfyre Cloud coming soon)
- Work exclusively with unstructured binary data (Lambda Tensors wins on structured data)

## Docs

| Document | Description |
|---|---|
| [Architecture](docs/architecture.md) | System design, data flow, layer model |
| [Benchmarks](docs/benchmarks.md) | Detailed performance numbers |
| [Lambda Tensors](docs/lambda-tensors.md) | Compression algorithm explanation |
| [API Reference](docs/api.md) | HTTP endpoints (bonfyre-api) |
| [CMS Guide](docs/cms.md) | Using bonfyre-cms |
| [Pipeline Guide](docs/pipeline.md) | Audio processing pipeline |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). We welcome:

- Bug reports and fixes
- Performance improvements
- New binary ideas
- Documentation
- Language bindings (Node, Python, Rust, Go)
- Package manager ports (Homebrew, apt, AUR, etc.)

## License

[MIT](LICENSE) — do whatever you want with it.

Made by [Nick Gonzales](https://github.com/nickgonzales).
